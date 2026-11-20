"""Bind a restricted SQL Server SELECT to its Dataset relations and columns."""

from __future__ import annotations

import re
from typing import NamedTuple

from sql_expression import ExpressionParser

Relation = tuple[str, str]
Grant = set[str] | None

SOURCE_ERROR = "Could not confidently identify referenced resources"
COLUMN_ERROR = "Could not confidently identify column ownership"
_UNSUPPORTED = "Unsupported SQL Server Dataset SELECT syntax"
_WORD = r"[^\W\d]\w*"
_LINE_END = re.compile(r"[\r\n]")
_TOKEN = re.compile(
    rf"(?P<space>[ \t\r\n\f\v]+)"
    rf"|(?P<string>[Nn]?'(?:[^']|'')*')"
    rf'|(?P<quoted>\[(?:[^\]]|\]\])*\]|"(?:[^"]|"")*")'
    rf"|(?P<parameter>:[A-Za-z_][A-Za-z0-9_]*)"
    rf"|(?P<binary>0[xX][0-9A-Fa-f]*\b)"
    rf"|(?P<number>(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?)"
    rf"|(?P<word>{_WORD})"
    rf"|(?P<punctuation>[.,()+*/%<>=!&|^~;-])"
)
_NAME_KINDS = {"word", "quoted"}
_CLAUSE_WORDS = {"where", "group", "having", "order"}
_JOIN_WORDS = {"join", "inner", "left", "right", "full", "cross"}
_SOURCE_BOUNDARIES = _CLAUSE_WORDS | _JOIN_WORDS | {"on", "outer"}
_OTHER_CLAUSES = {
    "apply",
    "except",
    "fetch",
    "for",
    "from",
    "into",
    "limit",
    "offset",
    "option",
    "pivot",
    "select",
    "tablesample",
    "top",
    "union",
    "unpivot",
    "with",
    "intersect",
}


class Token(NamedTuple):
    kind: str
    text: str
    start: int
    end: int

    @property
    def keyword(self) -> str:
        return self.text.lower() if self.kind == "word" and self.text.isascii() else ""

    @property
    def name(self) -> str:
        if self.kind == "quoted":
            close = self.text[-1]
            return self.text[1:-1].replace(close * 2, close)
        return self.text


def _block_end(sql: str, start: int) -> int:
    depth = 1
    index = start + 2
    while index < len(sql):
        if sql.startswith("/*", index):
            depth += 1
            index += 2
        elif sql.startswith("*/", index):
            depth -= 1
            index += 2
            if not depth:
                return index
        else:
            index += 1
    raise ValueError("Unterminated SQL block comment")


def sql_tokens(sql: str) -> list[Token]:
    if "\\" in sql or "$" in sql:
        raise ValueError(
            "Read SQL cannot contain backslashes or dollar-quote syntax (raw dollar characters); "
            "use bound parameters"
        )
    tokens = []
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            line_end = _LINE_END.search(sql, index + 2)
            end = line_end.start() if line_end else len(sql)
            tokens.append(Token("comment", sql[index:end], index, end))
        elif sql.startswith("/*", index):
            end = _block_end(sql, index)
            tokens.append(Token("comment", sql[index:end], index, end))
        else:
            match = _TOKEN.match(sql, index)
            if not match:
                raise ValueError(_UNSUPPORTED)
            end = match.end()
            kind = match.lastgroup
            if kind is not None and kind != "space":
                tokens.append(Token(kind, match.group(), index, end))
        index = end
    return tokens


def strip_comments(sql: str) -> str:
    chunks: list[str] = []
    start = 0
    for token in sql_tokens(sql):
        if token.kind == "comment":
            chunks.extend((sql[start : token.start], " "))
            start = token.end
    chunks.append(sql[start:])
    return "".join(chunks)


def quoted_name(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def resource_id(relation: Relation) -> str:
    # Core compares saved scopes against the decoded, dotted spelling.
    return ".".join(relation)


def _name_parts(
    tokens: list[Token], index: int, *, wildcard: bool = False
) -> tuple[list[str], int]:
    if index == len(tokens) or tokens[index].kind not in _NAME_KINDS or not tokens[index].name:
        raise ValueError(SOURCE_ERROR)
    names = [tokens[index].name]
    index += 1
    while index < len(tokens) and tokens[index].text == ".":
        index += 1
        if index == len(tokens):
            raise ValueError(SOURCE_ERROR)
        token = tokens[index]
        if wildcard and token.text == "*":
            return [*names, "*"], index + 1
        if token.kind not in _NAME_KINDS or not token.name:
            raise ValueError(SOURCE_ERROR)
        names.append(token.name)
        index += 1
    return names, index


def _segments(tokens: list[Token], *, comma: bool, keywords: set[str]) -> list[list[Token]]:
    groups: list[list[Token]] = [[]]
    depth = 0
    for token in tokens:
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
        if not 0 <= depth <= 48:
            raise ValueError(_UNSUPPORTED)
        if depth or not (token.keyword in keywords or comma and token.text == ","):
            groups[-1].append(token)
        else:
            groups.append([token] if token.keyword else [])
    if depth or not all(groups):
        raise ValueError(_UNSUPPORTED)
    return groups


class SelectScope:
    def __init__(
        self, tokens: list[Token], allowed: dict[Relation, Grant], functions: frozenset[str]
    ) -> None:
        self.tokens = tokens
        self.allowed = allowed
        self.relations_by_name: dict[str, list[Relation]] = {}
        for relation in allowed:
            self.relations_by_name.setdefault(relation[1], []).append(relation)
        self.functions = functions
        self.bindings: dict[tuple[str, ...], Relation] = {}
        self.sources: list[Relation] = []
        self.replacements: list[tuple[int, int, str]] = []
        self.restricted_sources: list[tuple[Relation, int, int, str]] = []
        self.used_columns: dict[Relation, set[str]] = {}
        self.outside_columns: set[str] = set()

    def _source(self, start: int) -> int:
        names, end = _name_parts(self.tokens, start)
        if len(names) == 2:
            relation = (names[0], names[1])
            matches = [relation] if relation in self.allowed else []
        elif len(names) == 1:
            matches = self.relations_by_name.get(names[0], [])
        else:
            matches = []
        if len(matches) != 1:
            raise ValueError(
                f"Query references resources outside dataset scope: {['.'.join(names)]}. "
                f"Allowed resources: {sorted(resource_id(item) for item in self.allowed)}"
            )
        relation = matches[0]
        if any(name.startswith(("#", "@")) for name in relation):
            raise ValueError(SOURCE_ERROR)
        self.sources.append(relation)
        source_end = self.tokens[end - 1].end
        explicit_alias = end < len(self.tokens) and self.tokens[end].keyword == "as"
        if explicit_alias:
            end += 1
        alias = self.tokens[end] if end < len(self.tokens) else None
        keys: list[tuple[str, ...]]
        if (
            alias is not None
            and alias.kind in _NAME_KINDS
            and alias.keyword not in _SOURCE_BOUNDARIES | _OTHER_CLAUSES
        ):
            if not alias.name:
                raise ValueError(SOURCE_ERROR)
            keys = [(alias.name,)]
            end += 1
        elif explicit_alias:
            raise ValueError(SOURCE_ERROR)
        else:
            keys = [(relation[1],), relation]
        for key in keys:
            if key in self.bindings:
                raise ValueError(COLUMN_ERROR)
            self.bindings[key] = relation
        source_start = self.tokens[start].start
        if self.allowed[relation] is None:
            qualified = ".".join(quoted_name(name) for name in relation)
            self.replacements.append((source_start, source_end, qualified))
        else:
            self.restricted_sources.append(
                (relation, source_start, self.tokens[end - 1].end, keys[0][0])
            )
        return end

    def _column(
        self, names: list[str], tokens: list[Token] | None = None, *, wildcard: bool = False
    ) -> None:
        if len(names) > 1:
            relation = self.bindings.get(tuple(names[:-1]))
            if relation is None:
                raise ValueError(COLUMN_ERROR)
            sources = [relation]
        elif wildcard:
            sources = self.sources
        elif len(self.sources) == 1 or all(
            self.allowed[relation] is None for relation in self.sources
        ):
            sources = self.sources
        else:
            raise ValueError(COLUMN_ERROR)
        for relation in sources:
            permitted = self.allowed[relation]
            if permitted is not None:
                if wildcard:
                    raise ValueError(
                        "Query references columns outside dataset scope: wildcard selection"
                    )
                if names[-1] not in permitted:
                    self.outside_columns.add(f"{resource_id(relation)}.{names[-1]}")
                    continue
                self.used_columns.setdefault(relation, set()).add(names[-1])
                if len(names) == 3 and tokens:
                    # An authorized derived table's outer name is its table alias,
                    # including when the input uses a schema-qualified column.
                    self.replacements.append(
                        (tokens[0].start, tokens[-3].end, quoted_name(relation[1]))
                    )

    def _expression(self, tokens: list[Token]) -> None:
        parser = ExpressionParser(tokens, self.functions)
        parser.expression()
        parser.finish()
        for names, name_tokens in parser.columns:
            self._column(names, name_tokens)

    def _projection(self, tokens: list[Token]) -> set[str]:
        aliases = set()
        for item in _segments(tokens, comma=True, keywords=set()):
            if item[0].text == "*":
                if len(item) != 1:
                    raise ValueError(_UNSUPPORTED)
                self._column(["*"], wildcard=True)
                continue
            if item[-1].text == "*" and len(item) >= 3 and item[-2].text == ".":
                names, end = _name_parts(item, 0, wildcard=True)
                if end != len(item):
                    raise ValueError(_UNSUPPORTED)
                self._column(names, wildcard=True)
                continue
            if len(item) > 2 and item[-2].keyword == "as" and item[-1].kind in _NAME_KINDS:
                alias = item[-1].name
                if not alias or alias in aliases:
                    raise ValueError(_UNSUPPORTED)
                aliases.add(alias)
                item = item[:-2]
            self._expression(item)
        return aliases

    def validate(self) -> None:
        if not self.tokens or self.tokens[0].keyword != "select":
            raise ValueError(_UNSUPPORTED)
        parts = _segments(self.tokens[1:], comma=False, keywords={"from"})
        if len(parts) != 2:
            raise ValueError(SOURCE_ERROR)
        projections, sources = parts
        if projections[0].keyword in {"all", "distinct"}:
            projections = projections[1:]
        self.tokens = sources[1:]
        cursor = self._source(0)
        predicates = []
        while cursor < len(self.tokens):
            token = self.tokens[cursor]
            if token.keyword in _CLAUSE_WORDS:
                break
            if token.text == ",":
                cursor = self._source(cursor + 1)
                continue
            if token.keyword not in _JOIN_WORDS:
                raise ValueError(SOURCE_ERROR)
            join_start = cursor
            if token.keyword != "join":
                cursor += 1
                if cursor < len(self.tokens) and self.tokens[cursor].keyword == "outer":
                    cursor += 1
            if cursor == len(self.tokens) or self.tokens[cursor].keyword != "join":
                raise ValueError(SOURCE_ERROR)
            cursor = self._source(cursor + 1)
            if self.tokens[join_start].keyword == "cross":
                continue
            if cursor == len(self.tokens) or self.tokens[cursor].keyword != "on":
                raise ValueError(SOURCE_ERROR)
            start = cursor + 1
            cursor = start
            depth = 0
            while cursor < len(self.tokens):
                token = self.tokens[cursor]
                if not depth and (
                    token.keyword in _CLAUSE_WORDS | _JOIN_WORDS or token.text == ","
                ):
                    break
                depth += (token.text == "(") - (token.text == ")")
                cursor += 1
            predicates.append(self.tokens[start:cursor])
        if not projections or cursor >= len(self.tokens) or self.tokens[cursor].keyword != "where":
            raise ValueError(_UNSUPPORTED)
        aliases = self._projection(projections)
        clauses = _segments(
            self.tokens[cursor:], comma=False, keywords={"group", "having", "order"}
        )
        for predicate in predicates:
            self._expression(predicate)
        previous = -1
        for clause in clauses:
            keyword = clause[0].keyword
            order = ["where", "group", "having", "order"].index(keyword)
            if order <= previous:
                raise ValueError(_UNSUPPORTED)
            previous = order
            if keyword in {"group", "order"}:
                if len(clause) < 2 or clause[1].keyword != "by":
                    raise ValueError(_UNSUPPORTED)
                for item in _segments(clause[2:], comma=True, keywords=set()):
                    if keyword == "order":
                        if item[-1].keyword in {"asc", "desc"}:
                            item = item[:-1]
                        if (
                            len(item) == 1
                            and item[0].kind in _NAME_KINDS
                            and item[0].name in aliases
                        ):
                            continue
                    self._expression(item)
            else:
                self._expression(clause[1:])
        if self.outside_columns:
            raise ValueError(
                f"Query references columns outside dataset scope: {sorted(self.outside_columns)}"
            )

    def render(self, sql: str) -> str:
        for relation, start, end, alias in self.restricted_sources:
            columns = ", ".join(
                quoted_name(name) for name in sorted(self.used_columns.get(relation, ()))
            )
            # SQL Server can interpret dotted names as CLR properties. Restrict
            # its outer query to authorized fields, even if name binding differs
            # from the scope check. A constant keeps COUNT(*) useful on its own.
            projection = columns or "1 AS [_flowsteward_row]"
            qualified = ".".join(quoted_name(name) for name in relation)
            # Interpolations contain only bracket-escaped identifiers or the fixed fallback.
            source_sql = f"(SELECT {projection} FROM {qualified}) AS {quoted_name(alias)}"  # nosec B608
            self.replacements.append((start, end, source_sql))
        chunks: list[str] = []
        cursor = 0
        for start, end, replacement in sorted(self.replacements):
            chunks.extend((sql[cursor:start], replacement))
            cursor = end
        chunks.append(sql[cursor:])
        return "".join(chunks)


def authorize(
    sql: str, allowed: dict[Relation, Grant], functions: frozenset[str]
) -> tuple[str, set[Relation]]:
    if not allowed:
        raise ValueError("Dataset resource scope is empty")
    scope = SelectScope(
        [token for token in sql_tokens(sql) if token.kind != "comment"], allowed, functions
    )
    scope.validate()
    return scope.render(sql), set(scope.sources)
