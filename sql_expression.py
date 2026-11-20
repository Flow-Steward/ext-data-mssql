"""Consume a bounded SQL Server scalar expression without skipping trailing tokens."""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sql_scope import Token


CAST_TYPES = frozenset(
    {
        "bigint",
        "binary",
        "bit",
        "char",
        "date",
        "datetime",
        "datetime2",
        "datetimeoffset",
        "decimal",
        "float",
        "int",
        "money",
        "nchar",
        "numeric",
        "nvarchar",
        "real",
        "smallint",
        "smallmoney",
        "time",
        "tinyint",
        "uniqueidentifier",
        "varbinary",
        "varchar",
    }
)
NAME_KINDS = {"word", "quoted"}
_UNSUPPORTED = "Unsupported or incomplete SQL Server statement"
_COMPARISONS = {"=", "<", ">", "<=", ">=", "<>", "!=", "!<", "!>"}
_OPERATORS = {
    "or": 1,
    "and": 2,
    **dict.fromkeys(_COMPARISONS | {"is", "in", "like", "between", "not"}, 3),
    **dict.fromkeys({"+", "-", "&", "|", "^"}, 4),
    **dict.fromkeys({"*", "/", "%"}, 5),
}


class ExpressionParser:
    """Read SQL tokens by role and collect only the actual column references."""

    def __init__(self, tokens: list[Token], functions: Collection[str]) -> None:
        self.tokens = tokens
        self.functions = functions
        self.index = 0
        self.columns: list[tuple[list[str], list[Token]]] = []

    @property
    def next(self) -> Token | None:
        return self.tokens[self.index] if self.index < len(self.tokens) else None

    def take(self, value: str) -> bool:
        token = self.next
        if (
            token is None
            or token.keyword != value
            and (token.kind != "punctuation" or token.text != value)
        ):
            return False
        self.index += 1
        return True

    def require(self, value: str) -> None:
        if not self.take(value):
            raise ValueError(_UNSUPPORTED)

    def finish(self) -> None:
        if self.next is not None:
            raise ValueError(_UNSUPPORTED)

    def identifier(self) -> str:
        token = self.next
        if token is None or token.kind not in NAME_KINDS or not token.name:
            raise ValueError(_UNSUPPORTED)
        self.index += 1
        return token.name

    def name(self) -> list[str]:
        parts = [self.identifier()]
        while self.take("."):
            parts.append(self.identifier())
        return parts

    def expression(self, minimum: int = 1, depth: int = 0) -> None:
        if depth > 48:
            raise ValueError(_UNSUPPORTED)
        self._atom(depth)
        while (operator := self._operator()) is not None:
            word, width = operator
            precedence = _OPERATORS[word]
            if precedence < minimum:
                return
            self.index += width
            if word == "not":
                following = self.next
                if following is None or following.keyword not in {"between", "in", "like"}:
                    raise ValueError(_UNSUPPORTED)
                word = following.keyword
                self.index += 1
            if word == "is":
                self.take("not")
                self.require("null")
            elif word == "in":
                self.require("(")
                self.expression(depth=depth + 1)
                while self.take(","):
                    self.expression(depth=depth + 1)
                self.require(")")
            else:
                self.expression(precedence + 1, depth + 1)
                if word == "between":
                    self.require("and")
                    self.expression(precedence + 1, depth + 1)
                if word == "like" and self.take("escape"):
                    self.expression(precedence + 1, depth + 1)

    def _operator(self) -> tuple[str, int] | None:
        token = self.next
        if token is None:
            return None
        if token.kind == "punctuation" and self.index + 1 < len(self.tokens):
            following = self.tokens[self.index + 1]
            combined = token.text + following.text
            if token.end == following.start and combined in _COMPARISONS:
                return combined, 2
        word = token.text if token.kind == "punctuation" else token.keyword
        return (word, 1) if word in _OPERATORS else None

    def _atom(self, depth: int) -> None:
        token = self.next
        if token is None:
            raise ValueError(_UNSUPPORTED)
        if token.kind in {"binary", "number", "string", "parameter"} or token.keyword in {
            "null",
            "current_timestamp",
        }:
            self.index += 1
            return
        if self.take("not"):
            self.expression(3, depth + 1)
            return
        if self.take("+") or self.take("-") or self.take("~"):
            self.expression(6, depth + 1)
            return
        if self.take("("):
            self.expression(depth=depth + 1)
            self.require(")")
            return
        if self.take("case"):
            self._case(depth)
            return
        start = self.index
        parts = self.name()
        if self.take("("):
            if len(parts) != 1 or token.keyword not in self.functions:
                raise ValueError(_UNSUPPORTED)
            self._function(token.keyword, depth)
            return
        self.columns.append((parts, self.tokens[start : self.index]))

    def _case(self, depth: int) -> None:
        if not self.take("when"):
            self.expression(depth=depth + 1)
            self.require("when")
        while True:
            self.expression(depth=depth + 1)
            self.require("then")
            self.expression(depth=depth + 1)
            if not self.take("when"):
                break
        if self.take("else"):
            self.expression(depth=depth + 1)
        self.require("end")

    def _function(self, function: str, depth: int) -> None:
        if function == "cast":
            self.expression(depth=depth + 1)
            self.require("as")
            type_token = self.next
            if type_token is None or type_token.keyword not in CAST_TYPES:
                raise ValueError(_UNSUPPORTED)
            self.index += 1
            if self.take("("):
                if not self.take("max"):
                    self._type_size()
                    if self.take(","):
                        self._type_size()
                self.require(")")
        elif function == "count" and self.take("*"):
            pass
        elif not self.take(")"):
            if not self.take("distinct"):
                self.take("all")
            self.expression(depth=depth + 1)
            while self.take(","):
                self.expression(depth=depth + 1)
        else:
            return
        self.require(")")

    def _type_size(self) -> None:
        token = self.next
        if token is None or token.kind != "number" or not token.text.isdecimal():
            raise ValueError(_UNSUPPORTED)
        self.index += 1
