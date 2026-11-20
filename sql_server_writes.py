"""Validate a single local SQL Server INSERT, UPDATE, or DELETE."""

from sql_expression import ExpressionParser
from sql_scope import sql_tokens

_FUNCTIONS = frozenset(
    {
        "abs",
        "avg",
        "cast",
        "ceiling",
        "coalesce",
        "concat",
        "count",
        "floor",
        "getdate",
        "lower",
        "ltrim",
        "max",
        "min",
        "nullif",
        "round",
        "rtrim",
        "substring",
        "sum",
        "sysdatetime",
        "trim",
        "upper",
    }
)
_EXTERNAL_ROWSETS = frozenset({"openrowset", "openquery", "opendatasource", "bulk"})


def validate_write(sql: str) -> list[tuple[int, int, str]]:
    tokens = [token for token in sql_tokens(sql) if token.kind != "comment"]
    parser = ExpressionParser(tokens, _FUNCTIONS)
    if parser.take("insert"):
        parser.take("into")
        _local_table(parser)
        if parser.take("("):
            parser.identifier()
            while parser.take(","):
                parser.identifier()
            parser.require(")")
        _output(parser)
        if parser.take("default"):
            parser.require("values")
        elif parser.take("values"):
            while True:
                parser.require("(")
                _expressions(parser)
                parser.require(")")
                if not parser.take(","):
                    break
        else:
            _local_select(parser)
    elif parser.take("update"):
        _local_table(parser)
        parser.require("set")
        while True:
            parser.identifier()
            parser.require("=")
            parser.expression()
            if not parser.take(","):
                break
        _output(parser)
        _where(parser)
    elif parser.take("delete"):
        parser.take("from")
        _local_table(parser)
        _output(parser)
        _where(parser)
    else:
        raise ValueError("Only INSERT, UPDATE and DELETE statements are supported")
    parser.take(";")
    parser.finish()
    return [
        (token.start, token.end, token.text[1:]) for token in tokens if token.kind == "parameter"
    ]


def _local_table(parser: ExpressionParser) -> None:
    token = parser.next
    if token is None or token.keyword in _EXTERNAL_ROWSETS:
        raise ValueError("A local SQL Server table is required")
    names = parser.name()
    if len(names) > 2 or any(name.startswith(("#", "@")) for name in names):
        raise ValueError("A local SQL Server table is required")


def _where(parser: ExpressionParser) -> None:
    if parser.take("where"):
        parser.expression()


def _expressions(parser: ExpressionParser) -> None:
    parser.expression()
    while parser.take(","):
        parser.expression()


def _projection(parser: ExpressionParser) -> None:
    start = parser.index
    if parser.take("*"):
        return
    token = parser.next
    if token is not None and token.kind in {"word", "quoted"}:
        parser.identifier()
        if parser.take(".") and parser.take("*"):
            return
    parser.index = start
    parser.expression()
    if parser.take("as"):
        parser.identifier()


def _projections(parser: ExpressionParser) -> None:
    _projection(parser)
    while parser.take(","):
        _projection(parser)


def _output(parser: ExpressionParser) -> None:
    if parser.take("output"):
        _projections(parser)


def _local_source(parser: ExpressionParser) -> None:
    _local_table(parser)
    if parser.take("as"):
        parser.identifier()


def _local_select(parser: ExpressionParser) -> None:
    parser.require("select")
    if not parser.take("all"):
        parser.take("distinct")
    _projections(parser)
    if not parser.take("from"):
        return
    _local_source(parser)
    while True:
        if parser.take(","):
            _local_source(parser)
            continue
        if parser.take("cross"):
            parser.require("join")
            _local_source(parser)
            continue
        if parser.take("left") or parser.take("right") or parser.take("full"):
            parser.take("outer")
            parser.require("join")
        elif parser.take("inner"):
            parser.require("join")
        elif not parser.take("join"):
            break
        _local_source(parser)
        parser.require("on")
        parser.expression()
    _where(parser)
    if parser.take("group"):
        parser.require("by")
        _expressions(parser)
    if parser.take("having"):
        parser.expression()
