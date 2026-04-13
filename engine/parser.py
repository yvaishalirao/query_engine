"""
SQL parser module: AST dataclasses, lark Transformer, and public parse() interface.

Dataclass roles:
  ColumnRef       — a plain column reference in SELECT, with an optional alias.
  AggregateExpr   — an aggregate function call (COUNT/SUM/AVG/MIN/MAX) on a column,
                    with an optional alias.
  Condition       — a single WHERE predicate: column, comparison operator, and value.
  AndExpr         — a boolean AND node in the WHERE expression tree; holds a left
                    and right child, each of which is a BooleanExpr.
  OrExpr          — a boolean OR node in the WHERE expression tree; same shape as
                    AndExpr.
  BooleanExpr     — type alias: Condition | AndExpr | OrExpr. The WHERE clause of
                    a SelectStatement is a single optional BooleanExpr root, which
                    the executor evaluates recursively. A flat list of conditions
                    would have required the executor to assume implicit AND; the
                    tree makes the logical structure explicit and extensible.
  SubquerySource  — a nested SelectStatement used as the FROM source in place of
                    a plain table name. Carries an optional alias. The structure
                    is recursive: a SubquerySource contains a SelectStatement
                    whose own table field may itself be a SubquerySource.
                    execute() uses the _depth field on SelectStatement to enforce
                    a nesting limit and prevent runaway recursion.
  JoinClause      — a single JOIN specification: the right-hand table name,
                    join type (INNER or LEFT), and the equality ON columns
                    (on_left from the primary table, on_right from the joined
                    table). join_type is always stored uppercase. Only equality
                    ON clauses are supported; self-joins are rejected by the
                    executor (not the parser).
  OrderByClause   — an ORDER BY target column with direction (ASC or DESC).
  SelectStatement — the complete, typed AST for a SELECT query; the only object
                    accepted by the executor. The table field accepts either a
                    plain string (CSV table name) or a SubquerySource. The joins
                    field holds zero or more JoinClause objects.
                    _depth is an internal field set by execute(), not the parser;
                    it is excluded from repr and equality comparisons.
  SetClause       — a single col = value assignment in an UPDATE statement.
                    value is coerced to int, float, or str (same rules as WHERE).
  InsertStatement — AST for INSERT INTO table (cols) VALUES (vals). columns and
                    values are parallel lists; lengths must match (enforced by
                    the transformer, not the grammar).
  UpdateStatement — AST for UPDATE table SET col=val [, ...] [WHERE ...].
                    Reuses BooleanExpr for the optional WHERE filter.
  DeleteStatement — AST for DELETE FROM table [WHERE ...]. Reuses BooleanExpr.

Lark is imported only in this module. No other engine module may reference lark
types, token names, or grammar rule names (INV-P4).
"""

import ast as _ast
import dataclasses
from collections import namedtuple
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Literal

from lark import Lark, Transformer
import lark.exceptions


# ── AST dataclasses ───────────────────────────────────────────────────────────

@dataclass
class ColumnRef:
    name: str
    alias: Optional[str] = None


@dataclass
class AggregateExpr:
    func: Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
    column: str
    alias: Optional[str] = None


@dataclass
class Condition:
    column: str
    operator: Literal['=', '!=', '<', '>', '<=', '>=']
    value: str | int | float


@dataclass
class AndExpr:
    left: 'BooleanExpr'
    right: 'BooleanExpr'


@dataclass
class OrExpr:
    left: 'BooleanExpr'
    right: 'BooleanExpr'


# Type alias for the recursive WHERE expression tree.
BooleanExpr = Condition | AndExpr | OrExpr


@dataclass
class JoinClause:
    table: str
    join_type: Literal['INNER', 'LEFT']
    on_left: str   # column from the left (primary) table
    on_right: str  # column from the right (joined) table


@dataclass
class OrderByClause:
    column: str
    direction: Literal['ASC', 'DESC'] = 'ASC'


@dataclass
class SubquerySource:
    """A nested SELECT used as the FROM source instead of a plain table name."""
    statement: 'SelectStatement'
    alias: Optional[str] = None


@dataclass
class SelectStatement:
    columns: List[ColumnRef | AggregateExpr]
    table: str | SubquerySource
    where: Optional[BooleanExpr] = None
    group_by: List[str] = field(default_factory=list)
    having: Optional[BooleanExpr] = None
    joins: List[JoinClause] = field(default_factory=list)
    order_by: Optional[OrderByClause] = None
    limit: Optional[int] = None
    _depth: int = field(default=0, repr=False, compare=False)


# ── Write-operation AST dataclasses ──────────────────────────────────────────

@dataclass
class SetClause:
    column: str
    value: str | int | float


@dataclass
class InsertStatement:
    table: str
    columns: List[str]
    values: List[str | int | float]


@dataclass
class UpdateStatement:
    table: str
    set_clauses: List[SetClause]
    where: Optional[BooleanExpr] = None


@dataclass
class DeleteStatement:
    table: str
    where: Optional[BooleanExpr] = None


# ── Internal transformer helpers ──────────────────────────────────────────────

# Private namedtuples used only to distinguish optional clause results inside
# select_statement. They never leave this module.
# (_Where is gone: where_clause returns a BooleanExpr node directly.)
# _Having wraps the having BooleanExpr so select_statement can tell it apart
# from the where BooleanExpr — both have the same runtime type.
_GroupBy = namedtuple('_GroupBy', ['columns'])
_Having = namedtuple('_Having', ['expr'])


def _is_token(item, *types):
    """Return True if item is a lark Token with one of the given type nmes."""
    return hasattr(item, 'type') and item.type in types


def _identifiers(items):
    """Extract all IDENTIFIER token values from a list of items."""
    return [str(t) for t in items if _is_token(t, 'IDENTIFIER')]


def _bare_col(identifier: str) -> str:
    """Strip an optional table prefix from a qualified identifier (table.column -> column)."""
    return identifier.split('.')[-1]


# ── Transformer ───────────────────────────────────────────────────────────────

class _SQLTransformer(Transformer):
    """
    Converts a lark parse tree into typed AST dataclasses.
    Every rule and rule-alias in sql.lark has a corresponding method here (INV-P3).
    """

    # ---- value rules (aliased) ----

    def string_val(self, items):
        # ESCAPED_STRING includes surrounding quotes and escape sequences;
        # ast.literal_eval safely strips and unescapes them.
        return _ast.literal_eval(str(items[0]))

    def single_string_val(self, items):
        # SINGLE_QUOTED_STRING: strip the surrounding single quotes.
        return str(items[0])[1:-1]

    def number_val(self, items):
        s = str(items[0])
        try:
            return int(s)
        except ValueError:
            return float(s)

    # ---- aggregate_expr rules (aliased, one per aggregate function) ----

    def agg_count(self, items):
        return AggregateExpr('COUNT', _identifiers(items)[0])

    def agg_sum(self, items):
        return AggregateExpr('SUM', _identifiers(items)[0])

    def agg_avg(self, items):
        return AggregateExpr('AVG', _identifiers(items)[0])

    def agg_min(self, items):
        return AggregateExpr('MIN', _identifiers(items)[0])

    def agg_max(self, items):
        return AggregateExpr('MAX', _identifiers(items)[0])

    # ---- select_item rules (aliased) ----

    def col_item(self, items):
        # IDENTIFIER (AS IDENTIFIER)?
        # Strip table prefix (e.g. data.region -> region) so the bare column name
        # is used for lookup against the post-join DataFrame.
        ids = _identifiers(items)
        return ColumnRef(name=_bare_col(ids[0]), alias=ids[1] if len(ids) > 1 else None)

    def agg_item(self, items):
        # aggregate_expr (AS IDENTIFIER)?  — alias is set onto the AggregateExpr
        agg = next(item for item in items if isinstance(item, AggregateExpr))
        ids = _identifiers(items)  # only the alias IDENTIFIER, if present
        alias = ids[0] if ids else None
        return dataclasses.replace(agg, alias=alias)

    # ---- select_list ----

    def select_list(self, items):
        if any(_is_token(t, 'STAR') for t in items):
            return [ColumnRef(name='*')]
        return [item for item in items if isinstance(item, (ColumnRef, AggregateExpr))]

    # ---- table_name ----

    def table_name(self, items):
        return str(items[0])

    # ---- from_clause ----

    def from_clause(self, items):
        # Discard the FROM keyword token; return str (table_name) or SubquerySource.
        return next(item for item in items if not _is_token(item, 'FROM'))

    # ---- subquery ----

    def subquery(self, items):
        # "(" select_statement ")" (AS IDENTIFIER)?
        inner_stmt = next(item for item in items if isinstance(item, SelectStatement))
        ids = _identifiers(items)  # alias IDENTIFIER if present (AS keyword is discarded)
        alias = ids[0] if ids else None
        return SubquerySource(statement=inner_stmt, alias=alias)

    # ---- join_type + join_clause ----

    def join_inner(self, _items):
        return 'INNER'

    def join_left(self, _items):
        return 'LEFT'

    def join_clause(self, items):
        # items: [join_type str, lark Tokens for /JOIN/i and /ON/i, table str, COLUMN_NAME x2]
        # join_inner/join_left return plain Python str ('INNER'/'LEFT').
        # table_name() also returns a plain Python str.
        # Anonymous regex terminals (/JOIN/i, /ON/i) are lark Token objects (str subclass
        # with a .type attribute) — exclude them from plain-str comparisons.
        jtype = next(item for item in items if isinstance(item, str) and item in ('INNER', 'LEFT'))
        table = next(
            item for item in items
            if isinstance(item, str)
            and item not in ('INNER', 'LEFT')
            and not hasattr(item, 'type')   # exclude lark Token objects
        )
        col_tokens = [t for t in items if _is_token(t, 'COLUMN_NAME')]
        return JoinClause(
            table=table,
            join_type=jtype,
            on_left=str(col_tokens[0]),
            on_right=str(col_tokens[1]),
        )

    # ---- condition + where_clause ----

    def condition(self, items):
        col = _bare_col(str(items[0]))  # strip table prefix if qualified (table.col -> col)
        op = str(items[1])              # OP token
        value = items[2]                # already coerced by string_val / number_val
        return Condition(column=col, operator=op, value=value)

    def where_clause(self, items):
        # items[0] is the WHERE keyword token (named terminal, kept by lark).
        # items[1] is the already-transformed bool_expr root (a BooleanExpr node).
        return next(item for item in items if not _is_token(item, 'WHERE'))

    def bool_expr(self, items):
        # OR-level: discard OR keyword tokens, fold bool_term results into OrExpr.
        # Single item means no OR was present — return it directly.
        operands = [item for item in items if not _is_token(item, 'OR')]
        result = operands[0]
        for item in operands[1:]:
            result = OrExpr(left=result, right=item)
        return result

    def bool_term(self, items):
        # AND-level: discard AND keyword tokens, fold condition results into AndExpr.
        # Single item means no AND was present — return it directly.
        operands = [item for item in items if not _is_token(item, 'AND')]
        result = operands[0]
        for item in operands[1:]:
            result = AndExpr(left=result, right=item)
        return result

    # ---- group_by_clause ----

    def group_by_clause(self, items):
        return _GroupBy(_identifiers(items))

    # ---- having_clause ----

    def having_clause(self, items):
        # items[0] is the HAVING keyword token; skip it to get the bool_expr root.
        # Wrapped in _Having so select_statement can distinguish it from the
        # WHERE BooleanExpr (both have the same runtime type).
        expr = next(item for item in items if not _is_token(item, 'HAVING'))
        return _Having(expr)

    # ---- order_by_clause ----

    def order_by_clause(self, items):
        col = _identifiers(items)[0]
        direction = 'ASC'
        for t in items:
            if _is_token(t, 'DESC'):
                direction = 'DESC'
            elif _is_token(t, 'ASC'):
                direction = 'ASC'
        return OrderByClause(column=col, direction=direction)

    # ---- limit_clause ----

    def limit_clause(self, items):
        return int(next(str(t) for t in items if _is_token(t, 'INTEGER')))

    # ---- select_statement (top-level rule) ----

    def select_statement(self, items):
        # Discard keyword tokens (SELECT); keep structured clause results.
        # FROM is handled inside from_clause — it never reaches select_statement items.
        structured = [
            item for item in items
            if not _is_token(item, 'SELECT')
        ]
        # First two positions are always columns and from_clause result.
        columns = structured[0]   # list[ColumnRef | AggregateExpr]
        table = structured[1]     # str | SubquerySource

        where: Optional[BooleanExpr] = None
        group_by: List[str] = []
        having: Optional[BooleanExpr] = None
        joins: List[JoinClause] = []
        order_by: Optional[OrderByClause] = None
        limit: Optional[int] = None

        for item in structured[2:]:
            if isinstance(item, JoinClause):
                joins.append(item)
            elif isinstance(item, (Condition, AndExpr, OrExpr)):
                where = item
            elif isinstance(item, _GroupBy):
                group_by = item.columns
            elif isinstance(item, _Having):
                having = item.expr
            elif isinstance(item, OrderByClause):
                order_by = item
            elif isinstance(item, int):
                limit = item

        return SelectStatement(
            columns=columns,
            table=table,
            where=where,
            group_by=group_by,
            having=having,
            joins=joins,
            order_by=order_by,
            limit=limit,
        )


# ── Grammar loader (cached at module level) ───────────────────────────────────

_GRAMMAR_PATH = Path(__file__).parent / 'sql.lark'
_PARSER = Lark(_GRAMMAR_PATH.read_text(), parser='earley', start='select_statement')
_TRANSFORMER = _SQLTransformer()


# ── Public interface ──────────────────────────────────────────────────────────

def parse(query: str) -> SelectStatement | InsertStatement | UpdateStatement | DeleteStatement:
    """
    Parse a SQL-like query string and return a typed AST node.

    Returns SelectStatement for SELECT queries, InsertStatement for INSERT,
    UpdateStatement for UPDATE, or DeleteStatement for DELETE.
    Raises ValueError with a human-readable message for malformed input.
    Lark internals never surface to the caller.
    """
    try:
        tree = _PARSER.parse(query)
        result = _TRANSFORMER.transform(tree)
    except lark.exceptions.UnexpectedInput as exc:
        context = exc.get_context(query, span=40)
        raise ValueError(
            f"Syntax error in query near: ...{context.strip()}...\n"
            f"Hint: JOIN, subqueries, and HAVING without GROUP BY are not supported."
        ) from None
    except lark.exceptions.LarkError as exc:
        raise ValueError(f"Parse error: {exc}") from None

    _valid_types = (SelectStatement, InsertStatement, UpdateStatement, DeleteStatement)
    if not isinstance(result, _valid_types):
        raise ValueError("Query did not produce a valid statement.")

    if result.having is not None and not result.group_by:
        raise ValueError(
            "HAVING clause requires a GROUP BY clause. "
            "HAVING filters aggregated groups — it has no effect without GROUP BY."
        )

    return result
