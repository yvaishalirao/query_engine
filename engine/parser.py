"""
SQL parser module: AST dataclasses, lark Transformer, and public parse() interface.

Dataclass roles:
  ColumnRef       — a plain column reference in SELECT, with an optional alias.
  AggregateExpr   — an aggregate function call (COUNT/SUM/AVG/MIN/MAX) on a column,
                    with an optional alias.
  Condition       — a single WHERE predicate: column, comparison operator, and value.
                    Only AND-chained conditions are supported; OR is rejected at parse time.
  OrderByClause   — an ORDER BY target column with direction (ASC or DESC).
  SelectStatement — the complete, typed AST for a SELECT query; the only object
                    accepted by the executor.

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
class OrderByClause:
    column: str
    direction: Literal['ASC', 'DESC'] = 'ASC'


@dataclass
class SelectStatement:
    columns: List[ColumnRef | AggregateExpr]
    table: str
    where: List[Condition] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: Optional[OrderByClause] = None
    limit: Optional[int] = None


# ── Internal transformer helpers ──────────────────────────────────────────────

# Private namedtuples used only to distinguish optional clause results inside
# select_statement. They never leave this module.
_Where = namedtuple('_Where', ['conditions'])
_GroupBy = namedtuple('_GroupBy', ['columns'])


def _is_token(item, *types):
    """Return True if item is a lark Token with one of the given type nmes."""
    return hasattr(item, 'type') and item.type in types


def _identifiers(items):
    """Extract all IDENTIFIER token values from a list of items."""
    return [str(t) for t in items if _is_token(t, 'IDENTIFIER')]


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
        ids = _identifiers(items)
        return ColumnRef(name=ids[0], alias=ids[1] if len(ids) > 1 else None)

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

    # ---- condition + where_clause ----

    def condition(self, items):
        col = str(items[0])   # IDENTIFIER token
        op = str(items[1])    # OP token
        value = items[2]      # already coerced by string_val / number_val
        return Condition(column=col, operator=op, value=value)

    def where_clause(self, items):
        return _Where([item for item in items if isinstance(item, Condition)])

    # ---- group_by_clause ----

    def group_by_clause(self, items):
        return _GroupBy(_identifiers(items))

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
        # Discard keyword tokens (SELECT, FROM); keep structured clause results.
        structured = [
            item for item in items
            if not _is_token(item, 'SELECT', 'FROM')
        ]
        # First two positions are always columns and table name.
        columns = structured[0]   # list[ColumnRef | AggregateExpr]
        table = structured[1]     # str

        where: List[Condition] = []
        group_by: List[str] = []
        order_by: Optional[OrderByClause] = None
        limit: Optional[int] = None

        for item in structured[2:]:
            if isinstance(item, _Where):
                where = item.conditions
            elif isinstance(item, _GroupBy):
                group_by = item.columns
            elif isinstance(item, OrderByClause):
                order_by = item
            elif isinstance(item, int):
                limit = item

        return SelectStatement(
            columns=columns,
            table=table,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
        )


# ── Grammar loader (cached at module level) ───────────────────────────────────

_GRAMMAR_PATH = Path(__file__).parent / 'sql.lark'
_PARSER = Lark(_GRAMMAR_PATH.read_text(), parser='earley', start='select_statement')
_TRANSFORMER = _SQLTransformer()


# ── Public interface ──────────────────────────────────────────────────────────

def parse(query: str) -> SelectStatement:
    """
    Parse a SQL-like query string and return a SelectStatement AST.

    Raises ValueError with a human-readable message if the query uses unsupported
    syntax (OR, subqueries, HAVING) or is otherwise malformed. Lark internals
    never surface to the caller.
    """
    try:
        tree = _PARSER.parse(query)
        result = _TRANSFORMER.transform(tree)
    except lark.exceptions.UnexpectedInput as exc:
        context = exc.get_context(query, span=40)
        raise ValueError(
            f"Syntax error in query near: ...{context.strip()}...\n"
            f"Hint: OR, JOIN, subqueries, and HAVING are not supported."
        ) from None
    except lark.exceptions.LarkError as exc:
        raise ValueError(f"Parse error: {exc}") from None

    if not isinstance(result, SelectStatement):
        raise ValueError("Query did not produce a valid SELECT statement.")

    return result
