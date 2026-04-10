"""
SQL parser module: AST dataclasses and parse interface.

Dataclass roles:
  ColumnRef       — a plain column reference in SELECT, with an optional alias.
  AggregateExpr   — an aggregate function call (COUNT/SUM/AVG/MIN/MAX) on a column,
                    with an optional alias.
  Condition       — a single WHERE predicate: column, comparison operator, and value.
                    Only AND-chained conditions are supported; OR is rejected at parse time.
  OrderByClause   — an ORDER BY target column with direction (ASC or DESC).
  SelectStatement — the complete, typed AST for a SELECT query; the only object
                    accepted by the executor.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Literal


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
