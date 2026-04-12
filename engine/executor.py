"""Execution pipeline: CSV loading, filtering, aggregation, sorting, limiting, and projection."""

import operator
import os

import pandas as pd
from engine.parser import AggregateExpr, ColumnRef, Condition, OrderByClause, SelectStatement


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV file and return its contents as a DataFrame.

    Raises ValueError if the file does not exist. The check happens before
    pd.read_csv so the error is explicit and human-readable (INV-S3).
    The returned DataFrame is a fresh object; no module-level state is kept (INV-S2).
    """
    if not os.path.exists(path):
        raise ValueError(f"CSV file not found: {path}")
    return pd.read_csv(path)


def validate_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Verify that every column name in *columns* exists in *df*.

    '*' is silently ignored (wildcard SELECT).
    Raises ValueError naming the first missing column (INV-E3).
    """
    for col in columns:
        if col == '*':
            continue
        if col not in df.columns:
            raise ValueError(f"Column not found: {col}")


# Maps operator strings to their corresponding operator functions.
_OPS = {
    '=':  operator.eq,
    '!=': operator.ne,
    '<':  operator.lt,
    '>':  operator.gt,
    '<=': operator.le,
    '>=': operator.ge,
}


def apply_filters(df: pd.DataFrame, conditions: list[Condition]) -> pd.DataFrame:
    """
    Apply WHERE conditions to *df* and return the filtered result.

    Each condition is evaluated as an explicit boolean mask (INV-E5: no df.query/eval).
    Masks are AND-chained. The source DataFrame is never mutated (INV-E1).
    Raises ValueError if a condition references a column not present in *df* (INV-E3).
    """
    if not conditions:
        return df

    combined = None
    for cond in conditions:
        if cond.column not in df.columns:
            raise ValueError(f"Column not found: {cond.column}")
        mask = _OPS[cond.operator](df[cond.column], cond.value)
        combined = mask if combined is None else (combined & mask)

    return df[combined]


# Aggregates that require a numeric column (INV-E4).
_NUMERIC_AGGS = {'SUM', 'AVG', 'MIN', 'MAX'}


def apply_aggregation(
    df: pd.DataFrame,
    group_by_cols: list[str],
    agg_exprs: list[AggregateExpr],
) -> pd.DataFrame:
    """
    Apply GROUP BY + aggregation to *df*.

    - No group_by_cols + agg_exprs  -> scalar aggregation over entire df, one row result.
    - group_by_cols non-empty       -> df.groupby(...) then aggregate.
    - SUM/AVG/MIN/MAX on non-numeric column raises ValueError before pandas runs (INV-E4).
    - COUNT works on any column dtype.
    - Output column name is alias if provided, else f"{func}({column})".
    - Returned DataFrame has no pandas index columns (INV-O3).
    - Never uses df.query() or df.eval() (INV-E5).
    """
    if not agg_exprs:
        return df

    # Validate numeric requirement before touching pandas (INV-E4).
    for expr in agg_exprs:
        if expr.func in _NUMERIC_AGGS:
            if expr.column not in df.columns:
                raise ValueError(f"Column not found: {expr.column}")
            if not pd.api.types.is_numeric_dtype(df[expr.column]):
                raise ValueError(
                    f"Aggregation {expr.func} requires numeric column: {expr.column}"
                )

    def _output_name(expr: AggregateExpr) -> str:
        return expr.alias if expr.alias else f"{expr.func}({expr.column})"

    def _aggregate_series(series: pd.Series, func: str) -> pd.Series:
        if func == 'COUNT':
            return series.count()
        if func == 'SUM':
            return series.sum()
        if func == 'AVG':
            return series.mean()
        if func == 'MIN':
            return series.min()
        if func == 'MAX':
            return series.max()
        raise ValueError(f"Unsupported aggregate function: {func}")

    if not group_by_cols:
        # Scalar aggregation: produce a single-row DataFrame.
        row = {}
        for expr in agg_exprs:
            if expr.column not in df.columns:
                raise ValueError(f"Column not found: {expr.column}")
            row[_output_name(expr)] = _aggregate_series(df[expr.column], expr.func)
        return pd.DataFrame([row])

    # Grouped aggregation.
    grouped = df.groupby(group_by_cols)
    agg_map = {}
    for expr in agg_exprs:
        if expr.column not in df.columns:
            raise ValueError(f"Column not found: {expr.column}")
        agg_map[expr.column] = agg_map.get(expr.column, [])
        agg_map[expr.column].append(expr.func)

    # Build per-column aggregation mapping for pandas.
    pandas_agg = {}
    for expr in agg_exprs:
        func_name = {
            'COUNT': 'count', 'SUM': 'sum', 'AVG': 'mean',
            'MIN': 'min', 'MAX': 'max',
        }[expr.func]
        pandas_agg[expr.column] = pandas_agg.get(expr.column, {})
        pandas_agg[expr.column][_output_name(expr)] = func_name

    # pandas named aggregation syntax: col=(source_col, func)
    named_agg = {}
    for expr in agg_exprs:
        func_name = {
            'COUNT': 'count', 'SUM': 'sum', 'AVG': 'mean',
            'MIN': 'min', 'MAX': 'max',
        }[expr.func]
        named_agg[_output_name(expr)] = pd.NamedAgg(column=expr.column, aggfunc=func_name)

    result = grouped.agg(**named_agg).reset_index()
    return result


def apply_sort(df: pd.DataFrame, order_by: OrderByClause | None) -> pd.DataFrame:
    """
    Sort *df* by the column and direction specified in *order_by*.

    Returns df unchanged if order_by is None.
    Uses df.sort_values() explicitly — no delegation (INV-E5).
    The source DataFrame is never mutated (INV-E1).
    Raises ValueError if the sort column does not exist (INV-E3).
    """
    if order_by is None:
        return df

    if order_by.column not in df.columns:
        raise ValueError(f"Column not found: {order_by.column}")

    ascending = order_by.direction == 'ASC'
    return df.sort_values(by=order_by.column, ascending=ascending)


def apply_limit(df: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    """
    Return the first *limit* rows of *df*.

    Returns df unchanged if limit is None.
    """
    if limit is None:
        return df
    return df.head(limit)


def _project(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Project the DataFrame down to the columns named in the SELECT list (INV-O3).

    SELECT * -> all columns unchanged.
    Otherwise: select each column in declaration order, applying any aliases.
    """
    if len(columns) == 1 and isinstance(columns[0], ColumnRef) and columns[0].name == '*':
        return df

    select_names = []
    rename_map = {}

    for col in columns:
        if isinstance(col, ColumnRef):
            select_names.append(col.name)
            if col.alias:
                rename_map[col.name] = col.alias
        elif isinstance(col, AggregateExpr):
            # apply_aggregation already named the column alias or f"{func}({column})"
            out_name = col.alias if col.alias else f"{col.func}({col.column})"
            select_names.append(out_name)

    result = df[select_names]
    if rename_map:
        result = result.rename(columns=rename_map)
    return result


def execute(stmt: SelectStatement, csv_path: str) -> pd.DataFrame:
    """
    Execute a SelectStatement against a CSV file and return the result as a DataFrame.

    This is the ONLY public execution entry point (INV-P1).
    Pipeline order is fixed and structural: load -> validate -> filter ->
    aggregate -> sort -> limit -> project (INV-E2).
    Never uses df.query() or df.eval() (INV-E5).
    """
    # INV-P1: only a fully-typed SelectStatement may enter the executor.
    assert isinstance(stmt, SelectStatement), (
        f"execute() requires a SelectStatement, got {type(stmt).__name__}"
    )

    # 1. Load CSV (file existence check inside load_csv — INV-S3).
    df = load_csv(csv_path)

    # 2. Validate columns referenced by plain ColumnRefs, WHERE, GROUP BY, ORDER BY.
    #    Aggregate source columns are validated inside apply_aggregation (INV-E4).
    cols_to_check = []
    for col in stmt.columns:
        if isinstance(col, ColumnRef) and col.name != '*':
            cols_to_check.append(col.name)
    cols_to_check += [c.column for c in stmt.where]
    cols_to_check += stmt.group_by
    # ORDER BY column is validated after aggregation (it may be an aggregate alias
    # that does not exist in the CSV — apply_sort raises ValueError if absent then).
    validate_columns(df, cols_to_check)

    # 3. Filter (INV-E2: before aggregation).
    result = apply_filters(df, stmt.where)

    # 4. Aggregate.
    agg_exprs = [col for col in stmt.columns if isinstance(col, AggregateExpr)]
    result = apply_aggregation(result, stmt.group_by, agg_exprs)

    # 5. Sort (INV-E2: after aggregation, before limit).
    result = apply_sort(result, stmt.order_by)

    # 6. Limit (INV-E2: last, so ORDER BY determines which rows are kept).
    result = apply_limit(result, stmt.limit)

    # 7. Project SELECT columns (INV-O3: only named columns, no index or extras).
    result = _project(result, stmt.columns)

    return result
