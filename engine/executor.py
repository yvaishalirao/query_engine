"""Execution pipeline: CSV loading, filtering, aggregation, sorting, limiting, and projection."""

import operator
import os

import pandas as pd
from engine.parser import Condition


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
