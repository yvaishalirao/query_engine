"""Execution pipeline: CSV loading, filtering, aggregation, sorting, limiting, and projection."""

import os

import pandas as pd


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
