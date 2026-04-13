"""
Write executor: CSV mutation operations for INSERT, UPDATE, and DELETE.

Completely separate from executor.py — no write logic lives there.
All three public functions follow the same safety contract:
  1. Validate the statement before touching the file.
  2. Backup the CSV, write the new content.
  3. On any write failure, restore the backup and re-raise as ValueError.

Imports _eval_bool_expr and load_csv from executor.py so that WHERE
evaluation and CSV loading use the exact same code path as SELECT queries.
"""

import os
import shutil

import pandas as pd

from engine.executor import _eval_bool_expr, load_csv
from engine.parser import (
    DeleteStatement, InsertStatement, SetClause, UpdateStatement,
)


# ── Backup helpers ────────────────────────────────────────────────────────────

def _backup_csv(path: str) -> str:
    """
    Copy *path* to path + '.bak' and return the backup path.

    Raises ValueError if the source file does not exist.
    """
    if not os.path.exists(path):
        raise ValueError(f"CSV file not found: {path}")
    backup_path = path + '.bak'
    shutil.copy2(path, backup_path)
    return backup_path


def _restore_backup(path: str, backup_path: str) -> None:
    """Copy *backup_path* back over *path*, restoring the original content."""
    shutil.copy2(backup_path, path)


# ── INSERT ────────────────────────────────────────────────────────────────────

def execute_insert(stmt: InsertStatement, csv_path: str) -> str:
    """
    Append one new row to the CSV described by *stmt*.

    Validates:
      - len(columns) == len(values); raises ValueError on mismatch.
      - Every column in stmt.columns exists in the CSV; raises ValueError if not.

    Returns "1 row inserted." on success.
    On write failure the original file is restored from backup.
    """
    if len(stmt.columns) != len(stmt.values):
        raise ValueError(
            f"INSERT column/value count mismatch: "
            f"{len(stmt.columns)} column(s) but {len(stmt.values)} value(s)."
        )

    df = load_csv(csv_path)

    for col in stmt.columns:
        if col not in df.columns:
            raise ValueError(f"Column not found: {col}")

    new_row = {col: val for col, val in zip(stmt.columns, stmt.values)}
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    backup = None
    try:
        backup = _backup_csv(csv_path)
        df.to_csv(csv_path, index=False)
    except Exception as e:
        if backup is not None:
            _restore_backup(csv_path, backup)
        raise ValueError(f"Write failed, original file restored: {e}")

    return "1 row inserted."


# ── UPDATE ────────────────────────────────────────────────────────────────────

def execute_update(stmt: UpdateStatement, csv_path: str) -> str:
    """
    Update rows in the CSV that match stmt.where, applying each SetClause.

    Validates that every target column in set_clauses exists in the CSV.
    If stmt.where is None all rows are updated.
    Returns f"{n} row(s) updated." on success.
    On write failure the original file is restored from backup.
    """
    df = load_csv(csv_path)

    for sc in stmt.set_clauses:
        if sc.column not in df.columns:
            raise ValueError(f"Column not found: {sc.column}")

    if stmt.where is not None:
        mask = _eval_bool_expr(df, stmt.where)
    else:
        mask = pd.Series([True] * len(df), index=df.index)

    n = int(mask.sum())

    for sc in stmt.set_clauses:
        df.loc[mask, sc.column] = sc.value

    backup = None
    try:
        backup = _backup_csv(csv_path)
        df.to_csv(csv_path, index=False)
    except Exception as e:
        if backup is not None:
            _restore_backup(csv_path, backup)
        raise ValueError(f"Write failed, original file restored: {e}")

    return f"{n} row(s) updated."


# ── DELETE ────────────────────────────────────────────────────────────────────

def execute_delete(stmt: DeleteStatement, csv_path: str) -> str:
    """
    Delete rows from the CSV that match stmt.where.

    Raises ValueError if stmt.where is None — unguarded DELETE is not allowed.
    Returns f"{n} row(s) deleted." on success.
    On write failure the original file is restored from backup.
    """
    if stmt.where is None:
        raise ValueError("DELETE without WHERE is not allowed.")

    df = load_csv(csv_path)
    mask = _eval_bool_expr(df, stmt.where)
    n = int(mask.sum())

    df = df[~mask].reset_index(drop=True)

    backup = None
    try:
        backup = _backup_csv(csv_path)
        df.to_csv(csv_path, index=False)
    except Exception as e:
        if backup is not None:
            _restore_backup(csv_path, backup)
        raise ValueError(f"Write failed, original file restored: {e}")

    return f"{n} row(s) deleted."
