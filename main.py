"""Entry point for the query engine CLI."""

import argparse
import os
import sys

from engine import write_executor
from engine.executor import execute
from engine.parser import (
    DeleteStatement, InsertStatement, SelectStatement, UpdateStatement, parse,
)
from engine.utils import format_error, format_result


def _run_query(query: str, csv_path: str) -> bool:
    """
    Parse and execute one query, printing the result or a formatted error.

    Returns True if execution succeeded, False on any error.
    The entire block is wrapped so no raw exception can reach stdout (INV-O1).
    """
    try:
        stmt = parse(query)
        if isinstance(stmt, SelectStatement):
            result = execute(stmt, csv_path)
            print(format_result(result))
        elif isinstance(stmt, InsertStatement):
            print(write_executor.execute_insert(stmt, csv_path))
        elif isinstance(stmt, UpdateStatement):
            print(write_executor.execute_update(stmt, csv_path))
        elif isinstance(stmt, DeleteStatement):
            print(write_executor.execute_delete(stmt, csv_path))
        return True
    except ValueError as exc:
        print(format_error(str(exc)))
        return False
    except Exception as exc:
        # Catch-all: internal errors must never surface as tracebacks (INV-O1).
        print(format_error(f"Unexpected error: {exc}"))
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "SQL-like query engine for CSV files. "
            "Supports SELECT (with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOIN, subqueries), "
            "INSERT INTO, UPDATE SET, and DELETE FROM."
        )
    )
    parser.add_argument(
        '--csv',
        default='data/sample.csv',
        metavar='PATH',
        help="Path to the CSV file (default: data/sample.csv)",
    )
    parser.add_argument(
        'query',
        nargs='?',
        default=None,
        help=(
            "SQL statement to execute. Omit to enter interactive mode. "
            "Write operations (INSERT/UPDATE/DELETE) modify the CSV in place "
            "and create a .bak backup before writing."
        ),
    )
    args = parser.parse_args()
    csv_path = args.csv

    # Validate CSV existence once before any query runs (INV-S3).
    if not os.path.exists(csv_path):
        print(format_error(f"CSV file not found: {csv_path}"))
        sys.exit(1)

    if args.query is not None:
        # Single-query mode.
        ok = _run_query(args.query, csv_path)
        sys.exit(0 if ok else 1)
    else:
        # Interactive loop.
        while True:
            try:
                raw = input("Enter query (or 'quit' to exit): ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if raw.lower() in ('quit', 'exit', 'q'):
                break
            if not raw:
                continue
            _run_query(raw, csv_path)


if __name__ == '__main__':
    main()
