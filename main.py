"""Entry point for the query engine CLI."""

import argparse
import os
import sys

from engine.executor import execute
from engine.parser import parse
from engine.utils import format_error, format_result


def _run_query(query: str, csv_path: str) -> bool:
    """
    Parse and execute one query, printing the result or a formatted error.

    Returns True if execution succeeded, False on any error.
    The entire block is wrapped so no raw exception can reach stdout (INV-O1).
    """
    try:
        stmt = parse(query)
        result = execute(stmt, csv_path)
        print(format_result(result))
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
        description="SQL-like query engine for CSV files."
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
        help="SQL query string. Omit to enter interactive mode.",
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
