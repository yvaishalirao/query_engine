"""Output formatting utilities: result table and error message formatting."""

import pandas as pd
from tabulate import tabulate


def format_result(df: pd.DataFrame) -> str:
    """
    Format a DataFrame as a plain-text table using tabulate.

    An empty DataFrame returns a header-only table — never an empty string (INV-O2).
    The output contains exactly the columns in df: no index, no pandas metadata (INV-O3).
    The caller is responsible for printing; this function only returns the string.
    """
    return tabulate(df, headers='keys', tablefmt='simple', showindex=False)


def format_error(message: str) -> str:
    """
    Wrap a plain-language error message with the 'Error: ' prefix (INV-O1).

    Never include exception types, tracebacks, or pandas internals — those must
    be stripped by the caller before passing *message* here.
    """
    return f"Error: {message}"
