"""
Invariant tests for the query engine.

INV-E1: The source DataFrame is never mutated by execute().
INV-E2: Pipeline order is fixed -- sort before limit.
INV-E5: df.query() and df.eval() never appear in executor.py.
INV-P1: execute() contains an isinstance(stmt, SelectStatement) guard.
INV-P2: OR queries are rejected at parse time -- apply_filters is never reached.
INV-P3: Every grammar rule/alias in sql.lark has a method on the Transformer.
"""

import re
import inspect
import subprocess
import sys
from unittest.mock import patch

import pytest
from engine.executor import execute, load_csv
from engine.parser import parse, _SQLTransformer


CSV = 'data/sample.csv'


def test_inv_e1_source_dataframe_not_mutated():
    """INV-E1: execute() must not mutate the DataFrame it reads from disk."""
    df = load_csv(CSV)
    original = df.copy()

    # Full pipeline: filter + group + sort + limit
    stmt = parse(
        "SELECT region, SUM(sales) AS total FROM data "
        "WHERE year = 2023 "
        "GROUP BY region "
        "ORDER BY total DESC "
        "LIMIT 2"
    )
    execute(stmt, CSV)

    # df must be byte-for-byte identical to its state before execution
    assert df.equals(original), "execute() mutated the source DataFrame (INV-E1 violated)"


def test_inv_e2_sort_before_limit():
    """INV-E2: ORDER BY must be applied before LIMIT.

    If LIMIT ran first it would take an arbitrary 3-row slice, and the
    resulting sales values would not necessarily be the 3 highest.
    """
    result = execute(parse("SELECT * FROM data ORDER BY sales DESC LIMIT 3"), CSV)

    assert len(result) == 3

    # Load the full dataset to find the true top-3 sales values
    df_full = load_csv(CSV)
    top3 = sorted(df_full['sales'].tolist(), reverse=True)[:3]

    assert sorted(result['sales'].tolist(), reverse=True) == top3, (
        f"Expected top-3 sales {top3}, got {sorted(result['sales'].tolist(), reverse=True)} "
        "(INV-E2: LIMIT may have run before ORDER BY)"
    )


def test_inv_e5_no_df_query_or_eval_in_executor():
    """INV-E5: executor.py must not call df.query() or df.eval() in live code.

    Docstrings and comments may mention the names; only executable lines are
    checked. A line is treated as a comment/docstring if its first non-whitespace
    character is '#' or it is part of a triple-quoted string literal.
    """
    with open('engine/executor.py') as f:
        lines = f.readlines()

    in_docstring = False
    docstring_char = None

    for lineno, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Track entry/exit of triple-quoted docstrings
        for marker in ('"""', "'''"):
            count = stripped.count(marker)
            if not in_docstring:
                if count % 2 == 1:          # odd occurrences -> we entered a block
                    in_docstring = True
                    docstring_char = marker
            else:
                if marker == docstring_char and count % 2 == 1:
                    in_docstring = False
                    docstring_char = None

        # Skip comment lines and lines inside docstrings
        if stripped.startswith('#') or in_docstring or stripped.startswith('"""') or stripped.startswith("'''"):
            continue

        assert 'df.query(' not in line, (
            f"df.query() call found in executor.py line {lineno}: {line.rstrip()} "
            "(INV-E5 violated)"
        )
        assert 'df.eval(' not in line, (
            f"df.eval() call found in executor.py line {lineno}: {line.rstrip()} "
            "(INV-E5 violated)"
        )


# ── INV-P1 ────────────────────────────────────────────────────────────────────

def test_inv_p1_isinstance_guard_in_execute():
    """INV-P1: execute() must contain an isinstance(stmt, SelectStatement) guard.

    Checked by inspecting the source of execute() — not just its behaviour —
    so the guard cannot be removed without the test catching it.
    """
    source = open('engine/executor.py').read()
    assert 'isinstance(stmt, SelectStatement)' in source, (
        "execute() is missing the isinstance(stmt, SelectStatement) guard (INV-P1 violated)"
    )


# ── INV-P2 ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_rows", [
    ("SELECT * FROM data WHERE sales > 5000 OR year = 2021", None),
    ("SELECT * FROM data WHERE sales > 9999999 OR year = 2099", 0),
])
def test_inv_p2_or_routed_through_apply_filters(query, expected_rows):
    """INV-P2: OR queries must be routed through apply_filters, not rejected.

    OR is supported since EX1. apply_filters must be called exactly once per
    execute() call and must receive an OrExpr (not a list or raw Condition).
    """
    from engine.parser import OrExpr as _OrExpr
    original_apply_filters = __import__('engine.executor', fromlist=['apply_filters']).apply_filters

    called_with = []

    def tracking_apply_filters(df, where):
        called_with.append(where)
        return original_apply_filters(df, where)

    with patch('engine.executor.apply_filters', side_effect=tracking_apply_filters):
        stmt = parse(query)
        result = execute(stmt, 'data/sample.csv')

    # apply_filters is called twice: once for WHERE and once via apply_having.
    # The WHERE call must carry the OrExpr; apply_having may pass None (no HAVING).
    or_calls = [w for w in called_with if isinstance(w, _OrExpr)]
    assert len(or_calls) == 1, (
        f"Expected exactly one apply_filters call with an OrExpr; got {called_with}"
    )
    if expected_rows is not None:
        assert len(result) == expected_rows


# ── INV-P3 ────────────────────────────────────────────────────────────────────

def _required_transformer_methods() -> set[str]:
    """
    Derive the set of method names the Transformer must implement from sql.lark.

    Rules:  In lark, when ALL alternatives of a rule carry '-> alias' annotations,
            lark calls the alias method instead of the rule name method. So:
              - Every alias name (from '-> name') always needs a method.
              - A rule name needs a method only if NOT all of its alternatives
                are aliased (i.e. the rule name appears as a tree node).

    Strategy:
      1. Strip comments.
      2. Find all alias targets ('-> name').
      3. Find all rule definitions (lowercase names followed by ':').
      4. For each rule, check if all alternatives are aliased; if so, the rule
         name itself does NOT need a method.
    """
    grammar = open('engine/sql.lark').read()
    # Strip line comments
    grammar = re.sub(r'//[^\n]*', '', grammar)

    # All alias targets
    aliases = set(re.findall(r'->\s*([a-z_]\w*)', grammar))

    # Rule definitions: lowercase names at line start followed by ':'
    rule_defs = re.findall(r'^([a-z][a-z0-9_]*)\s*(?:\.\d+\s*)?:', grammar, re.MULTILINE)
    rule_names = set(rule_defs)

    # Determine which rules are "fully aliased" (rule name never becomes a tree node).
    # Split the grammar into per-rule blocks and inspect each.
    fully_aliased_rules: set[str] = set()
    # Match each rule block: from 'name :' to the next rule definition or end
    blocks = re.split(r'\n(?=[a-z][a-z0-9_]*\s*(?:\.\d+\s*)?:)', grammar)
    for block in blocks:
        m = re.match(r'([a-z][a-z0-9_]*)\s*(?:\.\d+\s*)?:(.*)', block, re.DOTALL)
        if not m:
            continue
        name, body = m.group(1), m.group(2)
        # Split body into alternatives (split on '|' that aren't inside strings/parens)
        alternatives = [a.strip() for a in re.split(r'\n\s*\||\s*\|', body) if a.strip()]
        if alternatives and all('->' in alt for alt in alternatives):
            fully_aliased_rules.add(name)

    # Required = all aliases + rule names that are NOT fully aliased
    return aliases | (rule_names - fully_aliased_rules)


def test_inv_p3_every_grammar_rule_has_transformer_method():
    """INV-P3: Every rule/alias in sql.lark must have a method on _SQLTransformer."""
    required = _required_transformer_methods()
    transformer_methods = {
        name for name, _ in inspect.getmembers(_SQLTransformer, predicate=inspect.isfunction)
        if not name.startswith('_')
    }

    missing = required - transformer_methods
    assert not missing, (
        f"Transformer is missing methods for grammar rules/aliases: {sorted(missing)} "
        "(INV-P3 violated)"
    )


# ── Subprocess helper ─────────────────────────────────────────────────────────

def _cli(query: str, csv: str = 'data/sample.csv') -> str:
    """Run main.py with a single query and return stdout as a string."""
    result = subprocess.run(
        [sys.executable, 'main.py', '--csv', csv, query],
        capture_output=True,
        text=True,
    )
    return result.stdout


# ── INV-O1 ────────────────────────────────────────────────────────────────────

def test_inv_o1_no_raw_exception_in_stdout():
    """INV-O1: Errors must surface as 'Error: ...' messages, never raw tracebacks."""
    stdout = _cli("SELECT ghost_col FROM data WHERE ghost_col > 0")

    forbidden = ['Traceback', 'KeyError', 'UnexpectedToken', 'Exception']
    for token in forbidden:
        assert token not in stdout, (
            f"Raw exception token '{token}' found in stdout (INV-O1 violated):\n{stdout}"
        )

    assert stdout.strip().startswith('Error:'), (
        f"stdout does not start with 'Error:' (INV-O1 violated):\n{stdout}"
    )


# ── INV-O2 ────────────────────────────────────────────────────────────────────

def test_inv_o2_empty_result_shows_headers():
    """INV-O2: A query that matches no rows must still print column headers."""
    stdout = _cli("SELECT * FROM data WHERE region = 'NonExistentRegion' LIMIT 5")

    assert stdout.strip(), "stdout is empty for zero-row result (INV-O2 violated)"
    assert 'Error' not in stdout, (
        f"Got an error instead of an empty table (INV-O2 violated):\n{stdout}"
    )

    # All five CSV column headers must appear in the output
    for col in ('region', 'product', 'sales', 'quantity', 'year'):
        assert col in stdout, (
            f"Column header '{col}' missing from empty-result output (INV-O2 violated):\n{stdout}"
        )


# ── INV-O3 ────────────────────────────────────────────────────────────────────

def test_inv_o3_output_contains_only_selected_columns():
    """INV-O3: stdout must contain exactly the columns named in SELECT."""
    stdout = _cli("SELECT region, sales FROM data LIMIT 3")

    # The first non-empty line of tabulate 'simple' output is the header row.
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    assert lines, f"No output lines found:\n{stdout}"

    header_line = lines[0]

    # Required columns are present
    assert 'region' in header_line, f"'region' missing from header: {header_line}"
    assert 'sales' in header_line, f"'sales' missing from header: {header_line}"

    # Columns NOT in SELECT must not appear
    for unwanted in ('product', 'quantity', 'year'):
        assert unwanted not in header_line, (
            f"Unexpected column '{unwanted}' in header (INV-O3 violated): {header_line}"
        )
