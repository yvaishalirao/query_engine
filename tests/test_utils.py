"""Direct tests for engine/utils.py: format_result and format_error."""

import pandas as pd
import pytest
from engine.utils import format_error, format_result


def test_format_result_nonempty():
    df = pd.DataFrame({'region': ['North', 'South'], 'sales': [4200, 950]})
    out = format_result(df)
    assert 'region' in out and 'sales' in out
    assert 'North' in out and 'South' in out
    assert '0' not in out.split('\n')[0]   # no pandas index column in header


def test_format_result_empty_shows_headers():
    df = pd.DataFrame(columns=['region', 'sales'])
    out = format_result(df)
    assert out != '', "format_result returned empty string for empty DataFrame (INV-O2)"
    assert 'region' in out
    assert 'sales' in out


def test_format_error_prefix():
    msg = format_error('Column not found: ghost')
    assert msg == 'Error: Column not found: ghost'
    assert msg.startswith('Error:')


def test_format_error_no_internals():
    # format_error must not add anything beyond the prefix
    raw = 'some plain message'
    assert format_error(raw) == f'Error: {raw}'
