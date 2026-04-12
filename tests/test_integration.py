"""End-to-end integration tests: parse() -> execute() against data/sample.csv."""

import pytest
from engine.parser import parse
from engine.executor import execute

CSV = 'data/sample.csv'


def run(query: str):
    return execute(parse(query), CSV)



def test_select_star_all_rows_and_columns():
    result = run("SELECT * FROM data")
    assert list(result.columns) == ['region', 'product', 'sales', 'quantity', 'year']
    assert len(result) == 24


def test_select_star_limit():
    result = run("SELECT * FROM data LIMIT 5")
    assert len(result) == 5
    assert list(result.columns) == ['region', 'product', 'sales', 'quantity', 'year']



def test_where_single_string_condition():
    result = run("SELECT * FROM data WHERE region = 'North'")
    assert len(result) > 0
    assert all(result['region'] == 'North')
    assert set(result.columns) == {'region', 'product', 'sales', 'quantity', 'year'}



def test_where_and_conditions():
    result = run("SELECT * FROM data WHERE year = 2023 AND sales > 1000")
    assert len(result) > 0
    assert all(result['year'] == 2023)
    assert all(result['sales'] > 1000)



def test_group_by_sum_alias():
    result = run("SELECT region, SUM(sales) AS total FROM data GROUP BY region")
    assert list(result.columns) == ['region', 'total']
    assert len(result) == 4  # four distinct regions
    regions = set(result['region'])
    assert regions == {'North', 'South', 'East', 'West'}
    # North has the highest total sales in sample.csv
    north_total = result.loc[result['region'] == 'North', 'total'].iloc[0]
    assert north_total == 25300



def test_group_by_avg_order_by_desc():
    result = run(
        "SELECT region, AVG(sales) AS avg_s FROM data "
        "GROUP BY region ORDER BY avg_s DESC"
    )
    assert list(result.columns) == ['region', 'avg_s']
    assert len(result) == 4
    avgs = list(result['avg_s'])
    assert avgs == sorted(avgs, reverse=True), "Rows not in DESC order"
    assert result.iloc[0]['region'] == 'North'  # North has highest avg



def test_order_by_asc_limit():
    result = run("SELECT region, sales FROM data ORDER BY sales ASC LIMIT 2")
    assert len(result) == 2
    assert list(result.columns) == ['region', 'sales']
    sales = list(result['sales'])
    assert sales == sorted(sales), "Rows not in ASC order"
    assert sales[0] == 900  # minimum sales value in sample.csv



def test_missing_column_raises():
    with pytest.raises(ValueError, match=r"(?i)column not found|ghost_col"):
        run("SELECT ghost_col FROM data WHERE ghost_col > 0")


def test_or_condition():
    result = run("SELECT * FROM data WHERE sales > 1000 OR year = 2022")
    assert len(result) > 0
    # Every row must satisfy at least one branch of the OR
    assert all((result['sales'] > 1000) | (result['year'] == 2022))



def test_avg_on_non_numeric_raises():
    with pytest.raises(ValueError, match=r"(?i)numeric"):
        run("SELECT AVG(region) AS bad FROM data")


# ── Additional branch-coverage tests ─────────────────────────────────────────

def test_scalar_aggregation_no_group_by():
    # Covers executor.py _aggregate_series + scalar path (lines 106-125)
    for func in ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX'):
        result = run(f"SELECT {func}(sales) AS v FROM data")
        assert len(result) == 1
        assert 'v' in result.columns


def test_column_ref_alias_in_projection():
    # Covers executor.py _project rename path (lines 206, 214)
    result = run("SELECT region AS r, sales AS s FROM data LIMIT 2")
    assert list(result.columns) == ['r', 's']
    assert len(result) == 2


def test_order_by_missing_column_raises():
    # Covers executor.py apply_sort missing-column ValueError (line 172)
    with pytest.raises(ValueError, match=r"(?i)column not found"):
        run("SELECT region FROM data ORDER BY nonexistent_col ASC")


def test_validate_columns_wildcard_ignored():
    # Covers executor.py validate_columns '*' continue branch (line 32)
    result = run("SELECT * FROM data LIMIT 1")
    assert len(result) == 1
    assert list(result.columns) == ['region', 'product', 'sales', 'quantity', 'year']


def test_where_missing_column_raises():
    # Covers apply_filters missing-column ValueError.
    # validate_columns in execute() checks SELECT cols; a WHERE-only reference
    # that is absent from the CSV hits apply_filters' own guard.
    from engine.executor import apply_filters, load_csv
    from engine.parser import Condition
    df = load_csv(CSV)
    with pytest.raises(ValueError, match=r"Column not found"):
        apply_filters(df, Condition('nonexistent', '=', 1))


def test_double_quoted_string_in_where():
    # Covers parser.py string_val (double-quoted ESCAPED_STRING, line 98)
    result = run('SELECT * FROM data WHERE region = "North"')
    assert all(result['region'] == 'North')
    assert len(result) > 0


def test_agg_missing_column_in_grouped_path_raises():
    # Covers executor.py apply_aggregation grouped missing-column path (line 132)
    from engine.executor import apply_aggregation, load_csv
    from engine.parser import AggregateExpr
    df = load_csv(CSV)
    with pytest.raises(ValueError, match=r"Column not found"):
        apply_aggregation(df, ['region'], [AggregateExpr('SUM', 'nonexistent', 'x')])
