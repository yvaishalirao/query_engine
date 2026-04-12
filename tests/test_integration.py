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


def test_or_condition_raises():
    with pytest.raises(ValueError):
        run("SELECT region FROM data WHERE sales > 1000 OR year = 2022")



def test_avg_on_non_numeric_raises():
    with pytest.raises(ValueError, match=r"(?i)numeric"):
        run("SELECT AVG(region) AS bad FROM data")
