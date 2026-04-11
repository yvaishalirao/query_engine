"""Tests for engine/parser.py: valid AST shapes, rejection cases, and type coercion."""

import pytest
from engine.parser import (
    parse,
    SelectStatement,
    ColumnRef,
    AggregateExpr,
    Condition,
    OrderByClause,
)


# ── Valid queries ─────────────────────────────────────────────────────────────

def test_select_star():
    stmt = parse("SELECT * FROM data")
    assert isinstance(stmt, SelectStatement)
    assert stmt.columns == [ColumnRef(name="*")]
    assert stmt.table == "data"
    assert stmt.where == []
    assert stmt.group_by == []
    assert stmt.order_by is None
    assert stmt.limit is None


def test_select_multiple_columns():
    stmt = parse("SELECT col1, col2 FROM data")
    assert stmt.columns == [ColumnRef("col1"), ColumnRef("col2")]
    assert stmt.table == "data"


def test_select_aggregate_with_group_by():
    stmt = parse("SELECT region, AVG(sales) AS avg_s FROM data GROUP BY region")
    assert stmt.columns[0] == ColumnRef("region")
    assert stmt.columns[1] == AggregateExpr(func="AVG", column="sales", alias="avg_s")
    assert stmt.group_by == ["region"]


def test_where_single_condition():
    stmt = parse("SELECT x FROM data WHERE age > 25")
    assert len(stmt.where) == 1
    assert stmt.where[0] == Condition(column="age", operator=">", value=25)


def test_where_and_conditions():
    stmt = parse("SELECT x FROM data WHERE age > 25 AND salary < 50000")
    assert len(stmt.where) == 2
    assert stmt.where[0] == Condition("age", ">", 25)
    assert stmt.where[1] == Condition("salary", "<", 50000)


def test_order_by_desc_limit():
    stmt = parse("SELECT x FROM data ORDER BY sales DESC LIMIT 5")
    assert stmt.order_by == OrderByClause(column="sales", direction="DESC")
    assert stmt.limit == 5


def test_full_query_all_clauses():
    stmt = parse(
        "SELECT region, SUM(sales) AS total FROM data "
        "WHERE year = 2023 AND quantity > 10 "
        "GROUP BY region "
        "ORDER BY total DESC "
        "LIMIT 3"
    )
    assert isinstance(stmt, SelectStatement)
    assert stmt.columns[0] == ColumnRef("region")
    assert stmt.columns[1] == AggregateExpr("SUM", "sales", "total")
    assert stmt.table == "data"
    assert stmt.where[0] == Condition("year", "=", 2023)
    assert stmt.where[1] == Condition("quantity", ">", 10)
    assert stmt.group_by == ["region"]
    assert stmt.order_by == OrderByClause("total", "DESC")
    assert stmt.limit == 3


def test_order_by_asc_default():
    stmt = parse("SELECT x FROM data ORDER BY sales ASC")
    assert stmt.order_by == OrderByClause("sales", "ASC")


def test_column_alias():
    stmt = parse("SELECT region AS r FROM data")
    assert stmt.columns[0] == ColumnRef(name="region", alias="r")


def test_all_aggregates():
    for func in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
        stmt = parse(f"SELECT {func}(sales) AS result FROM data")
        agg = stmt.columns[0]
        assert isinstance(agg, AggregateExpr)
        assert agg.func == func
        assert agg.column == "sales"
        assert agg.alias == "result"


# ── Rejection cases ───────────────────────────────────────────────────────────

def test_or_condition_raises():
    with pytest.raises(ValueError, match=r"(?i)syntax error|not supported"):
        parse("SELECT x FROM data WHERE a > 1 OR b < 2")


def test_empty_string_raises():
    with pytest.raises(ValueError):
        parse("")


def test_missing_from_raises():
    with pytest.raises(ValueError):
        parse("SELECT region data")


def test_unsupported_aggregate_raises():
    with pytest.raises(ValueError):
        parse("SELECT MEDIAN(sales) FROM data")


def test_having_raises():
    with pytest.raises(ValueError):
        parse("SELECT region, AVG(sales) FROM data GROUP BY region HAVING AVG(sales) > 100")


def test_subquery_raises():
    with pytest.raises(ValueError):
        parse("SELECT region FROM (SELECT * FROM data)")


# ── Type coercion in WHERE values ─────────────────────────────────────────────

def test_coerce_integer():
    stmt = parse("SELECT x FROM data WHERE age > 25")
    val = stmt.where[0].value
    assert val == 25
    assert isinstance(val, int)


def test_coerce_float():
    stmt = parse("SELECT x FROM data WHERE price > 9.99")
    val = stmt.where[0].value
    assert abs(val - 9.99) < 1e-9
    assert isinstance(val, float)


def test_coerce_string():
    stmt = parse("SELECT x FROM data WHERE region = 'North'")
    val = stmt.where[0].value
    assert val == "North"
    assert isinstance(val, str)


def test_coerce_negative_number():
    stmt = parse("SELECT x FROM data WHERE temp > -5")
    val = stmt.where[0].value
    assert val == -5
    assert isinstance(val, int)
