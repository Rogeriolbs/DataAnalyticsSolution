"""Unit tests for Silver data quality checks."""
import pytest
from pyspark.sql import SparkSession, Row

from pipelines.silver.quality_checks import fact_sales_checker, dim_customer_checker


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_silver_quality")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def _fact_row(**kwargs):
    default = dict(
        transaction_id="TXN-00001",
        product_id="PRD-1001",
        date_id=20240101,
        quantity=2,
        net_amount=55.00,
        gross_amount=59.98,
        discount_pct=0.0,
    )
    default.update(kwargs)
    return Row(**default)


def test_fact_sales_passes(spark):
    df = spark.createDataFrame([_fact_row()])
    report = fact_sales_checker("run-s-001").run(df)
    assert report.passed


def test_fact_sales_fails_null_date_id(spark):
    df = spark.createDataFrame([_fact_row(date_id=None)])
    report = fact_sales_checker("run-s-002").run(df)
    assert not report.passed
    assert any("no_nulls:date_id" in r.rule_name for r in report.critical_failures)


def test_fact_sales_fails_negative_net_amount(spark):
    df = spark.createDataFrame([_fact_row(net_amount=-1.0)])
    report = fact_sales_checker("run-s-003").run(df)
    assert not report.passed


def _customer_row(**kwargs):
    default = dict(
        customer_id="CUST-0001",
        loyalty_tier="Gold",
        loyalty_points=500,
        scd_effective_from="2024-01-01",
        scd_effective_to="9999-12-31",
        scd_is_current=True,
    )
    default.update(kwargs)
    return Row(**default)


def test_dim_customer_passes(spark):
    df = spark.createDataFrame([_customer_row()])
    report = dim_customer_checker("run-s-010").run(df)
    assert report.passed


def test_dim_customer_fails_null_scd_fields(spark):
    df = spark.createDataFrame([_customer_row(scd_effective_from=None)])
    report = dim_customer_checker("run-s-011").run(df)
    assert not report.passed
