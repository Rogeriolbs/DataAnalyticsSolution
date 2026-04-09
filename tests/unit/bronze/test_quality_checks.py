"""Unit tests for Bronze data quality checks."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import Row

from pipelines.bronze.quality_checks import sales_checker, customers_checker, products_checker
from pipelines.common.quality import Severity


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_bronze_quality")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Sales quality checks
# ---------------------------------------------------------------------------

def _sales_df(spark, overrides: dict = {}):
    default = {
        "transaction_id": "TXN-00001",
        "product_id": "PRD-1001",
        "quantity": 2,
        "unit_price": 29.99,
        "discount_pct": 0.0,
        "channel": "in_store",
        "payment_method": "credit_card",
    }
    default.update(overrides)
    return spark.createDataFrame([Row(**default)])


def test_sales_passes_valid_row(spark):
    df = _sales_df(spark)
    report = sales_checker("run-001").run(df)
    assert report.passed
    assert len(report.critical_failures) == 0


def test_sales_fails_null_transaction_id(spark):
    df = _sales_df(spark, {"transaction_id": None})
    report = sales_checker("run-002").run(df)
    assert not report.passed
    assert any("no_nulls:transaction_id" in r.rule_name for r in report.critical_failures)


def test_sales_fails_invalid_channel(spark):
    df = _sales_df(spark, {"channel": "telegram"})
    report = sales_checker("run-003").run(df)
    assert not report.passed
    assert any("values_in_set:channel" in r.rule_name for r in report.critical_failures)


def test_sales_fails_negative_quantity(spark):
    df = _sales_df(spark, {"quantity": -1})
    report = sales_checker("run-004").run(df)
    assert not report.passed
    assert any("min_value:quantity" in r.rule_name for r in report.critical_failures)


def test_sales_fails_discount_over_100(spark):
    df = _sales_df(spark, {"discount_pct": 110.0})
    report = sales_checker("run-005").run(df)
    assert not report.passed
    assert any("max_value:discount_pct" in r.rule_name for r in report.critical_failures)


def test_sales_fails_duplicate_transaction_id(spark):
    row = Row(
        transaction_id="TXN-00001",
        product_id="PRD-1001",
        quantity=1,
        unit_price=9.99,
        discount_pct=0.0,
        channel="in_store",
        payment_method="cash",
    )
    df = spark.createDataFrame([row, row])
    report = sales_checker("run-006").run(df)
    assert not report.passed
    assert any("unique:transaction_id" in r.rule_name for r in report.critical_failures)


# ---------------------------------------------------------------------------
# Customers quality checks
# ---------------------------------------------------------------------------

def _customer_df(spark, overrides: dict = {}):
    default = {
        "customer_id": "CUST-0001",
        "email": "alice@example.com",
        "loyalty_tier": "Gold",
        "loyalty_points": 500,
    }
    default.update(overrides)
    return spark.createDataFrame([Row(**default)])


def test_customers_passes_valid_row(spark):
    df = _customer_df(spark)
    report = customers_checker("run-010").run(df)
    assert report.passed


def test_customers_fails_null_email(spark):
    df = _customer_df(spark, {"email": None})
    report = customers_checker("run-011").run(df)
    assert not report.passed
    assert any("no_nulls:email" in r.rule_name for r in report.critical_failures)


def test_customers_warns_invalid_loyalty_tier(spark):
    df = _customer_df(spark, {"loyalty_tier": "Diamond"})
    report = customers_checker("run-012").run(df)
    # Loyalty tier is WARNING severity — pipeline should still pass
    assert report.passed
    assert any("values_in_set:loyalty_tier" in r.rule_name for r in report.warnings)
