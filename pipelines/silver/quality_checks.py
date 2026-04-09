"""Data quality checks for the Silver layer."""
from pyspark.sql import DataFrame

from pipelines.common.quality import QualityChecker, Severity


def fact_sales_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("silver.fact_sales", run_id, layer="silver")
        .expect_no_nulls("transaction_id")
        .expect_no_nulls("product_id")
        .expect_no_nulls("date_id")
        .expect_unique("transaction_id")
        .expect_column_min("quantity", 1)
        .expect_column_min("net_amount", 0)
        .expect_column_min("gross_amount", 0)
        .expect_column_min("discount_pct", 0)
        .expect_column_max("discount_pct", 100)
        .expect_row_count_min(1)
    )


def dim_customer_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("silver.dim_customer", run_id, layer="silver")
        .expect_no_nulls("customer_id")
        .expect_no_nulls("scd_effective_from")
        .expect_no_nulls("scd_effective_to")
        .expect_no_nulls("scd_is_current")
        .expect_values_in_set(
            "loyalty_tier", {"Bronze", "Silver", "Gold", "Platinum"}
        )
        .expect_column_min("loyalty_points", 0)
        .expect_row_count_min(1)
    )


def dim_product_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("silver.dim_product", run_id, layer="silver")
        .expect_no_nulls("product_id")
        .expect_no_nulls("product_name")
        .expect_column_min("unit_price", 0)
        .expect_column_min("unit_cost", 0)
        .expect_row_count_min(1)
    )


def dim_store_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("silver.dim_store", run_id, layer="silver")
        .expect_no_nulls("store_id")
        .expect_unique("store_id")
        .expect_row_count_min(1)
    )
