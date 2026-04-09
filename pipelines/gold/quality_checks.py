"""Data quality checks for the Gold layer."""
from pipelines.common.quality import QualityChecker, Severity


def obt_sales_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("gold.obt_sales", run_id, layer="gold")
        .expect_no_nulls("transaction_id")
        .expect_no_nulls("product_id")
        .expect_no_nulls("product_category")
        .expect_unique("transaction_id")
        .expect_column_min("net_amount", 0)
        .expect_column_min("quantity", 1)
        .expect_row_count_min(1)
    )


def agg_daily_sales_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("gold.agg_daily_sales", run_id, layer="gold")
        .expect_no_nulls("year")
        .expect_no_nulls("month")
        .expect_column_min("total_net_revenue", 0)
        .expect_column_min("num_transactions", 1)
        .expect_row_count_min(1)
    )
