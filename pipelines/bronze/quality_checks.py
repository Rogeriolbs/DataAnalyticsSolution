"""Data quality checks for the Bronze layer."""
from pyspark.sql import DataFrame

from pipelines.common.quality import QualityChecker, Severity


def sales_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("bronze.sales", run_id, layer="bronze")
        .expect_no_nulls("transaction_id")
        .expect_no_nulls("product_id")
        .expect_no_nulls("quantity")
        .expect_no_nulls("unit_price")
        .expect_unique("transaction_id")
        .expect_column_min("quantity", 1)
        .expect_column_min("unit_price", 0)
        .expect_column_min("discount_pct", 0)
        .expect_column_max("discount_pct", 100)
        .expect_values_in_set("channel", {"in_store", "online", "mobile"})
        .expect_values_in_set(
            "payment_method",
            {"credit_card", "debit_card", "cash", "pix", "voucher"},
        )
        .expect_row_count_min(1)
    )


def customers_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("bronze.customers", run_id, layer="bronze")
        .expect_no_nulls("customer_id")
        .expect_no_nulls("email")
        .expect_unique("customer_id")
        .expect_values_in_set(
            "loyalty_tier",
            {"Bronze", "Silver", "Gold", "Platinum"},
            severity=Severity.WARNING,
        )
        .expect_column_min("loyalty_points", 0, severity=Severity.WARNING)
        .expect_row_count_min(1)
    )


def products_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("bronze.products", run_id, layer="bronze")
        .expect_no_nulls("product_id")
        .expect_no_nulls("product_name")
        .expect_no_nulls("unit_price")
        .expect_unique("product_id")
        .expect_unique("sku")
        .expect_column_min("unit_price", 0)
        .expect_column_min("unit_cost", 0)
        .expect_row_count_min(1)
    )


def stores_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("bronze.stores", run_id, layer="bronze")
        .expect_no_nulls("store_id")
        .expect_no_nulls("store_name")
        .expect_unique("store_id")
        .expect_row_count_min(1)
    )


def inventory_checker(run_id: str) -> QualityChecker:
    return (
        QualityChecker("bronze.inventory", run_id, layer="bronze")
        .expect_no_nulls("inventory_id")
        .expect_no_nulls("store_id")
        .expect_no_nulls("product_id")
        .expect_column_min("quantity_on_hand", 0)
        .expect_row_count_min(1)
    )
