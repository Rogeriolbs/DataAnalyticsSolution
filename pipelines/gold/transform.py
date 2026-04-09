"""
Gold transformation pipeline — Silver → Gold Delta Lake.

Deliverables:
  - Star Schema: slim fact + conformed dimensions for BI tools
  - One Big Table (OBT): pre-joined wide table for self-service analytics
  - Aggregated KPI tables: daily / monthly sales summaries
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipelines.common.config import Config
from pipelines.common.logger import get_logger, new_run_id, utc_now
from pipelines.common.spark_session import get_spark

logger = get_logger(__name__)

_NOW = utc_now()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _silver(spark: SparkSession, table: str) -> DataFrame:
    path = f"{Config.SILVER_PATH}/{table}"
    return spark.read.format("delta").load(path)


def _current(df: DataFrame) -> DataFrame:
    """Filter SCD2 table to current rows only."""
    return df.filter(F.col("scd_is_current") == True)  # noqa: E712


def _write_gold(df: DataFrame, name: str, partition_cols: list[str] | None = None) -> None:
    path = f"{Config.GOLD_PATH}/{name}"
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info(f"Gold table '{name}' written: {df.count()} rows → {path}")


# ---------------------------------------------------------------------------
# Star Schema tables
# ---------------------------------------------------------------------------

def build_gold_dim_customer(spark: SparkSession) -> DataFrame:
    return (
        _current(_silver(spark, "dim_customer"))
        .select(
            "customer_id", "first_name", "last_name", "email",
            "gender", "loyalty_tier", "loyalty_points",
            "registration_date", "city", "state", "country",
        )
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


def build_gold_dim_product(spark: SparkSession) -> DataFrame:
    return (
        _current(_silver(spark, "dim_product"))
        .select(
            "product_id", "product_name", "category", "subcategory",
            "brand", "sku", "unit_price", "unit_cost", "margin_pct",
            "is_active", "supplier_id",
        )
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


def build_gold_dim_store(spark: SparkSession) -> DataFrame:
    return (
        _silver(spark, "dim_store")
        .select("store_id", "store_name", "store_type", "city", "state", "region", "country")
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


def build_gold_dim_date(spark: SparkSession) -> DataFrame:
    return _silver(spark, "dim_date").withColumn(
        "_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType())
    )


def build_gold_fact_sales(spark: SparkSession) -> DataFrame:
    """
    Gold fact_sales — slim fact with only foreign keys and additive measures.
    """
    return (
        _silver(spark, "fact_sales")
        .select(
            "transaction_id", "order_id", "date_id",
            "store_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "discount_amount", "net_amount",
            "payment_method", "channel",
        )
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


# ---------------------------------------------------------------------------
# One Big Table (OBT)
# ---------------------------------------------------------------------------

def build_obt_sales(spark: SparkSession) -> DataFrame:
    """
    Pre-joined wide table for self-service analytics and Text-to-SQL.
    Joins fact_sales with all current dimension rows.
    """
    fact = _silver(spark, "fact_sales")
    dim_customer = _current(_silver(spark, "dim_customer"))
    dim_product = _current(_silver(spark, "dim_product"))
    dim_store = _silver(spark, "dim_store")
    dim_date = _silver(spark, "dim_date")

    return (
        fact
        .join(
            dim_date.select("date_id", "year", "month", "quarter", "day_name", "is_weekend", "year_month"),
            on="date_id", how="left",
        )
        .join(
            dim_store.select(
                "store_id",
                F.col("store_name").alias("store_name"),
                F.col("city").alias("store_city"),
                F.col("state").alias("store_state"),
                F.col("region").alias("store_region"),
            ),
            on="store_id", how="left",
        )
        .join(
            dim_customer.select(
                "customer_id",
                F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
                F.col("loyalty_tier").alias("customer_loyalty_tier"),
                F.col("gender").alias("customer_gender"),
                F.col("city").alias("customer_city"),
            ),
            on="customer_id", how="left",
        )
        .join(
            dim_product.select(
                "product_id",
                F.col("product_name"),
                F.col("category").alias("product_category"),
                F.col("subcategory").alias("product_subcategory"),
                F.col("brand").alias("product_brand"),
                F.col("margin_pct"),
            ),
            on="product_id", how="left",
        )
        .select(
            # Transaction keys
            "transaction_id", "order_id",
            # Time
            "transaction_date", "year", "month", "quarter", "day_name", "is_weekend", "year_month",
            # Store
            "store_id", "store_name", "store_city", "store_state", "store_region",
            # Customer
            "customer_id", "customer_name", "customer_loyalty_tier", "customer_gender", "customer_city",
            # Product
            "product_id", "product_name", "product_category", "product_subcategory", "product_brand",
            # Sales metrics
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "discount_amount", "net_amount", "margin_pct",
            # Transaction attributes
            "payment_method", "channel",
        )
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


# ---------------------------------------------------------------------------
# Aggregated KPI tables
# ---------------------------------------------------------------------------

def build_agg_daily_sales(spark: SparkSession) -> DataFrame:
    obt = build_obt_sales(spark)
    return (
        obt.groupBy("year", "month", "year_month", "transaction_date", "store_id", "store_name",
                    "store_region", "product_category", "channel")
        .agg(
            F.countDistinct("transaction_id").alias("num_transactions"),
            F.countDistinct("customer_id").alias("num_customers"),
            F.sum("quantity").alias("total_units"),
            F.sum("gross_amount").alias("total_gross"),
            F.sum("discount_amount").alias("total_discount"),
            F.sum("net_amount").alias("total_net_revenue"),
            F.avg("net_amount").alias("avg_order_value"),
            F.avg("margin_pct").alias("avg_margin_pct"),
        )
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


def build_agg_monthly_sales(spark: SparkSession) -> DataFrame:
    daily = build_agg_daily_sales(spark)
    return (
        daily.groupBy("year", "month", "year_month", "store_id", "store_name",
                      "store_region", "product_category", "channel")
        .agg(
            F.sum("num_transactions").alias("num_transactions"),
            F.sum("num_customers").alias("num_customers"),
            F.sum("total_units").alias("total_units"),
            F.sum("total_gross").alias("total_gross"),
            F.sum("total_discount").alias("total_discount"),
            F.sum("total_net_revenue").alias("total_net_revenue"),
            F.avg("avg_order_value").alias("avg_order_value"),
            F.avg("avg_margin_pct").alias("avg_margin_pct"),
        )
        .withColumn("_gold_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


# ---------------------------------------------------------------------------
# Orchestrate Gold run
# ---------------------------------------------------------------------------

def run(spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("gold_transform")
    run_id = new_run_id()
    started_at = utc_now()
    logger.info(f"Gold pipeline started | run_id={run_id}")

    results = {}

    # Star Schema dims
    for name, builder in [
        ("dim_customer", build_gold_dim_customer),
        ("dim_product", build_gold_dim_product),
        ("dim_store", build_gold_dim_store),
        ("dim_date", build_gold_dim_date),
    ]:
        df = builder(spark)
        _write_gold(df, name)
        results[name] = df.count()

    # Star Schema fact
    fact = build_gold_fact_sales(spark)
    _write_gold(fact, "fact_sales", partition_cols=["date_id"])
    results["fact_sales"] = fact.count()

    # OBT
    obt = build_obt_sales(spark)
    _write_gold(obt, "obt_sales", partition_cols=["year", "month"])
    results["obt_sales"] = obt.count()

    # Aggregations
    daily = build_agg_daily_sales(spark)
    _write_gold(daily, "agg_daily_sales", partition_cols=["year", "month"])
    results["agg_daily_sales"] = daily.count()

    monthly = build_agg_monthly_sales(spark)
    _write_gold(monthly, "agg_monthly_sales", partition_cols=["year"])
    results["agg_monthly_sales"] = monthly.count()

    finished_at = utc_now()
    duration_s = (finished_at - started_at).total_seconds()
    logger.info(f"Gold pipeline completed | run_id={run_id} | duration={duration_s:.1f}s")

    return {
        "run_id": run_id,
        "status": "success",
        "tables": results,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration_s,
    }


if __name__ == "__main__":
    print(run())
