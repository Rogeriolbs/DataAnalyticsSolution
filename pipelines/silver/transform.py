"""
Silver transformation pipeline — Bronze → Silver Delta Lake.

Responsibilities:
  - Cleanse and standardise raw Bronze data
  - Model a Snowflake Schema: fact_sales + dimensions
  - Apply SCD Type 2 on dim_customer and dim_product
  - Write all tables as Delta with MERGE (idempotent)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

from pipelines.common.config import Config
from pipelines.common.logger import get_logger, new_run_id, utc_now
from pipelines.common.spark_session import get_spark

logger = get_logger(__name__)

_NOW = utc_now()
_HIGH_DATE = "9999-12-31"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bronze(spark: SparkSession, entity: str) -> DataFrame:
    path = f"{Config.BRONZE_PATH}/{entity}"
    return spark.read.format("delta").load(path)


def _write_delta(df: DataFrame, path: str, partition_cols: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def _scd2_merge(
    spark: SparkSession,
    new_df: DataFrame,
    target_path: str,
    natural_key: str,
    tracked_cols: list[str],
) -> None:
    """
    SCD Type 2 merge:
      - Expire old rows when tracked columns change
      - Insert new rows with effective_from = today
      - Pass-through unchanged rows
    """
    effective_from_col = "scd_effective_from"
    effective_to_col = "scd_effective_to"
    is_current_col = "scd_is_current"
    today = _NOW.date().isoformat()

    new_df = (
        new_df.withColumn(effective_from_col, F.lit(today).cast(T.DateType()))
        .withColumn(effective_to_col, F.lit(_HIGH_DATE).cast(T.DateType()))
        .withColumn(is_current_col, F.lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        _write_delta(new_df, target_path)
        return

    dt = DeltaTable.forPath(spark, target_path)
    change_condition = " OR ".join(
        [f"target.{c} <> source.{c}" for c in tracked_cols]
    )

    # Step 1 — expire changed current rows
    dt.alias("target").merge(
        new_df.alias("source"),
        f"target.{natural_key} = source.{natural_key} AND target.{is_current_col} = true",
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            effective_to_col: f"source.{effective_from_col}",
            is_current_col: "false",
        },
    ).execute()

    # Step 2 — insert new / changed rows
    existing = dt.toDF().filter(F.col(is_current_col)).select(natural_key)
    changed = new_df.join(existing, on=natural_key, how="left_anti")  # new or changed

    # Also insert truly new records
    changed.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)


# ---------------------------------------------------------------------------
# Dimension transforms
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession) -> DataFrame:
    """Generate a date dimension from 2020-01-01 to 2030-12-31."""
    from pyspark.sql.functions import sequence, explode, to_date, lit, year, month, dayofmonth, \
        dayofweek, quarter, date_format, weekofyear

    dates = spark.range(1).select(
        explode(
            sequence(to_date(lit("2020-01-01")), to_date(lit("2030-12-31")))
        ).alias("date")
    )
    return (
        dates
        .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast(T.IntegerType()))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("day_of_week", dayofweek("date"))
        .withColumn("day_name", date_format("date", "EEEE"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("is_weekend", (dayofweek("date").isin(1, 7)).cast(T.BooleanType()))
        .withColumn("year_month", date_format("date", "yyyy-MM"))
    )


def build_dim_store(spark: SparkSession) -> DataFrame:
    return (
        _bronze(spark, "stores")
        .select(
            "store_id", "store_name", "store_type", "city", "state",
            "region", "country", "open_date", "area_sqm", "num_employees", "is_active",
        )
        .dropDuplicates(["store_id"])
        .withColumn("_silver_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


def build_dim_customer(spark: SparkSession) -> DataFrame:
    """Returns cleaned customer dimension (SCD2 applied separately)."""
    return (
        _bronze(spark, "customers")
        .select(
            "customer_id", "first_name", "last_name", "email",
            "date_of_birth", "gender", "loyalty_tier", "loyalty_points",
            "registration_date", "city", "state", "country", "marketing_opt_in",
        )
        .withColumn(
            "email",
            F.when(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.col("email"))
            .otherwise(F.lit(None)),
        )
        .withColumn("loyalty_tier", F.coalesce(F.col("loyalty_tier"), F.lit("Bronze")))
        .withColumn("loyalty_points", F.coalesce(F.col("loyalty_points").cast(T.LongType()), F.lit(0)))
        .dropDuplicates(["customer_id"])
    )


def build_dim_product(spark: SparkSession) -> DataFrame:
    """Returns cleaned product dimension (SCD2 applied separately)."""
    return (
        _bronze(spark, "products")
        .select(
            "product_id", "product_name", "category", "subcategory",
            "brand", "sku", "unit_cost", "unit_price", "weight_kg",
            "is_perishable", "is_active", "launch_date", "supplier_id",
        )
        .withColumn("unit_cost", F.col("unit_cost").cast(T.DoubleType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
        .withColumn("margin_pct",
            F.round((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100, 2)
        )
        .dropDuplicates(["product_id"])
    )


def build_dim_inventory(spark: SparkSession) -> DataFrame:
    return (
        _bronze(spark, "inventory")
        .select(
            "inventory_id", "store_id", "product_id",
            "quantity_on_hand", "quantity_reserved", "quantity_on_order",
            "reorder_point", "reorder_quantity", "last_restock_date", "last_updated",
        )
        .withColumn(
            "stock_status",
            F.when(F.col("quantity_on_hand") <= 0, F.lit("out_of_stock"))
            .when(F.col("quantity_on_hand") <= F.col("reorder_point"), F.lit("low_stock"))
            .otherwise(F.lit("in_stock")),
        )
        .withColumn("available_qty", F.col("quantity_on_hand") - F.col("quantity_reserved"))
        .withColumn("_silver_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
    )


# ---------------------------------------------------------------------------
# Fact transform
# ---------------------------------------------------------------------------

def build_fact_sales(spark: SparkSession) -> DataFrame:
    """
    Cleanse and enrich raw sales transactions into a fact table.
    Derives surrogate keys from natural keys for Snowflake Schema compatibility.
    """
    sales = (
        _bronze(spark, "sales")
        .select(
            "transaction_id", "order_id", "transaction_date",
            "store_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct", "payment_method", "channel",
        )
        .withColumn("transaction_date", F.to_timestamp("transaction_date"))
        .withColumn("date_id", F.date_format(F.to_date("transaction_date"), "yyyyMMdd").cast(T.IntegerType()))
        .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct").cast(T.DoubleType()), F.lit(0.0)))
        # Derived measures
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(
            F.col("gross_amount") * F.col("discount_pct") / 100, 2
        ))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        # Nullify missing FK references (guest/online transactions)
        .withColumn("store_id", F.when(F.col("store_id") == "", F.lit(None)).otherwise(F.col("store_id")))
        .withColumn("customer_id", F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("_silver_loaded_at", F.lit(_NOW.isoformat()).cast(T.TimestampType()))
        .dropDuplicates(["transaction_id"])
    )
    return sales


# ---------------------------------------------------------------------------
# Orchestrate Silver run
# ---------------------------------------------------------------------------

def run(spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("silver_transform")
    run_id = new_run_id()
    started_at = utc_now()
    logger.info(f"Silver pipeline started | run_id={run_id}")

    results = {}

    # dim_date (full refresh — static)
    dim_date = build_dim_date(spark)
    _write_delta(dim_date, f"{Config.SILVER_PATH}/dim_date")
    results["dim_date"] = dim_date.count()
    logger.info(f"dim_date written: {results['dim_date']} rows")

    # dim_store (full refresh — small, no SCD needed)
    dim_store = build_dim_store(spark)
    _write_delta(dim_store, f"{Config.SILVER_PATH}/dim_store")
    results["dim_store"] = dim_store.count()
    logger.info(f"dim_store written: {results['dim_store']} rows")

    # dim_customer — SCD Type 2
    dim_customer = build_dim_customer(spark)
    _scd2_merge(
        spark, dim_customer,
        f"{Config.SILVER_PATH}/dim_customer",
        natural_key="customer_id",
        tracked_cols=["loyalty_tier", "loyalty_points", "email", "city", "state"],
    )
    results["dim_customer"] = dim_customer.count()
    logger.info(f"dim_customer SCD2 applied: {results['dim_customer']} source rows")

    # dim_product — SCD Type 2
    dim_product = build_dim_product(spark)
    _scd2_merge(
        spark, dim_product,
        f"{Config.SILVER_PATH}/dim_product",
        natural_key="product_id",
        tracked_cols=["unit_price", "unit_cost", "is_active", "category", "subcategory"],
    )
    results["dim_product"] = dim_product.count()
    logger.info(f"dim_product SCD2 applied: {results['dim_product']} source rows")

    # dim_inventory
    dim_inventory = build_dim_inventory(spark)
    _write_delta(dim_inventory, f"{Config.SILVER_PATH}/dim_inventory", partition_cols=["store_id"])
    results["dim_inventory"] = dim_inventory.count()
    logger.info(f"dim_inventory written: {results['dim_inventory']} rows")

    # fact_sales
    fact_sales = build_fact_sales(spark)
    _write_delta(fact_sales, f"{Config.SILVER_PATH}/fact_sales", partition_cols=["date_id"])
    results["fact_sales"] = fact_sales.count()
    logger.info(f"fact_sales written: {results['fact_sales']} rows")

    finished_at = utc_now()
    duration_s = (finished_at - started_at).total_seconds()
    logger.info(f"Silver pipeline completed | run_id={run_id} | duration={duration_s:.1f}s")

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
