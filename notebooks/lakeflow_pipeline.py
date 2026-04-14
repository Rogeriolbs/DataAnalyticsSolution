# Databricks notebook source
# MAGIC %md
# MAGIC # DataAnalyticsSolution — Medallion Pipeline (Lakeflow / DLT)
# MAGIC
# MAGIC Defines the full Bronze → Silver → Gold pipeline using **Delta Live Tables**.
# MAGIC All objects are published under the three-part Unity Catalog namespace:
# MAGIC
# MAGIC ```
# MAGIC DataAnalyticsSolution.<layer>.<object>
# MAGIC ```
# MAGIC
# MAGIC | Layer  | Object type        | Objects |
# MAGIC |--------|--------------------|---------|
# MAGIC | bronze | Streaming Table    | sales, customers, products, stores, inventory |
# MAGIC | silver | Streaming Table (SCD2) | dim_customer, dim_product |
# MAGIC | silver | Materialized View  | dim_date, dim_store, dim_inventory, fact_sales |
# MAGIC | gold   | Materialized View  | dim_customer, dim_product, dim_store, dim_date, fact_sales, obt_sales, agg_daily_sales, agg_monthly_sales |
# MAGIC
# MAGIC **Pipeline parameters** (set in the DAB config / pipeline UI):
# MAGIC - `pipeline.landing_path` — DBFS or cloud storage path to the Landing Zone
# MAGIC - `pipeline.catalog`      — Unity Catalog name (default: `DataAnalyticsSolution`)

# COMMAND ----------

import dlt as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ─── Pipeline parameters ──────────────────────────────────────────────────────
LANDING = spark.conf.get("pipeline.landing_path", "/Volumes/DataAnalyticsSolution/landing")
_HIGH_DATE = "9999-12-31"

# ══════════════════════════════════════════════════════════════════════════════
# BRONZE — Streaming Tables
# Auto Loader ingests new files incrementally, exactly once.
# Published as: DataAnalyticsSolution.bronze.<table>
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------


def _bronze_csv_stream(entity: str):
    """Return a streaming DataFrame reading CSV files for a given landing entity."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{LANDING}/schemas/{entity}")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{LANDING}/{entity}")
        .withColumn("_source", F.lit(entity))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
    )


@dp.streaming_table(
    schema="bronze",
    comment="Raw sales transactions ingested from the Landing Zone via Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    partition_cols=["ingestion_date"],
)
@dp.expect("non_null_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("positive_quantity", "quantity > 0")
def sales():
    return _bronze_csv_stream("sales")


@dp.streaming_table(
    schema="bronze",
    comment="Customer master data ingested from the Landing Zone via Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    partition_cols=["ingestion_date"],
)
@dp.expect("non_null_customer_id", "customer_id IS NOT NULL")
def customers():
    return _bronze_csv_stream("customers")


@dp.streaming_table(
    schema="bronze",
    comment="Product catalog ingested from the Landing Zone via Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    partition_cols=["ingestion_date"],
)
@dp.expect("non_null_product_id", "product_id IS NOT NULL")
def products():
    return _bronze_csv_stream("products")


@dp.streaming_table(
    schema="bronze",
    comment="Store master data ingested from the Landing Zone via Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    partition_cols=["ingestion_date"],
)
@dp.expect("non_null_store_id", "store_id IS NOT NULL")
def stores():
    return _bronze_csv_stream("stores")


@dp.streaming_table(
    schema="bronze",
    comment="Inventory snapshots ingested from the Landing Zone via Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    partition_cols=["ingestion_date"],
)
@dp.expect("non_null_inventory_id", "inventory_id IS NOT NULL")
def inventory():
    return _bronze_csv_stream("inventory")


# ══════════════════════════════════════════════════════════════════════════════
# SILVER — Materialized Views and SCD Type 2 Streaming Tables
# Published as: DataAnalyticsSolution.silver.<table>
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# ── dim_date — generated calendar dimension ───────────────────────────────────

@dp.table(
    schema="silver",
    comment="Calendar dimension covering 2020-01-01 to 2030-12-31.",
    table_properties={"quality": "silver"},
)
def dim_date():
    return (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(F.to_date(F.lit("2020-01-01")), F.to_date(F.lit("2030-12-31")))
            ).alias("date")
        )
        .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast(T.IntegerType()))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7).cast(T.BooleanType()))
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    )


# COMMAND ----------

# ── dim_store — full-refresh materialized view ────────────────────────────────

@dp.table(
    schema="silver",
    comment="Store master dimension. Full refresh — no SCD needed for store attributes.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def dim_store():
    return (
        dp.read("bronze.stores")
        .select(
            "store_id", "store_name", "store_type", "city", "state",
            "region", "country", "open_date", "area_sqm", "num_employees", "is_active",
        )
        .dropDuplicates(["store_id"])
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )


# COMMAND ----------

# ── dim_customer — SCD Type 2 streaming table ─────────────────────────────────
# apply_changes() tracks history whenever loyalty_tier, loyalty_points, email,
# city, or state changes. Each change creates a new versioned row with
# scd_effective_from / scd_effective_to / scd_is_current managed by DLT.

dp.create_streaming_table(
    name="dim_customer",
    schema="silver",
    comment="Customer dimension with full SCD Type 2 history tracking.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    expect_all_or_drop={"valid_customer_id": "customer_id IS NOT NULL"},
)

dp.apply_changes(
    target="dim_customer",
    source="bronze.customers",
    keys=["customer_id"],
    sequence_by=F.col("ingestion_date"),
    stored_as_scd_type=2,
    track_history_column_list=["loyalty_tier", "loyalty_points", "email", "city", "state"],
    except_column_list=["_source", "_source_file", "_ingested_at", "ingestion_date"],
    apply_as_deletes=None,
)


# COMMAND ----------

# ── dim_product — SCD Type 2 streaming table ──────────────────────────────────
# Tracks history when unit_price, unit_cost, is_active, category, or
# subcategory changes.

dp.create_streaming_table(
    name="dim_product",
    schema="silver",
    comment="Product catalog dimension with full SCD Type 2 history tracking.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    expect_all_or_drop={"valid_product_id": "product_id IS NOT NULL"},
)

dp.apply_changes(
    target="dim_product",
    source="bronze.products",
    keys=["product_id"],
    sequence_by=F.col("ingestion_date"),
    stored_as_scd_type=2,
    track_history_column_list=["unit_price", "unit_cost", "is_active", "category", "subcategory"],
    except_column_list=["_source", "_source_file", "_ingested_at", "ingestion_date"],
    apply_as_deletes=None,
)


# COMMAND ----------

# ── dim_inventory — materialized view ─────────────────────────────────────────

@dp.table(
    schema="silver",
    comment="Inventory snapshot dimension with derived stock status.",
    table_properties={"quality": "silver"},
    partition_cols=["store_id"],
)
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL")
def dim_inventory():
    return (
        dp.read("bronze.inventory")
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
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )


# COMMAND ----------

# ── fact_sales — materialized view ────────────────────────────────────────────

@dp.table(
    schema="silver",
    comment="Cleansed sales fact table with derived gross, discount, and net measures.",
    table_properties={"quality": "silver"},
    partition_cols=["date_id"],
)
@dp.expect_or_fail("non_null_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("non_negative_net_amount", "net_amount >= 0")
def fact_sales():
    return (
        dp.read("bronze.sales")
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
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount_pct") / 100, 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("store_id",
            F.when(F.col("store_id") == "", F.lit(None)).otherwise(F.col("store_id")))
        .withColumn("customer_id",
            F.when(F.col("customer_id") == "", F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .dropDuplicates(["transaction_id"])
    )


# ══════════════════════════════════════════════════════════════════════════════
# GOLD — Materialized Views
# Consumption-ready Star Schema, OBT, and KPI aggregations.
# Published as: DataAnalyticsSolution.gold.<table>
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------

# ── Gold dimensions (current rows only from SCD2 tables) ──────────────────────

@dp.table(
    schema="gold",
    comment="Current customer records for BI and self-service analytics.",
    table_properties={"quality": "gold"},
)
def dim_customer():
    return (
        dp.read("silver.dim_customer")
        .filter(F.col("__END_AT").isNull())           # DLT SCD2: NULL end = current row
        .select(
            "customer_id", "first_name", "last_name", "email",
            "gender", "loyalty_tier", "loyalty_points",
            "registration_date", "city", "state", "country",
        )
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


@dp.table(
    schema="gold",
    comment="Current product records for BI and self-service analytics.",
    table_properties={"quality": "gold"},
)
def dim_product():
    return (
        dp.read("silver.dim_product")
        .filter(F.col("__END_AT").isNull())           # DLT SCD2: NULL end = current row
        .select(
            "product_id", "product_name", "category", "subcategory",
            "brand", "sku", "unit_price", "unit_cost", "is_active", "supplier_id",
        )
        .withColumn(
            "margin_pct",
            F.round((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100, 2),
        )
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


@dp.table(
    schema="gold",
    comment="Store dimension for BI (pass-through from Silver).",
    table_properties={"quality": "gold"},
)
def dim_store():
    return (
        dp.read("silver.dim_store")
        .select("store_id", "store_name", "store_type", "city", "state", "region", "country")
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


@dp.table(
    schema="gold",
    comment="Calendar dimension for BI (pass-through from Silver).",
    table_properties={"quality": "gold"},
)
def dim_date():
    return dp.read("silver.dim_date").withColumn("_gold_loaded_at", F.current_timestamp())


# COMMAND ----------

# ── Gold fact_sales — slim fact with only keys and additive measures ───────────

@dp.table(
    schema="gold",
    comment="Sales fact table optimised for Star Schema queries.",
    table_properties={"quality": "gold"},
    partition_cols=["date_id"],
)
@dp.expect_or_fail("non_null_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_fail("non_negative_net_amount", "net_amount >= 0")
def fact_sales():
    return (
        dp.read("silver.fact_sales")
        .select(
            "transaction_id", "order_id", "date_id",
            "store_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "discount_amount", "net_amount",
            "payment_method", "channel",
        )
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


# COMMAND ----------

# ── obt_sales — pre-joined wide table for self-service analytics ───────────────

@dp.table(
    schema="gold",
    comment="One Big Table: fact_sales pre-joined with all dimensions. Optimised for Text-to-SQL and self-service analytics.",
    table_properties={"quality": "gold"},
    partition_cols=["year", "month"],
)
def obt_sales():
    fact        = dp.read("silver.fact_sales")
    dim_dt      = dp.read("silver.dim_date")
    dim_str     = dp.read("silver.dim_store")
    dim_cust    = dp.read("silver.dim_customer").filter(F.col("__END_AT").isNull())
    dim_prod    = dp.read("silver.dim_product").filter(F.col("__END_AT").isNull())

    return (
        fact
        .join(
            dim_dt.select("date_id", "year", "month", "quarter", "day_name", "is_weekend", "year_month"),
            on="date_id", how="left",
        )
        .join(
            dim_str.select(
                "store_id",
                F.col("store_name"),
                F.col("city").alias("store_city"),
                F.col("state").alias("store_state"),
                F.col("region").alias("store_region"),
            ),
            on="store_id", how="left",
        )
        .join(
            dim_cust.select(
                "customer_id",
                F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
                F.col("loyalty_tier").alias("customer_loyalty_tier"),
                F.col("gender").alias("customer_gender"),
                F.col("city").alias("customer_city"),
            ),
            on="customer_id", how="left",
        )
        .join(
            dim_prod.select(
                "product_id",
                F.col("product_name"),
                F.col("category").alias("product_category"),
                F.col("subcategory").alias("product_subcategory"),
                F.col("brand").alias("product_brand"),
                F.round(
                    (F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100, 2
                ).alias("margin_pct"),
            ),
            on="product_id", how="left",
        )
        .select(
            "transaction_id", "order_id",
            "transaction_date", "year", "month", "quarter", "day_name", "is_weekend", "year_month",
            "store_id", "store_name", "store_city", "store_state", "store_region",
            "customer_id", "customer_name", "customer_loyalty_tier", "customer_gender", "customer_city",
            "product_id", "product_name", "product_category", "product_subcategory", "product_brand",
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "discount_amount", "net_amount", "margin_pct",
            "payment_method", "channel",
        )
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


# COMMAND ----------

# ── agg_daily_sales — KPI aggregation by day / store / category / channel ─────

@dp.table(
    schema="gold",
    comment="Daily sales KPIs aggregated by store, product category, and channel.",
    table_properties={"quality": "gold"},
    partition_cols=["year", "month"],
)
def agg_daily_sales():
    return (
        dp.read("gold.obt_sales")
        .groupBy(
            "year", "month", "year_month", "transaction_date",
            "store_id", "store_name", "store_region",
            "product_category", "channel",
        )
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
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


# COMMAND ----------

# ── agg_monthly_sales — KPI aggregation rolled up to month ────────────────────

@dp.table(
    schema="gold",
    comment="Monthly sales KPIs rolled up from the daily aggregation.",
    table_properties={"quality": "gold"},
    partition_cols=["year"],
)
def agg_monthly_sales():
    return (
        dp.read("gold.agg_daily_sales")
        .groupBy(
            "year", "month", "year_month",
            "store_id", "store_name", "store_region",
            "product_category", "channel",
        )
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
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )
