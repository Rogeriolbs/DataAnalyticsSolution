"""
Bronze ingestion pipeline — Landing Zone → Bronze Delta Lake.

Responsibilities:
  - Load raw CSV/JSON/Parquet files from the Landing Zone as-is
  - Validate schema against the contract; halt on breaking changes
  - Add ingestion metadata columns
  - Write to Delta Lake (append / merge-deduplication by primary key)
  - Log pipeline run metadata
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipelines.common.config import Config
from pipelines.common.logger import get_logger, new_run_id, utc_now
from pipelines.common.schema_validator import load_schema_contract, validate_schema_drift
from pipelines.common.spark_session import get_spark

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------

SOURCE_CONFIGS: dict[str, dict] = {
    "sales": {
        "landing_subpath": "sales",
        "file_format": "csv",
        "primary_key": "transaction_id",
        "partition_cols": ["ingestion_date"],
        "schema_contract": "landing/schemas/sales_schema.json",
    },
    "customers": {
        "landing_subpath": "customers",
        "file_format": "csv",
        "primary_key": "customer_id",
        "partition_cols": ["ingestion_date"],
        "schema_contract": "landing/schemas/customers_schema.json",
    },
    "products": {
        "landing_subpath": "products",
        "file_format": "csv",
        "primary_key": "product_id",
        "partition_cols": ["ingestion_date"],
        "schema_contract": "landing/schemas/products_schema.json",
    },
    "stores": {
        "landing_subpath": "stores",
        "file_format": "csv",
        "primary_key": "store_id",
        "partition_cols": ["ingestion_date"],
        "schema_contract": None,
    },
    "inventory": {
        "landing_subpath": "inventory",
        "file_format": "csv",
        "primary_key": "inventory_id",
        "partition_cols": ["ingestion_date"],
        "schema_contract": None,
    },
}


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

def _add_metadata(df: DataFrame, source: str, source_file: str, batch_id: str) -> DataFrame:
    """Append standard Bronze metadata columns to the raw DataFrame."""
    return (
        df.withColumn("_source", F.lit(source))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.lit(utc_now().isoformat()).cast(T.TimestampType()))
        .withColumn("ingestion_date", F.to_date(F.lit(utc_now().date().isoformat())))
    )


def _read_landing(spark: SparkSession, landing_path: str, file_format: str) -> DataFrame:
    reader = spark.read.option("header", "true").option("inferSchema", "true")
    if file_format == "csv":
        return reader.csv(landing_path)
    if file_format == "json":
        return reader.json(landing_path)
    if file_format == "parquet":
        return reader.parquet(landing_path)
    raise ValueError(f"Unsupported file format: {file_format}")


def _upsert_to_bronze(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    primary_key: str,
    partition_cols: list[str],
) -> int:
    """
    Merge-based upsert into Bronze Delta table.
    Creates the table on first run; merges on subsequent runs (idempotent).
    Returns the number of rows written.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        dt.alias("target").merge(
            df.alias("source"),
            f"target.{primary_key} = source.{primary_key} "
            f"AND target._source_file = source._source_file",
        ).whenNotMatchedInsertAll().execute()
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .option("mergeSchema", "true")
            .save(target_path)
        )
    return df.count()


def ingest_source(
    source_name: str,
    spark: SparkSession | None = None,
    project_root: str = ".",
) -> dict:
    """
    Ingest one source from Landing Zone into the Bronze layer.

    Args:
        source_name: Key in SOURCE_CONFIGS (e.g. 'sales').
        spark: Optional SparkSession — created if not provided.
        project_root: Filesystem root for resolving relative paths in local mode.

    Returns:
        Run result dict with status, rows_written, and any drift messages.
    """
    cfg = SOURCE_CONFIGS[source_name]
    spark = spark or get_spark(f"bronze_ingest_{source_name}")

    run_id = new_run_id()
    started_at = utc_now()

    landing_path = f"{Config.LANDING_PATH}/{cfg['landing_subpath']}"
    target_path = f"{Config.BRONZE_PATH}/{source_name}"

    logger.info(f"[{source_name}] run_id={run_id} | reading from {landing_path}")

    try:
        raw_df = _read_landing(spark, landing_path, cfg["file_format"])
        row_count_raw = raw_df.count()
        logger.info(f"[{source_name}] rows read: {row_count_raw}")

        # Schema drift check
        drift_messages: list[str] = []
        if cfg["schema_contract"]:
            contract_path = Path(project_root) / cfg["schema_contract"]
            if contract_path.exists():
                contract = load_schema_contract(contract_path)
                drift_messages = validate_schema_drift(raw_df, contract, source_name)

        enriched_df = _add_metadata(raw_df, source_name, landing_path, run_id)

        rows_written = _upsert_to_bronze(
            spark,
            enriched_df,
            target_path,
            cfg["primary_key"],
            cfg["partition_cols"],
        )

        finished_at = utc_now()
        duration_s = (finished_at - started_at).total_seconds()
        logger.info(
            f"[{source_name}] SUCCESS | rows_written={rows_written} | duration={duration_s:.1f}s"
        )

        return {
            "run_id": run_id,
            "source": source_name,
            "status": "success",
            "rows_read": row_count_raw,
            "rows_written": rows_written,
            "drift_messages": drift_messages,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": duration_s,
        }

    except Exception as exc:
        finished_at = utc_now()
        logger.error(f"[{source_name}] FAILED | error={exc}")
        raise RuntimeError(f"Bronze ingestion failed for source '{source_name}': {exc}") from exc


def run_all(spark: SparkSession | None = None, project_root: str = ".") -> list[dict]:
    """Run ingestion for all configured sources sequentially."""
    spark = spark or get_spark("bronze_ingest_all")
    results = []
    for source_name in SOURCE_CONFIGS:
        result = ingest_source(source_name, spark=spark, project_root=project_root)
        results.append(result)
    return results


if __name__ == "__main__":
    results = run_all()
    for r in results:
        print(r)
