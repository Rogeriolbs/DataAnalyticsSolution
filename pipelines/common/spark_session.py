"""Shared SparkSession builder with Delta Lake support."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "DataAnalyticsSolution") -> SparkSession:
    """Return an existing SparkSession or create one with Delta Lake configured."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return builder.getOrCreate()
