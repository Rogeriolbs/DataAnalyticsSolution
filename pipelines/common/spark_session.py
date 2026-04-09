"""Shared SparkSession builder with Delta Lake support."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "DataAnalyticsSolution") -> SparkSession:
    """Return an existing SparkSession or create one with Delta Lake configured.

    Uses configure_spark_with_delta_pip when running locally so the Delta JARs
    are resolved automatically. On Databricks the Delta runtime is pre-installed
    and getOrCreate() picks up the existing session.
    """
    try:
        # Running on Databricks — session already exists with Delta pre-loaded
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark
    except Exception:
        pass

    # Local mode — let delta-spark download and wire up the JARs
    import os
    from delta import configure_spark_with_delta_pip

    # On Windows, point the JVM at the Hadoop native libraries (winutils / hadoop.dll)
    hadoop_home = os.environ.get("HADOOP_HOME", "C:/hadoop")
    hadoop_bin = f"{hadoop_home}/bin"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={hadoop_bin}")
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={hadoop_bin}")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
