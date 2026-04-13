# Databricks notebook source
# MAGIC %md
# MAGIC # DataAnalyticsSolution — Free Tier Deployment
# MAGIC
# MAGIC Runs the full Bronze → Silver → Gold pipeline on Databricks Community Edition.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - This notebook lives inside a Databricks Repo:
# MAGIC   `Workspace > Repos > Add Repo > https://github.com/Rogeriolbs/DataAnalyticsSolution.git`
# MAGIC - Cluster: Single Node, Databricks Runtime 13.x LTS (or newer), no extra libraries needed.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 1 — Detect repo root and current user

# COMMAND ----------

import os
import sys

# Identify the logged-in user so we can resolve the Repos path.
username = spark.sql("SELECT current_user()").collect()[0][0]
repo_root = f"/Repos/{username}/DataAnalyticsSolution"

print(f"username : {username}")
print(f"repo_root: {repo_root}")

# Make the project importable as a package.
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 2 — Copy landing CSV files from the Repo to DBFS
# MAGIC
# MAGIC Spark cannot read directly from `/Repos/...` paths.
# MAGIC We use Python file I/O (which can access Repos via the local filesystem) to copy
# MAGIC each CSV into `/dbfs/FileStore/DataAnalyticsSolution/landing/`, which Spark sees as
# MAGIC `/FileStore/DataAnalyticsSolution/landing/`.

# COMMAND ----------

import shutil

DBFS_ROOT = "/dbfs/FileStore/DataAnalyticsSolution"
SPARK_ROOT = "/FileStore/DataAnalyticsSolution"

entities = ["sales", "customers", "products", "stores", "inventory"]

for entity in entities:
    src_dir = f"{repo_root}/landing/{entity}"
    dst_dir = f"{DBFS_ROOT}/landing/{entity}"
    os.makedirs(dst_dir, exist_ok=True)
    copied = 0
    for fname in os.listdir(src_dir):
        if fname.endswith((".csv", ".json", ".parquet")):
            shutil.copy2(f"{src_dir}/{fname}", f"{dst_dir}/{fname}")
            copied += 1
    print(f"  {entity}: {copied} file(s) copied → {dst_dir}")

print("\nLanding data ready in DBFS.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 3 — Set environment variables

# COMMAND ----------

os.environ["ENVIRONMENT"]  = "dev"
os.environ["LANDING_PATH"] = f"{SPARK_ROOT}/landing"
os.environ["BRONZE_PATH"]  = f"{SPARK_ROOT}/bronze"
os.environ["SILVER_PATH"]  = f"{SPARK_ROOT}/silver"
os.environ["GOLD_PATH"]    = f"{SPARK_ROOT}/gold"
os.environ["BRONZE_DB"]    = "bronze"
os.environ["SILVER_DB"]    = "silver"
os.environ["GOLD_DB"]      = "gold"

for k in ("ENVIRONMENT", "LANDING_PATH", "BRONZE_PATH", "SILVER_PATH", "GOLD_PATH"):
    print(f"  {k} = {os.environ[k]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 4 — Bronze: ingest Landing Zone → Bronze Delta tables

# COMMAND ----------

from pipelines.bronze.ingest import run_all as bronze_run_all

bronze_results = bronze_run_all(spark=spark, project_root=repo_root)

for r in bronze_results:
    status = r["status"]
    source = r["source"]
    rows   = r["rows_written"]
    dur    = r["duration_seconds"]
    print(f"  [{status.upper()}] {source}: {rows} rows written in {dur:.1f}s")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 5 — Silver: transform Bronze → Silver (Snowflake Schema + SCD Type 2)

# COMMAND ----------

from pipelines.silver.transform import run as silver_run

silver_result = silver_run(spark=spark)
print(silver_result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 6 — Gold: build Star Schema, OBT, and KPI aggregations

# COMMAND ----------

from pipelines.gold.transform import run as gold_run

gold_result = gold_run(spark=spark)
print(gold_result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 7 — Verify: list output tables in DBFS

# COMMAND ----------

for layer in ("bronze", "silver", "gold"):
    path = f"{SPARK_ROOT}/{layer}"
    try:
        tables = [f.name for f in dbutils.fs.ls(path)]
        print(f"{layer}: {tables}")
    except Exception as e:
        print(f"{layer}: could not list ({e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 8 — Quick sanity check: row counts per Gold table

# COMMAND ----------

gold_tables = ["fact_sales", "dim_customer", "dim_product", "dim_store",
               "obt_sales", "agg_daily_sales", "agg_monthly_sales"]

for table in gold_tables:
    path = f"{SPARK_ROOT}/gold/{table}"
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"  gold/{table}: {count} rows")
    except Exception as e:
        print(f"  gold/{table}: not found ({e})")
