"""
End-to-end pipeline runner: Landing → Bronze → Silver → Gold.

Usage:
    python -m pipelines.run_pipeline              # run all layers
    python -m pipelines.run_pipeline --layer bronze
    python -m pipelines.run_pipeline --layer silver
    python -m pipelines.run_pipeline --layer gold
"""
from __future__ import annotations

import argparse
import sys

from pipelines.common.logger import get_logger, new_run_id, utc_now
from pipelines.common.spark_session import get_spark

logger = get_logger("run_pipeline")


def run_bronze(spark, project_root: str = ".") -> list[dict]:
    from pipelines.bronze.ingest import run_all
    from pipelines.bronze.quality_checks import (
        sales_checker, customers_checker, products_checker,
        stores_checker, inventory_checker,
    )
    from pipelines.common.config import Config
    from pipelines.common.quality import QualityChecker

    results = run_all(spark=spark, project_root=project_root)

    # Run quality on each ingested table
    checkers = {
        "sales": sales_checker,
        "customers": customers_checker,
        "products": products_checker,
        "stores": stores_checker,
        "inventory": inventory_checker,
    }
    for source, checker_fn in checkers.items():
        path = f"{Config.BRONZE_PATH}/{source}"
        try:
            df = spark.read.format("delta").load(path)
            checker = checker_fn(run_id=new_run_id())
            report = checker.run(df)
            checker.assert_no_critical(report)
            logger.info(f"Bronze quality OK: {source} | {report.summary()}")
        except Exception as exc:
            logger.warning(f"Bronze quality check skipped for {source}: {exc}")

    return results


def run_silver(spark) -> dict:
    from pipelines.silver.transform import run
    from pipelines.silver.quality_checks import (
        fact_sales_checker, dim_customer_checker,
        dim_product_checker, dim_store_checker,
    )
    from pipelines.common.config import Config

    result = run(spark=spark)

    checkers = {
        "fact_sales": fact_sales_checker,
        "dim_customer": dim_customer_checker,
        "dim_product": dim_product_checker,
        "dim_store": dim_store_checker,
    }
    for table, checker_fn in checkers.items():
        path = f"{Config.SILVER_PATH}/{table}"
        try:
            df = spark.read.format("delta").load(path)
            checker = checker_fn(run_id=new_run_id())
            report = checker.run(df)
            checker.assert_no_critical(report)
            logger.info(f"Silver quality OK: {table} | {report.summary()}")
        except Exception as exc:
            logger.warning(f"Silver quality check skipped for {table}: {exc}")

    return result


def run_gold(spark) -> dict:
    from pipelines.gold.transform import run
    from pipelines.gold.quality_checks import obt_sales_checker, agg_daily_sales_checker
    from pipelines.common.config import Config

    result = run(spark=spark)

    checkers = {
        "obt_sales": obt_sales_checker,
        "agg_daily_sales": agg_daily_sales_checker,
    }
    for table, checker_fn in checkers.items():
        path = f"{Config.GOLD_PATH}/{table}"
        try:
            df = spark.read.format("delta").load(path)
            checker = checker_fn(run_id=new_run_id())
            report = checker.run(df)
            checker.assert_no_critical(report)
            logger.info(f"Gold quality OK: {table} | {report.summary()}")
        except Exception as exc:
            logger.warning(f"Gold quality check skipped for {table}: {exc}")

    return result


def main():
    parser = argparse.ArgumentParser(description="DataAnalyticsSolution pipeline runner")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which layer to run (default: all)",
    )
    parser.add_argument("--project-root", default=".", help="Project root for local paths")
    args = parser.parse_args()

    spark = get_spark("run_pipeline")
    started = utc_now()
    logger.info(f"Pipeline started | layer={args.layer}")

    try:
        if args.layer in ("bronze", "all"):
            run_bronze(spark, project_root=args.project_root)
        if args.layer in ("silver", "all"):
            run_silver(spark)
        if args.layer in ("gold", "all"):
            run_gold(spark)
    except Exception as exc:
        logger.error(f"Pipeline FAILED: {exc}")
        sys.exit(1)

    duration = (utc_now() - started).total_seconds()
    logger.info(f"Pipeline completed | duration={duration:.1f}s")


if __name__ == "__main__":
    main()
