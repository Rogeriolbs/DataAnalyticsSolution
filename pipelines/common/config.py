"""Central configuration loaded from environment variables."""
import os


class Config:
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "dev")

    # Storage root paths
    LANDING_PATH: str = os.getenv("LANDING_PATH", "/mnt/landing")
    BRONZE_PATH: str = os.getenv("BRONZE_PATH", "/mnt/bronze")
    SILVER_PATH: str = os.getenv("SILVER_PATH", "/mnt/silver")
    GOLD_PATH: str = os.getenv("GOLD_PATH", "/mnt/gold")

    # Catalog / database names
    BRONZE_DB: str = os.getenv("BRONZE_DB", "bronze")
    SILVER_DB: str = os.getenv("SILVER_DB", "silver")
    GOLD_DB: str = os.getenv("GOLD_DB", "gold")

    # Quality log table
    QUALITY_LOG_TABLE: str = f"{BRONZE_DB}._quality_log"
    PIPELINE_RUNS_TABLE: str = f"{BRONZE_DB}._pipeline_runs"
