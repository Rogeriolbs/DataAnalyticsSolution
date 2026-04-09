"""Schema contract validation and drift detection."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from pipelines.common.logger import get_logger

logger = get_logger(__name__)

# Map JSON Schema primitive types to PySpark types
_JSON_TO_SPARK: dict[str, T.DataType] = {
    "string": T.StringType(),
    "integer": T.LongType(),
    "number": T.DoubleType(),
    "boolean": T.BooleanType(),
}


def load_schema_contract(schema_path: str | Path) -> dict[str, Any]:
    with open(schema_path) as f:
        return json.load(f)


def validate_schema_drift(df: DataFrame, contract: dict[str, Any], source: str) -> list[str]:
    """
    Compare DataFrame columns against the schema contract.

    Returns a list of drift messages (empty = no drift).
    Raises RuntimeError on breaking changes (missing required columns).
    """
    required: set[str] = set(contract.get("required", []))
    contract_cols: set[str] = set(contract.get("properties", {}).keys())
    df_cols: set[str] = set(df.columns)

    issues: list[str] = []

    # Missing required columns → breaking
    missing_required = required - df_cols
    if missing_required:
        raise RuntimeError(
            f"[{source}] Breaking schema change — missing required columns: {missing_required}"
        )

    # New columns not in contract → additive drift (warn only)
    new_cols = df_cols - contract_cols
    if new_cols:
        msg = f"[{source}] Schema drift detected — new columns (additive): {new_cols}"
        logger.warning(msg)
        issues.append(msg)

    # Dropped optional columns → soft warning
    dropped_optional = (contract_cols - required) - df_cols
    if dropped_optional:
        msg = f"[{source}] Schema drift detected — missing optional columns: {dropped_optional}"
        logger.warning(msg)
        issues.append(msg)

    return issues
