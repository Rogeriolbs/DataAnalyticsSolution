"""
Data quality framework — expectation-based validation for each pipeline layer.

Usage:
    from pipelines.common.quality import QualityChecker, Severity

    checker = QualityChecker(spark, source="bronze.sales", run_id="abc")
    checker.expect_no_nulls("transaction_id")
    checker.expect_unique("transaction_id")
    checker.expect_values_in_set("channel", {"in_store", "online", "mobile"})
    checker.expect_column_min("quantity", 1)
    report = checker.run(df)
    checker.assert_no_critical(report)  # raises if any CRITICAL rule failed
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.logger import get_logger, utc_now

logger = get_logger(__name__)


class Severity(str, Enum):
    CRITICAL = "CRITICAL"  # pipeline halts on failure
    WARNING = "WARNING"    # logged but pipeline continues


@dataclass
class QualityRule:
    name: str
    severity: Severity
    check_fn: Callable[[DataFrame], bool]
    description: str


@dataclass
class RuleResult:
    rule_name: str
    severity: Severity
    passed: bool
    description: str
    failed_count: int = 0
    details: str = ""


@dataclass
class QualityReport:
    source: str
    run_id: str
    layer: str
    checked_at: str
    total_rows: int
    results: list[RuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == Severity.CRITICAL)

    @property
    def critical_failures(self) -> list[RuleResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.CRITICAL]

    @property
    def warnings(self) -> list[RuleResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.WARNING]

    def summary(self) -> dict:
        return {
            "source": self.source,
            "run_id": self.run_id,
            "layer": self.layer,
            "checked_at": self.checked_at,
            "total_rows": self.total_rows,
            "total_rules": len(self.results),
            "passed_rules": sum(1 for r in self.results if r.passed),
            "critical_failures": len(self.critical_failures),
            "warnings": len(self.warnings),
            "overall_passed": self.passed,
        }


class QualityChecker:
    """Fluent API for registering and running data quality checks."""

    def __init__(self, source: str, run_id: str, layer: str = "bronze"):
        self.source = source
        self.run_id = run_id
        self.layer = layer
        self._rules: list[QualityRule] = []

    # ------------------------------------------------------------------
    # Rule registration
    # ------------------------------------------------------------------

    def expect_no_nulls(
        self, column: str, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            count = df.filter(F.col(column).isNull()).count()
            return count == 0, count

        self._rules.append(QualityRule(
            name=f"no_nulls:{column}",
            severity=severity,
            check_fn=check,
            description=f"Column '{column}' must have no null values",
        ))
        return self

    def expect_unique(
        self, column: str, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            total = df.count()
            distinct = df.select(column).dropDuplicates().count()
            dupes = total - distinct
            return dupes == 0, dupes

        self._rules.append(QualityRule(
            name=f"unique:{column}",
            severity=severity,
            check_fn=check,
            description=f"Column '{column}' must have unique values",
        ))
        return self

    def expect_values_in_set(
        self, column: str, valid_values: set, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            count = df.filter(~F.col(column).isin(list(valid_values))).count()
            return count == 0, count

        self._rules.append(QualityRule(
            name=f"values_in_set:{column}",
            severity=severity,
            check_fn=check,
            description=f"Column '{column}' must only contain: {valid_values}",
        ))
        return self

    def expect_column_min(
        self, column: str, min_value: Any, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            count = df.filter(F.col(column) < min_value).count()
            return count == 0, count

        self._rules.append(QualityRule(
            name=f"min_value:{column}>={min_value}",
            severity=severity,
            check_fn=check,
            description=f"Column '{column}' must be >= {min_value}",
        ))
        return self

    def expect_column_max(
        self, column: str, max_value: Any, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            count = df.filter(F.col(column) > max_value).count()
            return count == 0, count

        self._rules.append(QualityRule(
            name=f"max_value:{column}<={max_value}",
            severity=severity,
            check_fn=check,
            description=f"Column '{column}' must be <= {max_value}",
        ))
        return self

    def expect_row_count_min(
        self, min_rows: int, severity: Severity = Severity.CRITICAL
    ) -> "QualityChecker":
        def check(df: DataFrame) -> tuple[bool, int]:
            count = df.count()
            failed = max(0, min_rows - count)
            return count >= min_rows, failed

        self._rules.append(QualityRule(
            name=f"row_count_min:{min_rows}",
            severity=severity,
            check_fn=check,
            description=f"DataFrame must have at least {min_rows} rows",
        ))
        return self

    def expect_referential_integrity(
        self,
        column: str,
        ref_df: DataFrame,
        ref_column: str,
        severity: Severity = Severity.WARNING,
    ) -> "QualityChecker":
        """All non-null values in `column` must exist in `ref_df.ref_column`."""
        def check(df: DataFrame) -> tuple[bool, int]:
            ref_vals = ref_df.select(ref_column).dropna()
            orphans = (
                df.filter(F.col(column).isNotNull())
                .join(ref_vals, df[column] == ref_vals[ref_column], how="left_anti")
                .count()
            )
            return orphans == 0, orphans

        self._rules.append(QualityRule(
            name=f"ref_integrity:{column}->{ref_column}",
            severity=severity,
            check_fn=check,
            description=f"'{column}' must reference existing '{ref_column}' values",
        ))
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, df: DataFrame) -> QualityReport:
        total_rows = df.count()
        checked_at = utc_now().isoformat()
        report = QualityReport(
            source=self.source,
            run_id=self.run_id,
            layer=self.layer,
            checked_at=checked_at,
            total_rows=total_rows,
        )

        for rule in self._rules:
            try:
                passed, failed_count = rule.check_fn(df)
                result = RuleResult(
                    rule_name=rule.name,
                    severity=rule.severity,
                    passed=passed,
                    description=rule.description,
                    failed_count=failed_count,
                )
                level = "INFO" if passed else ("ERROR" if rule.severity == Severity.CRITICAL else "WARNING")
                logger.log(
                    getattr(__import__("logging"), level),
                    f"[{self.source}] Rule '{rule.name}': {'PASS' if passed else 'FAIL'} "
                    f"(failed_rows={failed_count})",
                )
            except Exception as exc:
                result = RuleResult(
                    rule_name=rule.name,
                    severity=rule.severity,
                    passed=False,
                    description=rule.description,
                    details=str(exc),
                )
                logger.error(f"[{self.source}] Rule '{rule.name}' ERROR: {exc}")

            report.results.append(result)

        return report

    def assert_no_critical(self, report: QualityReport) -> None:
        """Raise QualityError if any CRITICAL rule failed."""
        if not report.passed:
            failures = [r.rule_name for r in report.critical_failures]
            raise QualityError(
                f"[{self.source}] Quality check FAILED — critical rules: {failures}"
            )


class QualityError(Exception):
    """Raised when a CRITICAL quality rule fails."""
