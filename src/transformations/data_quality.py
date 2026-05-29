"""
Data Quality validation module.

Runs automated checks at each pipeline layer to ensure:
  - No unexpected nulls in critical columns
  - No schema drift
  - Acceptable duplicate ratios
  - Value range validations

Azure equivalent: Azure Data Factory data flow validations /
                  Azure Purview data quality rules.
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QualityCheck:
    """Result of a single data quality check."""
    check_name: str
    column: str
    expected: str
    actual: str
    passed: bool
    severity: str  # "error" | "warning"
    details: Optional[str] = None


@dataclass
class QualityReport:
    """Aggregated report of all quality checks for a table."""
    table_name: str
    layer: str
    run_timestamp: str
    total_rows: int
    checks: List[QualityCheck]
    passed: bool

    @property
    def failed_checks(self) -> List[QualityCheck]:
        return [c for c in self.checks if not c.passed]

    @property
    def error_count(self) -> int:
        return sum(1 for c in self.failed_checks if c.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for c in self.failed_checks if c.severity == "warning")

    def to_json(self) -> str:
        """Serialize the report to JSON string."""
        data = {
            "table_name": self.table_name,
            "layer": self.layer,
            "run_timestamp": self.run_timestamp,
            "total_rows": self.total_rows,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "checks": [asdict(c) for c in self.checks],
        }
        return json.dumps(data, indent=2)


class DataQualityValidator:
    """
    Runs data quality checks on a Spark DataFrame.

    Checks performed:
    1. Null ratio per column (configurable threshold)
    2. Duplicate primary keys
    3. Row count minimum
    4. Numeric range validation
    5. Schema completeness
    """

    def __init__(self):
        self.config = get_config()
        self.dq_config = self.config["data_quality"]

    def check_null_ratio(
        self,
        df: DataFrame,
        column: str,
        max_ratio: float = None,
        severity: str = "error",
    ) -> QualityCheck:
        """
        Checks that the null ratio in a column is below the threshold.

        Args:
            df: DataFrame to check.
            column: Column name.
            max_ratio: Maximum acceptable null ratio (0.0–1.0).
            severity: 'error' fails the pipeline; 'warning' logs only.

        Returns:
            QualityCheck result.
        """
        if max_ratio is None:
            max_ratio = self.dq_config["max_null_ratio"]

        total = df.count()
        if total == 0:
            return QualityCheck(
                check_name="null_ratio",
                column=column,
                expected=f"<= {max_ratio:.1%}",
                actual="N/A (empty DataFrame)",
                passed=False,
                severity=severity,
                details="DataFrame is empty",
            )

        null_count = df.filter(F.col(column).isNull()).count()
        actual_ratio = null_count / total

        passed = actual_ratio <= max_ratio
        result = QualityCheck(
            check_name="null_ratio",
            column=column,
            expected=f"<= {max_ratio:.1%}",
            actual=f"{actual_ratio:.1%} ({null_count:,}/{total:,} nulls)",
            passed=passed,
            severity=severity,
            details=f"Found {null_count:,} null values out of {total:,} rows",
        )

        if not passed:
            log_fn = logger.error if severity == "error" else logger.warning
            log_fn(
                f"❌ DQ Check FAILED | null_ratio | column={column} | "
                f"expected<={max_ratio:.1%} | actual={actual_ratio:.1%}"
            )
        else:
            logger.debug(f"✅ DQ Check PASSED | null_ratio | column={column} | ratio={actual_ratio:.1%}")

        return result

    def check_duplicates(
        self,
        df: DataFrame,
        primary_key: List[str],
        max_ratio: float = None,
        severity: str = "error",
    ) -> QualityCheck:
        """
        Checks that duplicate rows (by primary key) are within acceptable limits.

        Args:
            df: DataFrame to check.
            primary_key: List of columns forming the primary key.
            max_ratio: Maximum acceptable duplicate ratio.
            severity: Severity level.

        Returns:
            QualityCheck result.
        """
        if max_ratio is None:
            max_ratio = self.dq_config["max_duplicate_ratio"]

        total = df.count()
        distinct = df.dropDuplicates(primary_key).count()
        duplicate_count = total - distinct
        actual_ratio = duplicate_count / total if total > 0 else 0

        passed = actual_ratio <= max_ratio
        pk_str = ", ".join(primary_key)
        result = QualityCheck(
            check_name="duplicate_ratio",
            column=pk_str,
            expected=f"<= {max_ratio:.1%}",
            actual=f"{actual_ratio:.1%} ({duplicate_count:,} duplicates)",
            passed=passed,
            severity=severity,
            details=f"Found {duplicate_count:,} duplicate rows on key [{pk_str}]",
        )

        if not passed:
            log_fn = logger.error if severity == "error" else logger.warning
            log_fn(f"❌ DQ Check FAILED | duplicates | pk={pk_str} | ratio={actual_ratio:.1%}")
        else:
            logger.debug(f"✅ DQ Check PASSED | duplicates | pk={pk_str} | ratio={actual_ratio:.1%}")

        return result

    def check_min_row_count(
        self,
        df: DataFrame,
        min_rows: int = None,
        severity: str = "error",
    ) -> QualityCheck:
        """
        Verifies that the DataFrame has at least min_rows rows.

        Args:
            df: DataFrame to check.
            min_rows: Minimum expected row count.
            severity: Severity level.

        Returns:
            QualityCheck result.
        """
        if min_rows is None:
            min_rows = self.dq_config["min_row_count"]

        actual_count = df.count()
        passed = actual_count >= min_rows

        result = QualityCheck(
            check_name="min_row_count",
            column="*",
            expected=f">= {min_rows:,} rows",
            actual=f"{actual_count:,} rows",
            passed=passed,
            severity=severity,
        )

        if not passed:
            log_fn = logger.error if severity == "error" else logger.warning
            log_fn(f"❌ DQ Check FAILED | min_row_count | expected>={min_rows:,} | actual={actual_count:,}")

        return result

    def check_value_range(
        self,
        df: DataFrame,
        column: str,
        min_val: float = None,
        max_val: float = None,
        severity: str = "warning",
    ) -> QualityCheck:
        """
        Validates that numeric column values fall within expected ranges.

        Args:
            df: DataFrame to check.
            column: Numeric column name.
            min_val: Minimum expected value (inclusive).
            max_val: Maximum expected value (inclusive).
            severity: Severity level.

        Returns:
            QualityCheck result.
        """
        conditions = []
        if min_val is not None:
            conditions.append(F.col(column) < min_val)
        if max_val is not None:
            conditions.append(F.col(column) > max_val)

        if not conditions:
            return QualityCheck(
                check_name="value_range",
                column=column,
                expected="No range defined",
                actual="N/A",
                passed=True,
                severity=severity,
            )

        out_of_range = df.filter(
            F.col(column).isNotNull() & (conditions[0] if len(conditions) == 1
             else conditions[0] | conditions[1])
        ).count()

        passed = out_of_range == 0
        range_str = f"[{min_val}, {max_val}]"

        result = QualityCheck(
            check_name="value_range",
            column=column,
            expected=f"All values in {range_str}",
            actual=f"{out_of_range:,} out-of-range values",
            passed=passed,
            severity=severity,
            details=f"Found {out_of_range:,} values outside {range_str}",
        )

        if not passed:
            logger.warning(f"⚠️  DQ Check FAILED | value_range | column={column} | out_of_range={out_of_range:,}")

        return result

    def validate_orders(self, df: DataFrame) -> QualityReport:
        """
        Runs the full quality check suite on the orders Silver table.

        Args:
            df: Orders enriched DataFrame.

        Returns:
            QualityReport with all check results.
        """
        logger.info("Running data quality checks on orders_enriched...")
        checks = []

        # Critical checks (errors stop the pipeline)
        checks.append(self.check_min_row_count(df, min_rows=1000, severity="error"))
        checks.append(self.check_duplicates(df, ["order_id"], severity="error"))
        checks.append(self.check_null_ratio(df, "order_id", max_ratio=0.0, severity="error"))
        checks.append(self.check_null_ratio(df, "customer_id", max_ratio=0.0, severity="error"))
        checks.append(self.check_null_ratio(df, "order_status", max_ratio=0.01, severity="error"))

        # Warning checks (logged but don't stop the pipeline)
        checks.append(self.check_null_ratio(df, "total_payment_value", max_ratio=0.05, severity="warning"))
        checks.append(self.check_value_range(df, "total_payment_value", min_val=0, max_val=100_000, severity="warning"))
        checks.append(self.check_null_ratio(df, "customer_unique_id", max_ratio=0.02, severity="warning"))

        passed = all(c.passed for c in checks if c.severity == "error")
        report = QualityReport(
            table_name="orders_enriched",
            layer="silver",
            run_timestamp=datetime.utcnow().isoformat(),
            total_rows=df.count(),
            checks=checks,
            passed=passed,
        )

        self._log_report_summary(report)
        return report

    def validate_rfm(self, df: DataFrame) -> QualityReport:
        """Runs quality checks on the customer_rfm Gold table."""
        checks = []
        checks.append(self.check_min_row_count(df, min_rows=100))
        checks.append(self.check_duplicates(df, ["customer_unique_id"]))
        checks.append(self.check_null_ratio(df, "rfm_score", max_ratio=0.0))
        checks.append(self.check_value_range(df, "r_score", min_val=1, max_val=5))
        checks.append(self.check_value_range(df, "f_score", min_val=1, max_val=5))
        checks.append(self.check_value_range(df, "m_score", min_val=1, max_val=5))

        passed = all(c.passed for c in checks if c.severity == "error")
        report = QualityReport(
            table_name="customer_rfm",
            layer="gold",
            run_timestamp=datetime.utcnow().isoformat(),
            total_rows=df.count(),
            checks=checks,
            passed=passed,
        )
        self._log_report_summary(report)
        return report

    def _log_report_summary(self, report: QualityReport) -> None:
        """Logs a summary of the quality report."""
        status = "✅ PASSED" if report.passed else "❌ FAILED"
        logger.info(
            f"DQ Report [{report.table_name}] | {status} | "
            f"Rows: {report.total_rows:,} | "
            f"Checks: {len(report.checks)} | "
            f"Errors: {report.error_count} | "
            f"Warnings: {report.warning_count}"
        )
        for check in report.failed_checks:
            level = logger.error if check.severity == "error" else logger.warning
            level(f"  → [{check.severity.upper()}] {check.check_name} on {check.column}: {check.actual}")
