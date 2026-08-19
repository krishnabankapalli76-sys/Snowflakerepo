"""
Data Quality Validation Framework
==================================
Runs schema, completeness, uniqueness, referential integrity,
statistical range, and custom rule checks. Integrates with
Snowflake and sends alerts via SNS / Slack.

Author: Data Engineering Team
Version: 1.0.0
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional

logger = logging.getLogger("data_quality")


# ─────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────
class Severity(str, Enum):
    CRITICAL = "CRITICAL"   # Block pipeline, alert PagerDuty
    HIGH = "HIGH"           # Alert Slack engineering channel
    MEDIUM = "MEDIUM"       # Log warning, continue
    LOW = "LOW"             # Log info only


class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"
    ERROR = "ERROR"


# ─────────────────────────────────────────────
# Result Dataclasses
# ─────────────────────────────────────────────
@dataclass
class CheckResult:
    check_name: str
    check_type: str
    table_name: str
    column_name: Optional[str]
    status: CheckStatus
    severity: Severity
    expected: Any
    actual: Any
    message: str
    rows_affected: int = 0
    run_at: datetime = field(default_factory=datetime.utcnow)
    sql_used: str = None


@dataclass
class ValidationReport:
    table_name: str
    run_id: str
    checks_run: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    checks_warned: int = 0
    results: List[CheckResult] = field(default_factory=list)
    start_time: datetime = None
    end_time: datetime = None
    overall_status: CheckStatus = CheckStatus.PASS

    @property
    def pass_rate(self) -> float:
        if self.checks_run == 0:
            return 0.0
        return self.checks_passed / self.checks_run * 100

    def has_blocking_failures(self) -> bool:
        return any(
            r.status == CheckStatus.FAIL and r.severity == Severity.CRITICAL
            for r in self.results
        )


# ─────────────────────────────────────────────
# Base Check
# ─────────────────────────────────────────────
class DataQualityCheck(ABC):
    def __init__(self, name: str, severity: Severity = Severity.HIGH):
        self.name = name
        self.severity = severity

    @abstractmethod
    def run(self, conn, table: str, column: str = None) -> CheckResult:
        pass


# ─────────────────────────────────────────────
# Concrete Checks
# ─────────────────────────────────────────────
class NullCheck(DataQualityCheck):
    """Verify a column has no NULL values (or within threshold)."""

    def __init__(self, column: str, max_null_pct: float = 0.0, **kwargs):
        super().__init__(f"null_check_{column}", **kwargs)
        self.column = column
        self.max_null_pct = max_null_pct

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        col = column or self.column
        sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
            ROUND(100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS null_pct
        FROM {table}
        """
        rows = conn.execute(sql)
        total, nulls, null_pct = rows[0]
        status = (
            CheckStatus.PASS if null_pct <= self.max_null_pct else CheckStatus.FAIL
        )
        return CheckResult(
            check_name=self.name,
            check_type="NULL_CHECK",
            table_name=table,
            column_name=col,
            status=status,
            severity=self.severity,
            expected=f"null_pct <= {self.max_null_pct}%",
            actual=f"null_pct = {null_pct}%",
            message=f"{nulls}/{total} nulls in {col} ({null_pct}%)",
            rows_affected=int(nulls or 0),
            sql_used=sql,
        )


class UniquenessCheck(DataQualityCheck):
    """Verify no duplicate values in a column (or composite key)."""

    def __init__(self, columns: List[str], **kwargs):
        super().__init__(f"uniqueness_{'_'.join(columns)}", **kwargs)
        self.columns = columns

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        cols_str = ", ".join(self.columns)
        sql = f"""
        SELECT COUNT(*) AS dup_count
        FROM (
            SELECT {cols_str}, COUNT(*) AS cnt
            FROM {table}
            GROUP BY {cols_str}
            HAVING COUNT(*) > 1
        ) duplicates
        """
        rows = conn.execute(sql)
        dup_count = rows[0][0]
        status = CheckStatus.PASS if dup_count == 0 else CheckStatus.FAIL
        return CheckResult(
            check_name=self.name,
            check_type="UNIQUENESS_CHECK",
            table_name=table,
            column_name=cols_str,
            status=status,
            severity=self.severity,
            expected="0 duplicate rows",
            actual=f"{dup_count} duplicate group(s)",
            message=f"Found {dup_count} duplicate key group(s) for ({cols_str})",
            rows_affected=dup_count,
            sql_used=sql,
        )


class RowCountCheck(DataQualityCheck):
    """Ensure row count is within expected range."""

    def __init__(self, min_rows: int = 1, max_rows: int = None, **kwargs):
        super().__init__("row_count_check", **kwargs)
        self.min_rows = min_rows
        self.max_rows = max_rows

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        sql = f"SELECT COUNT(*) FROM {table}"
        rows = conn.execute(sql)
        actual_count = rows[0][0]

        passed = actual_count >= self.min_rows
        if self.max_rows and actual_count > self.max_rows:
            passed = False

        expected = f">= {self.min_rows}"
        if self.max_rows:
            expected += f" AND <= {self.max_rows}"

        return CheckResult(
            check_name=self.name,
            check_type="ROW_COUNT_CHECK",
            table_name=table,
            column_name=None,
            status=CheckStatus.PASS if passed else CheckStatus.FAIL,
            severity=self.severity,
            expected=expected,
            actual=str(actual_count),
            message=f"Table {table} has {actual_count:,} rows",
            rows_affected=actual_count,
            sql_used=sql,
        )


class RecencyCheck(DataQualityCheck):
    """Ensure the latest record is recent enough (SLA check)."""

    def __init__(self, timestamp_col: str, max_lag_hours: int = 25, **kwargs):
        super().__init__(f"recency_check_{timestamp_col}", **kwargs)
        self.timestamp_col = timestamp_col
        self.max_lag_hours = max_lag_hours

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        sql = f"""
        SELECT
            MAX({self.timestamp_col}) AS latest_ts,
            DATEDIFF('hour', MAX({self.timestamp_col}), CURRENT_TIMESTAMP()) AS lag_hours
        FROM {table}
        """
        rows = conn.execute(sql)
        latest_ts, lag_hours = rows[0]

        status = (
            CheckStatus.PASS
            if lag_hours is not None and lag_hours <= self.max_lag_hours
            else CheckStatus.FAIL
        )
        return CheckResult(
            check_name=self.name,
            check_type="RECENCY_CHECK",
            table_name=table,
            column_name=self.timestamp_col,
            status=status,
            severity=self.severity,
            expected=f"lag <= {self.max_lag_hours}h",
            actual=f"lag = {lag_hours}h, latest = {latest_ts}",
            message=f"Latest {self.timestamp_col} is {lag_hours}h old",
            sql_used=sql,
        )


class ReferentialIntegrityCheck(DataQualityCheck):
    """Verify FK relationships: all values in child exist in parent."""

    def __init__(
        self,
        child_col: str,
        parent_table: str,
        parent_col: str,
        **kwargs,
    ):
        super().__init__(f"ref_integrity_{child_col}", **kwargs)
        self.child_col = child_col
        self.parent_table = parent_table
        self.parent_col = parent_col

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) AS orphan_count
        FROM {table} child
        WHERE child.{self.child_col} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {self.parent_table} parent
              WHERE parent.{self.parent_col} = child.{self.child_col}
          )
        """
        rows = conn.execute(sql)
        orphans = rows[0][0]
        status = CheckStatus.PASS if orphans == 0 else CheckStatus.FAIL
        return CheckResult(
            check_name=self.name,
            check_type="REFERENTIAL_INTEGRITY",
            table_name=table,
            column_name=self.child_col,
            status=status,
            severity=self.severity,
            expected="0 orphan records",
            actual=f"{orphans} orphan(s)",
            message=(
                f"{orphans} records in {table}.{self.child_col} "
                f"have no match in {self.parent_table}.{self.parent_col}"
            ),
            rows_affected=orphans,
            sql_used=sql,
        )


class StatisticalRangeCheck(DataQualityCheck):
    """Flag numeric outliers beyond N standard deviations."""

    def __init__(self, column: str, n_stddev: float = 3.0, **kwargs):
        super().__init__(f"stat_range_{column}", **kwargs)
        self.column = column
        self.n_stddev = n_stddev

    def run(self, conn, table: str, column: str = None) -> CheckResult:
        col = column or self.column
        sql = f"""
        WITH stats AS (
            SELECT
                AVG({col}) AS mean_val,
                STDDEV({col}) AS stddev_val
            FROM {table}
        )
        SELECT COUNT(*) AS outlier_count
        FROM {table}, stats
        WHERE ABS({col} - mean_val) > {self.n_stddev} * stddev_val
        """
        rows = conn.execute(sql)
        outliers = rows[0][0]
        status = CheckStatus.WARN if outliers > 0 else CheckStatus.PASS
        return CheckResult(
            check_name=self.name,
            check_type="STATISTICAL_RANGE",
            table_name=table,
            column_name=col,
            status=status,
            severity=Severity.MEDIUM,
            expected=f"< {self.n_stddev} stddev outliers",
            actual=f"{outliers} outlier(s)",
            message=f"{outliers} rows in {col} beyond {self.n_stddev}σ",
            rows_affected=outliers,
            sql_used=sql,
        )


# ─────────────────────────────────────────────
# Validation Engine
# ─────────────────────────────────────────────
class DataQualityEngine:
    """Runs a suite of checks and produces a full validation report."""

    def __init__(self, conn, alert_config: Dict = None):
        self.conn = conn
        self.alert_config = alert_config or {}

    def run_suite(
        self, table: str, checks: List[DataQualityCheck]
    ) -> ValidationReport:
        run_id = f"DQ_{table}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        report = ValidationReport(
            table_name=table,
            run_id=run_id,
            start_time=datetime.utcnow(),
        )

        logger.info(f"[{run_id}] Running {len(checks)} DQ checks on {table}")

        for check in checks:
            try:
                result = check.run(self.conn, table)
                report.results.append(result)
                report.checks_run += 1

                if result.status == CheckStatus.PASS:
                    report.checks_passed += 1
                    logger.info(f"  ✓ {result.check_name}: PASS")
                elif result.status == CheckStatus.FAIL:
                    report.checks_failed += 1
                    logger.error(f"  ✗ {result.check_name}: FAIL — {result.message}")
                    if result.severity in (Severity.CRITICAL, Severity.HIGH):
                        self._send_alert(result, report)
                elif result.status == CheckStatus.WARN:
                    report.checks_warned += 1
                    logger.warning(f"  ⚠ {result.check_name}: WARN — {result.message}")

            except Exception as e:
                logger.error(f"  ✗ {check.name}: ERROR — {e}", exc_info=True)
                report.results.append(
                    CheckResult(
                        check_name=check.name,
                        check_type="UNKNOWN",
                        table_name=table,
                        column_name=None,
                        status=CheckStatus.ERROR,
                        severity=check.severity,
                        expected="N/A",
                        actual=str(e),
                        message=f"Check execution error: {e}",
                    )
                )
                report.checks_failed += 1

        report.end_time = datetime.utcnow()
        report.overall_status = (
            CheckStatus.FAIL if report.checks_failed > 0 else CheckStatus.PASS
        )
        self._persist_results(report)
        logger.info(
            f"[{run_id}] Done: {report.checks_passed}/{report.checks_run} passed "
            f"({report.pass_rate:.1f}%) | blocking={report.has_blocking_failures()}"
        )
        return report

    def _send_alert(self, result: CheckResult, report: ValidationReport):
        """Send alert via SNS or Slack webhook."""
        message = {
            "run_id": report.run_id,
            "table": result.table_name,
            "check": result.check_name,
            "severity": result.severity.value,
            "message": result.message,
            "expected": result.expected,
            "actual": result.actual,
        }

        # SNS
        sns_arn = self.alert_config.get("sns_arn")
        if sns_arn:
            import boto3
            boto3.client("sns").publish(
                TopicArn=sns_arn,
                Subject=f"[{result.severity.value}] DQ Failure: {result.table_name}",
                Message=json.dumps(message, indent=2),
            )

        # Slack
        webhook = self.alert_config.get("slack_webhook")
        if webhook:
            import urllib.request
            payload = {
                "text": (
                    f":red_circle: *DQ {result.severity.value}* | "
                    f"`{result.table_name}` | {result.check_name}\n"
                    f"> {result.message}"
                )
            }
            req = urllib.request.Request(
                webhook,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req)

    def _persist_results(self, report: ValidationReport):
        """Write DQ results to Snowflake audit table."""
        try:
            insert_sql = """
            INSERT INTO AUDIT_DB.DATA_QUALITY.CHECK_RESULTS
                (RUN_ID, TABLE_NAME, CHECK_NAME, CHECK_TYPE, COLUMN_NAME,
                 STATUS, SEVERITY, EXPECTED, ACTUAL, MESSAGE, ROWS_AFFECTED,
                 RUN_AT, SQL_USED)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = [
                (
                    report.run_id, r.table_name, r.check_name, r.check_type,
                    r.column_name, r.status.value, r.severity.value,
                    str(r.expected), str(r.actual), r.message, r.rows_affected,
                    r.run_at, r.sql_used,
                )
                for r in report.results
            ]
            self.conn.execute_many(insert_sql, params)
        except Exception as e:
            logger.warning(f"Could not persist DQ results: {e}")


# ─────────────────────────────────────────────
# Pre-built Check Suites
# ─────────────────────────────────────────────
def orders_check_suite() -> List[DataQualityCheck]:
    return [
        RowCountCheck(min_rows=1, severity=Severity.CRITICAL),
        NullCheck("ORDER_ID", max_null_pct=0.0, severity=Severity.CRITICAL),
        NullCheck("CUSTOMER_ID", max_null_pct=0.0, severity=Severity.CRITICAL),
        NullCheck("ORDER_DATE", max_null_pct=0.0, severity=Severity.HIGH),
        UniquenessCheck(["ORDER_ID"], severity=Severity.CRITICAL),
        RecencyCheck("UPDATED_AT", max_lag_hours=25, severity=Severity.HIGH),
        StatisticalRangeCheck("TOTAL_AMOUNT", n_stddev=4.0),
        ReferentialIntegrityCheck(
            "CUSTOMER_ID",
            "RAW_DB.ORACLE_RAW.CUSTOMERS",
            "CUSTOMER_ID",
            severity=Severity.HIGH,
        ),
    ]


def customers_check_suite() -> List[DataQualityCheck]:
    return [
        RowCountCheck(min_rows=1, severity=Severity.CRITICAL),
        NullCheck("CUSTOMER_ID", max_null_pct=0.0, severity=Severity.CRITICAL),
        NullCheck("EMAIL", max_null_pct=5.0, severity=Severity.MEDIUM),
        UniquenessCheck(["CUSTOMER_ID"], severity=Severity.CRITICAL),
        UniquenessCheck(["EMAIL"], severity=Severity.HIGH),
        RecencyCheck("UPDATED_AT", max_lag_hours=25),
    ]
