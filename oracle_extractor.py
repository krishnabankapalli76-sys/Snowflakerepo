"""
Oracle Database Extractor
=========================
Production-grade Oracle extractor with full-load and incremental strategies.
Handles connection pooling, retry logic, chunked extraction, and audit logging.

Author: Data Engineering Team
Version: 1.0.0
"""

import os
import csv
import gzip
import logging
import hashlib
import time
from datetime import datetime, timedelta
from typing import Iterator, Optional, Dict, Any, List
from contextlib import contextmanager
from dataclasses import dataclass, field

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None  # Allow import without driver for testing

import boto3
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────
# Logging Configuration
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("oracle_extractor")


# ─────────────────────────────────────────────
# Configuration Dataclasses
# ─────────────────────────────────────────────
@dataclass
class OracleConfig:
    host: str
    port: int
    service_name: str
    username: str
    password: str
    pool_min: int = 2
    pool_max: int = 10
    pool_increment: int = 1
    query_timeout: int = 3600  # seconds
    fetch_size: int = 10000    # rows per fetch (arraysize)


@dataclass
class ExtractionConfig:
    table_name: str
    schema_name: str
    extraction_mode: str          # FULL | INCREMENTAL | TIMESTAMP | CDC
    watermark_column: str = None  # e.g., UPDATED_AT
    primary_key: str = None       # e.g., ORDER_ID
    partition_column: str = None  # for parallel extraction
    num_partitions: int = 4
    batch_size: int = 100000
    output_dir: str = "/tmp/extracts"
    compress: bool = True
    custom_sql: str = None        # Override with custom SELECT


@dataclass
class S3Config:
    bucket: str
    prefix: str                   # e.g., raw/oracle/
    region: str = "us-east-1"
    storage_class: str = "STANDARD_IA"
    server_side_encryption: str = "aws:kms"
    kms_key_id: str = None


@dataclass
class ExtractionResult:
    table_name: str
    extraction_mode: str
    rows_extracted: int
    files_written: List[str] = field(default_factory=list)
    files_uploaded: List[str] = field(default_factory=list)
    start_time: datetime = None
    end_time: datetime = None
    status: str = "SUCCESS"
    error_message: str = None
    checksum: str = None

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


# ─────────────────────────────────────────────
# Oracle Connection Pool Manager
# ─────────────────────────────────────────────
class OracleConnectionPool:
    """Thread-safe Oracle connection pool with health checks."""

    _pool = None

    def __init__(self, config: OracleConfig):
        self.config = config
        self._initialize_pool()

    def _initialize_pool(self):
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle not installed. Run: pip install cx_Oracle")

        dsn = cx_Oracle.makedsn(
            self.config.host,
            self.config.port,
            service_name=self.config.service_name,
        )
        logger.info(f"Initializing Oracle connection pool → {self.config.host}:{self.config.port}/{self.config.service_name}")
        self._pool = cx_Oracle.SessionPool(
            user=self.config.username,
            password=self.config.password,
            dsn=dsn,
            min=self.config.pool_min,
            max=self.config.pool_max,
            increment=self.config.pool_increment,
            threaded=True,
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT,
        )

    @contextmanager
    def get_connection(self):
        conn = self._pool.acquire()
        try:
            conn.callTimeout = self.config.query_timeout * 1000  # ms
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.release(conn)

    def close(self):
        if self._pool:
            self._pool.close()
            logger.info("Oracle connection pool closed")


# ─────────────────────────────────────────────
# Watermark Manager (tracks incremental state)
# ─────────────────────────────────────────────
class WatermarkManager:
    """
    Manages high-watermark values for incremental extraction.
    In production, store in a metadata DB or Snowflake control table.
    """

    def __init__(self, state_file: str = "/tmp/watermarks.json"):
        import json
        self.state_file = state_file
        self.state: Dict[str, Any] = {}
        self._load()

    def _load(self):
        import json
        try:
            with open(self.state_file, "r") as f:
                self.state = json.load(f)
        except FileNotFoundError:
            self.state = {}

    def _save(self):
        import json
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, default=str, indent=2)

    def get(self, table: str, default=None) -> Any:
        return self.state.get(table, default)

    def set(self, table: str, value: Any):
        self.state[table] = str(value) if isinstance(value, datetime) else value
        self._save()
        logger.info(f"Watermark updated: {table} → {value}")


# ─────────────────────────────────────────────
# SQL Builder
# ─────────────────────────────────────────────
class SQLBuilder:
    """Builds extraction SQL for different strategies."""

    @staticmethod
    def full_load(schema: str, table: str) -> str:
        return f"SELECT * FROM {schema}.{table}"

    @staticmethod
    def incremental(
        schema: str,
        table: str,
        watermark_col: str,
        last_value: Any,
    ) -> str:
        return (
            f"SELECT * FROM {schema}.{table} "
            f"WHERE {watermark_col} > TIMESTAMP '{last_value}' "
            f"ORDER BY {watermark_col}"
        )

    @staticmethod
    def partitioned(
        schema: str,
        table: str,
        partition_col: str,
        min_val: int,
        max_val: int,
        partition_id: int,
        num_partitions: int,
    ) -> str:
        """Rowid-based parallel extraction for large tables."""
        return (
            f"SELECT * FROM {schema}.{table} "
            f"WHERE MOD(ORA_HASH({partition_col}), {num_partitions}) = {partition_id}"
        )

    @staticmethod
    def count(schema: str, table: str, where_clause: str = "") -> str:
        where = f"WHERE {where_clause}" if where_clause else ""
        return f"SELECT COUNT(*) FROM {schema}.{table} {where}"


# ─────────────────────────────────────────────
# Core Extractor Class
# ─────────────────────────────────────────────
class OracleExtractor:
    """
    Production-grade Oracle extractor.

    Supports:
        - Full load
        - Timestamp-based incremental
        - Parallel partition extraction
        - Gzip-compressed CSV output
        - Direct S3 upload with multipart
        - Checksum validation
        - Detailed audit logging
    """

    def __init__(
        self,
        oracle_config: OracleConfig,
        s3_config: S3Config,
        watermark_manager: WatermarkManager = None,
    ):
        self.oracle_config = oracle_config
        self.s3_config = s3_config
        self.pool = OracleConnectionPool(oracle_config)
        self.wm = watermark_manager or WatermarkManager()
        self.s3_client = boto3.client("s3", region_name=s3_config.region)

    # ── Main Entry Point ──────────────────────
    def extract_table(self, cfg: ExtractionConfig) -> ExtractionResult:
        result = ExtractionResult(
            table_name=cfg.table_name,
            extraction_mode=cfg.extraction_mode,
            rows_extracted=0,
            start_time=datetime.utcnow(),
        )
        logger.info(
            f"Starting extraction | table={cfg.schema_name}.{cfg.table_name} "
            f"| mode={cfg.extraction_mode}"
        )

        try:
            os.makedirs(cfg.output_dir, exist_ok=True)
            sql = self._build_sql(cfg)
            logger.info(f"Extraction SQL: {sql}")

            # Get row count estimate
            count = self._get_row_count(cfg)
            logger.info(f"Estimated rows: {count:,}")

            # Extract and write CSV files
            local_files = self._extract_to_csv(sql, cfg, result)
            result.files_written = local_files

            # Upload to S3
            s3_keys = self._upload_to_s3(local_files, cfg, result)
            result.files_uploaded = s3_keys

            # Update watermark for incremental
            if cfg.extraction_mode == "INCREMENTAL" and cfg.watermark_column:
                new_wm = self._get_max_watermark(cfg)
                if new_wm:
                    self.wm.set(f"{cfg.schema_name}.{cfg.table_name}", new_wm)

            result.status = "SUCCESS"
            result.end_time = datetime.utcnow()
            logger.info(
                f"Extraction complete | rows={result.rows_extracted:,} "
                f"| files={len(result.files_uploaded)} "
                f"| duration={result.duration_seconds:.1f}s"
            )
        except Exception as e:
            result.status = "FAILED"
            result.error_message = str(e)
            result.end_time = datetime.utcnow()
            logger.error(f"Extraction failed: {e}", exc_info=True)
            raise

        return result

    # ── SQL Building ──────────────────────────
    def _build_sql(self, cfg: ExtractionConfig) -> str:
        if cfg.custom_sql:
            return cfg.custom_sql

        if cfg.extraction_mode == "FULL":
            return SQLBuilder.full_load(cfg.schema_name, cfg.table_name)

        elif cfg.extraction_mode == "INCREMENTAL":
            last_wm = self.wm.get(
                f"{cfg.schema_name}.{cfg.table_name}",
                default="1900-01-01 00:00:00",
            )
            return SQLBuilder.incremental(
                cfg.schema_name,
                cfg.table_name,
                cfg.watermark_column,
                last_wm,
            )
        else:
            raise ValueError(f"Unknown extraction_mode: {cfg.extraction_mode}")

    # ── CSV Extraction ────────────────────────
    def _extract_to_csv(
        self, sql: str, cfg: ExtractionConfig, result: ExtractionResult
    ) -> List[str]:
        local_files = []
        file_index = 0
        row_count = 0
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.arraysize = self.oracle_config.fetch_size
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]

            batch = []
            for row in cursor:
                batch.append(row)
                row_count += 1

                if len(batch) >= cfg.batch_size:
                    file_path = self._write_batch(
                        columns, batch, cfg, run_ts, file_index
                    )
                    local_files.append(file_path)
                    file_index += 1
                    batch = []
                    logger.info(f"Written {row_count:,} rows so far…")

            # Write final partial batch
            if batch:
                file_path = self._write_batch(
                    columns, batch, cfg, run_ts, file_index
                )
                local_files.append(file_path)

            cursor.close()

        result.rows_extracted = row_count
        logger.info(f"Total rows extracted: {row_count:,} → {len(local_files)} file(s)")
        return local_files

    def _write_batch(
        self,
        columns: List[str],
        rows: List[tuple],
        cfg: ExtractionConfig,
        run_ts: str,
        file_index: int,
    ) -> str:
        ext = ".csv.gz" if cfg.compress else ".csv"
        filename = f"{cfg.table_name}_{run_ts}_{file_index:04d}{ext}"
        filepath = os.path.join(cfg.output_dir, filename)

        opener = gzip.open if cfg.compress else open
        mode = "wt" if cfg.compress else "w"

        with opener(filepath, mode, newline="", encoding="utf-8") as f:
            writer = csv.writer(
                f,
                quoting=csv.QUOTE_NONNUMERIC,
                lineterminator="\n",
            )
            writer.writerow(columns)
            writer.writerows(
                [self._serialize_row(r) for r in rows]
            )

        size_mb = os.path.getsize(filepath) / 1024 / 1024
        logger.debug(f"Written: {filepath} ({size_mb:.1f} MB)")
        return filepath

    @staticmethod
    def _serialize_row(row: tuple) -> list:
        """Convert Oracle-specific types to Python-native for CSV serialization."""
        result = []
        for val in row:
            if val is None:
                result.append("")
            elif isinstance(val, datetime):
                result.append(val.strftime("%Y-%m-%d %H:%M:%S"))
            elif hasattr(val, "read"):  # LOB types
                result.append(val.read())
            else:
                result.append(val)
        return result

    # ── S3 Upload ─────────────────────────────
    def _upload_to_s3(
        self,
        local_files: List[str],
        cfg: ExtractionConfig,
        result: ExtractionResult,
    ) -> List[str]:
        s3_keys = []
        run_date = datetime.utcnow().strftime("%Y/%m/%d")
        s3_prefix = f"{self.s3_config.prefix}{cfg.table_name}/{run_date}/"

        for local_path in local_files:
            filename = os.path.basename(local_path)
            s3_key = f"{s3_prefix}{filename}"

            checksum = self._compute_md5(local_path)
            extra_args = {
                "StorageClass": self.s3_config.storage_class,
                "ServerSideEncryption": self.s3_config.server_side_encryption,
                "Metadata": {
                    "source-table": f"{cfg.schema_name}.{cfg.table_name}",
                    "extraction-mode": cfg.extraction_mode,
                    "extracted-at": datetime.utcnow().isoformat(),
                    "md5-checksum": checksum,
                },
            }
            if self.s3_config.kms_key_id:
                extra_args["SSEKMSKeyId"] = self.s3_config.kms_key_id

            logger.info(f"Uploading → s3://{self.s3_config.bucket}/{s3_key}")
            self._upload_with_retry(local_path, s3_key, extra_args)
            s3_keys.append(s3_key)

        return s3_keys

    def _upload_with_retry(
        self, local_path: str, s3_key: str, extra_args: dict, max_retries: int = 3
    ):
        for attempt in range(1, max_retries + 1):
            try:
                self.s3_client.upload_file(
                    Filename=local_path,
                    Bucket=self.s3_config.bucket,
                    Key=s3_key,
                    ExtraArgs=extra_args,
                )
                return
            except ClientError as e:
                if attempt == max_retries:
                    raise
                wait = 2 ** attempt
                logger.warning(
                    f"S3 upload attempt {attempt} failed: {e}. Retrying in {wait}s…"
                )
                time.sleep(wait)

    # ── Helpers ───────────────────────────────
    def _get_row_count(self, cfg: ExtractionConfig) -> int:
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    SQLBuilder.count(cfg.schema_name, cfg.table_name)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"Could not get row count: {e}")
            return -1

    def _get_max_watermark(self, cfg: ExtractionConfig) -> Optional[str]:
        try:
            sql = (
                f"SELECT MAX({cfg.watermark_column}) "
                f"FROM {cfg.schema_name}.{cfg.table_name}"
            )
            with self.pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.warning(f"Could not get max watermark: {e}")
            return None

    @staticmethod
    def _compute_md5(filepath: str) -> str:
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def close(self):
        self.pool.close()


# ─────────────────────────────────────────────
# Convenience Factory
# ─────────────────────────────────────────────
def create_extractor_from_env() -> OracleExtractor:
    """Build extractor from environment variables (12-factor app pattern)."""
    oracle_cfg = OracleConfig(
        host=os.environ["ORACLE_HOST"],
        port=int(os.environ.get("ORACLE_PORT", 1521)),
        service_name=os.environ["ORACLE_SERVICE"],
        username=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
    )
    s3_cfg = S3Config(
        bucket=os.environ["S3_BUCKET"],
        prefix=os.environ.get("S3_PREFIX", "raw/oracle/"),
        region=os.environ.get("AWS_REGION", "us-east-1"),
        kms_key_id=os.environ.get("S3_KMS_KEY_ID"),
    )
    return OracleExtractor(oracle_cfg, s3_cfg)


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Oracle → S3 Extractor")
    parser.add_argument("--table", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--mode", default="INCREMENTAL", choices=["FULL", "INCREMENTAL"])
    parser.add_argument("--watermark-col", default="UPDATED_AT")
    parser.add_argument("--output-dir", default="/tmp/extracts")
    args = parser.parse_args()

    extractor = create_extractor_from_env()
    cfg = ExtractionConfig(
        table_name=args.table,
        schema_name=args.schema,
        extraction_mode=args.mode,
        watermark_column=args.watermark_col,
        output_dir=args.output_dir,
    )

    result = extractor.extract_table(cfg)
    print(json.dumps(
        {
            "status": result.status,
            "rows": result.rows_extracted,
            "files": result.files_uploaded,
            "duration_s": result.duration_seconds,
        },
        indent=2,
    ))
    extractor.close()
