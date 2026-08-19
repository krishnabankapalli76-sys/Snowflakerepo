"""
Snowflake Loader — S3 → Snowflake COPY INTO
============================================
Handles: stage creation, COPY INTO, deduplication,
         merge (upsert), audit logging, error recovery.

Author: Data Engineering Team
Version: 1.0.0
"""

import os
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Any

import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError

logger = logging.getLogger("snowflake_loader")


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str = None
    private_key_path: str = None
    warehouse: str = "LOAD_WH"
    database: str = "RAW_DB"
    schema: str = "ORACLE_RAW"
    role: str = "LOADER_ROLE"
    query_tag: str = "oracle_to_snowflake_pipeline"


@dataclass
class LoadConfig:
    table_name: str
    s3_stage: str                  # Snowflake stage name (external S3 stage)
    s3_prefix: str                 # e.g., orders/2024/01/15/
    file_format: str = "CSV_GZIP_FORMAT"
    load_mode: str = "COPY"        # COPY | MERGE | APPEND
    primary_keys: List[str] = field(default_factory=list)
    truncate_before_load: bool = False
    on_error: str = "ABORT_STATEMENT"  # ABORT_STATEMENT | CONTINUE | SKIP_FILE
    purge_after_load: bool = False
    target_schema: str = None      # Override default schema


@dataclass
class LoadResult:
    table_name: str
    rows_loaded: int = 0
    rows_rejected: int = 0
    files_loaded: int = 0
    status: str = "SUCCESS"
    error_message: str = None
    load_id: str = None
    start_time: datetime = None
    end_time: datetime = None
    copy_history: List[Dict] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


# ─────────────────────────────────────────────
# Connection Manager
# ─────────────────────────────────────────────
class SnowflakeConnectionManager:

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        connect_kwargs = dict(
            account=self.config.account,
            user=self.config.user,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            role=self.config.role,
            query_tag=self.config.query_tag,
            session_parameters={
                "QUERY_TAG": self.config.query_tag,
                "TIMEZONE": "UTC",
                "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_NTZ",
            },
        )

        if self.config.private_key_path:
            from cryptography.hazmat.primitives.serialization import (
                load_pem_private_key,
            )
            from cryptography.hazmat.backends import default_backend

            with open(self.config.private_key_path, "rb") as key_file:
                private_key = load_pem_private_key(
                    key_file.read(),
                    password=os.environ.get("SNOWFLAKE_PK_PASSPHRASE", "").encode(),
                    backend=default_backend(),
                )
            connect_kwargs["private_key"] = private_key
        else:
            connect_kwargs["password"] = self.config.password

        logger.info(f"Connecting to Snowflake: {self.config.account} / {self.config.database}")
        self._conn = snowflake.connector.connect(**connect_kwargs)
        return self._conn

    def execute(self, sql: str, params=None) -> List[tuple]:
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def execute_many(self, sql: str, params_list: List[tuple]):
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        try:
            cursor.executemany(sql, params_list)
        finally:
            cursor.close()

    def close(self):
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed")


# ─────────────────────────────────────────────
# Core Loader
# ─────────────────────────────────────────────
class SnowflakeLoader:

    def __init__(self, sf_config: SnowflakeConfig):
        self.sf_config = sf_config
        self.conn_mgr = SnowflakeConnectionManager(sf_config)

    # ── Main Entry Point ──────────────────────
    def load_table(self, cfg: LoadConfig) -> LoadResult:
        result = LoadResult(
            table_name=cfg.table_name,
            start_time=datetime.utcnow(),
            load_id=f"LOAD_{cfg.table_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        )
        schema = cfg.target_schema or self.sf_config.schema
        full_table = f"{self.sf_config.database}.{schema}.{cfg.table_name}"

        logger.info(
            f"Loading {full_table} | mode={cfg.load_mode} | stage={cfg.s3_stage}"
        )

        try:
            self.conn_mgr.connect()
            self._set_session_context(result.load_id)

            if cfg.truncate_before_load and cfg.load_mode == "COPY":
                self._truncate_table(full_table)

            if cfg.load_mode == "COPY":
                self._copy_into(full_table, cfg, result)
            elif cfg.load_mode == "MERGE":
                self._stage_and_merge(full_table, cfg, result)
            elif cfg.load_mode == "APPEND":
                self._copy_into(full_table, cfg, result)

            self._write_audit_log(result, cfg)
            result.status = "SUCCESS"

        except Exception as e:
            result.status = "FAILED"
            result.error_message = str(e)
            logger.error(f"Load failed for {cfg.table_name}: {e}", exc_info=True)
            raise

        finally:
            result.end_time = datetime.utcnow()
            self.conn_mgr.close()

        logger.info(
            f"Load complete | rows={result.rows_loaded:,} "
            f"| rejected={result.rows_rejected} "
            f"| duration={result.duration_seconds:.1f}s"
        )
        return result

    # ── COPY INTO ─────────────────────────────
    def _copy_into(self, full_table: str, cfg: LoadConfig, result: LoadResult):
        copy_sql = f"""
        COPY INTO {full_table}
        FROM @{cfg.s3_stage}/{cfg.s3_prefix}
        FILE_FORMAT = (FORMAT_NAME = '{self.sf_config.database}.PUBLIC.{cfg.file_format}')
        ON_ERROR = '{cfg.on_error}'
        PURGE = {str(cfg.purge_after_load).upper()}
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = FALSE
        """
        logger.info(f"Executing COPY INTO: {copy_sql.strip()}")
        rows = self.conn_mgr.execute(copy_sql)

        for row in rows:
            # COPY INTO returns: file, status, rows_loaded, rows_parsed, errors, etc.
            result.files_loaded += 1
            if row[1] == "LOADED":
                result.rows_loaded += row[3] if len(row) > 3 else 0
            elif row[1] == "PARTIALLY_LOADED":
                result.rows_loaded += row[3] if len(row) > 3 else 0
                result.rows_rejected += row[4] if len(row) > 4 else 0
            result.copy_history.append({
                "file": row[0],
                "status": row[1],
            })

    # ── Stage & Merge (Upsert) ────────────────
    def _stage_and_merge(self, full_table: str, cfg: LoadConfig, result: LoadResult):
        stage_table = f"{full_table}_STAGE_LOAD"

        # 1. Load into a temporary staging table
        create_stage_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE {stage_table}
        LIKE {full_table}
        """
        self.conn_mgr.execute(create_stage_sql)

        copy_stage_sql = f"""
        COPY INTO {stage_table}
        FROM @{cfg.s3_stage}/{cfg.s3_prefix}
        FILE_FORMAT = (FORMAT_NAME = '{self.sf_config.database}.PUBLIC.{cfg.file_format}')
        ON_ERROR = '{cfg.on_error}'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        self.conn_mgr.execute(copy_stage_sql)

        # 2. MERGE from temp into target
        pk_conditions = " AND ".join(
            [f"tgt.{pk} = src.{pk}" for pk in cfg.primary_keys]
        )
        # Build dynamic column list (excluding PKs for UPDATE)
        col_sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{cfg.table_name.upper()}'"
        all_cols = [r[0] for r in self.conn_mgr.execute(col_sql)]
        update_cols = [c for c in all_cols if c not in cfg.primary_keys]
        update_set = ", ".join([f"tgt.{c} = src.{c}" for c in update_cols])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"src.{c}" for c in all_cols])

        merge_sql = f"""
        MERGE INTO {full_table} AS tgt
        USING {stage_table} AS src
        ON {pk_conditions}
        WHEN MATCHED THEN UPDATE SET
            {update_set},
            tgt._DW_UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT ({insert_cols}, _DW_INSERTED_AT)
            VALUES ({insert_vals}, CURRENT_TIMESTAMP())
        """
        logger.info("Executing MERGE (upsert)…")
        rows = self.conn_mgr.execute(merge_sql)
        if rows:
            result.rows_loaded = rows[0][0] + rows[0][1]  # rows inserted + updated
        logger.info(f"MERGE complete: {rows}")

    # ── Audit Log ─────────────────────────────
    def _write_audit_log(self, result: LoadResult, cfg: LoadConfig):
        audit_sql = """
        INSERT INTO AUDIT_DB.PIPELINE_AUDIT.LOAD_HISTORY
            (LOAD_ID, TABLE_NAME, LOAD_MODE, ROWS_LOADED, ROWS_REJECTED,
             FILES_LOADED, STATUS, START_TIME, END_TIME, DURATION_SECONDS,
             CREATED_AT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        try:
            self.conn_mgr.execute(
                audit_sql,
                (
                    result.load_id,
                    result.table_name,
                    cfg.load_mode,
                    result.rows_loaded,
                    result.rows_rejected,
                    result.files_loaded,
                    result.status,
                    result.start_time,
                    result.end_time,
                    result.duration_seconds,
                ),
            )
        except Exception as e:
            logger.warning(f"Could not write audit log (non-fatal): {e}")

    # ── Helpers ───────────────────────────────
    def _truncate_table(self, full_table: str):
        logger.warning(f"Truncating {full_table}")
        self.conn_mgr.execute(f"TRUNCATE TABLE {full_table}")

    def _set_session_context(self, load_id: str):
        self.conn_mgr.execute(
            f"ALTER SESSION SET QUERY_TAG = 'LOAD_ID={load_id}'"
        )


# ─────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────
def create_loader_from_env() -> SnowflakeLoader:
    cfg = SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "LOAD_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "RAW_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "ORACLE_RAW"),
        role=os.environ.get("SNOWFLAKE_ROLE", "LOADER_ROLE"),
    )
    return SnowflakeLoader(cfg)
