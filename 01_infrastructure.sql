-- =============================================================
-- 01_INFRASTRUCTURE.SQL
-- Snowflake Account-Level Infrastructure Setup
-- Author: Data Engineering Team | Version: 1.0
-- =============================================================
-- Run as SYSADMIN / ACCOUNTADMIN

USE ROLE SYSADMIN;

-- ─────────────────────────────────────────────
-- VIRTUAL WAREHOUSES
-- ─────────────────────────────────────────────

-- Loading Warehouse (used by Airflow / COPY INTO jobs)
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
    WAREHOUSE_SIZE     = 'MEDIUM'
    AUTO_SUSPEND       = 60       -- seconds of inactivity
    AUTO_RESUME        = TRUE
    MIN_CLUSTER_COUNT  = 1
    MAX_CLUSTER_COUNT  = 3        -- multi-cluster for parallel loads
    SCALING_POLICY     = 'ECONOMY'
    COMMENT            = 'Used for data loading from S3 into RAW layer';

-- Transformation Warehouse (used by DBT)
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE     = 'LARGE'
    AUTO_SUSPEND       = 120
    AUTO_RESUME        = TRUE
    MIN_CLUSTER_COUNT  = 1
    MAX_CLUSTER_COUNT  = 2
    SCALING_POLICY     = 'ECONOMY'
    COMMENT            = 'Used for DBT transformations (staging → marts)';

-- Analytics / BI Warehouse (used by BI tools, analysts)
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE     = 'SMALL'
    AUTO_SUSPEND       = 300
    AUTO_RESUME        = TRUE
    MIN_CLUSTER_COUNT  = 1
    MAX_CLUSTER_COUNT  = 4
    SCALING_POLICY     = 'STANDARD'
    COMMENT            = 'Used by analysts and BI tools (Tableau, Looker)';

-- Admin / Monitoring Warehouse
CREATE WAREHOUSE IF NOT EXISTS ADMIN_WH
    WAREHOUSE_SIZE     = 'X-SMALL'
    AUTO_SUSPEND       = 60
    AUTO_RESUME        = TRUE
    COMMENT            = 'Used for admin scripts, monitoring, governance';

-- ─────────────────────────────────────────────
-- DATABASES (Per Environment Layer)
-- ─────────────────────────────────────────────

-- Raw: unmodified data from Oracle, partitioned by source
CREATE DATABASE IF NOT EXISTS RAW_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw landing zone — data as-is from Oracle via S3';

-- Staging: light transforms, type casting, dedupe
CREATE DATABASE IF NOT EXISTS STAGING_DB
    DATA_RETENTION_TIME_IN_DAYS = 3
    COMMENT = 'Staging layer — cleansed, typed, deduplicated';

-- Analytics: business-ready dimensional models (DBT output)
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Analytics layer — star schema, marts, fact/dim tables';

-- Audit: pipeline metadata, DQ results, load history
CREATE DATABASE IF NOT EXISTS AUDIT_DB
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Pipeline audit trail, DQ results, SLA tracking';

-- ─────────────────────────────────────────────
-- SCHEMAS
-- ─────────────────────────────────────────────

-- Raw DB Schemas
CREATE SCHEMA IF NOT EXISTS RAW_DB.ORACLE_RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw Oracle source tables';

CREATE SCHEMA IF NOT EXISTS RAW_DB.FILE_META
    COMMENT = 'Metadata about loaded files (Snowpipe history, file manifests)';

-- Staging DB Schemas
CREATE SCHEMA IF NOT EXISTS STAGING_DB.ORACLE_STAGE
    DATA_RETENTION_TIME_IN_DAYS = 3
    COMMENT = 'DBT staging models from Oracle';

-- Analytics DB Schemas
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.SALES
    COMMENT = 'Sales dimensional models';

CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.FINANCE
    COMMENT = 'Finance dimensional models';

CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.OPERATIONS
    COMMENT = 'Operations dimensional models';

CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.SHARED
    COMMENT = 'Shared dimensions (customer, product, date, geography)';

-- Audit DB Schemas
CREATE SCHEMA IF NOT EXISTS AUDIT_DB.PIPELINE_AUDIT
    COMMENT = 'Pipeline load history and run metadata';

CREATE SCHEMA IF NOT EXISTS AUDIT_DB.DATA_QUALITY
    COMMENT = 'DQ check results, thresholds, SLA tracking';

-- ─────────────────────────────────────────────
-- FILE FORMATS (used by COPY INTO and Snowpipe)
-- ─────────────────────────────────────────────

USE DATABASE RAW_DB;

CREATE FILE FORMAT IF NOT EXISTS PUBLIC.CSV_GZIP_FORMAT
    TYPE                = 'CSV'
    COMPRESSION         = 'GZIP'
    FIELD_DELIMITER     = ','
    RECORD_DELIMITER    = '\n'
    SKIP_HEADER         = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF             = ('', 'NULL', 'null', 'N/A', '\\N')
    EMPTY_FIELD_AS_NULL = TRUE
    DATE_FORMAT         = 'YYYY-MM-DD'
    TIME_FORMAT         = 'HH24:MI:SS'
    TIMESTAMP_FORMAT    = 'YYYY-MM-DD HH24:MI:SS'
    TRIM_SPACE          = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard gzip-compressed CSV format for Oracle extracts';

CREATE FILE FORMAT IF NOT EXISTS PUBLIC.CSV_PLAIN_FORMAT
    TYPE                = 'CSV'
    COMPRESSION         = 'NONE'
    FIELD_DELIMITER     = ','
    RECORD_DELIMITER    = '\n'
    SKIP_HEADER         = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF             = ('', 'NULL', 'null', 'N/A', '\\N')
    EMPTY_FIELD_AS_NULL = TRUE
    DATE_FORMAT         = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT    = 'YYYY-MM-DD HH24:MI:SS'
    TRIM_SPACE          = TRUE;

CREATE FILE FORMAT IF NOT EXISTS PUBLIC.JSON_FORMAT
    TYPE        = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE;

-- ─────────────────────────────────────────────
-- AUDIT TABLES
-- ─────────────────────────────────────────────

USE DATABASE AUDIT_DB;

CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT.LOAD_HISTORY (
    LOAD_ID             VARCHAR(100)    NOT NULL,
    TABLE_NAME          VARCHAR(200)    NOT NULL,
    LOAD_MODE           VARCHAR(50),
    ROWS_LOADED         NUMBER(18,0)    DEFAULT 0,
    ROWS_REJECTED       NUMBER(18,0)    DEFAULT 0,
    FILES_LOADED        NUMBER(18,0)    DEFAULT 0,
    STATUS              VARCHAR(20),
    START_TIME          TIMESTAMP_NTZ,
    END_TIME            TIMESTAMP_NTZ,
    DURATION_SECONDS    FLOAT,
    ERROR_MESSAGE       VARCHAR(4000),
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DATA_QUALITY.CHECK_RESULTS (
    RESULT_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              VARCHAR(100)    NOT NULL,
    TABLE_NAME          VARCHAR(200)    NOT NULL,
    CHECK_NAME          VARCHAR(200)    NOT NULL,
    CHECK_TYPE          VARCHAR(100),
    COLUMN_NAME         VARCHAR(200),
    STATUS              VARCHAR(20),
    SEVERITY            VARCHAR(20),
    EXPECTED            VARCHAR(1000),
    ACTUAL              VARCHAR(1000),
    MESSAGE             VARCHAR(4000),
    ROWS_AFFECTED       NUMBER(18,0),
    RUN_AT              TIMESTAMP_NTZ,
    SQL_USED            VARCHAR(16000),
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (TABLE_NAME, RUN_AT::DATE);

-- SLA tracking summary (per pipeline run)
CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT.PIPELINE_RUN_SUMMARY (
    RUN_ID              VARCHAR(100)    NOT NULL PRIMARY KEY,
    DAG_ID              VARCHAR(200),
    PIPELINE_NAME       VARCHAR(200),
    ENVIRONMENT         VARCHAR(20),
    TABLES_PROCESSED    NUMBER,
    TOTAL_ROWS_LOADED   NUMBER(18,0),
    STATUS              VARCHAR(20),
    SLA_MET             BOOLEAN,
    SLA_TARGET_MINUTES  NUMBER,
    ACTUAL_DURATION_MIN FLOAT,
    START_TIME          TIMESTAMP_NTZ,
    END_TIME            TIMESTAMP_NTZ,
    TRIGGERED_BY        VARCHAR(200),
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
