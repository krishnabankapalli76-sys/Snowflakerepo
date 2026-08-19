-- =============================================================
-- 02_RAW_TABLES.SQL
-- Raw Layer — Oracle source tables mirrored 1:1
-- All columns VARCHAR to accept any incoming data without cast errors.
-- DW metadata columns (_DW_*) appended to every table.
-- Author: Data Engineering Team | Version: 1.0
-- =============================================================

USE ROLE LOADER_ROLE;
USE WAREHOUSE LOAD_WH;
USE DATABASE RAW_DB;
USE SCHEMA ORACLE_RAW;

-- ─────────────────────────────────────────────
-- ORDERS (raw)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ORDERS (
    -- Source columns (VARCHAR — no type casting at raw layer)
    ORDER_ID            VARCHAR(50),
    CUSTOMER_ID         VARCHAR(50),
    PRODUCT_ID          VARCHAR(50),
    ORDER_DATE          VARCHAR(30),
    SHIP_DATE           VARCHAR(30),
    STATUS              VARCHAR(50),
    QUANTITY            VARCHAR(20),
    UNIT_PRICE          VARCHAR(30),
    DISCOUNT_PCT        VARCHAR(20),
    TOTAL_AMOUNT        VARCHAR(30),
    CURRENCY            VARCHAR(10),
    REGION              VARCHAR(50),
    SALES_REP_ID        VARCHAR(50),
    CREATED_AT          VARCHAR(30),
    UPDATED_AT          VARCHAR(30),

    -- DW Metadata
    _DW_FILE_NAME       VARCHAR(500),
    _DW_FILE_ROW_NUMBER NUMBER(18,0),
    _DW_LOAD_ID         VARCHAR(100),
    _DW_LOADED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _DW_SOURCE_SYSTEM   VARCHAR(50)     DEFAULT 'ORACLE',
    _DW_IS_DELETED      BOOLEAN         DEFAULT FALSE,
    _DW_BATCH_ID        VARCHAR(100)
)
CLUSTER BY (_DW_LOADED_AT::DATE)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw orders from Oracle. No transformations applied.';

-- ─────────────────────────────────────────────
-- CUSTOMERS (raw)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS CUSTOMERS (
    CUSTOMER_ID         VARCHAR(50),
    FIRST_NAME          VARCHAR(200),
    LAST_NAME           VARCHAR(200),
    EMAIL               VARCHAR(500),
    PHONE               VARCHAR(100),
    ADDRESS_LINE1       VARCHAR(500),
    CITY                VARCHAR(200),
    STATE               VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTAL_CODE         VARCHAR(50),
    CUSTOMER_SEGMENT    VARCHAR(50),
    CREDIT_LIMIT        VARCHAR(30),
    ACCOUNT_STATUS      VARCHAR(30),
    CREATED_AT          VARCHAR(30),
    UPDATED_AT          VARCHAR(30),

    -- DW Metadata
    _DW_FILE_NAME       VARCHAR(500),
    _DW_FILE_ROW_NUMBER NUMBER(18,0),
    _DW_LOAD_ID         VARCHAR(100),
    _DW_LOADED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _DW_SOURCE_SYSTEM   VARCHAR(50)     DEFAULT 'ORACLE',
    _DW_IS_DELETED      BOOLEAN         DEFAULT FALSE,
    _DW_BATCH_ID        VARCHAR(100)
)
CLUSTER BY (_DW_LOADED_AT::DATE)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw customers from Oracle PIM system.';

-- ─────────────────────────────────────────────
-- PRODUCTS (raw)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS PRODUCTS (
    PRODUCT_ID          VARCHAR(50),
    PRODUCT_NAME        VARCHAR(500),
    CATEGORY            VARCHAR(100),
    SUBCATEGORY         VARCHAR(100),
    SKU                 VARCHAR(100),
    UNIT_COST           VARCHAR(30),
    LIST_PRICE          VARCHAR(30),
    WEIGHT_KG           VARCHAR(20),
    SUPPLIER_ID         VARCHAR(50),
    STOCK_QUANTITY      VARCHAR(20),
    REORDER_LEVEL       VARCHAR(20),
    IS_ACTIVE           VARCHAR(5),
    LAUNCH_DATE         VARCHAR(30),
    CREATED_AT          VARCHAR(30),
    UPDATED_AT          VARCHAR(30),

    -- DW Metadata
    _DW_FILE_NAME       VARCHAR(500),
    _DW_FILE_ROW_NUMBER NUMBER(18,0),
    _DW_LOAD_ID         VARCHAR(100),
    _DW_LOADED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _DW_SOURCE_SYSTEM   VARCHAR(50)     DEFAULT 'ORACLE',
    _DW_IS_DELETED      BOOLEAN         DEFAULT FALSE,
    _DW_BATCH_ID        VARCHAR(100)
)
CLUSTER BY (_DW_LOADED_AT::DATE)
DATA_RETENTION_TIME_IN_DAYS = 7;

-- ─────────────────────────────────────────────
-- COPY INTO TEMPLATES  (executed by Airflow)
-- ─────────────────────────────────────────────

/*
-- Parameterized COPY INTO — executed by Python with {variables}
COPY INTO RAW_DB.ORACLE_RAW.ORDERS (
    ORDER_ID, CUSTOMER_ID, PRODUCT_ID, ORDER_DATE, SHIP_DATE,
    STATUS, QUANTITY, UNIT_PRICE, DISCOUNT_PCT, TOTAL_AMOUNT,
    CURRENCY, REGION, SALES_REP_ID, CREATED_AT, UPDATED_AT,
    _DW_FILE_NAME, _DW_FILE_ROW_NUMBER, _DW_LOAD_ID
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        '{load_id}'
    FROM @RAW_DB.PUBLIC.ORACLE_S3_STAGE/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.PUBLIC.CSV_GZIP_FORMAT')
ON_ERROR  = 'ABORT_STATEMENT'
PURGE     = FALSE
FORCE     = FALSE;
*/

-- ─────────────────────────────────────────────
-- EXTERNAL STAGE (S3 → Snowflake)
-- ─────────────────────────────────────────────

-- Replace with your actual bucket, IAM role, and KMS key
CREATE STAGE IF NOT EXISTS RAW_DB.PUBLIC.ORACLE_S3_STAGE
    URL             = 's3://your-data-lake-bucket/raw/oracle/'
    STORAGE_INTEGRATION = S3_ORACLE_INTEGRATION
    FILE_FORMAT     = RAW_DB.PUBLIC.CSV_GZIP_FORMAT
    COMMENT         = 'External stage pointing to Oracle CSV extracts in S3';

-- List files in stage (useful for debugging)
-- LIST @RAW_DB.PUBLIC.ORACLE_S3_STAGE;

-- ─────────────────────────────────────────────
-- STREAM OBJECTS (for CDC / change detection)
-- ─────────────────────────────────────────────
CREATE STREAM IF NOT EXISTS RAW_DB.ORACLE_RAW.ORDERS_STREAM
    ON TABLE RAW_DB.ORACLE_RAW.ORDERS
    APPEND_ONLY = FALSE   -- captures INSERT, UPDATE, DELETE
    COMMENT = 'CDC stream on raw orders table for downstream processing';

CREATE STREAM IF NOT EXISTS RAW_DB.ORACLE_RAW.CUSTOMERS_STREAM
    ON TABLE RAW_DB.ORACLE_RAW.CUSTOMERS
    APPEND_ONLY = FALSE;
