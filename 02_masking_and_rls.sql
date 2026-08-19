-- =============================================================
-- 02_MASKING_AND_RLS.SQL
-- Dynamic Data Masking + Row-Level Security Policies
-- Author: Data Engineering Team | Version: 1.0
-- =============================================================

USE ROLE DATA_STEWARD_ROLE;
USE WAREHOUSE ADMIN_WH;
USE DATABASE ANALYTICS_DB;

-- ─────────────────────────────────────────────
-- MASKING POLICIES
-- ─────────────────────────────────────────────

-- Email masking: analysts see john.***@***.com format
-- Data engineers see full email
CREATE OR REPLACE MASKING POLICY ANALYTICS_DB.SHARED.MASK_EMAIL
    AS (val STRING) RETURNS STRING ->
        CASE
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_STEWARD_ROLE')
                THEN val
            WHEN val IS NULL
                THEN NULL
            ELSE
                CONCAT(
                    LEFT(SPLIT_PART(val, '@', 1), 2),
                    '***@***.',
                    SPLIT_PART(SPLIT_PART(val, '@', 2), '.', -1)
                )
        END
    COMMENT = 'Masks email — analysts see partial, engineers see full';

-- Phone masking: show only last 4 digits to analysts
CREATE OR REPLACE MASKING POLICY ANALYTICS_DB.SHARED.MASK_PHONE
    AS (val STRING) RETURNS STRING ->
        CASE
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_STEWARD_ROLE')
                THEN val
            WHEN val IS NULL
                THEN NULL
            ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4))
        END;

-- Credit card / financial masking
CREATE OR REPLACE MASKING POLICY ANALYTICS_DB.SHARED.MASK_CREDIT_LIMIT
    AS (val NUMBER) RETURNS NUMBER ->
        CASE
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_STEWARD_ROLE', 'ANALYST_ROLE')
                THEN val
            ELSE NULL
        END;

-- Address masking: city/country visible, street address masked
CREATE OR REPLACE MASKING POLICY ANALYTICS_DB.SHARED.MASK_ADDRESS
    AS (val STRING) RETURNS STRING ->
        CASE
            WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_STEWARD_ROLE')
                THEN val
            ELSE '*** MASKED ***'
        END;

-- ─────────────────────────────────────────────
-- APPLY MASKING POLICIES TO COLUMNS
-- ─────────────────────────────────────────────

-- Apply to dim_customer in analytics layer
ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN EMAIL     SET MASKING POLICY ANALYTICS_DB.SHARED.MASK_EMAIL;

ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN PHONE     SET MASKING POLICY ANALYTICS_DB.SHARED.MASK_PHONE;

ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN ADDRESS_LINE1 SET MASKING POLICY ANALYTICS_DB.SHARED.MASK_ADDRESS;

-- ─────────────────────────────────────────────
-- ROW ACCESS POLICIES (Row-Level Security)
-- ─────────────────────────────────────────────

-- Region-based RLS: sales reps only see their region's data
-- Mapping table: user → allowed regions
CREATE TABLE IF NOT EXISTS ANALYTICS_DB.SHARED.USER_REGION_ACCESS (
    SNOWFLAKE_USER  VARCHAR(200) NOT NULL,
    REGION          VARCHAR(100) NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate sample access mappings
INSERT INTO ANALYTICS_DB.SHARED.USER_REGION_ACCESS VALUES
    ('SR001', 'NORTH_AMERICA', CURRENT_TIMESTAMP()),
    ('SR002', 'EUROPE', CURRENT_TIMESTAMP()),
    ('SR003', 'ASIA_PACIFIC', CURRENT_TIMESTAMP()),
    ('SR004', 'LATIN_AMERICA', CURRENT_TIMESTAMP()),
    ('ANALYST_GLOBAL', 'ALL', CURRENT_TIMESTAMP());

-- Row access policy: data engineers / stewards see everything;
-- other users see only their permitted regions
CREATE OR REPLACE ROW ACCESS POLICY ANALYTICS_DB.SHARED.RAP_REGION_FILTER
    AS (region_col VARCHAR) RETURNS BOOLEAN ->
        -- Admins and engineers see all
        CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'DATA_STEWARD_ROLE', 'SYSADMIN')
        OR
        -- Global analysts see all
        EXISTS (
            SELECT 1 FROM ANALYTICS_DB.SHARED.USER_REGION_ACCESS
            WHERE SNOWFLAKE_USER = CURRENT_USER() AND REGION = 'ALL'
        )
        OR
        -- Others see only their mapped regions
        EXISTS (
            SELECT 1 FROM ANALYTICS_DB.SHARED.USER_REGION_ACCESS
            WHERE SNOWFLAKE_USER = CURRENT_USER()
              AND REGION = region_col
        )
    COMMENT = 'Restricts fact_orders rows by user-region mapping';

-- ─────────────────────────────────────────────
-- SNOWFLAKE TAGS (Data Classification)
-- ─────────────────────────────────────────────

USE ROLE DATA_STEWARD_ROLE;

-- Create tag objects
CREATE TAG IF NOT EXISTS ANALYTICS_DB.SHARED.PII_SENSITIVITY
    ALLOWED_VALUES 'HIGH', 'MEDIUM', 'LOW', 'NONE'
    COMMENT = 'PII sensitivity classification';

CREATE TAG IF NOT EXISTS ANALYTICS_DB.SHARED.DATA_DOMAIN
    ALLOWED_VALUES 'SALES', 'FINANCE', 'OPERATIONS', 'HR', 'PRODUCT'
    COMMENT = 'Business domain classification';

CREATE TAG IF NOT EXISTS ANALYTICS_DB.SHARED.DATA_OWNER
    COMMENT = 'Business data owner email or team name';

CREATE TAG IF NOT EXISTS ANALYTICS_DB.SHARED.RETENTION_POLICY
    ALLOWED_VALUES '30_DAYS', '1_YEAR', '3_YEARS', '7_YEARS', 'INDEFINITE'
    COMMENT = 'Data retention classification';

-- Apply tags to databases
ALTER DATABASE RAW_DB       SET TAG ANALYTICS_DB.SHARED.DATA_DOMAIN = 'SALES';
ALTER DATABASE ANALYTICS_DB SET TAG ANALYTICS_DB.SHARED.DATA_DOMAIN = 'SALES';

-- Apply tags to columns (example — dim_customer)
ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN EMAIL
        SET TAG ANALYTICS_DB.SHARED.PII_SENSITIVITY = 'HIGH';

ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN PHONE
        SET TAG ANALYTICS_DB.SHARED.PII_SENSITIVITY = 'HIGH';

ALTER TABLE IF EXISTS ANALYTICS_DB.SHARED.DIM_CUSTOMER
    MODIFY COLUMN CREDIT_LIMIT
        SET TAG ANALYTICS_DB.SHARED.PII_SENSITIVITY = 'MEDIUM';

-- ─────────────────────────────────────────────
-- NETWORK POLICY (IP Allowlisting)
-- ─────────────────────────────────────────────

-- Restrict Snowflake access to corporate IPs + known cloud NATs
CREATE NETWORK POLICY IF NOT EXISTS CORPORATE_NETWORK_POLICY
    ALLOWED_IP_LIST = (
        '10.0.0.0/8',       -- Corporate VPN range
        '172.16.0.0/12',    -- Internal networks
        '54.0.0.0/8'        -- AWS NAT gateway range (example)
    )
    COMMENT = 'Restricts Snowflake access to corporate network and cloud IPs';

-- Apply to service accounts
ALTER USER SVC_AIRFLOW SET NETWORK_POLICY = CORPORATE_NETWORK_POLICY;
ALTER USER SVC_DBT     SET NETWORK_POLICY = CORPORATE_NETWORK_POLICY;

-- ─────────────────────────────────────────────
-- AUDIT / GOVERNANCE VIEWS
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW AUDIT_DB.PIPELINE_AUDIT.V_POLICY_ASSIGNMENTS AS
SELECT
    pm.policy_name,
    pm.policy_db,
    pm.policy_schema,
    pm.policy_signature,
    pm.ref_entity_name,
    pm.ref_entity_domain,
    pm.ref_column_name,
    pm.ref_arg_column_names
FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES pm
ORDER BY pm.policy_name, pm.ref_entity_name;

CREATE OR REPLACE VIEW AUDIT_DB.PIPELINE_AUDIT.V_QUERY_HISTORY_DAILY AS
SELECT
    DATE_TRUNC('DAY', START_TIME)::DATE AS query_date,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TYPE,
    COUNT(*)                             AS query_count,
    SUM(TOTAL_ELAPSED_TIME) / 1000       AS total_seconds,
    SUM(CREDITS_USED_CLOUD_SERVICES)     AS cloud_credits,
    AVG(ROWS_PRODUCED)                   AS avg_rows
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('DAY', -30, CURRENT_DATE())
GROUP BY 1, 2, 3, 4, 5, 6, 7;
