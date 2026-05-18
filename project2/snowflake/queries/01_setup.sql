-- snowflake/queries/01_setup.sql
-- Run once to bootstrap Snowflake for Project 2.
-- Execute as SYSADMIN or higher.
-- Replace <YOUR_AWS_ACCOUNT_ID> and <YOUR_S3_BUCKET> with real values.

-- ── Warehouse & database ──────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS PROJECT2_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND   = 60        -- suspend after 60s idle (saves cost)
    AUTO_RESUME    = TRUE
    COMMENT        = 'Project 2 data engineering warehouse';

CREATE DATABASE IF NOT EXISTS PROJECT2_DW
    COMMENT = 'Project 2 e-commerce and financial data warehouse';

USE DATABASE PROJECT2_DW;

-- ── Schemas ───────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS RAW;        -- raw copies from S3
CREATE SCHEMA IF NOT EXISTS GOLD;       -- synced from Redshift gold layer
CREATE SCHEMA IF NOT EXISTS ANALYTICS;  -- Snowflake-native analytics views

-- ── Roles ─────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS PROJECT2_ENGINEER;
CREATE ROLE IF NOT EXISTS PROJECT2_ANALYST;

GRANT USAGE  ON WAREHOUSE PROJECT2_WH  TO ROLE PROJECT2_ENGINEER;
GRANT USAGE  ON WAREHOUSE PROJECT2_WH  TO ROLE PROJECT2_ANALYST;
GRANT USAGE  ON DATABASE  PROJECT2_DW  TO ROLE PROJECT2_ENGINEER;
GRANT USAGE  ON DATABASE  PROJECT2_DW  TO ROLE PROJECT2_ANALYST;
GRANT ALL    ON SCHEMA PROJECT2_DW.RAW  TO ROLE PROJECT2_ENGINEER;
GRANT ALL    ON SCHEMA PROJECT2_DW.GOLD TO ROLE PROJECT2_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA PROJECT2_DW.GOLD TO ROLE PROJECT2_ANALYST;

-- ── Storage integration (connects Snowflake to your S3 bucket) ────
-- After creating this, run DESCRIBE INTEGRATION s3_project2_integration
-- and copy the IAM_USER_ARN + EXTERNAL_ID into your S3 bucket policy.
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_project2_integration
    TYPE                  = EXTERNAL_STAGE
    STORAGE_PROVIDER      = S3
    ENABLED               = TRUE
    STORAGE_AWS_ROLE_ARN  = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/project2-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_S3_BUCKET>/gold/');

DESCRIBE INTEGRATION s3_project2_integration;
-- Copy IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from output
-- and add them to your S3 bucket trust policy.

-- ── External stage (points at S3 gold zone) ───────────────────────
CREATE STAGE IF NOT EXISTS PROJECT2_DW.RAW.s3_gold_stage
    URL                  = 's3://<YOUR_S3_BUCKET>/gold/'
    STORAGE_INTEGRATION  = s3_project2_integration
    FILE_FORMAT          = (TYPE = PARQUET)
    COMMENT              = 'S3 gold zone — synced from Redshift dbt output';

-- Verify stage is reachable
LIST @PROJECT2_DW.RAW.s3_gold_stage;
