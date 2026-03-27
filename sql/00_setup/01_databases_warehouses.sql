-- ================================================================
-- FILE: 01_databases_warehouses.sql
-- PURPOSE: Create all databases and compute warehouses
-- ROLE: ACCOUNTADMIN
-- RUN ORDER: 1st (before everything else)
-- ESTIMATED TIME: ~2 minutes
-- ================================================================

USE ROLE ACCOUNTADMIN;

-- ── 1. DATABASES ──────────────────────────────────────────────
-- Each database maps to a Medallion layer or cross-cutting concern

CREATE DATABASE IF NOT EXISTS ENTERPRISE_RAW
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'BRONZE: Raw ingestion from SAP(Europe) JDE(Americas) QAD(EMEA) Salesforce Kafka';

CREATE DATABASE IF NOT EXISTS ENTERPRISE_CURATED
  DATA_RETENTION_TIME_IN_DAYS = 30
  COMMENT = 'SILVER: 3NF cleansed, SHA256 hash-keyed, insert-only versioned tables';

CREATE DATABASE IF NOT EXISTS ENTERPRISE_ANALYTICS
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'GOLD: BI star schema + ML Feature Store + AI vector embeddings';

CREATE DATABASE IF NOT EXISTS DATA_PRODUCTS
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'DATA PRODUCTS: Secure views published to Snowflake Marketplace';

CREATE DATABASE IF NOT EXISTS PLATFORM_CONFIG
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'CONFIG: Source-to-target mappings, DQ rules, reference tables, lineage';

CREATE DATABASE IF NOT EXISTS PLATFORM_AUDIT
  DATA_RETENTION_TIME_IN_DAYS = 90
  COMMENT = 'AUDIT: DQ batch logs, rejected records, quarantine, batch tracking';

-- ── 2. WAREHOUSES ─────────────────────────────────────────────
-- Right-sized per workload type. All use AUTO_RESUME + AUTO_SUSPEND.

-- Ingestion: XSmall - Snowpipe notifications and lightweight tasks
CREATE WAREHOUSE IF NOT EXISTS INGEST_WH
  WAREHOUSE_SIZE   = 'XSMALL'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Snowpipe + lightweight DQ Task jobs. XSMALL is enough.';

-- Transformation: Small - Silver/Gold Dynamic Tables
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE   = 'SMALL'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Silver/Gold Dynamic Tables, incremental transforms, config-driven ETL';

-- Analytics: Medium, multi-cluster - concurrent BI queries
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
  WAREHOUSE_SIZE      = 'MEDIUM'
  AUTO_SUSPEND        = 300
  AUTO_RESUME         = TRUE
  MAX_CLUSTER_COUNT   = 3
  MIN_CLUSTER_COUNT   = 1
  SCALING_POLICY      = 'ECONOMY'
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Multi-cluster for concurrent Tableau/Power BI analysts. ECONOMY scales conservatively.';

-- ML: Large - Snowpark Python needs memory for pandas/scikit-learn
CREATE WAREHOUSE IF NOT EXISTS ML_WH
  WAREHOUSE_SIZE   = 'LARGE'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Snowpark Python feature engineering and ML model training. Suspend fast - expensive.';

-- ── 3. RESOURCE MONITOR ───────────────────────────────────────
-- Prevents cost overrun during the demo build

CREATE OR REPLACE RESOURCE MONITOR PLATFORM_DEMO_GUARD
  WITH CREDIT_QUOTA  = 300
  FREQUENCY          = MONTHLY
  START_TIMESTAMP    = IMMEDIATELY
  TRIGGERS
    ON  75 PERCENT DO NOTIFY
    ON  90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply the guard to expensive warehouses
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = PLATFORM_DEMO_GUARD;
ALTER WAREHOUSE ML_WH         SET RESOURCE_MONITOR = PLATFORM_DEMO_GUARD;

-- ── 4. VERIFY ─────────────────────────────────────────────────
SHOW DATABASES LIKE 'ENTERPRISE%';
SHOW WAREHOUSES LIKE '%WH';
