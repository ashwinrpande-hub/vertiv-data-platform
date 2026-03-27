-- ================================================================
-- FILE: 06b_gold_ml_ai.sql
-- PURPOSE: Gold ML Feature Store (point-in-time correct) + AI vectors
-- ROLE: PLATFORM_ADMIN
-- RUN ORDER: 6th (part b, after 06a_gold_bi.sql)
-- ================================================================

USE ROLE      PLATFORM_ADMIN;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE  ENTERPRISE_ANALYTICS;

-- ════════════════════════════════════════════════════════════════
-- ML FEATURE STORE: Point-in-time correct features
-- KEY PRINCIPLE: WHERE order_date < AS_OF_DATE (strict, never <=)
-- This prevents future data leakage in ML training
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.ML_FEATURES.CUSTOMER_REVENUE_FEATURES (
  FEATURE_ID            NUMBER        AUTOINCREMENT PRIMARY KEY,
  CUSTOMER_HK           VARCHAR(64)   NOT NULL,
  AS_OF_DATE            DATE          NOT NULL,   -- strict PIT snapshot date
  FEATURE_VERSION       NUMBER        DEFAULT 1,  -- increment when logic changes

  -- Revenue features (lagged windows, no future data)
  REV_L7D_USD           NUMBER(18,2),  -- revenue last 7 days before as_of_date
  REV_L30D_USD          NUMBER(18,2),  -- revenue last 30 days
  REV_L90D_USD          NUMBER(18,2),  -- revenue last 90 days
  REV_L365D_USD         NUMBER(18,2),  -- revenue last 365 days
  REV_PREV_YEAR_USD     NUMBER(18,2),  -- revenue 365-730 days ago

  -- Order count features
  ORDER_COUNT_L30       NUMBER,
  ORDER_COUNT_L90       NUMBER,
  ORDER_COUNT_L365      NUMBER,

  -- Value features
  AVG_ORDER_VALUE_L90   NUMBER(18,2),
  MAX_ORDER_VALUE       NUMBER(18,2),
  MIN_ORDER_VALUE       NUMBER(18,2),

  -- Recency / frequency features
  DAYS_SINCE_LAST_ORDER NUMBER,
  ORDER_FREQUENCY_L365  NUMBER(8,4),   -- orders per month over last year
  PRODUCT_MIX_HHI       NUMBER(8,4),   -- Herfindahl index (product concentration)

  -- Model output scores (computed by Snowpark ML)
  CHURN_RISK_SCORE      NUMBER(5,4),   -- 0-1 probability of churn in next 90 days
  UPSELL_PROPENSITY     NUMBER(5,4),   -- 0-1 probability of upsell

  LOAD_TIMESTAMP        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT UK_CUST_FEATURE UNIQUE (CUSTOMER_HK, AS_OF_DATE, FEATURE_VERSION)
)
CLUSTER BY (AS_OF_DATE, CUSTOMER_HK)
COMMENT = 'Customer revenue ML features - point-in-time correct via AS_OF_DATE < predicate';

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.ML_FEATURES.PRODUCT_AFFINITY_FEATURES (
  FEATURE_ID            NUMBER        AUTOINCREMENT PRIMARY KEY,
  CUSTOMER_HK           VARCHAR(64)   NOT NULL,
  PRODUCT_FAMILY        VARCHAR(100)  NOT NULL,
  AS_OF_DATE            DATE          NOT NULL,
  FEATURE_VERSION       NUMBER        DEFAULT 1,
  PURCHASE_COUNT_L365   NUMBER,
  LAST_PURCHASE_DATE    DATE,
  DAYS_SINCE_PURCHASE   NUMBER,
  REVENUE_SHARE_PCT     NUMBER(5,2),  -- % of customer total revenue from this family
  CROSS_SELL_SCORE      NUMBER(5,4),  -- likelihood to buy from adjacent family
  LOAD_TIMESTAMP        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (AS_OF_DATE, CUSTOMER_HK);

-- ML model predictions store
CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.ML_FEATURES.MODEL_PREDICTIONS (
  PREDICTION_ID         NUMBER        AUTOINCREMENT PRIMARY KEY,
  MODEL_NAME            VARCHAR(200),
  MODEL_VERSION         VARCHAR(50),
  ENTITY_TYPE           VARCHAR(50),   -- CUSTOMER | OPPORTUNITY | PRODUCT
  ENTITY_HK             VARCHAR(64),
  AS_OF_DATE            DATE,
  PREDICTION_LABEL      VARCHAR(100),
  PREDICTION_SCORE      NUMBER(8,6),
  PREDICTION_BAND       VARCHAR(20),   -- HIGH | MEDIUM | LOW
  CONFIDENCE_INTERVAL   NUMBER(5,4),
  FEATURE_IMPORTANCES   VARIANT,       -- JSON map of feature → importance
  LOAD_TIMESTAMP        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MODEL_NAME, AS_OF_DATE);

-- ════════════════════════════════════════════════════════════════
-- AI / GenAI ZONE: Snowflake VECTOR type + Cortex
-- ════════════════════════════════════════════════════════════════

-- Customer 360 vectors for semantic search
CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.AI.CUSTOMER_360_VECTORS (
  CUSTOMER_HK           VARCHAR(64)      PRIMARY KEY,
  CUSTOMER_NAME         VARCHAR(500),
  CUSTOMER_SUMMARY      VARCHAR(4000),   -- AI-generated narrative
  SUMMARY_VECTOR        ARRAY,  -- Snowflake native VECTOR type
  KEY_PRODUCTS          ARRAY,
  TOP_COUNTRIES         ARRAY,
  REVENUE_BAND          VARCHAR(20),     -- ENTERPRISE | MID_MARKET | SMB
  TAGS                  ARRAY,
  LAST_UPDATED          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Customer 360 vector embeddings via Cortex e5-base-v2 (768-dim) for semantic search';

-- Product descriptions with vectors
CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.AI.PRODUCT_VECTORS (
  PRODUCT_HK            VARCHAR(64)      PRIMARY KEY,
  PRODUCT_CODE          VARCHAR(100),
  PRODUCT_DESCRIPTION   VARCHAR(4000),
  TECH_SPECS            VARCHAR(2000),
  DESC_VECTOR           ARRAY,
  SPEC_TAGS             ARRAY,
  USE_CASE_TAGS         ARRAY,           -- DATA_CENTER | TELECOM | HEALTHCARE etc.
  LAST_UPDATED          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

-- Cortex Search index table (feeds managed Cortex Search Service)
CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.AI.CORTEX_SEARCH_INDEX (
  ENTITY_TYPE           VARCHAR(50),    -- DATA_PRODUCT | CUSTOMER | PRODUCT
  ENTITY_HK             VARCHAR(64),
  ENTITY_NAME           VARCHAR(500),
  SEARCH_BODY           VARCHAR(10000), -- Text indexed by Cortex
  METADATA              VARIANT,        -- Schema, SLA, owner etc.
  LAST_UPDATED          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Seed search index with data product metadata


-- ── Cortex: Generate customer narratives and embeddings ────────
-- Run AFTER feature pipeline has populated ML_FEATURES tables
-- This is a template - adjust column names to match your data

-- Step 1: Generate text narratives
-- (Run manually or via Snowpark task after features are loaded)


-- ── GRANTS ────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.ML_FEATURES TO ROLE ML_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.AI          TO ROLE ML_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.AI          TO ROLE GLOBAL_ANALYST;
GRANT SELECT ON ENTERPRISE_ANALYTICS.ML_FEATURES.MODEL_PREDICTIONS    TO ROLE GLOBAL_ANALYST;

-- ── VERIFY ────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA ENTERPRISE_ANALYTICS.ML_FEATURES;
SHOW TABLES IN SCHEMA ENTERPRISE_ANALYTICS.AI;
SELECT COUNT(*) AS PRODUCT_ROWS FROM ENTERPRISE_ANALYTICS.BI.DIM_PRODUCT;
SELECT COUNT(*) AS DATE_ROWS    FROM ENTERPRISE_ANALYTICS.BI.DIM_DATE;
SELECT COUNT(*) AS REGION_ROWS  FROM ENTERPRISE_ANALYTICS.BI.DIM_REGION;

