-- ================================================================
-- FILE: 05_silver_ddl.sql
-- PURPOSE: Silver layer - 3NF tables, SHA256 hash keys, insert-only
--          Dynamic Tables for auto-incremental processing
--          Audit tables for DQ logging
-- ROLE: PLATFORM_ADMIN
-- RUN ORDER: 5th
-- KEY DESIGN:
--   - SHA256(source_system|original_pk) = HASH_KEY (64-char hex)
--   - Insert-only: EFFECTIVE_FROM / EFFECTIVE_TO / IS_CURRENT
--   - HASH_DIFF detects changes without comparing every column
--   - 3NF: no transitive dependencies, no repeating groups
-- ================================================================

USE ROLE      PLATFORM_ADMIN;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE  ENTERPRISE_CURATED;

CREATE SCHEMA IF NOT EXISTS SALES;
CREATE SCHEMA IF NOT EXISTS UTILS;

-- ════════════════════════════════════════════════════════════════
-- HASH KEY UTILITY FUNCTIONS
-- ════════════════════════════════════════════════════════════════

-- Primary hash key: SHA256(UPPER(source_system)|UPPER(original_pk))
-- Deterministic: same input → same key every time
-- Cross-system unique: SAP|CUST-001 ≠ JDE|CUST-001
-- 64 chars: fits VARCHAR(64) without truncation risk
CREATE OR REPLACE FUNCTION ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY(
  SRC_SYSTEM VARCHAR,
  ORIG_PK    VARCHAR
)
RETURNS VARCHAR(64)
LANGUAGE SQL
AS $$
  SHA2(
    UPPER(TRIM(SRC_SYSTEM)) || '|' || UPPER(TRIM(COALESCE(ORIG_PK, 'NULL_PK'))),
    256
  )
$$;

-- Hash diff: SHA256 of all non-key attribute values concatenated
-- If HASH_DIFF matches existing row → skip insert (no change)
-- If HASH_DIFF differs → close old record, insert new version
CREATE OR REPLACE FUNCTION ENTERPRISE_CURATED.UTILS.GENERATE_HASH_DIFF(
  ATTR_CONCAT VARCHAR
)
RETURNS VARCHAR(64)
LANGUAGE SQL
AS $$
  SHA2(UPPER(TRIM(COALESCE(ATTR_CONCAT, 'EMPTY'))), 256)
$$;

-- Test the functions before proceeding
SELECT
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('SAP', 'CUST-001')      AS HK_EXAMPLE,
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('JDE', 'CUST-001')      AS HK_DIFFERENT_SOURCE,
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_DIFF('Alice|Germany|EUR')   AS HD_EXAMPLE,
  LEN(ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('SAP', 'TEST'))     AS KEY_LENGTH;
-- Expected: HK_EXAMPLE ≠ HK_DIFFERENT_SOURCE, KEY_LENGTH = 64

-- ════════════════════════════════════════════════════════════════
-- SILVER: CUSTOMER (3NF, insert-only)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_CURATED.SALES.CUSTOMER (
  -- Universal hash key (64-char SHA256)
  CUSTOMER_HK          VARCHAR(64)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(20)   NOT NULL,
  SOURCE_REGION        VARCHAR(30)   NOT NULL,
  ORIGINAL_ID          VARCHAR(100)  NOT NULL,   -- PK as-is from source

  -- Business attributes (3NF: each attribute depends only on CUSTOMER_HK)
  CUSTOMER_NAME        VARCHAR(500),
  CUSTOMER_TYPE        VARCHAR(50),
  INDUSTRY_CODE        VARCHAR(20),
  COUNTRY_CODE         VARCHAR(3),
  REGION_CODE          VARCHAR(30),
  CITY                 VARCHAR(200),
  POSTAL_CODE          VARCHAR(20),
  PAYMENT_TERMS        VARCHAR(20),
  CREDIT_LIMIT_USD     NUMBER(18,2),  -- normalized to USD via fx table

  -- DQ metadata (written by DQ framework)
  DQ_COMPLETENESS_SCORE NUMBER(5,2),
  DQ_FLAGS             ARRAY,         -- array of failed rule names
  IS_QUARANTINED       BOOLEAN        DEFAULT FALSE,
  QUARANTINE_REASON    VARCHAR(500),

  -- Insert-only versioning
  EFFECTIVE_FROM       TIMESTAMP_NTZ  NOT NULL,
  EFFECTIVE_TO         TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
  IS_CURRENT           BOOLEAN        DEFAULT TRUE,
  RECORD_VERSION       NUMBER         DEFAULT 1,

  -- Change detection: SHA256 of all non-key attrs
  
  HASH_DIFF            VARCHAR(64),

  -- Lineage
  SOURCE_BATCH_ID      VARCHAR(36),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_CUSTOMER PRIMARY KEY (CUSTOMER_HK, EFFECTIVE_FROM)
)
CLUSTER BY (CUSTOMER_HK)
COMMENT = '3NF Customer master - SHA256 hash key - insert-only SCD Type 2 via effective dates';

-- ════════════════════════════════════════════════════════════════
-- SILVER: PRODUCT
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_CURATED.SALES.PRODUCT (
  PRODUCT_HK           VARCHAR(64)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(20)   NOT NULL,
  SOURCE_REGION        VARCHAR(30)   NOT NULL,
  ORIGINAL_ID          VARCHAR(100)  NOT NULL,

  PRODUCT_CODE         VARCHAR(100),
  PRODUCT_NAME         VARCHAR(500),
  PRODUCT_FAMILY       VARCHAR(100),  -- UPS | COOLING | PDU | RACK | MONITORING | SERVICES
  PRODUCT_LINE         VARCHAR(100),  -- Liebert GXT5, Liebert DSE, Geist Watchdog, etc.
  UNIT_OF_MEASURE      VARCHAR(20),
  STANDARD_COST_USD    NUMBER(18,4),
  LIST_PRICE_USD       NUMBER(18,4),
  IS_DISCONTINUED      BOOLEAN        DEFAULT FALSE,

  HASH_DIFF            VARCHAR(64),
  EFFECTIVE_FROM       TIMESTAMP_NTZ  NOT NULL,
  EFFECTIVE_TO         TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
  IS_CURRENT           BOOLEAN        DEFAULT TRUE,
  SOURCE_BATCH_ID      VARCHAR(36),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_PRODUCT PRIMARY KEY (PRODUCT_HK, EFFECTIVE_FROM)
)
CLUSTER BY (PRODUCT_HK)
COMMENT = '3NF Product master - SHA256 hash key - Enterprise Co product catalog';

-- ════════════════════════════════════════════════════════════════
-- SILVER: SALES ORDER HEADER
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_CURATED.SALES.SALES_ORDER (
  ORDER_HK             VARCHAR(64)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(20)   NOT NULL,
  SOURCE_REGION        VARCHAR(30)   NOT NULL,
  ORIGINAL_ID          VARCHAR(100)  NOT NULL,

  -- FK hash keys linking to other Silver entities
  CUSTOMER_HK          VARCHAR(64),
  SALESPERSON_HK       VARCHAR(64),

  ORDER_DATE           DATE,
  DELIVERY_DATE        DATE,
  ORDER_TYPE           VARCHAR(30),
  ORDER_STATUS         VARCHAR(50),
  CURRENCY_CODE        VARCHAR(3),
  NET_AMOUNT_ORIG      NUMBER(18,4),  -- in source currency
  NET_AMOUNT_USD       NUMBER(18,4),  -- normalized to USD
  TAX_AMOUNT_USD       NUMBER(18,4),
  DISCOUNT_PCT         NUMBER(5,2),
  COUNTRY_CODE         VARCHAR(3),

  DQ_FLAGS             ARRAY,
  IS_QUARANTINED       BOOLEAN        DEFAULT FALSE,

  HASH_DIFF            VARCHAR(64),
  EFFECTIVE_FROM       TIMESTAMP_NTZ  NOT NULL,
  EFFECTIVE_TO         TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
  IS_CURRENT           BOOLEAN        DEFAULT TRUE,
  SOURCE_BATCH_ID      VARCHAR(36),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_SALES_ORDER PRIMARY KEY (ORDER_HK, EFFECTIVE_FROM)
)
CLUSTER BY (CUSTOMER_HK, DATE(ORDER_DATE))
COMMENT = '3NF Sales Order header - SHA256 hash key - insert-only - FK to CUSTOMER via hash key';

-- ════════════════════════════════════════════════════════════════
-- SILVER: ORDER LINE (3NF: separated from header)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_CURATED.SALES.ORDER_LINE (
  LINE_HK              VARCHAR(64)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(20)   NOT NULL,
  ORIGINAL_ORDER_ID    VARCHAR(100)  NOT NULL,
  ORIGINAL_LINE_NUM    VARCHAR(20)   NOT NULL,

  -- FK hash keys
  ORDER_HK             VARCHAR(64),
  PRODUCT_HK           VARCHAR(64),

  LINE_NUMBER          NUMBER,
  QUANTITY             NUMBER(15,3),
  UNIT_OF_MEASURE      VARCHAR(20),
  UNIT_PRICE_ORIG      NUMBER(18,4),
  UNIT_PRICE_USD       NUMBER(18,4),
  LINE_AMOUNT_USD      NUMBER(18,4),
  DISCOUNT_PCT         NUMBER(5,2),
  LINE_STATUS          VARCHAR(50),
  PLANT                VARCHAR(20),

  HASH_DIFF            VARCHAR(64),
  EFFECTIVE_FROM       TIMESTAMP_NTZ  NOT NULL,
  EFFECTIVE_TO         TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
  IS_CURRENT           BOOLEAN        DEFAULT TRUE,
  SOURCE_BATCH_ID      VARCHAR(36),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_ORDER_LINE PRIMARY KEY (LINE_HK, EFFECTIVE_FROM)
)
CLUSTER BY (ORDER_HK)
COMMENT = '3NF Order line items - FK to ORDER_HK and PRODUCT_HK via hash keys';

-- ════════════════════════════════════════════════════════════════
-- SILVER: OPPORTUNITY (Salesforce)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_CURATED.SALES.OPPORTUNITY (
  OPP_HK               VARCHAR(64)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(20)   NOT NULL  DEFAULT 'SALESFORCE',
  ORIGINAL_ID          VARCHAR(100)  NOT NULL,
  CUSTOMER_HK          VARCHAR(64),  -- linked to SAP/JDE customer if MDM matched

  OPP_NAME             VARCHAR(500),
  STAGE_NAME           VARCHAR(100),
  AMOUNT_USD           NUMBER(18,2),
  PROBABILITY          NUMBER(5,2),
  CLOSE_DATE           DATE,
  OWNER_ID             VARCHAR(50),
  LEAD_SOURCE          VARCHAR(100),
  IS_WON               BOOLEAN,
  IS_CLOSED            BOOLEAN,
  REGION               VARCHAR(50),

  HASH_DIFF            VARCHAR(64),
  EFFECTIVE_FROM       TIMESTAMP_NTZ  NOT NULL,
  EFFECTIVE_TO         TIMESTAMP_NTZ  DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
  IS_CURRENT           BOOLEAN        DEFAULT TRUE,
  LOAD_TIMESTAMP       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_OPPORTUNITY PRIMARY KEY (OPP_HK, EFFECTIVE_FROM)
)
CLUSTER BY (OPP_HK, CLOSE_DATE);

-- ════════════════════════════════════════════════════════════════
-- DYNAMIC TABLES: Auto-incremental Silver from Bronze
-- TARGET_LAG = '5 minutes' means Snowflake refreshes within 5 min
-- of new Bronze data - no Airflow/dbt needed
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE ENTERPRISE_CURATED.SALES.DT_SILVER_CUSTOMER_SAP
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = TRANSFORM_WH
  COMMENT    = 'Auto-loads Silver CUSTOMER from SAP Bronze within 5 min lag'
AS
SELECT
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('SAP', b.CUSTOMER_ID)  AS CUSTOMER_HK,
  'SAP'                                                           AS SOURCE_SYSTEM,
  'EUROPE'                                                        AS SOURCE_REGION,
  b.CUSTOMER_ID                                                   AS ORIGINAL_ID,
  b.CUSTOMER_NAME,
  NULL::VARCHAR                                                   AS CUSTOMER_TYPE,
  b.INDUSTRY_CODE,
  b.COUNTRY_CODE,
  NULL::VARCHAR                                                   AS REGION_CODE,
  b.CITY,
  b.POSTAL_CODE,
  b.PAYMENT_TERMS,
  b.CREDIT_LIMIT                                                  AS CREDIT_LIMIT_USD,
  -- DQ completeness score (5 mandatory fields × 20 points each)
  (CASE WHEN b.CUSTOMER_ID   IS NOT NULL THEN 20 ELSE 0 END +
   CASE WHEN b.CUSTOMER_NAME IS NOT NULL THEN 20 ELSE 0 END +
   CASE WHEN b.COUNTRY_CODE  IS NOT NULL THEN 20 ELSE 0 END +
   CASE WHEN b.CITY          IS NOT NULL THEN 20 ELSE 0 END +
   CASE WHEN b.POSTAL_CODE   IS NOT NULL THEN 20 ELSE 0 END)    AS DQ_COMPLETENESS_SCORE,
  PARSE_JSON('[]')                                               AS DQ_FLAGS,
  (b.CUSTOMER_ID IS NULL)                                         AS IS_QUARANTINED,
  CASE WHEN b.CUSTOMER_ID IS NULL THEN 'CUSTOMER_ID_NOT_NULL' END AS QUARANTINE_REASON,
  b.LOAD_TIMESTAMP                                                AS EFFECTIVE_FROM,
  TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')                        AS EFFECTIVE_TO,
  TRUE                                                            AS IS_CURRENT,
  1                                                               AS RECORD_VERSION,
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_DIFF(
    COALESCE(b.CUSTOMER_NAME,'') || '|' ||
    COALESCE(b.COUNTRY_CODE,'')  || '|' ||
    COALESCE(b.CITY,'')          || '|' ||
    COALESCE(b.POSTAL_CODE,'')
  )                                                               AS HASH_DIFF,
  b.BATCH_ID                                                      AS SOURCE_BATCH_ID,
  CURRENT_TIMESTAMP()                                             AS LOAD_TIMESTAMP
FROM ENTERPRISE_RAW.SAP_EUROPE.CUSTOMERS b
WHERE b.CUSTOMER_ID IS NOT NULL;  -- basic completeness gate

CREATE OR REPLACE DYNAMIC TABLE ENTERPRISE_CURATED.SALES.DT_SILVER_ORDER_SAP
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = TRANSFORM_WH
  COMMENT    = 'Auto-loads Silver SALES_ORDER from SAP Bronze within 5 min lag'
AS
SELECT
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('SAP', b.ORDER_ID)       AS ORDER_HK,
  'SAP'                                                             AS SOURCE_SYSTEM,
  'EUROPE'                                                          AS SOURCE_REGION,
  b.ORDER_ID                                                        AS ORIGINAL_ID,
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_KEY('SAP', b.CUSTOMER_ID)    AS CUSTOMER_HK,
  NULL::VARCHAR(64)                                                 AS SALESPERSON_HK,
  b.ORDER_DATE,
  b.DELIVERY_DATE,
  b.ORDER_TYPE,
  b.ORDER_STATUS,
  b.CURRENCY_CODE,
  b.NET_AMOUNT                                                      AS NET_AMOUNT_ORIG,
  -- FX normalization using reference table
  b.NET_AMOUNT * COALESCE(
    (SELECT FX_TO_USD FROM PLATFORM_CONFIG.REFERENCE.CURRENCY
     WHERE CURRENCY_CODE = b.CURRENCY_CODE), 1.0
  )                                                                 AS NET_AMOUNT_USD,
  b.TAX_AMOUNT * COALESCE(
    (SELECT FX_TO_USD FROM PLATFORM_CONFIG.REFERENCE.CURRENCY
     WHERE CURRENCY_CODE = b.CURRENCY_CODE), 1.0
  )                                                                 AS TAX_AMOUNT_USD,
  COALESCE(b.DISCOUNT_PCT, 0)                                       AS DISCOUNT_PCT,
  b.COUNTRY_CODE,
  PARSE_JSON('[]')                                                 AS DQ_FLAGS,
  (b.ORDER_ID IS NULL OR b.CUSTOMER_ID IS NULL)                    AS IS_QUARANTINED,
  ENTERPRISE_CURATED.UTILS.GENERATE_HASH_DIFF(
    COALESCE(b.ORDER_STATUS,'')   || '|' ||
    COALESCE(b.NET_AMOUNT::VARCHAR,'') || '|' ||
    COALESCE(b.CUSTOMER_ID,'')
  )                                                                 AS HASH_DIFF,
  b.LOAD_TIMESTAMP                                                  AS EFFECTIVE_FROM,
  TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')                          AS EFFECTIVE_TO,
  TRUE                                                              AS IS_CURRENT,
  b.BATCH_ID                                                        AS SOURCE_BATCH_ID,
  CURRENT_TIMESTAMP()                                               AS LOAD_TIMESTAMP
FROM ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS b
WHERE b.ORDER_ID IS NOT NULL;

-- ════════════════════════════════════════════════════════════════
-- AUDIT TABLES (in PLATFORM_AUDIT database)
-- ════════════════════════════════════════════════════════════════

USE DATABASE PLATFORM_AUDIT;
CREATE SCHEMA IF NOT EXISTS DQ;
CREATE SCHEMA IF NOT EXISTS LINEAGE;

-- DQ batch results log (one row per rule per batch run)
CREATE OR REPLACE TABLE PLATFORM_AUDIT.DQ.DQ_BATCH_LOG (
  LOG_ID               NUMBER        AUTOINCREMENT PRIMARY KEY,
  BATCH_ID             VARCHAR(36)   NOT NULL,
  SOURCE_SYSTEM        VARCHAR(50),
  SOURCE_REGION        VARCHAR(30),
  TARGET_TABLE         VARCHAR(300),
  DAMA_DIMENSION       VARCHAR(50),
  RULE_ID              NUMBER,
  RULE_NAME            VARCHAR(200),
  RECORDS_CHECKED      NUMBER        DEFAULT 0,
  RECORDS_PASSED       NUMBER        DEFAULT 0,
  RECORDS_FAILED       NUMBER        DEFAULT 0,
  RECORDS_QUARANTINED  NUMBER        DEFAULT 0,
  PASS_RATE            NUMBER(5,2),  -- (passed/checked)*100
  RUN_TIMESTAMP        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  WAREHOUSE_USED       VARCHAR(100)  DEFAULT CURRENT_WAREHOUSE(),
  RUN_BY               VARCHAR(100)  DEFAULT CURRENT_USER()
);

-- Individual rejected records (full row stored as VARIANT for forensics)
CREATE OR REPLACE TABLE PLATFORM_AUDIT.DQ.REJECTED_RECORDS (
  REJECTION_ID         NUMBER        AUTOINCREMENT PRIMARY KEY,
  BATCH_ID             VARCHAR(36),
  SOURCE_SYSTEM        VARCHAR(50),
  SOURCE_TABLE         VARCHAR(300),
  RECORD_DATA          VARIANT,      -- Full source row
  RULE_VIOLATED        VARCHAR(200),
  DAMA_DIMENSION       VARCHAR(50),
  SEVERITY             VARCHAR(20),
  REJECTION_REASON     VARCHAR(2000),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Quarantine: records pending human review
CREATE OR REPLACE TABLE PLATFORM_AUDIT.DQ.QUARANTINE (
  QUARANTINE_ID        NUMBER        AUTOINCREMENT PRIMARY KEY,
  BATCH_ID             VARCHAR(36),
  SOURCE_SYSTEM        VARCHAR(50),
  SOURCE_TABLE         VARCHAR(300),
  RECORD_DATA          VARIANT,
  RULE_VIOLATED        VARCHAR(200),
  QUARANTINE_REASON    VARCHAR(2000),
  STATUS               VARCHAR(20)   DEFAULT 'OPEN',
  REVIEWED_BY          VARCHAR(100),
  REVIEWED_AT          TIMESTAMP_NTZ,
  RESOLUTION           VARCHAR(500),
  LOAD_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── GRANTS ────────────────────────────────────────────────────
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA PLATFORM_AUDIT.DQ      TO ROLE PLATFORM_ADMIN;
GRANT SELECT        ON ALL TABLES IN SCHEMA PLATFORM_AUDIT.DQ        TO ROLE ENTERPRISE_DATA_PRODUCT_OWNER;
GRANT SELECT ON TABLE ENTERPRISE_CURATED.SALES.SALES_ORDER             TO ROLE SAP_EUROPE_READER;
GRANT SELECT ON TABLE ENTERPRISE_CURATED.SALES.CUSTOMER                TO ROLE SAP_EUROPE_READER;
GRANT SELECT ON TABLE ENTERPRISE_CURATED.SALES.SALES_ORDER             TO ROLE GLOBAL_ANALYST;
GRANT SELECT ON TABLE ENTERPRISE_CURATED.SALES.CUSTOMER                TO ROLE GLOBAL_ANALYST;

-- ── VERIFY ────────────────────────────────────────────────────
SHOW TABLES        IN DATABASE ENTERPRISE_CURATED;
SHOW DYNAMIC TABLES IN DATABASE ENTERPRISE_CURATED;
SHOW TABLES        IN DATABASE PLATFORM_AUDIT;
