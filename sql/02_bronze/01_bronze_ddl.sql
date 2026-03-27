-- ================================================================
-- FILE: 04_bronze_ddl.sql
-- PURPOSE: Bronze layer - all raw ingestion tables, stages, pipes, streams
-- ROLE: PLATFORM_ADMIN
-- RUN ORDER: 4th
-- DESIGN: Schema-on-write for ERP (known schemas) + VARIANT for events
--         Every row has: load_id, load_timestamp, batch_id, raw_record
-- ================================================================

USE ROLE      PLATFORM_ADMIN;
USE WAREHOUSE INGEST_WH;
USE DATABASE  ENTERPRISE_RAW;

-- ── SCHEMAS (one per source = RBAC boundary) ──────────────────
CREATE SCHEMA IF NOT EXISTS SAP_EUROPE;
CREATE SCHEMA IF NOT EXISTS JDE_AMERICAS;
CREATE SCHEMA IF NOT EXISTS QAD_EMEA;
CREATE SCHEMA IF NOT EXISTS SALESFORCE_GLOBAL;
CREATE SCHEMA IF NOT EXISTS REALTIME;
CREATE SCHEMA IF NOT EXISTS STAGE;

-- Schema-level grants (Row Access Policy filters rows within schemas)
GRANT USAGE ON SCHEMA ENTERPRISE_RAW.SAP_EUROPE       TO ROLE SAP_EUROPE_READER;
GRANT USAGE ON SCHEMA ENTERPRISE_RAW.JDE_AMERICAS     TO ROLE JDE_AMERICAS_READER;
GRANT USAGE ON SCHEMA ENTERPRISE_RAW.QAD_EMEA         TO ROLE QAD_EMEA_READER;

-- ════════════════════════════════════════════════════════════════
-- SAP S/4HANA — EUROPE
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS (
  -- Business columns (SAP field → readable name)
  ORDER_ID           VARCHAR(50),    -- VBELN: Sales Document Number
  ORDER_TYPE         VARCHAR(10),    -- AUART: TA=Standard, KA=Consignment, RE=Returns
  ORDER_DATE         DATE,           -- AUDAT: Document Date
  DELIVERY_DATE      DATE,           -- EDATU: Requested Delivery Date
  CUSTOMER_ID        VARCHAR(50),    -- KUNNR: Sold-to party
  SHIP_TO_PARTY      VARCHAR(50),    -- KUNNR from VBPA: Ship-to
  SALES_ORG          VARCHAR(10),    -- VKORG: Sales Organisation
  DISTRIBUTION_CH    VARCHAR(10),    -- VTWEG: Distribution Channel
  DIVISION           VARCHAR(10),    -- SPART: Division
  ORDER_STATUS       VARCHAR(30),    -- GBSTA: Overall processing status
  CURRENCY_CODE      VARCHAR(3),     -- WAERK: Document currency
  NET_AMOUNT         NUMBER(18,4),   -- NETWR: Net value
  TAX_AMOUNT         NUMBER(18,4),   -- MWSBP: Tax amount
  DISCOUNT_PCT       NUMBER(5,2),
  COUNTRY_CODE       VARCHAR(3),
  REGION_CODE        VARCHAR(10),
  CREATED_BY         VARCHAR(50),    -- ERNAM
  CHANGED_DATE       TIMESTAMP_NTZ,  -- AEDAT
  -- Bronze standard metadata (every Bronze table has these exact columns)
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'SAP',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'EUROPE',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  FILE_NAME          VARCHAR(500),
  ROW_NUMBER         NUMBER,
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  CHANGE_TYPE        VARCHAR(10)     DEFAULT 'INSERT',   -- INSERT|UPDATE|DELETE
  RAW_RECORD         VARIANT         -- Full JSON for forensic replay
)
CLUSTER BY (LOAD_DATE, ORDER_DATE)
COMMENT = 'SAP VBAK Sales Order header - Europe region - Bronze raw - schema-on-write';

CREATE OR REPLACE TABLE ENTERPRISE_RAW.SAP_EUROPE.ORDER_ITEMS (
  ORDER_ID           VARCHAR(50),    -- VBELN
  LINE_ITEM          NUMBER,         -- POSNR: Item number
  PRODUCT_CODE       VARCHAR(50),    -- MATNR: Material
  PRODUCT_DESC       VARCHAR(500),   -- ARKTX: Description
  QUANTITY           NUMBER(15,3),   -- KWMENG: Order quantity
  UNIT_OF_MEASURE    VARCHAR(10),    -- VRKME: Sales unit
  UNIT_PRICE         NUMBER(18,4),   -- NETPR: Net price
  LINE_AMOUNT        NUMBER(18,4),   -- NETWR: Net value
  DISCOUNT_PCT       NUMBER(5,2),
  CURRENCY_CODE      VARCHAR(3),
  PLANT              VARCHAR(10),    -- WERKS: Plant
  ITEM_STATUS        VARCHAR(30),
  -- Bronze metadata
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'SAP',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'EUROPE',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE, ORDER_ID)
COMMENT = 'SAP VBAP Sales Order items - Europe - Bronze raw';

CREATE OR REPLACE TABLE ENTERPRISE_RAW.SAP_EUROPE.CUSTOMERS (
  CUSTOMER_ID        VARCHAR(50),    -- KUNNR
  CUSTOMER_NAME      VARCHAR(500),   -- NAME1
  CUSTOMER_TYPE      VARCHAR(20),
  STREET             VARCHAR(200),   -- STRAS
  CITY               VARCHAR(200),   -- ORT01
  POSTAL_CODE        VARCHAR(20),    -- PSTLZ
  COUNTRY_CODE       VARCHAR(3),     -- LAND1
  PHONE              VARCHAR(50),    -- TELF1 (PII)
  EMAIL              VARCHAR(200),   -- SMTP_ADDR (PII)
  TAX_NUMBER         VARCHAR(50),    -- STCD1 (sensitive)
  CREDIT_LIMIT       NUMBER(18,2),   -- KLIMK
  PAYMENT_TERMS      VARCHAR(10),    -- ZTERM
  INDUSTRY_CODE      VARCHAR(10),
  SALES_DISTRICT     VARCHAR(20),
  -- Bronze metadata
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'SAP',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'EUROPE',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE)
COMMENT = 'SAP KNA1 Customer master - Europe - Bronze raw - CONTAINS PII';

-- ════════════════════════════════════════════════════════════════
-- JDE E1 — AMERICAS
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_RAW.JDE_AMERICAS.SALES_ORDERS (
  ORDER_ID           VARCHAR(50),    -- SDDOCO: Order Number
  ORDER_TYPE         VARCHAR(10),    -- SDDCTO: Document Type
  ORDER_DATE         DATE,           -- SDRQAJ: Requested date (Julian converted)
  CUSTOMER_ID        VARCHAR(50),    -- SDAN8: Sold-to Address Book Number
  SHIP_TO_ID         VARCHAR(50),    -- SDSHAN8
  BRANCH_PLANT       VARCHAR(20),    -- SDMCU: Business Unit
  STATUS_CODE        VARCHAR(10),    -- SDNXTR: Next status
  QUANTITY_ORDERED   NUMBER(15,3),   -- SDUORG
  QUANTITY_SHIPPED   NUMBER(15,3),   -- SDSOQS
  UNIT_PRICE         NUMBER(18,4),   -- SDUPRC
  EXTENDED_PRICE     NUMBER(18,4),   -- SDAEXP
  CURRENCY_CODE      VARCHAR(3),     -- SDCRCD
  COUNTRY_CODE       VARCHAR(3),
  -- Bronze metadata
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'JDE',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'AMERICAS',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE, ORDER_DATE)
COMMENT = 'JDE F4211 Sales Detail - Americas - Bronze raw';

CREATE OR REPLACE TABLE ENTERPRISE_RAW.JDE_AMERICAS.CUSTOMERS (
  CUSTOMER_ID        VARCHAR(50),    -- AN8: Address Book Number
  CUSTOMER_NAME      VARCHAR(500),   -- ALPH
  ADDRESS_LINE_1     VARCHAR(200),   -- ADD1 (PII)
  CITY               VARCHAR(200),   -- CTY1
  STATE_CODE         VARCHAR(10),    -- ADDS
  ZIP_CODE           VARCHAR(20),    -- ADDZ
  COUNTRY_CODE       VARCHAR(3),     -- CTR
  PHONE              VARCHAR(50),    -- PH (PII)
  TAX_ID             VARCHAR(50),    -- TAX (sensitive)
  CREDIT_LIMIT       NUMBER(18,2),   -- ACL
  PAYMENT_TERMS      VARCHAR(10),    -- PTC
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'JDE',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'AMERICAS',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE)
COMMENT = 'JDE F0101 Address Book customers - Americas - Bronze raw - CONTAINS PII';

-- ════════════════════════════════════════════════════════════════
-- QAD ERP — EMEA
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_RAW.QAD_EMEA.SALES_ORDERS (
  ORDER_ID           VARCHAR(50),    -- so_nbr
  ORDER_DATE         DATE,           -- so_ord_date
  CUSTOMER_ID        VARCHAR(50),    -- so_cust_id
  SITE               VARCHAR(20),    -- so_site
  ORDER_STATUS       VARCHAR(30),    -- so_status
  ORDER_TYPE         VARCHAR(20),    -- so_type
  CURRENCY_CODE      VARCHAR(3),     -- so_curr
  TOTAL_AMOUNT       NUMBER(18,4),   -- so_total
  DISCOUNT_PCT       NUMBER(5,2),    -- so_disc_pct
  COUNTRY_CODE       VARCHAR(3),
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'QAD',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'EMEA',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE, ORDER_DATE)
COMMENT = 'QAD so_mstr Sales Orders - EMEA - Bronze raw';

CREATE OR REPLACE TABLE ENTERPRISE_RAW.QAD_EMEA.CUSTOMERS (
  CUSTOMER_ID        VARCHAR(50),    -- cust_id
  CUSTOMER_NAME      VARCHAR(500),   -- cust_name
  CITY               VARCHAR(200),
  COUNTRY_CODE       VARCHAR(3),     -- cust_country
  PHONE              VARCHAR(50),    -- (PII)
  CREDIT_LIMIT       NUMBER(18,2),
  PAYMENT_TERMS      VARCHAR(20),
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'QAD',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'EMEA',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE);

-- ════════════════════════════════════════════════════════════════
-- SALESFORCE CRM — GLOBAL
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_RAW.SALESFORCE_GLOBAL.OPPORTUNITIES (
  OPP_ID             VARCHAR(50),    -- Id (18-char SFDC ID)
  OPP_NAME           VARCHAR(500),   -- Name
  ACCOUNT_ID         VARCHAR(50),    -- AccountId
  STAGE_NAME         VARCHAR(100),   -- StageName
  AMOUNT             NUMBER(18,2),   -- Amount
  PROBABILITY        NUMBER(5,2),    -- Probability
  CLOSE_DATE         DATE,           -- CloseDate
  CREATED_DATE       TIMESTAMP_NTZ,  -- CreatedDate
  MODIFIED_DATE      TIMESTAMP_NTZ,  -- LastModifiedDate
  OWNER_ID           VARCHAR(50),    -- OwnerId
  LEAD_SOURCE        VARCHAR(100),   -- LeadSource
  IS_WON             BOOLEAN,        -- IsWon
  IS_CLOSED          BOOLEAN,        -- IsClosed
  REGION             VARCHAR(50),
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'SALESFORCE',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'GLOBAL',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE, CLOSE_DATE)
COMMENT = 'Salesforce Opportunity - Global - Bronze raw';

CREATE OR REPLACE TABLE ENTERPRISE_RAW.SALESFORCE_GLOBAL.ACCOUNTS (
  ACCOUNT_ID         VARCHAR(50),
  ACCOUNT_NAME       VARCHAR(500),   -- (PII-adjacent)
  INDUSTRY           VARCHAR(100),
  ANNUAL_REVENUE     NUMBER(18,2),
  EMPLOYEE_COUNT     NUMBER,
  BILLING_COUNTRY    VARCHAR(3),
  REGION             VARCHAR(50),
  SOURCE_SYSTEM      VARCHAR(20)     DEFAULT 'SALESFORCE',
  SOURCE_REGION      VARCHAR(20)     DEFAULT 'GLOBAL',
  LOAD_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE(),
  BATCH_ID           VARCHAR(36),
  IS_DELETED         BOOLEAN         DEFAULT FALSE,
  RAW_RECORD         VARIANT
)
CLUSTER BY (LOAD_DATE);

-- ════════════════════════════════════════════════════════════════
-- REAL-TIME KAFKA EVENTS — Schema-on-read (VARIANT payload)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_RAW.REALTIME.ORDER_EVENTS (
  EVENT_ID           VARCHAR(36)     NOT NULL,
  KAFKA_TOPIC        VARCHAR(200),
  KAFKA_PARTITION    NUMBER,
  KAFKA_OFFSET       NUMBER,
  EVENT_TIMESTAMP    TIMESTAMP_NTZ,
  EVENT_TYPE         VARCHAR(50),    -- ORDER_CREATED|UPDATED|SHIPPED|CANCELLED
  SOURCE_SYSTEM      VARCHAR(20),
  EVENT_PAYLOAD      VARIANT,        -- Full JSON event - schema-on-read
  LOAD_TIMESTAMP     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  LOAD_DATE          DATE            DEFAULT CURRENT_DATE()
)
CLUSTER BY (DATE(EVENT_TIMESTAMP))
COMMENT = 'Kafka order events - real-time via Snowpipe Streaming - schema-on-read VARIANT';

-- ════════════════════════════════════════════════════════════════
-- STAGES, FILE FORMATS, PIPES
-- ════════════════════════════════════════════════════════════════

-- Internal stage for CSV file drops
CREATE OR REPLACE STAGE ENTERPRISE_RAW.STAGE.ERP_LANDING
  COMMENT = 'Internal stage for ERP batch CSV files. In prod: replace with external S3 stage.';

-- File formats
CREATE OR REPLACE FILE FORMAT ENTERPRISE_RAW.STAGE.CSV_FORMAT
  TYPE                    = 'CSV'
  SKIP_HEADER             = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL     = TRUE
  DATE_FORMAT             = 'AUTO'
  TIMESTAMP_FORMAT        = 'AUTO'
  NULL_IF                 = ('NULL','null','','N/A');

CREATE OR REPLACE FILE FORMAT ENTERPRISE_RAW.STAGE.JSON_FORMAT
  TYPE                    = 'JSON'
  STRIP_OUTER_ARRAY       = TRUE
  ENABLE_OCTAL            = FALSE;

-- Snowpipe for SAP orders
CREATE OR REPLACE PIPE ENTERPRISE_RAW.STAGE.PIPE_SAP_ORDERS
  AUTO_INGEST = FALSE    -- set TRUE + add SQS notification when S3 is connected
  COMMENT     = 'Snowpipe for SAP Europe sales orders CSV'
AS
COPY INTO ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS
  (ORDER_ID, ORDER_DATE, CUSTOMER_ID, CURRENCY_CODE, NET_AMOUNT, ORDER_STATUS, BATCH_ID, FILE_NAME)
FROM (
  SELECT $1, TRY_TO_DATE($2), $3, $4, TRY_TO_NUMBER($5), $6,
         METADATA$FILENAME, METADATA$FILENAME
  FROM @ENTERPRISE_RAW.STAGE.ERP_LANDING/sap_europe/orders/
)
FILE_FORMAT = (FORMAT_NAME = 'ENTERPRISE_RAW.STAGE.CSV_FORMAT')
ON_ERROR    = 'CONTINUE';

-- ════════════════════════════════════════════════════════════════
-- BRONZE STREAMS (feed Silver Dynamic Tables)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE STREAM ENTERPRISE_RAW.SAP_EUROPE.STR_SALES_ORDERS
  ON TABLE ENTERPRISE_RAW.SAP_EUROPE.SALES_ORDERS
  APPEND_ONLY = TRUE
  COMMENT     = 'CDC stream → Silver DT_SILVER_ORDER_SAP';

CREATE OR REPLACE STREAM ENTERPRISE_RAW.SAP_EUROPE.STR_CUSTOMERS
  ON TABLE ENTERPRISE_RAW.SAP_EUROPE.CUSTOMERS
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM ENTERPRISE_RAW.JDE_AMERICAS.STR_SALES_ORDERS
  ON TABLE ENTERPRISE_RAW.JDE_AMERICAS.SALES_ORDERS
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM ENTERPRISE_RAW.QAD_EMEA.STR_SALES_ORDERS
  ON TABLE ENTERPRISE_RAW.QAD_EMEA.SALES_ORDERS
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM ENTERPRISE_RAW.SALESFORCE_GLOBAL.STR_OPPORTUNITIES
  ON TABLE ENTERPRISE_RAW.SALESFORCE_GLOBAL.OPPORTUNITIES
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM ENTERPRISE_RAW.REALTIME.STR_ORDER_EVENTS
  ON TABLE ENTERPRISE_RAW.REALTIME.ORDER_EVENTS
  APPEND_ONLY = TRUE;

-- ── TABLE GRANTS ──────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.SAP_EUROPE       TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.JDE_AMERICAS     TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.QAD_EMEA         TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.SALESFORCE_GLOBAL TO ROLE PLATFORM_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.REALTIME          TO ROLE PLATFORM_ADMIN;

-- Regional readers can see their schema's tables (row policy filters rows)
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.SAP_EUROPE       TO ROLE SAP_EUROPE_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.JDE_AMERICAS     TO ROLE JDE_AMERICAS_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_RAW.QAD_EMEA         TO ROLE QAD_EMEA_READER;

-- ── VERIFY ────────────────────────────────────────────────────
SHOW TABLES IN DATABASE ENTERPRISE_RAW;
SHOW STREAMS IN DATABASE ENTERPRISE_RAW;
