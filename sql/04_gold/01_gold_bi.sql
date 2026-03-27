-- ================================================================
-- FILE: 06a_gold_bi.sql
-- PURPOSE: Gold BI zone - star schema, dim tables, fact table,
--          materialized views, search optimization
-- ROLE: PLATFORM_ADMIN
-- RUN ORDER: 6th (part a)
-- ================================================================

USE ROLE      PLATFORM_ADMIN;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE  ENTERPRISE_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS BI;
CREATE SCHEMA IF NOT EXISTS ML_FEATURES;
CREATE SCHEMA IF NOT EXISTS AI;

-- ════════════════════════════════════════════════════════════════
-- DIM_DATE: Pre-populated static dimension (2015-2030)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.BI.DIM_DATE (
  DATE_KEY        NUMBER        NOT NULL PRIMARY KEY,  -- YYYYMMDD integer for fast joins
  CALENDAR_DATE   DATE          NOT NULL UNIQUE,
  DAY_OF_WEEK     NUMBER,       -- 0=Sunday, 6=Saturday
  DAY_NAME        VARCHAR(20),
  DAY_OF_MONTH    NUMBER,
  WEEK_OF_YEAR    NUMBER,
  MONTH_NUMBER    NUMBER,
  MONTH_NAME      VARCHAR(20),
  QUARTER_NUMBER  NUMBER,
  QUARTER_NAME    VARCHAR(6),   -- Q1, Q2, Q3, Q4
  YEAR_NUMBER     NUMBER,
  IS_WEEKEND      BOOLEAN,
  IS_HOLIDAY      BOOLEAN DEFAULT FALSE,
  FISCAL_QUARTER  NUMBER,       -- Enterprise Co fiscal calendar (same as calendar for now)
  FISCAL_YEAR     NUMBER
)
;

-- Populate DIM_DATE for 2015-2030 (~5,479 rows)
INSERT INTO ENTERPRISE_ANALYTICS.BI.DIM_DATE
WITH DATE_SPINE AS (
  SELECT DATEADD(DAY, SEQ4(), '2015-01-01') AS D
  FROM TABLE(GENERATOR(ROWCOUNT => 5479))
)
SELECT
  TO_NUMBER(TO_CHAR(D, 'YYYYMMDD'))                   AS DATE_KEY,
  D                                                    AS CALENDAR_DATE,
  DAYOFWEEK(D)                                         AS DAY_OF_WEEK,
  DAYNAME(D)                                           AS DAY_NAME,
  DAY(D)                                               AS DAY_OF_MONTH,
  WEEKOFYEAR(D)                                        AS WEEK_OF_YEAR,
  MONTH(D)                                             AS MONTH_NUMBER,
  MONTHNAME(D)                                         AS MONTH_NAME,
  QUARTER(D)                                           AS QUARTER_NUMBER,
  'Q' || QUARTER(D)                                   AS QUARTER_NAME,
  YEAR(D)                                              AS YEAR_NUMBER,
  DAYOFWEEK(D) IN (0,6)                               AS IS_WEEKEND,
  FALSE                                                AS IS_HOLIDAY,
  QUARTER(D)                                           AS FISCAL_QUARTER,
  YEAR(D)                                              AS FISCAL_YEAR
FROM DATE_SPINE;

-- ════════════════════════════════════════════════════════════════
-- DIM_REGION: Static region / ERP mapping
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.BI.DIM_REGION (
  REGION_SK       NUMBER        AUTOINCREMENT PRIMARY KEY,
  REGION_HK       VARCHAR(64),  -- SHA256(region|sub_region) for Silver FK
  REGION_NAME     VARCHAR(100),
  SUB_REGION      VARCHAR(100),
  COUNTRY_CODE    VARCHAR(3),
  COUNTRY_NAME    VARCHAR(100),
  ERP_SYSTEM      VARCHAR(20),
  LOAD_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO ENTERPRISE_ANALYTICS.BI.DIM_REGION
  (REGION_HK, REGION_NAME, SUB_REGION, COUNTRY_CODE, COUNTRY_NAME, ERP_SYSTEM)
SELECT SHA2(v.r), v.n, v.s, v.c, v.cn, v.e FROM (
  SELECT 'EUROPE|DACH' r,'Europe' n,'DACH' s,'DEU' c,'Germany' cn,'SAP' e UNION ALL
  SELECT 'EUROPE|UK','Europe','UK & Ireland','GBR','United Kingdom','SAP' UNION ALL
  SELECT 'EUROPE|BENELUX','Europe','Benelux','NLD','Netherlands','SAP' UNION ALL
  SELECT 'EUROPE|NORDICS','Europe','Nordics','SWE','Sweden','SAP' UNION ALL
  SELECT 'EUROPE|SOUTH','Europe','Southern EU','ESP','Spain','SAP' UNION ALL
  SELECT 'AMERICAS|US','Americas','United States','USA','United States','JDE' UNION ALL
  SELECT 'AMERICAS|CA','Americas','Canada','CAN','Canada','JDE' UNION ALL
  SELECT 'AMERICAS|LATAM','Americas','Latin America','BRA','Brazil','JDE' UNION ALL
  SELECT 'EMEA|GULF','EMEA','Gulf','ARE','UAE','QAD' UNION ALL
  SELECT 'EMEA|AFRICA','EMEA','Africa','ZAF','South Africa','QAD'
) v;

-- ════════════════════════════════════════════════════════════════
-- DIM_PRODUCT: Conformed from Silver PRODUCT hash key
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.BI.DIM_PRODUCT (
  PRODUCT_SK       NUMBER        AUTOINCREMENT PRIMARY KEY,
  PRODUCT_HK       VARCHAR(64),  -- FK to Silver PRODUCT
  PRODUCT_CODE     VARCHAR(100),
  PRODUCT_NAME     VARCHAR(500),
  PRODUCT_FAMILY   VARCHAR(100), -- UPS | COOLING | PDU | RACK | MONITORING | SERVICES
  PRODUCT_LINE     VARCHAR(100),
  UNIT_OF_MEASURE  VARCHAR(20),
  LIST_PRICE_USD   NUMBER(18,4),
  IS_DISCONTINUED  BOOLEAN,
  EFFECTIVE_FROM   DATE,
  EFFECTIVE_TO     DATE,
  IS_CURRENT       BOOLEAN        DEFAULT TRUE,
  LOAD_TIMESTAMP   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Seed Enterprise Co product catalog
INSERT INTO ENTERPRISE_ANALYTICS.BI.DIM_PRODUCT
  (PRODUCT_HK, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_FAMILY, PRODUCT_LINE,
   UNIT_OF_MEASURE, LIST_PRICE_USD, IS_DISCONTINUED, EFFECTIVE_FROM, IS_CURRENT)
SELECT SHA2(v.c), v.c, v.n, v.f, v.l, v.u, v.p, v.d, v.e, TRUE
FROM (
  SELECT 'UPS-GXT5-10K' c,'Liebert GXT5 10kVA UPS' n,'UPS' f,'Liebert GXT5' l,'EA' u,8500 p,FALSE d,'2020-01-01' e UNION ALL
  SELECT 'UPS-GXT5-20K','Liebert GXT5 20kVA UPS','UPS','Liebert GXT5','EA',14500,FALSE,'2020-01-01' UNION ALL
  SELECT 'UPS-EXM-80K','Liebert EXM 80kVA UPS','UPS','Liebert EXM','EA',42000,FALSE,'2020-01-01' UNION ALL
  SELECT 'COOL-DSE-035','Liebert DSE 35kW Cool','COOLING','Liebert DSE','EA',18000,FALSE,'2020-01-01' UNION ALL
  SELECT 'COOL-PCW-030','Liebert PCW 30kW Cool','COOLING','Liebert PCW','EA',22000,FALSE,'2020-01-01' UNION ALL
  SELECT 'PDU-GW-24A','Geist Watchdog 24A PDU','PDU','Geist Watchdog','EA',1200,FALSE,'2020-01-01' UNION ALL
  SELECT 'PDU-GW-48A-M','Geist Watchdog 48A PDU','PDU','Geist Watchdog','EA',2100,FALSE,'2020-01-01' UNION ALL
  SELECT 'RACK-VR-42U','Enterprise Co VR 42U Rack','RACK','Enterprise Co VR','EA',3500,FALSE,'2020-01-01' UNION ALL
  SELECT 'RACK-VR-48U-OF','Enterprise Co VR 48U Rack','RACK','Enterprise Co VR','EA',4200,FALSE,'2020-01-01' UNION ALL
  SELECT 'MON-TRELLIS-ENT','Trellis Enterprise','MONITORING','Trellis','EA',35000,FALSE,'2020-01-01' UNION ALL
  SELECT 'SVC-PM-ANNUAL','Preventive Maintenance','SERVICES','Services','EA',5000,FALSE,'2020-01-01'
) v;

-- ════════════════════════════════════════════════════════════════
-- DIM_CUSTOMER: Unified across all source systems
-- Connected via Silver SHA256 hash keys
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE ENTERPRISE_ANALYTICS.BI.DIM_CUSTOMER (
  CUSTOMER_SK          NUMBER        AUTOINCREMENT PRIMARY KEY,
  MASTER_CUSTOMER_HK   VARCHAR(64),  -- Master identity after MDM resolution
  -- Source system hash keys (FK back to Silver)
  SAP_CUSTOMER_HK      VARCHAR(64),
  JDE_CUSTOMER_HK      VARCHAR(64),
  QAD_CUSTOMER_HK      VARCHAR(64),
  SFDC_ACCOUNT_HK      VARCHAR(64),
  -- Conformed attributes (best-source-wins logic in Dynamic Table)
  CUSTOMER_NAME        VARCHAR(500),
  CUSTOMER_TYPE        VARCHAR(50),
  INDUSTRY             VARCHAR(100),
  COUNTRY_CODE         VARCHAR(3),
  COUNTRY_NAME         VARCHAR(100),
  REGION               VARCHAR(50),
  IS_KEY_ACCOUNT       BOOLEAN       DEFAULT FALSE,
  SEGMENT              VARCHAR(50),  -- ENTERPRISE | MID_MARKET | SMB
  -- SCD Type 2
  EFFECTIVE_FROM       DATE,
  EFFECTIVE_TO         DATE,
  IS_CURRENT           BOOLEAN       DEFAULT TRUE,
  DW_VERSION           NUMBER        DEFAULT 1,
  LOAD_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
;

-- ════════════════════════════════════════════════════════════════
-- FACT_SALES_ORDERS: Dynamic Table from Silver (15-min lag)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE ENTERPRISE_ANALYTICS.BI.FACT_SALES_ORDERS
  TARGET_LAG = '15 minutes'
  WAREHOUSE  = TRANSFORM_WH
AS
SELECT
  so.ORDER_HK,
  so.CUSTOMER_HK,
  1                                                AS PRODUCT_SK,
  TO_NUMBER(TO_CHAR(so.ORDER_DATE, 'YYYYMMDD'))    AS DATE_KEY,
  1                                                AS REGION_SK,
  so.ORDER_DATE,
  so.ORDER_TYPE,
  so.ORDER_STATUS,
  so.CURRENCY_CODE,
  so.NET_AMOUNT_ORIG,
  so.NET_AMOUNT_USD,
  so.TAX_AMOUNT_USD,
  COALESCE(so.NET_AMOUNT_USD,0) + COALESCE(so.TAX_AMOUNT_USD,0) AS TOTAL_AMOUNT_USD,
  COALESCE(so.DISCOUNT_PCT, 0) AS DISCOUNT_PCT,
  so.SOURCE_SYSTEM,
  so.SOURCE_REGION,
  so.EFFECTIVE_FROM AS ORDER_LOADED_AT
FROM ENTERPRISE_CURATED.SALES.SALES_ORDER so
WHERE so.IS_CURRENT = TRUE
  AND so.IS_QUARANTINED = FALSE; ;

-- ════════════════════════════════════════════════════════════════
-- MATERIALIZED VIEWS: Pre-aggregated BI summaries
-- Snowflake auto-refreshes these when base tables change
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ENTERPRISE_ANALYTICS.BI.MV_REGIONAL_SALES_DAILY AS
SELECT
  SOURCE_SYSTEM,
  SOURCE_REGION,
  DATE_KEY,
  REGION_SK,
  COUNT(DISTINCT ORDER_HK)    AS ORDER_COUNT,
  SUM(NET_AMOUNT_USD)         AS TOTAL_REVENUE_USD,
  SUM(TAX_AMOUNT_USD)         AS TOTAL_TAX_USD,
  SUM(TOTAL_AMOUNT_USD)       AS TOTAL_BILLED_USD,
  AVG(NET_AMOUNT_USD)         AS AVG_ORDER_VALUE_USD,
  AVG(DISCOUNT_PCT)           AS AVG_DISCOUNT_PCT,
  COUNT(DISTINCT CUSTOMER_HK) AS UNIQUE_CUSTOMERS
FROM ENTERPRISE_ANALYTICS.BI.FACT_SALES_ORDERS
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW ENTERPRISE_ANALYTICS.BI.MV_PRODUCT_REVENUE_MONTHLY AS
SELECT
  PRODUCT_SK,
  SOURCE_SYSTEM,
  DATE_KEY,
  SUM(NET_AMOUNT_USD)         AS TOTAL_REVENUE_USD,
  COUNT(DISTINCT ORDER_HK)    AS ORDER_COUNT,
  COUNT(DISTINCT CUSTOMER_HK) AS UNIQUE_CUSTOMERS,
  AVG(DISCOUNT_PCT)           AS AVG_DISCOUNT_PCT
FROM ENTERPRISE_ANALYTICS.BI.FACT_SALES_ORDERS
GROUP BY 1,2,3;

-- ── SEARCH OPTIMIZATION (point-lookup acceleration) ───────────
ALTER TABLE ENTERPRISE_ANALYTICS.BI.DIM_CUSTOMER
  ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_NAME, COUNTRY_CODE, MASTER_CUSTOMER_HK);

ALTER TABLE ENTERPRISE_ANALYTICS.BI.DIM_PRODUCT
  ADD SEARCH OPTIMIZATION ON EQUALITY(PRODUCT_CODE, PRODUCT_NAME, PRODUCT_FAMILY);

-- ── GRANTS ────────────────────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.BI           TO ROLE GLOBAL_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.BI           TO ROLE FINANCE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_ANALYTICS.BI           TO ROLE EXEC_VP;
GRANT SELECT ON MATERIALIZED VIEW ENTERPRISE_ANALYTICS.BI.MV_REGIONAL_SALES_DAILY    TO ROLE GLOBAL_ANALYST;
GRANT SELECT ON MATERIALIZED VIEW ENTERPRISE_ANALYTICS.BI.MV_PRODUCT_REVENUE_MONTHLY TO ROLE GLOBAL_ANALYST;

