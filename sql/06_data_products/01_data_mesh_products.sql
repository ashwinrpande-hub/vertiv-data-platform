-- ================================================================
-- FILE: 08_data_products_mesh.sql
-- PURPOSE: Enhance data products with full Data Mesh compliance:
--          - Domain ownership + data product catalog
--          - SLO / SLI definitions per product
--          - Input/output contracts (versioned)
--          - Per-product DQ trust scores
--          - Marketplace listing metadata
-- PRINCIPLES: Dehghani Data Mesh (martinfowler.com)
-- ROLE: VERTIV_PLATFORM_ADMIN
-- ================================================================

USE ROLE      VERTIV_PLATFORM_ADMIN;
USE WAREHOUSE INGEST_WH;

-- ════════════════════════════════════════════════════════════════
-- 1. DATA PRODUCT CATALOG
--    Every data product is registered here with full metadata.
--    This is the "data product as a first-class citizen" concept
--    from Dehghani's mesh principles.
-- ════════════════════════════════════════════════════════════════

USE DATABASE VERTIV_CONFIG;

CREATE OR REPLACE TABLE VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG (
  PRODUCT_ID            VARCHAR(50)    PRIMARY KEY,
  PRODUCT_NAME          VARCHAR(200)   NOT NULL,
  PRODUCT_VERSION       VARCHAR(20)    DEFAULT 'v1',

  -- Domain ownership (Principle 1: Domain-oriented ownership)
  DOMAIN_NAME           VARCHAR(100),  -- Sales | Finance | Operations | RevOps
  DOMAIN_OWNER_TEAM     VARCHAR(200),  -- Team accountable for product quality
  DOMAIN_OWNER_EMAIL    VARCHAR(200),
  DATA_STEWARD          VARCHAR(200),

  -- Product description (8 characteristics: Understandable/Self-describable)
  DESCRIPTION           VARCHAR(2000),
  USE_CASES             VARCHAR(2000),  -- What consumers use this for
  KEY_ENTITIES          VARCHAR(500),   -- Main business objects in product

  -- Addressability (8 characteristics: Addressable)
  SNOWFLAKE_DATABASE    VARCHAR(100)   DEFAULT 'VERTIV_PRODUCTS',
  SNOWFLAKE_SCHEMA      VARCHAR(100),
  PRIMARY_VIEW          VARCHAR(200),  -- Main entry point for consumers
  SHARE_NAME            VARCHAR(200),  -- Snowflake Share for Marketplace

  -- Input contract (what this product depends on)
  INPUT_SOURCES         VARCHAR(1000), -- Source systems and Silver tables
  INPUT_REFRESH_TRIGGER VARCHAR(200),  -- What triggers a refresh

  -- Output contract (stable schema promise to consumers)
  OUTPUT_FORMAT         VARCHAR(50)    DEFAULT 'SNOWFLAKE_TABLE',
  SCHEMA_VERSION        VARCHAR(20)    DEFAULT 'v1',
  BREAKING_CHANGE_POLICY VARCHAR(500), -- How breaking changes are handled

  -- SLOs (Service Level Objectives) - makes product Trustworthy
  SLO_FRESHNESS_MINUTES NUMBER         DEFAULT 60,    -- max age of data
  SLO_AVAILABILITY_PCT  NUMBER(5,2)    DEFAULT 99.5,  -- uptime target
  SLO_QUERY_P95_SECONDS NUMBER(5,2)    DEFAULT 5.0,   -- p95 query latency
  SLO_DQ_COMPLETENESS   NUMBER(5,2)    DEFAULT 99.0,  -- min completeness %
  SLO_DQ_VALIDITY       NUMBER(5,2)    DEFAULT 99.0,  -- min validity %
  SLO_DQ_UNIQUENESS     NUMBER(5,2)    DEFAULT 100.0, -- min uniqueness %

  -- Access control (8 characteristics: Secure)
  ACCESS_CLASSIFICATION VARCHAR(50)    DEFAULT 'INTERNAL',
  -- INTERNAL | PARTNER | PUBLIC
  REQUIRED_ROLE         VARCHAR(200),
  PII_PRESENT           BOOLEAN        DEFAULT FALSE,
  GDPR_APPLICABLE       BOOLEAN        DEFAULT FALSE,

  -- Tags for discovery (8 characteristics: Discoverable)
  TAGS                  VARCHAR(2000),
  DOMAIN_TAGS           VARCHAR(500),
  CONSUMER_PERSONAS     VARCHAR(500),  -- DATA_ANALYST | ML_ENGINEER | EXECUTIVE | FINANCE

  -- Lifecycle
  STATUS                VARCHAR(20)    DEFAULT 'ACTIVE',
  -- ACTIVE | DEPRECATED | BETA
  DEPRECATED_DATE       DATE,
  SUCCESSOR_PRODUCT_ID  VARCHAR(50),   -- for migrations
  CREATED_AT            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ── Seed: 4 Data Products ─────────────────────────────────────
INSERT INTO VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG VALUES

-- PRODUCT 1: Sales Performance
('DP-001',
 'Sales Performance',
 'v1',
 'Sales',
 'Sales Operations Team',
 'sales-ops@vertiv.com',
 'Chandan Patil (Principal Data Architect)',
 'Unified sales order performance across SAP (Europe), JDE (Americas), and QAD (EMEA). '||
 'Pre-aggregated by region, product family, and time period. Single source of truth for '||
 'revenue reporting across all ERP systems.',
 'Revenue reporting dashboards | Regional performance comparisons | Sales quota tracking | '||
 'Executive MBR/QBR decks | Finance FP&A models',
 'Sales Orders, Customers, Products, Regions',
 'VERTIV_PRODUCTS', 'SALES_PERFORMANCE',
 'VERTIV_PRODUCTS.SALES_PERFORMANCE.SALES_ORDERS_V1',
 'VERTIV_SALES_PERFORMANCE_SHARE',
 'Silver: VERTIV_CURATED.SALES.SALES_ORDER (SAP+JDE+QAD) | Gold: FACT_SALES_ORDERS Dynamic Table',
 'Dynamic Table refresh (15 min lag) + Silver refresh (5 min lag)',
 'SNOWFLAKE_TABLE', 'v1',
 'Breaking changes create v2 view. v1 deprecated with 90-day notice. Schema additions are non-breaking.',
 15, 99.5, 5.0, 99.0, 99.0, 100.0,
 'INTERNAL', 'VERTIV_GLOBAL_ANALYST', FALSE, FALSE,
 'sales,revenue,orders,ERP,SAP,JDE,QAD,regional',
 'Sales,Finance',
 'DATA_ANALYST,FINANCE_ANALYST,EXECUTIVE',
 'ACTIVE', NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- PRODUCT 2: Customer 360
('DP-002',
 'Customer 360',
 'v1',
 'Sales',
 'CRM & Customer Intelligence Team',
 'crm-team@vertiv.com',
 'Chandan Patil (Principal Data Architect)',
 'Unified customer master combining SAP, JDE, QAD, and Salesforce. Includes ML-scored '||
 'features (churn risk, upsell propensity), revenue history, and behavioral signals. '||
 'Enables personalized sales motions and proactive retention strategies.',
 'Customer segmentation | Churn prediction | Upsell identification | Account planning | '||
 'Sales rep prioritization | Customer health scoring',
 'Customers, ML Features, Churn Risk, Upsell Scores',
 'VERTIV_PRODUCTS', 'CUSTOMER_360',
 'VERTIV_PRODUCTS.CUSTOMER_360.CUSTOMERS_V1',
 'VERTIV_CUSTOMER_360_SHARE',
 'Silver: VERTIV_CURATED.SALES.CUSTOMER | Gold: DIM_CUSTOMER, CUSTOMER_REVENUE_FEATURES, MODEL_PREDICTIONS',
 'Daily feature pipeline (feature_engineering.py) + Dynamic Table refresh (5 min)',
 'SNOWFLAKE_TABLE', 'v1',
 'Breaking changes create v2. ML model versions tracked separately in MODEL_PREDICTIONS.',
 5, 99.9, 3.0, 99.5, 99.0, 100.0,
 'INTERNAL', 'VERTIV_GLOBAL_ANALYST', FALSE, TRUE,
 'customer,360,churn,upsell,ML,CRM,Salesforce,features',
 'Sales,CRM,Data Science',
 'ML_ENGINEER,DATA_ANALYST,SALES_REP',
 'ACTIVE', NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- PRODUCT 3: Revenue Analytics
('DP-003',
 'Revenue Analytics',
 'v1',
 'Finance',
 'Finance & FP&A Team',
 'finance@vertiv.com',
 'Chandan Patil (Principal Data Architect)',
 'Monthly and quarterly revenue by product family, region, and ERP system. '||
 'Designed for Finance FP&A, executive reporting, and board-level dashboards. '||
 'Pre-aggregated for sub-second performance. Available daily by 06:00 UTC.',
 'Monthly close reporting | Budget vs actual | Product line P&L | Board decks | '||
 'Revenue forecasting inputs | Gross margin analysis',
 'Revenue, Product Family, Region, Time',
 'VERTIV_PRODUCTS', 'REVENUE_ANALYTICS',
 'VERTIV_PRODUCTS.REVENUE_ANALYTICS.MONTHLY_REVENUE_V1',
 NULL,  -- No share yet - internal Finance only
 'Gold: MV_PRODUCT_REVENUE_MONTHLY (view on FACT_SALES_ORDERS)',
 'Daily batch by 06:00 UTC',
 'SNOWFLAKE_TABLE', 'v1',
 'Schema is frozen for Finance systems. Any change requires Finance sign-off and 180-day notice.',
 60, 99.5, 10.0, 99.0, 99.5, 100.0,
 'INTERNAL', 'VERTIV_FINANCE_ANALYST', FALSE, FALSE,
 'revenue,finance,FP&A,monthly,product-family,P&L',
 'Finance',
 'FINANCE_ANALYST,EXECUTIVE',
 'ACTIVE', NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),

-- PRODUCT 4: Opportunity Signals
('DP-004',
 'Opportunity Signals',
 'v1',
 'Revenue Operations',
 'RevOps & Sales Intelligence Team',
 'revops@vertiv.com',
 'Chandan Patil (Principal Data Architect)',
 'Real-time ML prediction scores for sales opportunities: churn risk alerts, upsell '||
 'propensity, and deal health signals. Refreshed every 2 minutes from Kafka streams '||
 'and ML model predictions. Enables data-driven sales rep prioritization.',
 'Sales rep daily prioritization | Opportunity pipeline health | '||
 'At-risk account alerts | Upsell target lists | RevOps pipeline reviews',
 'Opportunities, Predictions, Churn Signals, Upsell Scores',
 'VERTIV_PRODUCTS', 'OPPORTUNITY_SIGNALS',
 'VERTIV_PRODUCTS.OPPORTUNITY_SIGNALS.PREDICTIONS_V1',
 NULL,
 'Gold: MODEL_PREDICTIONS | Silver: OPPORTUNITY | Real-time: REALTIME.ORDER_EVENTS',
 'Continuous - ML pipeline runs every 2 min',
 'SNOWFLAKE_TABLE', 'v1',
 'Model versions tracked via MODEL_VERSION field. New model = new version, not breaking change.',
 2, 99.9, 2.0, 98.0, 99.0, 100.0,
 'INTERNAL', 'VERTIV_GLOBAL_ANALYST', FALSE, FALSE,
 'ML,predictions,churn,upsell,opportunities,real-time,RevOps',
 'Sales,RevOps',
 'ML_ENGINEER,DATA_ANALYST,SALES_REP',
 'ACTIVE', NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ════════════════════════════════════════════════════════════════
-- 2. SLI TRACKING TABLE (Service Level Indicators - actual measurements)
--    SLO = target. SLI = actual measured value.
--    This is what makes products "Trustworthy" per Dehghani.
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG (
  SLI_ID                NUMBER        AUTOINCREMENT PRIMARY KEY,
  PRODUCT_ID            VARCHAR(50),
  MEASUREMENT_TIME      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  -- Freshness SLI: actual data age in minutes
  DATA_AGE_MINUTES      NUMBER,
  FRESHNESS_MET         BOOLEAN,       -- SLI <= SLO_FRESHNESS_MINUTES

  -- DQ SLIs: actual pass rates from DQ_BATCH_LOG
  DQ_COMPLETENESS_PCT   NUMBER(5,2),
  DQ_VALIDITY_PCT       NUMBER(5,2),
  DQ_UNIQUENESS_PCT     NUMBER(5,2),
  DQ_ACCURACY_PCT       NUMBER(5,2),
  DQ_CONSISTENCY_PCT    NUMBER(5,2),
  DQ_TIMELINESS_PCT     NUMBER(5,2),
  OVERALL_DQ_SCORE      NUMBER(5,2),   -- weighted average of 6 dimensions

  -- Availability SLI
  IS_AVAILABLE          BOOLEAN        DEFAULT TRUE,
  DOWNTIME_MINUTES      NUMBER         DEFAULT 0,

  -- Overall SLO compliance
  ALL_SLOS_MET          BOOLEAN,
  NOTES                 VARCHAR(500)
);

-- Seed with sample SLI measurements for demo
INSERT INTO VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG
  (PRODUCT_ID, DATA_AGE_MINUTES, FRESHNESS_MET,
   DQ_COMPLETENESS_PCT, DQ_VALIDITY_PCT, DQ_UNIQUENESS_PCT,
   DQ_ACCURACY_PCT, DQ_CONSISTENCY_PCT, DQ_TIMELINESS_PCT,
   OVERALL_DQ_SCORE, ALL_SLOS_MET, NOTES)
VALUES
  ('DP-001', 8,  TRUE, 99.9, 100.0, 100.0, 98.8, 100.0, 94.8, 98.9, TRUE,  'Normal operation'),
  ('DP-002', 3,  TRUE, 99.8, 99.5,  100.0, 99.1, 99.0,  95.0, 98.7, TRUE,  'Normal operation'),
  ('DP-003', 45, TRUE, 100.0,100.0, 100.0, 99.5, 100.0, 98.0, 99.6, TRUE,  'Daily batch complete'),
  ('DP-004', 1,  TRUE, 98.5, 99.0,  100.0, 97.2, 98.5,  99.0, 98.7, TRUE,  'Real-time pipeline healthy');

-- ════════════════════════════════════════════════════════════════
-- 3. DATA PRODUCT TRUST SCORE VIEW
--    Aggregates DQ metrics + SLI into a single trust score per product
--    This feeds the DQ dashboard "trust" panel
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE AS
SELECT
  c.PRODUCT_ID,
  c.PRODUCT_NAME,
  c.DOMAIN_NAME,
  c.DOMAIN_OWNER_TEAM,
  c.SLO_FRESHNESS_MINUTES,
  c.SLO_DQ_COMPLETENESS,
  c.SLO_DQ_VALIDITY,
  c.SLO_DQ_UNIQUENESS,
  c.STATUS,
  -- Latest SLI measurements
  s.DATA_AGE_MINUTES,
  s.FRESHNESS_MET,
  s.DQ_COMPLETENESS_PCT,
  s.DQ_VALIDITY_PCT,
  s.DQ_UNIQUENESS_PCT,
  s.DQ_ACCURACY_PCT,
  s.DQ_CONSISTENCY_PCT,
  s.DQ_TIMELINESS_PCT,
  s.OVERALL_DQ_SCORE,
  s.ALL_SLOS_MET,
  s.MEASUREMENT_TIME,
  -- Trust score: weighted DQ + freshness compliance
  ROUND(
    s.OVERALL_DQ_SCORE * 0.7 +
    CASE WHEN s.FRESHNESS_MET THEN 100 ELSE 50 END * 0.3,
    1
  ) AS TRUST_SCORE,
  -- Traffic light
  CASE
    WHEN ROUND(s.OVERALL_DQ_SCORE * 0.7 + CASE WHEN s.FRESHNESS_MET THEN 100 ELSE 50 END * 0.3, 1) >= 98
    THEN 'GREEN'
    WHEN ROUND(s.OVERALL_DQ_SCORE * 0.7 + CASE WHEN s.FRESHNESS_MET THEN 100 ELSE 50 END * 0.3, 1) >= 95
    THEN 'AMBER'
    ELSE 'RED'
  END AS TRUST_BAND
FROM VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG c
JOIN (
  -- Latest SLI per product
  SELECT * FROM VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG
  WHERE (PRODUCT_ID, MEASUREMENT_TIME) IN (
    SELECT PRODUCT_ID, MAX(MEASUREMENT_TIME)
    FROM VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG
    GROUP BY PRODUCT_ID
  )
) s ON c.PRODUCT_ID = s.PRODUCT_ID
WHERE c.STATUS = 'ACTIVE';

-- ════════════════════════════════════════════════════════════════
-- 4. INPUT/OUTPUT CONTRACT DEFINITIONS
--    Explicit schema contracts for each product.
--    "Input port" = what the product consumes.
--    "Output port" = what consumers receive (stable, versioned).
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT (
  CONTRACT_ID           NUMBER        AUTOINCREMENT PRIMARY KEY,
  PRODUCT_ID            VARCHAR(50),
  CONTRACT_VERSION      VARCHAR(20)   DEFAULT 'v1',
  COLUMN_NAME           VARCHAR(200),
  DATA_TYPE             VARCHAR(100),
  NULLABLE              BOOLEAN       DEFAULT TRUE,
  DESCRIPTION           VARCHAR(500),
  IS_PII                BOOLEAN       DEFAULT FALSE,
  IS_MASKED             BOOLEAN       DEFAULT FALSE,
  EXAMPLE_VALUE         VARCHAR(200),
  IS_BREAKING_IF_REMOVED BOOLEAN      DEFAULT TRUE
);

-- DP-001: Sales Performance output contract
INSERT INTO VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT
  (PRODUCT_ID, CONTRACT_VERSION, COLUMN_NAME, DATA_TYPE, NULLABLE, DESCRIPTION, EXAMPLE_VALUE)
VALUES
('DP-001','v1','ORDER_HK',       'VARCHAR(64)', FALSE, 'SHA256 hash key for order (unique across all ERP systems)', 'a3f4b2c1...'),
('DP-001','v1','CUSTOMER_HK',    'VARCHAR(64)', FALSE, 'SHA256 hash key for customer', 'd9e8f7a6...'),
('DP-001','v1','ORDER_DATE',     'DATE',        FALSE, 'Date order was placed in source ERP', '2024-03-15'),
('DP-001','v1','ORDER_TYPE',     'VARCHAR(30)', TRUE,  'Order classification (STANDARD, RUSH, CONTRACT)', 'STANDARD'),
('DP-001','v1','ORDER_STATUS',   'VARCHAR(50)', TRUE,  'Current fulfillment status', 'DELIVERED'),
('DP-001','v1','CURRENCY_CODE',  'VARCHAR(3)',  FALSE, 'ISO 4217 currency code', 'EUR'),
('DP-001','v1','NET_AMOUNT_USD', 'NUMBER(18,4)',FALSE, 'Net order value normalized to USD', '45231.5000'),
('DP-001','v1','SOURCE_SYSTEM',  'VARCHAR(20)', FALSE, 'Originating ERP: SAP | JDE | QAD', 'SAP'),
('DP-001','v1','SOURCE_REGION',  'VARCHAR(20)', FALSE, 'Geographic region: EUROPE | AMERICAS | EMEA', 'EUROPE'),
('DP-001','v1','REGION_NAME',    'VARCHAR(100)',TRUE,  'Human-readable region name', 'Europe'),
('DP-001','v1','COUNTRY_CODE',   'VARCHAR(3)',  TRUE,  'ISO 3166-1 alpha-3 country code', 'DEU'),
('DP-001','v1','YEAR_NUMBER',    'NUMBER',      FALSE, 'Calendar year', '2024'),
('DP-001','v1','QUARTER_NAME',   'VARCHAR(6)',  FALSE, 'Fiscal quarter', 'Q1');

-- DP-002: Customer 360 output contract
INSERT INTO VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT
  (PRODUCT_ID, CONTRACT_VERSION, COLUMN_NAME, DATA_TYPE, NULLABLE, DESCRIPTION, IS_PII, IS_MASKED, EXAMPLE_VALUE)
VALUES
('DP-002','v1','CUSTOMER_HK',        'VARCHAR(64)', FALSE,'Master customer hash key',           FALSE,FALSE,'a3f4...'),
('DP-002','v1','CUSTOMER_NAME',      'VARCHAR(500)',FALSE,'Legal entity name',                  TRUE, TRUE, 'Acme Corp'),
('DP-002','v1','INDUSTRY',           'VARCHAR(100)',TRUE, 'Industry vertical',                  FALSE,FALSE,'DATA_CENTER'),
('DP-002','v1','REGION',             'VARCHAR(50)', TRUE, 'Geographic region',                  FALSE,FALSE,'EUROPE'),
('DP-002','v1','SEGMENT',            'VARCHAR(50)', TRUE, 'ENTERPRISE | MID_MARKET | SMB',      FALSE,FALSE,'ENTERPRISE'),
('DP-002','v1','REV_L30D_USD',       'NUMBER(18,2)',TRUE, 'Revenue last 30 days (USD)',          FALSE,FALSE,'125000.00'),
('DP-002','v1','REV_L90D_USD',       'NUMBER(18,2)',TRUE, 'Revenue last 90 days (USD)',          FALSE,FALSE,'380000.00'),
('DP-002','v1','REV_L365D_USD',      'NUMBER(18,2)',TRUE, 'Revenue last 365 days (USD)',         FALSE,FALSE,'1250000.00'),
('DP-002','v1','CHURN_RISK_SCORE',   'NUMBER(5,4)', TRUE, '0-1 churn probability (90-day)',      FALSE,FALSE,'0.1823'),
('DP-002','v1','UPSELL_PROPENSITY',  'NUMBER(5,4)', TRUE, '0-1 upsell likelihood',              FALSE,FALSE,'0.7250'),
('DP-002','v1','DAYS_SINCE_LAST_ORDER','NUMBER',   TRUE, 'Days since most recent order',        FALSE,FALSE,'12'),
('DP-002','v1','AS_OF_DATE',         'DATE',        FALSE,'Point-in-time snapshot date',        FALSE,FALSE,'2024-03-25');

-- ════════════════════════════════════════════════════════════════
-- 5. MARKETPLACE DISCOVERY VIEW
--    What a consumer sees when browsing the data catalog.
--    Addresses: Discoverable, Addressable, Self-describable characteristics.
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE AS
SELECT
  c.PRODUCT_ID,
  c.PRODUCT_NAME,
  c.PRODUCT_VERSION,
  c.DOMAIN_NAME,
  c.DOMAIN_OWNER_TEAM,
  c.DESCRIPTION,
  c.USE_CASES,
  c.KEY_ENTITIES,
  -- Addressable: unique, permanent address
  c.SNOWFLAKE_DATABASE || '.' || c.SNOWFLAKE_SCHEMA || '.' || c.PRIMARY_VIEW
    AS FULL_ADDRESS,
  c.SHARE_NAME,
  -- SLO summary for consumers to evaluate trustworthiness
  c.SLO_FRESHNESS_MINUTES || ' min freshness'  AS FRESHNESS_SLA,
  c.SLO_AVAILABILITY_PCT  || '% availability'  AS AVAILABILITY_SLA,
  c.SLO_DQ_COMPLETENESS   || '% completeness'  AS DQ_COMPLETENESS_SLA,
  -- Current trust score
  t.TRUST_SCORE,
  t.TRUST_BAND,
  t.DATA_AGE_MINUTES       AS CURRENT_DATA_AGE_MINUTES,
  t.OVERALL_DQ_SCORE       AS CURRENT_DQ_SCORE,
  -- Access info
  c.ACCESS_CLASSIFICATION,
  c.REQUIRED_ROLE,
  c.PII_PRESENT,
  c.GDPR_APPLICABLE,
  -- Discovery tags
  c.TAGS,
  c.CONSUMER_PERSONAS,
  c.STATUS,
  -- How to consume (Natively Accessible)
  'SQL: SELECT * FROM ' || c.SNOWFLAKE_DATABASE || '.' ||
    c.SNOWFLAKE_SCHEMA || '.' || c.PRIMARY_VIEW AS SQL_ACCESS_EXAMPLE,
  'Request access via: data-platform@vertiv.com' AS ACCESS_REQUEST
FROM VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG c
LEFT JOIN VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE t
  ON c.PRODUCT_ID = t.PRODUCT_ID
WHERE c.STATUS = 'ACTIVE'
ORDER BY c.DOMAIN_NAME, c.PRODUCT_NAME;

-- ════════════════════════════════════════════════════════════════
-- 6. CONSUMER QUERY — How a consumer discovers and accesses products
--    This is the "Show how they will discover and search for Data Products"
-- ════════════════════════════════════════════════════════════════

-- STEP 1: Consumer browses the marketplace catalog
-- "I need data about customers and churn risk"
SELECT
  PRODUCT_ID,
  PRODUCT_NAME,
  DOMAIN_NAME,
  DESCRIPTION,
  TRUST_SCORE,
  TRUST_BAND,
  FRESHNESS_SLA,
  SQL_ACCESS_EXAMPLE,
  ACCESS_REQUEST
FROM VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE
WHERE CONSUMER_PERSONAS LIKE '%ML_ENGINEER%'
ORDER BY TRUST_SCORE DESC;

-- STEP 2: Consumer checks the output contract before building on it
-- "What columns does Customer 360 expose? Are there PII columns?"
SELECT
  COLUMN_NAME,
  DATA_TYPE,
  NULLABLE,
  IS_PII,
  IS_MASKED,
  DESCRIPTION,
  EXAMPLE_VALUE
FROM VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT
WHERE PRODUCT_ID = 'DP-002'
  AND CONTRACT_VERSION = 'v1'
ORDER BY CONTRACT_ID;

-- STEP 3: Consumer checks trust score before depending on data
SELECT
  PRODUCT_NAME,
  TRUST_SCORE,
  TRUST_BAND,
  DQ_COMPLETENESS_PCT,
  DQ_VALIDITY_PCT,
  DQ_UNIQUENESS_PCT,
  DQ_ACCURACY_PCT,
  DQ_CONSISTENCY_PCT,
  DQ_TIMELINESS_PCT,
  DATA_AGE_MINUTES,
  FRESHNESS_MET
FROM VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE
ORDER BY TRUST_SCORE DESC;

-- STEP 4: Consumer accesses the product data via Snowflake Share
-- (This runs in the CONSUMER'S Snowflake account after accepting the share)
-- CREATE DATABASE VERTIV_SALES_FROM_SHARE FROM SHARE <vertiv_account>.VERTIV_SALES_PERFORMANCE_SHARE;
-- SELECT * FROM VERTIV_SALES_FROM_SHARE.SALES_PERFORMANCE.SALES_ORDERS_V1 LIMIT 100;

-- STEP 5: Federated governance — add a consumer account to share
-- ALTER SHARE VERTIV_SALES_PERFORMANCE_SHARE ADD ACCOUNTS = <consumer_snowflake_account>;

-- ════════════════════════════════════════════════════════════════
-- 7. GRANTS — Analysts can query marketplace and trust scores
-- ════════════════════════════════════════════════════════════════
GRANT SELECT ON VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG    TO ROLE VERTIV_GLOBAL_ANALYST;
GRANT SELECT ON VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT TO ROLE VERTIV_GLOBAL_ANALYST;
GRANT SELECT ON VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG         TO ROLE VERTIV_GLOBAL_ANALYST;
GRANT SELECT ON VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE      TO ROLE VERTIV_GLOBAL_ANALYST;
GRANT SELECT ON VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE   TO ROLE VERTIV_GLOBAL_ANALYST;
GRANT SELECT ON VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE      TO ROLE VERTIV_DATA_PRODUCT_OWNER;
GRANT SELECT ON VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE   TO ROLE VERTIV_DATA_PRODUCT_OWNER;

-- ── VERIFY ────────────────────────────────────────────────────
SELECT PRODUCT_NAME, TRUST_SCORE, TRUST_BAND, FRESHNESS_SLA
FROM VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE;
