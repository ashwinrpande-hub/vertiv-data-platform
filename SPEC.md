# Vertiv Data Marketplace — Claude Code Spec

## Purpose
This spec gives Claude Code everything it needs to continue, debug, or extend the
Vertiv Data Marketplace project with zero assumptions. Read this entire file before
taking any action.

---

## 1. Candidate & Environment

| Field | Value |
|---|---|
| Candidate | Ashwin Pande |
| GitHub user | ashwinrpande-hub |
| GitHub repo | https://github.com/ashwinrpande-hub/vertiv-data-platform |
| Snowflake account | drdwkxe-gmc96114 |
| Snowflake user | CHANDANPATIL99 |
| Snowflake role (default) | VERTIV_GLOBAL_ANALYST |
| Python version | 3.11.9 (venv at .venv\Scripts\) |
| Node version | v24.14.1 |
| npm version | 11.11.0 |
| OS | Windows 11, Asus VivoBook |
| Shell | PowerShell inside VS Code |
| Project root | C:\Study\vertiv-data-platform |
| Python venv | Always active — prompt shows (.venv) |
| Encoding | Set PYTHONIOENCODING=utf-8 before running any Python script |
| Live DQ dashboard | https://vertiv-data-platform-tech-challange.streamlit.app |

---

## 2. Project File Structure

```
C:\Study\vertiv-data-platform\
├── amplify.yml                          ← AWS Amplify build spec (project root)
├── load_data.py                         ← Bronze data loader (write_pandas)
├── requirements.txt                     ← Unpinned Python deps
├── .env                                 ← Snowflake credentials (gitignored)
├── .env.template                        ← Template (committed)
├── .github/
│   └── workflows/
│       ├── deploy.yml                   ← Original CI/CD
│       └── deploy-marketplace.yml       ← Marketplace CI/CD (Lambda + Amplify)
├── sql/
│   ├── 00_setup/   databases, warehouses, roles
│   ├── 01_config/  STT map, DQ rules, currency/country ref
│   ├── 02_bronze/  10 raw tables, stage, pipes, streams
│   ├── 03_silver/  Hash UDFs, Dynamic Tables, audit tables
│   ├── 04_gold/    BI star schema, ML features, AI vectors
│   ├── 05_security/ Masking, row access, secure views, shares
│   ├── 06_data_products/ Data mesh catalog, SLOs, trust scores
│   └── 07_automation/   Snowflake Tasks
├── python/
│   ├── data_generator/data_generator.py
│   ├── snowpark/dq_framework.py
│   ├── snowpark/feature_engineering.py
│   └── streamlit/streamlit_dq_dashboard.py
├── scripts/
│   └── deploy.py                        ← 9-step SQL deployer
└── marketplace-ui/                      ← DATA MARKETPLACE (new)
    ├── .env.template                    ← Combined frontend + backend env vars
    ├── sql_access_requests.sql          ← Run in Snowflake to add ACCESS_REQUESTS table
    ├── frontend/
    │   ├── index.html
    │   ├── package.json
    │   ├── vite.config.js
    │   └── src/
    │       ├── main.jsx                 ← React entry point
    │       ├── App.jsx                  ← ALL components in one file (no sub-components)
    │       └── styles.css               ← All styles (dark theme, Vertiv branding)
    └── backend/
        ├── main.py                      ← FastAPI + Mangum (Lambda handler)
        ├── requirements.txt             ← fastapi, mangum, snowflake-connector-python, pydantic, uvicorn
        └── template.yaml               ← AWS SAM template (Lambda + API Gateway)
```

---

## 3. Snowflake — Deployed Objects (All Live)

### Databases
- VERTIV_RAW — Bronze raw ingestion
- VERTIV_CURATED — Silver transformation layer
- VERTIV_ANALYTICS — Gold BI + ML + AI layers
- VERTIV_PRODUCTS — Data products (Secure Views)
- VERTIV_CONFIG — STT map, DQ rules, reference data
- VERTIV_AUDIT — DQ logs, rejected records, quarantine

### Warehouses
- INGEST_WH (XSmall)
- TRANSFORM_WH (Small)
- ANALYTICS_WH (Medium, 1–3 clusters auto-scale)
- ML_WH (Large)

### Roles (9 total)
VERTIV_PLATFORM_ADMIN, VERTIV_GLOBAL_ANALYST, VERTIV_SAP_EUROPE_READER,
VERTIV_JDE_AMERICAS_READER, VERTIV_QAD_EMEA_READER, VERTIV_ML_ENGINEER,
VERTIV_DATA_PRODUCT_OWNER, VERTIV_FINANCE_ANALYST, VERTIV_EXEC_VP

### Bronze Tables (all in VERTIV_RAW)
| Schema | Table | Rows |
|---|---|---|
| SAP_EUROPE | SALES_ORDERS | 5,000 |
| SAP_EUROPE | CUSTOMERS | 500 |
| JDE_AMERICAS | SALES_ORDERS | 5,000 |
| JDE_AMERICAS | CUSTOMERS | 500 |
| QAD_EMEA | SALES_ORDERS | 5,000 |
| QAD_EMEA | CUSTOMERS | 500 |
| SALESFORCE_GLOBAL | OPPORTUNITIES | 2,500 |
| REALTIME | ORDER_EVENTS | stream |

### Silver (VERTIV_CURATED)
- UDFs: GENERATE_HASH_KEY, GENERATE_HASH_DIFF
- Dynamic Tables: DT_SILVER_ORDER_SAP (5 min lag), DT_SILVER_CUSTOMER_SAP
- Tables: SALES_ORDER, CUSTOMER
- DQ audit: DQ_BATCH_LOG, REJECTED_RECORDS, QUARANTINE
- NOTE: Only SAP has Silver Dynamic Tables. JDE/QAD have no Silver DTs yet.

### Gold BI (VERTIV_ANALYTICS.BI)
- DIM_DATE (5,479 rows), DIM_REGION, DIM_PRODUCT (11 SKUs), DIM_CUSTOMER
- FACT_SALES_ORDERS — Dynamic Table, 15 min lag
- MV_REGIONAL_SALES_DAILY — regular VIEW (not materialized — cannot build MV on DT)
- MV_PRODUCT_REVENUE_MONTHLY — regular VIEW

### Gold ML (VERTIV_ANALYTICS.ML)
- CUSTOMER_REVENUE_FEATURES
- MODEL_PREDICTIONS

### Gold AI (VERTIV_ANALYTICS.AI)
- CUSTOMER_360_VECTORS (ARRAY type, not VECTOR — Snowflake limitation)
- CORTEX_SEARCH_INDEX

### Security
- Masking policies: MASK_PII_STRING, MASK_EMAIL, MASK_FINANCIAL_NUMBER
- Row access: REGIONAL_DATA_POLICY
- 2 Snowflake Shares: VERTIV_SALES_PERFORMANCE_SHARE, VERTIV_CUSTOMER_360_SHARE

### Data Products (VERTIV_PRODUCTS) — all Secure Views v1
| ID | Name | Trust Score | Access Tier | Share |
|---|---|---|---|---|
| DP-001 | Sales Performance | 98.9% | Restricted | Yes |
| DP-002 | Customer 360 | 98.7% | Restricted | Yes |
| DP-003 | Revenue Analytics | 99.6% | Public | No |
| DP-004 | Opportunity Signals | 98.7% | Restricted | No |

### Data Mesh (VERTIV_PRODUCTS)
- DATA_PRODUCT_CATALOG
- PRODUCT_SLI_LOG
- PRODUCT_OUTPUT_CONTRACT
- V_PRODUCT_TRUST_SCORE
- V_DATA_MARKETPLACE ← the backend API queries this view

### Automation Tasks
- TASK_DQ_FRAMEWORK_ALL — every 6 hours
- TASK_ML_FEATURE_PIPELINE — daily 02:00 UTC
- TASK_SLI_MEASUREMENT — every 30 minutes

---

## 4. Marketplace UI — Frontend

### Tech Stack
- React 18 + Vite 5
- No external UI library (all custom CSS in styles.css)
- No React Router (single page, modal-based)
- Fonts: Barlow Condensed (display), IBM Plex Sans (body), IBM Plex Mono (code)
  — loaded from Google Fonts CDN

### Environment Variables (frontend)
| Variable | Default | Purpose |
|---|---|---|
| VITE_USE_MOCK | true | If "false", hits real FastAPI backend |
| VITE_API_URL | http://localhost:8000 | FastAPI base URL |

- VITE_USE_MOCK=true means all data comes from MOCK_PRODUCTS array in App.jsx
- VITE_USE_MOCK=false means it calls the FastAPI backend at VITE_API_URL
- The mock data exactly mirrors the real Snowflake data products

### Key Design Decisions
- ALL components are in App.jsx — no separate component files
- ALL styles are in styles.css — no CSS modules, no Tailwind
- Dark theme: background #080b12, surface #0e1320, card #131828
- Vertiv orange accent: #fe5b1b
- Trust score rings are custom SVG (not a library)
- Search debounce: 350ms
- Mock search filters MOCK_PRODUCTS client-side (no API call)

### Local Dev Commands
```powershell
cd C:\Study\vertiv-data-platform\marketplace-ui\frontend
npm install          # first time only
npm run dev          # starts at http://localhost:3000
npm run build        # outputs to dist/
npm run preview      # preview production build
```

### Current State
- Runs locally on http://localhost:3000 — VERIFIED WORKING
- Shows all 4 data products with correct trust scores, tags, filters
- Search works (client-side mock mode)
- Modal works: Overview tab, Schema tab, Access Request tab
- NOT yet deployed to AWS Amplify

---

## 5. Marketplace UI — Backend

### Tech Stack
- FastAPI (Python 3.11)
- Mangum — wraps FastAPI as AWS Lambda handler
- snowflake-connector-python — direct SQL connection (not Snowpark)
- uvicorn — local dev server

### Environment Variables (backend)
| Variable | Default | Purpose |
|---|---|---|
| SNOWFLAKE_ACCOUNT | (required) | drdwkxe-gmc96114 |
| SNOWFLAKE_USER | (required) | CHANDANPATIL99 |
| SNOWFLAKE_PASSWORD | (required) | from .env file |
| SNOWFLAKE_WAREHOUSE | ANALYTICS_WH | compute |
| SNOWFLAKE_DATABASE | VERTIV_PRODUCTS | default DB |
| SNOWFLAKE_SCHEMA | SALES_PERFORMANCE | default schema |
| SNOWFLAKE_ROLE | VERTIV_GLOBAL_ANALYST | query role |
| ALLOWED_ORIGINS | http://localhost:3000 | CORS — comma-separated |

### API Endpoints
| Method | Path | Description |
|---|---|---|
| GET | /health | Liveness check |
| GET | /products | All data products from V_DATA_MARKETPLACE |
| GET | /products/{product_id} | Single product detail |
| GET | /search?q= | Cortex AI search, falls back to keyword |
| POST | /access-request | Logs request to VERTIV_AUDIT.PUBLIC.ACCESS_REQUESTS |
| GET | /metrics | Platform stats (product count, avg trust, total rows) |

### Snowflake Queries
- /products → SELECT from VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
- /search → SNOWFLAKE.CORTEX.SEARCH_PREVIEW on VERTIV_ANALYTICS.AI.CORTEX_SEARCH_INDEX
  (falls back to LIKE keyword search if Cortex fails)
- /access-request → INSERT into VERTIV_AUDIT.PUBLIC.ACCESS_REQUESTS
  (this table must be created by running sql_access_requests.sql first)

### Lambda Handler
- Handler is `main.handler` (Mangum wraps the FastAPI `app`)
- Defined at bottom of main.py: `handler = Mangum(app, lifespan="off")`

### Local Dev
```powershell
cd C:\Study\vertiv-data-platform\marketplace-ui\backend
pip install -r requirements.txt
# Load credentials first (see section 8)
uvicorn main:app --reload --port 8000
# Test: http://localhost:8000/health
# Docs: http://localhost:8000/docs
```

---

## 6. AWS Infrastructure

### Deployment Target
- Frontend: AWS Amplify (free tier) — auto-deploys from GitHub
- Backend: AWS Lambda + API Gateway via AWS SAM (free tier)
- No VPC, no RDS, no containers — pure serverless
- Region: us-east-1

### AWS Amplify Setup
- Build spec: amplify.yml at project root
- Build command: cd marketplace-ui/frontend && npm ci && npm run build
- Artifact base directory: marketplace-ui/frontend/dist
- Environment variables to set in Amplify Console:
  - VITE_USE_MOCK = false
  - VITE_API_URL = <API Gateway URL from SAM deploy>
- Expected URL format: https://main.d1xxxxxxxxxx.amplifyapp.com

### AWS SAM Setup (Lambda + API Gateway)
- Template: marketplace-ui/backend/template.yaml
- Stack name: vertiv-marketplace-api
- Snowflake credentials stored in AWS SSM Parameter Store:
  - /vertiv/snowflake/account (String)
  - /vertiv/snowflake/user (String)
  - /vertiv/snowflake/password (SecureString)
- SAM deploy commands:
```bash
cd marketplace-ui/backend
sam build
sam deploy --guided   # first time (saves samconfig.toml)
sam deploy            # subsequent deploys
```
- Output key: ApiUrl → paste this into Amplify VITE_API_URL

### GitHub Secrets Required (for CI/CD workflow)
| Secret | Value |
|---|---|
| AWS_ACCESS_KEY_ID | AWS IAM user key |
| AWS_SECRET_ACCESS_KEY | AWS IAM user secret |
| SNOWFLAKE_ACCOUNT | drdwkxe-gmc96114 |
| SNOWFLAKE_USER | CHANDANPATIL99 |
| SNOWFLAKE_PASSWORD | (from .env) |
| AMPLIFY_APP_ID | (from Amplify Console after app created) |
| AMPLIFY_APP_URL | https://main.d1xxxxxxxxxx.amplifyapp.com |
| API_GATEWAY_URL | (from SAM deploy output) |

---

## 7. Known Snowflake Gotchas (DO NOT repeat these mistakes)

| Issue | Fix |
|---|---|
| CHECK constraints | Not supported in Snowflake DDL — remove them |
| MATERIALIZED VIEWs on Dynamic Tables | Not allowed — use regular VIEWs instead |
| VECTOR type | Not available — use ARRAY as workaround |
| SHA2() inside VALUES | Use SELECT...FROM VALUES pattern instead |
| ARRAY_CONSTRUCT in VALUES | Use comma-separated VARCHAR strings |
| ARRAY_CONTAINS on VARCHAR | Use LIKE '%value%' |
| Semicolons inside SQL comments | Breaks statement splitting — remove them |
| GRANT SELECT ON ALL VIEWS IN SCHEMA (for shares) | Must grant individually per view |
| Snowflake Tasks — child task | Root task must be suspended before adding children |
| COMMENT ON TASK | Not supported — remove COMMENT statements |
| BOM character in SQL files | Caused by PowerShell file creation — use UTF-8 no BOM |
| RESULT_SCAN(LAST_QUERY_ID()) after CALL | Unreliable — use CREATE TEMP TABLE AS SELECT instead |
| Correlated ML subqueries inside Dynamic Tables | Not supported — materialize separately |
| Streamlit TEMP tables | Session-scoped — not accessible across sessions |
| Altair in Streamlit-in-Snowflake | configure_background("transparent") fails on layered charts |
| MATERIALIZED VIEW naming (MV_ prefix) | Fine as a naming convention, just use regular VIEWs |

---

## 8. Known Python/Windows Gotchas

| Issue | Fix |
|---|---|
| Python 3.13 | Breaks Snowflake packages — use 3.11 only |
| Emoji in scripts | Replace with [OK], [ERROR], [WARN], [DONE] for Windows cp1252 |
| PYTHONIOENCODING | Always set: $env:PYTHONIOENCODING="utf-8" |
| Windows temp dir | Use tempfile.gettempdir() not /tmp |
| Bronze data loading | Use write_pandas() — never PUT/COPY INTO programmatically |
| .env file format | No spaces around =, no quotes: KEY=value |
| Streamlit + Snowflake | Stop Streamlit before testing Snowflake connection |
| npm not found | Restart terminal after Node.js install to pick up PATH |
| Node version | v24.14.1 installed and working |

---

## 9. Loading Credentials in PowerShell

```powershell
# Always run this before any Python script that needs Snowflake
Get-Content .env | ForEach-Object {
    if ($_ -match "^([^#].+)=(.+)$") {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
}
```

---

## 10. Key Commands Reference

```powershell
cd C:\Study\vertiv-data-platform

# Credentials
Get-Content .env | ForEach-Object { if ($_ -match "^([^#].+)=(.+)$") { [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim()) } }

# Deploy SQL steps
python scripts/deploy.py                              # dry run
python scripts/deploy.py --execute                    # full deploy
python scripts/deploy.py --execute --step 6           # single step

# Data loading
python load_data.py                                   # load all Bronze

# DQ Framework
python python/snowpark/dq_framework.py --source SAP
python python/snowpark/dq_framework.py --source ALL

# ML Features
python python/snowpark/feature_engineering.py

# Streamlit DQ Dashboard (local)
streamlit run python/streamlit/streamlit_dq_dashboard.py

# Marketplace frontend (local)
cd marketplace-ui\frontend && npm run dev             # http://localhost:3000

# Marketplace backend (local)
cd marketplace-ui\backend && uvicorn main:app --reload --port 8000

# Git
git add . && git commit -m "message" && git push origin main
```

---

## 11. Current Deployment Status

| Component | Status | URL |
|---|---|---|
| Snowflake platform (all 9 SQL steps) | LIVE | drdwkxe-gmc96114.snowflakecomputing.com |
| DQ Streamlit dashboard | LIVE | https://vertiv-data-platform-tech-challange.streamlit.app |
| Marketplace frontend (local) | WORKING | http://localhost:3000 |
| Marketplace frontend (Amplify) | NOT DEPLOYED | pending |
| Marketplace backend (Lambda) | NOT DEPLOYED | pending |
| ACCESS_REQUESTS table (Snowflake) | NOT CREATED | run sql_access_requests.sql |

---

## 12. Presentation Context

- Interview: Vertiv (power/cooling/data center company)
- Role: Principal Data Architect
- Format: 60 min — 15 min technical deep dive + 15 min executive pitch + 30 min Q&A
- Deck: Vertiv_DataPlatform_Presentation.pptx (19 slides, in Downloads, NOT in git)
  - Vertiv colors: orange #FE5B1B, rainbow bar header
  - Fonts: Georgia (headings), Arial (body)
- RTM: Vertiv_RTM_v1.xlsx (35 requirements mapped, in Downloads)
- Key talking points:
  - Dynamic Tables vs Airflow: no orchestration platform needed
  - SHA256 hash keys: deterministic, no key store, cross-system unique
  - PIT-correct ML: WHERE order_date < AS_OF_DATE (strict less-than)
  - Config-driven: new source = INSERT config rows only
  - Data Mesh: 4 Dehghani principles + 8 product characteristics implemented
  - AI search: Snowflake Cortex chosen over AWS OpenSearch (zero infra, co-located)
  - DAMA 6-dimension DQ: Completeness, Uniqueness, Validity, Accuracy, Consistency, Timeliness

---

## 13. Next Tasks (in priority order)

1. Deploy marketplace frontend to AWS Amplify (get shareable URL)
2. Run sql_access_requests.sql in Snowflake
3. Deploy marketplace backend to AWS Lambda via SAM
4. Run feature_engineering.py to populate ML predictions
5. Run DQ framework for JDE and QAD sources
6. Push all new files to GitHub
7. Rehearse 60-min presentation

---

## 14. Architecture Summary

```
Sources: SAP Europe + JDE Americas + QAD EMEA + Salesforce Global
    ↓ write_pandas (Python)
Bronze (VERTIV_RAW) — raw tables, streams
    ↓ Dynamic Tables (5 min lag)
Silver (VERTIV_CURATED) — SHA256 keys, SCD2, DQ audit, DAMA 6-dim validation
    ↓ Dynamic Tables (15 min lag)
Gold BI (VERTIV_ANALYTICS.BI) — star schema (DIM + FACT)
Gold ML (VERTIV_ANALYTICS.ML) — PIT-correct feature store, XGBoost predictions
Gold AI (VERTIV_ANALYTICS.AI) — Cortex vector embeddings, Cortex Search
    ↓ Secure Views
Data Products (VERTIV_PRODUCTS) — DP-001 to DP-004, output contracts, SLOs
    ↓ Snowflake Shares
External consumers / AWS Lambda API / Streamlit Dashboard / React Marketplace UI
```
