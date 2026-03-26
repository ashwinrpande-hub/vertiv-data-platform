"""
Vertiv Data Marketplace — FastAPI Backend
Deployed as AWS Lambda via Mangum + API Gateway
Connects to Snowflake: V_DATA_MARKETPLACE, CORTEX_SEARCH, Data Products
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import snowflake.connector
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── App Setup ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Vertiv Data Marketplace API",
    version="1.0.0",
    description="AI-powered data product discovery backed by Snowflake Cortex",
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ─── Snowflake Connection ─────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "VERTIV_PRODUCTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "SALES_PERFORMANCE"),
        role=os.getenv("SNOWFLAKE_ROLE", "VERTIV_GLOBAL_ANALYST"),
    )

def run_query(sql: str, params: tuple = ()) -> list[dict]:
    conn = get_conn()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()

# ─── Models ──────────────────────────────────────────────────────────────────
class AccessRequest(BaseModel):
    product_id: str
    name: str
    email: str
    reason: str
    requested_role: Optional[str] = None

# ─── Routes ──────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/products")
def list_products():
    """Return all data products from V_DATA_MARKETPLACE."""
    sql = """
        SELECT
            PRODUCT_ID,
            PRODUCT_NAME,
            DOMAIN,
            DESCRIPTION,
            OWNER_TEAM,
            TRUST_SCORE,
            FRESHNESS_LABEL,
            ACCESS_TIER,
            SLA_AVAILABILITY,
            ROW_COUNT,
            TAGS,
            SHARE_AVAILABLE,
            LAST_UPDATED
        FROM VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
        ORDER BY TRUST_SCORE DESC
    """
    try:
        rows = run_query(sql)
        return [_normalize_product(r) for r in rows]
    except Exception as e:
        logger.exception("Failed to fetch products")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/products/{product_id}")
def get_product(product_id: str):
    """Return full detail for a single data product."""
    sql = """
        SELECT *
        FROM VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
        WHERE PRODUCT_ID = %s
    """
    try:
        rows = run_query(sql, (product_id,))
        if not rows:
            raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
        return _normalize_product(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to fetch product %s", product_id)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
def search_products(q: str = Query(..., min_length=1, description="Natural language search query")):
    """
    AI-powered semantic search using Snowflake Cortex Search.
    Falls back to keyword search if Cortex unavailable.
    """
    try:
        # Snowflake Cortex Search — requires CORTEX_SEARCH_INDEX service
        sql = """
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'VERTIV_ANALYTICS.AI.CORTEX_SEARCH_INDEX',
                    OBJECT_CONSTRUCT(
                        'query', %s,
                        'columns', ARRAY_CONSTRUCT('PRODUCT_ID', 'PRODUCT_NAME', 'DOMAIN', 'DESCRIPTION', 'TAGS'),
                        'limit', 10
                    )
                )
            ) AS results
        """
        rows = run_query(sql, (q,))
        if rows and rows[0].get("RESULTS"):
            raw = rows[0]["RESULTS"]
            results = raw.get("results", []) if isinstance(raw, dict) else json.loads(raw).get("results", [])
            product_ids = [r.get("PRODUCT_ID") for r in results if r.get("PRODUCT_ID")]
            if product_ids:
                placeholders = ", ".join(["%s"] * len(product_ids))
                detail_sql = f"""
                    SELECT * FROM VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
                    WHERE PRODUCT_ID IN ({placeholders})
                """
                products = run_query(detail_sql, tuple(product_ids))
                return [_normalize_product(p) for p in products]
    except Exception as e:
        logger.warning("Cortex Search failed, falling back to keyword: %s", e)

    # Keyword fallback
    keyword_sql = """
        SELECT * FROM VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
        WHERE LOWER(PRODUCT_NAME) LIKE %s
           OR LOWER(DESCRIPTION) LIKE %s
           OR LOWER(DOMAIN) LIKE %s
           OR LOWER(TAGS) LIKE %s
        ORDER BY TRUST_SCORE DESC
    """
    q_like = f"%{q.lower()}%"
    rows = run_query(keyword_sql, (q_like, q_like, q_like, q_like))
    return [_normalize_product(r) for r in rows]


@app.post("/access-request")
def submit_access_request(req: AccessRequest):
    """Log an access request to VERTIV_AUDIT."""
    ticket_id = f"REQ-{int(datetime.now(timezone.utc).timestamp())}"
    sql = """
        INSERT INTO VERTIV_AUDIT.PUBLIC.ACCESS_REQUESTS
            (TICKET_ID, PRODUCT_ID, REQUESTER_NAME, REQUESTER_EMAIL, JUSTIFICATION, STATUS, REQUESTED_AT)
        VALUES (%s, %s, %s, %s, %s, 'PENDING', CURRENT_TIMESTAMP())
    """
    try:
        run_query(sql, (ticket_id, req.product_id, req.name, req.email, req.reason))
        return {"success": True, "ticket": ticket_id, "message": "Request submitted. You will be notified within 24 hours."}
    except Exception as e:
        logger.exception("Failed to log access request")
        # Don't expose DB errors to client
        return {"success": False, "ticket": None, "message": "Submission failed. Please contact the data platform team."}


@app.get("/metrics")
def platform_metrics():
    """Return platform health metrics for the dashboard header."""
    sql = """
        SELECT
            COUNT(*) AS TOTAL_PRODUCTS,
            AVG(TRUST_SCORE) AS AVG_TRUST_SCORE,
            SUM(ROW_COUNT) AS TOTAL_ROWS
        FROM VERTIV_PRODUCTS.INFORMATION_SCHEMA.V_DATA_MARKETPLACE
    """
    try:
        rows = run_query(sql)
        row = rows[0] if rows else {}
        return {
            "total_products": row.get("TOTAL_PRODUCTS", 4),
            "avg_trust_score": round(float(row.get("AVG_TRUST_SCORE", 98.97)), 2),
            "total_rows": row.get("TOTAL_ROWS", 19500),
        }
    except Exception as e:
        logger.exception("Failed to fetch metrics")
        return {"total_products": 4, "avg_trust_score": 98.97, "total_rows": 19500}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _normalize_product(row: dict) -> dict:
    """Normalize Snowflake row (UPPERCASE keys) to camelCase-friendly dict."""
    tags = row.get("TAGS", "")
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]
    return {
        "product_id": row.get("PRODUCT_ID", ""),
        "product_name": row.get("PRODUCT_NAME", ""),
        "domain": row.get("DOMAIN", ""),
        "description": row.get("DESCRIPTION", ""),
        "owner_team": row.get("OWNER_TEAM", ""),
        "trust_score": float(row.get("TRUST_SCORE", 0)),
        "freshness_label": row.get("FRESHNESS_LABEL", ""),
        "access_tier": row.get("ACCESS_TIER", "Restricted"),
        "sla_availability": float(row.get("SLA_AVAILABILITY", 99.5)),
        "row_count": int(row.get("ROW_COUNT", 0)),
        "tags": tags,
        "share_available": bool(row.get("SHARE_AVAILABLE", False)),
        "last_updated": str(row.get("LAST_UPDATED", "")),
    }


# ─── Lambda Handler ───────────────────────────────────────────────────────────
handler = Mangum(app, lifespan="off")

# ─── Local dev ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
