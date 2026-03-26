"""
Vertiv DataMesh Marketplace API
FastAPI backend connecting to Snowflake
Deploy: AWS Lambda via Mangum | Local: uvicorn main:app --reload
"""
import os
from typing import Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="Vertiv DataMesh Marketplace API",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="ANALYTICS_WH",
        database="VERTIV_CONFIG",
        schema="METADATA",
        role="VERTIV_GLOBAL_ANALYST",
    )

def query_sf(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [c[0].lower() for c in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close(); conn.close()
    return rows

class AccessRequest(BaseModel):
    product_id: str
    requester: str
    team: str
    use_case: str
    email: str

@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/products")
def list_products(domain: Optional[str] = None, min_trust: float = 0.0):
    rows = query_sf("""
        SELECT m.PRODUCT_ID, m.PRODUCT_NAME, m.PRODUCT_VERSION,
               m.DOMAIN_NAME, m.DOMAIN_OWNER_TEAM, m.DESCRIPTION,
               m.USE_CASES, m.SLO_FRESHNESS_MINUTES, m.SLO_AVAILABILITY_PCT,
               m.ACCESS_CLASSIFICATION, m.REQUIRED_ROLE,
               m.PII_PRESENT, m.GDPR_APPLICABLE, m.STATUS,
               t.TRUST_SCORE, t.TRUST_BAND, t.OVERALL_DQ_SCORE,
               t.DATA_AGE_MINUTES, t.FRESHNESS_MET
        FROM VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG m
        LEFT JOIN VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE t
          ON m.PRODUCT_ID = t.PRODUCT_ID
        WHERE m.STATUS = 'ACTIVE'
          AND COALESCE(t.TRUST_SCORE, 0) >= %s
    """, [min_trust])
    if domain:
        rows = [r for r in rows if r.get("domain_name","").upper() == domain.upper()]
    return rows

@app.get("/api/products/{product_id}")
def get_product(product_id: str):
    rows = query_sf("""
        SELECT m.*, t.TRUST_SCORE, t.TRUST_BAND, t.OVERALL_DQ_SCORE,
               t.DQ_COMPLETENESS_PCT, t.DQ_ACCURACY_PCT, t.DQ_VALIDITY_PCT,
               t.DQ_UNIQUENESS_PCT, t.DQ_TIMELINESS_PCT, t.DQ_CONSISTENCY_PCT,
               t.DATA_AGE_MINUTES
        FROM VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG m
        LEFT JOIN VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE t
          ON m.PRODUCT_ID = t.PRODUCT_ID
        WHERE m.PRODUCT_ID = %s
    """, [product_id])
    if not rows:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return rows[0]

@app.get("/api/products/{product_id}/schema")
def get_schema(product_id: str):
    rows = query_sf("""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, IS_PII,
               IS_MASKED, DESCRIPTION, EXAMPLE_VALUE
        FROM VERTIV_CONFIG.METADATA.PRODUCT_OUTPUT_CONTRACT
        WHERE PRODUCT_ID = %s AND CONTRACT_VERSION = 'v1'
        ORDER BY CONTRACT_ID
    """, [product_id])
    return {"product_id": product_id, "columns": rows}

@app.get("/api/products/{product_id}/trust")
def get_trust(product_id: str):
    current = query_sf("SELECT * FROM VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE WHERE PRODUCT_ID = %s", [product_id])
    history = query_sf("SELECT * FROM VERTIV_CONFIG.METADATA.PRODUCT_SLI_LOG WHERE PRODUCT_ID = %s ORDER BY MEASUREMENT_TIME DESC LIMIT 30", [product_id])
    return {"product_id": product_id, "current": current[0] if current else {}, "history": history}

@app.get("/api/search")
def ai_search(q: str = Query(..., min_length=2)):
    try:
        rows = query_sf("""
            SELECT ENTITY_TYPE, ENTITY_HK, ENTITY_NAME, METADATA,
                   VECTOR_COSINE_SIMILARITY(
                     SEARCH_VECTOR,
                     SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', %s)
                   ) AS SIMILARITY_SCORE
            FROM VERTIV_ANALYTICS.AI.CORTEX_SEARCH_INDEX
            ORDER BY SIMILARITY_SCORE DESC LIMIT 5
        """, [q])
        return {"mode": "cortex_semantic", "results": rows}
    except Exception:
        rows = query_sf("""
            SELECT m.PRODUCT_ID, m.PRODUCT_NAME, m.DOMAIN_NAME,
                   m.DESCRIPTION, t.TRUST_SCORE, t.TRUST_BAND, 0.75 AS SIMILARITY_SCORE
            FROM VERTIV_CONFIG.METADATA.DATA_PRODUCT_CATALOG m
            LEFT JOIN VERTIV_CONFIG.METADATA.V_PRODUCT_TRUST_SCORE t ON m.PRODUCT_ID = t.PRODUCT_ID
            WHERE LOWER(m.DESCRIPTION) LIKE %s OR LOWER(m.PRODUCT_NAME) LIKE %s
            ORDER BY t.TRUST_SCORE DESC LIMIT 5
        """, [f"%{q.lower()}%", f"%{q.lower()}%"])
        return {"mode": "keyword_fallback", "results": rows}

@app.get("/api/marketplace")
def marketplace():
    rows = query_sf("SELECT * FROM VERTIV_CONFIG.METADATA.V_DATA_MARKETPLACE ORDER BY TRUST_SCORE DESC")
    return {"products": rows, "count": len(rows)}

@app.post("/api/access/request")
def request_access(req: AccessRequest):
    return {
        "status": "submitted",
        "request_id": f"REQ-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "product_id": req.product_id,
        "message": "Request submitted. Domain owner will review within 2 business days.",
    }

try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except ImportError:
    pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
