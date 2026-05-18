"""
api/routers/query.py - clean rewrite with correct schema dbt_dev_gold
"""
from fastapi import APIRouter, HTTPException, Depends
from api.core.database import get_redshift_pool
from api.core.config import settings
from api.schemas.models import RAGQueryRequest, RAGQueryResponse
import json, logging, re

log = logging.getLogger(__name__)
router = APIRouter()

SYSTEM_PROMPT = """You are a data analyst. Convert questions to SQL.
Tables available:
  dbt_dev_gold.mart_customer_360 - customer profiles with ltv, orders, risk
  dbt_dev_gold.mart_daily_order_summary - daily order KPIs
  dbt_dev_gold.mart_fraud_summary - fraud signals by risk tier

Rules: PostgreSQL only. Always use dbt_dev_gold. schema prefix. Always LIMIT 100.
Return ONLY JSON: {"sql": "...", "explanation": "..."}"""

async def generate_sql(question):
    if not settings.anthropic_api_key:
        return (
            "SELECT customer_id, customer_name, country, estimated_ltv, ltv_tier "
            "FROM dbt_dev_gold.mart_customer_360 ORDER BY estimated_ltv DESC LIMIT 10",
            "Top 10 customers by LTV (demo mode)"
        )
    import anthropic
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514", max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    txt = msg.content[0].text.strip()
    try:
        p = json.loads(txt)
        return p.get("sql"), p.get("explanation", "")
    except Exception:
        m = re.search(r'\{.*\}', txt, re.DOTALL)
        if m:
            p = json.loads(m.group())
            return p.get("sql"), p.get("explanation", "")
        return None, txt

def is_safe(sql):
    if not sql: return False
    u = sql.upper().strip()
    if not u.startswith("SELECT"): return False
    for kw in ["INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE"]:
        if kw in u: return False
    return True

@router.post("/", response_model=RAGQueryResponse)
async def natural_language_query(request: RAGQueryRequest, db=Depends(get_redshift_pool)):
    log.info(f"RAG query: {request.question}")
    sql, explanation = await generate_sql(request.question)
    if not sql:
        return RAGQueryResponse(question=request.question, answer=explanation,
                                sql=None, data=None, row_count=0, data_source="none")
    if not is_safe(sql):
        raise HTTPException(status_code=400, detail="Query failed safety check")
    try:
        cur = db.cursor()
        safe_sql = sql.rstrip(";")
        if "LIMIT" not in safe_sql.upper():
            safe_sql += f" LIMIT {request.max_rows}"
        cur.execute(safe_sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        data = [dict(zip(cols, r)) for r in rows]
        for row in data:
            for k, v in row.items():
                if hasattr(v, 'isoformat'): row[k] = v.isoformat()
        return RAGQueryResponse(question=request.question, answer=explanation,
                                sql=safe_sql, data=data, row_count=len(data),
                                data_source="local_postgres")
    except Exception as e:
        log.error(f"SQL error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
