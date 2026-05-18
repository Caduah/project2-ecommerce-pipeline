"""
api/routers/fraud.py
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from api.core.database import get_redshift_pool
from api.schemas.models import FraudSummary
import logging

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/summary", response_model=list[FraudSummary])
async def get_fraud_summary(
    days:      int = Query(default=30, ge=1, le=180),
    risk_tier: Optional[str] = None,
    db=Depends(get_redshift_pool),
):
    """Fraud signal summary by risk tier and merchant category."""
    conditions = ["txn_date >= CURRENT_DATE - INTERVAL '%s days'"]
    params = [days]
    if risk_tier:
        conditions.append("risk_tier = %s")
        params.append(risk_tier.upper())

    sql = f"""
        SELECT
            txn_date, risk_tier, merchant_category,
            SUM(txn_count)::int             AS txn_count,
            SUM(flagged_txn_count)::int      AS flagged_txn_count,
            SUM(high_risk_count)::int        AS high_risk_count,
            AVG(flag_rate_pct)::float        AS flag_rate_pct,
            SUM(amount_at_risk)::float       AS amount_at_risk
        FROM gold.mart_fraud_summary
        WHERE {' AND '.join(conditions)}
        GROUP BY txn_date, risk_tier, merchant_category
        ORDER BY txn_date DESC, high_risk_count DESC
        LIMIT 200
    """
    try:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [FraudSummary(**dict(zip(cols, r))) for r in rows]
    except Exception as e:
        log.error(f"Error fetching fraud summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))
