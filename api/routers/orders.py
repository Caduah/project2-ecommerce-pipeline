"""
api/routers/orders.py
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from api.core.database import get_redshift_pool
from api.schemas.models import DailyOrderSummary
import logging

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/summary", response_model=list[DailyOrderSummary])
async def get_order_summary(
    days:    int = Query(default=30, ge=1, le=365),
    country: Optional[str] = None,
    db=Depends(get_redshift_pool),
):
    """Daily order KPIs for the last N days."""
    conditions = ["order_date >= CURRENT_DATE - INTERVAL '%s days'"]
    params = [days]
    if country:
        conditions.append("shipping_country = %s")
        params.append(country)

    sql = f"""
        SELECT
            order_date, shipping_country,
            SUM(net_revenue)::float         AS net_revenue,
            SUM(order_count)::int           AS order_count,
            SUM(unique_customers)::int      AS unique_customers,
            AVG(avg_order_value)::float     AS avg_order_value,
            SUM(new_customers)::int         AS new_customers,
            SUM(returning_customers)::int   AS returning_customers
        FROM gold.mart_daily_order_summary
        WHERE {' AND '.join(conditions)}
        GROUP BY order_date, shipping_country
        ORDER BY order_date DESC
        LIMIT 500
    """
    try:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [DailyOrderSummary(**dict(zip(cols, r))) for r in rows]
    except Exception as e:
        log.error(f"Error fetching order summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))
