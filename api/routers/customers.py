"""
api/routers/customers.py
Customer 360 endpoints.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from api.core.database import get_redshift_pool, get_neo4j_driver
from api.schemas.models import Customer360, CustomerGraphContext
import logging

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/{customer_id}", response_model=Customer360)
async def get_customer_360(
    customer_id: str,
    db=Depends(get_redshift_pool),
):
    """
    Returns full customer 360 profile from the gold mart.
    Includes order metrics, transaction metrics, LTV, and risk signals.
    """
    sql = """
        SELECT
            customer_id, resolved_entity_id, customer_name, email,
            country, segment, loyalty_tier, ltv_tier, churn_status,
            total_orders, total_order_revenue, avg_order_value,
            revenue_last_30d, estimated_ltv, total_risk_score,
            high_risk_txn_count, ever_velocity_spike, ever_amount_anomaly
        FROM gold.mart_customer_360
        WHERE customer_id = %s
        LIMIT 1
    """
    try:
        cur = db.cursor()
        cur.execute(sql, (customer_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        cols = [d[0] for d in cur.description]
        return Customer360(**dict(zip(cols, row)))
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error fetching customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{customer_id}/graph", response_model=CustomerGraphContext)
async def get_customer_graph_context(
    customer_id: str,
    neo4j=Depends(get_neo4j_driver),
):
    """
    Returns customer context from the Neo4j knowledge graph.
    Shows orders, merchants, transactions, and linked entity profiles.
    """
    cypher = """
        MATCH (c:Customer {customer_id: $customer_id})
        OPTIONAL MATCH (c)-[:PLACED]->(o:Order)
        OPTIONAL MATCH (o)-[:PAID_VIA]->(m:Merchant)
        OPTIONAL MATCH (c)-[:MADE]->(t:Transaction)
        OPTIONAL MATCH (c)-[:SAME_AS]-(twin:Customer)
        RETURN
            c.customer_id           AS customer_id,
            COUNT(DISTINCT o)       AS total_orders,
            SUM(o.net_amount)       AS total_spend,
            COUNT(DISTINCT m)       AS distinct_merchants,
            COUNT(DISTINCT t)       AS total_transactions,
            AVG(t.amount)           AS avg_txn_amount,
            COUNT(DISTINCT twin)    AS linked_profiles
    """
    try:
        with neo4j.session() as session:
            result = session.run(cypher, customer_id=customer_id)
            record = result.single()
            if not record or record["customer_id"] is None:
                raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found in graph")
            return CustomerGraphContext(
                customer_id        = record["customer_id"],
                total_orders       = record["total_orders"] or 0,
                total_spend        = float(record["total_spend"] or 0),
                distinct_merchants = record["distinct_merchants"] or 0,
                total_transactions = record["total_transactions"] or 0,
                avg_txn_amount     = float(record["avg_txn_amount"] or 0),
                linked_profiles    = record["linked_profiles"] or 0,
            )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Neo4j error for customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=list[Customer360])
async def list_customers(
    country:    Optional[str] = None,
    ltv_tier:   Optional[str] = None,
    churn_status: Optional[str] = None,
    limit:      int = Query(default=20, ge=1, le=100),
    offset:     int = Query(default=0, ge=0),
    db=Depends(get_redshift_pool),
):
    """List customers with optional filtering."""
    conditions = ["1=1"]
    params = []

    if country:
        conditions.append("country = %s")
        params.append(country)
    if ltv_tier:
        conditions.append("ltv_tier = %s")
        params.append(ltv_tier)
    if churn_status:
        conditions.append("churn_status = %s")
        params.append(churn_status)

    params.extend([limit, offset])
    sql = f"""
        SELECT customer_id, resolved_entity_id, customer_name, email,
               country, segment, loyalty_tier, ltv_tier, churn_status,
               total_orders, total_order_revenue, avg_order_value,
               revenue_last_30d, estimated_ltv, total_risk_score,
               high_risk_txn_count, ever_velocity_spike, ever_amount_anomaly
        FROM gold.mart_customer_360
        WHERE {' AND '.join(conditions)}
        ORDER BY estimated_ltv DESC
        LIMIT %s OFFSET %s
    """
    try:
        cur = db.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [Customer360(**dict(zip(cols, row))) for row in rows]
    except Exception as e:
        log.error(f"Error listing customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))
