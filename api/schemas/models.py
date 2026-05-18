"""
api/schemas/models.py
Pydantic request/response models for all endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class Customer360(BaseModel):
    customer_id:           str
    resolved_entity_id:    Optional[str]
    customer_name:         Optional[str]
    email:                 Optional[str]
    country:               Optional[str]
    segment:               Optional[str]
    loyalty_tier:          Optional[str]
    ltv_tier:              Optional[str]
    churn_status:          Optional[str]
    total_orders:          int = 0
    total_order_revenue:   float = 0.0
    avg_order_value:       float = 0.0
    revenue_last_30d:      float = 0.0
    estimated_ltv:         float = 0.0
    total_risk_score:      int = 0
    high_risk_txn_count:   int = 0
    ever_velocity_spike:   bool = False
    ever_amount_anomaly:   bool = False


class CustomerGraphContext(BaseModel):
    customer_id:        str
    total_orders:       int
    total_spend:        float
    distinct_merchants: int
    total_transactions: int
    avg_txn_amount:     float
    linked_profiles:    int


class DailyOrderSummary(BaseModel):
    order_date:         date
    shipping_country:   str
    net_revenue:        float
    order_count:        int
    unique_customers:   int
    avg_order_value:    float
    new_customers:      int
    returning_customers:int


class FraudSummary(BaseModel):
    txn_date:           date
    risk_tier:          str
    merchant_category:  str
    txn_count:          int
    flagged_txn_count:  int
    high_risk_count:    int
    flag_rate_pct:      float
    amount_at_risk:     float


class EntityProfile(BaseModel):
    customer_id:         str
    source_system:       Optional[str]
    email:               Optional[str]
    er_confidence:       Optional[float]
    linked_profiles:     list[str] = []


class RAGQueryRequest(BaseModel):
    question: str = Field(
        ...,
        description="Natural language question about your data",
        examples=["Which customers have the highest LTV in the US?",
                  "Show me high-risk transactions from last week",
                  "What is the fraud rate by merchant category?"]
    )
    max_rows: int = Field(default=10, ge=1, le=100)


class RAGQueryResponse(BaseModel):
    question:    str
    answer:      str
    sql:         Optional[str] = None
    data:        Optional[list[dict]] = None
    row_count:   int = 0
    data_source: str = "redshift_gold"
