-- models/marts/mart_customer_360.sql
-- The single most important mart in the project.
-- Joins customer profile + order metrics + transaction metrics
-- into one wide table. Every BI dashboard and API endpoint starts here.

{{
    config(
        materialized = 'table',
        dist         = 'customer_id',
        sort         = ['customer_id'],
        tags         = ['gold', 'customer', 'priority']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_metrics as (
    select * from {{ ref('int_customer_order_metrics') }}
),

txn_metrics as (
    select * from {{ ref('int_customer_transaction_metrics') }}
),

final as (
    select
        -- ── Identity ──────────────────────────────────────────────
        c.customer_id,
        c.resolved_entity_id,
        c.customer_name,
        c.email,
        c.phone,

        -- ── Profile ───────────────────────────────────────────────
        c.country,
        c.city,
        c.age_band,
        c.segment,
        c.loyalty_tier,
        c.is_active,
        c.registration_ts,
        c.customer_tenure_band,
        c.days_since_registration,

        -- ── Order metrics ─────────────────────────────────────────
        coalesce(o.total_orders, 0)             as total_orders,
        coalesce(o.delivered_orders, 0)         as delivered_orders,
        coalesce(o.cancelled_orders, 0)         as cancelled_orders,
        coalesce(o.refunded_orders, 0)          as refunded_orders,
        coalesce(o.total_revenue, 0)            as total_order_revenue,
        coalesce(o.avg_order_value, 0)          as avg_order_value,
        coalesce(o.total_items, 0)              as total_items_ordered,
        coalesce(o.cancellation_rate_pct, 0)    as cancellation_rate_pct,
        coalesce(o.refund_rate_pct, 0)          as refund_rate_pct,
        coalesce(o.orders_per_month, 0)         as orders_per_month,
        coalesce(o.revenue_last_30d, 0)         as revenue_last_30d,
        coalesce(o.revenue_last_90d, 0)         as revenue_last_90d,
        coalesce(o.orders_last_30d, 0)          as orders_last_30d,
        o.first_order_ts,
        o.last_order_ts,
        o.customer_lifespan_days,
        o.most_recent_payment_method,

        -- ── Transaction metrics ───────────────────────────────────
        coalesce(t.total_txn_count, 0)          as total_txn_count,
        coalesce(t.gross_txn_revenue, 0)        as gross_txn_revenue,
        coalesce(t.net_txn_revenue, 0)          as net_txn_revenue,
        coalesce(t.avg_txn_amount, 0)           as avg_txn_amount,
        coalesce(t.refund_count, 0)             as txn_refund_count,
        coalesce(t.total_refunded, 0)           as total_refunded,
        coalesce(t.intl_txn_rate_pct, 0)        as intl_txn_rate_pct,
        t.most_recent_device,
        t.first_txn_ts,
        t.last_txn_ts,

        -- ── LTV & risk ────────────────────────────────────────────
        coalesce(t.estimated_ltv, 0)            as estimated_ltv,
        coalesce(t.total_risk_score, 0)         as total_risk_score,
        coalesce(t.high_risk_txn_count, 0)      as high_risk_txn_count,
        coalesce(t.ever_velocity_spike, false)  as ever_velocity_spike,
        coalesce(t.ever_amount_anomaly, false)  as ever_amount_anomaly,

        -- ── LTV tier (for segmentation) ───────────────────────────
        case
            when coalesce(t.estimated_ltv, 0) >= 10000  then 'platinum'
            when coalesce(t.estimated_ltv, 0) >= 5000   then 'gold'
            when coalesce(t.estimated_ltv, 0) >= 1000   then 'silver'
            when coalesce(t.estimated_ltv, 0) >= 100    then 'bronze'
            else 'new'
        end                                             as ltv_tier,

        -- ── Churn risk (simple rules — replace with ML in Phase 6) ─
        case
            when coalesce(o.orders_last_30d, 0) = 0
              and coalesce(o.total_orders, 0) > 3      then 'at_risk'
            when coalesce(o.last_order_ts, '2000-01-01')
              < dateadd('day', -90, current_timestamp)  then 'churned'
            when coalesce(o.orders_per_month, 0) >= 2  then 'loyal'
            else 'active'
        end                                             as churn_status,

        -- ── Audit ─────────────────────────────────────────────────
        c.source_system,
        c.er_confidence,
        current_timestamp                               as mart_updated_at

    from customers c
    left join order_metrics o   on c.customer_id = o.customer_id
    left join txn_metrics   t   on c.customer_id = t.customer_id
)

select * from final
