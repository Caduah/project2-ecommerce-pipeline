-- models/intermediate/int_customer_transaction_metrics.sql
-- Per-customer transaction metrics + simple LTV calculation.

{{
    config(
        materialized = 'incremental',
        unique_key   = 'customer_id',
        dist         = 'customer_id',
        sort         = ['customer_id']
    )
}}

with txns as (
    select * from {{ ref('stg_transactions') }}
    where transaction_type = 'purchase'
    and status = 'completed'

    {% if is_incremental() %}
        and transaction_ts > (select max(last_txn_ts) from {{ this }})
    {% endif %}
),

refunds as (
    select
        customer_id,
        sum(amount)     as total_refunded,
        count(*)        as refund_count
    from {{ ref('stg_transactions') }}
    where transaction_type = 'refund'
    and status = 'completed'
    group by customer_id
),

fraud_flags as (
    select
        customer_id,
        sum(risk_score)                                         as total_risk_score,
        count(case when risk_tier = 'HIGH' then 1 end)          as high_risk_txn_count,
        max(risk_score)                                         as max_risk_score,
        bool_or(flag_velocity_spike)                            as ever_velocity_spike,
        bool_or(flag_amount_anomaly)                            as ever_amount_anomaly
    from {{ ref('stg_transactions') }}
    group by customer_id
),

txn_metrics as (
    select
        t.customer_id,

        -- Volume
        count(distinct t.transaction_id)            as total_txn_count,
        count(distinct t.txn_date)                  as active_days,

        -- Revenue
        sum(t.amount)                               as gross_txn_revenue,
        avg(t.amount)                               as avg_txn_amount,
        max(t.amount)                               as max_txn_amount,

        -- International
        sum(case when t.is_international then 1 else 0 end) as intl_txn_count,
        round(
            sum(case when t.is_international then 1 else 0 end)::decimal
            / nullif(count(*), 0) * 100, 2
        )                                           as intl_txn_rate_pct,

        -- Device mix
        count(distinct t.device_type)               as distinct_devices,
        max(t.device_type)                          as most_recent_device,

        -- Timestamps
        min(t.transaction_ts)                       as first_txn_ts,
        max(t.transaction_ts)                       as last_txn_ts,

        current_timestamp                           as updated_at

    from txns t
    group by t.customer_id
),

combined as (
    select
        m.customer_id,
        m.total_txn_count,
        m.active_days,
        m.gross_txn_revenue,
        m.avg_txn_amount,
        m.max_txn_amount,
        m.intl_txn_count,
        m.intl_txn_rate_pct,
        m.distinct_devices,
        m.most_recent_device,
        m.first_txn_ts,
        m.last_txn_ts,

        -- Refunds
        coalesce(r.total_refunded, 0)               as total_refunded,
        coalesce(r.refund_count, 0)                 as refund_count,
        m.gross_txn_revenue - coalesce(r.total_refunded, 0) as net_txn_revenue,

        -- Fraud
        coalesce(f.total_risk_score, 0)             as total_risk_score,
        coalesce(f.high_risk_txn_count, 0)          as high_risk_txn_count,
        coalesce(f.max_risk_score, 0)               as max_risk_score,
        coalesce(f.ever_velocity_spike, false)       as ever_velocity_spike,
        coalesce(f.ever_amount_anomaly, false)       as ever_amount_anomaly,

        -- Simple LTV: net revenue × predicted retention factor
        -- Replace with ML model output in Phase 6
        round(
            (m.gross_txn_revenue - coalesce(r.total_refunded, 0))
            * case
                when m.active_days > 180 then 2.5
                when m.active_days > 90  then 1.8
                when m.active_days > 30  then 1.3
                else 1.0
              end,
            2
        )                                           as estimated_ltv,

        m.updated_at

    from txn_metrics m
    left join refunds r      on m.customer_id = r.customer_id
    left join fraud_flags f  on m.customer_id = f.customer_id
)

select * from combined
