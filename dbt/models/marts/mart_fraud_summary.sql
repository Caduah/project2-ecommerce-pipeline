-- models/marts/mart_fraud_summary.sql
-- Daily fraud signal summary — feeds the risk dashboard and
-- is synced to Snowflake for the fraud analytics team.

{{
    config(
        materialized = 'table',
        sort         = ['txn_date', 'risk_tier'],
        tags         = ['gold', 'fraud', 'daily']
    )
}}

with txns as (
    select * from {{ ref('stg_transactions') }}
),

daily_fraud as (
    select
        txn_date,
        txn_year,
        txn_month,
        risk_tier,
        merchant_category,
        is_international,
        device_type,

        -- Volume
        count(distinct transaction_id)          as txn_count,
        count(distinct customer_id)             as unique_customers,
        count(distinct merchant_id)             as unique_merchants,

        -- Amount
        sum(amount)                             as total_amount,
        avg(amount)                             as avg_amount,

        -- Flag counts
        sum(case when flag_high_value           then 1 else 0 end) as high_value_count,
        sum(case when flag_velocity_spike       then 1 else 0 end) as velocity_spike_count,
        sum(case when flag_amount_anomaly       then 1 else 0 end) as amount_anomaly_count,
        sum(case when flag_intl_high_value      then 1 else 0 end) as intl_high_value_count,
        sum(case when flag_new_customer_high_value then 1 else 0 end) as new_cust_high_value_count,

        -- Any flag
        sum(case when risk_score > 0 then 1 else 0 end)  as flagged_txn_count,
        sum(case when risk_tier = 'HIGH' then 1 else 0 end) as high_risk_count,

        -- Flag rates
        round(
            sum(case when risk_score > 0 then 1 else 0 end)::decimal
            / nullif(count(*), 0) * 100, 2
        )                                               as flag_rate_pct,
        round(
            sum(case when risk_tier = 'HIGH' then 1 else 0 end)::decimal
            / nullif(count(*), 0) * 100, 2
        )                                               as high_risk_rate_pct,

        -- Amount at risk (flagged transactions)
        sum(case when risk_score > 0 then amount else 0 end) as amount_at_risk,

        -- Velocity stats
        avg(txn_velocity_1h)                            as avg_velocity_1h,
        max(txn_velocity_1h)                            as max_velocity_1h,

        current_timestamp                               as mart_updated_at

    from txns
    group by
        txn_date, txn_year, txn_month, risk_tier,
        merchant_category, is_international, device_type
)

select * from daily_fraud
