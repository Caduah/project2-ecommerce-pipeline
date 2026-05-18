-- models/staging/stg_transactions.sql

with source as (
    select * from {{ source('staging', 'transactions') }}
),

renamed as (
    select
        -- Keys
        transaction_id,
        order_id,
        customer_id,
        merchant_id,

        -- Core fields
        transaction_type,
        status,
        amount,
        currency,
        payment_method,

        -- Card info (partially masked)
        card_bin,
        card_last4,

        -- Location / device
        is_international,
        ip_country,
        device_type,
        merchant_category,

        -- Fraud signals from silver
        txn_velocity_1h,
        coalesce(avg_amount_30d, 0)         as avg_amount_30d,
        coalesce(stddev_amount_30d, 0)      as stddev_amount_30d,
        coalesce(txn_count_30d, 0)          as txn_count_30d,
        flag_high_value,
        flag_velocity_spike,
        flag_amount_anomaly,
        flag_intl_high_value,
        flag_new_customer_high_value,
        risk_score,
        risk_tier,

        -- Timestamps
        transaction_ts,
        txn_date,
        txn_year,
        txn_month,

        -- Audit
        source_system,
        ingest_ts,
        silver_ts,
        pipeline_version

    from source
    where
        transaction_id is not null
        and customer_id is not null
        and merchant_id is not null
        and transaction_ts is not null
        and amount is not null
)

select * from renamed
