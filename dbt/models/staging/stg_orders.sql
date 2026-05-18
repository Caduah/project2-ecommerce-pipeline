-- models/staging/stg_orders.sql
-- Thin view on top of staging.orders.
-- Only renames columns, casts types, and applies light filters.
-- No business logic here — that lives in intermediate models.

with source as (
    select * from {{ source('staging', 'orders') }}
),

renamed as (
    select
        -- Keys
        order_id,
        customer_id,
        merchant_id,

        -- Status
        order_status,
        case
            when order_status in ('delivered', 'shipped') then true
            else false
        end                                     as is_completed,

        -- Timestamps
        order_ts,
        updated_ts,
        order_date,
        order_year,
        order_month,
        order_dow,
        is_weekend,

        -- Financials
        currency,
        gross_amount,
        coalesce(discount_amount, 0)            as discount_amount,
        coalesce(discount_pct, 0)               as discount_pct,
        net_amount,
        coalesce(item_count, 1)                 as item_count,

        -- Attributes
        payment_method,
        shipping_country,
        source_system,

        -- Audit
        ingest_ts,
        silver_ts,
        pipeline_version

    from source
    where
        order_id is not null
        and customer_id is not null
        and order_ts is not null
        and net_amount >= 0       -- exclude negative net amounts (data error)
)

select * from renamed
