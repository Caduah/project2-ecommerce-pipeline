-- models/intermediate/int_customer_order_metrics.sql
-- Computes per-customer order metrics used by multiple downstream marts.
-- Materialized as incremental — only processes new orders each run.

{{
    config(
        materialized = 'incremental',
        unique_key   = 'customer_id',
        dist         = 'customer_id',
        sort         = ['customer_id'],
        on_schema_change = 'append_new_columns'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Only look at orders updated since last run
        where order_ts > (select max(last_order_ts) from {{ this }})
    {% endif %}
),

customer_metrics as (
    select
        customer_id,

        -- Volume
        count(distinct order_id)                        as total_orders,
        count(distinct case
            when order_status = 'delivered' then order_id
        end)                                            as delivered_orders,
        count(distinct case
            when order_status = 'cancelled' then order_id
        end)                                            as cancelled_orders,
        count(distinct case
            when order_status = 'refunded' then order_id
        end)                                            as refunded_orders,

        -- Revenue
        sum(net_amount)                                 as total_revenue,
        avg(net_amount)                                 as avg_order_value,
        max(net_amount)                                 as max_order_value,
        min(net_amount)                                 as min_order_value,
        sum(discount_amount)                            as total_discounts,
        avg(discount_pct)                               as avg_discount_pct,

        -- Items
        sum(item_count)                                 as total_items,
        avg(item_count)                                 as avg_items_per_order,

        -- Behaviour
        count(distinct case
            when is_weekend then order_id
        end)                                            as weekend_orders,
        count(distinct payment_method)                  as distinct_payment_methods,
        count(distinct shipping_country)                as distinct_shipping_countries,

        -- Most common payment method
        max(payment_method)                             as most_recent_payment_method,

        -- Timestamps
        min(order_ts)                                   as first_order_ts,
        max(order_ts)                                   as last_order_ts,
        datediff('day', min(order_ts), max(order_ts))   as customer_lifespan_days,

        -- Order frequency (orders per month active)
        case
            when datediff('day', min(order_ts), max(order_ts)) > 30
            then round(
                count(distinct order_id)::decimal
                / (datediff('day', min(order_ts), max(order_ts)) / 30.0),
                2
            )
            else count(distinct order_id)
        end                                             as orders_per_month,

        -- Cancellation rate
        round(
            count(distinct case when order_status = 'cancelled' then order_id end)::decimal
            / nullif(count(distinct order_id), 0) * 100,
            2
        )                                               as cancellation_rate_pct,

        -- Refund rate
        round(
            count(distinct case when order_status = 'refunded' then order_id end)::decimal
            / nullif(count(distinct order_id), 0) * 100,
            2
        )                                               as refund_rate_pct,

        -- Last 30 / 90 days
        sum(case
            when order_ts >= dateadd('day', -30, current_timestamp) then net_amount
        end)                                            as revenue_last_30d,
        sum(case
            when order_ts >= dateadd('day', -90, current_timestamp) then net_amount
        end)                                            as revenue_last_90d,
        count(case
            when order_ts >= dateadd('day', -30, current_timestamp) then 1
        end)                                            as orders_last_30d,

        current_timestamp                               as updated_at

    from orders
    group by customer_id
)

select * from customer_metrics
