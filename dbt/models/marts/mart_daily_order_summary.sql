-- models/marts/mart_daily_order_summary.sql
-- Daily aggregated order KPIs — feeds the main business dashboard.

{{
    config(
        materialized = 'table',
        sort         = ['order_date', 'shipping_country'],
        tags         = ['gold', 'orders', 'daily']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

daily as (
    select
        order_date,
        order_year,
        order_month,
        is_weekend,
        shipping_country,
        currency,
        order_status,

        -- Volume
        count(distinct order_id)                as order_count,
        count(distinct customer_id)             as unique_customers,

        -- Revenue
        sum(gross_amount)                       as gross_revenue,
        sum(discount_amount)                    as total_discounts,
        sum(net_amount)                         as net_revenue,
        avg(net_amount)                         as avg_order_value,
        max(net_amount)                         as max_order_value,

        -- Items
        sum(item_count)                         as total_items,
        avg(item_count)                         as avg_items_per_order,

        -- Discount metrics
        avg(discount_pct)                       as avg_discount_pct,
        sum(case when discount_amount > 0 then 1 else 0 end) as discounted_orders,

        -- New vs returning (first order per customer)
        count(distinct case
            when order_date = first_order_date then customer_id
        end)                                    as new_customers,
        count(distinct case
            when order_date > first_order_date then customer_id
        end)                                    as returning_customers,

        current_timestamp                       as mart_updated_at

    from orders
    left join (
        -- First order date per customer
        select customer_id, min(order_date) as first_order_date
        from {{ ref('stg_orders') }}
        group by customer_id
    ) fod using (customer_id)

    group by
        order_date, order_year, order_month,
        is_weekend, shipping_country, currency, order_status
)

select * from daily
