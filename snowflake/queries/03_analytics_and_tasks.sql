-- snowflake/queries/03_analytics_and_tasks.sql
-- Analytics views on top of gold tables.
-- These are what the fraud team and BI tools query directly in Snowflake.

USE DATABASE PROJECT2_DW;
USE WAREHOUSE PROJECT2_WH;

-- ── Analytics views ───────────────────────────────────────────────

-- High-value customer segments
CREATE OR REPLACE VIEW ANALYTICS.vw_customer_segments AS
SELECT
    country,
    ltv_tier,
    churn_status,
    segment,
    loyalty_tier,
    COUNT(*)                        AS customer_count,
    AVG(estimated_ltv)              AS avg_ltv,
    SUM(estimated_ltv)              AS total_ltv,
    AVG(total_orders)               AS avg_orders,
    AVG(revenue_last_30d)           AS avg_revenue_30d,
    SUM(CASE WHEN ever_velocity_spike THEN 1 ELSE 0 END) AS risky_customers
FROM GOLD.customer_360
WHERE is_active = TRUE
GROUP BY country, ltv_tier, churn_status, segment, loyalty_tier
ORDER BY total_ltv DESC;

-- Daily revenue trend (last 90 days)
CREATE OR REPLACE VIEW ANALYTICS.vw_revenue_trend AS
SELECT
    order_date,
    shipping_country,
    SUM(net_revenue)                AS net_revenue,
    SUM(order_count)                AS order_count,
    SUM(unique_customers)           AS unique_customers,
    AVG(avg_order_value)            AS avg_order_value,
    SUM(new_customers)              AS new_customers,
    -- 7-day rolling average
    AVG(SUM(net_revenue)) OVER (
        PARTITION BY shipping_country
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                               AS revenue_7d_rolling_avg
FROM GOLD.daily_order_summary
WHERE order_date >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY order_date, shipping_country
ORDER BY order_date DESC;

-- Fraud risk dashboard
CREATE OR REPLACE VIEW ANALYTICS.vw_fraud_dashboard AS
SELECT
    txn_date,
    risk_tier,
    merchant_category,
    SUM(txn_count)                  AS total_txns,
    SUM(flagged_txn_count)          AS total_flagged,
    SUM(high_risk_count)            AS total_high_risk,
    SUM(amount_at_risk)             AS total_amount_at_risk,
    AVG(flag_rate_pct)              AS avg_flag_rate,
    AVG(high_risk_rate_pct)         AS avg_high_risk_rate,
    -- Week over week change
    LAG(SUM(high_risk_count), 7) OVER (
        PARTITION BY risk_tier
        ORDER BY txn_date
    )                               AS high_risk_count_prev_week,
    SUM(high_risk_count) - LAG(SUM(high_risk_count), 7) OVER (
        PARTITION BY risk_tier
        ORDER BY txn_date
    )                               AS high_risk_wow_change
FROM GOLD.fraud_summary
WHERE txn_date >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY txn_date, risk_tier, merchant_category
ORDER BY txn_date DESC, total_high_risk DESC;

-- Cross-cloud summary (the money shot for interviews)
-- Shows data that exists in both Redshift and Snowflake
CREATE OR REPLACE VIEW ANALYTICS.vw_executive_summary AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(net_revenue)                AS monthly_revenue,
    SUM(order_count)                AS monthly_orders,
    SUM(unique_customers)           AS monthly_active_customers,
    SUM(new_customers)              AS monthly_new_customers,
    ROUND(SUM(new_customers) / NULLIF(SUM(unique_customers),0) * 100, 1)
                                    AS new_customer_rate_pct
FROM GOLD.daily_order_summary
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;

-- ── Scheduled tasks (replace Airflow sync for Snowflake-native schedule) ──
-- Task runs daily at 04:00 UTC (after dbt gold run completes at ~03:00)

CREATE OR REPLACE TASK PROJECT2_DW.GOLD.task_refresh_fraud_view
    WAREHOUSE = PROJECT2_WH
    SCHEDULE  = 'USING CRON 0 4 * * * UTC'
    COMMENT   = 'Refreshes fraud dashboard materialized table daily'
AS
    CREATE OR REPLACE TABLE ANALYTICS.fraud_dashboard_snapshot AS
    SELECT *, CURRENT_TIMESTAMP() AS snapshot_ts
    FROM ANALYTICS.vw_fraud_dashboard;

-- Activate the task (tasks start SUSPENDED by default)
ALTER TASK PROJECT2_DW.GOLD.task_refresh_fraud_view RESUME;

-- ── Data sharing (Snowflake's killer feature) ─────────────────────
-- Share gold data with a partner org without copying it

CREATE SHARE IF NOT EXISTS project2_fraud_share
    COMMENT = 'Share fraud summary with risk analytics partner';

GRANT USAGE ON DATABASE PROJECT2_DW TO SHARE project2_fraud_share;
GRANT USAGE ON SCHEMA PROJECT2_DW.ANALYTICS TO SHARE project2_fraud_share;
GRANT SELECT ON TABLE PROJECT2_DW.GOLD.fraud_summary TO SHARE project2_fraud_share;

-- Add a consumer account (replace with real Snowflake account ID)
-- ALTER SHARE project2_fraud_share ADD ACCOUNTS = '<partner_account>';

SHOW SHARES;
