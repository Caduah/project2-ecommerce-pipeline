-- snowflake/queries/02_tables_and_pipes.sql
-- Creates gold tables and Snowpipe for auto-ingestion from S3.

USE DATABASE PROJECT2_DW;
USE WAREHOUSE PROJECT2_WH;

-- ── Gold tables (mirror of Redshift gold marts) ───────────────────

CREATE TABLE IF NOT EXISTS GOLD.customer_360 (
    customer_id              VARCHAR(64)    NOT NULL,
    resolved_entity_id       VARCHAR(64),
    customer_name            VARCHAR(256),
    email                    VARCHAR(256),
    country                  VARCHAR(4),
    city                     VARCHAR(128),
    age_band                 VARCHAR(8),
    segment                  VARCHAR(32),
    loyalty_tier             VARCHAR(16),
    is_active                BOOLEAN,
    registration_ts          TIMESTAMP_TZ,
    customer_tenure_band     VARCHAR(16),
    total_orders             NUMBER(10,0),
    delivered_orders         NUMBER(10,0),
    total_order_revenue      NUMBER(18,2),
    avg_order_value          NUMBER(18,2),
    orders_per_month         NUMBER(8,2),
    revenue_last_30d         NUMBER(18,2),
    revenue_last_90d         NUMBER(18,2),
    total_txn_count          NUMBER(10,0),
    net_txn_revenue          NUMBER(18,2),
    avg_txn_amount           NUMBER(18,2),
    estimated_ltv            NUMBER(18,2),
    ltv_tier                 VARCHAR(16),
    churn_status             VARCHAR(16),
    total_risk_score         NUMBER(8,0),
    high_risk_txn_count      NUMBER(8,0),
    ever_velocity_spike      BOOLEAN,
    ever_amount_anomaly      BOOLEAN,
    mart_updated_at          TIMESTAMP_TZ,
    _loaded_at               TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (country, ltv_tier)
COMMENT = 'Customer 360 view synced from Redshift gold mart';

CREATE TABLE IF NOT EXISTS GOLD.daily_order_summary (
    order_date               DATE          NOT NULL,
    order_year               NUMBER(4,0),
    order_month              NUMBER(2,0),
    is_weekend               BOOLEAN,
    shipping_country         VARCHAR(4),
    currency                 VARCHAR(8),
    order_status             VARCHAR(32),
    order_count              NUMBER(10,0),
    unique_customers         NUMBER(10,0),
    gross_revenue            NUMBER(18,2),
    total_discounts          NUMBER(18,2),
    net_revenue              NUMBER(18,2),
    avg_order_value          NUMBER(18,2),
    new_customers            NUMBER(10,0),
    returning_customers      NUMBER(10,0),
    mart_updated_at          TIMESTAMP_TZ,
    _loaded_at               TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (order_date, shipping_country)
COMMENT = 'Daily order KPIs synced from Redshift';

CREATE TABLE IF NOT EXISTS GOLD.fraud_summary (
    txn_date                 DATE          NOT NULL,
    txn_year                 NUMBER(4,0),
    txn_month                NUMBER(2,0),
    risk_tier                VARCHAR(8),
    merchant_category        VARCHAR(64),
    is_international         BOOLEAN,
    device_type              VARCHAR(32),
    txn_count                NUMBER(10,0),
    unique_customers         NUMBER(10,0),
    total_amount             NUMBER(18,2),
    flagged_txn_count        NUMBER(10,0),
    high_risk_count          NUMBER(10,0),
    flag_rate_pct            NUMBER(8,2),
    high_risk_rate_pct       NUMBER(8,2),
    amount_at_risk           NUMBER(18,2),
    avg_velocity_1h          NUMBER(8,2),
    mart_updated_at          TIMESTAMP_TZ,
    _loaded_at               TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (txn_date, risk_tier)
COMMENT = 'Daily fraud signal summary — shared with risk analytics team';

-- ── Snowpipe — auto-ingest when new files land in S3 gold zone ────
-- Snowpipe listens to S3 event notifications and loads automatically.

CREATE PIPE IF NOT EXISTS PROJECT2_DW.RAW.pipe_customer_360
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-loads customer_360 parquet from S3 gold zone'
    AS
    COPY INTO GOLD.customer_360 (
        customer_id, resolved_entity_id, customer_name, email,
        country, city, age_band, segment, loyalty_tier, is_active,
        registration_ts, customer_tenure_band,
        total_orders, delivered_orders, total_order_revenue, avg_order_value,
        orders_per_month, revenue_last_30d, revenue_last_90d,
        total_txn_count, net_txn_revenue, avg_txn_amount,
        estimated_ltv, ltv_tier, churn_status,
        total_risk_score, high_risk_txn_count,
        ever_velocity_spike, ever_amount_anomaly, mart_updated_at
    )
    FROM (
        SELECT
            $1:customer_id::VARCHAR,
            $1:resolved_entity_id::VARCHAR,
            $1:customer_name::VARCHAR,
            $1:email::VARCHAR,
            $1:country::VARCHAR,
            $1:city::VARCHAR,
            $1:age_band::VARCHAR,
            $1:segment::VARCHAR,
            $1:loyalty_tier::VARCHAR,
            $1:is_active::BOOLEAN,
            $1:registration_ts::TIMESTAMP_TZ,
            $1:customer_tenure_band::VARCHAR,
            $1:total_orders::NUMBER,
            $1:delivered_orders::NUMBER,
            $1:total_order_revenue::NUMBER,
            $1:avg_order_value::NUMBER,
            $1:orders_per_month::NUMBER,
            $1:revenue_last_30d::NUMBER,
            $1:revenue_last_90d::NUMBER,
            $1:total_txn_count::NUMBER,
            $1:net_txn_revenue::NUMBER,
            $1:avg_txn_amount::NUMBER,
            $1:estimated_ltv::NUMBER,
            $1:ltv_tier::VARCHAR,
            $1:churn_status::VARCHAR,
            $1:total_risk_score::NUMBER,
            $1:high_risk_txn_count::NUMBER,
            $1:ever_velocity_spike::BOOLEAN,
            $1:ever_amount_anomaly::BOOLEAN,
            $1:mart_updated_at::TIMESTAMP_TZ
        FROM @PROJECT2_DW.RAW.s3_gold_stage/ecommerce/customer_360/
    )
    FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

-- After creating pipes, run this to get the SQS ARN
-- and add it to your S3 bucket event notifications:
SHOW PIPES;
-- Copy notification_channel value and add it in AWS S3 console:
-- S3 bucket → Properties → Event notifications → Add notification
-- Event type: s3:ObjectCreated:*  |  Prefix: gold/  |  Destination: SQS
