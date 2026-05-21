-- infrastructure/scripts/redshift_schema.sql
-- Project 2 — Redshift star schema
-- Run once to bootstrap the warehouse.
-- Execute as: psql $REDSHIFT_URL -f redshift_schema.sql

-- ── Schemas ───────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;   -- raw copies from S3 via COPY
CREATE SCHEMA IF NOT EXISTS warehouse; -- dimension + fact tables
CREATE SCHEMA IF NOT EXISTS gold;      -- dbt-built mart tables (final)

-- ──────────────────────────────────────────────────────────────────
-- STAGING TABLES
-- Exact mirror of silver Parquet files — loaded via Redshift COPY.
-- dbt staging models read from here.
-- ──────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS staging.orders CASCADE;
CREATE TABLE staging.orders (
    order_id          VARCHAR(64)    NOT NULL,
    customer_id       VARCHAR(64)    NOT NULL,
    merchant_id       VARCHAR(64),
    order_status      VARCHAR(32),
    order_ts          TIMESTAMP,
    updated_ts        TIMESTAMP,
    order_date        DATE,
    order_year        SMALLINT,
    order_month       SMALLINT,
    order_dow         SMALLINT,
    is_weekend        BOOLEAN,
    currency          VARCHAR(8),
    gross_amount      DECIMAL(18,2),
    discount_amount   DECIMAL(18,2),
    discount_pct      DECIMAL(8,4),
    net_amount        DECIMAL(18,2),
    item_count        SMALLINT,
    payment_method    VARCHAR(64),
    shipping_country  VARCHAR(4),
    source_system     VARCHAR(32),
    ingest_ts         TIMESTAMP,
    silver_ts         TIMESTAMP,
    pipeline_version  VARCHAR(8)
)
DISTKEY(customer_id)
SORTKEY(order_ts, customer_id);

DROP TABLE IF EXISTS staging.transactions CASCADE;
CREATE TABLE staging.transactions (
    transaction_id              VARCHAR(64)   NOT NULL,
    order_id                    VARCHAR(64),
    customer_id                 VARCHAR(64)   NOT NULL,
    merchant_id                 VARCHAR(64)   NOT NULL,
    transaction_ts              TIMESTAMP,
    transaction_type            VARCHAR(32),
    status                      VARCHAR(32),
    amount                      DECIMAL(18,2),
    currency                    VARCHAR(8),
    payment_method              VARCHAR(64),
    card_bin                    VARCHAR(8),
    card_last4                  VARCHAR(4),
    is_international            BOOLEAN,
    ip_country                  VARCHAR(4),
    device_type                 VARCHAR(32),
    merchant_category           VARCHAR(64),
    txn_velocity_1h             SMALLINT,
    avg_amount_30d              DECIMAL(18,2),
    stddev_amount_30d           DECIMAL(18,2),
    txn_count_30d               INTEGER,
    flag_high_value             BOOLEAN,
    flag_velocity_spike         BOOLEAN,
    flag_amount_anomaly         BOOLEAN,
    flag_intl_high_value        BOOLEAN,
    flag_new_customer_high_value BOOLEAN,
    risk_score                  SMALLINT,
    risk_tier                   VARCHAR(8),
    source_system               VARCHAR(32),
    ingest_ts                   TIMESTAMP,
    txn_date                    DATE,
    txn_year                    SMALLINT,
    txn_month                   SMALLINT,
    silver_ts                   TIMESTAMP,
    pipeline_version            VARCHAR(8)
)
DISTKEY(customer_id)
SORTKEY(transaction_ts, customer_id);

DROP TABLE IF EXISTS staging.customers CASCADE;
CREATE TABLE staging.customers (
    customer_id           VARCHAR(64)  NOT NULL,
    source_system         VARCHAR(32),
    first_name            VARCHAR(128),
    last_name             VARCHAR(128),
    email                 VARCHAR(256),
    phone                 VARCHAR(32),
    email_normalised      VARCHAR(256),
    phone_normalised      VARCHAR(16),
    full_name_norm        VARCHAR(256),
    last_name_soundex     VARCHAR(8),
    city                  VARCHAR(128),
    state_province        VARCHAR(64),
    postal_code           VARCHAR(16),
    country               VARCHAR(4),
    age                   SMALLINT,
    age_band              VARCHAR(8),
    days_since_registration INTEGER,
    customer_tenure_band  VARCHAR(16),
    segment               VARCHAR(32),
    loyalty_tier          VARCHAR(16),
    is_active             BOOLEAN,
    registration_ts       TIMESTAMP,
    resolved_entity_id    VARCHAR(64),
    er_confidence         DECIMAL(6,4),
    silver_ts             TIMESTAMP,
    pipeline_version      VARCHAR(8)
)
DISTKEY(customer_id)
SORTKEY(customer_id);

-- ──────────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ──────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS warehouse.dim_customer CASCADE;
CREATE TABLE warehouse.dim_customer (
    customer_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id           VARCHAR(64)   NOT NULL,
    resolved_entity_id    VARCHAR(64),
    full_name             VARCHAR(256),
    email_normalised      VARCHAR(256),
    phone_normalised      VARCHAR(16),
    country               VARCHAR(4),
    city                  VARCHAR(128),
    state_province        VARCHAR(64),
    postal_code           VARCHAR(16),
    age_band              VARCHAR(8),
    customer_tenure_band  VARCHAR(16),
    segment               VARCHAR(32),
    loyalty_tier          VARCHAR(16),
    is_active             BOOLEAN,
    registration_ts       TIMESTAMP,
    -- SCD Type 2 fields
    valid_from            TIMESTAMP     NOT NULL,
    valid_to              TIMESTAMP,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE,
    record_hash           VARCHAR(64)   -- SHA256 of mutable fields
)
DISTKEY(customer_id)
SORTKEY(customer_id, valid_from);

DROP TABLE IF EXISTS warehouse.dim_merchant CASCADE;
CREATE TABLE warehouse.dim_merchant (
    merchant_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    merchant_id           VARCHAR(64)   NOT NULL,
    merchant_name         VARCHAR(256),
    merchant_category     VARCHAR(64),
    country               VARCHAR(4),
    city                  VARCHAR(128),
    is_active             BOOLEAN,
    risk_tier             VARCHAR(8),
    valid_from            TIMESTAMP     NOT NULL,
    valid_to              TIMESTAMP,
    is_current            BOOLEAN       NOT NULL DEFAULT TRUE
)
DISTSTYLE ALL    -- small dimension, broadcast to all nodes
SORTKEY(merchant_id);

DROP TABLE IF EXISTS warehouse.dim_date CASCADE;
CREATE TABLE warehouse.dim_date (
    date_sk               INTEGER       NOT NULL PRIMARY KEY,  -- YYYYMMDD
    full_date             DATE          NOT NULL,
    year                  SMALLINT,
    quarter               SMALLINT,
    month                 SMALLINT,
    month_name            VARCHAR(16),
    week_of_year          SMALLINT,
    day_of_month          SMALLINT,
    day_of_week           SMALLINT,     -- 1=Mon … 7=Sun
    day_name              VARCHAR(16),
    is_weekend            BOOLEAN,
    is_holiday            BOOLEAN       DEFAULT FALSE,
    fiscal_year           SMALLINT,
    fiscal_quarter        SMALLINT
)
DISTSTYLE ALL
SORTKEY(full_date);

DROP TABLE IF EXISTS warehouse.dim_payment_method CASCADE;
CREATE TABLE warehouse.dim_payment_method (
    payment_method_sk     INTEGER       NOT NULL PRIMARY KEY,
    payment_method_code   VARCHAR(64)   NOT NULL,
    payment_method_name   VARCHAR(128),
    payment_category      VARCHAR(64),  -- card / wallet / bank / bnpl / crypto
    is_digital            BOOLEAN
)
DISTSTYLE ALL;

-- ──────────────────────────────────────────────────────────────────
-- FACT TABLES
-- ──────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS warehouse.fact_orders CASCADE;
CREATE TABLE warehouse.fact_orders (
    order_sk              BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id              VARCHAR(64)   NOT NULL,
    customer_sk           BIGINT        REFERENCES warehouse.dim_customer(customer_sk),
    merchant_sk           BIGINT        REFERENCES warehouse.dim_merchant(merchant_sk),
    order_date_sk         INTEGER       REFERENCES warehouse.dim_date(date_sk),
    payment_method_sk     INTEGER       REFERENCES warehouse.dim_payment_method(payment_method_sk),
    -- Measures
    order_status          VARCHAR(32),
    gross_amount          DECIMAL(18,2),
    discount_amount       DECIMAL(18,2),
    discount_pct          DECIMAL(8,4),
    net_amount            DECIMAL(18,2),
    item_count            SMALLINT,
    currency              VARCHAR(8),
    shipping_country      VARCHAR(4),
    -- Audit
    order_ts              TIMESTAMP,
    pipeline_version      VARCHAR(8),
    loaded_at             TIMESTAMP     DEFAULT GETDATE()
)
DISTKEY(customer_sk)
SORTKEY(order_date_sk, customer_sk);

DROP TABLE IF EXISTS warehouse.fact_transactions CASCADE;
CREATE TABLE warehouse.fact_transactions (
    transaction_sk              BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id              VARCHAR(64)   NOT NULL,
    order_id                    VARCHAR(64),
    customer_sk                 BIGINT        REFERENCES warehouse.dim_customer(customer_sk),
    merchant_sk                 BIGINT        REFERENCES warehouse.dim_merchant(merchant_sk),
    txn_date_sk                 INTEGER       REFERENCES warehouse.dim_date(date_sk),
    payment_method_sk           INTEGER       REFERENCES warehouse.dim_payment_method(payment_method_sk),
    -- Measures
    transaction_type            VARCHAR(32),
    status                      VARCHAR(32),
    amount                      DECIMAL(18,2),
    currency                    VARCHAR(8),
    is_international            BOOLEAN,
    ip_country                  VARCHAR(4),
    device_type                 VARCHAR(32),
    -- Fraud signals
    txn_velocity_1h             SMALLINT,
    risk_score                  SMALLINT,
    risk_tier                   VARCHAR(8),
    flag_high_value             BOOLEAN,
    flag_velocity_spike         BOOLEAN,
    flag_amount_anomaly         BOOLEAN,
    -- Audit
    transaction_ts              TIMESTAMP,
    pipeline_version            VARCHAR(8),
    loaded_at                   TIMESTAMP     DEFAULT GETDATE()
)
DISTKEY(customer_sk)
SORTKEY(txn_date_sk, customer_sk);

-- ──────────────────────────────────────────────────────────────────
-- DATE DIMENSION SEED
-- Populate 10 years: 2020-01-01 → 2029-12-31
-- ──────────────────────────────────────────────────────────────────
INSERT INTO warehouse.dim_date
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)  AS date_sk,
    d::DATE                                   AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT         AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT         AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT         AS month,
    TO_CHAR(d, 'Month')                       AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT         AS week_of_year,
    EXTRACT(DAY     FROM d)::SMALLINT         AS day_of_month,
    EXTRACT(DOW     FROM d)::SMALLINT + 1     AS day_of_week,
    TO_CHAR(d, 'Day')                         AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE                                     AS is_holiday,
    CASE WHEN EXTRACT(MONTH FROM d) >= 4
         THEN EXTRACT(YEAR FROM d)::SMALLINT
         ELSE EXTRACT(YEAR FROM d)::SMALLINT - 1
    END                                       AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (4,5,6)   THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9)   THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 3
        ELSE 4
    END::SMALLINT                             AS fiscal_quarter
FROM (
    SELECT DATEADD(DAY, n, '2020-01-01'::DATE) AS d
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS n
        FROM stl_scan LIMIT 3653   -- 10 years
    )
) dates;

-- ──────────────────────────────────────────────────────────────────
-- PAYMENT METHOD SEED
-- ──────────────────────────────────────────────────────────────────
INSERT INTO warehouse.dim_payment_method VALUES
(1,  'credit_card',       'Credit Card',          'card',   TRUE),
(2,  'debit_card',        'Debit Card',           'card',   TRUE),
(3,  'paypal',            'PayPal',               'wallet', TRUE),
(4,  'bank_transfer',     'Bank Transfer',        'bank',   TRUE),
(5,  'crypto',            'Cryptocurrency',       'crypto', TRUE),
(6,  'buy_now_pay_later', 'Buy Now Pay Later',    'bnpl',   TRUE),
(7,  'apple_pay',         'Apple Pay',            'wallet', TRUE),
(8,  'google_pay',        'Google Pay',           'wallet', TRUE),
(9,  'cash_on_delivery',  'Cash on Delivery',     'cash',   FALSE),
(99, 'other',             'Other / Unknown',      'other',  FALSE);

COMMIT;
