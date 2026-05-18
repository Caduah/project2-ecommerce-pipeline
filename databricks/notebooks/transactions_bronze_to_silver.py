# Databricks notebook source
# transactions_bronze_to_silver.py
#
# Reads raw financial transaction events from S3 bronze zone.
# Applies validation, anomaly flagging, and enrichment before
# writing to silver as Delta.
#
# Key difference from orders: transactions are immutable financial
# records. We never overwrite — we append with a change flag.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, LongType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
dbutils.widgets.text("s3_bucket", "project2-data-lake-dev", "S3 Bucket")
dbutils.widgets.text("lookback_days", "7", "Lookback days for velocity checks")

EXECUTION_DATE = dbutils.widgets.get("execution_date")
S3_BUCKET      = dbutils.widgets.get("s3_bucket")
LOOKBACK_DAYS  = int(dbutils.widgets.get("lookback_days"))

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/financial/transactions/date={EXECUTION_DATE}/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/financial/transactions/"
BAD_PATH    = f"s3://{S3_BUCKET}/staging/quarantine/transactions/date={EXECUTION_DATE}/"

print(f"Execution date : {EXECUTION_DATE}")
print(f"Bronze path    : {BRONZE_PATH}")

# COMMAND ----------
# ── Schema ────────────────────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  False),
    StructField("order_id",         StringType(),  True),
    StructField("customer_id",      StringType(),  False),
    StructField("merchant_id",      StringType(),  False),
    StructField("transaction_ts",   StringType(),  True),
    StructField("amount",           StringType(),  True),
    StructField("currency",         StringType(),  True),
    StructField("transaction_type", StringType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("card_bin",         StringType(),  True),   # first 6 digits
    StructField("card_last4",       StringType(),  True),
    StructField("ip_country",       StringType(),  True),
    StructField("device_type",      StringType(),  True),
    StructField("merchant_category",StringType(),  True),
    StructField("is_international", StringType(),  True),
    StructField("source_system",    StringType(),  True),
    StructField("ingest_ts",        StringType(),  True),
])

VALID_TYPES    = {"purchase", "refund", "chargeback", "adjustment", "fee", "transfer"}
VALID_STATUSES = {"pending", "completed", "failed", "reversed", "disputed"}
HIGH_VALUE_THRESHOLD = 5000.0   # flag transactions above this for review
VELOCITY_WINDOW_HOURS = 1       # max transactions per customer per hour

# COMMAND ----------
# ── Read bronze ───────────────────────────────────────────────────
print("Reading bronze transactions...")

df_raw = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .option("badRecordsPath", BAD_PATH)
    .option("mode", "PERMISSIVE")
    .parquet(BRONZE_PATH)
)

raw_count = df_raw.count()
print(f"Raw count: {raw_count:,}")

if raw_count == 0:
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# ── Step 1: Type casting ──────────────────────────────────────────
print("Step 1: Casting types...")

df_typed = (
    df_raw
    .withColumn("transaction_ts",
        F.coalesce(
            F.to_timestamp("transaction_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp("transaction_ts", "yyyy-MM-dd HH:mm:ss"),
        )
    )
    .withColumn("ingest_ts",         F.to_timestamp("ingest_ts"))
    .withColumn("amount",            F.col("amount").cast(DoubleType()))
    .withColumn("is_international",  F.col("is_international").cast(BooleanType()))
    .withColumn("transaction_type",  F.lower(F.trim("transaction_type")))
    .withColumn("status",            F.lower(F.trim("status")))
    .withColumn("currency",          F.upper(F.trim("currency")))
    .withColumn("payment_method",    F.lower(F.trim("payment_method")))
    .withColumn("merchant_category", F.lower(F.trim("merchant_category")))
    .withColumn("device_type",       F.lower(F.trim("device_type")))
    .withColumn("ip_country",        F.upper(F.trim("ip_country")))
)

# COMMAND ----------
# ── Step 2: Validation ────────────────────────────────────────────
print("Step 2: Validating records...")

df_validated = (
    df_typed
    .withColumn("_err_null_txn_id",
        F.col("transaction_id").isNull()
    )
    .withColumn("_err_null_customer",
        F.col("customer_id").isNull()
    )
    .withColumn("_err_null_merchant",
        F.col("merchant_id").isNull()
    )
    .withColumn("_err_null_ts",
        F.col("transaction_ts").isNull()
    )
    .withColumn("_err_null_amount",
        F.col("amount").isNull()
    )
    .withColumn("_err_negative_amount",
        (F.col("amount") < 0) & (F.col("transaction_type") == "purchase")
    )
    .withColumn("_err_invalid_type",
        ~F.col("transaction_type").isin(list(VALID_TYPES))
    )
    .withColumn("_err_invalid_status",
        ~F.col("status").isin(list(VALID_STATUSES))
    )
    .withColumn("_err_future_ts",
        F.col("transaction_ts") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR")
    )
)

error_cols = [c for c in df_validated.columns if c.startswith("_err_")]
any_error  = F.lit(False)
for ec in error_cols:
    any_error = any_error | F.col(ec)
df_validated = df_validated.withColumn("_is_valid", ~any_error)

df_clean     = df_validated.filter("_is_valid = true")
df_quarantine= df_validated.filter("_is_valid = false")

print(f"  Valid: {df_clean.count():,}  |  Quarantine: {df_quarantine.count():,}")

if df_quarantine.count() > 0 and not DRY_RUN:
    df_quarantine.write.mode("overwrite").parquet(BAD_PATH)

# COMMAND ----------
# ── Step 3: Anomaly / fraud signal flagging ───────────────────────
# These are signals, not labels. The fraud model (Phase 6) uses them.
print("Step 3: Computing fraud signals...")

# 3a. Velocity — how many txns has this customer done in last hour?
velocity_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("transaction_ts").cast("long"))
    .rangeBetween(-(VELOCITY_WINDOW_HOURS * 3600), 0)
)

# 3b. Customer spend baseline (from silver history — last 30 days)
lookback_start = F.date_sub(F.lit(EXECUTION_DATE).cast("date"), 30)

try:
    df_history = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("transaction_ts") >= lookback_start)
        .groupBy("customer_id")
        .agg(
            F.avg("amount").alias("avg_amount_30d"),
            F.stddev("amount").alias("stddev_amount_30d"),
            F.count("*").alias("txn_count_30d"),
        )
    )
    df_with_history = df_clean.join(df_history, on="customer_id", how="left")
except Exception:
    # First run — no history yet
    df_with_history = (
        df_clean
        .withColumn("avg_amount_30d",    F.lit(None).cast(DoubleType()))
        .withColumn("stddev_amount_30d", F.lit(None).cast(DoubleType()))
        .withColumn("txn_count_30d",     F.lit(0).cast(IntegerType()))
    )

df_flagged = (
    df_with_history
    # Velocity in current batch
    .withColumn("txn_velocity_1h",
        F.count("transaction_id").over(velocity_window)
    )
    # High-value flag
    .withColumn("flag_high_value",
        F.col("amount") > HIGH_VALUE_THRESHOLD
    )
    # Velocity spike
    .withColumn("flag_velocity_spike",
        F.col("txn_velocity_1h") > 5
    )
    # Amount anomaly (> 3 std devs from customer baseline)
    .withColumn("flag_amount_anomaly",
        F.when(
            F.col("stddev_amount_30d").isNotNull() & (F.col("stddev_amount_30d") > 0),
            F.abs(F.col("amount") - F.col("avg_amount_30d")) > (3 * F.col("stddev_amount_30d"))
        ).otherwise(F.lit(False))
    )
    # International + high value
    .withColumn("flag_intl_high_value",
        F.col("is_international") & (F.col("amount") > 1000)
    )
    # New customer (< 5 historical txns) + high value
    .withColumn("flag_new_customer_high_value",
        (F.col("txn_count_30d") < 5) & (F.col("amount") > 500)
    )
    # Composite risk score (simple additive — upgraded in Phase 6)
    .withColumn("risk_score",
        (F.col("flag_high_value").cast(IntegerType()) * 2)
        + (F.col("flag_velocity_spike").cast(IntegerType()) * 3)
        + (F.col("flag_amount_anomaly").cast(IntegerType()) * 3)
        + (F.col("flag_intl_high_value").cast(IntegerType()) * 2)
        + (F.col("flag_new_customer_high_value").cast(IntegerType()) * 1)
    )
    .withColumn("risk_tier",
        F.when(F.col("risk_score") >= 6, "HIGH")
        .when(F.col("risk_score") >= 3, "MEDIUM")
        .otherwise("LOW")
    )
)

# COMMAND ----------
# ── Step 4: Deduplication ─────────────────────────────────────────
print("Step 4: Deduplicating transactions...")

dedup_window = (
    Window
    .partitionBy("transaction_id")
    .orderBy(F.col("ingest_ts").desc_nulls_last())
)

df_deduped = (
    df_flagged
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_is_valid", *error_cols)
)

# COMMAND ----------
# ── Step 5: Derived date columns + silver schema ──────────────────
print("Step 5: Finalising silver schema...")

SILVER_COLS = [
    "transaction_id", "order_id", "customer_id", "merchant_id",
    "transaction_ts", "transaction_type", "status",
    "amount", "currency", "payment_method",
    "card_bin", "card_last4",
    "is_international", "ip_country", "device_type",
    "merchant_category",
    "txn_velocity_1h", "avg_amount_30d", "stddev_amount_30d", "txn_count_30d",
    "flag_high_value", "flag_velocity_spike", "flag_amount_anomaly",
    "flag_intl_high_value", "flag_new_customer_high_value",
    "risk_score", "risk_tier",
    "source_system", "ingest_ts",
]

df_silver = (
    df_deduped
    .select(*SILVER_COLS)
    .withColumn("txn_date",          F.to_date("transaction_ts"))
    .withColumn("txn_year",          F.year("transaction_ts"))
    .withColumn("txn_month",         F.month("transaction_ts"))
    .withColumn("pipeline_version",  F.lit("2.0"))
    .withColumn("silver_ts",         F.current_timestamp())
)

# COMMAND ----------
# ── Step 6: Write to Delta (append — txns are immutable) ──────────
print("Step 6: Writing to silver Delta...")

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (silver_table.alias("existing")
     .merge(
         df_silver.alias("new"),
         "existing.transaction_id = new.transaction_id"
     )
     .whenNotMatchedInsertAll()   # insert only — never overwrite a txn
     .execute()
    )
else:
    (df_silver.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("txn_year", "txn_month")
     .option("overwriteSchema", "true")
     .save(SILVER_PATH)
    )

print("Write complete.")

# COMMAND ----------
# ── Step 7: Optimize ──────────────────────────────────────────────
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    ZORDER BY (customer_id, transaction_ts)
""")

# COMMAND ----------
# ── Step 8: Metrics ───────────────────────────────────────────────
high_risk_count = df_silver.filter(F.col("risk_tier") == "HIGH").count()
flagged_count   = df_silver.filter(F.col("risk_score") > 0).count()

metrics = {
    "execution_date":   EXECUTION_DATE,
    "raw_count":        raw_count,
    "silver_count":     df_deduped.count(),
    "quarantine_count": df_quarantine.count(),
    "high_risk_count":  high_risk_count,
    "flagged_count":    flagged_count,
    "flag_rate_pct":    round(flagged_count / raw_count * 100, 2) if raw_count > 0 else 0,
}

print("\n── Pipeline metrics ─────────────────────────────────────")
for k, v in metrics.items():
    print(f"  {k:<25}: {v}")

dbutils.notebook.exit(str(metrics))
