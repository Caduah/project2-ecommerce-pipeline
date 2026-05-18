# Databricks notebook source
# orders_bronze_to_silver.py
#
# Reads raw order events from S3 bronze zone, applies cleaning,
# validation, deduplication and writes to S3 silver zone as Delta.
#
# Run via Airflow DatabricksRunNowOperator with params:
#   {"source_table": "orders", "execution_date": "2025-01-15"}

# COMMAND ----------
# %pip install great-expectations==0.18.0
# Uncomment above on first run in a new cluster

# COMMAND ----------
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType
)
from delta.tables import DeltaTable

# ── Widgets (Databricks job parameters) ──────────────────────────
dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
dbutils.widgets.text("s3_bucket", "project2-data-lake-dev", "S3 Bucket")
dbutils.widgets.text("dry_run", "false", "Dry Run")

EXECUTION_DATE = dbutils.widgets.get("execution_date")
S3_BUCKET      = dbutils.widgets.get("s3_bucket")
DRY_RUN        = dbutils.widgets.get("dry_run").lower() == "true"

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/ecommerce/orders/date={EXECUTION_DATE}/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/ecommerce/orders/"
CHECKPOINT  = f"s3://{S3_BUCKET}/staging/databricks_checkpoints/orders_silver/"
BAD_PATH    = f"s3://{S3_BUCKET}/staging/quarantine/orders/date={EXECUTION_DATE}/"

print(f"Execution date : {EXECUTION_DATE}")
print(f"Bronze path    : {BRONZE_PATH}")
print(f"Silver path    : {SILVER_PATH}")
print(f"Dry run        : {DRY_RUN}")

# COMMAND ----------
# ── Schema definition ─────────────────────────────────────────────
# Define explicitly — never infer schema from raw data in production.
# If source sends a new column it lands in bronze; we promote it
# deliberately when we're ready.

BRONZE_SCHEMA = StructType([
    StructField("order_id",        StringType(),    nullable=False),
    StructField("customer_id",     StringType(),    nullable=False),
    StructField("merchant_id",     StringType(),    nullable=True),
    StructField("order_status",    StringType(),    nullable=True),
    StructField("order_ts",        StringType(),    nullable=True),  # raw string
    StructField("updated_ts",      StringType(),    nullable=True),
    StructField("currency",        StringType(),    nullable=True),
    StructField("gross_amount",    StringType(),    nullable=True),  # raw string
    StructField("discount_amount", StringType(),    nullable=True),
    StructField("item_count",      StringType(),    nullable=True),
    StructField("payment_method",  StringType(),    nullable=True),
    StructField("shipping_country",StringType(),    nullable=True),
    StructField("source_system",   StringType(),    nullable=True),
    StructField("ingest_ts",       StringType(),    nullable=True),
])

VALID_STATUSES  = {"pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"}
VALID_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY", "NGN", "GHS"}
VALID_PAYMENTS  = {"credit_card", "debit_card", "paypal", "bank_transfer",
                   "crypto", "buy_now_pay_later", "apple_pay", "google_pay"}

# COMMAND ----------
# ── Read bronze ───────────────────────────────────────────────────
print("Reading bronze data...")

df_raw = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .option("badRecordsPath", BAD_PATH)   # malformed rows go here, don't crash
    .option("mode", "PERMISSIVE")
    .parquet(BRONZE_PATH)
)

raw_count = df_raw.count()
print(f"Raw record count: {raw_count:,}")

if raw_count == 0:
    print("No data found for this date. Exiting.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------
# ── Step 1: Type casting & timestamp parsing ──────────────────────
print("Step 1: Casting types...")

df_typed = (
    df_raw
    .withColumn("order_ts",
        F.coalesce(
            F.to_timestamp("order_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("order_ts", "yyyy-MM-dd"),
        )
    )
    .withColumn("updated_ts",
        F.coalesce(
            F.to_timestamp("updated_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp("updated_ts", "yyyy-MM-dd HH:mm:ss"),
        )
    )
    .withColumn("ingest_ts",      F.to_timestamp("ingest_ts"))
    .withColumn("gross_amount",   F.col("gross_amount").cast(DoubleType()))
    .withColumn("discount_amount",F.col("discount_amount").cast(DoubleType()))
    .withColumn("item_count",     F.col("item_count").cast(IntegerType()))
    # Normalise string fields
    .withColumn("order_status",    F.lower(F.trim("order_status")))
    .withColumn("currency",        F.upper(F.trim("currency")))
    .withColumn("payment_method",  F.lower(F.trim(F.regexp_replace("payment_method", " ", "_"))))
    .withColumn("shipping_country",F.upper(F.trim("shipping_country")))
    .withColumn("source_system",   F.lower(F.trim("source_system")))
)

# COMMAND ----------
# ── Step 2: Derived columns ───────────────────────────────────────
print("Step 2: Adding derived columns...")

df_enriched = (
    df_typed
    .withColumn("net_amount",
        F.round(F.col("gross_amount") - F.coalesce(F.col("discount_amount"), F.lit(0.0)), 2)
    )
    .withColumn("order_date",   F.to_date("order_ts"))
    .withColumn("order_year",   F.year("order_ts"))
    .withColumn("order_month",  F.month("order_ts"))
    .withColumn("order_dow",    F.dayofweek("order_ts"))       # 1=Sun … 7=Sat
    .withColumn("is_weekend",   F.dayofweek("order_ts").isin(1, 7))
    .withColumn("discount_pct",
        F.when(F.col("gross_amount") > 0,
            F.round(F.col("discount_amount") / F.col("gross_amount") * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("pipeline_version", F.lit("2.0"))
    .withColumn("silver_ts",        F.current_timestamp())
)

# COMMAND ----------
# ── Step 3: Validation & quarantine ──────────────────────────────
print("Step 3: Validating records...")

# Build a bitmask of failures — easy to debug in downstream queries
df_validated = (
    df_enriched
    .withColumn("_err_null_order_id",
        F.col("order_id").isNull()
    )
    .withColumn("_err_null_customer_id",
        F.col("customer_id").isNull()
    )
    .withColumn("_err_null_order_ts",
        F.col("order_ts").isNull()
    )
    .withColumn("_err_invalid_status",
        ~F.col("order_status").isin(list(VALID_STATUSES))
    )
    .withColumn("_err_invalid_currency",
        F.col("currency").isNotNull() & ~F.col("currency").isin(list(VALID_CURRENCIES))
    )
    .withColumn("_err_negative_amount",
        F.col("gross_amount") < 0
    )
    .withColumn("_err_future_order",
        F.col("order_ts") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR")
    )
    .withColumn("_err_zero_items",
        F.col("item_count") <= 0
    )
)

# Combine into a single flag
error_cols = [c for c in df_validated.columns if c.startswith("_err_")]
df_validated = df_validated.withColumn(
    "_is_valid",
    ~F.array(*(F.col(c) for c in error_cols)).cast("array<boolean>").getItem(0)
)
# Simpler: valid if no error flag is True
any_error = F.lit(False)
for ec in error_cols:
    any_error = any_error | F.col(ec)
df_validated = df_validated.withColumn("_is_valid", ~any_error)

df_clean     = df_validated.filter(F.col("_is_valid") == True)
df_quarantine= df_validated.filter(F.col("_is_valid") == False)

clean_count     = df_clean.count()
quarantine_count= df_quarantine.count()
print(f"  Valid records    : {clean_count:,}")
print(f"  Quarantine records: {quarantine_count:,}")

if quarantine_count > 0:
    print("  Quarantine breakdown:")
    for ec in error_cols:
        n = df_quarantine.filter(F.col(ec) == True).count()
        if n > 0:
            print(f"    {ec}: {n:,}")

# Write quarantine for investigation
if quarantine_count > 0 and not DRY_RUN:
    (df_quarantine
     .write.mode("overwrite")
     .parquet(BAD_PATH))
    print(f"  Quarantine written to {BAD_PATH}")

# COMMAND ----------
# ── Step 4: Deduplication ─────────────────────────────────────────
print("Step 4: Deduplicating...")

# Keep latest record per order_id (highest updated_ts wins)
window = (
    F.window_spec()
    if False else
    __import__("pyspark.sql.window", fromlist=["Window"])
    .Window.partitionBy("order_id")
    .orderBy(F.col("updated_ts").desc_nulls_last())
)

from pyspark.sql.window import Window

dedup_window = Window.partitionBy("order_id").orderBy(F.col("updated_ts").desc_nulls_last())

df_deduped = (
    df_clean
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_is_valid", *error_cols)
)

dedup_count = df_deduped.count()
dupes_removed = clean_count - dedup_count
print(f"  After dedup: {dedup_count:,} records ({dupes_removed:,} duplicates removed)")

# COMMAND ----------
# ── Step 5: Select final silver columns ───────────────────────────
print("Step 5: Projecting silver schema...")

SILVER_COLS = [
    "order_id", "customer_id", "merchant_id",
    "order_status", "order_ts", "updated_ts", "order_date",
    "order_year", "order_month", "order_dow", "is_weekend",
    "currency", "gross_amount", "discount_amount", "discount_pct",
    "net_amount", "item_count", "payment_method", "shipping_country",
    "source_system", "ingest_ts", "pipeline_version", "silver_ts",
]

df_silver = df_deduped.select(*SILVER_COLS)

# COMMAND ----------
# ── Step 6: Write to Delta Lake ───────────────────────────────────
print("Step 6: Writing to silver Delta table...")

if DRY_RUN:
    print("DRY RUN — skipping write. Sample output:")
    df_silver.show(5, truncate=False)
    dbutils.notebook.exit("DRY_RUN_COMPLETE")

# Upsert into existing Delta table (MERGE) — safe for reruns
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("  Delta table exists — performing MERGE (upsert)...")
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (silver_table.alias("existing")
     .merge(
         df_silver.alias("new"),
         "existing.order_id = new.order_id"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
else:
    print("  First run — creating Delta table...")
    (df_silver.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("order_year", "order_month")
     .option("overwriteSchema", "true")
     .save(SILVER_PATH)
    )

print("Silver write complete.")

# COMMAND ----------
# ── Step 7: Update table stats & optimize ─────────────────────────
print("Step 7: Running OPTIMIZE + ZORDER...")

spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    ZORDER BY (customer_id, order_ts)
""")

spark.sql(f"ANALYZE TABLE delta.`{SILVER_PATH}` COMPUTE STATISTICS")

# COMMAND ----------
# ── Step 8: Metrics summary ───────────────────────────────────────
metrics = {
    "execution_date":    EXECUTION_DATE,
    "raw_count":         raw_count,
    "quarantine_count":  quarantine_count,
    "clean_count":       clean_count,
    "dupes_removed":     dupes_removed,
    "silver_count":      dedup_count,
    "quarantine_rate_pct": round(quarantine_count / raw_count * 100, 2) if raw_count > 0 else 0,
    "dupe_rate_pct":     round(dupes_removed / clean_count * 100, 2) if clean_count > 0 else 0,
}

print("\n── Pipeline metrics ─────────────────────────────────────")
for k, v in metrics.items():
    print(f"  {k:<25}: {v}")

dbutils.notebook.exit(str(metrics))
