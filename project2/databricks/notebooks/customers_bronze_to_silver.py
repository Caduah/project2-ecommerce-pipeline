# Databricks notebook source
# customers_bronze_to_silver.py
#
# Reads raw customer records from S3 bronze, normalises them,
# and prepares blocking keys for the entity resolution step (Phase 6).
# This notebook does NOT resolve entities — it prepares the data so
# the entity resolution job (Databricks ML) can run efficiently.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DateType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import re

# COMMAND ----------
dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
dbutils.widgets.text("s3_bucket", "project2-data-lake-dev", "S3 Bucket")

EXECUTION_DATE = dbutils.widgets.get("execution_date")
S3_BUCKET      = dbutils.widgets.get("s3_bucket")

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/ecommerce/customers/date={EXECUTION_DATE}/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/ecommerce/customers/"
BAD_PATH    = f"s3://{S3_BUCKET}/staging/quarantine/customers/date={EXECUTION_DATE}/"

print(f"Execution date : {EXECUTION_DATE}")
print(f"Bronze path    : {BRONZE_PATH}")

# COMMAND ----------
# ── Schema ────────────────────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("customer_id",     StringType(), False),
    StructField("source_system",   StringType(), True),
    StructField("first_name",      StringType(), True),
    StructField("last_name",       StringType(), True),
    StructField("email",           StringType(), True),
    StructField("phone",           StringType(), True),
    StructField("date_of_birth",   StringType(), True),
    StructField("gender",          StringType(), True),
    StructField("address_line1",   StringType(), True),
    StructField("address_line2",   StringType(), True),
    StructField("city",            StringType(), True),
    StructField("state_province",  StringType(), True),
    StructField("postal_code",     StringType(), True),
    StructField("country",         StringType(), True),
    StructField("registration_ts", StringType(), True),
    StructField("is_active",       StringType(), True),
    StructField("segment",         StringType(), True),
    StructField("loyalty_tier",    StringType(), True),
    StructField("ingest_ts",       StringType(), True),
])

VALID_SEGMENTS      = {"new", "returning", "vip", "at_risk", "churned", "prospect"}
VALID_LOYALTY_TIERS = {"bronze", "silver", "gold", "platinum", "none"}

# COMMAND ----------
# ── Read bronze ───────────────────────────────────────────────────
print("Reading bronze customers...")

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
print("Step 1: Type casting...")

df_typed = (
    df_raw
    .withColumn("registration_ts",
        F.coalesce(
            F.to_timestamp("registration_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp("registration_ts", "yyyy-MM-dd HH:mm:ss"),
        )
    )
    .withColumn("date_of_birth",
        F.coalesce(
            F.to_date("date_of_birth", "yyyy-MM-dd"),
            F.to_date("date_of_birth", "MM/dd/yyyy"),
            F.to_date("date_of_birth", "dd-MM-yyyy"),
        )
    )
    .withColumn("ingest_ts",   F.to_timestamp("ingest_ts"))
    .withColumn("is_active",   F.col("is_active").cast(BooleanType()))
    .withColumn("segment",     F.lower(F.trim("segment")))
    .withColumn("loyalty_tier",F.lower(F.trim("loyalty_tier")))
    .withColumn("gender",      F.lower(F.trim("gender")))
    .withColumn("country",     F.upper(F.trim("country")))
)

# COMMAND ----------
# ── Step 2: PII normalisation ─────────────────────────────────────
# Normalise before entity resolution — matching works on clean keys.
print("Step 2: Normalising PII fields...")

# Email normalisation UDF
@F.udf(StringType())
def normalise_email(email):
    if not email:
        return None
    e = email.strip().lower()
    # Remove Gmail dots trick and plus alias
    if "@gmail.com" in e:
        local, domain = e.split("@", 1)
        local = local.replace(".", "").split("+")[0]
        e = f"{local}@{domain}"
    return e

# Phone normalisation — strip everything except digits, keep last 10
@F.udf(StringType())
def normalise_phone(phone):
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 10:
        return digits[-10:]
    return digits if digits else None

# Name normalisation
@F.udf(StringType())
def normalise_name(name):
    if not name:
        return None
    import unicodedata
    # Remove accents
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase, strip, collapse whitespace
    return " ".join(ascii_name.lower().strip().split())

df_normalised = (
    df_typed
    .withColumn("email_normalised",  normalise_email("email"))
    .withColumn("phone_normalised",  normalise_phone("phone"))
    .withColumn("first_name_norm",   normalise_name("first_name"))
    .withColumn("last_name_norm",    normalise_name("last_name"))
    .withColumn("full_name_norm",
        F.concat_ws(" ", F.col("first_name_norm"), F.col("last_name_norm"))
    )
    .withColumn("postal_code_norm",
        F.regexp_replace(F.upper(F.trim("postal_code")), r"[^A-Z0-9]", "")
    )
    .withColumn("city_norm",
        F.lower(F.regexp_replace(F.trim("city"), r"\s+", "_"))
    )
)

# COMMAND ----------
# ── Step 3: Blocking keys for entity resolution ───────────────────
# Blocking keys reduce the pairwise comparison space from O(n²) to O(n).
# Two records only get compared if they share at least one blocking key.
# We use 3 keys — any one match is enough to trigger comparison.
print("Step 3: Building entity resolution blocking keys...")

df_with_keys = (
    df_normalised
    # Key 1: first 3 chars of last name + postal code
    .withColumn("block_key_1",
        F.when(
            F.col("last_name_norm").isNotNull() & F.col("postal_code_norm").isNotNull(),
            F.concat(
                F.substring("last_name_norm", 1, 3),
                F.lit("_"),
                F.col("postal_code_norm")
            )
        )
    )
    # Key 2: normalised email domain + first 3 chars of last name
    .withColumn("email_domain",
        F.when(
            F.col("email_normalised").isNotNull(),
            F.split("email_normalised", "@").getItem(1)
        )
    )
    .withColumn("block_key_2",
        F.when(
            F.col("email_normalised").isNotNull(),
            F.col("email_normalised")          # exact email is the strongest key
        )
    )
    # Key 3: normalised phone
    .withColumn("block_key_3",
        F.col("phone_normalised")
    )
    # Soundex of last name (catches spelling variants)
    .withColumn("last_name_soundex",
        F.soundex("last_name_norm")
    )
)

# COMMAND ----------
# ── Step 4: Compute age & derived fields ─────────────────────────
print("Step 4: Derived fields...")

df_enriched = (
    df_with_keys
    .withColumn("age",
        F.when(
            F.col("date_of_birth").isNotNull(),
            F.floor(
                F.datediff(F.current_date(), "date_of_birth") / 365.25
            )
        )
    )
    .withColumn("age_band",
        F.when(F.col("age") < 25, "18-24")
        .when(F.col("age") < 35, "25-34")
        .when(F.col("age") < 45, "35-44")
        .when(F.col("age") < 55, "45-54")
        .when(F.col("age") < 65, "55-64")
        .when(F.col("age") >= 65, "65+")
        .otherwise("unknown")
    )
    .withColumn("days_since_registration",
        F.datediff(F.current_date(), F.to_date("registration_ts"))
    )
    .withColumn("customer_tenure_band",
        F.when(F.col("days_since_registration") < 30,  "new_30d")
        .when(F.col("days_since_registration") < 90,  "new_90d")
        .when(F.col("days_since_registration") < 365, "established")
        .otherwise("loyal")
    )
)

# COMMAND ----------
# ── Step 5: Validation ────────────────────────────────────────────
print("Step 5: Validating records...")

df_validated = (
    df_enriched
    .withColumn("_err_null_id",     F.col("customer_id").isNull())
    .withColumn("_err_no_contact",
        F.col("email_normalised").isNull() & F.col("phone_normalised").isNull()
    )
    .withColumn("_err_invalid_email",
        F.col("email").isNotNull() &
        ~F.col("email").rlike(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
    )
    .withColumn("_err_invalid_segment",
        F.col("segment").isNotNull() & ~F.col("segment").isin(list(VALID_SEGMENTS))
    )
    .withColumn("_err_future_dob",
        F.col("date_of_birth") > F.current_date()
    )
    .withColumn("_err_underage",
        F.col("age").isNotNull() & (F.col("age") < 13)
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

# COMMAND ----------
# ── Step 6: Deduplication ─────────────────────────────────────────
print("Step 6: Deduplicating...")

dedup_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("ingest_ts").desc_nulls_last())
)

df_deduped = (
    df_clean
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_is_valid", *error_cols)
)

# COMMAND ----------
# ── Step 7: Silver schema ─────────────────────────────────────────
print("Step 7: Projecting final silver schema...")

SILVER_COLS = [
    # Identity
    "customer_id", "source_system",
    # Raw PII (keep for audit)
    "first_name", "last_name", "email", "phone",
    "date_of_birth", "gender",
    # Normalised PII (used for matching)
    "first_name_norm", "last_name_norm", "full_name_norm",
    "email_normalised", "phone_normalised", "last_name_soundex",
    # Address
    "address_line1", "address_line2", "city", "city_norm",
    "state_province", "postal_code", "postal_code_norm", "country",
    # Blocking keys
    "block_key_1", "block_key_2", "block_key_3", "email_domain",
    # Derived
    "age", "age_band", "days_since_registration", "customer_tenure_band",
    # Segmentation
    "segment", "loyalty_tier", "is_active",
    # Timestamps
    "registration_ts", "ingest_ts",
]

df_silver = (
    df_deduped
    .select(*SILVER_COLS)
    .withColumn("pipeline_version", F.lit("2.0"))
    .withColumn("silver_ts",        F.current_timestamp())
    # Placeholder — entity resolution job in Phase 6 fills this
    .withColumn("resolved_entity_id", F.lit(None).cast(StringType()))
    .withColumn("er_confidence",      F.lit(None).cast("double"))
)

# COMMAND ----------
# ── Step 8: Write to Delta (MERGE — customer records are mutable) ──
print("Step 8: Writing to silver Delta...")

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (silver_table.alias("existing")
     .merge(df_silver.alias("new"), "existing.customer_id = new.customer_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
else:
    (df_silver.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("country")
     .option("overwriteSchema", "true")
     .save(SILVER_PATH)
    )

print("Write complete.")

# COMMAND ----------
# ── Step 9: Optimize ──────────────────────────────────────────────
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    ZORDER BY (email_normalised, last_name_soundex)
""")

# COMMAND ----------
# ── Step 10: Metrics ──────────────────────────────────────────────
er_ready = df_silver.filter(
    F.col("block_key_2").isNotNull() | F.col("block_key_3").isNotNull()
).count()

metrics = {
    "execution_date":   EXECUTION_DATE,
    "raw_count":        raw_count,
    "silver_count":     df_deduped.count(),
    "quarantine_count": df_quarantine.count(),
    "er_ready_count":   er_ready,
    "no_contact_info":  raw_count - er_ready,
}

print("\n── Pipeline metrics ─────────────────────────────────────")
for k, v in metrics.items():
    print(f"  {k:<25}: {v}")

dbutils.notebook.exit(str(metrics))
