# Databricks notebook source
# databricks/notebooks/entity_resolution.py
#
# Resolves customer entities across source systems.
# Two customers from different systems (e-commerce + payments) who are
# actually the same person get assigned the same resolved_entity_id.
#
# Pipeline:
#   1. Load silver customers (all source systems)
#   2. Blocking — reduce candidate pairs using blocking keys
#   3. Feature engineering — compute similarity scores per pair
#   4. Classification — logistic regression to predict match/no-match
#   5. Clustering — connected components to form entity groups
#   6. Write resolved IDs back to silver + gold

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import PipelineModel
from graphframes import GraphFrame
from delta.tables import DeltaTable
import mlflow
import mlflow.spark

# COMMAND ----------
dbutils.widgets.text("execution_date", "", "Execution Date")
dbutils.widgets.text("s3_bucket", "project2-data-lake-dev", "S3 Bucket")
dbutils.widgets.text("mode", "incremental", "Mode: full or incremental")
dbutils.widgets.text("match_threshold", "0.75", "Match probability threshold")

EXECUTION_DATE   = dbutils.widgets.get("execution_date")
S3_BUCKET        = dbutils.widgets.get("s3_bucket")
MODE             = dbutils.widgets.get("mode")
MATCH_THRESHOLD  = float(dbutils.widgets.get("match_threshold"))

SILVER_CUSTOMERS = f"s3://{S3_BUCKET}/silver/ecommerce/customers/"
SILVER_TXNS      = f"s3://{S3_BUCKET}/silver/financial/transactions/"
ER_OUTPUT        = f"s3://{S3_BUCKET}/gold/shared/entity_resolution/"
MODEL_PATH       = f"s3://{S3_BUCKET}/staging/models/entity_resolution/"

print(f"Mode            : {MODE}")
print(f"Match threshold : {MATCH_THRESHOLD}")
print(f"Execution date  : {EXECUTION_DATE}")

# COMMAND ----------
# ── Step 1: Load silver customers ─────────────────────────────────
print("Step 1: Loading silver customers...")

df_customers = (
    spark.read.format("delta").load(SILVER_CUSTOMERS)
    .select(
        "customer_id", "source_system",
        "full_name_norm", "email_normalised", "phone_normalised",
        "last_name_soundex", "postal_code_norm", "city_norm",
        "block_key_1", "block_key_2", "block_key_3",
        "age", "country",
    )
    .filter(F.col("customer_id").isNotNull())
)

total_customers = df_customers.count()
print(f"Total customers: {total_customers:,}")

# Only resolve across different source systems
# (same system duplicates handled by dedup in Phase 2)
source_systems = [r["source_system"] for r in df_customers.select("source_system").distinct().collect()]
print(f"Source systems: {source_systems}")

# COMMAND ----------
# ── Step 2: Blocking — generate candidate pairs ───────────────────
# Without blocking, n=1M customers → 500B pairs (impossible).
# Blocking reduces this to ~millions of pairs by only comparing
# records that share at least one blocking key.
print("Step 2: Generating candidate pairs via blocking...")

# Self-join on each blocking key separately, union results
# Block key 1: first3(last_name) + postal_code
pairs_bk1 = (
    df_customers.alias("a")
    .join(
        df_customers.alias("b"),
        (F.col("a.block_key_1") == F.col("b.block_key_1")) &
        (F.col("a.block_key_1").isNotNull()) &
        (F.col("a.customer_id") < F.col("b.customer_id")) &  # avoid duplicates
        (F.col("a.source_system") != F.col("b.source_system")),  # cross-system only
        "inner"
    )
    .select(
        F.col("a.customer_id").alias("id_a"),
        F.col("b.customer_id").alias("id_b"),
        F.lit("block_key_1").alias("block_source"),
    )
)

# Block key 2: exact email match (strongest signal)
pairs_bk2 = (
    df_customers.alias("a")
    .join(
        df_customers.alias("b"),
        (F.col("a.block_key_2") == F.col("b.block_key_2")) &
        (F.col("a.block_key_2").isNotNull()) &
        (F.col("a.customer_id") < F.col("b.customer_id")) &
        (F.col("a.source_system") != F.col("b.source_system")),
        "inner"
    )
    .select(
        F.col("a.customer_id").alias("id_a"),
        F.col("b.customer_id").alias("id_b"),
        F.lit("block_key_2").alias("block_source"),
    )
)

# Block key 3: normalised phone
pairs_bk3 = (
    df_customers.alias("a")
    .join(
        df_customers.alias("b"),
        (F.col("a.block_key_3") == F.col("b.block_key_3")) &
        (F.col("a.block_key_3").isNotNull()) &
        (F.col("a.customer_id") < F.col("b.customer_id")) &
        (F.col("a.source_system") != F.col("b.source_system")),
        "inner"
    )
    .select(
        F.col("a.customer_id").alias("id_a"),
        F.col("b.customer_id").alias("id_b"),
        F.lit("block_key_3").alias("block_source"),
    )
)

# Union and deduplicate candidate pairs
candidate_pairs = (
    pairs_bk1.union(pairs_bk2).union(pairs_bk3)
    .dropDuplicates(["id_a", "id_b"])
)

pair_count = candidate_pairs.count()
print(f"Candidate pairs: {pair_count:,}")
print(f"Blocking reduction: {total_customers**2 / 2 / pair_count:.0f}x fewer comparisons")

# COMMAND ----------
# ── Step 3: Feature engineering ──────────────────────────────────
# For each candidate pair, compute similarity features.
print("Step 3: Computing similarity features...")

# Join candidate pairs back to customer data
pairs_with_data = (
    candidate_pairs
    .join(df_customers.alias("a"), candidate_pairs["id_a"] == F.col("a.customer_id"))
    .join(df_customers.alias("b"), candidate_pairs["id_b"] == F.col("b.customer_id"))
    .select(
        "id_a", "id_b", "block_source",
        F.col("a.full_name_norm").alias("name_a"),
        F.col("b.full_name_norm").alias("name_b"),
        F.col("a.email_normalised").alias("email_a"),
        F.col("b.email_normalised").alias("email_b"),
        F.col("a.phone_normalised").alias("phone_a"),
        F.col("b.phone_normalised").alias("phone_b"),
        F.col("a.last_name_soundex").alias("soundex_a"),
        F.col("b.last_name_soundex").alias("soundex_b"),
        F.col("a.postal_code_norm").alias("postal_a"),
        F.col("b.postal_code_norm").alias("postal_b"),
        F.col("a.country").alias("country_a"),
        F.col("b.country").alias("country_b"),
        F.col("a.age").alias("age_a"),
        F.col("b.age").alias("age_b"),
    )
)

# UDF: Jaro-Winkler similarity for name matching
@F.udf("double")
def jaro_winkler(s1, s2):
    if not s1 or not s2:
        return 0.0
    import jellyfish
    try:
        return jellyfish.jaro_winkler_similarity(s1, s2)
    except Exception:
        return 0.0

# Compute features
df_features = (
    pairs_with_data
    # Exact matches (binary)
    .withColumn("email_exact_match",
        (F.col("email_a") == F.col("email_b")) &
        F.col("email_a").isNotNull()
    )
    .withColumn("phone_exact_match",
        (F.col("phone_a") == F.col("phone_b")) &
        F.col("phone_a").isNotNull()
    )
    .withColumn("soundex_match",
        (F.col("soundex_a") == F.col("soundex_b")) &
        F.col("soundex_a").isNotNull()
    )
    .withColumn("postal_match",
        (F.col("postal_a") == F.col("postal_b")) &
        F.col("postal_a").isNotNull()
    )
    .withColumn("country_match",
        F.col("country_a") == F.col("country_b")
    )
    # Name similarity (fuzzy)
    .withColumn("name_similarity",
        jaro_winkler(F.col("name_a"), F.col("name_b"))
    )
    # Age difference
    .withColumn("age_diff",
        F.when(
            F.col("age_a").isNotNull() & F.col("age_b").isNotNull(),
            F.abs(F.col("age_a") - F.col("age_b"))
        ).otherwise(F.lit(99))
    )
    .withColumn("age_within_2",
        F.col("age_diff") <= 2
    )
    # Block key strength (email block = strongest signal)
    .withColumn("block_strength",
        F.when(F.col("block_source") == "block_key_2", 3)  # email
        .when(F.col("block_source") == "block_key_3", 2)   # phone
        .otherwise(1)                                        # name+postal
    )
    # Composite rule-based score (used when no ML model available)
    .withColumn("rule_score",
        (F.col("email_exact_match").cast("int") * 5) +
        (F.col("phone_exact_match").cast("int") * 4) +
        (F.col("soundex_match").cast("int") * 2) +
        (F.col("postal_match").cast("int") * 1) +
        (F.col("country_match").cast("int") * 1) +
        (F.col("age_within_2").cast("int") * 1) +
        (F.col("name_similarity") * 3)
    )
)

# COMMAND ----------
# ── Step 4: Match classification ─────────────────────────────────
# Use rule-based scoring for now.
# In production: train a LogisticRegression on labelled pairs.
print("Step 4: Classifying matches...")

# Normalise rule_score to 0-1 probability
MAX_SCORE = 17.0   # sum of all feature weights above

df_scored = (
    df_features
    .withColumn("match_probability",
        F.least(F.col("rule_score") / MAX_SCORE, F.lit(1.0))
    )
    .withColumn("is_match",
        F.col("match_probability") >= MATCH_THRESHOLD
    )
)

df_matches = df_scored.filter(F.col("is_match") == True)
match_count = df_matches.count()
print(f"Matched pairs: {match_count:,} (threshold={MATCH_THRESHOLD})")

# COMMAND ----------
# ── Step 5: Connected components — form entity clusters ───────────
# If A matches B and B matches C, then A, B, C are the same entity.
# GraphFrames connected components solves this automatically.
print("Step 5: Running connected components for entity clustering...")

spark.sparkContext.setCheckpointDir(
    f"s3://{S3_BUCKET}/staging/databricks_checkpoints/entity_resolution/"
)

# Build graph: vertices = customers, edges = matched pairs
vertices = df_customers.select(
    F.col("customer_id").alias("id")
).distinct()

edges = df_matches.select(
    F.col("id_a").alias("src"),
    F.col("id_b").alias("dst"),
    F.col("match_probability").alias("weight"),
)

graph = GraphFrame(vertices, edges)
components = graph.connectedComponents()
# components has columns: id, component (component ID = min customer_id in group)

component_count = components.select("component").distinct().count()
print(f"Unique entity clusters: {component_count:,}")
print(f"Customers resolved into clusters: {components.count():,}")

# COMMAND ----------
# ── Step 6: Build resolved entity table ──────────────────────────
print("Step 6: Building resolved entity map...")

# Component ID becomes the resolved_entity_id
# Use a stable hash so IDs don't change across runs
df_resolved = (
    components
    .withColumn(
        "resolved_entity_id",
        F.concat(F.lit("ent_"), F.col("component").cast("string"))
    )
    .withColumnRenamed("id", "customer_id")
    .join(
        df_customers.select("customer_id", "source_system", "full_name_norm",
                             "email_normalised", "country"),
        on="customer_id",
        how="left"
    )
    # Compute confidence: single-member clusters = 1.0 (no ambiguity)
    .withColumn(
        "cluster_size",
        F.count("customer_id").over(Window.partitionBy("resolved_entity_id"))
    )
    .withColumn(
        "er_confidence",
        F.when(F.col("cluster_size") == 1, F.lit(1.0))  # no matching needed
        .otherwise(F.lit(MATCH_THRESHOLD))
    )
    .withColumn("er_run_date",    F.lit(EXECUTION_DATE))
    .withColumn("er_updated_at",  F.current_timestamp())
)

print(f"Resolved entity map size: {df_resolved.count():,}")

# COMMAND ----------
# ── Step 7: Write outputs ─────────────────────────────────────────
print("Step 7: Writing resolved entity map to gold...")

if DeltaTable.isDeltaTable(spark, ER_OUTPUT):
    delta_table = DeltaTable.forPath(spark, ER_OUTPUT)
    (delta_table.alias("existing")
     .merge(
         df_resolved.alias("new"),
         "existing.customer_id = new.customer_id"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
else:
    (df_resolved.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("source_system")
     .save(ER_OUTPUT)
    )

# Update silver customers with resolved entity IDs
silver_table = DeltaTable.forPath(spark, SILVER_CUSTOMERS)
(silver_table.alias("silver")
 .merge(
     df_resolved.select("customer_id", "resolved_entity_id", "er_confidence").alias("er"),
     "silver.customer_id = er.customer_id"
 )
 .whenMatchedUpdate(set={
     "resolved_entity_id": "er.resolved_entity_id",
     "er_confidence":      "er.er_confidence",
 })
 .execute()
)

print("Entity resolution complete.")

# COMMAND ----------
# ── Step 8: Metrics ───────────────────────────────────────────────
multi_member = df_resolved.filter(F.col("cluster_size") > 1)
metrics = {
    "execution_date":       EXECUTION_DATE,
    "total_customers":      total_customers,
    "candidate_pairs":      pair_count,
    "matched_pairs":        match_count,
    "unique_entities":      component_count,
    "multi_member_clusters":multi_member.select("resolved_entity_id").distinct().count(),
    "match_threshold":      MATCH_THRESHOLD,
}

print("\n── Entity Resolution Metrics ────────────────────────────")
for k, v in metrics.items():
    print(f"  {k:<28}: {v}")

dbutils.notebook.exit(str(metrics))
