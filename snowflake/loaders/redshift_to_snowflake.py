"""
snowflake/loaders/redshift_to_snowflake.py

Syncs gold mart tables from Redshift → S3 → Snowflake.

Flow:
  1. UNLOAD gold tables from Redshift to S3 (parquet)
  2. Snowpipe picks up the files automatically (if configured)
  3. OR call COPY INTO manually for immediate sync

Usage:
    python redshift_to_snowflake.py --table customer_360 --date 2025-01-15
    python redshift_to_snowflake.py --all --date 2025-01-15
"""

import argparse
import logging
import os
from datetime import datetime

import boto3
import redshift_connector
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
S3_BUCKET    = os.environ.get("S3_BUCKET", "project2-data-lake-dev")
S3_ROLE_ARN  = os.environ.get("REDSHIFT_S3_ROLE_ARN", "")

REDSHIFT_CFG = {
    "host":     os.environ.get("REDSHIFT_HOST", ""),
    "port":     5439,
    "database": os.environ.get("REDSHIFT_DB", "project2"),
    "user":     os.environ.get("REDSHIFT_USER", "admin"),
    "password": os.environ.get("REDSHIFT_PASSWORD", ""),
}

SNOWFLAKE_CFG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "user":      os.environ.get("SNOWFLAKE_USER", ""),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "role":      "SYSADMIN",
    "warehouse": "PROJECT2_WH",
    "database":  "PROJECT2_DW",
    "schema":    "GOLD",
}

# Tables to sync: (redshift_schema.table, snowflake_table, s3_prefix)
SYNC_TABLES = [
    ("gold.mart_customer_360",      "customer_360",      "gold/ecommerce/customer_360"),
    ("gold.mart_daily_order_summary","daily_order_summary","gold/ecommerce/daily_order_summary"),
    ("gold.mart_fraud_summary",     "fraud_summary",     "gold/financial/fraud_summary"),
]


def unload_from_redshift(table: str, s3_prefix: str, execution_date: str) -> str:
    """Unload a Redshift table to S3 as Parquet."""
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}/date={execution_date}/"

    unload_sql = f"""
        UNLOAD ('SELECT * FROM {table}')
        TO '{s3_path}'
        IAM_ROLE '{S3_ROLE_ARN}'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        PARALLEL ON
        MAXFILESIZE 256 MB;
    """

    log.info(f"Unloading {table} → {s3_path}")
    conn = redshift_connector.connect(**REDSHIFT_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute(unload_sql)
        conn.commit()
        log.info(f"Unload complete: {table}")
    finally:
        conn.close()

    return s3_path


def copy_into_snowflake(snowflake_table: str, s3_prefix: str, execution_date: str) -> int:
    """COPY INTO Snowflake table from S3 parquet files."""
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}/date={execution_date}/"

    copy_sql = f"""
        COPY INTO GOLD.{snowflake_table}
        FROM '{s3_path}'
        STORAGE_INTEGRATION = s3_project2_integration
        FILE_FORMAT = (
            TYPE = PARQUET
            SNAPPY_COMPRESSION = TRUE
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = FALSE
        ON_ERROR = CONTINUE;
    """

    log.info(f"COPY INTO GOLD.{snowflake_table} from {s3_path}")
    conn = snowflake.connector.connect(**SNOWFLAKE_CFG)
    try:
        cur = conn.cursor()
        cur.execute(copy_sql)
        results = cur.fetchall()
        rows_loaded = sum(r[3] for r in results if r[3])  # rows_loaded column
        log.info(f"Loaded {rows_loaded} rows into GOLD.{snowflake_table}")
        return rows_loaded
    finally:
        conn.close()


def sync_table(
    redshift_table: str,
    snowflake_table: str,
    s3_prefix: str,
    execution_date: str,
) -> dict:
    """Full sync cycle for one table."""
    start = datetime.now()

    # Step 1: Unload from Redshift to S3
    unload_from_redshift(redshift_table, s3_prefix, execution_date)

    # Step 2: COPY INTO Snowflake from S3
    rows_loaded = copy_into_snowflake(snowflake_table, s3_prefix, execution_date)

    duration = (datetime.now() - start).total_seconds()
    return {
        "table":         snowflake_table,
        "rows_loaded":   rows_loaded,
        "execution_date":execution_date,
        "duration_sec":  round(duration, 1),
    }


def verify_snowflake_counts() -> dict:
    """Quick row count check across all gold tables."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CFG)
    counts = {}
    try:
        cur = conn.cursor()
        for _, sf_table, _ in SYNC_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM GOLD.{sf_table}")
            counts[sf_table] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def main():
    parser = argparse.ArgumentParser(description="Sync Redshift gold → Snowflake")
    parser.add_argument("--date",  required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--table", help="Specific table to sync")
    parser.add_argument("--all",   action="store_true", help="Sync all tables")
    parser.add_argument("--verify",action="store_true", help="Just verify row counts")
    args = parser.parse_args()

    if args.verify:
        counts = verify_snowflake_counts()
        log.info("Snowflake row counts:")
        for table, count in counts.items():
            log.info(f"  GOLD.{table}: {count:,} rows")
        return

    tables_to_sync = SYNC_TABLES
    if args.table:
        tables_to_sync = [t for t in SYNC_TABLES if t[1] == args.table]
        if not tables_to_sync:
            log.error(f"Unknown table: {args.table}")
            return

    results = []
    for rs_table, sf_table, s3_prefix in tables_to_sync:
        result = sync_table(rs_table, sf_table, s3_prefix, args.date)
        results.append(result)
        log.info(f"  {result}")

    log.info(f"\nSync complete. {len(results)} tables synced.")
    total_rows = sum(r["rows_loaded"] for r in results)
    log.info(f"Total rows loaded: {total_rows:,}")


if __name__ == "__main__":
    main()
