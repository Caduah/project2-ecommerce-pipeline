"""
ingestion/lambda/s3_trigger.py

Lambda function triggered by S3 PUT events on the bronze zone.
When new Parquet files land in bronze/, this function:

  1. Parses the S3 key to extract the domain, table, and date partition
  2. Validates the file (size, format, expected prefix)
  3. Updates a DynamoDB tracking table (idempotency)
  4. Triggers the Airflow DAG via the Airflow REST API
  5. Sends a notification to SNS if something goes wrong

Trigger: S3 Event Notification → Lambda
    s3://project2-data-lake-dev/bronze/**/date=*/*.parquet → this function

Deploy:
    zip lambda_package.zip s3_trigger.py
    aws lambda create-function \
        --function-name project2-s3-bronze-trigger \
        --runtime python3.11 \
        --handler s3_trigger.handler \
        --zip-file fileb://lambda_package.zip \
        --role arn:aws:iam::ACCOUNT_ID:role/project2-lambda-role
"""

import json
import logging
import os
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# ── Environment variables (set in Lambda console or Terraform) ────
AIRFLOW_BASE_URL   = os.environ.get("AIRFLOW_BASE_URL", "")
AIRFLOW_USERNAME   = os.environ.get("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD   = os.environ.get("AIRFLOW_PASSWORD", "")
AIRFLOW_DAG_ID     = os.environ.get("AIRFLOW_DAG_ID", "project2_master_pipeline")
SNS_ALERT_ARN      = os.environ.get("SNS_ALERT_ARN", "")
DYNAMO_TABLE       = os.environ.get("DYNAMO_TABLE", "project2-pipeline-runs")
MIN_FILE_SIZE_BYTES= int(os.environ.get("MIN_FILE_SIZE_BYTES", "100"))

# ── Domain detection ──────────────────────────────────────────────
# Map S3 prefix → domain label used in DAG conf
PREFIX_TO_DOMAIN = {
    "bronze/ecommerce/orders":        "orders",
    "bronze/ecommerce/customers":     "customers",
    "bronze/ecommerce/clickstream":   "clickstream",
    "bronze/financial/transactions":  "transactions",
    "bronze/financial/merchants":     "merchants",
}

dynamo = boto3.resource("dynamodb")
sns    = boto3.client("sns")


def parse_s3_key(bucket: str, key: str) -> dict:
    """
    Extracts metadata from the S3 key.

    Expected format:
        bronze/{domain}/{table}/date={YYYY-MM-DD}/hour={HH}/file.parquet
    or:
        bronze/{domain}/{table}/date={YYYY-MM-DD}/file.parquet
    """
    key = unquote_plus(key)

    # Extract date partition
    date_match = re.search(r"date=(\d{4}-\d{2}-\d{2})", key)
    execution_date = date_match.group(1) if date_match else None

    # Extract hour partition (optional)
    hour_match = re.search(r"hour=(\d{2})", key)
    hour = hour_match.group(1) if hour_match else None

    # Match domain from prefix
    domain = None
    for prefix, name in PREFIX_TO_DOMAIN.items():
        if key.startswith(prefix):
            domain = name
            break

    return {
        "bucket":         bucket,
        "key":            key,
        "execution_date": execution_date,
        "hour":           hour,
        "domain":         domain,
        "file_name":      key.split("/")[-1],
    }


def is_already_processed(execution_date: str, domain: str) -> bool:
    """
    Check DynamoDB to avoid triggering the DAG multiple times
    for the same date + domain combination.
    """
    try:
        table = dynamo.Table(DYNAMO_TABLE)
        resp  = table.get_item(
            Key={"pk": f"PIPELINE#{execution_date}", "sk": f"DOMAIN#{domain}"}
        )
        item = resp.get("Item")
        if item and item.get("status") == "TRIGGERED":
            return True
        return False
    except Exception as e:
        log.warning(f"DynamoDB check failed: {e} — proceeding anyway")
        return False


def mark_as_triggered(execution_date: str, domain: str, dag_run_id: str) -> None:
    try:
        table = dynamo.Table(DYNAMO_TABLE)
        table.put_item(Item={
            "pk":          f"PIPELINE#{execution_date}",
            "sk":          f"DOMAIN#{domain}",
            "status":      "TRIGGERED",
            "dag_run_id":  dag_run_id,
            "triggered_at":datetime.now(timezone.utc).isoformat(),
            "ttl":         int(datetime.now(timezone.utc).timestamp()) + (7 * 86400),  # 7d TTL
        })
    except Exception as e:
        log.warning(f"DynamoDB write failed: {e}")


def trigger_airflow_dag(execution_date: str, domain: str, s3_key: str) -> str:
    """
    Triggers the Airflow DAG via REST API.
    Returns the dag_run_id.
    """
    dag_run_id = f"lambda_trigger_{execution_date}_{domain}_{int(datetime.now().timestamp())}"

    payload = json.dumps({
        "dag_run_id": dag_run_id,
        "conf": {
            "execution_date": execution_date,
            "triggered_by":   "lambda_s3_trigger",
            "domain":         domain,
            "source_s3_key":  s3_key,
        }
    }).encode("utf-8")

    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"

    # Basic auth header
    import base64
    credentials = base64.b64encode(
        f"{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}".encode()
    ).decode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Basic {credentials}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
            log.info(f"DAG triggered: {body.get('dag_run_id')}")
            return dag_run_id
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log.error(f"Airflow API error {e.code}: {body}")
        raise
    except Exception as e:
        log.error(f"Failed to trigger Airflow DAG: {e}")
        raise


def send_alert(message: str, subject: str = "Project2 Pipeline Alert") -> None:
    if not SNS_ALERT_ARN:
        return
    try:
        sns.publish(
            TopicArn=SNS_ALERT_ARN,
            Subject=subject,
            Message=message,
        )
    except Exception as e:
        log.error(f"SNS alert failed: {e}")


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.
    Processes S3 event notifications.
    """
    log.info(f"Received event: {json.dumps(event)}")

    processed = []
    skipped   = []
    errors    = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]
        size   = record["s3"]["object"].get("size", 0)

        try:
            # Parse metadata from S3 key
            meta = parse_s3_key(bucket, key)
            log.info(f"Processing: {meta}")

            # Skip non-parquet files and placeholder .keep files
            if not key.endswith(".parquet"):
                log.info(f"Skipping non-parquet file: {key}")
                skipped.append(key)
                continue

            # Skip files that are too small (likely empty or corrupt)
            if size < MIN_FILE_SIZE_BYTES:
                log.warning(f"File too small ({size} bytes), skipping: {key}")
                skipped.append(key)
                continue

            # Skip unknown domains
            if not meta["domain"]:
                log.warning(f"Unknown domain for key: {key}")
                skipped.append(key)
                continue

            # Skip missing date partition
            if not meta["execution_date"]:
                log.warning(f"No date partition found in key: {key}")
                skipped.append(key)
                continue

            # Idempotency check
            if is_already_processed(meta["execution_date"], meta["domain"]):
                log.info(
                    f"Already triggered for {meta['execution_date']}/{meta['domain']}, skipping"
                )
                skipped.append(key)
                continue

            # Trigger the DAG
            dag_run_id = trigger_airflow_dag(
                execution_date=meta["execution_date"],
                domain=meta["domain"],
                s3_key=key,
            )

            # Mark as triggered in DynamoDB
            mark_as_triggered(meta["execution_date"], meta["domain"], dag_run_id)
            processed.append({"key": key, "dag_run_id": dag_run_id})

        except Exception as e:
            log.error(f"Failed to process record {key}: {e}")
            errors.append({"key": key, "error": str(e)})
            send_alert(
                subject="Project2 Lambda Trigger Error",
                message=f"Failed to process S3 event for {key}\nError: {e}",
            )

    result = {
        "processed": len(processed),
        "skipped":   len(skipped),
        "errors":    len(errors),
        "details":   {"processed": processed, "skipped": skipped, "errors": errors},
    }

    log.info(f"Lambda complete: {result}")
    return result
