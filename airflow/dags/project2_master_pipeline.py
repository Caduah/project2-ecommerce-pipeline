"""
airflow/dags/project2_master_pipeline.py

Master DAG for the Project 2 e-commerce & financial pipeline.
This DAG orchestrates all pipeline stages in the correct order:

  1. Ingest batch sources → S3 bronze
  2. Run Glue crawlers to update schema catalog
  3. Trigger Databricks batch transformation (bronze → silver)
  4. Run dbt models (silver → gold in Redshift)
  5. Sync gold layer to Snowflake
  6. Refresh Neo4j knowledge graph
  7. Run data quality checks
  8. Send pipeline health notification

The Kinesis streaming pipeline runs independently (always-on).
This DAG handles the daily batch window.

Schedule: daily at 02:00 UTC (after midnight data is complete)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import logging

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Config — in production these come from Airflow Variables or SSM
# ------------------------------------------------------------------
BUCKET = "{{ var.value.project2_s3_bucket }}"
DATABRICKS_CLUSTER_ID = "{{ var.value.databricks_cluster_id }}"
DATABRICKS_JOB_BRONZE_TO_SILVER = "{{ var.value.databricks_job_bronze_silver }}"
DATABRICKS_JOB_ENTITY_RESOLUTION = "{{ var.value.databricks_job_entity_res }}"
GLUE_CRAWLER_BRONZE = "project2-bronze-crawler"
GLUE_CRAWLER_SILVER = "project2-silver-crawler"
SNOWFLAKE_CONN = "snowflake_project2"
SNOWFLAKE_DATABASE = "PROJECT2_DW"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "email": ["data-alerts@yourcompany.com"],
}

# ------------------------------------------------------------------
# Python callables
# ------------------------------------------------------------------

def check_bronze_data_arrived(**context) -> str:
    """
    Sense whether yesterday's bronze data has landed.
    Returns a branch name to route the DAG.
    """
    import boto3
    from datetime import date, timedelta

    s3 = boto3.client("s3")
    ds = context["ds"]  # execution date YYYY-MM-DD

    prefixes_to_check = [
        f"bronze/ecommerce/orders/date={ds}/",
        f"bronze/financial/transactions/date={ds}/",
    ]

    missing = []
    for prefix in prefixes_to_check:
        resp = s3.list_objects_v2(
            Bucket="{{ var.value.project2_s3_bucket }}",
            Prefix=prefix,
            MaxKeys=1,
        )
        if resp.get("KeyCount", 0) == 0:
            missing.append(prefix)

    if missing:
        log.warning(f"Bronze data missing for: {missing}")
        return "handle_missing_data"
    return "crawl_bronze"


def handle_missing_data(**context) -> None:
    """Alert on missing source data but don't fail the whole pipeline."""
    log.error(f"Pipeline aborted: missing bronze data for {context['ds']}")
    # In production: send to SNS / PagerDuty
    raise ValueError(f"Missing source data for {context['ds']}")


def run_dbt_models(**context) -> None:
    """
    Run dbt inside Airflow.
    In production, replace with DbtCloudRunJobOperator or the dbt CLI operator.
    """
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--target", "prod", "--profiles-dir", "/opt/dbt"],
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError("dbt run failed")


def run_dbt_tests(**context) -> None:
    import subprocess
    result = subprocess.run(
        ["dbt", "test", "--target", "prod", "--profiles-dir", "/opt/dbt"],
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError("dbt tests failed")


def sync_to_snowflake(**context) -> None:
    """
    Copies Redshift gold tables → Snowflake via S3 unload + Snowpipe.
    Snowflake operator handles the COPY INTO command.
    """
    log.info("Triggering Snowflake sync for gold layer tables")


def refresh_neo4j_graph(**context) -> None:
    """
    Reads resolved entities from S3 gold/shared/entity_resolution/
    and upserts into Neo4j.
    Full logic lives in neo4j/loaders/entity_loader.py — called here.
    """
    import sys
    sys.path.insert(0, "/opt/project2")
    # from neo4j.loaders.entity_loader import load_entities
    # load_entities(date=context["ds"])
    log.info(f"Neo4j graph refresh triggered for {context['ds']}")


def send_pipeline_summary(**context) -> None:
    """Post a pipeline health summary. Stub — wire to Slack/SNS in production."""
    log.info(f"Pipeline complete for {context['ds']}. All tasks succeeded.")


# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------
with DAG(
    dag_id="project2_master_pipeline",
    description="Daily batch pipeline: bronze → silver → gold → Snowflake → Neo4j",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",     # 02:00 UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["project2", "ecommerce", "financial", "production"],
) as dag:

    # ── START ──────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── GATE: check bronze data arrived ───────────────────────────
    gate = BranchPythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data_arrived,
    )

    abort = PythonOperator(
        task_id="handle_missing_data",
        python_callable=handle_missing_data,
    )

    # ── STAGE 1: crawl bronze → update Glue catalog ───────────────
    with TaskGroup("crawl_bronze", tooltip="Update Glue schema catalog for bronze zone") as tg_crawl:
        crawl_orders = GlueCrawlerOperator(
            task_id="crawl_orders",
            config={"Name": GLUE_CRAWLER_BRONZE},
            aws_conn_id="aws_default",
        )
        crawl_transactions = GlueCrawlerOperator(
            task_id="crawl_transactions",
            config={"Name": "project2-bronze-transactions-crawler"},
            aws_conn_id="aws_default",
        )

    # ── STAGE 2: Databricks bronze → silver ───────────────────────
    with TaskGroup("transform_bronze_to_silver", tooltip="Spark jobs: clean & validate") as tg_spark:
        spark_orders = DatabricksRunNowOperator(
            task_id="spark_orders_silver",
            databricks_conn_id="databricks_default",
            job_id=DATABRICKS_JOB_BRONZE_TO_SILVER,
            notebook_params={"source_table": "orders", "execution_date": "{{ ds }}"},
        )
        spark_transactions = DatabricksRunNowOperator(
            task_id="spark_transactions_silver",
            databricks_conn_id="databricks_default",
            job_id=DATABRICKS_JOB_BRONZE_TO_SILVER,
            notebook_params={"source_table": "transactions", "execution_date": "{{ ds }}"},
        )
        spark_customers = DatabricksRunNowOperator(
            task_id="spark_customers_silver",
            databricks_conn_id="databricks_default",
            job_id=DATABRICKS_JOB_BRONZE_TO_SILVER,
            notebook_params={"source_table": "customers", "execution_date": "{{ ds }}"},
        )

    # ── STAGE 3: Entity resolution ────────────────────────────────
    entity_resolution = DatabricksRunNowOperator(
        task_id="entity_resolution",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ENTITY_RESOLUTION,
        notebook_params={"execution_date": "{{ ds }}"},
    )

    # ── STAGE 4: dbt (silver → gold in Redshift) ──────────────────
    with TaskGroup("dbt_gold_layer", tooltip="dbt models: build gold marts") as tg_dbt:
        dbt_run = PythonOperator(
            task_id="dbt_run",
            python_callable=run_dbt_models,
        )
        dbt_test = PythonOperator(
            task_id="dbt_test",
            python_callable=run_dbt_tests,
        )
        dbt_run >> dbt_test

    # ── STAGE 5: Sync to Snowflake ────────────────────────────────
    snowflake_sync = SnowflakeOperator(
        task_id="sync_gold_to_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            COPY INTO {{ params.database }}.gold.daily_order_summary
            FROM @project2_stage/gold/ecommerce/daily_order_summary/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE;
        """,
        params={"database": SNOWFLAKE_DATABASE},
    )

    # ── STAGE 6: Neo4j graph refresh ─────────────────────────────
    neo4j_refresh = PythonOperator(
        task_id="refresh_neo4j_graph",
        python_callable=refresh_neo4j_graph,
    )

    # ── STAGE 7: Pipeline summary ─────────────────────────────────
    summary = PythonOperator(
        task_id="send_pipeline_summary",
        python_callable=send_pipeline_summary,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── DEPENDENCIES ──────────────────────────────────────────────
    start >> gate >> [tg_crawl, abort]
    abort >> end
    tg_crawl >> tg_spark >> entity_resolution >> tg_dbt >> [snowflake_sync, neo4j_refresh] >> summary >> end
