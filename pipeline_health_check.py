"""
pipeline_health_check.py

Checks every running component of Project 2 and reports status.
Run from: ~/project2-pipeline/project2/project2/

Usage:
    python pipeline_health_check.py
"""

import subprocess
import json
import time
import sys
from datetime import datetime

# ── Colours ───────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):   print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg): print(f"  {RED}✗{RESET} {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET} {msg}")
def header(msg): print(f"\n{BOLD}{BLUE}── {msg} {'─' * (50 - len(msg))}{RESET}")

results = {"passed": 0, "failed": 0, "warned": 0}

def check(name, passed, message="", warning=False):
    if passed:
        ok(f"{name}: {message}")
        results["passed"] += 1
    elif warning:
        warn(f"{name}: {message}")
        results["warned"] += 1
    else:
        fail(f"{name}: {message}")
        results["failed"] += 1
    return passed


# ──────────────────────────────────────────────────────────────────
header("1. Docker containers")
# ──────────────────────────────────────────────────────────────────
try:
    out = subprocess.check_output(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
                                  text=True)
    containers = {}
    for line in out.strip().split("\n"):
        if "\t" in line:
            name, status = line.split("\t", 1)
            containers[name] = status

    expected = {
        "airflow-airflow-webserver-1": "Airflow webserver",
        "airflow-airflow-scheduler-1": "Airflow scheduler",
        "airflow-postgres-1":          "Airflow postgres",
        "project2-neo4j-1":            "Neo4j",
        "project2-localstack-1":       "LocalStack (AWS)",
    }

    for container, label in expected.items():
        status = containers.get(container, "NOT RUNNING")
        running = "Up" in status
        check(label, running, status if running else "not found")

except Exception as e:
    fail(f"Docker check failed: {e}")


# ──────────────────────────────────────────────────────────────────
header("2. Airflow")
# ──────────────────────────────────────────────────────────────────
try:
    import urllib.request
    resp = urllib.request.urlopen("http://localhost:8081/health", timeout=5)
    data = json.loads(resp.read())
    check("Airflow UI",       True, "reachable on port 8081")
    check("Airflow scheduler",
          data.get("scheduler", {}).get("status") == "healthy",
          data.get("scheduler", {}).get("status", "unknown"))
    check("Airflow metadb",
          data.get("metadatabase", {}).get("status") == "healthy",
          data.get("metadatabase", {}).get("status", "unknown"))
except Exception as e:
    fail(f"Airflow UI: {e}")

# Check DAG is loaded
try:
    result = subprocess.run(
        ["docker", "exec", "airflow-airflow-webserver-1",
         "airflow", "dags", "list"],
        capture_output=True, text=True, timeout=15
    )
    dag_loaded = "project2_master_pipeline" in result.stdout
    check("DAG project2_master_pipeline", dag_loaded,
          "loaded" if dag_loaded else "not found in DAG list")
except Exception as e:
    fail(f"DAG check: {e}")


# ──────────────────────────────────────────────────────────────────
header("3. Postgres (local data store)")
# ──────────────────────────────────────────────────────────────────
try:
    import psycopg2
    conn = psycopg2.connect(
        "postgresql://airflow:airflow@localhost:5434/airflow",
        connect_timeout=5
    )
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0].split(",")[0]
    check("Postgres connection", True, version)

    # Check if dbt schemas exist
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name IN ('dbt_dev_staging', 'dbt_dev_warehouse', 'dbt_dev_gold', 'gold')
    """)
    schemas = [r[0] for r in cur.fetchall()]
    check("dbt schemas created", len(schemas) > 0,
          f"found: {schemas}" if schemas else "no dbt schemas found — run dbt run first",
          warning=len(schemas) == 0)
    conn.close()
except Exception as e:
    fail(f"Postgres: {e}")


# ──────────────────────────────────────────────────────────────────
header("4. dbt")
# ──────────────────────────────────────────────────────────────────
import os
dbt_dir = os.path.expanduser("~/project2-pipeline/project2/project2/dbt")
if os.path.exists(dbt_dir):
    try:
        result = subprocess.run(
            ["dbt", "debug", "--target", "local", "--profiles-dir", dbt_dir],
            capture_output=True, text=True, cwd=dbt_dir, timeout=30
        )
        connected = "Connection test: [OK" in result.stdout
        check("dbt connection",    connected, "connected to local postgres" if connected else "connection failed")
        check("dbt project valid", "dbt_project.yml file [OK" in result.stdout, "dbt_project.yml OK")
        check("dbt profiles valid","profiles.yml file [OK" in result.stdout, "profiles.yml OK")
    except Exception as e:
        fail(f"dbt: {e}")
else:
    warn(f"dbt directory not found at {dbt_dir}")


# ──────────────────────────────────────────────────────────────────
header("5. LocalStack (Kinesis + S3)")
# ──────────────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen(
        "http://localhost:4566/_localstack/health", timeout=5
    )
    health = json.loads(resp.read())
    services = health.get("services", {})
    for svc in ["s3", "kinesis", "dynamodb", "sns"]:
        status = services.get(svc, "not found")
        check(f"LocalStack {svc}", status in ("available", "running"),
              status)
except Exception as e:
    fail(f"LocalStack: {e}")

# Check Kinesis streams
try:
    import boto3
    kinesis = boto3.client("kinesis",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    streams = kinesis.list_streams()["StreamNames"]
    expected_streams = [
        "project2-orders-stream",
        "project2-transactions-stream",
        "project2-clickstream-stream",
    ]
    for s in expected_streams:
        check(f"Kinesis stream: {s}", s in streams,
              "exists" if s in streams else "not found — run bootstrap")
except Exception as e:
    fail(f"Kinesis streams: {e}")

# Check S3 bucket
try:
    s3 = boto3.client("s3",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    check("S3 bucket project2-data-lake-dev",
          "project2-data-lake-dev" in buckets,
          "exists" if "project2-data-lake-dev" in buckets else "not found — run bootstrap")

    # Count parquet files in bronze
    resp = s3.list_objects_v2(Bucket="project2-data-lake-dev", Prefix="bronze/")
    parquet_files = [o["Key"] for o in resp.get("Contents", [])
                     if o["Key"].endswith(".parquet")]
    check("Bronze zone parquet files",
          len(parquet_files) > 0,
          f"{len(parquet_files)} files found" if parquet_files else "0 files — run producer + consumer",
          warning=len(parquet_files) == 0)
except Exception as e:
    fail(f"S3: {e}")


# ──────────────────────────────────────────────────────────────────
header("6. Neo4j knowledge graph")
# ──────────────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen("http://localhost:7475", timeout=5)
    check("Neo4j browser", True, "reachable on port 7475")
except Exception as e:
    fail(f"Neo4j browser: {e}")

try:
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(
        "bolt://localhost:7688",
        auth=("neo4j", "project2neo4j")
    )
    with driver.session() as session:
        counts = {}
        for label in ["Customer", "Order", "Transaction", "Merchant"]:
            n = session.run(f"MATCH (n:{label}) RETURN COUNT(n) AS c").single()["c"]
            counts[label] = n

        total = sum(counts.values())
        check("Neo4j nodes loaded", total > 0,
              f"{total} nodes — " + ", ".join(f"{k}: {v}" for k,v in counts.items()))

        edges = session.run("MATCH ()-[r]->() RETURN COUNT(r) AS c").single()["c"]
        check("Neo4j relationships", edges > 0, f"{edges} relationships")

        same_as = session.run("MATCH ()-[r:SAME_AS]-() RETURN COUNT(r) AS c").single()["c"]
        check("SAME_AS edges (entity resolution)", same_as > 0,
              f"{same_as} edges — entity resolution working")
    driver.close()
except Exception as e:
    fail(f"Neo4j: {e}")


# ──────────────────────────────────────────────────────────────────
header("7. FastAPI data product")
# ──────────────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen("http://localhost:8001/health", timeout=5)
    data = json.loads(resp.read())
    check("FastAPI health",   data.get("status") == "healthy", "healthy on port 8001")
    check("FastAPI version",  True, data.get("version", "unknown"))
except Exception as e:
    fail(f"FastAPI: {e}")

try:
    resp = urllib.request.urlopen("http://localhost:8001/metrics", timeout=5)
    data = json.loads(resp.read())
    check("FastAPI /metrics", True,
          f"pipeline reporting {data.get('total_customers', 0):,} customers")
except Exception as e:
    fail(f"FastAPI /metrics: {e}")

# Test RAG endpoint
try:
    import urllib.request, urllib.parse
    payload = json.dumps({"question": "show me customers", "max_rows": 5}).encode()
    req = urllib.request.Request(
        "http://localhost:8001/query/",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    check("FastAPI RAG endpoint", "answer" in data,
          f"returned answer with {data.get('row_count', 0)} rows")
except Exception as e:
    fail(f"FastAPI RAG: {e}")


# ──────────────────────────────────────────────────────────────────
header("8. Project files")
# ──────────────────────────────────────────────────────────────────
base = os.path.expanduser("~/project2-pipeline/project2/project2")
key_files = [
    ("airflow/dags/project2_master_pipeline.py", "Airflow master DAG"),
    ("databricks/notebooks/orders_bronze_to_silver.py", "Spark orders notebook"),
    ("databricks/notebooks/transactions_bronze_to_silver.py", "Spark transactions notebook"),
    ("databricks/notebooks/customers_bronze_to_silver.py", "Spark customers notebook"),
    ("databricks/notebooks/entity_resolution.py", "Entity resolution notebook"),
    ("dbt/models/marts/mart_customer_360.sql", "dbt customer 360 mart"),
    ("dbt/models/marts/mart_fraud_summary.sql", "dbt fraud mart"),
    ("snowflake/queries/01_setup.sql", "Snowflake setup SQL"),
    ("neo4j/loaders/entity_loader.py", "Neo4j entity loader"),
    ("api/main.py", "FastAPI main"),
    ("api/routers/query.py", "RAG query router"),
]
for path, label in key_files:
    full = os.path.join(base, path)
    exists = os.path.exists(full)
    check(label, exists, "present" if exists else f"missing at {path}")


# ──────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────
total = results["passed"] + results["failed"] + results["warned"]
print(f"\n{'='*55}")
print(f"{BOLD}Pipeline Health Summary{RESET}")
print(f"{'='*55}")
print(f"  {GREEN}Passed : {results['passed']}{RESET}")
print(f"  {YELLOW}Warned : {results['warned']}{RESET}")
print(f"  {RED}Failed : {results['failed']}{RESET}")
print(f"  Total  : {total}")
print(f"{'='*55}")

if results["failed"] == 0:
    print(f"\n{GREEN}{BOLD}All systems operational!{RESET}")
elif results["failed"] <= 3:
    print(f"\n{YELLOW}{BOLD}Pipeline mostly healthy — minor issues to fix.{RESET}")
else:
    print(f"\n{RED}{BOLD}Pipeline has issues — check failed items above.{RESET}")
