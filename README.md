# Project 2 — E-commerce & Financial Transactions Pipeline

A production-grade, end-to-end data engineering pipeline built on AWS, Databricks, Snowflake, Apache Airflow, dbt, Neo4j, and FastAPI. The pipeline ingests real-time and batch data from e-commerce and financial systems, transforms it through a medallion architecture, resolves customer entities across source systems, builds a knowledge graph, and exposes the results through a REST API with a natural language query layer.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│   Orders · Transactions · Clickstream · Products · APIs    │
└───────────────────┬─────────────────────┬───────────────────┘
                    │ Real-time            │ Batch
                    ▼                      ▼
         ┌──────────────────┐   ┌──────────────────┐
         │ Kinesis Streams  │   │   S3 Raw Zone     │
         │ (3 streams)      │   │   (bronze/)       │
         └────────┬─────────┘   └────────┬──────────┘
                  └──────────┬────────────┘
                             ▼
                    ┌─────────────────┐
                    │  AWS Lambda     │
                    │ (S3 trigger →   │
                    │  Airflow DAG)   │
                    └────────┬────────┘
                             ▼
         ┌───────────────────────────────────────────┐
         │           Apache Airflow MWAA             │
         │     Orchestrates all pipeline stages      │
         └──────┬────────────┬──────────┬────────────┘
                │            │          │
                ▼            ▼          ▼
    ┌───────────────┐  ┌──────────┐  ┌──────────────┐
    │  Databricks   │  │ AWS Glue │  │     dbt      │
    │  (PySpark)    │  │ Catalog  │  │  (gold layer)│
    │ bronze→silver │  │ crawlers │  │              │
    └───────┬───────┘  └──────────┘  └──────┬───────┘
            ▼                               ▼
┌───────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                        │
│  S3 Data Lake (bronze/silver/gold) · Redshift · Snowflake │
└──────────────────────┬────────────────────────────────────┘
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
  ┌─────────┐    ┌──────────┐   ┌───────────┐
  │   dbt   │    │  Neo4j   │   │ Snowflake │
  │  models │    │  Graph   │   │ Analytics │
  └────┬────┘    └────┬─────┘   └─────┬─────┘
       └──────────────┼───────────────┘
                      ▼
              ┌───────────────┐
              │   FastAPI     │
              │  + RAG Layer  │
              │ (Claude API)  │
              └───────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow (MWAA) |
| Streaming | AWS Kinesis Data Streams |
| Serverless triggers | AWS Lambda + S3 Event Notifications |
| Batch processing | Databricks (Apache Spark), Delta Lake |
| Schema catalog | AWS Glue Data Catalog |
| Data lake | AWS S3 (bronze / silver / gold zones) |
| Analytical warehouse | Amazon Redshift |
| Cross-cloud analytics | Snowflake + Snowpipe + Data Sharing |
| Transformations | dbt (Redshift + Snowflake adapters) |
| Entity resolution | Spark ML + GraphFrames connected components |
| Knowledge graph | Neo4j (APOC + Graph Data Science) |
| Data product API | FastAPI |
| Natural language queries | RAG with Anthropic Claude API |
| Infrastructure as code | Terraform |
| Local development | Docker Compose + LocalStack |

---

## Project Structure

```
project2/
├── airflow/
│   ├── dags/
│   │   └── project2_master_pipeline.py   ← 7-stage Airflow DAG
│   └── docker-compose.yml
├── databricks/
│   └── notebooks/
│       ├── orders_bronze_to_silver.py       ← PySpark: clean & validate orders
│       ├── transactions_bronze_to_silver.py ← PySpark: fraud signal detection
│       ├── customers_bronze_to_silver.py    ← PySpark: PII normalisation
│       └── entity_resolution.py             ← Spark ML + GraphFrames ER
├── dbt/
│   ├── models/
│   │   ├── staging/      ← stg_orders, stg_transactions, stg_customers
│   │   ├── intermediate/ ← customer order & transaction metrics
│   │   └── marts/        ← customer_360, daily_order_summary, fraud_summary
│   ├── dbt_project.yml
│   └── profiles.yml
├── ingestion/
│   ├── kinesis/
│   │   ├── producer.py   ← Kinesis event producer
│   │   └── consumer.py   ← Kinesis → S3 Parquet consumer
│   └── lambda/
│       └── s3_trigger.py ← S3 PUT → Airflow DAG trigger
├── snowflake/
│   ├── queries/
│   │   ├── 01_setup.sql             ← warehouse, schemas, storage integration
│   │   ├── 02_tables_and_pipes.sql  ← gold tables + Snowpipe
│   │   └── 03_analytics_and_tasks.sql
│   └── loaders/
│       └── redshift_to_snowflake.py
├── neo4j/
│   ├── queries/
│   │   ├── 01_schema.cypher
│   │   └── 02_analytics_queries.cypher
│   └── loaders/
│       ├── entity_loader.py
│       └── seed_local.py
├── api/
│   ├── main.py
│   ├── routers/
│   │   ├── customers.py  ← GET /customers/{id}
│   │   ├── orders.py     ← GET /orders/summary
│   │   ├── fraud.py      ← GET /fraud/summary
│   │   ├── entities.py   ← GET /entities/{entity_id}
│   │   └── query.py      ← POST /query (RAG)
│   └── core/
│       ├── config.py
│       └── database.py
├── infrastructure/
│   ├── terraform/
│   │   └── kinesis_lambda.tf
│   └── scripts/
│       ├── setup_s3_lake.py
│       ├── redshift_schema.sql
│       └── bootstrap_localstack.py
├── pipeline_health_check.py
└── requirements.txt
```

---

## Data Pipeline Stages

### Stage 1 — Ingestion
**Streaming:** Orders, transactions, and clickstream events are published to three Kinesis Data Streams. A Python consumer reads from each stream and flushes records to S3 bronze as partitioned Parquet files every 60 seconds or 10,000 records.

**Batch:** Product catalog updates and CRM exports land directly in S3 bronze via scheduled batch scripts.

**Trigger:** An AWS Lambda function fires on every S3 PUT to `bronze/**/*.parquet`. It checks DynamoDB for idempotency and triggers the Airflow DAG via the REST API.

### Stage 2 — Bronze → Silver (Databricks PySpark)
Three notebooks run in parallel:

- **Orders** — type casting, derived columns, validation bitmask, quarantine, deduplication, Delta MERGE upsert, OPTIMIZE + ZORDER
- **Transactions** — fraud signal computation: velocity windows, amount anomaly detection (3σ from 30-day baseline), composite risk scoring, risk tier classification
- **Customers** — PII normalisation, soundex, blocking keys for entity resolution

### Stage 3 — Silver → Gold (dbt)
Three-layer model structure against Redshift:
- **Staging** → thin views, light cleaning
- **Intermediate** → incremental tables with business logic
- **Marts** → `mart_customer_360`, `mart_daily_order_summary`, `mart_fraud_summary`

### Stage 4 — Entity Resolution (Spark ML + GraphFrames)
Resolves the same customer appearing in multiple source systems:
1. **Blocking** — 3 blocking keys reduce O(n²) to ~2M candidate pairs per 1M customers
2. **Feature engineering** — Jaro-Winkler name similarity, exact email/phone, soundex, age
3. **Classification** — threshold-based matching (default 0.75)
4. **Clustering** — GraphFrames connected components for transitive matching

### Stage 5 — Knowledge Graph (Neo4j)
Customer → Order → Merchant → Transaction graph with SAME_AS edges from entity resolution. Powers fraud ring detection, product affinity, PageRank VIP scoring, and churn risk via network contagion.

### Stage 6 — Snowflake
Gold marts sync from Redshift → S3 → Snowflake via Snowpipe auto-ingest. Data Sharing exposes fraud summary to partners without copying data.

### Stage 7 — Data Product API (FastAPI + RAG)
REST API with a natural language query endpoint powered by Claude. The gold schema is injected as context — ask questions in plain English, get SQL + results back.

---

## Local Development

### Prerequisites
- Docker Desktop, Python 3.11, Git

### Quick Start

```bash
# 1. Clone
git clone https://github.com/<your-username>/project2-pipeline.git
cd project2-pipeline/project2

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start Airflow (port 8081)
cd airflow && docker-compose up -d && cd ..

# 4. Start LocalStack + bootstrap AWS resources
docker-compose -f docker-compose-phase4.yml up -d localstack
docker-compose -f docker-compose-phase4.yml run bootstrap
docker-compose -f docker-compose-phase4.yml run producer
docker-compose -f docker-compose-phase4.yml run consumer

# 5. Start Neo4j (port 7475) and seed graph
docker-compose -f docker-compose-phase6.yml up -d neo4j
docker-compose -f docker-compose-phase6.yml run graph-seeder

# 6. Start API (port 8001)
pip install -r requirements-api.txt
uvicorn api.main:app --reload --port 8001

# 7. Run dbt
cd dbt && dbt run --target local && cd ..

# 8. Health check
python pipeline_health_check.py
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8081 | admin / admin |
| Neo4j Browser | http://localhost:7475 | neo4j / project2neo4j |
| FastAPI Docs | http://localhost:8001/docs | — |
| LocalStack | http://localhost:4566 | — |

---

## API Endpoints

```bash
# Health
GET  http://localhost:8001/health

# Customer 360
GET  http://localhost:8001/customers/{customer_id}
GET  http://localhost:8001/customers/{customer_id}/graph
GET  http://localhost:8001/customers/?country=US&ltv_tier=platinum

# Analytics
GET  http://localhost:8001/orders/summary?days=30
GET  http://localhost:8001/fraud/summary?risk_tier=HIGH

# Entity resolution
GET  http://localhost:8001/entities/{entity_id}

# RAG natural language query
POST http://localhost:8001/query/
Content-Type: application/json
{"question": "Which customers have the highest lifetime value in the US?", "max_rows": 10}
```

---

## Production Deployment

### AWS Infrastructure
```bash
cd infrastructure/terraform
terraform init
terraform apply -var="s3_bucket=your-bucket" -var="airflow_url=https://your-mwaa-url"
```

### Environment Variables
```bash
export REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=yourpassword
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=yourpassword
export NEO4J_URI=bolt://your-neo4j:7687
export NEO4J_PASSWORD=yourpassword
export ANTHROPIC_API_KEY=your-key
```

---

## Key Design Decisions

**Why both Redshift and Snowflake?** Redshift handles high-concurrency analytics and powers the API. Snowflake handles partner-facing analytics and data sharing — demonstrating proficiency with both major warehouse platforms.

**Why Databricks over Glue ETL?** Databricks provides full Spark control, Delta Lake, and GraphFrames for entity resolution. Glue is retained for schema cataloging where it genuinely excels.

**Why GraphFrames for entity resolution?** Connected components is the correct algorithm for transitive matching (A=B + B=C → A=C=B). SQL joins cannot express this efficiently at scale.

**Why Neo4j alongside Redshift?** Fraud ring detection and network-based churn risk are graph problems. Cypher handles them in milliseconds where SQL joins become exponentially expensive.

---

## Author
CALEB DUAH
