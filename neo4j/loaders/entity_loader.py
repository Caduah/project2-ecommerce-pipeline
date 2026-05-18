"""
neo4j/loaders/entity_loader.py

Loads entity resolution results from S3 gold zone into Neo4j.
Builds the full knowledge graph:
  - Customer nodes with resolved entity IDs
  - Order nodes and PLACED relationships
  - Transaction nodes and MADE relationships
  - Merchant nodes and PAID_VIA relationships
  - SAME_AS edges between matched customer profiles

Usage:
    python entity_loader.py --date 2025-01-15
    python entity_loader.py --date 2025-01-15 --full-reload
"""

import argparse
import logging
import os
from typing import Iterator

import boto3
import pandas as pd
import pyarrow.parquet as pq
from neo4j import GraphDatabase
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "project2neo4j")
S3_BUCKET      = os.environ.get("S3_BUCKET",      "project2-data-lake-dev")
BATCH_SIZE     = 500   # nodes per Neo4j transaction


class Neo4jLoader:

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )
        self.s3 = boto3.client("s3")

    def close(self):
        self.driver.close()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _run_query(self, session, query: str, params: dict = None) -> list:
        result = session.run(query, params or {})
        return result.data()

    def _read_parquet_from_s3(self, prefix: str) -> pd.DataFrame:
        """Read all Parquet files under an S3 prefix into a DataFrame."""
        resp = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = [obj["Key"] for obj in resp.get("Contents", [])
                 if obj["Key"].endswith(".parquet")]

        if not files:
            log.warning(f"No parquet files found at s3://{S3_BUCKET}/{prefix}")
            return pd.DataFrame()

        dfs = []
        for key in files:
            obj = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
            import io
            dfs.append(pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas())

        return pd.concat(dfs, ignore_index=True)

    def _batch(self, df: pd.DataFrame) -> Iterator[list[dict]]:
        """Yield records in batches."""
        records = df.to_dict("records")
        for i in range(0, len(records), BATCH_SIZE):
            yield records[i : i + BATCH_SIZE]

    # ── Node loaders ─────────────────────────────────────────────

    def load_customers(self, execution_date: str) -> int:
        """Upsert Customer nodes from gold entity resolution table."""
        prefix = f"gold/shared/entity_resolution/date={execution_date}/"
        df = self._read_parquet_from_s3(prefix)
        if df.empty:
            log.warning("No entity resolution data found")
            return 0

        total = 0
        with self.driver.session() as session:
            for batch in self._batch(df):
                self._run_query(session, """
                    UNWIND $rows AS row
                    MERGE (c:Customer {customer_id: row.customer_id})
                    SET
                        c.resolved_entity_id = row.resolved_entity_id,
                        c.source_system      = row.source_system,
                        c.full_name          = row.full_name_norm,
                        c.email_normalised   = row.email_normalised,
                        c.country            = row.country,
                        c.er_confidence      = row.er_confidence,
                        c.cluster_size       = row.cluster_size,
                        c.updated_at         = datetime()
                """, {"rows": batch})
                total += len(batch)

        log.info(f"Loaded {total} Customer nodes")
        return total

    def load_orders(self, execution_date: str) -> int:
        """Upsert Order nodes and PLACED relationships."""
        prefix = f"silver/ecommerce/orders/date={execution_date}/"
        df = self._read_parquet_from_s3(prefix)
        if df.empty:
            return 0

        # Keep only columns needed for graph
        cols = ["order_id", "customer_id", "merchant_id", "order_status",
                "order_ts", "order_date", "net_amount", "item_count",
                "currency", "payment_method", "shipping_country"]
        df = df[[c for c in cols if c in df.columns]]

        total = 0
        with self.driver.session() as session:
            for batch in self._batch(df):
                # Create Order nodes + PLACED edges
                self._run_query(session, """
                    UNWIND $rows AS row
                    MERGE (o:Order {order_id: row.order_id})
                    SET
                        o.status          = row.order_status,
                        o.order_ts        = row.order_ts,
                        o.order_date      = row.order_date,
                        o.net_amount      = toFloat(row.net_amount),
                        o.item_count      = toInteger(row.item_count),
                        o.currency        = row.currency,
                        o.payment_method  = row.payment_method,
                        o.shipping_country= row.shipping_country
                    WITH o, row
                    MATCH (c:Customer {customer_id: row.customer_id})
                    MERGE (c)-[:PLACED]->(o)
                    WITH o, row
                    WHERE row.merchant_id IS NOT NULL
                    MERGE (m:Merchant {merchant_id: row.merchant_id})
                    MERGE (o)-[:PAID_VIA]->(m)
                """, {"rows": batch})
                total += len(batch)

        log.info(f"Loaded {total} Order nodes")
        return total

    def load_transactions(self, execution_date: str) -> int:
        """Upsert Transaction nodes and MADE relationships."""
        prefix = f"silver/financial/transactions/date={execution_date}/"
        df = self._read_parquet_from_s3(prefix)
        if df.empty:
            return 0

        cols = ["transaction_id", "customer_id", "merchant_id", "order_id",
                "transaction_ts", "txn_date", "amount", "currency",
                "transaction_type", "status", "risk_score", "risk_tier",
                "is_international", "device_type", "merchant_category"]
        df = df[[c for c in cols if c in df.columns]]

        total = 0
        with self.driver.session() as session:
            for batch in self._batch(df):
                self._run_query(session, """
                    UNWIND $rows AS row
                    MERGE (t:Transaction {transaction_id: row.transaction_id})
                    SET
                        t.amount           = toFloat(row.amount),
                        t.currency         = row.currency,
                        t.txn_type         = row.transaction_type,
                        t.status           = row.status,
                        t.txn_date         = row.txn_date,
                        t.risk_score       = toInteger(row.risk_score),
                        t.risk_tier        = row.risk_tier,
                        t.is_international = toBoolean(row.is_international),
                        t.device_type      = row.device_type
                    WITH t, row
                    MATCH (c:Customer {customer_id: row.customer_id})
                    MERGE (c)-[:MADE]->(t)
                    WITH t, row
                    WHERE row.merchant_id IS NOT NULL
                    MERGE (m:Merchant {merchant_id: row.merchant_id})
                    SET m.merchant_category = row.merchant_category
                    MERGE (t)-[:AT]->(m)
                """, {"rows": batch})
                total += len(batch)

        log.info(f"Loaded {total} Transaction nodes")
        return total

    def load_same_as_edges(self, execution_date: str) -> int:
        """
        Create SAME_AS edges between Customer nodes in the same entity cluster.
        These edges represent the entity resolution result in graph form.
        """
        prefix = f"gold/shared/entity_resolution/date={execution_date}/"
        df = self._read_parquet_from_s3(prefix)
        if df.empty:
            return 0

        # Only multi-member clusters need SAME_AS edges
        multi = df[df["cluster_size"] > 1][["customer_id", "resolved_entity_id", "er_confidence"]]

        if multi.empty:
            log.info("No multi-member clusters found — no SAME_AS edges needed")
            return 0

        total = 0
        with self.driver.session() as session:
            for batch in self._batch(multi):
                # Create edges between all members of the same cluster
                self._run_query(session, """
                    UNWIND $rows AS row
                    MATCH (c1:Customer {resolved_entity_id: row.resolved_entity_id})
                    MATCH (c2:Customer {resolved_entity_id: row.resolved_entity_id})
                    WHERE c1.customer_id < c2.customer_id
                    MERGE (c1)-[r:SAME_AS]-(c2)
                    SET r.confidence  = toFloat(row.er_confidence),
                        r.created_at  = datetime()
                """, {"rows": batch})
                total += len(batch)

        log.info(f"Processed {total} records for SAME_AS edges")
        return total

    def update_customer_metrics(self) -> None:
        """
        Compute and store graph-derived metrics on Customer nodes.
        Runs after all nodes and edges are loaded.
        """
        with self.driver.session() as session:
            # Degree centrality — how many orders/transactions per customer
            self._run_query(session, """
                MATCH (c:Customer)
                OPTIONAL MATCH (c)-[:PLACED]->(o:Order)
                OPTIONAL MATCH (c)-[:MADE]->(t:Transaction)
                WITH c,
                     COUNT(DISTINCT o) AS order_count,
                     COUNT(DISTINCT t) AS txn_count,
                     SUM(o.net_amount) AS total_spend
                SET c.graph_order_count = order_count,
                    c.graph_txn_count   = txn_count,
                    c.graph_total_spend = total_spend,
                    c.updated_at        = datetime()
            """)
            log.info("Customer metrics updated")

    def run(self, execution_date: str, full_reload: bool = False) -> dict:
        """Main entry point — runs the full load pipeline."""
        log.info(f"Starting Neo4j load for {execution_date}")
        if full_reload:
            log.warning("Full reload — clearing existing graph data")
            with self.driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")

        customers  = self.load_customers(execution_date)
        orders     = self.load_orders(execution_date)
        txns       = self.load_transactions(execution_date)
        same_as    = self.load_same_as_edges(execution_date)
        self.update_customer_metrics()

        metrics = {
            "execution_date":  execution_date,
            "customers_loaded":customers,
            "orders_loaded":   orders,
            "txns_loaded":     txns,
            "same_as_edges":   same_as,
        }

        log.info("Neo4j load complete:")
        for k, v in metrics.items():
            log.info(f"  {k}: {v}")

        return metrics


def main():
    parser = argparse.ArgumentParser(description="Load entity resolution data into Neo4j")
    parser.add_argument("--date",        required=True)
    parser.add_argument("--full-reload", action="store_true")
    args = parser.parse_args()

    loader = Neo4jLoader()
    try:
        loader.run(args.date, full_reload=args.full_reload)
    finally:
        loader.close()


if __name__ == "__main__":
    main()
