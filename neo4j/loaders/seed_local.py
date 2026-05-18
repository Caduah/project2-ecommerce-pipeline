"""
neo4j/loaders/seed_local.py

Seeds Neo4j with realistic sample data for local testing.
Creates 50 customers, 200 orders, 300 transactions, 20 merchants,
and 10 entity resolution matches (SAME_AS edges).

Run via: docker-compose -f docker-compose-phase6.yml run graph-seeder
"""

import os
import random
import uuid
from datetime import datetime, timedelta
from neo4j import GraphDatabase

NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "project2neo4j")

COUNTRIES   = ["US", "GB", "CA", "DE", "NG", "GH", "AU"]
CATEGORIES  = ["electronics", "fashion", "food", "beauty", "sports"]
SEGMENTS    = ["new", "returning", "vip", "at_risk"]
LTV_TIERS   = ["platinum", "gold", "silver", "bronze", "new"]
RISK_TIERS  = ["LOW", "MEDIUM", "HIGH"]
SOURCES     = ["ecommerce_app", "payments_service", "mobile_app"]


def random_date(days_back=365):
    return (datetime.now() - timedelta(days=random.randint(0, days_back))).strftime("%Y-%m-%d")


def seed(driver):
    with driver.session() as session:

        print("Clearing existing data...")
        session.run("MATCH (n) DETACH DELETE n")

        # ── Merchants ─────────────────────────────────────────────
        print("Creating merchants...")
        merchants = []
        for i in range(20):
            m = {
                "merchant_id":      f"merch_{i:04d}",
                "merchant_name":    f"Merchant {i}",
                "merchant_category":random.choice(CATEGORIES),
                "country":          random.choice(COUNTRIES),
                "risk_tier":        random.choice(["LOW", "MEDIUM"]),
            }
            merchants.append(m)

        session.run("""
            UNWIND $rows AS row
            MERGE (m:Merchant {merchant_id: row.merchant_id})
            SET m.merchant_name     = row.merchant_name,
                m.merchant_category = row.merchant_category,
                m.country           = row.country,
                m.risk_tier         = row.risk_tier
        """, {"rows": merchants})

        # ── Customers ─────────────────────────────────────────────
        print("Creating customers...")
        customers = []
        entity_groups = []  # pairs to connect with SAME_AS

        for i in range(50):
            entity_id = f"ent_{i // 5}"  # groups of 5 share an entity
            c = {
                "customer_id":       f"cust_{i:04d}",
                "resolved_entity_id":entity_id,
                "source_system":     random.choice(SOURCES),
                "full_name":         f"Customer {i} Name",
                "email_normalised":  f"customer{i}@example.com",
                "country":           random.choice(COUNTRIES),
                "segment":           random.choice(SEGMENTS),
                "ltv_tier":          random.choice(LTV_TIERS),
                "churn_status":      random.choice(["active", "at_risk", "churned"]),
                "estimated_ltv":     round(random.uniform(100, 15000), 2),
                "total_risk_score":  random.randint(0, 15),
                "high_risk_txn_count":random.randint(0, 5),
                "er_confidence":     round(random.uniform(0.75, 1.0), 3),
            }
            customers.append(c)

        session.run("""
            UNWIND $rows AS row
            MERGE (c:Customer {customer_id: row.customer_id})
            SET c += row
        """, {"rows": customers})

        # ── Orders ────────────────────────────────────────────────
        print("Creating orders and PLACED relationships...")
        orders = []
        for i in range(200):
            cust = random.choice(customers)
            merch = random.choice(merchants)
            orders.append({
                "order_id":        f"ord_{uuid.uuid4().hex[:10]}",
                "customer_id":     cust["customer_id"],
                "merchant_id":     merch["merchant_id"],
                "order_status":    random.choice(["delivered", "shipped", "cancelled"]),
                "order_date":      random_date(),
                "net_amount":      round(random.uniform(10, 2000), 2),
                "item_count":      random.randint(1, 10),
                "currency":        "USD",
                "payment_method":  random.choice(["credit_card", "paypal", "apple_pay"]),
                "shipping_country":cust["country"],
            })

        session.run("""
            UNWIND $rows AS row
            MERGE (o:Order {order_id: row.order_id})
            SET o.status          = row.order_status,
                o.order_date      = row.order_date,
                o.net_amount      = row.net_amount,
                o.item_count      = row.item_count,
                o.currency        = row.currency,
                o.payment_method  = row.payment_method,
                o.shipping_country= row.shipping_country
            WITH o, row
            MATCH (c:Customer {customer_id: row.customer_id})
            MERGE (c)-[:PLACED]->(o)
            WITH o, row
            MATCH (m:Merchant {merchant_id: row.merchant_id})
            MERGE (o)-[:PAID_VIA]->(m)
        """, {"rows": orders})

        # ── Transactions ──────────────────────────────────────────
        print("Creating transactions and MADE relationships...")
        transactions = []
        for i in range(300):
            cust = random.choice(customers)
            merch = random.choice(merchants)
            transactions.append({
                "transaction_id":  f"txn_{uuid.uuid4().hex[:10]}",
                "customer_id":     cust["customer_id"],
                "merchant_id":     merch["merchant_id"],
                "amount":          round(random.uniform(5, 5000), 2),
                "txn_date":        random_date(),
                "risk_score":      random.randint(0, 15),
                "risk_tier":       random.choice(RISK_TIERS),
                "is_international":random.choice([True, False]),
                "device_type":     random.choice(["mobile", "desktop", "tablet"]),
                "merchant_category":merch["merchant_category"],
            })

        session.run("""
            UNWIND $rows AS row
            MERGE (t:Transaction {transaction_id: row.transaction_id})
            SET t.amount          = row.amount,
                t.txn_date        = row.txn_date,
                t.risk_score      = row.risk_score,
                t.risk_tier       = row.risk_tier,
                t.is_international= row.is_international,
                t.device_type     = row.device_type
            WITH t, row
            MATCH (c:Customer {customer_id: row.customer_id})
            MERGE (c)-[:MADE]->(t)
            WITH t, row
            MATCH (m:Merchant {merchant_id: row.merchant_id})
            SET m.merchant_category = row.merchant_category
            MERGE (t)-[:AT]->(m)
        """, {"rows": transactions})

        # ── SAME_AS edges (entity resolution result) ──────────────
        print("Creating SAME_AS edges...")
        session.run("""
            MATCH (c1:Customer), (c2:Customer)
            WHERE c1.resolved_entity_id = c2.resolved_entity_id
              AND c1.customer_id < c2.customer_id
              AND c1.source_system <> c2.source_system
            MERGE (c1)-[r:SAME_AS]-(c2)
            SET r.confidence = 0.92,
                r.created_at = datetime()
        """)

        # ── Verify ────────────────────────────────────────────────
        print("\nGraph statistics:")
        for label in ["Customer", "Order", "Transaction", "Merchant"]:
            count = session.run(f"MATCH (n:{label}) RETURN COUNT(n) AS c").single()["c"]
            print(f"  {label}: {count}")

        edge_count = session.run("MATCH ()-[r]->() RETURN COUNT(r) AS c").single()["c"]
        same_as = session.run("MATCH ()-[r:SAME_AS]-() RETURN COUNT(r) AS c").single()["c"]
        print(f"  Total relationships: {edge_count}")
        print(f"  SAME_AS edges: {same_as}")
        print("\nSeed complete! Open http://localhost:7474 to explore the graph.")
        print("Login: neo4j / project2neo4j")


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        seed(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
