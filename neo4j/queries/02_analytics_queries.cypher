// neo4j/queries/02_analytics_queries.cypher
// Production analytics queries for the Project 2 knowledge graph.
// Each query is labelled with its use case.

// ── 1. Customer 360 — full graph context for one customer ─────────
// Use case: API endpoint, customer service lookup
MATCH (c:Customer {customer_id: $customer_id})
OPTIONAL MATCH (c)-[:PLACED]->(o:Order)
OPTIONAL MATCH (o)-[:PAID_VIA]->(m:Merchant)
OPTIONAL MATCH (c)-[:MADE]->(t:Transaction)
OPTIONAL MATCH (c)-[:SAME_AS]-(twin:Customer)
RETURN
    c.customer_id           AS customer_id,
    c.resolved_entity_id    AS entity_id,
    c.full_name             AS name,
    c.ltv_tier              AS ltv_tier,
    c.churn_status          AS churn_status,
    COUNT(DISTINCT o)       AS total_orders,
    SUM(o.net_amount)       AS total_spend,
    COUNT(DISTINCT m)       AS distinct_merchants,
    COUNT(DISTINCT t)       AS total_transactions,
    AVG(t.amount)           AS avg_txn_amount,
    COUNT(DISTINCT twin)    AS linked_profiles;


// ── 2. Fraud ring detection ───────────────────────────────────────
// Use case: Find clusters of customers sharing high-risk signals
// Customers connected via shared merchant + high risk score
MATCH (c1:Customer)-[:MADE]->(t1:Transaction {risk_tier: 'HIGH'})
      -[:AT]->(m:Merchant)
      <-[:AT]-(t2:Transaction {risk_tier: 'HIGH'})
      <-[:MADE]-(c2:Customer)
WHERE c1 <> c2
  AND t1.txn_date >= date() - duration('P30D')
WITH c1, c2, m, COUNT(*) AS shared_high_risk_txns
WHERE shared_high_risk_txns >= 3
RETURN
    c1.customer_id          AS customer_1,
    c2.customer_id          AS customer_2,
    m.merchant_id           AS shared_merchant,
    m.merchant_category     AS category,
    shared_high_risk_txns   AS shared_risk_events
ORDER BY shared_high_risk_txns DESC
LIMIT 50;


// ── 3. Product affinity (co-purchase graph) ───────────────────────
// Use case: Recommendation engine input
// Products frequently bought together in the same order
MATCH (p1:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(p2:Product)
WHERE p1.product_id < p2.product_id
WITH p1, p2, COUNT(o) AS co_purchases
WHERE co_purchases >= 10
RETURN
    p1.product_id           AS product_1,
    p1.name                 AS product_1_name,
    p2.product_id           AS product_2,
    p2.name                 AS product_2_name,
    co_purchases
ORDER BY co_purchases DESC
LIMIT 100;


// ── 4. Merchant risk network ──────────────────────────────────────
// Use case: Which merchants are connected to the most high-risk customers?
MATCH (m:Merchant)<-[:PAID_VIA]-(o:Order)<-[:PLACED]-(c:Customer)
WHERE c.high_risk_txn_count > 0
WITH m, COUNT(DISTINCT c) AS risky_customers,
     AVG(c.total_risk_score) AS avg_customer_risk
WHERE risky_customers >= 5
RETURN
    m.merchant_id           AS merchant_id,
    m.merchant_name         AS merchant_name,
    m.merchant_category     AS category,
    risky_customers,
    avg_customer_risk
ORDER BY risky_customers DESC
LIMIT 20;


// ── 5. Entity resolution — find all profiles for one real person ──
// Use case: GDPR right-to-erasure, customer deduplication audit
MATCH (c:Customer {resolved_entity_id: $entity_id})
OPTIONAL MATCH (c)-[:SAME_AS*1..3]-(linked:Customer)
RETURN DISTINCT
    c.customer_id           AS customer_id,
    c.source_system         AS source_system,
    c.email_normalised      AS email,
    c.er_confidence         AS confidence,
    collect(DISTINCT linked.customer_id) AS linked_profiles;


// ── 6. Customer churn risk — graph-based signals ──────────────────
// Use case: Identify at-risk customers via network context
// Customers whose friends (shared merchant network) have churned
MATCH (at_risk:Customer {churn_status: 'active'})
      -[:PLACED]->(:Order)-[:PAID_VIA]->(m:Merchant)
      <-[:PAID_VIA]-(:Order)<-[:PLACED]-(churned:Customer {churn_status: 'churned'})
WHERE at_risk.last_order_date >= date() - duration('P30D')
WITH at_risk, COUNT(DISTINCT churned) AS churned_neighbors
WHERE churned_neighbors >= 2
RETURN
    at_risk.customer_id     AS customer_id,
    at_risk.ltv_tier        AS ltv_tier,
    at_risk.total_spend     AS total_spend,
    churned_neighbors       AS churned_in_network
ORDER BY at_risk.estimated_ltv DESC
LIMIT 100;


// ── 7. PageRank — most influential customers ──────────────────────
// Use case: Identify VIPs for loyalty programs
// (Run via GDS library — Graph Data Science)
CALL gds.pageRank.stream('customer-merchant-graph', {
    maxIterations: 20,
    dampingFactor: 0.85
})
YIELD nodeId, score
MATCH (c:Customer) WHERE id(c) = nodeId
RETURN c.customer_id AS customer_id,
       c.full_name   AS name,
       c.ltv_tier    AS ltv_tier,
       score         AS pagerank_score
ORDER BY score DESC
LIMIT 50;
