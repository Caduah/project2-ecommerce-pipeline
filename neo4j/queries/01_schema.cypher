// neo4j/queries/01_schema.cypher
// Sets up the Neo4j knowledge graph schema for Project 2.
// Run once after starting Neo4j.
//
// Graph model:
//   (:Customer) -[:PLACED]->    (:Order)
//   (:Customer) -[:MADE]->      (:Transaction)
//   (:Order)    -[:CONTAINS]->  (:Product)
//   (:Order)    -[:PAID_VIA]->  (:Merchant)
//   (:Customer) -[:SAME_AS]->   (:Customer)  // entity resolution edges
//   (:Customer) -[:LIVES_IN]->  (:Location)
//   (:Merchant) -[:LOCATED_IN]->(:Location)

// ── Constraints (enforce uniqueness + create indexes) ─────────────
CREATE CONSTRAINT customer_id IF NOT EXISTS
    FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;

CREATE CONSTRAINT resolved_entity_id IF NOT EXISTS
    FOR (c:Customer) REQUIRE c.resolved_entity_id IS NOT NULL;

CREATE CONSTRAINT order_id IF NOT EXISTS
    FOR (o:Order) REQUIRE o.order_id IS UNIQUE;

CREATE CONSTRAINT transaction_id IF NOT EXISTS
    FOR (t:Transaction) REQUIRE t.transaction_id IS UNIQUE;

CREATE CONSTRAINT merchant_id IF NOT EXISTS
    FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE;

CREATE CONSTRAINT product_id IF NOT EXISTS
    FOR (p:Product) REQUIRE p.product_id IS UNIQUE;

CREATE CONSTRAINT location_key IF NOT EXISTS
    FOR (l:Location) REQUIRE l.location_key IS UNIQUE;

// ── Indexes for common query patterns ─────────────────────────────
CREATE INDEX customer_email IF NOT EXISTS
    FOR (c:Customer) ON (c.email_normalised);

CREATE INDEX customer_segment IF NOT EXISTS
    FOR (c:Customer) ON (c.segment, c.ltv_tier);

CREATE INDEX transaction_risk IF NOT EXISTS
    FOR (t:Transaction) ON (t.risk_tier, t.txn_date);

CREATE INDEX merchant_category IF NOT EXISTS
    FOR (m:Merchant) ON (m.merchant_category);

// ── Full-text search index ─────────────────────────────────────────
CREATE FULLTEXT INDEX customer_name_search IF NOT EXISTS
    FOR (c:Customer) ON EACH [c.full_name, c.email_normalised];
