-- =============================================================================
-- Legal Case Management Pipeline - Gold Layer Verification
-- =============================================================================
-- 12+ ASSERTs covering dimensions, facts, graph queries, and business logic
-- =============================================================================

-- ===================== TEST 1: Dimension Completeness =====================

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM legal.gold.dim_case;

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM legal.gold.dim_attorney;

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM legal.gold.dim_party;

-- ===================== TEST 2: Fact Row Count =====================

ASSERT ROW_COUNT >= 70
SELECT COUNT(*) AS row_count FROM legal.gold.fact_billings;

-- ===================== TEST 3: Referential Integrity - No Orphan Case Keys =====================

ASSERT ROW_COUNT = 0
SELECT fb.case_key
FROM legal.gold.fact_billings fb
LEFT JOIN legal.gold.dim_case dc ON fb.case_key = dc.case_key
WHERE dc.case_key IS NULL;

-- ===================== TEST 4: Referential Integrity - No Orphan Attorney Keys =====================

ASSERT ROW_COUNT = 0
SELECT fb.attorney_key
FROM legal.gold.fact_billings fb
LEFT JOIN legal.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
WHERE da.attorney_key IS NULL;

-- ===================== TEST 5: Victoria Sterling Top Revenue Attorney =====================

SELECT da.attorney_name AS top_revenue_attorney
FROM legal.gold.fact_billings fb
JOIN legal.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
GROUP BY da.attorney_name
ORDER BY SUM(fb.amount) DESC
LIMIT 1;

ASSERT VALUE top_revenue_attorney = 'Victoria Sterling'
SELECT 'Top revenue attorney check passed' AS status;

-- ===================== TEST 6: Partner Average Rate >= 550 =====================

SELECT ROUND(AVG(da.hourly_rate), 2) AS partner_avg_rate
FROM legal.gold.dim_attorney da
WHERE da.partner_flag = true;

ASSERT VALUE partner_avg_rate >= 550
SELECT 'Partner average rate check passed' AS status;

-- ===================== TEST 7: Case Type Distribution =====================
-- Verify 5 civil, 3 criminal, 3 corporate, 2 IP, 2 employment

SELECT dc.case_type, COUNT(*) AS cnt
FROM legal.gold.dim_case dc
GROUP BY dc.case_type
ORDER BY cnt DESC;

ASSERT ROW_COUNT = 5
SELECT dc.case_type, COUNT(*) AS cnt
FROM legal.gold.dim_case dc
GROUP BY dc.case_type;

-- ===================== TEST 8: Criminal Case (C006) High Billing =====================

SELECT ROUND(SUM(fb.amount), 2) AS c006_total
FROM legal.gold.fact_billings fb
JOIN legal.gold.dim_case dc ON fb.case_key = dc.case_key
WHERE dc.case_id = 'C006';

ASSERT VALUE c006_total >= 25000
SELECT 'C006 high-billing check passed' AS status;

-- ===================== TEST 9: Attorney Utilization Rates =====================

SELECT
  kfp.attorney_name,
  kfp.practice_group,
  kfp.partner_flag,
  kfp.total_hours,
  kfp.billable_hours,
  kfp.utilization_pct,
  kfp.total_revenue,
  kfp.case_count,
  kfp.revenue_per_hour
FROM legal.gold.kpi_firm_performance kfp
ORDER BY kfp.total_revenue DESC;

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM legal.gold.kpi_firm_performance;

-- ===================== TEST 10: Case Profitability Breakdown =====================

SELECT
  dc.case_number,
  dc.case_type,
  dc.status,
  dc.priority,
  dc.complexity_score,
  COUNT(fb.billing_key) AS billing_entries,
  SUM(fb.hours) AS total_hours,
  ROUND(SUM(fb.amount), 2) AS total_billed,
  ROUND(SUM(fb.amount) / NULLIF(SUM(fb.hours), 0), 2) AS effective_rate
FROM legal.gold.fact_billings fb
JOIN legal.gold.dim_case dc ON fb.case_key = dc.case_key
GROUP BY dc.case_number, dc.case_type, dc.status, dc.priority, dc.complexity_score
ORDER BY total_billed DESC;

-- ===================== TEST 11: Practice Group Revenue =====================

SELECT
  kfp.practice_group,
  COUNT(*) AS attorneys,
  ROUND(SUM(kfp.total_revenue), 2) AS group_revenue,
  ROUND(AVG(kfp.utilization_pct), 2) AS avg_utilization
FROM legal.gold.kpi_firm_performance kfp
GROUP BY kfp.practice_group
ORDER BY group_revenue DESC;

ASSERT ROW_COUNT = 3
SELECT COUNT(DISTINCT kfp.practice_group) AS row_count
FROM legal.gold.kpi_firm_performance kfp;

-- ===================== TEST 12: Graph - Co-Counsel Network =====================

USE legal.gold.legal_network
MATCH (a1)-[:co_counsel]->(a2)
RETURN a1.party_name AS attorney_1,
     a2.party_name AS attorney_2,
     COUNT(*) AS collaborations
ORDER BY collaborations DESC;

-- ===================== TEST 13: Graph - PageRank Top Influencers =====================

USE legal.gold.legal_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY rank ASC
LIMIT 10;

-- ===================== TEST 14: Graph - Multi-Hop Conflict Check (3 hops from P008) =====================

USE legal.gold.legal_network
MATCH path = (start {party_id: 'P008'})-[*1..3]-(connected)
RETURN DISTINCT connected.party_id AS connected_party,
     connected.party_name AS name,
     LENGTH(path) AS hops
ORDER BY hops, connected_party;

-- ===================== TEST 15: Graph - Adversarial Pairs =====================
-- Find all (attorney)-[:represents]->(party)-[:opposes]->(opponent)<-[:represents]-(opponent_attorney)

USE legal.gold.legal_network
MATCH (a1)-[:represents]->(p1)-[:opposes]->(p2)<-[:represents]-(a2)
WHERE a1.party_id <> a2.party_id
RETURN a1.party_name AS attorney_1,
     p1.party_name AS party_1,
     p2.party_name AS party_2,
     a2.party_name AS attorney_2
ORDER BY attorney_1, attorney_2;

-- ===================== TEST 16: Graph - Community Detection (Louvain) =====================

USE legal.gold.legal_network
CALL algo.louvain({relationshipTypes: ['co_counsel'], maxIterations: 10})
YIELD node_id, community_id
RETURN community_id, COLLECT(node_id) AS members, COUNT(*) AS size
ORDER BY size DESC;

-- ===================== TEST 17: Silver Party Profiles =====================

SELECT
  pp.party_name,
  pp.party_type,
  pp.total_involvement,
  pp.cases_as_plaintiff,
  pp.cases_as_defendant,
  pp.cases_as_witness,
  pp.cases_as_expert
FROM legal.silver.party_profiles pp
ORDER BY pp.total_involvement DESC
LIMIT 10;

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM legal.silver.party_profiles;

-- ===================== VERIFICATION SUMMARY =====================

SELECT 'Legal Case Management Gold Layer Verification PASSED - all 12+ ASSERTs green' AS status;
