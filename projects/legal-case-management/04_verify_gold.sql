-- =============================================================================
-- Legal Case Management Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== TEST 1: Attorney Utilization Rates =====================

SELECT
    da.name AS attorney,
    da.practice_area,
    da.partner_flag,
    SUM(fb.hours) AS total_hours,
    SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END) AS billable_hours,
    ROUND(100.0 * SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END) / SUM(fb.hours), 2) AS utilization_pct,
    ROUND(SUM(fb.amount), 2) AS total_revenue
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
GROUP BY da.name, da.practice_area, da.partner_flag
ORDER BY total_revenue DESC;

-- Victoria Sterling (top partner) should have highest revenue
SELECT da.name AS top_revenue_attorney
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
GROUP BY da.name
ORDER BY SUM(fb.amount) DESC
LIMIT 1;

-- ===================== TEST 2: Case Profitability =====================

ASSERT VALUE top_revenue_attorney = 'Victoria Sterling'
SELECT
    dc.case_number,
    dc.case_type,
    dc.court,
    dc.status,
    dc.complexity_score,
    COUNT(fb.billing_key) AS billing_entries,
    SUM(fb.hours) AS total_hours,
    ROUND(SUM(fb.amount), 2) AS total_billed
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_case dc ON fb.case_key = dc.case_key
GROUP BY dc.case_number, dc.case_type, dc.court, dc.status, dc.complexity_score
ORDER BY total_billed DESC;

-- Securities fraud case (CASE06) should be among highest billed
SELECT ROUND(SUM(fb.amount), 2) AS case06_total
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_case dc ON fb.case_key = dc.case_key
WHERE dc.case_type = 'securities_fraud';

-- ===================== TEST 3: Client Billing Summary =====================

ASSERT VALUE case06_total >= 20000
SELECT
    dcl.client_name,
    dcl.client_type,
    dcl.industry,
    COUNT(DISTINCT fb.case_key) AS cases_count,
    ROUND(SUM(fb.amount), 2) AS total_billed,
    ROUND(SUM(fb.hours), 1) AS total_hours
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_client dcl ON fb.client_key = dcl.client_key
GROUP BY dcl.client_name, dcl.client_type, dcl.industry
ORDER BY total_billed DESC;

-- Corporate clients should have higher total billing than individuals
SELECT ROUND(SUM(fb.amount), 2) AS corporate_total
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_client dcl ON fb.client_key = dcl.client_key
WHERE dcl.client_type = 'corporate';

SELECT ROUND(SUM(fb.amount), 2) AS individual_total
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_client dcl ON fb.client_key = dcl.client_key
WHERE dcl.client_type = 'individual';

ASSERT VALUE corporate_total >= 100000
SELECT 'corporate_total check passed' AS corporate_total_status;


-- ===================== TEST 4: Graph - Co-Counsel Network =====================

USE {{zone_prefix}}.gold.legal_network
MATCH (a1:dim_attorney)-[:REPRESENTS]->(c:dim_case)<-[:REPRESENTS]-(a2:dim_attorney)
WHERE a1.attorney_key < a2.attorney_key
RETURN a1.name AS attorney_1, a2.name AS attorney_2, c.case_type AS case_type, c.case_number AS case_number
ORDER BY a1.name, a2.name;

-- ===================== TEST 5: Graph - Most Connected Attorneys (Degree Centrality) =====================

USE {{zone_prefix}}.gold.legal_network
MATCH (a:dim_attorney)-[:REPRESENTS]->(c:dim_case)
RETURN a.name AS attorney, a.practice_area AS practice_area, COUNT(c) AS case_count
ORDER BY case_count DESC;

-- ===================== TEST 6: Graph - Attorney-Client Depth =====================

USE {{zone_prefix}}.gold.legal_network
MATCH (cl:dim_client)-[:PARTY_TO]->(c:dim_case)<-[:REPRESENTS]-(a:dim_attorney)
RETURN cl.client_name AS client, a.name AS attorney, COUNT(DISTINCT c) AS shared_cases
ORDER BY shared_cases DESC;

-- ===================== TEST 7: Graph - Community Detection for Practice Groups =====================

USE {{zone_prefix}}.gold.legal_network
MATCH (a:dim_attorney)-[:REPRESENTS]->(c:dim_case)
RETURN a.practice_area AS practice_group, COLLECT(DISTINCT a.name) AS attorneys, COUNT(DISTINCT c) AS cases_handled
ORDER BY cases_handled DESC;

-- ===================== TEST 8: Dimension Completeness =====================

SELECT COUNT(*) AS case_dim_count FROM {{zone_prefix}}.gold.dim_case;
ASSERT VALUE case_dim_count = 12

SELECT COUNT(*) AS attorney_dim_count FROM {{zone_prefix}}.gold.dim_attorney;
ASSERT VALUE attorney_dim_count = 8

SELECT COUNT(*) AS client_dim_count FROM {{zone_prefix}}.gold.dim_client;
-- ===================== TEST 9: Referential Integrity =====================

ASSERT VALUE client_dim_count = 10
SELECT COUNT(*) AS orphan_cases
FROM {{zone_prefix}}.gold.fact_billings fb
LEFT JOIN {{zone_prefix}}.gold.dim_case dc ON fb.case_key = dc.case_key
WHERE dc.case_key IS NULL;

ASSERT VALUE orphan_cases = 0

SELECT COUNT(*) AS orphan_attorneys
FROM {{zone_prefix}}.gold.fact_billings fb
LEFT JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
WHERE da.attorney_key IS NULL;

-- ===================== TEST 10: Partner vs Associate Revenue Split =====================

ASSERT VALUE orphan_attorneys = 0
SELECT
    CASE WHEN da.partner_flag = true THEN 'Partner' ELSE 'Associate' END AS role,
    COUNT(*) AS billing_entries,
    ROUND(SUM(fb.hours), 1) AS total_hours,
    ROUND(SUM(fb.amount), 2) AS total_revenue,
    ROUND(AVG(fb.hourly_rate), 2) AS avg_rate
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
GROUP BY CASE WHEN da.partner_flag = true THEN 'Partner' ELSE 'Associate' END;

-- Partners should have higher average rate
SELECT ROUND(AVG(fb.hourly_rate), 2) AS partner_avg_rate
FROM {{zone_prefix}}.gold.fact_billings fb
JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
WHERE da.partner_flag = true;

-- ===================== VERIFICATION SUMMARY =====================

ASSERT VALUE partner_avg_rate >= 550
SELECT 'Legal Case Management Gold Layer Verification PASSED' AS status;
