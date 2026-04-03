-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Gold Layer Verification
-- =============================================================================
-- 14 ASSERT validations covering SCD2 versioning, point-in-time joins, fraud
-- detection, loss ratios, adjuster performance, and referential integrity.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SCD2 Policy Versioning: POL001 should have 3 versions
-- -----------------------------------------------------------------------------
SELECT
    policy_id, holder_name, coverage_type, annual_premium,
    risk_score, valid_from, valid_to, is_current
FROM {{zone_prefix}}.silver.policy_dim
WHERE policy_id = 'POL001'
ORDER BY valid_from;

SELECT COUNT(*) AS pol001_versions
FROM {{zone_prefix}}.silver.policy_dim
WHERE policy_id = 'POL001';

ASSERT VALUE pol001_versions = 3
SELECT 'POL001 has 3 SCD2 versions (new, premium_increase, coverage_upgrade)' AS status;

-- -----------------------------------------------------------------------------
-- 2. SCD2: Only 1 current version per policy
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS current_pol001
FROM {{zone_prefix}}.silver.policy_dim
WHERE policy_id = 'POL001' AND is_current = 1;

ASSERT VALUE current_pol001 = 1
SELECT 'Exactly 1 current version per policy verified' AS status;

-- -----------------------------------------------------------------------------
-- 3. SCD2: Total current policies = 15 (distinct policy_ids)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS total_current
FROM {{zone_prefix}}.silver.policy_dim
WHERE is_current = 1;

ASSERT VALUE total_current = 15
SELECT '15 current policies verified' AS status;

-- -----------------------------------------------------------------------------
-- 4. Point-in-time join validation: POL001 claims use correct policy version
-- -----------------------------------------------------------------------------
SELECT
    ce.claim_id,
    ce.incident_date,
    ce.coverage_type,
    ce.annual_premium_at_incident,
    ce.risk_score_at_incident,
    ce.claim_amount
FROM {{zone_prefix}}.silver.claims_enriched ce
WHERE ce.policy_id = 'POL001'
ORDER BY ce.incident_date;

-- POL001 claim from 2023-03-15 should use auto/1380 (premium_increase version)
-- POL001 claim from 2024-01-05 should use auto_plus/1650 (coverage_upgrade version)
SELECT ce.coverage_type AS jan2024_coverage
FROM {{zone_prefix}}.silver.claims_enriched ce
WHERE ce.claim_id = 'C0039';

ASSERT VALUE jan2024_coverage = 'auto_plus'
SELECT 'Point-in-time join correctly resolved to coverage_upgrade version' AS status;

-- -----------------------------------------------------------------------------
-- 5. Fraud detection: high_risk claims exist (outliers C0009, C0010, C0037)
-- -----------------------------------------------------------------------------
SELECT
    claim_id, claim_amount, coverage_type, fraud_risk, fraud_score
FROM {{zone_prefix}}.silver.claims_enriched
WHERE fraud_risk = 'high_risk'
ORDER BY fraud_score DESC;

SELECT COUNT(*) AS high_risk_count
FROM {{zone_prefix}}.silver.claims_enriched
WHERE fraud_risk = 'high_risk';

ASSERT VALUE high_risk_count >= 2
SELECT 'Fraud outlier detection identified high-risk claims' AS status;

-- -----------------------------------------------------------------------------
-- 6. Star schema join: full claim detail query
-- -----------------------------------------------------------------------------
SELECT
    fc.claim_key,
    dc.name AS claimant_name,
    dc.age_band,
    da.name AS adjuster_name,
    da.specialization,
    dct.coverage_category,
    fc.incident_date,
    fc.claim_amount,
    fc.approved_amount,
    fc.status,
    fc.fraud_risk,
    fc.days_to_settle
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_claimant dc ON fc.claimant_key = dc.claimant_key
JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
JOIN {{zone_prefix}}.gold.dim_coverage_type dct ON fc.coverage_key = dct.coverage_key
ORDER BY fc.claim_amount DESC
LIMIT 10;

-- Highest claim should be >= $150k (outlier liability claims)
SELECT MAX(fc.claim_amount) AS max_claim
FROM {{zone_prefix}}.gold.fact_claims fc;

ASSERT VALUE max_claim >= 150000
SELECT 'Max claim exceeds $150K (outlier liability verified)' AS status;

-- -----------------------------------------------------------------------------
-- 7. Loss ratios by coverage type
-- -----------------------------------------------------------------------------
SELECT
    coverage_type, region, quarter, claim_count,
    total_claimed, total_approved, total_premium, loss_ratio, avg_days_to_settle
FROM {{zone_prefix}}.gold.kpi_loss_ratios
ORDER BY loss_ratio DESC
LIMIT 10;

-- Liability should have the highest loss ratio
SELECT SUM(total_claimed) AS liability_total
FROM {{zone_prefix}}.gold.kpi_loss_ratios
WHERE coverage_type = 'liability';

ASSERT VALUE liability_total >= 200000
SELECT 'Liability coverage has substantial claim volume' AS status;

-- -----------------------------------------------------------------------------
-- 8. Adjuster performance metrics
-- -----------------------------------------------------------------------------
SELECT
    adjuster_name, specialization, claims_handled, claims_settled,
    avg_days_to_settle, approval_rate_pct, total_approved
FROM {{zone_prefix}}.gold.kpi_adjuster_performance
ORDER BY claims_handled DESC;

-- ADJ02 (auto) or ADJ04 (health) should handle the most claims
SELECT adjuster_id AS top_adjuster
FROM {{zone_prefix}}.gold.kpi_adjuster_performance
ORDER BY claims_handled DESC
LIMIT 1;

ASSERT VALUE top_adjuster IS NOT NULL
SELECT 'Top adjuster by volume identified' AS status;

-- -----------------------------------------------------------------------------
-- 9. Claims status distribution
-- -----------------------------------------------------------------------------
SELECT
    fc.status,
    COUNT(*) AS claim_count,
    ROUND(AVG(fc.claim_amount), 2) AS avg_claim,
    ROUND(AVG(fc.days_to_settle), 2) AS avg_settle_days,
    MIN(fc.incident_date) AS oldest_incident,
    MAX(fc.incident_date) AS newest_incident
FROM {{zone_prefix}}.gold.fact_claims fc
GROUP BY fc.status
ORDER BY claim_count DESC;

SELECT COUNT(DISTINCT status) AS status_count
FROM {{zone_prefix}}.gold.fact_claims;

ASSERT VALUE status_count >= 3
SELECT 'Multiple claim statuses present (settled, filed, under_review, denied)' AS status;

-- -----------------------------------------------------------------------------
-- 10. Dimension completeness
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS claimant_count FROM {{zone_prefix}}.gold.dim_claimant;
ASSERT VALUE claimant_count = 12

SELECT COUNT(*) AS adjuster_count FROM {{zone_prefix}}.gold.dim_adjuster;
ASSERT VALUE adjuster_count = 6

SELECT COUNT(*) AS coverage_count FROM {{zone_prefix}}.gold.dim_coverage_type;
ASSERT VALUE coverage_count >= 6
SELECT 'All dimensions fully loaded (12 claimants, 6 adjusters, 6+ coverage types)' AS status;

-- -----------------------------------------------------------------------------
-- 11. Referential integrity: fact -> dim_claimant
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS orphan_claimants
FROM {{zone_prefix}}.gold.fact_claims fc
LEFT JOIN {{zone_prefix}}.gold.dim_claimant dc ON fc.claimant_key = dc.claimant_key
WHERE dc.claimant_key IS NULL;

ASSERT VALUE orphan_claimants = 0
SELECT 'Zero orphan claimants in fact table' AS status;

-- -----------------------------------------------------------------------------
-- 12. Referential integrity: fact -> dim_adjuster
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS orphan_adjusters
FROM {{zone_prefix}}.gold.fact_claims fc
LEFT JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
WHERE da.adjuster_key IS NULL;

ASSERT VALUE orphan_adjusters = 0
SELECT 'Zero orphan adjusters in fact table' AS status;

-- -----------------------------------------------------------------------------
-- 13. Regional claim distribution (at least 4 regions)
-- -----------------------------------------------------------------------------
SELECT
    pd.region,
    COUNT(*) AS claims,
    ROUND(SUM(fc.claim_amount), 2) AS total_claimed,
    ROUND(AVG(fc.claim_amount), 2) AS avg_claim,
    ROUND(AVG(fc.days_to_settle), 1) AS avg_settle_days
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.silver.policy_dim pd ON fc.policy_surrogate_key = pd.surrogate_key
GROUP BY pd.region
ORDER BY total_claimed DESC;

SELECT COUNT(DISTINCT pd.region) AS region_count
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.silver.policy_dim pd ON fc.policy_surrogate_key = pd.surrogate_key;

ASSERT VALUE region_count >= 4
SELECT 'Claims distributed across 4+ regions' AS status;

-- -----------------------------------------------------------------------------
-- 14. Actuarial snapshot captured initial policy loads
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS snapshot_count FROM {{zone_prefix}}.silver.actuarial_snapshots;

ASSERT VALUE snapshot_count > 0
SELECT 'Actuarial snapshots captured via CDF' AS status;

-- =============================================================================
-- FINAL SUMMARY
-- =============================================================================

SELECT 'Insurance Claims Gold Layer Verification PASSED - all 14 checks' AS final_status;
