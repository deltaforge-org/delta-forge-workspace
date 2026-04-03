-- =============================================================================
-- Insurance Claims Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== TEST 1: SCD2 Policy Versioning =====================

SELECT
    policy_id,
    holder_name,
    coverage_type,
    annual_premium,
    risk_score,
    valid_from,
    valid_to,
    is_current
FROM {{zone_prefix}}.gold.dim_policy
WHERE policy_id IN ('POL001', 'POL004')
ORDER BY policy_id, valid_from;

-- POL001 should have 3 versions (new, premium_increase, coverage_upgrade)
SELECT COUNT(*) AS pol001_versions
FROM {{zone_prefix}}.gold.dim_policy
WHERE policy_id = 'POL001';

-- Only one current version per policy
ASSERT VALUE pol001_versions = 3
SELECT COUNT(*) AS current_pol001
FROM {{zone_prefix}}.gold.dim_policy
WHERE policy_id = 'POL001' AND is_current = 1;

-- ===================== TEST 2: Star Schema Join - Claim Detail =====================

ASSERT VALUE current_pol001 = 1
SELECT
    fc.claim_key,
    dp.holder_name,
    dp.coverage_type,
    dc.name AS claimant_name,
    dc.age_band,
    da.name AS adjuster_name,
    da.specialization,
    fc.incident_date,
    fc.claim_amount,
    fc.approved_amount,
    fc.status,
    fc.days_to_settle
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key
JOIN {{zone_prefix}}.gold.dim_claimant dc ON fc.claimant_key = dc.claimant_key
JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
ORDER BY fc.claim_amount DESC
LIMIT 10;

-- Highest claim should be >= $100k (workplace injury liability)
SELECT MAX(fc.claim_amount) AS max_claim
FROM {{zone_prefix}}.gold.fact_claims fc;

-- ===================== TEST 3: Loss Ratio by Coverage Type =====================

ASSERT VALUE max_claim >= 100000
SELECT
    coverage_type,
    region,
    claim_count,
    total_claimed,
    total_approved,
    loss_ratio,
    avg_days_to_settle
FROM {{zone_prefix}}.gold.kpi_loss_ratios
ORDER BY loss_ratio DESC;

-- Loss ratio should exist for liability
SELECT SUM(total_claimed) AS liability_claims
FROM {{zone_prefix}}.gold.kpi_loss_ratios
WHERE coverage_type = 'liability';

-- ===================== TEST 4: Claims Aging Report =====================

ASSERT VALUE liability_claims >= 100000
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

-- Should have multiple statuses
SELECT COUNT(DISTINCT status) AS status_count
FROM {{zone_prefix}}.gold.fact_claims;

-- ===================== TEST 5: Adjuster Performance =====================

ASSERT VALUE status_count >= 3
SELECT
    da.name AS adjuster,
    da.specialization,
    da.years_experience,
    COUNT(fc.claim_key) AS claims_handled,
    ROUND(AVG(fc.days_to_settle), 1) AS avg_days_to_settle,
    ROUND(SUM(fc.approved_amount) / NULLIF(SUM(fc.claim_amount), 0) * 100, 2) AS approval_rate_pct
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
GROUP BY da.name, da.specialization, da.years_experience
ORDER BY claims_handled DESC;

-- Auto adjuster should handle most claims
SELECT da.specialization AS top_adj_spec
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
GROUP BY da.specialization
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ===================== TEST 6: Point-in-Time Policy Join Validation =====================

-- Claims should join to the correct policy version based on incident date
ASSERT VALUE top_adj_spec = 'auto'
SELECT
    fc.incident_date,
    dp.policy_id,
    dp.coverage_type,
    dp.annual_premium,
    dp.valid_from,
    dp.valid_to,
    fc.claim_amount
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key
WHERE dp.policy_id = 'POL001'
ORDER BY fc.incident_date;

-- ===================== TEST 7: Dimension Completeness =====================

SELECT COUNT(*) AS claimant_count FROM {{zone_prefix}}.gold.dim_claimant;
ASSERT VALUE claimant_count = 10

SELECT COUNT(*) AS adjuster_count FROM {{zone_prefix}}.gold.dim_adjuster;
-- ===================== TEST 8: Referential Integrity =====================

ASSERT VALUE adjuster_count = 5
SELECT COUNT(*) AS orphan_policies
FROM {{zone_prefix}}.gold.fact_claims fc
LEFT JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key
WHERE dp.surrogate_key IS NULL;

ASSERT VALUE orphan_policies = 0

SELECT COUNT(*) AS orphan_claimants
FROM {{zone_prefix}}.gold.fact_claims fc
LEFT JOIN {{zone_prefix}}.gold.dim_claimant dc ON fc.claimant_key = dc.claimant_key
WHERE dc.claimant_key IS NULL;

-- ===================== TEST 9: Regional Claim Distribution =====================

ASSERT VALUE orphan_claimants = 0
SELECT
    dp.region,
    COUNT(*) AS claims,
    ROUND(SUM(fc.claim_amount), 2) AS total_claimed,
    ROUND(AVG(fc.claim_amount), 2) AS avg_claim
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key
GROUP BY dp.region
ORDER BY total_claimed DESC;

-- Should have at least 4 regions
SELECT COUNT(DISTINCT dp.region) AS region_count
FROM {{zone_prefix}}.gold.fact_claims fc
JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key;

-- ===================== VERIFICATION SUMMARY =====================

ASSERT VALUE region_count >= 4
SELECT 'Insurance Claims Gold Layer Verification PASSED' AS status;
