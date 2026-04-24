-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Cleanup
-- =============================================================================
-- This cleanup pipeline is DISABLED by default. It must be manually activated
-- before execution to prevent accidental data loss.
-- =============================================================================

PIPELINE insurance_claims_cleanup
  DESCRIPTION 'Cleanup pipeline for Insurance Claims: drops all objects. DISABLED by default.'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'cleanup', 'maintenance', 'insurance-claims'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON ins.gold.dim_claimant (name);
DROP PSEUDONYMISATION RULE IF EXISTS ON ins.bronze.raw_claimants (ssn);
DROP PSEUDONYMISATION RULE IF EXISTS ON ins.bronze.raw_policies (ssn);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS ins.gold.kpi_adjuster_performance WITH FILES;
DROP DELTA TABLE IF EXISTS ins.gold.kpi_loss_ratios WITH FILES;
DROP DELTA TABLE IF EXISTS ins.gold.fact_claims WITH FILES;
DROP DELTA TABLE IF EXISTS ins.gold.dim_coverage_type WITH FILES;
DROP DELTA TABLE IF EXISTS ins.gold.dim_adjuster WITH FILES;
DROP DELTA TABLE IF EXISTS ins.gold.dim_claimant WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS ins.silver.actuarial_snapshots WITH FILES;
DROP DELTA TABLE IF EXISTS ins.silver.claims_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS ins.silver.policy_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS ins.bronze.raw_claims WITH FILES;
DROP DELTA TABLE IF EXISTS ins.bronze.raw_adjusters WITH FILES;
DROP DELTA TABLE IF EXISTS ins.bronze.raw_claimants WITH FILES;
DROP DELTA TABLE IF EXISTS ins.bronze.raw_policies WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS ins.gold;
DROP SCHEMA IF EXISTS ins.silver;
DROP SCHEMA IF EXISTS ins.bronze;

-- ===================== DROP ZONE =====================

DROP ZONE IF EXISTS ins;
