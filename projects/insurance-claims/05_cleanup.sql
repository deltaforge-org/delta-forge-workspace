-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Cleanup
-- =============================================================================
-- This cleanup pipeline is DISABLED by default. It must be manually activated
-- before execution to prevent accidental data loss.
-- =============================================================================

PIPELINE insurance_claims_cleanup
  DESCRIPTION 'Cleanup pipeline for Insurance Claims — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'insurance-claims'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_claimant (name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_claimants (ssn);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_policies (ssn);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_adjuster_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_loss_ratios WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_claims WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_coverage_type WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_adjuster WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_claimant WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.actuarial_snapshots WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.claims_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.policy_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_claims WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_adjusters WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_claimants WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_policies WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONE =====================

DROP ZONE IF EXISTS {{zone_prefix}};
