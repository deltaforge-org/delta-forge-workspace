-- =============================================================================
-- Government Tax Filing Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE 10_cleanup
  DESCRIPTION 'Cleanup pipeline for Government Tax Filing — drops all objects. DISABLED by default.'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'cleanup', 'maintenance', 'government-tax-filing'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS tax.gold.kpi_preparer_quality;
DROP TABLE IF EXISTS tax.gold.kpi_revenue_analysis;
DROP TABLE IF EXISTS tax.gold.fact_filings;
DROP TABLE IF EXISTS tax.gold.dim_fiscal_year;
DROP TABLE IF EXISTS tax.gold.dim_preparer;
DROP TABLE IF EXISTS tax.gold.dim_jurisdiction;
DROP TABLE IF EXISTS tax.gold.dim_taxpayer;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS tax.silver.audit_trail;
DROP TABLE IF EXISTS tax.silver.taxpayer_profiles;
DROP TABLE IF EXISTS tax.silver.amendments_applied;
DROP TABLE IF EXISTS tax.silver.filings_immutable;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS tax.bronze.raw_amendments;
DROP TABLE IF EXISTS tax.bronze.raw_filings;
DROP TABLE IF EXISTS tax.bronze.raw_preparers;
DROP TABLE IF EXISTS tax.bronze.raw_jurisdictions;
DROP TABLE IF EXISTS tax.bronze.raw_taxpayers;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS tax.gold;
DROP SCHEMA IF EXISTS tax.silver;
DROP SCHEMA IF EXISTS tax.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS tax;
