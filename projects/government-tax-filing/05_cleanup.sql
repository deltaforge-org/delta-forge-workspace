-- =============================================================================
-- Government Tax Filing Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE government_tax_filing_cleanup
  DESCRIPTION 'Cleanup pipeline for Government Tax Filing — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'government-tax-filing'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.gold.kpi_revenue_analysis;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.fact_filings;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_preparer;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_jurisdiction;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_taxpayer;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.silver.taxpayer_enriched;
DROP TABLE IF EXISTS {{zone_prefix}}.silver.filings;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_filings;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_preparers;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_jurisdictions;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_taxpayers;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS {{zone_prefix}};
