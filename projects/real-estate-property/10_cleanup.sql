-- =============================================================================
-- Real Estate Property Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE 10_cleanup
  DESCRIPTION 'Cleanup pipeline for Real Estate Property — drops all objects. DISABLED by default.'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'cleanup', 'maintenance', 'real-estate-property'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS realty.gold.kpi_assessment_accuracy;
DROP TABLE IF EXISTS realty.gold.kpi_market_trends;
DROP TABLE IF EXISTS realty.gold.fact_transactions;
DROP TABLE IF EXISTS realty.gold.dim_property_type;
DROP TABLE IF EXISTS realty.gold.dim_agent;
DROP TABLE IF EXISTS realty.gold.dim_neighborhood;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS realty.silver.correction_log;
DROP TABLE IF EXISTS realty.silver.transactions_enriched;
DROP TABLE IF EXISTS realty.silver.property_dim;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS realty.bronze.raw_agents;
DROP TABLE IF EXISTS realty.bronze.raw_neighborhoods;
DROP TABLE IF EXISTS realty.bronze.raw_transactions;
DROP TABLE IF EXISTS realty.bronze.raw_assessments;
DROP TABLE IF EXISTS realty.bronze.raw_properties;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS realty.gold;
DROP SCHEMA IF EXISTS realty.silver;
DROP SCHEMA IF EXISTS realty.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS realty;
