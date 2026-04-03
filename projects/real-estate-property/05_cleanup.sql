-- =============================================================================
-- Real Estate Property Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE real_estate_property_cleanup
  DESCRIPTION 'Cleanup pipeline for Real Estate Property — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'real-estate-property'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.gold.kpi_market_trends;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.fact_transactions;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_neighborhood;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_agent;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_buyer;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_property;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.silver.transaction_enriched;
DROP TABLE IF EXISTS {{zone_prefix}}.silver.property_scd2;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_neighborhoods;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_agents;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_buyers;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_transactions;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_properties;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS {{zone_prefix}};
