-- =============================================================================
-- Nonprofit Donations Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE nonprofit_donations_cleanup
  DESCRIPTION 'Cleanup pipeline for Nonprofit Donations — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'nonprofit-donations'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_fundraising WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_donations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_fund WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_campaign WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_donor WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.donations_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.donors_tiered WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_donations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_funds WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_campaigns WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_donors WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zones
DROP ZONE IF EXISTS {{zone_prefix}};
