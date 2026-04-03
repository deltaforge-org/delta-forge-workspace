-- =============================================================================
-- Agriculture Crop Yields Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE agriculture_crop_yields_cleanup
  DESCRIPTION 'Cleanup pipeline for Agriculture Crop Yields — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'agriculture-crop-yields'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_yield_analysis WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_harvest WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_weather WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_season WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_crop WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_field WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.harvests_enriched WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sensors WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_harvests WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_weather WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_fields WITH FILES;
