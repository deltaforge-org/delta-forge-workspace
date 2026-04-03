-- =============================================================================
-- Automotive Telematics Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE automotive_telematics_cleanup
  DESCRIPTION 'Cleanup pipeline for Automotive Telematics — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'automotive-telematics'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_fleet_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_trips WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_route WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_driver WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_vehicle WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.trips_enriched WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_trips WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_routes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_drivers WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_vehicles WITH FILES;
