-- =============================================================================
-- Energy Smart Meters Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE energy_smart_meters_cleanup
  DESCRIPTION 'Cleanup pipeline for Energy Smart Meters — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'energy-smart-meters'
  STATUS disabled
  LIFECYCLE production
;


-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_consumption_billing WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_meter_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_region WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_tariff WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_meter WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.readings_costed WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_meters WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_tariffs WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_regions WITH FILES;
