-- =============================================================================
-- Manufacturing IoT Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE manufacturing_iot_cleanup
  DESCRIPTION 'Cleanup pipeline for Manufacturing Iot — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'manufacturing-iot'
  STATUS disabled
  LIFECYCLE production
;


-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_oee WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_sensor_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_shift WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_production_line WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_sensor WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.readings_validated WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sensor_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_shifts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_production_lines WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sensors WITH FILES;
