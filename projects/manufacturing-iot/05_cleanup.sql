-- =============================================================================
-- Manufacturing IoT Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE manufacturing_iot_cleanup
  DESCRIPTION 'Cleanup pipeline for Manufacturing IoT — drops all objects including smoothed readings and equipment status. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'manufacturing-iot'
  STATUS disabled
  LIFECYCLE production
;


-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_anomaly_trends WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_oee WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_shift WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_line WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_sensor WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.equipment_status WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.readings_smoothed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.readings_validated WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_readings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_production_targets WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_shifts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_production_lines WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sensors WITH FILES;
