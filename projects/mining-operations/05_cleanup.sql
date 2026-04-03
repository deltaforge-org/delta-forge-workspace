-- =============================================================================
-- Mining Operations Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE mining_operations_cleanup
  DESCRIPTION 'Cleanup pipeline for Mining Operations — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'mining-operations'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_production WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_extraction WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_shift WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_equipment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_pit WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_site WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.extractions_enriched WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_extractions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_shifts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_equipment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_pits WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sites WITH FILES;
