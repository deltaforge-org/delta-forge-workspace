-- =============================================================================
-- Food Safety Inspections Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE food_safety_inspections_cleanup
  DESCRIPTION 'Cleanup pipeline for Food Safety Inspections — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'food-safety-inspections'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_compliance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_inspections WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_violation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_district WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_inspector WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_establishment WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.inspections_scored WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_inspections WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_violations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_inspectors WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_establishments WITH FILES;
