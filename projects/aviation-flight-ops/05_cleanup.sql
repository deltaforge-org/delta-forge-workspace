-- =============================================================================
-- Aviation Flight Operations Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE aviation_flight_ops_cleanup
  DESCRIPTION 'Cleanup pipeline for Aviation Flight Ops — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'aviation-flight-ops'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_otp WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_flights WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_date WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_crew WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_aircraft WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_route WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.flights_enriched WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_flight_ops WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_crew WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_aircraft WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_routes WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zones
DROP ZONE IF EXISTS {{zone_prefix}};
