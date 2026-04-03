-- =============================================================================
-- Hospitality Reservations Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE hospitality_reservations_cleanup
  DESCRIPTION 'Cleanup pipeline for Hospitality Reservations — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'hospitality-reservations'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_revenue_management WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_reservations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_channel WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_guest WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_room_type WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_property WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.bookings_deduped WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_bookings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_channels WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_guests WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_room_types WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_properties WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zones
DROP ZONE IF EXISTS {{zone_prefix}};
