-- =============================================================================
-- Media Streaming Analytics Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE media_streaming_cleanup
  DESCRIPTION 'Cleanup pipeline for Media Streaming — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'media-streaming'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_engagement WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_viewing_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_session WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_device WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_content WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_user WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.engagement_scored WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.sessions_reconstructed WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_viewing_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_devices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_content WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_users WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zones
DROP ZONE IF EXISTS {{zone_prefix}};
