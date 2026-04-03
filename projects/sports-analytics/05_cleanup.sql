-- =============================================================================
-- Sports Analytics Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE sports_analytics_cleanup
  DESCRIPTION 'Cleanup pipeline for Sports Analytics — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'sports-analytics'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (graph -> gold -> silver -> bronze)

-- Graph
DROP GRAPH IF EXISTS {{zone_prefix}}.gold.player_network;

-- Graph backing tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.player_network_edges WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.player_network_vertices WITH FILES;

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_player_rankings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_match_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_season WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_match WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_team WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_player WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.player_rolling_stats WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_transfers WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_match_stats WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_matches WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_players WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_teams WITH FILES;
