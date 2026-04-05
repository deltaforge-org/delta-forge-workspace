-- =============================================================================
-- Telecom CDR Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE telecom_cdr_10_cleanup
  DESCRIPTION 'Cleanup pipeline for Telecom CDR — drops all objects across 3 schema versions. DISABLED by default.'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'cleanup', 'maintenance', 'telecom-cdr'
  STATUS disabled
  LIFECYCLE production
;


-- Gold tables
DROP DELTA TABLE IF EXISTS telco.gold.kpi_churn_risk WITH FILES;
DROP DELTA TABLE IF EXISTS telco.gold.kpi_network_quality WITH FILES;
DROP DELTA TABLE IF EXISTS telco.gold.fact_calls WITH FILES;
DROP DELTA TABLE IF EXISTS telco.gold.dim_plan WITH FILES;
DROP DELTA TABLE IF EXISTS telco.gold.dim_tower WITH FILES;
DROP DELTA TABLE IF EXISTS telco.gold.dim_subscriber WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS telco.silver.sessions WITH FILES;
DROP DELTA TABLE IF EXISTS telco.silver.subscriber_profiles WITH FILES;
DROP DELTA TABLE IF EXISTS telco.silver.cdr_unified WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS telco.bronze.raw_cdr_v3 WITH FILES;
DROP DELTA TABLE IF EXISTS telco.bronze.raw_cdr_v2 WITH FILES;
DROP DELTA TABLE IF EXISTS telco.bronze.raw_cdr_v1 WITH FILES;
DROP DELTA TABLE IF EXISTS telco.bronze.raw_cell_towers WITH FILES;
DROP DELTA TABLE IF EXISTS telco.bronze.raw_subscribers WITH FILES;
