-- =============================================================================
-- Logistics Shipments Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE logistics_shipments_cleanup
  DESCRIPTION 'Cleanup pipeline for Logistics Shipments — drops all objects. DISABLED by default.'
  SCHEDULE 'logistics_6hr_schedule'
  TAGS 'cleanup', 'maintenance', 'logistics-shipments'
  STATUS disabled
  LIFECYCLE production
;

-- Gold tables
DROP DELTA TABLE IF EXISTS logi.gold.kpi_sla_compliance WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.kpi_delivery_performance WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.fact_shipments WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.dim_route WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.dim_customer WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.dim_location WITH FILES;
DROP DELTA TABLE IF EXISTS logi.gold.dim_carrier WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS logi.silver.sla_violations WITH FILES;
DROP DELTA TABLE IF EXISTS logi.silver.shipment_status WITH FILES;
DROP DELTA TABLE IF EXISTS logi.silver.events_deduped WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS logi.bronze.raw_tracking_events WITH FILES;
DROP DELTA TABLE IF EXISTS logi.bronze.raw_sla_contracts WITH FILES;
DROP DELTA TABLE IF EXISTS logi.bronze.raw_customers WITH FILES;
DROP DELTA TABLE IF EXISTS logi.bronze.raw_locations WITH FILES;
DROP DELTA TABLE IF EXISTS logi.bronze.raw_carriers WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS logi.gold;
DROP SCHEMA IF EXISTS logi.silver;
DROP SCHEMA IF EXISTS logi.bronze;

-- Zone
DROP ZONE IF EXISTS logi;
