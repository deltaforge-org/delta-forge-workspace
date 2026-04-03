-- =============================================================================
-- Logistics Shipments Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE logistics_shipments_cleanup
  DESCRIPTION 'Cleanup pipeline for Logistics Shipments — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'logistics-shipments'
  STATUS disabled
  LIFECYCLE production
;

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_sla_compliance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_delivery_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_shipments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_route WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_customer WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_location WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_carrier WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.sla_violations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.shipment_status WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.events_deduped WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_tracking_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sla_contracts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_customers WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_locations WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_carriers WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zone
DROP ZONE IF EXISTS {{zone_prefix}};
