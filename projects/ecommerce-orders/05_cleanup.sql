-- =============================================================================
-- E-Commerce Orders Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE ecommerce_orders_cleanup
  DESCRIPTION 'Cleanup pipeline for Ecommerce Orders — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'ecommerce-orders'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_sales_dashboard WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_order_lines WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_channel WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_product WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_customer WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.customer_rfm WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.orders_merged WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_products WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_customers WITH FILES;
