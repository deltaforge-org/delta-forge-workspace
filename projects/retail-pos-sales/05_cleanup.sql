-- =============================================================================
-- Retail POS Sales Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE retail_pos_sales_cleanup
  DESCRIPTION 'Cleanup pipeline for Retail Pos Sales — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'retail-pos-sales'
  STATUS disabled
  LIFECYCLE production
;

-- Drop all objects in reverse dependency order (gold -> silver -> bronze -> schemas -> zones)

-- Gold tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_store_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_sales WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_cashier WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_date WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_product WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_store WITH FILES;

-- Silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.basket_metrics WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.transactions_enriched WITH FILES;

-- Bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_cashiers WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_products WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_stores WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Zones
DROP ZONE IF EXISTS {{zone_prefix}};
