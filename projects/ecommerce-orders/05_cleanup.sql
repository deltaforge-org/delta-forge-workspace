-- =============================================================================
-- Omnichannel E-Commerce Orders Pipeline: Cleanup
-- =============================================================================
-- This cleanup pipeline is DISABLED by default. It must be manually activated
-- before execution to prevent accidental data loss.
-- =============================================================================

PIPELINE ecommerce_orders_cleanup
  DESCRIPTION 'Cleanup pipeline for E-Commerce Orders — drops all objects. DISABLED by default.'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'cleanup', 'maintenance', 'ecommerce-orders'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON ecom.bronze.raw_customers (email);
DROP PSEUDONYMISATION RULE ON ecom.silver.customer_rfm (email);
DROP PSEUDONYMISATION RULE ON ecom.bronze.raw_customers (address);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS ecom.gold.kpi_funnel_analysis WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.kpi_sales_dashboard WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.fact_order_lines WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.dim_date WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.dim_channel WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.dim_product WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.gold.dim_customer WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS ecom.silver.sessions WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.silver.inventory_adjustments WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.silver.customer_rfm WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.silver.orders_unified WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS ecom.bronze.raw_browsing_events WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.bronze.raw_pos_orders WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.bronze.raw_mobile_orders WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.bronze.raw_web_orders WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.bronze.raw_products WITH FILES;
DROP DELTA TABLE IF EXISTS ecom.bronze.raw_customers WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS ecom.gold;
DROP SCHEMA IF EXISTS ecom.silver;
DROP SCHEMA IF EXISTS ecom.bronze;

-- ===================== DROP ZONE =====================

DROP ZONE IF EXISTS ecom;
