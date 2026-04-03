-- =============================================================================
-- Maintenance: Cleanup (Teardown)
-- =============================================================================
-- Disables the pipeline and drops all objects. Use this to fully reset the
-- project environment. PIPELINE STATUS disabled prevents scheduled runs.
-- =============================================================================

PIPELINE STATUS supply_chain_analytics disabled;

-- Drop gold KPI tables
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.kpi_demand_forecast;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.kpi_inventory_health;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.kpi_supplier_scorecard;

-- Drop gold fact tables
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.fact_orders;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.fact_inventory;

-- Drop gold dimension tables
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.dim_store;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.dim_warehouse;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.dim_product;
DROP TABLE IF EXISTS {{zone_prefix}}_gold.analytics.dim_supplier;

-- Drop silver tables
DROP TABLE IF EXISTS {{zone_prefix}}_silver.cleansed.demand_signals;
DROP TABLE IF EXISTS {{zone_prefix}}_silver.cleansed.shipment_tracking;
DROP TABLE IF EXISTS {{zone_prefix}}_silver.cleansed.order_fulfillment;
DROP TABLE IF EXISTS {{zone_prefix}}_silver.cleansed.inventory_positions;

-- Drop bronze tables
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.pos_demand;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.transport_shipments;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.warehouse_movements;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.purchase_orders;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.stores;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.products;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.warehouses;
DROP TABLE IF EXISTS {{zone_prefix}}_bronze.raw.suppliers;

-- Drop schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}_gold.analytics;
DROP SCHEMA IF EXISTS {{zone_prefix}}_silver.cleansed;
DROP SCHEMA IF EXISTS {{zone_prefix}}_bronze.raw;

-- Drop zones
DROP ZONE IF EXISTS {{zone_prefix}}_gold;
DROP ZONE IF EXISTS {{zone_prefix}}_silver;
DROP ZONE IF EXISTS {{zone_prefix}}_bronze;
