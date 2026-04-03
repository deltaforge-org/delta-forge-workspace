-- =============================================================================
-- Maintenance: Cleanup (Teardown)
-- =============================================================================
-- Drops all objects created by this project. PIPELINE STATUS disabled prevents
-- accidental scheduled execution. Must be manually activated before running.
-- =============================================================================

PIPELINE supply_chain_cleanup
  DESCRIPTION 'Cleanup pipeline for Supply Chain Analytics — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'supply-chain-analytics'
  STATUS disabled
  LIFECYCLE production;

-- Drop gold KPI tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_demand_forecast WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_inventory_health WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_supplier_scorecard WITH FILES;

-- Drop gold fact tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_inventory WITH FILES;

-- Drop gold dimension tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_store WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_warehouse WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_product WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_supplier WITH FILES;

-- Drop silver tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.demand_signals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.shipment_tracking WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.order_fulfillment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.inventory_positions WITH FILES;

-- Drop bronze tables
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.pos_demand WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.transport_shipments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.warehouse_movements WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.purchase_orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.stores WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.products WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.warehouses WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.suppliers WITH FILES;

-- Drop schemas
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- Drop zone
DROP ZONE IF EXISTS {{zone_prefix}};
