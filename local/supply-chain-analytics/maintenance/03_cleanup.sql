-- =============================================================================
-- Maintenance: Cleanup (Teardown)
-- =============================================================================
-- Drops all objects created by this project. PIPELINE STATUS disabled prevents
-- accidental scheduled execution. Must be manually activated before running.
-- =============================================================================

PIPELINE supply_chain_cleanup
  DESCRIPTION 'Cleanup pipeline for Supply Chain Analytics: drops all objects. DISABLED by default.'
  SCHEDULE 'supply_chain_daily'
  TAGS 'cleanup', 'maintenance', 'supply-chain-analytics'
  STATUS disabled
  LIFECYCLE production
;

-- Drop gold KPI tables
DROP DELTA TABLE IF EXISTS sc.gold.kpi_demand_forecast WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.kpi_inventory_health WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.kpi_supplier_scorecard WITH FILES;

-- Drop gold fact tables
DROP DELTA TABLE IF EXISTS sc.gold.fact_orders WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.fact_inventory WITH FILES;

-- Drop gold dimension tables
DROP DELTA TABLE IF EXISTS sc.gold.dim_store WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.dim_warehouse WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.dim_product WITH FILES;
DROP DELTA TABLE IF EXISTS sc.gold.dim_supplier WITH FILES;

-- Drop silver tables
DROP DELTA TABLE IF EXISTS sc.silver.demand_signals WITH FILES;
DROP DELTA TABLE IF EXISTS sc.silver.shipment_tracking WITH FILES;
DROP DELTA TABLE IF EXISTS sc.silver.order_fulfillment WITH FILES;
DROP DELTA TABLE IF EXISTS sc.silver.inventory_positions WITH FILES;

-- Drop bronze tables
DROP DELTA TABLE IF EXISTS sc.bronze.pos_demand WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.transport_shipments WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.warehouse_movements WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.purchase_orders WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.stores WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.products WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.warehouses WITH FILES;
DROP DELTA TABLE IF EXISTS sc.bronze.suppliers WITH FILES;

-- Drop schemas
DROP SCHEMA IF EXISTS sc.gold;
DROP SCHEMA IF EXISTS sc.silver;
DROP SCHEMA IF EXISTS sc.bronze;

-- Drop zone
DROP ZONE IF EXISTS sc;
