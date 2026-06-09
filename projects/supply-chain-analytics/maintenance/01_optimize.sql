-- =============================================================================
-- Maintenance: Optimize Tables
-- =============================================================================
-- Compacts small files and applies Z-ORDER on frequently queried columns
-- to improve read performance for downstream analytics and dashboards.
-- =============================================================================

-- Silver tables: optimize on join/filter keys
OPTIMIZE sc.silver.inventory_positions
  ZORDER BY (warehouse_id, sku);

OPTIMIZE sc.silver.order_fulfillment
  ZORDER BY (supplier_id, sku, order_date);

OPTIMIZE sc.silver.shipment_tracking
  ZORDER BY (shipment_id, current_status);

OPTIMIZE sc.silver.demand_signals
  ZORDER BY (store_id, sku, sale_date);

-- Gold fact tables: optimize on common filter patterns
OPTIMIZE sc.gold.fact_inventory
  ZORDER BY (warehouse_id, sku, snapshot_date);

OPTIMIZE sc.gold.fact_orders
  ZORDER BY (supplier_id, sku, order_date);

-- Gold KPIs: optimize for dashboard queries
OPTIMIZE sc.gold.kpi_supplier_scorecard
  ZORDER BY (supplier_id);

OPTIMIZE sc.gold.kpi_inventory_health
  ZORDER BY (warehouse_id, sku, health_status);

OPTIMIZE sc.gold.kpi_demand_forecast
  ZORDER BY (store_id, sku);
