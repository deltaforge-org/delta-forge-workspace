-- =============================================================================
-- Maintenance: Optimize Tables
-- =============================================================================
-- Compacts small files and applies Z-ORDER on frequently queried columns
-- to improve read performance for downstream analytics and dashboards.
-- =============================================================================

-- Silver tables: optimize on join/filter keys
OPTIMIZE {{zone_prefix}}.silver.inventory_positions
  ZORDER BY (warehouse_id, sku);

OPTIMIZE {{zone_prefix}}.silver.order_fulfillment
  ZORDER BY (supplier_id, sku, order_date);

OPTIMIZE {{zone_prefix}}.silver.shipment_tracking
  ZORDER BY (shipment_id, current_status);

OPTIMIZE {{zone_prefix}}.silver.demand_signals
  ZORDER BY (store_id, sku, sale_date);

-- Gold fact tables: optimize on common filter patterns
OPTIMIZE {{zone_prefix}}.gold.fact_inventory
  ZORDER BY (warehouse_id, sku, snapshot_date);

OPTIMIZE {{zone_prefix}}.gold.fact_orders
  ZORDER BY (supplier_id, sku, order_date);

-- Gold KPIs: optimize for dashboard queries
OPTIMIZE {{zone_prefix}}.gold.kpi_supplier_scorecard
  ZORDER BY (supplier_id);

OPTIMIZE {{zone_prefix}}.gold.kpi_inventory_health
  ZORDER BY (warehouse_id, sku, health_status);

OPTIMIZE {{zone_prefix}}.gold.kpi_demand_forecast
  ZORDER BY (store_id, sku);
