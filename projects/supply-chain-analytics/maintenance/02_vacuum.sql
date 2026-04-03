-- =============================================================================
-- Maintenance: Vacuum Tables
-- =============================================================================
-- Removes old data files no longer referenced by the Delta transaction log.
-- Retains 30 days of history for time-travel queries.
-- =============================================================================

-- Silver tables
VACUUM {{zone_prefix}}.silver.inventory_positions RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.silver.order_fulfillment RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.silver.shipment_tracking RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.silver.demand_signals RETAIN 720 HOURS;

-- Gold dimension tables
VACUUM {{zone_prefix}}.gold.dim_supplier RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.dim_product RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.dim_warehouse RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.dim_store RETAIN 720 HOURS;

-- Gold fact tables
VACUUM {{zone_prefix}}.gold.fact_inventory RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.fact_orders RETAIN 720 HOURS;

-- Gold KPI tables
VACUUM {{zone_prefix}}.gold.kpi_supplier_scorecard RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.kpi_inventory_health RETAIN 720 HOURS;
VACUUM {{zone_prefix}}.gold.kpi_demand_forecast RETAIN 720 HOURS;
