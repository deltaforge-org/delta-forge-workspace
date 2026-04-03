-- =============================================================================
-- Maintenance: Vacuum Tables
-- =============================================================================
-- Removes old data files no longer referenced by the Delta transaction log.
-- Retains 30 days of history for time-travel queries.
-- =============================================================================

-- Silver tables
VACUUM sc.silver.inventory_positions RETAIN 720 HOURS;
VACUUM sc.silver.order_fulfillment RETAIN 720 HOURS;
VACUUM sc.silver.shipment_tracking RETAIN 720 HOURS;
VACUUM sc.silver.demand_signals RETAIN 720 HOURS;

-- Gold dimension tables
VACUUM sc.gold.dim_supplier RETAIN 720 HOURS;
VACUUM sc.gold.dim_product RETAIN 720 HOURS;
VACUUM sc.gold.dim_warehouse RETAIN 720 HOURS;
VACUUM sc.gold.dim_store RETAIN 720 HOURS;

-- Gold fact tables
VACUUM sc.gold.fact_inventory RETAIN 720 HOURS;
VACUUM sc.gold.fact_orders RETAIN 720 HOURS;

-- Gold KPI tables
VACUUM sc.gold.kpi_supplier_scorecard RETAIN 720 HOURS;
VACUUM sc.gold.kpi_inventory_health RETAIN 720 HOURS;
VACUUM sc.gold.kpi_demand_forecast RETAIN 720 HOURS;
