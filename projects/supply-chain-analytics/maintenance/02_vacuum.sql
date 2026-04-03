-- =============================================================================
-- Maintenance: Vacuum Tables
-- =============================================================================
-- Removes old data files no longer referenced by the Delta transaction log.
-- Retains 30 days of history for time-travel queries.
-- =============================================================================

-- Silver tables
VACUUM {{zone_prefix}}_silver.cleansed.inventory_positions RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_silver.cleansed.order_fulfillment RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_silver.cleansed.shipment_tracking RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_silver.cleansed.demand_signals RETAIN 720 HOURS;

-- Gold dimension tables
VACUUM {{zone_prefix}}_gold.analytics.dim_supplier RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.dim_product RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.dim_warehouse RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.dim_store RETAIN 720 HOURS;

-- Gold fact tables
VACUUM {{zone_prefix}}_gold.analytics.fact_inventory RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.fact_orders RETAIN 720 HOURS;

-- Gold KPI tables
VACUUM {{zone_prefix}}_gold.analytics.kpi_supplier_scorecard RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.kpi_inventory_health RETAIN 720 HOURS;
VACUUM {{zone_prefix}}_gold.analytics.kpi_demand_forecast RETAIN 720 HOURS;
