-- =============================================================================
-- Gold: Dimension — Store
-- =============================================================================
-- Store dimension with regional assignment, format type, and aggregated
-- demand metrics from the silver demand_signals table.
-- =============================================================================

CREATE OR REPLACE TABLE {{zone_prefix}}_gold.analytics.dim_store AS
SELECT
  s.store_id,
  s.store_name,
  s.city,
  s.state,
  s.region,
  s.format,
  s.active,
  COALESCE(dem.total_revenue, 0) AS total_revenue,
  COALESCE(dem.total_net_sold, 0) AS total_net_sold,
  COALESCE(dem.unique_skus, 0) AS unique_skus_sold,
  COALESCE(dem.avg_daily_demand, 0) AS avg_daily_demand,
  COALESCE(dem.days_with_sales, 0) AS days_with_sales,
  CURRENT_TIMESTAMP AS updated_at
FROM {{zone_prefix}}_bronze.raw.stores s
LEFT JOIN (
  SELECT
    store_id,
    SUM(revenue) AS total_revenue,
    SUM(net_sold) AS total_net_sold,
    COUNT(DISTINCT sku) AS unique_skus,
    AVG(net_sold) AS avg_daily_demand,
    COUNT(DISTINCT sale_date) AS days_with_sales
  FROM {{zone_prefix}}_silver.cleansed.demand_signals
  GROUP BY store_id
) dem ON dem.store_id = s.store_id;
