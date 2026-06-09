-- =============================================================================
-- Gold: KPI: Demand Forecast
-- =============================================================================
-- Demand trend analysis per store x SKU:
--   - demand_trend: slope of the 7-day moving average (positive = growing)
--   - seasonality_signal: variance of daily demand (high variance = seasonal)
--   - safety_stock_recommendation: avg_daily_demand * lead_time * 1.5
--   - projected_stockout_date: estimated date on-hand reaches zero
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.kpi_demand_forecast AS
WITH demand_series AS (
  SELECT
    ds.store_id,
    ds.sku,
    ds.sale_date,
    ds.net_sold,
    ds.demand_7d_avg,
    ds.nearest_wh,
    ds.current_on_hand,
    -- Calculate trend: difference between current and previous 7d avg
    ds.demand_7d_avg - LAG(ds.demand_7d_avg, 1) OVER (
      PARTITION BY ds.store_id, ds.sku ORDER BY ds.sale_date
    ) AS demand_trend_delta
  FROM sc.silver.demand_signals ds
),
demand_stats AS (
  SELECT
    store_id,
    sku,
    nearest_wh,
    AVG(net_sold) AS avg_daily_demand,
    MAX(demand_7d_avg) AS peak_demand_7d,
    MIN(demand_7d_avg) AS trough_demand_7d,
    VARIANCE(net_sold) AS demand_variance,
    STDDEV(net_sold) AS demand_stddev,
    AVG(demand_trend_delta) AS avg_trend_slope,
    MAX(sale_date) AS latest_sale_date,
    MAX(current_on_hand) AS current_on_hand,
    COUNT(*) AS observation_count
  FROM demand_series
  GROUP BY store_id, sku, nearest_wh
),
lead_times AS (
  SELECT
    po.sku,
    AVG(s.lead_time_days) AS avg_lead_time
  FROM sc.bronze.purchase_orders po
  JOIN sc.bronze.suppliers s ON s.supplier_id = po.supplier_id
  GROUP BY po.sku
)
SELECT
  dstat.store_id,
  st.store_name,
  dstat.sku,
  dp.product_name,
  dp.category AS product_category,
  dstat.nearest_wh,
  dstat.avg_daily_demand,
  dstat.peak_demand_7d,
  dstat.trough_demand_7d,
  dstat.demand_variance,
  dstat.demand_stddev,
  -- Trend direction
  dstat.avg_trend_slope AS demand_trend,
  CASE
    WHEN dstat.avg_trend_slope > 1.0 THEN 'accelerating'
    WHEN dstat.avg_trend_slope > 0.0 THEN 'growing'
    WHEN dstat.avg_trend_slope > -1.0 THEN 'stable'
    ELSE 'declining'
  END AS trend_label,
  -- Seasonality: coefficient of variation (stddev / mean)
  CASE
    WHEN dstat.avg_daily_demand > 0
    THEN dstat.demand_stddev / dstat.avg_daily_demand
    ELSE 0
  END AS seasonality_signal,
  -- Safety stock recommendation: avg_daily_demand * lead_time * 1.5
  CAST(
    dstat.avg_daily_demand * COALESCE(lt.avg_lead_time, 10) * 1.5
  AS DECIMAL(10,2)) AS safety_stock_recommendation,
  -- Projected stockout: on_hand / avg_daily_demand days from now
  CASE
    WHEN dstat.avg_daily_demand > 0
    THEN CAST(dstat.current_on_hand AS DECIMAL(10,2)) / dstat.avg_daily_demand
    ELSE NULL
  END AS days_until_stockout,
  dstat.current_on_hand,
  dstat.observation_count,
  CURRENT_TIMESTAMP AS updated_at
FROM demand_stats dstat
LEFT JOIN lead_times lt ON lt.sku = dstat.sku
LEFT JOIN sc.gold.dim_product dp ON dp.sku = dstat.sku
LEFT JOIN sc.gold.dim_store st ON st.store_id = dstat.store_id;
