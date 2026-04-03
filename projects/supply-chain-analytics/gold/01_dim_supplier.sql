-- =============================================================================
-- Gold: Dimension — Supplier
-- =============================================================================
-- Enriches raw supplier reference data with performance metrics derived from
-- the silver order_fulfillment table: on-time delivery rate, average fill rate,
-- average lead time, total spend, and order count.
-- =============================================================================

CREATE OR REPLACE TABLE {{zone_prefix}}.gold.dim_supplier AS
SELECT
  s.supplier_id,
  s.supplier_name,
  s.country,
  s.lead_time_days AS stated_lead_time_days,
  s.reliability_rating,
  s.payment_terms,
  s.active,
  COALESCE(perf.order_count, 0) AS order_count,
  COALESCE(perf.avg_fill_rate, 0) AS avg_fill_rate,
  COALESCE(perf.avg_days_to_receive, 0) AS avg_days_to_receive,
  COALESCE(perf.on_time_delivery_pct, 0) AS on_time_delivery_pct,
  COALESCE(perf.total_spend, 0) AS total_spend,
  CURRENT_TIMESTAMP AS updated_at
FROM {{zone_prefix}}.bronze.suppliers s
LEFT JOIN (
  SELECT
    supplier_id,
    COUNT(*) AS order_count,
    AVG(fill_rate) AS avg_fill_rate,
    AVG(days_to_receive) AS avg_days_to_receive,
    CAST(SUM(CASE WHEN on_time_flag = true THEN 1 ELSE 0 END) AS DECIMAL(10,4))
      / CAST(COUNT(*) AS DECIMAL(10,4)) AS on_time_delivery_pct,
    SUM(total_cost) AS total_spend
  FROM {{zone_prefix}}.silver.order_fulfillment
  GROUP BY supplier_id
) perf ON perf.supplier_id = s.supplier_id;
