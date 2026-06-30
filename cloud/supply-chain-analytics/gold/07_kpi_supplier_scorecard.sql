-- =============================================================================
-- Gold: KPI: Supplier Scorecard
-- =============================================================================
-- Aggregated supplier performance metrics:
--   - on_time_delivery_pct: orders received by expected date / total orders
--   - avg_fill_rate: average qty_received / qty_ordered across all POs
--   - avg_lead_time: average days from order to receipt
--   - total_spend: sum of all PO costs
--   - defect_rate: shipments with exceptions / total shipments
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.kpi_supplier_scorecard AS
WITH supplier_orders AS (
  SELECT
    fo.supplier_id,
    fo.supplier_name,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN fo.on_time_flag = true THEN 1 ELSE 0 END) AS on_time_orders,
    AVG(fo.fill_rate) AS avg_fill_rate,
    AVG(fo.days_to_receive) AS avg_lead_time,
    SUM(fo.total_cost) AS total_spend,
    SUM(fo.qty_ordered) AS total_qty_ordered,
    SUM(fo.qty_received) AS total_qty_received
  FROM sc.gold.fact_orders fo
  GROUP BY fo.supplier_id, fo.supplier_name
),
supplier_shipments AS (
  SELECT
    fo.supplier_id,
    COUNT(DISTINCT fo.shipment_id) AS total_shipments,
    SUM(CASE WHEN fo.shipment_exception = true THEN 1 ELSE 0 END) AS exception_shipments
  FROM sc.gold.fact_orders fo
  WHERE fo.shipment_id IS NOT NULL
  GROUP BY fo.supplier_id
)
SELECT
  so.supplier_id,
  so.supplier_name,
  so.total_orders,
  CAST(so.on_time_orders AS DECIMAL(10,4)) / CAST(so.total_orders AS DECIMAL(10,4)) AS on_time_delivery_pct,
  so.avg_fill_rate,
  so.avg_lead_time AS avg_lead_time_days,
  so.total_spend,
  so.total_qty_ordered,
  so.total_qty_received,
  COALESCE(ss.total_shipments, 0) AS total_shipments,
  CASE
    WHEN COALESCE(ss.total_shipments, 0) > 0
    THEN CAST(COALESCE(ss.exception_shipments, 0) AS DECIMAL(10,4)) / CAST(ss.total_shipments AS DECIMAL(10,4))
    ELSE 0
  END AS defect_rate,
  -- Composite score: 40% on-time + 30% fill rate + 20% inverse defect + 10% speed
  (
    0.40 * (CAST(so.on_time_orders AS DECIMAL(10,4)) / CAST(so.total_orders AS DECIMAL(10,4)))
    + 0.30 * so.avg_fill_rate
    + 0.20 * (1.0 - CASE
        WHEN COALESCE(ss.total_shipments, 0) > 0
        THEN CAST(COALESCE(ss.exception_shipments, 0) AS DECIMAL(10,4)) / CAST(ss.total_shipments AS DECIMAL(10,4))
        ELSE 0
      END)
    + 0.10 * CASE
        WHEN so.avg_lead_time <= 7 THEN 1.0
        WHEN so.avg_lead_time <= 10 THEN 0.8
        WHEN so.avg_lead_time <= 14 THEN 0.6
        ELSE 0.4
      END
  ) AS composite_score,
  CURRENT_TIMESTAMP AS updated_at
FROM supplier_orders so
LEFT JOIN supplier_shipments ss ON ss.supplier_id = so.supplier_id;
