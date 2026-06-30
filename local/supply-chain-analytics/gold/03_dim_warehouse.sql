-- =============================================================================
-- Gold: Dimension: Warehouse
-- =============================================================================
-- Warehouse dimension enriched with capacity utilization derived from current
-- inventory positions. Shows total on-hand units, unique SKU count, and
-- utilization percentage against stated capacity.
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.dim_warehouse AS
SELECT
  w.warehouse_id,
  w.warehouse_name,
  w.city,
  w.state,
  w.region,
  w.capacity_units,
  w.active,
  COALESCE(inv.total_on_hand, 0) AS total_on_hand,
  COALESCE(inv.sku_count, 0) AS unique_sku_count,
  CASE
    WHEN w.capacity_units > 0
    THEN CAST(COALESCE(inv.total_on_hand, 0) AS DECIMAL(10,4)) / CAST(w.capacity_units AS DECIMAL(10,4))
    ELSE 0
  END AS utilization_pct,
  COALESCE(ship.active_shipments, 0) AS active_outbound_shipments,
  CURRENT_TIMESTAMP AS updated_at
FROM sc.bronze.warehouses w
LEFT JOIN (
  SELECT
    warehouse_id,
    SUM(on_hand_qty) AS total_on_hand,
    COUNT(DISTINCT sku) AS sku_count
  FROM sc.silver.inventory_positions
  GROUP BY warehouse_id
) inv ON inv.warehouse_id = w.warehouse_id
LEFT JOIN (
  SELECT
    origin_wh,
    COUNT(*) AS active_shipments
  FROM sc.silver.shipment_tracking
  WHERE current_status NOT IN ('delivered', 'exception')
  GROUP BY origin_wh
) ship ON ship.origin_wh = w.warehouse_id;
