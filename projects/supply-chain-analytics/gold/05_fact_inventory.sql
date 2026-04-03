-- =============================================================================
-- Gold: Fact — Inventory Snapshot
-- =============================================================================
-- Daily inventory snapshot per warehouse x product. Combines on-hand from
-- silver inventory_positions, in-transit from shipment_tracking, and on-order
-- from open POs. Calculates days_of_supply and reorder_flag.
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.fact_inventory AS
WITH on_hand AS (
  SELECT
    ip.warehouse_id,
    ip.sku,
    ip.on_hand_qty
  FROM sc.silver.inventory_positions ip
),
in_transit AS (
  -- Units currently in transit TO stores (originated from this warehouse)
  SELECT
    st.origin_wh AS warehouse_id,
    wm.sku,
    SUM(ABS(wm.quantity)) AS in_transit_qty
  FROM sc.silver.shipment_tracking st
  JOIN sc.bronze.warehouse_movements wm
    ON wm.reference_id = st.shipment_id AND wm.movement_type = 'ship'
  WHERE st.current_status IN ('dispatched', 'in_transit', 'customs_hold')
  GROUP BY st.origin_wh, wm.sku
),
on_order AS (
  -- Units on open POs not yet received
  SELECT
    wm_ref.warehouse_id,
    po.sku,
    SUM(po.qty_ordered) AS on_order_qty
  FROM sc.bronze.purchase_orders po
  -- Map POs to warehouses via previously received POs from same supplier
  LEFT JOIN (
    SELECT DISTINCT
      wm.warehouse_id,
      po2.supplier_id
    FROM sc.bronze.warehouse_movements wm
    JOIN sc.bronze.purchase_orders po2 ON wm.reference_id = po2.po_id
    WHERE wm.movement_type = 'receipt'
  ) wm_ref ON wm_ref.supplier_id = po.supplier_id
  WHERE po.status IN ('ordered', 'in_transit')
  GROUP BY wm_ref.warehouse_id, po.sku
),
avg_demand AS (
  SELECT
    nearest_wh AS warehouse_id,
    sku,
    AVG(net_sold) AS avg_daily_demand
  FROM sc.silver.demand_signals
  GROUP BY nearest_wh, sku
),
supplier_lead AS (
  SELECT
    po.sku,
    AVG(s.lead_time_days) AS avg_lead_time
  FROM sc.bronze.purchase_orders po
  JOIN sc.bronze.suppliers s ON s.supplier_id = po.supplier_id
  GROUP BY po.sku
)
SELECT
  oh.warehouse_id,
  oh.sku,
  oh.on_hand_qty,
  COALESCE(it.in_transit_qty, 0) AS in_transit_qty,
  COALESCE(oo.on_order_qty, 0) AS on_order_qty,
  oh.on_hand_qty + COALESCE(it.in_transit_qty, 0) + COALESCE(oo.on_order_qty, 0) AS total_position,
  CASE
    WHEN COALESCE(ad.avg_daily_demand, 0) > 0
    THEN CAST(oh.on_hand_qty AS DECIMAL(10,2)) / ad.avg_daily_demand
    ELSE NULL
  END AS days_of_supply,
  COALESCE(ad.avg_daily_demand, 0) AS avg_daily_demand,
  COALESCE(sl.avg_lead_time, 0) AS avg_lead_time_days,
  CASE
    WHEN COALESCE(ad.avg_daily_demand, 0) > 0
     AND (CAST(oh.on_hand_qty AS DECIMAL(10,2)) / ad.avg_daily_demand) < COALESCE(sl.avg_lead_time, 14)
    THEN true
    ELSE false
  END AS reorder_flag,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS updated_at
FROM on_hand oh
LEFT JOIN in_transit it ON it.warehouse_id = oh.warehouse_id AND it.sku = oh.sku
LEFT JOIN on_order oo ON oo.warehouse_id = oh.warehouse_id AND oo.sku = oh.sku
LEFT JOIN avg_demand ad ON ad.warehouse_id = oh.warehouse_id AND ad.sku = oh.sku
LEFT JOIN supplier_lead sl ON sl.sku = oh.sku;
