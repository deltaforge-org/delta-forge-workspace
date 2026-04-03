-- =============================================================================
-- Silver: Order Fulfillment
-- =============================================================================
-- Joins purchase orders with warehouse receipt movements to compute:
--   - fill_rate: qty_received / qty_ordered (how much of the PO was fulfilled)
--   - days_to_receive: actual receipt date minus order date
--   - on_time_flag: whether goods arrived by expected_date
-- Only processes POs with status 'received'.
-- =============================================================================

CREATE TABLE IF NOT EXISTS sc.silver.order_fulfillment (
  po_id             STRING,
  supplier_id       STRING,
  sku               STRING,
  qty_ordered       INT,
  qty_received      INT,
  fill_rate         DECIMAL(5,4),
  order_date        DATE,
  expected_date     DATE,
  received_date     DATE,
  days_to_receive   INT,
  on_time_flag      BOOLEAN,
  unit_cost         DECIMAL(10,2),
  total_cost        DECIMAL(12,2),
  updated_at        TIMESTAMP
);

MERGE INTO sc.silver.order_fulfillment AS tgt
USING (
  SELECT
    po.po_id,
    po.supplier_id,
    po.sku,
    po.qty_ordered,
    COALESCE(wm.qty_received, 0) AS qty_received,
    CAST(COALESCE(wm.qty_received, 0) AS DECIMAL(10,4)) / CAST(po.qty_ordered AS DECIMAL(10,4)) AS fill_rate,
    po.order_date,
    po.expected_date,
    po.received_date,
    DATEDIFF(po.received_date, po.order_date) AS days_to_receive,
    CASE WHEN po.received_date <= po.expected_date THEN true ELSE false END AS on_time_flag,
    po.unit_cost,
    po.qty_ordered * po.unit_cost AS total_cost
  FROM sc.bronze.purchase_orders po
  LEFT JOIN (
    SELECT
      reference_id,
      SUM(quantity) AS qty_received
    FROM sc.bronze.warehouse_movements
    WHERE movement_type = 'receipt'
      AND reference_id IS NOT NULL
    GROUP BY reference_id
  ) wm ON wm.reference_id = po.po_id
  WHERE po.status = 'received'
) AS src
ON tgt.po_id = src.po_id
WHEN MATCHED THEN UPDATE SET
  qty_received    = src.qty_received,
  fill_rate       = src.fill_rate,
  received_date   = src.received_date,
  days_to_receive = src.days_to_receive,
  on_time_flag    = src.on_time_flag,
  total_cost      = src.total_cost,
  updated_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  po_id, supplier_id, sku, qty_ordered, qty_received, fill_rate,
  order_date, expected_date, received_date, days_to_receive, on_time_flag,
  unit_cost, total_cost, updated_at
) VALUES (
  src.po_id, src.supplier_id, src.sku, src.qty_ordered, src.qty_received, src.fill_rate,
  src.order_date, src.expected_date, src.received_date, src.days_to_receive, src.on_time_flag,
  src.unit_cost, src.total_cost, CURRENT_TIMESTAMP
);
