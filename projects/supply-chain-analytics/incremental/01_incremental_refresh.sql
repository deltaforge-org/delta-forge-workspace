-- =============================================================================
-- Incremental: Delta Processing
-- =============================================================================
-- Uses INCREMENTAL_FILTER to process only new/changed records since the last
-- pipeline run. This avoids full-table scans on large bronze tables by
-- filtering on ingested_at timestamps.
-- =============================================================================

-- Incremental purchase orders: only process rows ingested after last watermark
CREATE OR REPLACE TEMPORARY VIEW incremental_purchase_orders AS
SELECT *
FROM {{zone_prefix}}.bronze.purchase_orders
WHERE INCREMENTAL_FILTER(ingested_at);

-- Incremental warehouse movements
CREATE OR REPLACE TEMPORARY VIEW incremental_warehouse_movements AS
SELECT *
FROM {{zone_prefix}}.bronze.warehouse_movements
WHERE INCREMENTAL_FILTER(ingested_at);

-- Incremental transport shipments
CREATE OR REPLACE TEMPORARY VIEW incremental_transport_shipments AS
SELECT *
FROM {{zone_prefix}}.bronze.transport_shipments
WHERE INCREMENTAL_FILTER(ingested_at);

-- Incremental POS demand
CREATE OR REPLACE TEMPORARY VIEW incremental_pos_demand AS
SELECT *
FROM {{zone_prefix}}.bronze.pos_demand
WHERE INCREMENTAL_FILTER(ingested_at);

-- Refresh inventory positions from incremental movements only
MERGE INTO {{zone_prefix}}.silver.inventory_positions AS tgt
USING (
  SELECT
    warehouse_id,
    sku,
    SUM(quantity) AS delta_qty,
    SUM(CASE WHEN movement_type = 'receipt' THEN quantity ELSE 0 END) AS delta_received,
    SUM(CASE WHEN movement_type = 'pick' THEN ABS(quantity) ELSE 0 END) AS delta_picked,
    SUM(CASE WHEN movement_type = 'ship' THEN ABS(quantity) ELSE 0 END) AS delta_shipped,
    SUM(CASE WHEN movement_type IN ('cycle_count', 'adjustment') THEN quantity ELSE 0 END) AS delta_adjusted,
    MAX(movement_ts) AS last_movement_ts
  FROM incremental_warehouse_movements
  GROUP BY warehouse_id, sku
) AS src
ON tgt.warehouse_id = src.warehouse_id AND tgt.sku = src.sku
WHEN MATCHED THEN UPDATE SET
  on_hand_qty      = tgt.on_hand_qty + src.delta_qty,
  total_received   = tgt.total_received + src.delta_received,
  total_picked     = tgt.total_picked + src.delta_picked,
  total_shipped    = tgt.total_shipped + src.delta_shipped,
  total_adjusted   = tgt.total_adjusted + src.delta_adjusted,
  last_movement_ts = GREATEST(tgt.last_movement_ts, src.last_movement_ts),
  updated_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  warehouse_id, sku, on_hand_qty, total_received, total_picked,
  total_shipped, total_adjusted, last_movement_ts, updated_at
) VALUES (
  src.warehouse_id, src.sku, src.delta_qty, src.delta_received, src.delta_picked,
  src.delta_shipped, src.delta_adjusted, src.last_movement_ts, CURRENT_TIMESTAMP
);

-- Refresh order fulfillment from incremental POs
MERGE INTO {{zone_prefix}}.silver.order_fulfillment AS tgt
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
  FROM incremental_purchase_orders po
  LEFT JOIN (
    SELECT reference_id, SUM(quantity) AS qty_received
    FROM {{zone_prefix}}.bronze.warehouse_movements
    WHERE movement_type = 'receipt' AND reference_id IS NOT NULL
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
