-- =============================================================================
-- Silver: Inventory Positions
-- =============================================================================
-- Computes current on-hand inventory per warehouse x SKU by aggregating all
-- warehouse movements (receipts add, picks/ships subtract, adjustments apply).
-- Uses MERGE for idempotent updates: safe to re-run on each pipeline cycle.
-- =============================================================================

CREATE TABLE IF NOT EXISTS sc.silver.inventory_positions (
  warehouse_id    STRING,
  sku             STRING,
  on_hand_qty     INT,
  total_received  INT,
  total_picked    INT,
  total_shipped   INT,
  total_adjusted  INT,
  last_movement_ts TIMESTAMP,
  updated_at      TIMESTAMP
);

MERGE INTO sc.silver.inventory_positions AS tgt
USING (
  SELECT
    wm.warehouse_id,
    wm.sku,
    SUM(wm.quantity) AS on_hand_qty,
    SUM(CASE WHEN wm.movement_type = 'receipt' THEN wm.quantity ELSE 0 END) AS total_received,
    SUM(CASE WHEN wm.movement_type = 'pick' THEN ABS(wm.quantity) ELSE 0 END) AS total_picked,
    SUM(CASE WHEN wm.movement_type = 'ship' THEN ABS(wm.quantity) ELSE 0 END) AS total_shipped,
    SUM(CASE WHEN wm.movement_type IN ('cycle_count', 'adjustment') THEN wm.quantity ELSE 0 END) AS total_adjusted,
    MAX(wm.movement_ts) AS last_movement_ts
  FROM sc.bronze.warehouse_movements wm
  GROUP BY wm.warehouse_id, wm.sku
) AS src
ON tgt.warehouse_id = src.warehouse_id AND tgt.sku = src.sku
WHEN MATCHED THEN UPDATE SET
  on_hand_qty      = src.on_hand_qty,
  total_received   = src.total_received,
  total_picked     = src.total_picked,
  total_shipped    = src.total_shipped,
  total_adjusted   = src.total_adjusted,
  last_movement_ts = src.last_movement_ts,
  updated_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  warehouse_id, sku, on_hand_qty, total_received, total_picked,
  total_shipped, total_adjusted, last_movement_ts, updated_at
) VALUES (
  src.warehouse_id, src.sku, src.on_hand_qty, src.total_received, src.total_picked,
  src.total_shipped, src.total_adjusted, src.last_movement_ts, CURRENT_TIMESTAMP
);
