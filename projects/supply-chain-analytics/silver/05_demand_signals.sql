-- =============================================================================
-- Silver: Demand Signals
-- =============================================================================
-- Enriches POS demand data with:
--   - 7-day moving average demand per store x SKU
--   - net_sold (qty_sold - qty_returned)
--   - stockout_risk_score: flags SKUs where rolling demand exceeds 80% of
--     current on-hand inventory at the nearest warehouse
-- =============================================================================

CREATE TABLE IF NOT EXISTS sc.silver.demand_signals (
  store_id          STRING,
  sku               STRING,
  sale_date         DATE,
  qty_sold          INT,
  qty_returned      INT,
  net_sold          INT,
  revenue           DECIMAL(10,2),
  demand_7d_avg     DECIMAL(10,2),
  nearest_wh        STRING,
  current_on_hand   INT,
  stockout_risk     STRING,
  updated_at        TIMESTAMP
);

MERGE INTO sc.silver.demand_signals AS tgt
USING (
  WITH daily_demand AS (
    SELECT
      pd.store_id,
      pd.sku,
      pd.sale_date,
      pd.qty_sold,
      pd.qty_returned,
      pd.qty_sold - pd.qty_returned AS net_sold,
      pd.revenue,
      AVG(pd.qty_sold - pd.qty_returned) OVER (
        PARTITION BY pd.store_id, pd.sku
        ORDER BY pd.sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS demand_7d_avg
    FROM sc.bronze.pos_demand pd
  ),
  store_warehouse AS (
    -- Map each store to its nearest warehouse by region
    SELECT s.store_id, w.warehouse_id AS nearest_wh
    FROM sc.bronze.stores s
    JOIN sc.bronze.warehouses w ON s.region = w.region
  ),
  inventory AS (
    SELECT warehouse_id, sku, on_hand_qty
    FROM sc.silver.inventory_positions
  )
  SELECT
    dd.store_id,
    dd.sku,
    dd.sale_date,
    dd.qty_sold,
    dd.qty_returned,
    dd.net_sold,
    dd.revenue,
    dd.demand_7d_avg,
    sw.nearest_wh,
    COALESCE(inv.on_hand_qty, 0) AS current_on_hand,
    CASE
      WHEN COALESCE(inv.on_hand_qty, 0) = 0 THEN 'critical'
      WHEN dd.demand_7d_avg > COALESCE(inv.on_hand_qty, 0) * 0.8 THEN 'warning'
      ELSE 'healthy'
    END AS stockout_risk
  FROM daily_demand dd
  LEFT JOIN store_warehouse sw ON sw.store_id = dd.store_id
  LEFT JOIN inventory inv ON inv.warehouse_id = sw.nearest_wh AND inv.sku = dd.sku
) AS src
ON tgt.store_id = src.store_id AND tgt.sku = src.sku AND tgt.sale_date = src.sale_date
WHEN MATCHED THEN UPDATE SET
  qty_sold        = src.qty_sold,
  qty_returned    = src.qty_returned,
  net_sold        = src.net_sold,
  revenue         = src.revenue,
  demand_7d_avg   = src.demand_7d_avg,
  nearest_wh      = src.nearest_wh,
  current_on_hand = src.current_on_hand,
  stockout_risk   = src.stockout_risk,
  updated_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  store_id, sku, sale_date, qty_sold, qty_returned, net_sold, revenue,
  demand_7d_avg, nearest_wh, current_on_hand, stockout_risk, updated_at
) VALUES (
  src.store_id, src.sku, src.sale_date, src.qty_sold, src.qty_returned, src.net_sold, src.revenue,
  src.demand_7d_avg, src.nearest_wh, src.current_on_hand, src.stockout_risk, CURRENT_TIMESTAMP
);
