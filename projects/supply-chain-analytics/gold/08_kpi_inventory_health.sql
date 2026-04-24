-- =============================================================================
-- Gold: KPI: Inventory Health
-- =============================================================================
-- Per product x warehouse inventory health metrics:
--   - stockout_rate: % of days with zero on-hand (estimated)
--   - overstock_rate: % above 2x safety stock threshold
--   - avg_days_of_supply: on_hand / avg_daily_demand
--   - inventory_turnover: total_sold / avg_on_hand (annualized)
--   - health_status: critical / warning / healthy / overstock
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.kpi_inventory_health AS
WITH inventory_facts AS (
  SELECT
    fi.warehouse_id,
    fi.sku,
    fi.on_hand_qty,
    fi.in_transit_qty,
    fi.on_order_qty,
    fi.total_position,
    fi.days_of_supply,
    fi.avg_daily_demand,
    fi.avg_lead_time_days,
    fi.reorder_flag
  FROM sc.gold.fact_inventory fi
),
demand_totals AS (
  SELECT
    nearest_wh AS warehouse_id,
    sku,
    SUM(net_sold) AS total_sold,
    COUNT(DISTINCT sale_date) AS days_observed
  FROM sc.silver.demand_signals
  GROUP BY nearest_wh, sku
)
SELECT
  inf.warehouse_id,
  inf.sku,
  dp.product_name,
  dp.category AS product_category,
  inf.on_hand_qty,
  inf.avg_daily_demand,
  inf.days_of_supply,
  inf.avg_lead_time_days,
  -- Safety stock = avg_daily_demand * lead_time * 1.5
  CAST(inf.avg_daily_demand * inf.avg_lead_time_days * 1.5 AS DECIMAL(10,2)) AS safety_stock,
  -- Stockout rate estimate: if on_hand = 0, mark as stocked out
  CASE WHEN inf.on_hand_qty <= 0 THEN 1.0 ELSE 0.0 END AS stockout_flag,
  -- Overstock: on_hand > 2x safety stock
  CASE
    WHEN inf.avg_daily_demand > 0
     AND inf.on_hand_qty > (inf.avg_daily_demand * inf.avg_lead_time_days * 1.5 * 2)
    THEN true
    ELSE false
  END AS overstock_flag,
  -- Inventory turnover (annualized): (total_sold / days_observed) * 365 / on_hand
  CASE
    WHEN inf.on_hand_qty > 0 AND COALESCE(dt.days_observed, 0) > 0
    THEN CAST(
      (CAST(COALESCE(dt.total_sold, 0) AS DECIMAL(10,2)) / dt.days_observed * 365.0)
      / inf.on_hand_qty
    AS DECIMAL(10,2))
    ELSE 0
  END AS inventory_turnover,
  -- Health status
  CASE
    WHEN inf.on_hand_qty <= 0 THEN 'stockout'
    WHEN inf.days_of_supply IS NOT NULL AND inf.days_of_supply < inf.avg_lead_time_days THEN 'critical'
    WHEN inf.days_of_supply IS NOT NULL AND inf.days_of_supply < (inf.avg_lead_time_days * 2) THEN 'warning'
    WHEN inf.avg_daily_demand > 0
     AND inf.on_hand_qty > (inf.avg_daily_demand * inf.avg_lead_time_days * 1.5 * 2) THEN 'overstock'
    ELSE 'healthy'
  END AS health_status,
  inf.reorder_flag,
  CURRENT_TIMESTAMP AS updated_at
FROM inventory_facts inf
LEFT JOIN demand_totals dt ON dt.warehouse_id = inf.warehouse_id AND dt.sku = inf.sku
LEFT JOIN sc.gold.dim_product dp ON dp.sku = inf.sku;
