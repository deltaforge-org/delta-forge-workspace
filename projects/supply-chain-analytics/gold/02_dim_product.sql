-- =============================================================================
-- Gold: Dimension — Product
-- =============================================================================
-- Product dimension with category hierarchy, total inventory across all
-- warehouses, and total POS revenue to date.
-- =============================================================================

CREATE OR REPLACE TABLE {{zone_prefix}}_gold.analytics.dim_product AS
SELECT
  p.sku,
  p.product_name,
  p.category,
  p.subcategory,
  p.unit_weight_kg,
  p.unit_price,
  p.active,
  COALESCE(inv.total_on_hand, 0) AS total_on_hand,
  COALESCE(inv.warehouse_count, 0) AS stocked_warehouse_count,
  COALESCE(rev.total_revenue, 0) AS total_pos_revenue,
  COALESCE(rev.total_units_sold, 0) AS total_units_sold,
  CURRENT_TIMESTAMP AS updated_at
FROM {{zone_prefix}}_bronze.raw.products p
LEFT JOIN (
  SELECT
    sku,
    SUM(on_hand_qty) AS total_on_hand,
    COUNT(DISTINCT warehouse_id) AS warehouse_count
  FROM {{zone_prefix}}_silver.cleansed.inventory_positions
  GROUP BY sku
) inv ON inv.sku = p.sku
LEFT JOIN (
  SELECT
    sku,
    SUM(revenue) AS total_revenue,
    SUM(net_sold) AS total_units_sold
  FROM {{zone_prefix}}_silver.cleansed.demand_signals
  GROUP BY sku
) rev ON rev.sku = p.sku;
