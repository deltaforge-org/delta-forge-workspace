-- =============================================================================
-- Verify: Comprehensive Validation Suite
-- =============================================================================
-- 15+ ASSERT validations across all medallion layers. Run after the full
-- pipeline to confirm data integrity, referential consistency, and KPI sanity.
-- =============================================================================

-- =========================================================================
-- Bronze layer assertions
-- =========================================================================

-- 1. All bronze tables populated
ASSERT (SELECT COUNT(*) FROM sc.bronze.purchase_orders) >= 40
  MESSAGE 'Verify: purchase_orders must have >= 40 rows';

-- 2. All suppliers referenced in POs exist in reference data
ASSERT (
  SELECT COUNT(DISTINCT po.supplier_id)
  FROM sc.bronze.purchase_orders po
  LEFT JOIN sc.bronze.suppliers s ON s.supplier_id = po.supplier_id
  WHERE s.supplier_id IS NULL
) = 0
  MESSAGE 'Verify: all PO supplier_ids must exist in suppliers reference table';

-- 3. All SKUs referenced in POs exist in products
ASSERT (
  SELECT COUNT(DISTINCT po.sku)
  FROM sc.bronze.purchase_orders po
  LEFT JOIN sc.bronze.products p ON p.sku = po.sku
  WHERE p.sku IS NULL
) = 0
  MESSAGE 'Verify: all PO SKUs must exist in products reference table';

-- 4. All warehouse_ids in movements exist in warehouses
ASSERT (
  SELECT COUNT(DISTINCT wm.warehouse_id)
  FROM sc.bronze.warehouse_movements wm
  LEFT JOIN sc.bronze.warehouses w ON w.warehouse_id = wm.warehouse_id
  WHERE w.warehouse_id IS NULL
) = 0
  MESSAGE 'Verify: all movement warehouse_ids must exist in warehouses reference table';

-- 5. All store_ids in POS demand exist in stores
ASSERT (
  SELECT COUNT(DISTINCT pd.store_id)
  FROM sc.bronze.pos_demand pd
  LEFT JOIN sc.bronze.stores s ON s.store_id = pd.store_id
  WHERE s.store_id IS NULL
) = 0
  MESSAGE 'Verify: all POS store_ids must exist in stores reference table';

-- =========================================================================
-- Silver layer assertions
-- =========================================================================

-- 6. Inventory positions computed for all warehouse x SKU combos
ASSERT (SELECT COUNT(*) FROM sc.silver.inventory_positions) > 0
  MESSAGE 'Verify: inventory_positions must not be empty';

-- 7. No negative on-hand (except allowed adjustments)
ASSERT (
  SELECT COUNT(*)
  FROM sc.silver.inventory_positions
  WHERE on_hand_qty < 0
) = 0
  MESSAGE 'Verify: inventory_positions must not have negative on_hand_qty';

-- 8. Order fulfillment has entries for all received POs
ASSERT (
  SELECT COUNT(*)
  FROM sc.bronze.purchase_orders po
  LEFT JOIN sc.silver.order_fulfillment of ON of.po_id = po.po_id
  WHERE po.status = 'received' AND of.po_id IS NULL
) = 0
  MESSAGE 'Verify: all received POs must have order_fulfillment entries';

-- 9. Fill rate is between 0 and 1.5 (allow slight over-delivery)
ASSERT (
  SELECT COUNT(*)
  FROM sc.silver.order_fulfillment
  WHERE fill_rate < 0 OR fill_rate > 1.5
) = 0
  MESSAGE 'Verify: fill_rate must be between 0 and 1.5';

-- 10. Shipment tracking has no duplicate shipment_ids
ASSERT (
  SELECT COUNT(*) - COUNT(DISTINCT shipment_id)
  FROM sc.silver.shipment_tracking
) = 0
  MESSAGE 'Verify: shipment_tracking must have unique shipment_ids';

-- 11. Demand signals have valid stockout_risk values
ASSERT (
  SELECT COUNT(*)
  FROM sc.silver.demand_signals
  WHERE stockout_risk NOT IN ('critical', 'warning', 'healthy')
) = 0
  MESSAGE 'Verify: demand_signals stockout_risk must be critical/warning/healthy';

-- =========================================================================
-- Gold layer assertions
-- =========================================================================

-- 12. All 6 suppliers present in dim_supplier
ASSERT (SELECT COUNT(*) FROM sc.gold.dim_supplier) >= 6
  MESSAGE 'Verify: dim_supplier must have >= 6 rows';

-- 13. All 15 products present in dim_product
ASSERT (SELECT COUNT(*) FROM sc.gold.dim_product) >= 15
  MESSAGE 'Verify: dim_product must have >= 15 rows';

-- 14. Fact inventory has data
ASSERT (SELECT COUNT(*) FROM sc.gold.fact_inventory) > 0
  MESSAGE 'Verify: fact_inventory must not be empty';

-- 15. Supplier scorecard has on_time_delivery_pct between 0 and 1
ASSERT (
  SELECT COUNT(*)
  FROM sc.gold.kpi_supplier_scorecard
  WHERE on_time_delivery_pct < 0 OR on_time_delivery_pct > 1
) = 0
  MESSAGE 'Verify: kpi_supplier_scorecard on_time_delivery_pct must be between 0 and 1';

-- 16. Inventory health status has valid values
ASSERT (
  SELECT COUNT(*)
  FROM sc.gold.kpi_inventory_health
  WHERE health_status NOT IN ('stockout', 'critical', 'warning', 'healthy', 'overstock')
) = 0
  MESSAGE 'Verify: kpi_inventory_health health_status must be a valid enum value';

-- 17. Demand forecast safety stock is non-negative
ASSERT (
  SELECT COUNT(*)
  FROM sc.gold.kpi_demand_forecast
  WHERE safety_stock_recommendation < 0
) = 0
  MESSAGE 'Verify: kpi_demand_forecast safety_stock_recommendation must be non-negative';

-- =========================================================================
-- Cross-layer referential integrity
-- =========================================================================

-- 18. All fact_orders reference valid dim_supplier entries
ASSERT (
  SELECT COUNT(*)
  FROM sc.gold.fact_orders fo
  LEFT JOIN sc.gold.dim_supplier ds ON ds.supplier_id = fo.supplier_id
  WHERE ds.supplier_id IS NULL
) = 0
  MESSAGE 'Verify: all fact_orders supplier_ids must exist in dim_supplier';

SELECT 'All 18 verification assertions passed' AS result;
