-- =============================================================================
-- Silver: Validate Bronze Layer
-- =============================================================================
-- Pre-flight checks ensuring all bronze tables are populated and data quality
-- meets minimum thresholds before silver transformations begin.
-- =============================================================================

-- Verify purchase orders loaded
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.purchase_orders) >= 40
  MESSAGE 'Bronze purchase_orders must have at least 40 rows';

-- Verify warehouse movements loaded
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.warehouse_movements) >= 50
  MESSAGE 'Bronze warehouse_movements must have at least 50 rows';

-- Verify transport shipments loaded
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.transport_shipments) >= 35
  MESSAGE 'Bronze transport_shipments must have at least 35 rows';

-- Verify POS demand loaded
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.pos_demand) >= 45
  MESSAGE 'Bronze pos_demand must have at least 45 rows';

-- Verify reference data loaded
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.suppliers) >= 6
  MESSAGE 'Bronze suppliers must have at least 6 rows';

ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.warehouses) >= 4
  MESSAGE 'Bronze warehouses must have at least 4 rows';

ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.products) >= 15
  MESSAGE 'Bronze products must have at least 15 rows';

ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.stores) >= 8
  MESSAGE 'Bronze stores must have at least 8 rows';

-- Data quality: no NULL primary keys
ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.purchase_orders WHERE po_id IS NULL) = 0
  MESSAGE 'purchase_orders must not have NULL po_id';

ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.warehouse_movements WHERE movement_id IS NULL) = 0
  MESSAGE 'warehouse_movements must not have NULL movement_id';

ASSERT (SELECT COUNT(*) FROM {{zone_prefix}}_bronze.raw.transport_shipments WHERE event_id IS NULL) = 0
  MESSAGE 'transport_shipments must not have NULL event_id';

-- Data quality: valid movement types
ASSERT (
  SELECT COUNT(*)
  FROM {{zone_prefix}}_bronze.raw.warehouse_movements
  WHERE movement_type NOT IN ('receipt', 'pick', 'ship', 'cycle_count', 'adjustment')
) = 0
  MESSAGE 'warehouse_movements contains invalid movement_type values';

-- Data quality: positive quantities on POs
ASSERT (
  SELECT COUNT(*)
  FROM {{zone_prefix}}_bronze.raw.purchase_orders
  WHERE qty_ordered <= 0
) = 0
  MESSAGE 'purchase_orders must not have non-positive qty_ordered';
