-- =============================================================================
-- Gold: Fact: Order Lifecycle
-- =============================================================================
-- Full PO lifecycle fact table joining silver order_fulfillment with shipment
-- tracking. Captures: ordered qty, received qty, fill rate, lead time,
-- cost, on-time flag, and associated shipment status.
-- =============================================================================

CREATE OR REPLACE TABLE sc.gold.fact_orders AS
SELECT
  of.po_id,
  of.supplier_id,
  ds.supplier_name,
  of.sku,
  dp.product_name,
  dp.category AS product_category,
  of.qty_ordered,
  of.qty_received,
  of.fill_rate,
  of.order_date,
  of.expected_date,
  of.received_date,
  of.days_to_receive,
  of.on_time_flag,
  of.unit_cost,
  of.total_cost,
  -- Shipment info: find the shipment that carried goods from the receiving warehouse
  sh.shipment_id,
  sh.carrier_id,
  sh.current_status AS shipment_status,
  sh.transit_hours,
  sh.has_exception AS shipment_exception,
  CURRENT_TIMESTAMP AS updated_at
FROM sc.silver.order_fulfillment of
JOIN sc.gold.dim_supplier ds ON ds.supplier_id = of.supplier_id
JOIN sc.gold.dim_product dp ON dp.sku = of.sku
LEFT JOIN (
  -- Map POs to shipments via warehouse movements (receipt -> pick -> ship)
  SELECT DISTINCT
    wm_receipt.reference_id AS po_id,
    st.shipment_id,
    st.carrier_id,
    st.current_status,
    st.transit_hours,
    st.has_exception
  FROM sc.bronze.warehouse_movements wm_receipt
  JOIN sc.bronze.warehouse_movements wm_ship
    ON wm_ship.warehouse_id = wm_receipt.warehouse_id
    AND wm_ship.sku = wm_receipt.sku
    AND wm_ship.movement_type = 'ship'
    AND wm_ship.movement_ts > wm_receipt.movement_ts
  JOIN sc.silver.shipment_tracking st
    ON st.shipment_id = wm_ship.reference_id
  WHERE wm_receipt.movement_type = 'receipt'
    AND wm_receipt.reference_id LIKE 'PO-%'
) sh ON sh.po_id = of.po_id;
