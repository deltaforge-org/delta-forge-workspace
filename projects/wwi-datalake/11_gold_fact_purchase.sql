-- ============================================================================
-- Gold Fact: Purchases
-- ============================================================================
-- PO-line grain with fulfillment %, lead time, order/received totals.
-- ============================================================================

PIPELINE wwi_lake.gold_fact_purchase
    DESCRIPTION 'WWI gold - fact_purchase from silver purchase view'
    TAGS 'wwi', 'medallion', 'gold', 'fact'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_purchase (
    purchase_order_line_id INT NOT NULL, purchase_order_id INT NOT NULL,
    supplier_key INT NOT NULL, delivery_method_key INT,
    order_date_key INT NOT NULL, expected_delivery_date_key INT, last_receipt_date_key INT,
    stock_item_id INT NOT NULL, description VARCHAR,
    ordered_outers INT, received_outers INT, expected_unit_price_per_outer DECIMAL(18,2),
    is_order_finalized BOOLEAN, is_order_line_finalized BOOLEAN,
    order_total DECIMAL(18,2), received_total DECIMAL(18,2),
    fulfillment_pct DECIMAL(18,4), days_to_deliver INT
) LOCATION 'wwi/gold/fact_purchase';

MERGE INTO wwi_lake.gold.fact_purchase AS tgt
USING (
    SELECT purchase_order_line_id, purchase_order_id, supplier_key, delivery_method_key,
        order_date_key, expected_delivery_date_key, last_receipt_date_key,
        stock_item_id, description, ordered_outers, received_outers,
        expected_unit_price_per_outer, is_order_finalized, is_order_line_finalized,
        order_total, received_total, fulfillment_pct, days_to_deliver
    FROM wwi_lake.silver.v_purchase
) AS src ON tgt.purchase_order_line_id = src.purchase_order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
