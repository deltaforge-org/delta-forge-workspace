-- ============================================================================
-- WWI Data Lake - Gold Fact: Purchases
-- ============================================================================
-- Materializes the purchase fact at PO-line grain from silver v_purchase.
-- Each row is one line on a purchase order with order/received totals,
-- fulfillment percentage, and supplier lead time.
--
-- PO lines can be updated (received_outers increases over time), so MERGE
-- handles both inserts and updates. No source-side deletes expected.
-- ============================================================================

PIPELINE wwi_lake.gold_fact_purchase
    DESCRIPTION 'Materialize purchase fact from silver v_purchase view'
    SCHEDULE '0 6 * * *'
    TIMEZONE 'UTC'
    TAGS 'wwi', 'gold', 'fact', 'purchasing'
    SLA 1.0
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

MERGE INTO wwi_lake.gold.fact_purchase AS tgt
USING (
    SELECT
        purchase_order_line_id,
        purchase_order_id,
        supplier_key,
        delivery_method_key,
        order_date_key,
        expected_delivery_date_key,
        last_receipt_date_key,
        stock_item_id,
        description,
        ordered_outers,
        received_outers,
        expected_unit_price_per_outer,
        is_order_finalized,
        is_order_line_finalized,
        order_total,
        received_total,
        fulfillment_pct,
        days_to_deliver
    FROM wwi_lake.silver.v_purchase
) AS src ON tgt.purchase_order_line_id = src.purchase_order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
