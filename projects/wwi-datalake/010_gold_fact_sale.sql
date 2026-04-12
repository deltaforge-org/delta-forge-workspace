-- ============================================================================
-- WWI Data Lake - Gold Fact: Sales
-- ============================================================================
-- Materializes the sales fact at invoice-line grain from the silver v_sale
-- view. Each row represents one line item on an invoice, carrying financial
-- measures (line_amount, tax, profit, margin) and operational metrics
-- (days_to_fulfill, backorder flag).
--
-- MERGE on invoice_line_id (natural key, immutable once invoiced).
-- No NOT MATCHED BY SOURCE -- invoice lines are append-only, never deleted.
-- ============================================================================

PIPELINE wwi_lake.gold_fact_sale
    DESCRIPTION 'Materialize sales fact from silver v_sale view'
    SCHEDULE '0 6 * * *'
    TIMEZONE 'UTC'
    TAGS 'wwi', 'gold', 'fact', 'sales'
    SLA 1.5
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

MERGE INTO wwi_lake.gold.fact_sale AS tgt
USING (
    SELECT
        invoice_line_id,
        invoice_id,
        order_id,
        customer_key,
        bill_to_customer_key,
        salesperson_key,
        delivery_method_key,
        delivery_city_key,
        invoice_date_key,
        order_date_key,
        stock_item_id,
        description,
        quantity,
        unit_price,
        tax_rate,
        line_amount,
        tax_amount,
        total_including_tax,
        line_profit,
        profit_margin_pct,
        is_credit_note,
        is_backorder,
        days_to_fulfill,
        total_dry_items,
        total_chiller_items
    FROM wwi_lake.silver.v_sale
) AS src ON tgt.invoice_line_id = src.invoice_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
