-- ============================================================================
-- Gold Fact: Sales
-- ============================================================================
-- Invoice-line grain with profit margin, fulfillment days, backorder flag.
-- ============================================================================

PIPELINE wwi_lake.gold_fact_sale
    DESCRIPTION 'WWI gold - fact_sale from silver sale view'
    TAGS 'wwi', 'medallion', 'gold', 'fact'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_sale (
    invoice_line_id INT NOT NULL, invoice_id INT NOT NULL, order_id INT,
    customer_key INT NOT NULL, bill_to_customer_key INT, salesperson_key INT,
    delivery_method_key INT, delivery_city_key INT,
    invoice_date_key INT NOT NULL, order_date_key INT,
    stock_item_id INT NOT NULL, description VARCHAR,
    quantity INT NOT NULL, unit_price DECIMAL(18,2) NOT NULL, tax_rate DECIMAL(18,3) NOT NULL,
    line_amount DECIMAL(18,2) NOT NULL, tax_amount DECIMAL(18,2) NOT NULL,
    total_including_tax DECIMAL(18,2) NOT NULL, line_profit DECIMAL(18,2) NOT NULL,
    profit_margin_pct DECIMAL(18,4), is_credit_note BOOLEAN, is_backorder BOOLEAN,
    days_to_fulfill INT, total_dry_items INT, total_chiller_items INT
) LOCATION 'wwi/gold/fact_sale';

MERGE INTO wwi_lake.gold.fact_sale AS tgt
USING (
    SELECT invoice_line_id, invoice_id, order_id, customer_key, bill_to_customer_key,
        salesperson_key, delivery_method_key, delivery_city_key,
        invoice_date_key, order_date_key, stock_item_id, description,
        quantity, unit_price, tax_rate, line_amount, tax_amount,
        total_including_tax, line_profit, profit_margin_pct,
        is_credit_note, is_backorder, days_to_fulfill,
        total_dry_items, total_chiller_items
    FROM wwi_lake.silver.v_sale
) AS src ON tgt.invoice_line_id = src.invoice_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
