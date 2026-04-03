-- =============================================================================
-- Retail POS Sales Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- Inserts new bronze rows (April 2024 batch), runs incremental MERGE, verifies.

-- Show current watermark
SELECT MAX(txn_id) AS max_txn_id, MAX(enriched_at) AS latest_enriched
FROM {{zone_prefix}}.silver.transactions_enriched;

-- Current row counts before incremental load
SELECT 'silver.transactions_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.transactions_enriched
UNION ALL
SELECT 'gold.fact_sales', COUNT(*)
FROM {{zone_prefix}}.gold.fact_sales;

-- =============================================================================
-- Insert 10 new bronze transaction rows (April 2024 incremental batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
(1076, 'RCP-20240401-048', 'STR-001', 'SKU-1001', 'EMP-101', '2024-04-01',  3,   4.99, 0.00, 'credit_card', 'BSK-048', 'Midwest',   '2024-04-01T08:00:00'),
(1077, 'RCP-20240401-048', 'STR-001', 'SKU-4002', 'EMP-101', '2024-04-01',  1,  14.99, 0.05, 'credit_card', 'BSK-048', 'Midwest',   '2024-04-01T08:00:00'),
(1078, 'RCP-20240403-049', 'STR-003', 'SKU-2001', 'EMP-104', '2024-04-03',  2,  49.99, 0.00, 'debit_card',  'BSK-049', 'Northeast', '2024-04-03T11:30:00'),
(1079, 'RCP-20240405-050', 'STR-005', 'SKU-3003', 'EMP-107', '2024-04-05',  1,  49.99, 0.10, 'mobile_pay',  'BSK-050', 'West',      '2024-04-05T10:15:00'),
(1080, 'RCP-20240405-050', 'STR-005', 'SKU-3001', 'EMP-107', '2024-04-05',  3,  14.99, 0.00, 'mobile_pay',  'BSK-050', 'West',      '2024-04-05T10:15:00'),
(1081, 'RCP-20240408-051', 'STR-002', 'SKU-4004', 'EMP-103', '2024-04-08',  2,  24.99, 0.00, 'cash',        'BSK-051', 'Midwest',   '2024-04-08T09:45:00'),
(1082, 'RCP-20240410-052', 'STR-004', 'SKU-2004', 'EMP-105', '2024-04-10',  1,  59.99, 0.05, 'credit_card', 'BSK-052', 'Northeast', '2024-04-10T09:00:00'),
(1083, 'RCP-20240412-053', 'STR-006', 'SKU-1004', 'EMP-108', '2024-04-12', -1,  16.99, 0.00, 'debit_card',  'BSK-053', 'West',      '2024-04-12T18:30:00'),
(1084, 'RCP-20240415-054', 'STR-001', 'SKU-2003', 'EMP-102', '2024-04-15',  2,  29.99, 0.00, 'credit_card', 'BSK-054', 'Midwest',   '2024-04-15T14:00:00'),
(1085, 'RCP-20240418-055', 'STR-005', 'SKU-4001', 'EMP-107', '2024-04-18',  1,  12.99, 0.00, 'mobile_pay',  'BSK-055', 'West',      '2024-04-18T11:00:00');

ASSERT ROW_COUNT = 10
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE to silver: only process rows newer than watermark
-- Uses INCREMENTAL_FILTER macro to dynamically determine the filter condition
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS tgt
USING (
    SELECT
        t.txn_id,
        t.receipt_id,
        t.store_id,
        t.sku,
        t.employee_id,
        t.txn_date,
        t.quantity,
        t.unit_price,
        t.discount_pct,
        CAST(t.quantity * t.unit_price * (1.0 - t.discount_pct) AS DECIMAL(12,2)) AS line_total,
        t.payment_method,
        t.basket_id,
        t.region,
        p.product_name,
        p.category,
        p.unit_cost,
        CASE WHEN t.quantity < 0 THEN true ELSE false END AS is_return,
        false AS anomaly_flag,
        t.ingested_at
    FROM {{zone_prefix}}.bronze.raw_transactions t
    LEFT JOIN {{zone_prefix}}.bronze.raw_products p ON t.sku = p.sku
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transactions_enriched, txn_id, txn_date, 3)}}
) AS src
ON tgt.txn_id = src.txn_id
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.is_return     = src.is_return,
    tgt.enriched_at   = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    txn_id, receipt_id, store_id, sku, employee_id, txn_date, quantity, unit_price,
    discount_pct, line_total, payment_method, basket_id, region, product_name,
    category, unit_cost, is_return, anomaly_flag, enriched_at
) VALUES (
    src.txn_id, src.receipt_id, src.store_id, src.sku, src.employee_id, src.txn_date,
    src.quantity, src.unit_price, src.discount_pct, src.line_total, src.payment_method,
    src.basket_id, src.region, src.product_name, src.category, src.unit_cost,
    src.is_return, src.anomaly_flag, src.ingested_at
);

-- =============================================================================
-- Incremental refresh of basket metrics
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.basket_metrics AS tgt
USING (
    SELECT
        basket_id,
        store_id,
        MIN(txn_date) AS txn_date,
        COUNT(*) AS items_in_basket,
        CAST(SUM(line_total) AS DECIMAL(12,2)) AS basket_value,
        MAX(CASE WHEN is_return THEN true ELSE false END) AS has_return,
        MAX(enriched_at) AS enriched_at
    FROM {{zone_prefix}}.silver.transactions_enriched
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.basket_metrics, basket_id, txn_date, 3)}}
    GROUP BY basket_id, store_id
) AS src
ON tgt.basket_id = src.basket_id
WHEN MATCHED THEN UPDATE SET
    tgt.items_in_basket = src.items_in_basket,
    tgt.basket_value    = src.basket_value,
    tgt.has_return      = src.has_return,
    tgt.enriched_at     = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    basket_id, store_id, txn_date, items_in_basket, basket_value, has_return, enriched_at
) VALUES (
    src.basket_id, src.store_id, src.txn_date, src.items_in_basket,
    src.basket_value, src.has_return, src.enriched_at
);

-- =============================================================================
-- Incremental refresh of gold fact_sales
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_sales AS tgt
USING (
    SELECT
        t.txn_id        AS sale_key,
        t.store_id      AS store_key,
        t.sku           AS product_key,
        t.employee_id   AS cashier_key,
        CAST(t.txn_date AS STRING) AS date_key,
        t.receipt_id,
        t.quantity,
        t.unit_price,
        t.discount_pct,
        t.line_total,
        t.payment_method,
        t.basket_id
    FROM {{zone_prefix}}.silver.transactions_enriched t
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_sales, sale_key, txn_date, 3)}}
) AS src
ON tgt.sale_key = src.sale_key
WHEN MATCHED THEN UPDATE SET
    tgt.quantity       = src.quantity,
    tgt.unit_price     = src.unit_price,
    tgt.discount_pct   = src.discount_pct,
    tgt.line_total     = src.line_total
WHEN NOT MATCHED THEN INSERT (
    sale_key, store_key, product_key, cashier_key, date_key, receipt_id,
    quantity, unit_price, discount_pct, line_total, payment_method, basket_id
) VALUES (
    src.sale_key, src.store_key, src.product_key, src.cashier_key, src.date_key,
    src.receipt_id, src.quantity, src.unit_price, src.discount_pct, src.line_total,
    src.payment_method, src.basket_id
);

-- =============================================================================
-- Incremental dim_date for new dates
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_date AS tgt
USING (
    SELECT DISTINCT
        CAST(txn_date AS STRING) AS date_key,
        txn_date AS full_date,
        CASE EXTRACT(DOW FROM txn_date)
            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(WEEK FROM txn_date) AS week_number,
        EXTRACT(MONTH FROM txn_date) AS month,
        EXTRACT(QUARTER FROM txn_date) AS quarter,
        EXTRACT(YEAR FROM txn_date) AS year,
        CASE WHEN EXTRACT(DOW FROM txn_date) IN (0, 6) THEN true ELSE false END AS is_weekend,
        false AS is_holiday
    FROM {{zone_prefix}}.silver.transactions_enriched
    WHERE txn_date >= '2024-04-01'
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day_of_week, week_number, month, quarter, year, is_weekend, is_holiday
) VALUES (
    src.date_key, src.full_date, src.day_of_week, src.week_number, src.month,
    src.quarter, src.year, src.is_weekend, src.is_holiday
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Silver should now have 75 + 10 = 85 rows
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.transactions_enriched;
-- Gold fact should also have 85 rows (returns included in fact)
ASSERT VALUE silver_total = 85
SELECT COUNT(*) AS gold_fact_total FROM {{zone_prefix}}.gold.fact_sales;
-- Verify April dates exist in dim_date
ASSERT VALUE gold_fact_total = 85
SELECT COUNT(*) AS april_dates
FROM {{zone_prefix}}.gold.dim_date
WHERE month = 4 AND year = 2024;

ASSERT VALUE april_dates > 0
SELECT 'april_dates check passed' AS april_dates_status;

