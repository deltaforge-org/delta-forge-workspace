-- =============================================================================
-- E-Commerce Orders Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing: insert new bronze rows, run MERGE,
-- verify only new records are processed.

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "order_line_id > 'OL-0072' AND order_date > '2024-06-28'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.orders_merged, order_line_id, order_date, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.orders_merged
-- SELECT * FROM {{zone_prefix}}.bronze.raw_orders
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.orders_merged, order_line_id, order_date, 3)}};

-- Show current watermark (latest merged_at in silver)
SELECT MAX(merged_at) AS current_watermark
FROM {{zone_prefix}}.silver.orders_merged;

-- Show current row counts before incremental load
SELECT 'silver.orders_merged' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.orders_merged
UNION ALL
SELECT 'gold.fact_order_lines', COUNT(*)
FROM {{zone_prefix}}.gold.fact_order_lines;

-- =============================================================================
-- Insert 8 new bronze order lines (incremental batch - July 2024)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_orders VALUES
('ORD-0072', 'OL-0073', 'C001', 'P002', 'web',         'Desktop',  'US-East',   '2024-07-01', 1, 129.99,  0.00,  6.99, 'delivered',  false, 'web-feed',    '2024-07-02T00:00:00'),
('ORD-0073', 'OL-0074', 'C005', 'P011', 'mobile',      'iOS',      'US-East',   '2024-07-03', 2,  29.99,  0.10,  3.99, 'delivered',  false, 'mobile-feed', '2024-07-04T00:00:00'),
('ORD-0074', 'OL-0075', 'C003', 'P008', 'marketplace', 'Amazon',   'US-Central','2024-07-05', 1, 199.99,  0.00,  9.99, 'shipped',    false, 'mkt-feed',    '2024-07-06T00:00:00'),
('ORD-0075', 'OL-0076', 'C010', 'P004', 'web',         'Desktop',  'US-West',   '2024-07-07', 1,  89.99,  0.05,  5.99, 'delivered',  false, 'web-feed',    '2024-07-08T00:00:00'),
('ORD-0076', 'OL-0077', 'C007', 'P019', 'mobile',      'Android',  'US-West',   '2024-07-09', 1,  89.99,  0.00,  5.99, 'cancelled',  true,  'mobile-feed', '2024-07-10T00:00:00'),
('ORD-0077', 'OL-0078', 'C012', 'P006', 'marketplace', 'Amazon',   'US-Central','2024-07-11', 1, 149.99,  0.10,  8.99, 'delivered',  false, 'mkt-feed',    '2024-07-12T00:00:00'),
('ORD-0078', 'OL-0079', 'C014', 'P015', 'web',         'Desktop',  'US-East',   '2024-07-13', 1, 149.99,  0.00,  6.99, 'delivered',  false, 'web-feed',    '2024-07-14T00:00:00'),
('ORD-0079', 'OL-0080', 'C009', 'P003', 'mobile',      'iOS',      'US-East',   '2024-07-15', 3,  39.99,  0.00,  3.99, 'delivered',  false, 'mobile-feed', '2024-07-16T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only process rows newer than watermark
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.orders_merged AS tgt
USING (
    SELECT
        order_id,
        order_line_id,
        customer_id,
        product_id,
        channel,
        platform,
        region,
        order_date,
        quantity,
        unit_price,
        discount_pct,
        CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
        shipping_cost,
        order_status,
        CASE WHEN order_status = 'cancelled' THEN true ELSE is_deleted END AS is_deleted,
        ingested_at
    FROM {{zone_prefix}}.bronze.raw_orders
    WHERE ingested_at > (SELECT COALESCE(MAX(merged_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.orders_merged)
) AS src
ON tgt.order_line_id = src.order_line_id
WHEN MATCHED AND src.ingested_at > tgt.merged_at THEN UPDATE SET
    tgt.order_status = src.order_status,
    tgt.is_deleted   = src.is_deleted,
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.shipping_cost = src.shipping_cost,
    tgt.merged_at     = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    order_id, order_line_id, customer_id, product_id, channel, platform, region,
    order_date, quantity, unit_price, discount_pct, line_total, shipping_cost,
    order_status, is_deleted, merged_at
) VALUES (
    src.order_id, src.order_line_id, src.customer_id, src.product_id, src.channel,
    src.platform, src.region, src.order_date, src.quantity, src.unit_price,
    src.discount_pct, src.line_total, src.shipping_cost, src.order_status,
    src.is_deleted, src.ingested_at
);

-- Refresh gold fact for new records (only non-deleted)
MERGE INTO {{zone_prefix}}.gold.fact_order_lines AS tgt
USING (
    SELECT
        o.order_line_id                                        AS order_line_key,
        o.order_id                                             AS order_key,
        o.product_id                                           AS product_key,
        o.customer_id                                          AS customer_key,
        o.channel || '-' || o.platform || '-' || o.region      AS channel_key,
        o.order_date,
        o.quantity,
        o.unit_price,
        o.discount_pct,
        o.line_total,
        o.shipping_cost
    FROM {{zone_prefix}}.silver.orders_merged o
    WHERE o.is_deleted = false
      AND o.order_date >= '2024-07-01'
) AS src
ON tgt.order_line_key = src.order_line_key
WHEN MATCHED THEN UPDATE SET
    tgt.quantity      = src.quantity,
    tgt.line_total    = src.line_total
WHEN NOT MATCHED THEN INSERT (
    order_line_key, order_key, product_key, customer_key, channel_key,
    order_date, quantity, unit_price, discount_pct, line_total, shipping_cost
) VALUES (
    src.order_line_key, src.order_key, src.product_key, src.customer_key,
    src.channel_key, src.order_date, src.quantity, src.unit_price,
    src.discount_pct, src.line_total, src.shipping_cost
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Silver should now have 72 + 8 = 80 rows total
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.orders_merged;
-- Verify new watermark advanced
ASSERT ROW_COUNT = 80
SELECT MAX(merged_at) AS new_watermark
FROM {{zone_prefix}}.silver.orders_merged;

-- Verify CDF captured the incremental changes
SELECT COUNT(*) AS cdf_changes
FROM table_changes('{{zone_prefix}}.silver.orders_merged', 1);
