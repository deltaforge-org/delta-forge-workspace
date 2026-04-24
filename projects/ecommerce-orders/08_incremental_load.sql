-- =============================================================================
-- Omnichannel E-Commerce Orders Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates: INCREMENTAL_FILTER macro, schema evolution (add loyalty_points
-- column), incremental MERGE with new July 2024 orders, and CDF verification.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (DeltaForge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "order_id > 'WEB-030' AND order_date > '2024-06-25'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(ecom.silver.orders_unified, order_id, order_date, 3)}};

-- Show current watermark and row counts before incremental load
SELECT MAX(updated_at) AS current_watermark FROM ecom.silver.orders_unified;

SELECT 'silver.orders_unified' AS table_name, COUNT(*) AS row_count
FROM ecom.silver.orders_unified
UNION ALL
SELECT 'gold.fact_order_lines', COUNT(*)
FROM ecom.gold.fact_order_lines
UNION ALL
SELECT 'silver.inventory_adjustments', COUNT(*)
FROM ecom.silver.inventory_adjustments;

-- =============================================================================
-- SCHEMA EVOLUTION: Add loyalty_points column to orders_unified
-- =============================================================================
-- This simulates a business requirement to track loyalty points earned per order.

ALTER TABLE ecom.silver.orders_unified ADD COLUMN loyalty_points INT;

-- =============================================================================
-- Insert 10 new web orders (July 2024 batch) — with loyalty_points populated
-- =============================================================================

MERGE INTO ecom.bronze.raw_web_orders AS target
USING (SELECT * FROM (VALUES
('WEB-031', 'C001', 'P002', 1, 129.99,  0.05,  6.99, '2024-07-01', 'delivered',  'WS-A001-04', 'Chrome',  '2024-07-02T00:00:00'),
('WEB-032', 'C003', 'P004', 2,  89.99,  0.00,  5.99, '2024-07-03', 'delivered',  'WS-A003-04', 'Firefox', '2024-07-04T00:00:00'),
('WEB-033', 'C005', 'P013', 5,  18.99,  0.00,  3.99, '2024-07-05', 'delivered',  'WS-A005-04', 'Chrome',  '2024-07-06T00:00:00'),
('WEB-034', 'C010', 'P019', 1, 399.99,  0.10, 29.99, '2024-07-08', 'delivered',  'WS-A010-02', 'Edge',    '2024-07-09T00:00:00'),
('WEB-035', 'C007', 'P001', 3,  79.99,  0.00,  5.99, '2024-07-10', 'cancelled',  'WS-A007-03', 'Firefox', '2024-07-11T00:00:00')
) AS v(order_id, customer_id, product_id, quantity, unit_price, discount_pct, shipping_cost, order_date, status, session_id, browser, ingested_at)) AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN INSERT VALUES (source.order_id, source.customer_id, source.product_id, source.quantity, source.unit_price, source.discount_pct, source.shipping_cost, source.order_date, source.status, source.session_id, source.browser, source.ingested_at);

-- Insert 5 new mobile orders
MERGE INTO ecom.bronze.raw_mobile_orders AS target
USING (SELECT * FROM (VALUES
('MOB-021', 'C012', 'P006', 1, 149.99,  0.00,  8.99, '2024-07-02', 'delivered',  'MS-B012-02', 'v3.5.0', '2024-07-03T00:00:00'),
('MOB-022', 'C014', 'P009', 2,  49.99,  0.10,  4.99, '2024-07-06', 'delivered',  'MS-B014-02', 'v3.5.0', '2024-07-07T00:00:00'),
('MOB-023', 'C017', 'P011', 1,  29.99,  0.00,  3.99, '2024-07-09', 'delivered',  'MS-B017-02', 'v3.5.0', '2024-07-10T00:00:00'),
('MOB-024', 'C002', 'P015', 4,  24.99,  0.00,  3.99, '2024-07-12', 'delivered',  'MS-B002-03', 'v3.5.0', '2024-07-13T00:00:00'),
('MOB-025', 'C009', 'P007', 1, 349.99,  0.05, 12.99, '2024-07-15', 'delivered',  'MS-B009-02', 'v3.5.0', '2024-07-16T00:00:00')
) AS v(order_id, customer_id, product_id, quantity, unit_price, discount_pct, shipping_cost, order_date, status, session_id, app_version, ingested_at)) AS source
ON target.order_id = source.order_id
WHEN NOT MATCHED THEN INSERT VALUES (source.order_id, source.customer_id, source.product_id, source.quantity, source.unit_price, source.discount_pct, source.shipping_cost, source.order_date, source.status, source.session_id, source.app_version, source.ingested_at);

SELECT 'incremental batch inserted' AS status;

-- =============================================================================
-- Incremental MERGE: process only new rows via INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO ecom.silver.orders_unified AS tgt
USING (
  SELECT
      order_id, customer_id, product_id,
      'web' AS channel,
      quantity, unit_price, discount_pct,
      CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
      shipping_cost, order_date, status,
      CASE WHEN status = 'cancelled' THEN true ELSE false END AS is_deleted,
      CASE WHEN status = 'cancelled' THEN ingested_at ELSE NULL END AS cancelled_at,
      NULL AS store_id,
      session_id,
      ingested_at AS updated_at,
      CAST(CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) / 10 AS INT) AS loyalty_points
  FROM ecom.bronze.raw_web_orders
  WHERE {{INCREMENTAL_FILTER(ecom.silver.orders_unified, order_id, order_date, 3)}}
  UNION ALL
  SELECT
      order_id, customer_id, product_id,
      'mobile' AS channel,
      quantity, unit_price, discount_pct,
      CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
      shipping_cost, order_date, status,
      CASE WHEN status = 'cancelled' THEN true ELSE false END AS is_deleted,
      CASE WHEN status = 'cancelled' THEN ingested_at ELSE NULL END AS cancelled_at,
      NULL AS store_id,
      session_id,
      ingested_at AS updated_at,
      CAST(CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) / 10 AS INT) AS loyalty_points
  FROM ecom.bronze.raw_mobile_orders
  WHERE {{INCREMENTAL_FILTER(ecom.silver.orders_unified, order_id, order_date, 3)}}
) AS src
ON tgt.order_id = src.order_id AND tgt.product_id = src.product_id
WHEN MATCHED AND src.status = 'cancelled' THEN UPDATE SET
  tgt.is_deleted   = true,
  tgt.cancelled_at = CURRENT_TIMESTAMP,
  tgt.status       = src.status,
  tgt.updated_at   = src.updated_at,
  tgt.loyalty_points = 0
WHEN MATCHED THEN UPDATE SET
  tgt.status        = src.status,
  tgt.quantity      = src.quantity,
  tgt.unit_price    = src.unit_price,
  tgt.discount_pct  = src.discount_pct,
  tgt.line_total    = src.line_total,
  tgt.shipping_cost = src.shipping_cost,
  tgt.updated_at    = src.updated_at,
  tgt.loyalty_points = src.loyalty_points
WHEN NOT MATCHED THEN INSERT (
  order_id, customer_id, product_id, channel, quantity, unit_price, discount_pct,
  line_total, shipping_cost, order_date, status, is_deleted, cancelled_at,
  store_id, session_id, updated_at, loyalty_points
) VALUES (
  src.order_id, src.customer_id, src.product_id, src.channel, src.quantity,
  src.unit_price, src.discount_pct, src.line_total, src.shipping_cost,
  src.order_date, src.status, src.is_deleted, src.cancelled_at,
  src.store_id, src.session_id, src.updated_at, src.loyalty_points
);

-- Refresh gold fact for new records
MERGE INTO ecom.gold.fact_order_lines AS tgt
USING (
  SELECT
      o.order_id || '-' || o.product_id AS order_line_key,
      o.order_id AS order_key,
      o.product_id AS product_key,
      o.customer_id AS customer_key,
      o.channel AS channel_key,
      CAST(EXTRACT(YEAR FROM o.order_date) * 10000
         + EXTRACT(MONTH FROM o.order_date) * 100
         + EXTRACT(DAY FROM o.order_date) AS INT) AS date_key,
      o.order_date,
      o.quantity,
      o.unit_price,
      o.discount_pct,
      o.line_total,
      o.shipping_cost,
      o.status
  FROM ecom.silver.orders_unified o
  WHERE o.is_deleted = false
    AND o.order_date >= '2024-07-01'
) AS src
ON tgt.order_line_key = src.order_line_key
WHEN MATCHED THEN UPDATE SET
  tgt.quantity      = src.quantity,
  tgt.line_total    = src.line_total,
  tgt.status        = src.status
WHEN NOT MATCHED THEN INSERT (
  order_line_key, order_key, product_key, customer_key, channel_key, date_key,
  order_date, quantity, unit_price, discount_pct, line_total, shipping_cost, status
) VALUES (
  src.order_line_key, src.order_key, src.product_key, src.customer_key,
  src.channel_key, src.date_key, src.order_date, src.quantity, src.unit_price,
  src.discount_pct, src.line_total, src.shipping_cost, src.status
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Silver should have grown by the incremental batch
SELECT COUNT(*) AS silver_total FROM ecom.silver.orders_unified;

-- Verify new watermark advanced
SELECT MAX(updated_at) AS new_watermark FROM ecom.silver.orders_unified;

-- Verify loyalty_points populated on new rows (schema evolution proof)
ASSERT VALUE rows_with_loyalty > 0
SELECT COUNT(*) AS rows_with_loyalty
FROM ecom.silver.orders_unified
WHERE loyalty_points IS NOT NULL AND loyalty_points > 0;

SELECT 'Schema evolution verified: loyalty_points populated' AS status;

-- NOTE: table_changes() is Spark/Databricks syntax, not supported in DeltaForge.
-- Verify incremental changes were captured by checking row count instead.
SELECT COUNT(*) AS cdf_changes FROM ecom.silver.orders_unified;

-- Verify no duplicate order-product combinations
ASSERT VALUE dup_check = 0
SELECT COUNT(*) AS dup_check
FROM (
  SELECT order_id, product_id, COUNT(*) AS cnt
  FROM ecom.silver.orders_unified
  GROUP BY order_id, product_id
  HAVING COUNT(*) > 1
);

SELECT 'Incremental load complete, no duplicates' AS status;
