-- =============================================================================
-- E-Commerce Orders Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact table row count (non-deleted orders only)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_order_lines_count
FROM {{zone_prefix}}.gold.fact_order_lines;

-- -----------------------------------------------------------------------------
-- 2. Verify all 20 products loaded into dim_product
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_order_lines_count > 60
SELECT COUNT(*) AS product_count
FROM {{zone_prefix}}.gold.dim_product;

-- -----------------------------------------------------------------------------
-- 3. Verify all 15 customers loaded into dim_customer
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS customer_count
FROM {{zone_prefix}}.gold.dim_customer;

-- -----------------------------------------------------------------------------
-- 4. Revenue by channel - verify all 3 channels present
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 15
SELECT
    dc.channel_name,
    COUNT(DISTINCT f.order_key) AS orders,
    SUM(f.line_total)           AS total_revenue,
    CAST(AVG(f.line_total) AS DECIMAL(10,2)) AS avg_line_value
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
GROUP BY dc.channel_name
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 5. Top 5 products by revenue with profitability
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    dp.product_name,
    dp.category,
    dp.brand,
    SUM(f.quantity)    AS total_units_sold,
    SUM(f.line_total)  AS total_revenue,
    SUM(f.line_total) - SUM(f.quantity * dp.unit_cost) AS gross_profit,
    CAST(
        (SUM(f.line_total) - SUM(f.quantity * dp.unit_cost)) * 100.0 / NULLIF(SUM(f.line_total), 0)
    AS DECIMAL(5,2)) AS margin_pct
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product_name, dp.category, dp.brand
ORDER BY total_revenue DESC
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 6. Customer segmentation verification (RFM)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    rfm_segment,
    COUNT(*)                         AS customer_count,
    CAST(AVG(lifetime_orders) AS DECIMAL(5,1)) AS avg_lifetime_orders
FROM {{zone_prefix}}.gold.dim_customer
GROUP BY rfm_segment
ORDER BY customer_count DESC;

-- -----------------------------------------------------------------------------
-- 7. Monthly revenue trend with running total (window function)
-- -----------------------------------------------------------------------------
ASSERT VALUE customer_count > 0
SELECT
    DATE_TRUNC('month', f.order_date) AS order_month,
    COUNT(DISTINCT f.order_key)       AS monthly_orders,
    SUM(f.line_total)                 AS monthly_revenue,
    SUM(SUM(f.line_total)) OVER (ORDER BY DATE_TRUNC('month', f.order_date)) AS cumulative_revenue
FROM {{zone_prefix}}.gold.fact_order_lines f
GROUP BY DATE_TRUNC('month', f.order_date)
ORDER BY order_month;

-- -----------------------------------------------------------------------------
-- 8. Cancellation rate from KPI dashboard
-- -----------------------------------------------------------------------------
ASSERT VALUE monthly_orders > 0
SELECT
    channel,
    SUM(total_orders)        AS total_orders,
    CAST(AVG(cancellation_rate) AS DECIMAL(5,4)) AS avg_cancellation_rate,
    SUM(total_revenue)       AS total_revenue
FROM {{zone_prefix}}.gold.kpi_sales_dashboard
GROUP BY channel
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT VALUE total_orders > 0
SELECT COUNT(*) AS orphaned_customers
FROM {{zone_prefix}}.gold.fact_order_lines f
LEFT JOIN {{zone_prefix}}.gold.dim_customer dc ON f.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

ASSERT VALUE orphaned_customers = 0

SELECT COUNT(*) AS orphaned_products
FROM {{zone_prefix}}.gold.fact_order_lines f
LEFT JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. Soft-delete verification: cancelled orders NOT in fact table
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_products = 0
SELECT COUNT(*) AS deleted_in_fact
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.silver.orders_merged o ON f.order_line_key = o.order_line_id
WHERE o.is_deleted = true;

ASSERT VALUE deleted_in_fact = 0
SELECT 'deleted_in_fact check passed' AS deleted_in_fact_status;

