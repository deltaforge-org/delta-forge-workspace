-- =============================================================================
-- Omnichannel E-Commerce Orders Pipeline: Gold Layer Verification
-- =============================================================================
-- 12 ASSERT validations across star schema integrity, RFM segmentation,
-- channel mix, funnel conversion, inventory CDF, and referential integrity.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact table has non-deleted orders only (no cancelled/soft-deleted)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_count
FROM {{zone_prefix}}.gold.fact_order_lines;

ASSERT VALUE fact_count > 60
SELECT 'Fact table row count validated' AS status;

-- -----------------------------------------------------------------------------
-- 2. Verify all 20 products loaded into dim_product
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS product_count
FROM {{zone_prefix}}.gold.dim_product;

ASSERT ROW_COUNT = 20
SELECT 'All 20 products present in dim_product' AS status;

-- -----------------------------------------------------------------------------
-- 3. Verify all 18 customers loaded into dim_customer with RFM segments
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS customer_count
FROM {{zone_prefix}}.gold.dim_customer;

ASSERT ROW_COUNT = 18
SELECT 'All 18 customers present in dim_customer' AS status;

-- -----------------------------------------------------------------------------
-- 4. Verify all 3 channels present in dim_channel (web, mobile, pos)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS channel_count
FROM {{zone_prefix}}.gold.dim_channel;

ASSERT ROW_COUNT = 3
SELECT 'All 3 channels present in dim_channel' AS status;

-- -----------------------------------------------------------------------------
-- 5. Revenue by channel — verify all 3 channels have revenue
-- -----------------------------------------------------------------------------
SELECT
    dc.channel_name,
    COUNT(DISTINCT f.order_key) AS orders,
    SUM(f.line_total) AS total_revenue,
    CAST(AVG(f.line_total) AS DECIMAL(10,2)) AS avg_line_value
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
GROUP BY dc.channel_name
ORDER BY total_revenue DESC;

ASSERT ROW_COUNT = 3
SELECT 'All 3 channels have revenue' AS status;

-- -----------------------------------------------------------------------------
-- 6. Top 5 products by revenue with margin analysis
-- -----------------------------------------------------------------------------
SELECT
    dp.product_name,
    dp.category,
    dp.brand,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.line_total) AS total_revenue,
    SUM(f.line_total) - SUM(f.quantity * dp.unit_cost) AS gross_profit,
    CAST(
        (SUM(f.line_total) - SUM(f.quantity * dp.unit_cost)) * 100.0 / NULLIF(SUM(f.line_total), 0)
    AS DECIMAL(5,2)) AS margin_pct
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product_name, dp.category, dp.brand
ORDER BY total_revenue DESC
LIMIT 5;

ASSERT ROW_COUNT = 5
SELECT 'Top 5 product profitability computed' AS status;

-- -----------------------------------------------------------------------------
-- 7. RFM customer segmentation — verify distribution
-- -----------------------------------------------------------------------------
SELECT
    rfm_segment,
    COUNT(*) AS customer_count,
    CAST(AVG(lifetime_orders) AS DECIMAL(5,1)) AS avg_lifetime_orders,
    CAST(AVG(lifetime_revenue) AS DECIMAL(10,2)) AS avg_lifetime_revenue
FROM {{zone_prefix}}.gold.dim_customer
GROUP BY rfm_segment
ORDER BY customer_count DESC;

ASSERT VALUE customer_count > 0
SELECT 'RFM segmentation distribution verified' AS status;

-- -----------------------------------------------------------------------------
-- 8. Monthly revenue trend with running total (window function)
-- -----------------------------------------------------------------------------
SELECT
    dd.month_name,
    dd.year,
    dd.month,
    COUNT(DISTINCT f.order_key) AS monthly_orders,
    SUM(f.line_total) AS monthly_revenue,
    SUM(SUM(f.line_total)) OVER (ORDER BY dd.year, dd.month) AS cumulative_revenue
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.month_name, dd.year, dd.month
ORDER BY dd.year, dd.month;

ASSERT VALUE monthly_orders > 0
SELECT 'Monthly trend with cumulative revenue validated' AS status;

-- -----------------------------------------------------------------------------
-- 9. Cancellation rate from KPI dashboard by channel
-- -----------------------------------------------------------------------------
SELECT
    channel,
    SUM(total_orders) AS total_orders,
    SUM(cancelled_orders) AS total_cancelled,
    CAST(AVG(cancellation_rate) AS DECIMAL(5,4)) AS avg_cancellation_rate,
    SUM(total_revenue) AS total_revenue,
    CAST(AVG(repeat_rate_pct) AS DECIMAL(5,2)) AS avg_repeat_rate
FROM {{zone_prefix}}.gold.kpi_sales_dashboard
GROUP BY channel
ORDER BY total_revenue DESC;

ASSERT VALUE total_orders > 0
SELECT 'KPI sales dashboard cancellation rates verified' AS status;

-- -----------------------------------------------------------------------------
-- 10. Funnel conversion rates from kpi_funnel_analysis
-- -----------------------------------------------------------------------------
SELECT
    report_month,
    total_sessions,
    browse_sessions,
    cart_sessions,
    checkout_sessions,
    purchase_sessions,
    browse_to_cart_pct,
    cart_to_checkout_pct,
    checkout_to_purchase_pct,
    overall_conversion_pct
FROM {{zone_prefix}}.gold.kpi_funnel_analysis
ORDER BY report_month;

ASSERT VALUE total_sessions > 0
SELECT 'Funnel conversion analysis validated' AS status;

-- -----------------------------------------------------------------------------
-- 11. Referential integrity: all fact FKs exist in dimensions
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS orphaned_customers
FROM {{zone_prefix}}.gold.fact_order_lines f
LEFT JOIN {{zone_prefix}}.gold.dim_customer dc ON f.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

ASSERT VALUE orphaned_customers = 0
SELECT COUNT(*) AS orphaned_products
FROM {{zone_prefix}}.gold.fact_order_lines f
LEFT JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
WHERE dp.product_key IS NULL;

ASSERT VALUE orphaned_products = 0
SELECT COUNT(*) AS orphaned_channels
FROM {{zone_prefix}}.gold.fact_order_lines f
LEFT JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
WHERE dc.channel_key IS NULL;

ASSERT VALUE orphaned_channels = 0
SELECT 'Referential integrity: zero orphans across all dimensions' AS status;

-- -----------------------------------------------------------------------------
-- 12. Soft-delete verification: cancelled orders NOT in fact table
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS deleted_in_fact
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.silver.orders_unified o
    ON f.order_key = o.order_id AND f.product_key = o.product_id
WHERE o.is_deleted = true;

ASSERT VALUE deleted_in_fact = 0
SELECT 'Soft-delete verification: no cancelled orders in fact table' AS status;

-- -----------------------------------------------------------------------------
-- 13. Inventory adjustments CDF verification
-- -----------------------------------------------------------------------------
SELECT
    change_type,
    COUNT(*) AS adjustment_count,
    SUM(quantity_delta) AS total_quantity_delta
FROM {{zone_prefix}}.silver.inventory_adjustments
GROUP BY change_type
ORDER BY adjustment_count DESC;

ASSERT VALUE adjustment_count > 0
SELECT 'Inventory CDF adjustments verified' AS status;

-- -----------------------------------------------------------------------------
-- 14. Category revenue breakdown with rank
-- -----------------------------------------------------------------------------
SELECT
    dp.category,
    SUM(f.line_total) AS category_revenue,
    RANK() OVER (ORDER BY SUM(f.line_total) DESC) AS revenue_rank,
    CAST(SUM(f.line_total) * 100.0 / SUM(SUM(f.line_total)) OVER () AS DECIMAL(5,2)) AS pct_of_total
FROM {{zone_prefix}}.gold.fact_order_lines f
JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.category
ORDER BY category_revenue DESC;

ASSERT VALUE category_revenue > 0
SELECT 'Gold layer verification COMPLETE' AS final_status;
