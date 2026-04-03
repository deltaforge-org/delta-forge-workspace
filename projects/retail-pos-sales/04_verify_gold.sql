-- =============================================================================
-- Retail POS Sales Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_sales row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_sales_count
FROM {{zone_prefix}}.gold.fact_sales;

-- -----------------------------------------------------------------------------
-- 2. Verify all 6 stores in dim_store
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_sales_count >= 75
SELECT COUNT(*) AS store_count
FROM {{zone_prefix}}.gold.dim_store;

-- -----------------------------------------------------------------------------
-- 3. Verify all 20 products in dim_product
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS product_count
FROM {{zone_prefix}}.gold.dim_product;

-- -----------------------------------------------------------------------------
-- 4. Verify all 8 cashiers in dim_cashier
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS cashier_count
FROM {{zone_prefix}}.gold.dim_cashier;

-- -----------------------------------------------------------------------------
-- 5. Revenue by region with star schema join
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    ds.region,
    COUNT(DISTINCT ds.store_id)         AS stores,
    COUNT(DISTINCT f.receipt_id)        AS transactions,
    CAST(SUM(f.line_total) AS DECIMAL(14,2)) AS total_revenue,
    CAST(AVG(f.line_total) AS DECIMAL(10,2)) AS avg_line_value
FROM {{zone_prefix}}.gold.fact_sales f
JOIN {{zone_prefix}}.gold.dim_store ds ON f.store_key = ds.store_key
GROUP BY ds.region
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 6. Top 5 product categories by revenue with margin analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    dp.category,
    SUM(f.quantity)                      AS total_units,
    CAST(SUM(f.line_total) AS DECIMAL(14,2)) AS total_revenue,
    CAST(SUM(f.line_total) - SUM(f.quantity * dp.unit_cost) AS DECIMAL(14,2)) AS gross_profit,
    CAST(
        (SUM(f.line_total) - SUM(f.quantity * dp.unit_cost)) * 100.0
        / NULLIF(SUM(f.line_total), 0)
    AS DECIMAL(5,2)) AS margin_pct
FROM {{zone_prefix}}.gold.fact_sales f
JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 7. Monthly revenue trend with running total (window function)
-- -----------------------------------------------------------------------------
ASSERT VALUE margin_pct > 0 WHERE category = 'Electronics'
SELECT
    dd.year,
    dd.month,
    CAST(SUM(f.line_total) AS DECIMAL(14,2)) AS monthly_revenue,
    CAST(SUM(SUM(f.line_total)) OVER (ORDER BY dd.year, dd.month) AS DECIMAL(14,2)) AS cumulative_revenue,
    CAST(
        SUM(f.line_total) - LAG(SUM(f.line_total)) OVER (ORDER BY dd.year, dd.month)
    AS DECIMAL(14,2)) AS mom_change
FROM {{zone_prefix}}.gold.fact_sales f
JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- -----------------------------------------------------------------------------
-- 8. Cashier productivity metrics with ranking
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 3
SELECT
    dc.name AS cashier_name,
    dc.certification_level,
    COUNT(DISTINCT f.receipt_id)         AS transactions_handled,
    CAST(SUM(f.line_total) AS DECIMAL(14,2)) AS total_revenue,
    CAST(AVG(f.line_total) AS DECIMAL(10,2)) AS avg_line_value,
    DENSE_RANK() OVER (ORDER BY SUM(f.line_total) DESC) AS revenue_rank
FROM {{zone_prefix}}.gold.fact_sales f
JOIN {{zone_prefix}}.gold.dim_cashier dc ON f.cashier_key = dc.cashier_key
GROUP BY dc.name, dc.certification_level
ORDER BY revenue_rank;

-- -----------------------------------------------------------------------------
-- 9. Weekend vs weekday sales analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)                             AS line_items,
    COUNT(DISTINCT f.receipt_id)         AS transactions,
    CAST(SUM(f.line_total) AS DECIMAL(14,2)) AS total_revenue,
    CAST(AVG(f.line_total) AS DECIMAL(10,2)) AS avg_line_value
FROM {{zone_prefix}}.gold.fact_sales f
JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
GROUP BY CASE WHEN dd.is_weekend THEN 'Weekend' ELSE 'Weekday' END;

-- -----------------------------------------------------------------------------
-- 10. KPI store performance verification - regional ranking
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 2
SELECT
    store_id,
    region,
    month,
    total_revenue,
    total_transactions,
    avg_basket_value,
    gross_margin_pct,
    rank_in_region
FROM {{zone_prefix}}.gold.kpi_store_performance
WHERE rank_in_region = 1
ORDER BY month, region;

-- -----------------------------------------------------------------------------
-- Verification Summary
-- -----------------------------------------------------------------------------
ASSERT VALUE rank_in_region = 1
SELECT
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_sales) AS fact_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_store) AS dim_store_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_product) AS dim_product_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_date) AS dim_date_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_cashier) AS dim_cashier_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.kpi_store_performance) AS kpi_rows;
