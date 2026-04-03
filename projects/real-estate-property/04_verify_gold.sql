-- =============================================================================
-- Real Estate Property Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== QUERY 1: Market Trends KPI Summary =====================

SELECT
    k.city,
    k.property_type,
    k.quarter,
    k.median_sale_price,
    k.avg_price_per_sqft,
    k.avg_days_on_market,
    k.total_transactions,
    k.over_asking_pct,
    k.inventory_months
FROM {{zone_prefix}}.gold.kpi_market_trends k
ORDER BY k.city, k.property_type, k.quarter;

-- ===================== QUERY 2: Star Schema Join — Full Transaction Detail =====================

ASSERT ROW_COUNT > 0
SELECT
    ft.transaction_key,
    dp.property_id,
    dp.address,
    dp.city,
    dp.property_type,
    dp.bedrooms,
    dp.sqft,
    db.name              AS buyer_name,
    db.buyer_type,
    da.name              AS agent_name,
    da.brokerage,
    ft.transaction_date,
    ft.list_price,
    ft.sale_price,
    ft.price_per_sqft,
    ft.days_on_market,
    ft.over_asking_pct
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp  ON ft.property_key = dp.surrogate_key
JOIN {{zone_prefix}}.gold.dim_buyer db     ON ft.buyer_key = db.buyer_key
JOIN {{zone_prefix}}.gold.dim_agent da     ON ft.agent_key = da.agent_key
ORDER BY ft.transaction_date;

-- ===================== QUERY 3: SCD2 Property Price History =====================
-- Show properties with multiple price versions (price reductions)

ASSERT VALUE transaction_key > 0
SELECT
    dp.property_id,
    dp.address,
    dp.city,
    dp.list_price,
    dp.valid_from,
    dp.valid_to,
    dp.is_current,
    COUNT(*) OVER (PARTITION BY dp.property_id)             AS total_versions
FROM {{zone_prefix}}.gold.dim_property dp
WHERE dp.property_id IN (
    SELECT property_id FROM {{zone_prefix}}.gold.dim_property
    GROUP BY property_id HAVING COUNT(*) > 1
)
ORDER BY dp.property_id, dp.valid_from;

-- ===================== QUERY 4: Agent Performance Scorecard =====================

ASSERT VALUE total_versions > 1
SELECT
    da.name                                                  AS agent_name,
    da.brokerage,
    da.years_experience,
    da.specialization,
    COUNT(*)                                                 AS total_deals,
    ROUND(SUM(ft.sale_price), 2)                            AS total_volume,
    ROUND(AVG(ft.sale_price), 2)                            AS avg_sale_price,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_dom,
    ROUND(AVG(ft.over_asking_pct), 2)                       AS avg_over_asking_pct,
    ROUND(AVG(ft.price_per_sqft), 2)                        AS avg_price_per_sqft
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_agent da ON ft.agent_key = da.agent_key
GROUP BY da.name, da.brokerage, da.years_experience, da.specialization
ORDER BY total_volume DESC;

-- ===================== QUERY 5: Neighborhood Valuation Comparison =====================

ASSERT ROW_COUNT = 6
SELECT
    dn.name                                                  AS neighborhood,
    dn.city,
    dn.median_income,
    dn.school_rating,
    dn.walkability_score,
    COUNT(ft.transaction_key)                                AS transactions,
    ROUND(AVG(ft.sale_price), 2)                            AS avg_sale_price,
    ROUND(AVG(ft.price_per_sqft), 2)                        AS avg_price_per_sqft,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_dom
FROM {{zone_prefix}}.gold.dim_neighborhood dn
LEFT JOIN {{zone_prefix}}.gold.dim_property dp
    ON dp.city = dn.city AND dp.is_current = true
LEFT JOIN {{zone_prefix}}.gold.fact_transactions ft
    ON ft.property_key = dp.surrogate_key
GROUP BY dn.name, dn.city, dn.median_income, dn.school_rating, dn.walkability_score
ORDER BY avg_sale_price DESC;

-- ===================== QUERY 6: Price Momentum using LAG =====================
-- Quarter-over-quarter median price change per city

ASSERT ROW_COUNT = 5
SELECT
    k.city,
    k.quarter,
    k.median_sale_price,
    LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.quarter) AS prev_quarter_price,
    ROUND(
        100.0 * (k.median_sale_price - LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.quarter))
        / NULLIF(LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.quarter), 0),
        2
    )                                                        AS price_momentum_pct
FROM {{zone_prefix}}.gold.kpi_market_trends k
WHERE k.property_type = 'Single Family'
ORDER BY k.city, k.quarter;

-- ===================== QUERY 7: Bidding War Analysis (Over-Asking Transactions) =====================

ASSERT VALUE median_sale_price > 0
SELECT
    dp.city,
    dp.property_type,
    ft.transaction_date,
    ft.list_price,
    ft.sale_price,
    ft.over_asking_pct,
    ft.days_on_market,
    CASE
        WHEN ft.over_asking_pct > 5 THEN 'Bidding War'
        WHEN ft.over_asking_pct > 0 THEN 'Over Asking'
        WHEN ft.over_asking_pct = 0 THEN 'At Asking'
        ELSE 'Below Asking'
    END                                                      AS price_category
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp ON ft.property_key = dp.surrogate_key
ORDER BY ft.over_asking_pct DESC;

-- ===================== QUERY 8: Buyer Type Analysis =====================

ASSERT VALUE sale_price > 0
SELECT
    db.buyer_type,
    db.pre_approved_flag,
    COUNT(*)                                                 AS transactions,
    ROUND(AVG(ft.sale_price), 2)                            AS avg_purchase_price,
    ROUND(AVG(ft.over_asking_pct), 2)                       AS avg_over_asking,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_dom_at_purchase
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_buyer db ON ft.buyer_key = db.buyer_key
GROUP BY db.buyer_type, db.pre_approved_flag
ORDER BY transactions DESC;

-- ===================== QUERY 9: Property Type Comparison =====================

ASSERT VALUE transactions > 0
SELECT
    dp.property_type,
    COUNT(*)                                                 AS total_sales,
    ROUND(AVG(dp.sqft), 0)                                 AS avg_sqft,
    ROUND(AVG(ft.sale_price), 2)                            AS avg_sale_price,
    ROUND(AVG(ft.price_per_sqft), 2)                        AS avg_price_per_sqft,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_dom,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.sale_price), 2) AS median_price
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp ON ft.property_key = dp.surrogate_key
GROUP BY dp.property_type
ORDER BY avg_sale_price DESC;

-- ===================== QUERY 10: Repeat Sales Analysis (Same Property Sold Multiple Times) =====================

ASSERT VALUE total_sales > 0
SELECT
    dp.property_id,
    dp.address,
    dp.city,
    ft.transaction_date,
    ft.sale_price,
    LAG(ft.sale_price) OVER (PARTITION BY dp.property_id ORDER BY ft.transaction_date) AS prev_sale_price,
    ROUND(
        100.0 * (ft.sale_price - LAG(ft.sale_price) OVER (PARTITION BY dp.property_id ORDER BY ft.transaction_date))
        / NULLIF(LAG(ft.sale_price) OVER (PARTITION BY dp.property_id ORDER BY ft.transaction_date), 0),
        2
    )                                                        AS appreciation_pct
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp ON ft.property_key = dp.surrogate_key
WHERE dp.property_id IN (
    SELECT dp2.property_id
    FROM {{zone_prefix}}.gold.fact_transactions ft2
    JOIN {{zone_prefix}}.gold.dim_property dp2 ON ft2.property_key = dp2.surrogate_key
    GROUP BY dp2.property_id
    HAVING COUNT(*) > 1
)
ORDER BY dp.property_id, ft.transaction_date;

ASSERT VALUE sale_price > 0
SELECT 'sale_price check passed' AS sale_price_status;

