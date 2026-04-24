-- =============================================================================
-- Real Estate Property Pipeline - Gold Layer Verification
-- =============================================================================
-- 12+ ASSERTs validating SCD2 integrity, star schema, assessment accuracy,
-- market trends, neighborhood benchmarking, and derived metrics.
-- =============================================================================

PIPELINE realty_verify_gold
  DESCRIPTION 'Gold layer verification for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'verification', 'real-estate-property'
  LIFECYCLE production
;


-- ===================== QUERY 1: SCD2 Property Assessment History =====================
-- Show properties with multiple assessment versions (value changes over time)

ASSERT ROW_COUNT >= 3
SELECT
  pd.parcel_id,
  pd.address,
  pd.city,
  pd.county,
  pd.assessed_value,
  pd.land_value,
  pd.improvement_value,
  pd.assessment_year,
  pd.valid_from,
  pd.valid_to,
  pd.is_current,
  COUNT(*) OVER (PARTITION BY pd.parcel_id)                      AS total_versions,
  -- YoY value change using LAG
  LAG(pd.assessed_value) OVER (
      PARTITION BY pd.parcel_id ORDER BY pd.assessment_year
  )                                                               AS prev_assessed_value,
  ROUND(
      100.0 * (pd.assessed_value - LAG(pd.assessed_value) OVER (
          PARTITION BY pd.parcel_id ORDER BY pd.assessment_year
      )) / NULLIF(LAG(pd.assessed_value) OVER (
          PARTITION BY pd.parcel_id ORDER BY pd.assessment_year
      ), 0),
      2
  )                                                               AS yoy_appreciation_pct
FROM realty.silver.property_dim pd
WHERE pd.parcel_id IN (
  SELECT parcel_id FROM realty.silver.property_dim
  GROUP BY parcel_id HAVING COUNT(*) > 1
)
ORDER BY pd.parcel_id, pd.assessment_year;

-- ===================== QUERY 2: SCD2 Integrity Check =====================
-- Verify: exactly 40 total rows, 18 with is_current = true

ASSERT ROW_COUNT = 40
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim WHERE is_current = true;

-- ===================== QUERY 3: Star Schema Join: Full Transaction Detail =====================

ASSERT ROW_COUNT >= 25
SELECT
  ft.transaction_key,
  ft.parcel_id,
  dn.neighborhood_name,
  dn.city,
  dn.county,
  da.agent_name,
  da.brokerage,
  dpt.property_type,
  ft.transaction_date,
  ft.sale_price,
  ft.assessed_value_at_sale,
  ft.price_per_sqft,
  ft.days_on_market,
  ft.over_asking_pct,
  ft.assessed_vs_sale_ratio,
  ft.assessment_outlier,
  ft.financing_type
FROM realty.gold.fact_transactions ft
JOIN realty.gold.dim_neighborhood dn    ON ft.neighborhood_key = dn.neighborhood_key
JOIN realty.gold.dim_agent da           ON ft.agent_key = da.agent_key
JOIN realty.gold.dim_property_type dpt  ON ft.property_type_key = dpt.property_type_key
ORDER BY ft.transaction_date;

-- ===================== QUERY 4: Assessment Accuracy KPI =====================

ASSERT ROW_COUNT > 0
SELECT
  k.county,
  k.property_type,
  k.assessment_year,
  k.total_sales,
  k.avg_assessed_value,
  k.avg_sale_price,
  k.avg_ratio,
  k.median_ratio,
  k.outlier_count,
  k.outlier_rate_pct,
  k.cod
FROM realty.gold.kpi_assessment_accuracy k
ORDER BY k.county, k.property_type, k.assessment_year;

-- ===================== QUERY 5: Assessment Outlier Detail =====================
-- Properties where |assessed_vs_sale_ratio - 1.0| > 0.20

ASSERT ROW_COUNT >= 1
SELECT
  ft.parcel_id,
  dn.neighborhood_name,
  ft.transaction_date,
  ft.assessed_value_at_sale,
  ft.sale_price,
  ft.assessed_vs_sale_ratio,
  ROUND(ABS(ft.assessed_vs_sale_ratio - 1.0), 2)                AS ratio_deviation,
  ft.assessment_outlier
FROM realty.gold.fact_transactions ft
JOIN realty.gold.dim_neighborhood dn ON ft.neighborhood_key = dn.neighborhood_key
WHERE ft.assessment_outlier = true
ORDER BY ABS(ft.assessed_vs_sale_ratio - 1.0) DESC;

-- ===================== QUERY 6: Market Trends KPI Summary =====================

ASSERT ROW_COUNT > 0
SELECT
  k.city,
  k.property_type,
  k.sale_quarter,
  k.median_sale_price,
  k.avg_price_per_sqft,
  k.avg_days_on_market,
  k.total_transactions,
  k.avg_over_asking_pct,
  k.inventory_months
FROM realty.gold.kpi_market_trends k
ORDER BY k.city, k.property_type, k.sale_quarter;

-- ===================== QUERY 7: Agent Performance Scorecard =====================

ASSERT ROW_COUNT = 8
SELECT
  da.agent_name,
  da.brokerage,
  da.county,
  da.years_experience,
  da.specialization,
  COUNT(*)                                                        AS total_deals,
  ROUND(SUM(ft.sale_price), 2)                                   AS total_volume,
  ROUND(AVG(ft.sale_price), 2)                                   AS avg_sale_price,
  ROUND(AVG(ft.days_on_market), 1)                               AS avg_dom,
  ROUND(AVG(ft.over_asking_pct), 2)                              AS avg_over_asking_pct,
  ROUND(AVG(ft.price_per_sqft), 2)                               AS avg_price_per_sqft
FROM realty.gold.fact_transactions ft
JOIN realty.gold.dim_agent da ON ft.agent_key = da.agent_key
GROUP BY da.agent_name, da.brokerage, da.county, da.years_experience, da.specialization
ORDER BY total_volume DESC;

-- ===================== QUERY 8: Neighborhood Valuation Benchmark =====================

ASSERT ROW_COUNT = 6
SELECT
  dn.neighborhood_name,
  dn.city,
  dn.county,
  dn.median_income,
  dn.school_rating,
  dn.walkability_score,
  COUNT(ft.transaction_key)                                       AS transactions,
  ROUND(AVG(ft.sale_price), 2)                                   AS avg_sale_price,
  ROUND(AVG(ft.assessed_value_at_sale), 2)                       AS avg_assessed_value,
  ROUND(AVG(ft.price_per_sqft), 2)                               AS avg_price_per_sqft,
  ROUND(AVG(ft.days_on_market), 1)                               AS avg_dom,
  ROUND(AVG(ft.assessed_vs_sale_ratio), 2)                       AS avg_assessment_ratio
FROM realty.gold.dim_neighborhood dn
LEFT JOIN realty.silver.property_dim pd
  ON pd.neighborhood_id = dn.neighborhood_id AND pd.is_current = true
LEFT JOIN realty.gold.fact_transactions ft
  ON ft.parcel_id = pd.parcel_id
GROUP BY dn.neighborhood_name, dn.city, dn.county, dn.median_income, dn.school_rating, dn.walkability_score
ORDER BY avg_sale_price DESC;

-- ===================== QUERY 9: Price Momentum using LAG =====================
-- Quarter-over-quarter median price change per city for Single Family homes

ASSERT VALUE median_sale_price > 0
SELECT
  k.city,
  k.sale_quarter,
  k.median_sale_price,
  LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.sale_quarter) AS prev_quarter_price,
  ROUND(
      100.0 * (k.median_sale_price - LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.sale_quarter))
      / NULLIF(LAG(k.median_sale_price) OVER (PARTITION BY k.city ORDER BY k.sale_quarter), 0),
      2
  )                                                               AS price_momentum_pct
FROM realty.gold.kpi_market_trends k
WHERE k.property_type = 'Single Family'
ORDER BY k.city, k.sale_quarter;

-- ===================== QUERY 10: Repeat Sales Analysis (Same Property Sold Multiple Times) =====================

ASSERT ROW_COUNT >= 1
SELECT
  ft.parcel_id,
  pd.address,
  pd.city,
  ft.transaction_date,
  ft.sale_price,
  LAG(ft.sale_price) OVER (PARTITION BY ft.parcel_id ORDER BY ft.transaction_date) AS prev_sale_price,
  ROUND(
      100.0 * (ft.sale_price - LAG(ft.sale_price) OVER (PARTITION BY ft.parcel_id ORDER BY ft.transaction_date))
      / NULLIF(LAG(ft.sale_price) OVER (PARTITION BY ft.parcel_id ORDER BY ft.transaction_date), 0),
      2
  )                                                               AS appreciation_pct
FROM realty.gold.fact_transactions ft
JOIN realty.silver.property_dim pd ON ft.parcel_id = pd.parcel_id AND pd.is_current = true
WHERE ft.parcel_id IN (
  SELECT parcel_id
  FROM realty.gold.fact_transactions
  GROUP BY parcel_id
  HAVING COUNT(*) > 1
)
ORDER BY ft.parcel_id, ft.transaction_date;

-- ===================== QUERY 11: Property Type Comparison =====================

ASSERT VALUE total_sales > 0
SELECT
  dpt.property_type,
  dpt.avg_sqft,
  dpt.avg_assessed_value,
  dpt.property_count,
  COUNT(*)                                                        AS total_sales,
  ROUND(AVG(ft.sale_price), 2)                                   AS avg_sale_price,
  ROUND(AVG(ft.price_per_sqft), 2)                               AS avg_price_per_sqft,
  ROUND(AVG(ft.days_on_market), 1)                               AS avg_dom,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.sale_price), 2) AS median_price
FROM realty.gold.fact_transactions ft
JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
GROUP BY dpt.property_type, dpt.avg_sqft, dpt.avg_assessed_value, dpt.property_count
ORDER BY avg_sale_price DESC;

-- ===================== QUERY 12: RESTORE Correction Log =====================

ASSERT ROW_COUNT >= 1
SELECT
  cl.correction_id,
  cl.table_name,
  cl.operation,
  cl.restored_to_version,
  cl.reason,
  cl.row_count_before,
  cl.row_count_after,
  cl.correction_timestamp
FROM realty.silver.correction_log cl
ORDER BY cl.correction_id;

-- ===================== FINAL: Summary assertions =====================

ASSERT ROW_COUNT >= 25
SELECT COUNT(*) AS row_count FROM realty.gold.fact_transactions;

ASSERT ROW_COUNT >= 6
SELECT COUNT(*) AS row_count FROM realty.gold.dim_neighborhood;
