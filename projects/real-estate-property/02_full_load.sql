-- =============================================================================
-- Real Estate Property Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- 14-step DAG: validate_bronze -> load_scd2_batch1 + load_transactions (parallel) ->
-- load_scd2_batch2 -> load_scd2_batch3 -> enrich_transactions_point_in_time ->
-- dim_neighborhood + dim_agent + dim_property_type (parallel) ->
-- build_fact_transactions -> kpi_market_trends + kpi_assessment_accuracy (parallel) ->
-- restore_correction_demo -> bloom_and_optimize
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE realty_daily_schedule
  CRON '0 9 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 2
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE realty_property_pipeline
  DESCRIPTION 'Daily property assessment pipeline: SCD2 with 3 annual assessment batches, RESTORE correction, point-in-time transaction enrichment, assessment accuracy analysis, market trends'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'real-estate,property,SCD2,RESTORE,assessment'
  SLA 2700
  FAIL_FAST true
  LIFECYCLE production
;
-- ===================== validate_bronze =====================

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_properties;

ASSERT ROW_COUNT = 40
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_assessments;

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_transactions;

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_neighborhoods;

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_agents;

-- ===================== load_scd2_batch1 =====================
-- Batch 1: 2022 assessments for all 18 properties.
-- Initial load: all records start as is_current = true.

INSERT INTO realty.silver.property_dim
SELECT
    ROW_NUMBER() OVER (ORDER BY p.parcel_id)                     AS surrogate_key,
    p.parcel_id,
    p.address,
    p.city,
    p.county,
    p.state,
    p.zip,
    p.neighborhood_id,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.sqft,
    p.lot_acres,
    p.year_built,
    a.assessed_value,
    a.land_value,
    a.improvement_value,
    a.assessment_year,
    CAST(a.assessment_date AS DATE)                              AS valid_from,
    CAST(NULL AS DATE)                                           AS valid_to,
    true                                                          AS is_current
FROM realty.bronze.raw_properties p
JOIN realty.bronze.raw_assessments a
    ON p.parcel_id = a.parcel_id AND a.assessment_year = 2022;

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

-- ===================== load_transactions (parallel with batch1) =====================
-- Raw transaction load into silver happens after batch1 and will be enriched later.

-- Placeholder: transactions will be enriched after all SCD2 batches are loaded.
-- We verify the bronze data is ready.
ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_transactions;

-- ===================== load_scd2_batch2 =====================
-- Batch 2: 2023 assessments for 12 properties. Expire old records, insert new.

-- Step A: Expire current records for properties being reassessed in 2023
MERGE INTO realty.silver.property_dim AS tgt
USING (
    SELECT DISTINCT a.parcel_id, CAST(a.assessment_date AS DATE) AS new_valid_from
    FROM realty.bronze.raw_assessments a
    WHERE a.assessment_year = 2023
) AS src
ON tgt.parcel_id = src.parcel_id AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
    valid_to = src.new_valid_from,
    is_current = false;

-- Step B: Insert new 2023 assessment records as current
INSERT INTO realty.silver.property_dim
SELECT
    (SELECT COALESCE(MAX(surrogate_key), 0) FROM realty.silver.property_dim)
        + ROW_NUMBER() OVER (ORDER BY p.parcel_id)               AS surrogate_key,
    p.parcel_id,
    p.address,
    p.city,
    p.county,
    p.state,
    p.zip,
    p.neighborhood_id,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.sqft,
    p.lot_acres,
    p.year_built,
    a.assessed_value,
    a.land_value,
    a.improvement_value,
    a.assessment_year,
    CAST(a.assessment_date AS DATE)                              AS valid_from,
    CAST(NULL AS DATE)                                           AS valid_to,
    true                                                          AS is_current
FROM realty.bronze.raw_properties p
JOIN realty.bronze.raw_assessments a
    ON p.parcel_id = a.parcel_id AND a.assessment_year = 2023;

-- Verify: 18 original + 12 new = 30 rows total
ASSERT ROW_COUNT = 30
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

-- ===================== load_scd2_batch3 =====================
-- Batch 3: 2024 assessments for 8 properties. Same expire-insert pattern.

-- Expire current records for properties being reassessed in 2024
MERGE INTO realty.silver.property_dim AS tgt
USING (
    SELECT DISTINCT a.parcel_id, CAST(a.assessment_date AS DATE) AS new_valid_from
    FROM realty.bronze.raw_assessments a
    WHERE a.assessment_year = 2024
) AS src
ON tgt.parcel_id = src.parcel_id AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
    valid_to = src.new_valid_from,
    is_current = false;

-- Insert new 2024 assessment records
INSERT INTO realty.silver.property_dim
SELECT
    (SELECT COALESCE(MAX(surrogate_key), 0) FROM realty.silver.property_dim)
        + ROW_NUMBER() OVER (ORDER BY p.parcel_id)               AS surrogate_key,
    p.parcel_id,
    p.address,
    p.city,
    p.county,
    p.state,
    p.zip,
    p.neighborhood_id,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.sqft,
    p.lot_acres,
    p.year_built,
    a.assessed_value,
    a.land_value,
    a.improvement_value,
    a.assessment_year,
    CAST(a.assessment_date AS DATE)                              AS valid_from,
    CAST(NULL AS DATE)                                           AS valid_to,
    true                                                          AS is_current
FROM realty.bronze.raw_properties p
JOIN realty.bronze.raw_assessments a
    ON p.parcel_id = a.parcel_id AND a.assessment_year = 2024;

-- Verify: 18 + 12 + 8 = 38 total rows, only 18 with is_current = true
ASSERT ROW_COUNT = 38
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim WHERE is_current = true;

-- ===================== enrich_transactions_point_in_time =====================
-- Join transactions with the SCD2 property_dim at the point in time of the sale.
-- Assessed value at time of sale = the assessment that was current on transaction_date.
-- Also calculate assessed_vs_sale_ratio and flag assessment outliers (|ratio - 1| > 0.20).

INSERT INTO realty.silver.transactions_enriched
SELECT
    t.transaction_id,
    t.parcel_id,
    t.buyer_name,
    t.seller_name,
    t.agent_id,
    CAST(t.transaction_date AS DATE)                             AS transaction_date,
    t.sale_price,
    -- Point-in-time assessed value: the assessment current at time of sale
    pd.assessed_value                                             AS assessed_value_at_sale,
    -- Price per sqft
    ROUND(t.sale_price / NULLIF(pd.sqft, 0), 2)                 AS price_per_sqft,
    t.days_on_market,
    -- Over asking: compare sale to assessed (proxy for list price in assessor context)
    ROUND(100.0 * (t.sale_price - pd.assessed_value) / NULLIF(pd.assessed_value, 0), 2) AS over_asking_pct,
    -- Assessment accuracy ratio: assessed / sale
    ROUND(pd.assessed_value / NULLIF(t.sale_price, 0), 2)       AS assessed_vs_sale_ratio,
    -- Assessment outlier: |ratio - 1.0| > 0.20
    CASE
        WHEN ABS(pd.assessed_value / NULLIF(t.sale_price, 0) - 1.0) > 0.20 THEN true
        ELSE false
    END                                                           AS assessment_outlier,
    t.financing_type,
    t.closing_costs
FROM realty.bronze.raw_transactions t
JOIN realty.silver.property_dim pd
    ON t.parcel_id = pd.parcel_id
    -- Point-in-time: valid_from <= transaction_date AND (valid_to IS NULL OR valid_to > transaction_date)
    AND pd.valid_from <= CAST(t.transaction_date AS DATE)
    AND (pd.valid_to IS NULL OR pd.valid_to > CAST(t.transaction_date AS DATE));

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM realty.silver.transactions_enriched;

-- ===================== dim_neighborhood =====================

INSERT INTO realty.gold.dim_neighborhood
SELECT
    ROW_NUMBER() OVER (ORDER BY n.neighborhood_id)               AS neighborhood_key,
    n.neighborhood_id,
    n.neighborhood_name,
    n.city,
    n.county,
    n.state,
    n.median_income,
    n.school_rating,
    n.crime_index,
    n.walkability_score
FROM realty.bronze.raw_neighborhoods n;

-- ===================== dim_agent =====================

INSERT INTO realty.gold.dim_agent
SELECT
    ROW_NUMBER() OVER (ORDER BY a.agent_id)                     AS agent_key,
    a.agent_id,
    a.agent_name,
    a.brokerage,
    a.license_number,
    a.years_experience,
    a.specialization,
    a.county
FROM realty.bronze.raw_agents a;

-- ===================== dim_property_type =====================

INSERT INTO realty.gold.dim_property_type
SELECT
    ROW_NUMBER() OVER (ORDER BY pd.property_type)                AS property_type_key,
    pd.property_type,
    CAST(ROUND(AVG(pd.sqft), 0) AS INT)                         AS avg_sqft,
    ROUND(AVG(pd.assessed_value), 2)                             AS avg_assessed_value,
    COUNT(DISTINCT pd.parcel_id)                                 AS property_count
FROM realty.silver.property_dim pd
WHERE pd.is_current = true
GROUP BY pd.property_type;

-- ===================== build_fact_transactions =====================
-- Star schema fact joining enriched transactions with all dimensions.

INSERT INTO realty.gold.fact_transactions
SELECT
    ROW_NUMBER() OVER (ORDER BY te.transaction_id)               AS transaction_key,
    te.parcel_id,
    dn.neighborhood_key,
    da.agent_key,
    dpt.property_type_key,
    te.transaction_date,
    te.sale_price,
    te.assessed_value_at_sale,
    te.price_per_sqft,
    te.days_on_market,
    te.over_asking_pct,
    te.assessed_vs_sale_ratio,
    te.assessment_outlier,
    te.financing_type,
    te.closing_costs
FROM realty.silver.transactions_enriched te
JOIN realty.silver.property_dim pd
    ON te.parcel_id = pd.parcel_id AND pd.is_current = true
JOIN realty.gold.dim_neighborhood dn
    ON pd.neighborhood_id = dn.neighborhood_id
JOIN realty.gold.dim_agent da
    ON te.agent_id = da.agent_id
JOIN realty.gold.dim_property_type dpt
    ON pd.property_type = dpt.property_type;

-- ===================== kpi_market_trends =====================
-- Market trends by city x property_type x quarter.

INSERT INTO realty.gold.kpi_market_trends
SELECT
    pd.city,
    dpt.property_type,
    CONCAT(
        CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING),
        '-Q',
        CAST(
            CASE
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 3 THEN 1
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 6 THEN 2
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 9 THEN 3
                ELSE 4
            END
        AS STRING)
    )                                                             AS sale_quarter,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.sale_price), 2) AS median_sale_price,
    ROUND(AVG(ft.price_per_sqft), 2)                             AS avg_price_per_sqft,
    ROUND(AVG(ft.days_on_market), 1)                             AS avg_days_on_market,
    COUNT(*)                                                      AS total_transactions,
    ROUND(AVG(ft.over_asking_pct), 2)                            AS avg_over_asking_pct,
    -- Inventory months approximation
    ROUND(18.0 / NULLIF(COUNT(*) / 3.0, 0), 1)                  AS inventory_months,
    0.00                                                          AS yoy_price_change_pct
FROM realty.gold.fact_transactions ft
JOIN realty.silver.property_dim pd ON ft.parcel_id = pd.parcel_id AND pd.is_current = true
JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
GROUP BY pd.city, dpt.property_type,
    CONCAT(
        CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING),
        '-Q',
        CAST(
            CASE
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 3 THEN 1
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 6 THEN 2
                WHEN EXTRACT(MONTH FROM ft.transaction_date) <= 9 THEN 3
                ELSE 4
            END
        AS STRING)
    );

-- ===================== kpi_assessment_accuracy =====================
-- Assessed value vs actual sale price analysis by county x property_type x assessment_year.
-- COD (Coefficient of Dispersion) = avg absolute deviation from median ratio.

INSERT INTO realty.gold.kpi_assessment_accuracy
SELECT
    pd.county,
    dpt.property_type,
    pd.assessment_year,
    COUNT(*)                                                      AS total_sales,
    ROUND(AVG(ft.assessed_value_at_sale), 2)                     AS avg_assessed_value,
    ROUND(AVG(ft.sale_price), 2)                                 AS avg_sale_price,
    ROUND(AVG(ft.assessed_vs_sale_ratio), 2)                     AS avg_ratio,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.assessed_vs_sale_ratio), 2) AS median_ratio,
    SUM(CASE WHEN ft.assessment_outlier = true THEN 1 ELSE 0 END) AS outlier_count,
    ROUND(
        100.0 * SUM(CASE WHEN ft.assessment_outlier = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    )                                                             AS outlier_rate_pct,
    -- COD: average |ratio - median_ratio| / median_ratio * 100
    ROUND(
        100.0 * AVG(ABS(ft.assessed_vs_sale_ratio
            - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.assessed_vs_sale_ratio)))
        / NULLIF(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.assessed_vs_sale_ratio), 0),
        2
    )                                                             AS cod
FROM realty.gold.fact_transactions ft
JOIN realty.silver.property_dim pd
    ON ft.parcel_id = pd.parcel_id
    AND pd.valid_from <= ft.transaction_date
    AND (pd.valid_to IS NULL OR pd.valid_to > ft.transaction_date)
JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
GROUP BY pd.county, dpt.property_type, pd.assessment_year;

-- ===================== restore_correction_demo =====================
-- Simulate a bad assessment batch: insert garbage data, capture the damage,
-- RESTORE to prior version, and log the correction.

-- Record row count before bad batch
SELECT COUNT(*) AS rows_before_bad_batch FROM realty.silver.property_dim;

-- Insert obviously wrong assessments: all properties assessed at $1
INSERT INTO realty.silver.property_dim
SELECT
    (SELECT COALESCE(MAX(surrogate_key), 0) FROM realty.silver.property_dim)
        + ROW_NUMBER() OVER (ORDER BY parcel_id)                 AS surrogate_key,
    parcel_id, address, city, county, state, zip, neighborhood_id, property_type,
    bedrooms, bathrooms, sqft, lot_acres, year_built,
    1.00                                                          AS assessed_value,
    0.50                                                          AS land_value,
    0.50                                                          AS improvement_value,
    9999                                                          AS assessment_year,
    CAST('2099-01-01' AS DATE)                                   AS valid_from,
    CAST(NULL AS DATE)                                           AS valid_to,
    true                                                          AS is_current
FROM realty.bronze.raw_properties;

-- Show the damage: 38 + 18 = 56 rows now, with bad $1 assessments
ASSERT ROW_COUNT > 38
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

-- RESTORE to the version before the bad insert
RESTORE realty.silver.property_dim TO VERSION 6;

-- Verify recovery: back to 38 rows
ASSERT ROW_COUNT = 38
SELECT COUNT(*) AS row_count FROM realty.silver.property_dim;

-- Log the correction event
INSERT INTO realty.silver.correction_log VALUES (
    1,
    'silver.property_dim',
    'RESTORE',
    6,
    'Rolled back bad assessment batch (all properties assessed at $1)',
    56,
    38,
    CURRENT_TIMESTAMP
);

-- ===================== bloom_and_optimize =====================

OPTIMIZE realty.silver.property_dim;
OPTIMIZE realty.silver.transactions_enriched;
OPTIMIZE realty.gold.fact_transactions;
OPTIMIZE realty.gold.dim_neighborhood;
OPTIMIZE realty.gold.dim_agent;
OPTIMIZE realty.gold.dim_property_type;
OPTIMIZE realty.gold.kpi_market_trends;
OPTIMIZE realty.gold.kpi_assessment_accuracy;
