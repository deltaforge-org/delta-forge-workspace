-- =============================================================================
-- Real Estate Property Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE realty_daily_schedule
    CRON '0 9 * * *'
    TIMEZONE 'America/New_York'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE realty_property_pipeline
    DESCRIPTION 'Full load: transform property and transaction data with SCD2 price tracking'
    SCHEDULE 'realty_daily_schedule'
    TAGS 'real-estate,property,scd2,full-load'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: Bronze -> Silver (SCD2 Property Initial Load) =====================
-- Load all property listings as initial SCD2 records

INSERT INTO {{zone_prefix}}.silver.property_scd2
SELECT
    ROW_NUMBER() OVER (ORDER BY p.property_id, p.list_date)  AS surrogate_key,
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip,
    p.neighborhood_id,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.sqft,
    p.lot_acres,
    p.year_built,
    p.list_price,
    p.listing_status,
    p.seller_name,
    CAST(p.list_date AS DATE)                                AS valid_from,
    CAST(NULL AS DATE)                                       AS valid_to,
    true                                                      AS is_current
FROM {{zone_prefix}}.bronze.raw_properties p;

-- ===================== STEP 2: SCD2 Two-Pass MERGE — Expire Old Price Records =====================
-- When a property has multiple records (price changes), expire the older ones

MERGE INTO {{zone_prefix}}.silver.property_scd2 AS tgt
USING (
    SELECT property_id,
           MAX(valid_from) AS latest_from
    FROM {{zone_prefix}}.silver.property_scd2
    GROUP BY property_id
    HAVING COUNT(*) > 1
) AS latest
ON tgt.property_id = latest.property_id
   AND tgt.valid_from < latest.latest_from
   AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
    valid_to = latest.latest_from,
    is_current = false;

-- ===================== STEP 3: Bronze -> Silver (Transaction Enrichment) =====================
-- Calculate price_per_sqft, days_on_market, over_asking_pct

INSERT INTO {{zone_prefix}}.silver.transaction_enriched
SELECT
    t.transaction_id,
    t.property_id,
    t.buyer_id,
    t.agent_id,
    CAST(t.transaction_date AS DATE)                         AS transaction_date,
    p.list_price,
    t.sale_price,
    ROUND(t.sale_price / NULLIF(p.sqft, 0), 2)             AS price_per_sqft,
    DATEDIFF(CAST(t.transaction_date AS DATE), CAST(p.list_date AS DATE)) AS days_on_market,
    ROUND(100.0 * (t.sale_price - p.list_price) / NULLIF(p.list_price, 0), 2) AS over_asking_pct,
    t.financing_type
FROM {{zone_prefix}}.bronze.raw_transactions t
JOIN (
    -- Get the listing price that was current at time of sale
    SELECT property_id, list_price, sqft, list_date,
           ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY list_date DESC) AS rn
    FROM {{zone_prefix}}.bronze.raw_properties
) p ON t.property_id = p.property_id AND p.rn = 1;

-- ===================== STEP 4: Silver -> Gold (dim_neighborhood) =====================

INSERT INTO {{zone_prefix}}.gold.dim_neighborhood
ASSERT ROW_COUNT = 22
SELECT
    ROW_NUMBER() OVER (ORDER BY n.neighborhood_id)          AS neighborhood_key,
    n.neighborhood_name                                      AS name,
    n.city,
    n.state,
    n.median_income,
    n.school_rating,
    n.crime_index,
    n.walkability_score
FROM {{zone_prefix}}.bronze.raw_neighborhoods n;

-- ===================== STEP 5: Silver -> Gold (dim_agent) =====================

INSERT INTO {{zone_prefix}}.gold.dim_agent
ASSERT ROW_COUNT = 5
SELECT
    ROW_NUMBER() OVER (ORDER BY a.agent_id)                 AS agent_key,
    a.agent_name                                             AS name,
    a.brokerage,
    a.license_number,
    a.years_experience,
    a.specialization
FROM {{zone_prefix}}.bronze.raw_agents a;

-- ===================== STEP 6: Silver -> Gold (dim_buyer) =====================

INSERT INTO {{zone_prefix}}.gold.dim_buyer
ASSERT ROW_COUNT = 6
SELECT
    ROW_NUMBER() OVER (ORDER BY b.buyer_id)                 AS buyer_key,
    b.buyer_name                                             AS name,
    b.buyer_type,
    b.pre_approved_flag,
    b.budget_range
FROM {{zone_prefix}}.bronze.raw_buyers b;

-- ===================== STEP 7: Silver -> Gold (dim_property from SCD2) =====================

INSERT INTO {{zone_prefix}}.gold.dim_property
ASSERT ROW_COUNT = 12
SELECT
    s.surrogate_key,
    s.property_id,
    s.address,
    s.city,
    s.state,
    s.zip,
    s.property_type,
    s.bedrooms,
    s.bathrooms,
    s.sqft,
    s.lot_acres,
    s.year_built,
    s.list_price,
    s.valid_from,
    s.valid_to,
    s.is_current
FROM {{zone_prefix}}.silver.property_scd2 s;

-- ===================== STEP 8: Silver -> Gold (fact_transactions) =====================

INSERT INTO {{zone_prefix}}.gold.fact_transactions
SELECT
    ROW_NUMBER() OVER (ORDER BY te.transaction_id)          AS transaction_key,
    dp.surrogate_key                                         AS property_key,
    db.buyer_key,
    NULL                                                     AS seller_key,
    da.agent_key,
    te.transaction_date,
    te.list_price,
    te.sale_price,
    te.price_per_sqft,
    te.days_on_market,
    te.over_asking_pct
FROM {{zone_prefix}}.silver.transaction_enriched te
JOIN {{zone_prefix}}.gold.dim_property dp ON te.property_id = dp.property_id AND dp.is_current = true
JOIN {{zone_prefix}}.gold.dim_buyer db    ON te.buyer_id = (
    SELECT b.buyer_id FROM {{zone_prefix}}.bronze.raw_buyers b WHERE b.buyer_name = db.name
)
JOIN {{zone_prefix}}.gold.dim_agent da    ON te.agent_id = (
    SELECT a.agent_id FROM {{zone_prefix}}.bronze.raw_agents a WHERE a.agent_name = da.name
);

-- ===================== STEP 9: Silver -> Gold (kpi_market_trends) =====================

INSERT INTO {{zone_prefix}}.gold.kpi_market_trends
ASSERT ROW_COUNT = 22
SELECT
    dp.city,
    dp.property_type,
    CONCAT(CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ft.transaction_date) AS STRING)) AS quarter,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.sale_price), 2) AS median_sale_price,
    ROUND(AVG(ft.price_per_sqft), 2)                        AS avg_price_per_sqft,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_days_on_market,
    COUNT(*)                                                 AS total_transactions,
    ROUND(AVG(ft.over_asking_pct), 2)                       AS over_asking_pct,
    -- Inventory months: approximate (active listings / monthly sales rate)
    ROUND(
        15.0 / NULLIF(COUNT(*) / 3.0, 0),
        1
    )                                                        AS inventory_months,
    0.00                                                     AS price_change_yoy_pct
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp ON ft.property_key = dp.surrogate_key
GROUP BY dp.city, dp.property_type,
    CONCAT(CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ft.transaction_date) AS STRING));

-- ===================== STEP 10: RESTORE Demo =====================
-- Simulate a bad import by inserting garbage data, then restore

INSERT INTO {{zone_prefix}}.bronze.raw_properties VALUES
    ('PROP-BAD', 'Bad Data Row', 'Nowhere', 'XX', '00000', 'NBH-XXX', 'Unknown', 0, 0.0, 0, 0.000, 1900, 0.00, 'Error', '1900-01-01', 'Bad Import', '2025-01-01T00:00:00');

-- Restore to version before the bad insert
RESTORE {{zone_prefix}}.bronze.raw_properties TO VERSION 1;

-- ===================== OPTIMIZE =====================

OPTIMIZE {{zone_prefix}}.silver.property_scd2;
OPTIMIZE {{zone_prefix}}.silver.transaction_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_transactions;
OPTIMIZE {{zone_prefix}}.gold.dim_property;
OPTIMIZE {{zone_prefix}}.gold.kpi_market_trends;
