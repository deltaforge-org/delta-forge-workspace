-- =============================================================================
-- Real Estate Property Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new transactions and property listing updates

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "transaction_id > 'TXN-022' AND transaction_date > '2024-12-24'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transaction_enriched, transaction_id, transaction_date, 7)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.transaction_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_transactions
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transaction_enriched, transaction_id, transaction_date, 7)}};

-- ===================== STEP 1: Watermark Check =====================

SELECT MAX(transaction_date) AS last_watermark
FROM {{zone_prefix}}.silver.transaction_enriched;

-- ===================== STEP 2: New Bronze Listings and Transactions =====================

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
    ('TXN-023', 'PROP-011', 'BUY-04', 'AGT-01', '2025-01-15', 825000.00,  'Conventional', 20000.00, '2025-01-15T09:00:00'),
    ('TXN-024', 'PROP-008', 'BUY-09', 'AGT-04', '2025-02-01', 475000.00,  'Conventional', 11500.00, '2025-02-01T09:00:00'),
    ('TXN-025', 'PROP-014', 'BUY-07', 'AGT-02', '2025-02-20', 440000.00,  'Conventional', 10700.00, '2025-02-20T09:00:00');

-- ===================== STEP 3: Incremental Silver Enrichment =====================

INSERT INTO {{zone_prefix}}.silver.transaction_enriched
ASSERT ROW_COUNT = 3
SELECT
    t.transaction_id,
    t.property_id,
    t.buyer_id,
    t.agent_id,
    CAST(t.transaction_date AS DATE),
    p.list_price,
    t.sale_price,
    ROUND(t.sale_price / NULLIF(p.sqft, 0), 2),
    DATEDIFF(CAST(t.transaction_date AS DATE), CAST(p.list_date AS DATE)),
    ROUND(100.0 * (t.sale_price - p.list_price) / NULLIF(p.list_price, 0), 2),
    t.financing_type
FROM {{zone_prefix}}.bronze.raw_transactions t
JOIN (
    SELECT property_id, list_price, sqft, list_date,
           ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY list_date DESC) AS rn
    FROM {{zone_prefix}}.bronze.raw_properties
) p ON t.property_id = p.property_id AND p.rn = 1
WHERE t.ingested_at > '2025-01-01T12:00:00';

-- ===================== STEP 4: Merge New Transactions into Gold Fact =====================

MERGE INTO {{zone_prefix}}.gold.fact_transactions AS tgt
USING (
ASSERT ROW_COUNT = 3
    SELECT
        ROW_NUMBER() OVER (ORDER BY te.transaction_id) + 22 AS transaction_key,
        dp.surrogate_key                                      AS property_key,
        db.buyer_key,
        NULL                                                  AS seller_key,
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
    )
    WHERE te.transaction_date > '2024-12-31'
) AS src
ON tgt.transaction_key = src.transaction_key
WHEN NOT MATCHED THEN INSERT (
    transaction_key, property_key, buyer_key, seller_key, agent_key,
    transaction_date, list_price, sale_price, price_per_sqft, days_on_market, over_asking_pct
) VALUES (
    src.transaction_key, src.property_key, src.buyer_key, src.seller_key, src.agent_key,
    src.transaction_date, src.list_price, src.sale_price, src.price_per_sqft, src.days_on_market, src.over_asking_pct
);

-- ===================== STEP 5: Rebuild KPI =====================

DELETE FROM {{zone_prefix}}.gold.kpi_market_trends WHERE 1=1;

INSERT INTO {{zone_prefix}}.gold.kpi_market_trends
SELECT
    dp.city,
    dp.property_type,
    CONCAT(CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ft.transaction_date) AS STRING)) AS quarter,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.sale_price), 2) AS median_sale_price,
    ROUND(AVG(ft.price_per_sqft), 2)                        AS avg_price_per_sqft,
    ROUND(AVG(ft.days_on_market), 1)                        AS avg_days_on_market,
    COUNT(*)                                                 AS total_transactions,
    ROUND(AVG(ft.over_asking_pct), 2)                       AS over_asking_pct,
    ROUND(15.0 / NULLIF(COUNT(*) / 3.0, 0), 1)             AS inventory_months,
    0.00                                                     AS price_change_yoy_pct
FROM {{zone_prefix}}.gold.fact_transactions ft
JOIN {{zone_prefix}}.gold.dim_property dp ON ft.property_key = dp.surrogate_key
GROUP BY dp.city, dp.property_type,
    CONCAT(CAST(EXTRACT(YEAR FROM ft.transaction_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ft.transaction_date) AS STRING));

-- ===================== STEP 6: Verify =====================

ASSERT VALUE sale_price > 0 WHERE transaction_date > '2025-01-01'
ASSERT ROW_COUNT > 0
SELECT 'sale_price check passed' AS sale_price_status;


OPTIMIZE {{zone_prefix}}.gold.fact_transactions;
