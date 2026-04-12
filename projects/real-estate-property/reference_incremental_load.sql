-- =============================================================================
-- Real Estate Property Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new transactions and assessment updates arriving
-- after the initial full load.  Uses INCREMENTAL_FILTER macro.
-- =============================================================================
-- 9-step DAG: check_watermark -> ingest_new_bronze -> enrich_new_transactions ->
-- merge_fact_transactions -> rebuild_kpi_market + rebuild_kpi_assessment (parallel) ->
-- scd2_incremental_assessment -> incremental_optimize
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- ============================================================================

PRINT {{INCREMENTAL_FILTER(realty.silver.transactions_enriched, transaction_id, transaction_date, 7)}};

-- ===================== PIPELINE =====================

PIPELINE realty_incremental_pipeline
  DESCRIPTION 'Incremental property pipeline: new transactions, SCD2 assessment updates, rebuilt KPIs'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'real-estate,property,incremental,SCD2'
  SLA 1800
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== check_watermark =====================

SELECT MAX(transaction_date) AS last_transaction_watermark
FROM realty.silver.transactions_enriched;

SELECT MAX(valid_from) AS last_assessment_watermark
FROM realty.silver.property_dim;

-- ===================== ingest_new_bronze =====================
-- Simulate 3 new transactions arriving in 2025.

MERGE INTO realty.bronze.raw_transactions AS tgt
USING (
    VALUES
        ('TXN-026', 'PRC-011', 'Ryan Patel',       'William Taylor', 'AGT-01', '2025-01-15', 845000.00, 'Jumbo',        21000.00, 18, '2025-01-15T09:00:00'),
        ('TXN-027', 'PRC-008', 'Sophia Anderson',  'James Wilson',   'AGT-04', '2025-02-01', 505000.00, 'Conventional', 12200.00, 25, '2025-02-01T09:00:00'),
        ('TXN-028', 'PRC-014', 'Olivia Robinson',  'Emily Garcia',   'AGT-02', '2025-02-20', 445000.00, 'Conventional', 10800.00, 30, '2025-02-20T09:00:00')
) AS src (transaction_id, parcel_id, buyer_name, seller_name, agent_id, transaction_date, sale_price, financing_type, closing_costs, days_on_market, ingested_at)
ON tgt.transaction_id = src.transaction_id
WHEN NOT MATCHED THEN INSERT (
    transaction_id, parcel_id, buyer_name, seller_name, agent_id, transaction_date,
    sale_price, financing_type, closing_costs, days_on_market, ingested_at
) VALUES (
    src.transaction_id, src.parcel_id, src.buyer_name, src.seller_name, src.agent_id, src.transaction_date,
    src.sale_price, src.financing_type, src.closing_costs, src.days_on_market, src.ingested_at
);

-- ===================== enrich_new_transactions =====================

INSERT INTO realty.silver.transactions_enriched
SELECT
    t.transaction_id,
    t.parcel_id,
    t.buyer_name,
    t.seller_name,
    t.agent_id,
    CAST(t.transaction_date AS DATE)                             AS transaction_date,
    t.sale_price,
    pd.assessed_value                                             AS assessed_value_at_sale,
    ROUND(t.sale_price / NULLIF(pd.sqft, 0), 2)                 AS price_per_sqft,
    t.days_on_market,
    ROUND(100.0 * (t.sale_price - pd.assessed_value) / NULLIF(pd.assessed_value, 0), 2) AS over_asking_pct,
    ROUND(pd.assessed_value / NULLIF(t.sale_price, 0), 2)       AS assessed_vs_sale_ratio,
    CASE
        WHEN ABS(pd.assessed_value / NULLIF(t.sale_price, 0) - 1.0) > 0.20 THEN true
        ELSE false
    END                                                           AS assessment_outlier,
    t.financing_type,
    t.closing_costs
FROM realty.bronze.raw_transactions t
JOIN realty.silver.property_dim pd
    ON t.parcel_id = pd.parcel_id
    AND pd.valid_from <= CAST(t.transaction_date AS DATE)
    AND (pd.valid_to IS NULL OR pd.valid_to > CAST(t.transaction_date AS DATE))
WHERE {{INCREMENTAL_FILTER(realty.silver.transactions_enriched, transaction_id, transaction_date, 7)}};

-- ===================== merge_fact_transactions =====================

MERGE INTO realty.gold.fact_transactions AS tgt
USING (
    SELECT
        CAST((SELECT COALESCE(MAX(transaction_key), 0) FROM realty.gold.fact_transactions)
            + ROW_NUMBER() OVER (ORDER BY te.transaction_id) AS INT)  AS transaction_key,
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
        ON pd.property_type = dpt.property_type
    WHERE te.transaction_date > '2024-12-31'
) AS src
ON tgt.transaction_key = src.transaction_key
WHEN NOT MATCHED THEN INSERT (
    transaction_key, parcel_id, neighborhood_key, agent_key, property_type_key,
    transaction_date, sale_price, assessed_value_at_sale, price_per_sqft,
    days_on_market, over_asking_pct, assessed_vs_sale_ratio, assessment_outlier,
    financing_type, closing_costs
) VALUES (
    src.transaction_key, src.parcel_id, src.neighborhood_key, src.agent_key, src.property_type_key,
    src.transaction_date, src.sale_price, src.assessed_value_at_sale, src.price_per_sqft,
    src.days_on_market, src.over_asking_pct, src.assessed_vs_sale_ratio, src.assessment_outlier,
    src.financing_type, src.closing_costs
);

-- ===================== rebuild_kpi_market =====================
-- NOTE: The CONCAT quarter expression is computed once in a CTE to avoid
-- duplicating complex expressions in SELECT + GROUP BY, which can trigger
-- the DataFusion common_sub_expression_eliminate optimizer bug.

DELETE FROM realty.gold.kpi_market_trends WHERE 1=1;

INSERT INTO realty.gold.kpi_market_trends
WITH market_base AS (
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
        ) AS sale_quarter,
        ft.sale_price,
        ft.price_per_sqft,
        ft.days_on_market,
        ft.over_asking_pct
    FROM realty.gold.fact_transactions ft
    JOIN realty.silver.property_dim pd ON ft.parcel_id = pd.parcel_id AND pd.is_current = true
    JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
)
SELECT
    city,
    property_type,
    sale_quarter,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sale_price), 2),
    ROUND(AVG(price_per_sqft), 2),
    ROUND(AVG(days_on_market), 1),
    COUNT(*),
    ROUND(AVG(over_asking_pct), 2),
    ROUND(18.0 / NULLIF(COUNT(*) / 3.0, 0), 1),
    0.00
FROM market_base
GROUP BY city, property_type, sale_quarter;

-- ===================== rebuild_kpi_assessment =====================
-- NOTE: PERCENTILE_CONT() must not appear more than once in the same SELECT list.
-- Repeating it triggers a DataFusion optimizer bug in common_sub_expression_eliminate.
-- Workaround: compute the percentile once in a CTE and reference the result by name.

DELETE FROM realty.gold.kpi_assessment_accuracy WHERE 1=1;

INSERT INTO realty.gold.kpi_assessment_accuracy
WITH base_agg AS (
    SELECT
        pd.county,
        dpt.property_type,
        pd.assessment_year,
        COUNT(*)                                                            AS total_sales,
        ROUND(AVG(ft.assessed_value_at_sale), 2)                           AS avg_assessed_value,
        ROUND(AVG(ft.sale_price), 2)                                       AS avg_sale_price,
        ROUND(AVG(ft.assessed_vs_sale_ratio), 2)                           AS avg_ratio,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.assessed_vs_sale_ratio), 2) AS median_ratio,
        SUM(CASE WHEN ft.assessment_outlier = true THEN 1 ELSE 0 END)      AS outlier_count
    FROM realty.gold.fact_transactions ft
    JOIN realty.silver.property_dim pd
        ON ft.parcel_id = pd.parcel_id
        AND pd.valid_from <= ft.transaction_date
        AND (pd.valid_to IS NULL OR pd.valid_to > ft.transaction_date)
    JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
    GROUP BY pd.county, dpt.property_type, pd.assessment_year
),
-- Compute COD: need per-row deviation from median, so join median back to detail rows
cod_calc AS (
    SELECT
        pd.county,
        dpt.property_type,
        pd.assessment_year,
        ROUND(
            100.0 * AVG(ABS(ft.assessed_vs_sale_ratio - ba.median_ratio))
            / NULLIF(ba.median_ratio, 0),
            2
        ) AS cod
    FROM realty.gold.fact_transactions ft
    JOIN realty.silver.property_dim pd
        ON ft.parcel_id = pd.parcel_id
        AND pd.valid_from <= ft.transaction_date
        AND (pd.valid_to IS NULL OR pd.valid_to > ft.transaction_date)
    JOIN realty.gold.dim_property_type dpt ON ft.property_type_key = dpt.property_type_key
    JOIN base_agg ba
        ON pd.county = ba.county
        AND dpt.property_type = ba.property_type
        AND pd.assessment_year = ba.assessment_year
    GROUP BY pd.county, dpt.property_type, pd.assessment_year, ba.median_ratio
)
SELECT
    b.county,
    b.property_type,
    b.assessment_year,
    b.total_sales,
    b.avg_assessed_value,
    b.avg_sale_price,
    b.avg_ratio,
    b.median_ratio,
    b.outlier_count,
    ROUND(100.0 * b.outlier_count / NULLIF(b.total_sales, 0), 2) AS outlier_rate_pct,
    COALESCE(c.cod, 0.00)                                         AS cod
FROM base_agg b
LEFT JOIN cod_calc c
    ON b.county = c.county
    AND b.property_type = c.property_type
    AND b.assessment_year = c.assessment_year;

-- ===================== scd2_incremental_assessment =====================
-- Placeholder for processing new assessment batches arriving incrementally.
-- In production this would use the same expire-insert pattern from the full load.

-- Verify SCD2 integrity: all parcels should have exactly one is_current = true record
ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count
FROM realty.silver.property_dim
WHERE is_current = true;

-- ===================== incremental_optimize =====================

OPTIMIZE realty.silver.transactions_enriched;
OPTIMIZE realty.gold.fact_transactions;
OPTIMIZE realty.gold.kpi_market_trends;
OPTIMIZE realty.gold.kpi_assessment_accuracy;
