-- =============================================================================
-- Government Tax Filing Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE tax_daily_schedule
    CRON '0 1 * * *'
    TIMEZONE 'America/New_York'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE tax_filing_pipeline
    DESCRIPTION 'Full load: transform tax filing data through medallion layers'
    SCHEDULE 'tax_daily_schedule'
    TAGS 'government,tax,full-load'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: Bronze -> Silver (Filings Enrichment) =====================
-- Calculate effective tax rate, deduction percentage, flag audit candidates, derive income brackets

INSERT INTO {{zone_prefix}}.silver.filings
SELECT
    f.filing_id,
    f.taxpayer_id,
    f.jurisdiction_id,
    f.preparer_id,
    f.fiscal_year,
    CAST(f.filing_date AS DATE)                                 AS filing_date,
    f.gross_income,
    f.deductions,
    f.taxable_income,
    f.tax_owed,
    f.tax_paid,
    f.refund_amount,
    f.filing_status,
    f.amended_flag,
    -- Effective tax rate
    CASE
        WHEN f.gross_income > 0 THEN ROUND(f.tax_owed / f.gross_income, 4)
        ELSE 0.0000
    END                                                          AS effective_tax_rate,
    -- Deduction percentage
    CASE
        WHEN f.gross_income > 0 THEN ROUND(100.0 * f.deductions / f.gross_income, 2)
        ELSE 0.00
    END                                                          AS deduction_pct,
    -- Audit flag: deductions > 40% of gross income
    CASE
        WHEN f.gross_income > 0 AND (f.deductions / f.gross_income) > 0.40 THEN true
        ELSE false
    END                                                          AS audit_flag,
    -- Income bracket
    CASE
        WHEN f.gross_income < 50000                THEN 'Under $50K'
        WHEN f.gross_income BETWEEN 50000 AND 100000  THEN '$50K-$100K'
        WHEN f.gross_income BETWEEN 100001 AND 250000 THEN '$100K-$250K'
        WHEN f.gross_income BETWEEN 250001 AND 500000 THEN '$250K-$500K'
        WHEN f.gross_income BETWEEN 500001 AND 1000000 THEN '$500K-$1M'
        ELSE 'Over $1M'
    END                                                          AS income_bracket
FROM {{zone_prefix}}.bronze.raw_filings f;

-- ===================== STEP 2: Bronze -> Silver (Taxpayer Enrichment) =====================

INSERT INTO {{zone_prefix}}.silver.taxpayer_enriched
ASSERT ROW_COUNT = 42
SELECT
    t.taxpayer_id,
    TRIM(t.taxpayer_name)                                       AS taxpayer_name,
    t.ssn,
    t.filing_type,
    t.state,
    t.dependent_count,
    -- Derive bracket from most recent filing
    latest.income_bracket,
    latest.filing_count                                          AS total_filings
FROM {{zone_prefix}}.bronze.raw_taxpayers t
LEFT JOIN (
    SELECT
        taxpayer_id,
        income_bracket,
        COUNT(*)                                                 AS filing_count,
        ROW_NUMBER() OVER (PARTITION BY taxpayer_id ORDER BY fiscal_year DESC, filing_date DESC) AS rn
    FROM {{zone_prefix}}.silver.filings
    WHERE amended_flag = false
    GROUP BY taxpayer_id, income_bracket, fiscal_year, filing_date
) latest ON t.taxpayer_id = latest.taxpayer_id AND latest.rn = 1;

-- ===================== STEP 3: Silver -> Gold (dim_jurisdiction) =====================

INSERT INTO {{zone_prefix}}.gold.dim_jurisdiction
ASSERT ROW_COUNT = 15
SELECT
    ROW_NUMBER() OVER (ORDER BY j.jurisdiction_id)  AS jurisdiction_key,
    j.jurisdiction_name,
    j.jurisdiction_level,
    j.state,
    j.tax_rate,
    j.standard_deduction
FROM {{zone_prefix}}.bronze.raw_jurisdictions j;

-- ===================== STEP 4: Silver -> Gold (dim_preparer) =====================
-- Calculate error rate based on amended filings attributed to preparer

INSERT INTO {{zone_prefix}}.gold.dim_preparer
ASSERT ROW_COUNT = 5
SELECT
    ROW_NUMBER() OVER (ORDER BY p.preparer_id)      AS preparer_key,
    p.preparer_name,
    p.firm,
    p.certification,
    COALESCE(
        ROUND(
            100.0 * err.amended_count / NULLIF(err.total_count, 0),
            2
        ),
        0.00
    )                                                AS error_rate
FROM {{zone_prefix}}.bronze.raw_preparers p
LEFT JOIN (
    SELECT
        preparer_id,
        COUNT(*)                                     AS total_count,
        SUM(CASE WHEN amended_flag = true THEN 1 ELSE 0 END) AS amended_count
    FROM {{zone_prefix}}.silver.filings
    GROUP BY preparer_id
) err ON p.preparer_id = err.preparer_id;

-- ===================== STEP 5: Silver -> Gold (dim_taxpayer) =====================

INSERT INTO {{zone_prefix}}.gold.dim_taxpayer
ASSERT ROW_COUNT = 4
SELECT
    ROW_NUMBER() OVER (ORDER BY te.taxpayer_id)     AS taxpayer_key,
    te.taxpayer_id,
    te.filing_type,
    te.state,
    te.income_bracket,
    te.dependent_count
FROM {{zone_prefix}}.silver.taxpayer_enriched te;

-- ===================== STEP 6: Silver -> Gold (fact_filings) =====================

INSERT INTO {{zone_prefix}}.gold.fact_filings
ASSERT ROW_COUNT = 15
SELECT
    ROW_NUMBER() OVER (ORDER BY sf.filing_id)       AS filing_key,
    dt.taxpayer_key,
    dj.jurisdiction_key,
    dp.preparer_key,
    sf.fiscal_year,
    sf.filing_date,
    sf.gross_income,
    sf.deductions,
    sf.taxable_income,
    sf.tax_owed,
    sf.tax_paid,
    sf.refund_amount,
    sf.filing_status,
    sf.amended_flag
FROM {{zone_prefix}}.silver.filings sf
JOIN {{zone_prefix}}.gold.dim_taxpayer dt        ON sf.taxpayer_id = dt.taxpayer_id
JOIN {{zone_prefix}}.gold.dim_jurisdiction dj    ON sf.jurisdiction_id = (
    SELECT j.jurisdiction_id FROM {{zone_prefix}}.bronze.raw_jurisdictions j
    WHERE j.jurisdiction_name = dj.jurisdiction_name
)
LEFT JOIN {{zone_prefix}}.gold.dim_preparer dp   ON sf.preparer_id = (
    SELECT p.preparer_id FROM {{zone_prefix}}.bronze.raw_preparers p
    WHERE p.preparer_name = dp.preparer_name
);

-- ===================== STEP 7: Silver -> Gold (kpi_revenue_analysis) =====================

INSERT INTO {{zone_prefix}}.gold.kpi_revenue_analysis
ASSERT ROW_COUNT = 42
SELECT
    ff.fiscal_year,
    dj.jurisdiction_name                                        AS jurisdiction,
    COUNT(*)                                                    AS total_filings,
    SUM(ff.taxable_income)                                      AS total_taxable_income,
    SUM(ff.tax_owed)                                            AS total_tax_collected,
    SUM(CASE WHEN ff.refund_amount > 0 THEN ff.refund_amount ELSE 0 END) AS total_refunds,
    ROUND(AVG(
        CASE WHEN ff.gross_income > 0 THEN ff.tax_owed / ff.gross_income ELSE 0 END
    ), 4)                                                       AS avg_effective_rate,
    SUM(CASE WHEN sf.audit_flag = true THEN 1 ELSE 0 END)     AS audit_flag_count,
    -- Compliance rate: accepted or filed / total non-amended
    ROUND(
        100.0 * SUM(CASE WHEN ff.filing_status IN ('Accepted', 'Filed') AND ff.amended_flag = false THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN ff.amended_flag = false THEN 1 ELSE 0 END), 0),
        2
    )                                                           AS compliance_rate
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
JOIN {{zone_prefix}}.silver.filings sf ON ff.filing_date = sf.filing_date
    AND ff.fiscal_year = sf.fiscal_year
    AND ff.gross_income = sf.gross_income
GROUP BY ff.fiscal_year, dj.jurisdiction_name;

ASSERT ROW_COUNT > 0
SELECT 'row count check' AS status;


-- ===================== OPTIMIZE =====================

OPTIMIZE {{zone_prefix}}.silver.filings;
OPTIMIZE {{zone_prefix}}.silver.taxpayer_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_filings;
OPTIMIZE {{zone_prefix}}.gold.dim_taxpayer;
OPTIMIZE {{zone_prefix}}.gold.kpi_revenue_analysis;
