-- =============================================================================
-- Government Tax Filing Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new filings and status changes via CDF

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "filing_id > 'FIL-2024-006' AND filing_date > '2025-03-08'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.filings, filing_id, filing_date, 7)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.filings
-- SELECT * FROM {{zone_prefix}}.bronze.raw_filings
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.filings, filing_id, filing_date, 7)}};

-- ===================== STEP 1: Watermark Check =====================

SELECT MAX(filing_date) AS last_watermark
FROM {{zone_prefix}}.silver.filings;

-- ===================== STEP 2: New Bronze Filings =====================
-- Simulating late FY2024 filings and status updates

INSERT INTO {{zone_prefix}}.bronze.raw_filings VALUES
    ('FIL-2024-007', 'TP-1004', 'JUR-FED', 'PREP-04', 2024, '2025-04-01', 61000.00,  13850.00,  47150.00,  5586.50,   5800.00,   213.50,   'Filed',     false, NULL, '2025-04-01T01:00:00'),
    ('FIL-2024-008', 'TP-1007', 'JUR-FED', 'PREP-04', 2024, '2025-03-28', 68000.00,  13850.00,  54150.00,  6753.50,   7000.00,   246.50,   'Filed',     false, NULL, '2025-04-01T01:00:00'),
    ('FIL-2024-009', 'TP-1008', 'JUR-FED', 'PREP-01', 2024, '2025-03-25', 79000.00,  13850.00,  65150.00,  8707.50,   9000.00,   292.50,   'Filed',     false, NULL, '2025-04-01T01:00:00'),
    ('FIL-2024-010', 'TP-2003', 'JUR-FED', 'PREP-01', 2024, '2025-03-20', 490000.00, 210000.00, 280000.00, 61600.00,  62000.00,  400.00,   'Filed',     false, NULL, '2025-04-01T01:00:00');

-- ===================== STEP 3: Status Change — CDF Tracking =====================
-- Update filing statuses: Filed -> Accepted, Under Review -> Audited

MERGE INTO {{zone_prefix}}.silver.filings AS tgt
USING (
ASSERT ROW_COUNT = 4
    SELECT
        f.filing_id,
        f.taxpayer_id,
        f.jurisdiction_id,
        f.preparer_id,
        f.fiscal_year,
        CAST(f.filing_date AS DATE) AS filing_date,
        f.gross_income,
        f.deductions,
        f.taxable_income,
        f.tax_owed,
        f.tax_paid,
        f.refund_amount,
        'Accepted' AS filing_status,
        f.amended_flag,
        CASE WHEN f.gross_income > 0 THEN ROUND(f.tax_owed / f.gross_income, 4) ELSE 0.0000 END AS effective_tax_rate,
        CASE WHEN f.gross_income > 0 THEN ROUND(100.0 * f.deductions / f.gross_income, 2) ELSE 0.00 END AS deduction_pct,
        CASE WHEN f.gross_income > 0 AND (f.deductions / f.gross_income) > 0.40 THEN true ELSE false END AS audit_flag,
        CASE
            WHEN f.gross_income < 50000 THEN 'Under $50K'
            WHEN f.gross_income BETWEEN 50000 AND 100000 THEN '$50K-$100K'
            WHEN f.gross_income BETWEEN 100001 AND 250000 THEN '$100K-$250K'
            WHEN f.gross_income BETWEEN 250001 AND 500000 THEN '$250K-$500K'
            WHEN f.gross_income BETWEEN 500001 AND 1000000 THEN '$500K-$1M'
            ELSE 'Over $1M'
        END AS income_bracket
    FROM {{zone_prefix}}.bronze.raw_filings f
    WHERE f.filing_status = 'Filed'
      AND f.ingested_at <= '2025-01-02T00:00:00'
) AS src
ON tgt.filing_id = src.filing_id
WHEN MATCHED THEN UPDATE SET
    filing_status = 'Accepted';

-- ===================== STEP 4: Incremental Silver Enrichment (New Filings Only) =====================

INSERT INTO {{zone_prefix}}.silver.filings
SELECT
    f.filing_id,
    f.taxpayer_id,
    f.jurisdiction_id,
    f.preparer_id,
    f.fiscal_year,
    CAST(f.filing_date AS DATE),
    f.gross_income,
    f.deductions,
    f.taxable_income,
    f.tax_owed,
    f.tax_paid,
    f.refund_amount,
    f.filing_status,
    f.amended_flag,
    CASE WHEN f.gross_income > 0 THEN ROUND(f.tax_owed / f.gross_income, 4) ELSE 0.0000 END,
    CASE WHEN f.gross_income > 0 THEN ROUND(100.0 * f.deductions / f.gross_income, 2) ELSE 0.00 END,
    CASE WHEN f.gross_income > 0 AND (f.deductions / f.gross_income) > 0.40 THEN true ELSE false END,
    CASE
        WHEN f.gross_income < 50000 THEN 'Under $50K'
        WHEN f.gross_income BETWEEN 50000 AND 100000 THEN '$50K-$100K'
        WHEN f.gross_income BETWEEN 100001 AND 250000 THEN '$100K-$250K'
        WHEN f.gross_income BETWEEN 250001 AND 500000 THEN '$250K-$500K'
        WHEN f.gross_income BETWEEN 500001 AND 1000000 THEN '$500K-$1M'
        ELSE 'Over $1M'
    END
FROM {{zone_prefix}}.bronze.raw_filings f
WHERE f.ingested_at > '2025-01-01T12:00:00';

-- ===================== STEP 5: Merge New Filings into Gold Fact =====================

MERGE INTO {{zone_prefix}}.gold.fact_filings AS tgt
USING (
ASSERT ROW_COUNT = 4
    SELECT
        ROW_NUMBER() OVER (ORDER BY sf.filing_id) + 42  AS filing_key,
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
    JOIN {{zone_prefix}}.gold.dim_taxpayer dt ON sf.taxpayer_id = dt.taxpayer_id
    JOIN {{zone_prefix}}.gold.dim_jurisdiction dj ON sf.jurisdiction_id = (
        SELECT j.jurisdiction_id FROM {{zone_prefix}}.bronze.raw_jurisdictions j
        WHERE j.jurisdiction_name = dj.jurisdiction_name
    )
    LEFT JOIN {{zone_prefix}}.gold.dim_preparer dp ON sf.preparer_id = (
        SELECT p.preparer_id FROM {{zone_prefix}}.bronze.raw_preparers p
        WHERE p.preparer_name = dp.preparer_name
    )
    WHERE sf.filing_date > '2025-03-15'
) AS src
ON tgt.filing_key = src.filing_key
WHEN NOT MATCHED THEN INSERT (
    filing_key, taxpayer_key, jurisdiction_key, preparer_key, fiscal_year,
    filing_date, gross_income, deductions, taxable_income, tax_owed, tax_paid,
    refund_amount, filing_status, amended_flag
) VALUES (
    src.filing_key, src.taxpayer_key, src.jurisdiction_key, src.preparer_key,
    src.fiscal_year, src.filing_date, src.gross_income, src.deductions,
    src.taxable_income, src.tax_owed, src.tax_paid, src.refund_amount,
    src.filing_status, src.amended_flag
);

-- ===================== STEP 6: Rebuild KPI =====================

DELETE FROM {{zone_prefix}}.gold.kpi_revenue_analysis WHERE 1=1;

INSERT INTO {{zone_prefix}}.gold.kpi_revenue_analysis
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


OPTIMIZE {{zone_prefix}}.gold.fact_filings;
