-- =============================================================================
-- Government Tax Filing Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== QUERY 1: Revenue Analysis by Fiscal Year =====================

SELECT
    k.fiscal_year,
    k.jurisdiction,
    k.total_filings,
    k.total_taxable_income,
    k.total_tax_collected,
    k.total_refunds,
    k.avg_effective_rate,
    k.audit_flag_count,
    k.compliance_rate
FROM {{zone_prefix}}.gold.kpi_revenue_analysis k
ORDER BY k.fiscal_year, k.jurisdiction;

-- ===================== QUERY 2: Star Schema Join — Full Filing Detail =====================

ASSERT ROW_COUNT > 0
SELECT
    ff.filing_key,
    dt.taxpayer_id,
    dt.filing_type,
    dt.state,
    dj.jurisdiction_name,
    dj.tax_rate,
    dp.preparer_name,
    dp.certification,
    ff.fiscal_year,
    ff.filing_date,
    ff.gross_income,
    ff.deductions,
    ff.taxable_income,
    ff.tax_owed,
    ff.refund_amount,
    ff.filing_status
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_taxpayer dt        ON ff.taxpayer_key = dt.taxpayer_key
JOIN {{zone_prefix}}.gold.dim_jurisdiction dj    ON ff.jurisdiction_key = dj.jurisdiction_key
LEFT JOIN {{zone_prefix}}.gold.dim_preparer dp   ON ff.preparer_key = dp.preparer_key
ORDER BY ff.fiscal_year, dt.taxpayer_id;

-- ===================== QUERY 3: Audit Candidate Analysis =====================
-- Taxpayers with deductions > 40% of gross income across multiple years

ASSERT VALUE filing_key > 0
SELECT
    dt.taxpayer_id,
    dt.filing_type,
    dt.state,
    ff.fiscal_year,
    ff.gross_income,
    ff.deductions,
    ROUND(100.0 * ff.deductions / NULLIF(ff.gross_income, 0), 2) AS deduction_pct,
    ff.filing_status
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.deductions > 0.40 * ff.gross_income
ORDER BY dt.taxpayer_id, ff.fiscal_year;

-- ===================== QUERY 4: Preparer Error Analysis =====================

ASSERT VALUE deduction_pct > 40
SELECT
    dp.preparer_name,
    dp.firm,
    dp.certification,
    dp.error_rate,
    COUNT(*)                                                    AS total_filings_prepared,
    SUM(CASE WHEN ff.amended_flag = true THEN 1 ELSE 0 END)   AS amended_count,
    SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END) AS audit_count,
    ROUND(AVG(ff.gross_income), 2)                             AS avg_client_income
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_preparer dp ON ff.preparer_key = dp.preparer_key
GROUP BY dp.preparer_name, dp.firm, dp.certification, dp.error_rate
ORDER BY total_filings_prepared DESC;

-- ===================== QUERY 5: Income Distribution by Bracket using NTILE =====================

ASSERT ROW_COUNT = 4
SELECT
    dt.income_bracket,
    NTILE(4) OVER (ORDER BY ff.gross_income)                   AS income_quartile,
    ff.gross_income,
    ff.tax_owed,
    ROUND(ff.tax_owed / NULLIF(ff.gross_income, 0), 4)        AS effective_rate,
    dt.filing_type
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.amended_flag = false
  AND ff.fiscal_year = 2023
ORDER BY ff.gross_income;

-- ===================== QUERY 6: Year-over-Year Tax Revenue Trend =====================

ASSERT VALUE gross_income > 0
SELECT
    dj.jurisdiction_name,
    ff.fiscal_year,
    SUM(ff.tax_owed)                                            AS total_tax,
    LAG(SUM(ff.tax_owed)) OVER (PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year) AS prev_year_tax,
    ROUND(
        100.0 * (SUM(ff.tax_owed) - LAG(SUM(ff.tax_owed)) OVER (PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year))
        / NULLIF(LAG(SUM(ff.tax_owed)) OVER (PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year), 0),
        2
    )                                                           AS yoy_change_pct
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
WHERE ff.amended_flag = false
GROUP BY dj.jurisdiction_name, ff.fiscal_year
ORDER BY dj.jurisdiction_name, ff.fiscal_year;

-- ===================== QUERY 7: Compliance Rate by Jurisdiction =====================

ASSERT VALUE total_tax > 0
SELECT
    dj.jurisdiction_name,
    dj.jurisdiction_level,
    COUNT(*)                                                    AS total_filings,
    SUM(CASE WHEN ff.filing_status = 'Accepted' THEN 1 ELSE 0 END) AS accepted,
    SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END) AS audited,
    SUM(CASE WHEN ff.filing_status = 'Under Review' THEN 1 ELSE 0 END) AS under_review,
    SUM(CASE WHEN ff.amended_flag = true THEN 1 ELSE 0 END)   AS amended,
    ROUND(
        100.0 * SUM(CASE WHEN ff.filing_status IN ('Accepted', 'Filed') THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                           AS compliance_rate_pct
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
GROUP BY dj.jurisdiction_name, dj.jurisdiction_level
ORDER BY total_filings DESC;

-- ===================== QUERY 8: Refund vs Tax Owed Analysis =====================

ASSERT ROW_COUNT = 5
SELECT
    ff.fiscal_year,
    dt.filing_type,
    COUNT(*)                                                    AS filings,
    SUM(CASE WHEN ff.refund_amount > 0 THEN 1 ELSE 0 END)    AS got_refund,
    SUM(CASE WHEN ff.refund_amount < 0 THEN 1 ELSE 0 END)    AS owed_more,
    ROUND(AVG(ff.refund_amount), 2)                            AS avg_refund,
    ROUND(AVG(ff.tax_owed), 2)                                 AS avg_tax_owed
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
GROUP BY ff.fiscal_year, dt.filing_type
ORDER BY ff.fiscal_year, dt.filing_type;

-- ===================== QUERY 9: Top Taxpayers by Total Tax Contribution =====================

ASSERT VALUE filings > 0
SELECT
    dt.taxpayer_id,
    dt.filing_type,
    dt.state,
    SUM(ff.tax_owed)                                            AS total_tax_contributed,
    COUNT(DISTINCT ff.fiscal_year)                              AS years_filed,
    ROUND(AVG(ff.gross_income), 2)                             AS avg_gross_income,
    PERCENT_RANK() OVER (ORDER BY SUM(ff.tax_owed))            AS tax_percentile
FROM {{zone_prefix}}.gold.fact_filings ff
JOIN {{zone_prefix}}.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.amended_flag = false
GROUP BY dt.taxpayer_id, dt.filing_type, dt.state
ORDER BY total_tax_contributed DESC;

-- ===================== QUERY 10: Filing Timeliness Analysis =====================

ASSERT VALUE total_tax_contributed > 0
SELECT
    ff.fiscal_year,
    dp.preparer_name,
    COUNT(*)                                                    AS filings,
    MIN(ff.filing_date)                                         AS earliest_filing,
    MAX(ff.filing_date)                                         AS latest_filing,
    ROUND(AVG(ff.gross_income), 2)                             AS avg_income
FROM {{zone_prefix}}.gold.fact_filings ff
LEFT JOIN {{zone_prefix}}.gold.dim_preparer dp ON ff.preparer_key = dp.preparer_key
WHERE ff.amended_flag = false
GROUP BY ff.fiscal_year, dp.preparer_name
ORDER BY ff.fiscal_year, dp.preparer_name;

ASSERT VALUE filings > 0
SELECT 'filings check passed' AS filings_status;

