-- =============================================================================
-- Government Tax Filing Pipeline - Gold Layer Verification
-- =============================================================================
-- 10+ ASSERTs validating the star schema, amendment logic, audit trail,
-- revenue KPIs, preparer quality, and derived metrics.
-- =============================================================================

-- ===================== QUERY 1: Revenue Analysis by Fiscal Year x Jurisdiction =====================

ASSERT ROW_COUNT > 0
SELECT
  k.fiscal_year,
  k.jurisdiction_name,
  k.total_filings,
  k.total_taxable_income,
  k.total_tax_collected,
  k.total_refunds,
  k.avg_effective_rate,
  k.audit_flag_count,
  k.compliance_rate,
  k.amendment_count,
  k.audit_yield_pct
FROM tax.gold.kpi_revenue_analysis k
ORDER BY k.fiscal_year, k.jurisdiction_name;

-- ===================== QUERY 2: Star Schema Join — Full Filing Detail with Amendments =====================

ASSERT ROW_COUNT >= 50
SELECT
  ff.filing_key,
  dt.taxpayer_id,
  dt.filing_type,
  dt.state,
  dj.jurisdiction_name,
  dj.base_tax_rate,
  dp.preparer_name,
  dp.certification,
  dfy.fiscal_year,
  dfy.filing_deadline,
  ff.filing_date,
  ff.gross_income,
  ff.deductions,
  ff.taxable_income,
  ff.effective_taxable_income,
  ff.tax_owed,
  ff.effective_tax_owed,
  ff.was_amended,
  ff.amendment_reason,
  ff.amendment_count,
  ff.effective_tax_rate,
  ff.audit_flag,
  ff.filing_status
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt       ON ff.taxpayer_key = dt.taxpayer_key
JOIN tax.gold.dim_jurisdiction dj   ON ff.jurisdiction_key = dj.jurisdiction_key
LEFT JOIN tax.gold.dim_preparer dp  ON ff.preparer_key = dp.preparer_key
JOIN tax.gold.dim_fiscal_year dfy   ON ff.fiscal_year_key = dfy.fiscal_year_key
ORDER BY ff.fiscal_year, dt.taxpayer_id, dj.jurisdiction_name;

-- ===================== QUERY 3: Audit Candidate Analysis =====================
-- Taxpayers with deductions > 40% of gross income across multiple years

ASSERT ROW_COUNT >= 4
SELECT
  dt.taxpayer_id,
  dt.filing_type,
  dt.state,
  ff.fiscal_year,
  ff.gross_income,
  ff.deductions,
  ROUND(100.0 * ff.deductions / NULLIF(ff.gross_income, 0), 2) AS deduction_pct,
  ff.audit_flag,
  ff.filing_status,
  ff.was_amended,
  ff.effective_tax_owed - ff.tax_owed                            AS audit_tax_adjustment
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.audit_flag = true
ORDER BY dt.taxpayer_id, ff.fiscal_year;

-- ===================== QUERY 4: Amendment Impact Analysis =====================
-- Show all amended filings: original vs effective values

ASSERT ROW_COUNT >= 1
SELECT
  ff.filing_key,
  dt.taxpayer_id,
  ff.fiscal_year,
  ff.taxable_income                                              AS original_taxable,
  ff.effective_taxable_income                                    AS amended_taxable,
  ff.effective_taxable_income - ff.taxable_income                AS taxable_delta,
  ff.tax_owed                                                    AS original_tax,
  ff.effective_tax_owed                                          AS amended_tax,
  ff.effective_tax_owed - ff.tax_owed                            AS tax_delta,
  ff.amendment_reason,
  ff.amendment_count
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.was_amended = true
ORDER BY ABS(ff.effective_tax_owed - ff.tax_owed) DESC;

-- ===================== QUERY 5: Preparer Quality Scorecard =====================

ASSERT ROW_COUNT = 5
SELECT
  k.preparer_name,
  k.firm,
  k.certification,
  k.total_filings,
  k.amendment_count,
  k.amendment_rate_pct,
  k.audit_count,
  k.audit_rate_pct,
  k.avg_client_income,
  k.avg_deduction_pct,
  k.avg_effective_rate
FROM tax.gold.kpi_preparer_quality k
ORDER BY k.amendment_rate_pct DESC;

-- ===================== QUERY 6: Fiscal Year Dimension Validation =====================

ASSERT ROW_COUNT = 3
SELECT
  dfy.fiscal_year,
  dfy.filing_deadline,
  dfy.total_filings,
  dfy.total_amendments,
  dfy.audit_flag_count
FROM tax.gold.dim_fiscal_year dfy
ORDER BY dfy.fiscal_year;

-- ===================== QUERY 7: Year-over-Year Tax Revenue Trend =====================
-- Uses LAG to compute YoY change per jurisdiction

ASSERT VALUE total_effective_tax > 0
SELECT
  dj.jurisdiction_name,
  ff.fiscal_year,
  SUM(ff.effective_tax_owed)                                     AS total_effective_tax,
  LAG(SUM(ff.effective_tax_owed)) OVER (
      PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year
  )                                                               AS prev_year_tax,
  ROUND(
      100.0 * (SUM(ff.effective_tax_owed)
          - LAG(SUM(ff.effective_tax_owed)) OVER (PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year))
      / NULLIF(LAG(SUM(ff.effective_tax_owed)) OVER (PARTITION BY dj.jurisdiction_name ORDER BY ff.fiscal_year), 0),
      2
  )                                                               AS yoy_change_pct
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
GROUP BY dj.jurisdiction_name, ff.fiscal_year
ORDER BY dj.jurisdiction_name, ff.fiscal_year;

-- ===================== QUERY 8: Income Distribution by Bracket with NTILE =====================

ASSERT ROW_COUNT > 0
SELECT
  dt.income_bracket,
  NTILE(4) OVER (ORDER BY ff.gross_income)                       AS income_quartile,
  ff.gross_income,
  ff.effective_tax_owed,
  ff.effective_tax_rate,
  dt.filing_type,
  ff.was_amended
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
WHERE ff.fiscal_year = 2023
ORDER BY ff.gross_income;

-- ===================== QUERY 9: Compliance Rate by Jurisdiction =====================

ASSERT VALUE total_filings > 0
SELECT
  dj.jurisdiction_name,
  dj.jurisdiction_level,
  COUNT(*)                                                       AS total_filings,
  SUM(CASE WHEN ff.filing_status = 'Accepted' THEN 1 ELSE 0 END) AS accepted,
  SUM(CASE WHEN ff.filing_status = 'Filed' THEN 1 ELSE 0 END)    AS filed,
  SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END)  AS audited,
  SUM(CASE WHEN ff.filing_status = 'Under Review' THEN 1 ELSE 0 END) AS under_review,
  SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END)         AS amended,
  ROUND(
      100.0 * SUM(CASE WHEN ff.filing_status IN ('Accepted', 'Filed') THEN 1 ELSE 0 END) / COUNT(*),
      2
  )                                                               AS compliance_rate_pct
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
GROUP BY dj.jurisdiction_name, dj.jurisdiction_level
ORDER BY total_filings DESC;

-- ===================== QUERY 10: Top Taxpayers by Total Tax Contribution =====================

ASSERT VALUE total_effective_tax_contributed > 0
SELECT
  dt.taxpayer_id,
  dt.filing_type,
  dt.state,
  dt.ever_audited,
  SUM(ff.effective_tax_owed)                                     AS total_effective_tax_contributed,
  COUNT(DISTINCT ff.fiscal_year)                                 AS years_filed,
  ROUND(AVG(ff.gross_income), 2)                                AS avg_gross_income,
  SUM(ff.amendment_count)                                        AS total_amendments,
  PERCENT_RANK() OVER (ORDER BY SUM(ff.effective_tax_owed))     AS tax_percentile
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
GROUP BY dt.taxpayer_id, dt.filing_type, dt.state, dt.ever_audited
ORDER BY total_effective_tax_contributed DESC;

-- ===================== QUERY 11: Audit Trail Completeness =====================

ASSERT ROW_COUNT >= 50
SELECT
  at.table_name,
  at.operation,
  COUNT(*)                                                       AS record_count,
  MIN(at.change_timestamp)                                       AS earliest,
  MAX(at.change_timestamp)                                       AS latest
FROM tax.silver.audit_trail at
GROUP BY at.table_name, at.operation
ORDER BY at.table_name, at.operation;

-- ===================== QUERY 12: Refund vs Tax Owed Analysis by Filing Type =====================

ASSERT VALUE filings > 0
SELECT
  ff.fiscal_year,
  dt.filing_type,
  COUNT(*)                                                       AS filings,
  SUM(CASE WHEN ff.refund_amount > 0 THEN 1 ELSE 0 END)       AS got_refund,
  SUM(CASE WHEN ff.refund_amount < 0 THEN 1 ELSE 0 END)       AS owed_more,
  ROUND(AVG(ff.refund_amount), 2)                               AS avg_refund,
  ROUND(AVG(ff.effective_tax_owed), 2)                          AS avg_effective_tax
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_taxpayer dt ON ff.taxpayer_key = dt.taxpayer_key
GROUP BY ff.fiscal_year, dt.filing_type
ORDER BY ff.fiscal_year, dt.filing_type;

-- ===================== FINAL: Summary assertion =====================

ASSERT ROW_COUNT >= 50
SELECT COUNT(*) AS row_count FROM tax.gold.fact_filings;
