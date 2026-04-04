-- =============================================================================
-- Government Tax Filing Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new filings and amendments arriving after the
-- initial full load.  Uses INCREMENTAL_FILTER macro, CDF tracking on
-- filings_immutable, and rebuilds gold KPIs.
-- =============================================================================
-- 10-step DAG: check_watermark -> ingest_new_bronze -> append_new_filings +
-- apply_new_amendments (parallel) -> log_incremental_audit ->
-- merge_fact_filings -> rebuild_kpi_revenue + rebuild_kpi_preparer (parallel)
-- -> incremental_optimize
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "filing_id > 'FIL-2024-012' AND filing_date > '2025-03-25'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

PRINT {{INCREMENTAL_FILTER(tax.silver.filings_immutable, filing_id, filing_date, 7)}};

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE tax_incremental_schedule CRON '0 */6 * * *' TIMEZONE 'America/New_York' RETRIES 2 TIMEOUT 1800 MAX_CONCURRENT 1 INACTIVE;

PIPELINE tax_incremental_pipeline DESCRIPTION 'Incremental tax filing pipeline: appends new filings, applies new amendments, rebuilds KPIs' SCHEDULE 'tax_incremental_schedule' TAGS 'government,tax,incremental,CDF' SLA 1800 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP 1: check_watermark =====================

STEP check_watermark
  TIMEOUT '1m'
AS
  SELECT MAX(filing_date) AS last_filing_watermark
  FROM tax.silver.filings_immutable;

  SELECT MAX(amendment_date) AS last_amendment_watermark
  FROM tax.silver.amendments_applied;

-- ===================== STEP 2: ingest_new_bronze =====================
-- Simulate late FY2024 filings and new amendments arriving after cutoff.

STEP ingest_new_bronze
  DEPENDS ON (check_watermark)
  TIMEOUT '3m'
AS
  -- 4 new late filings
  INSERT INTO tax.bronze.raw_filings VALUES
      ('FIL-2024-013', 'TP-1007', 'JUR-FED', 'PREP-04', 2024, '2025-04-10', 69000.00,  13850.00,  55150.00,  6893.50, 7100.00,   206.50, 'Filed', '1040', NULL, '2025-04-10T01:00:00'),
      ('FIL-2024-014', 'TP-1008', 'JUR-FED', 'PREP-01', 2024, '2025-04-08', 80000.00,  13850.00,  66150.00,  8873.50, 9200.00,   326.50, 'Filed', '1040', NULL, '2025-04-10T01:00:00'),
      ('FIL-2024-015', 'TP-1009', 'JUR-FED', 'PREP-04', 2024, '2025-04-12', 53000.00,  13850.00,  39150.00,  4479.50, 4800.00,   320.50, 'Filed', '1040', NULL, '2025-04-12T01:00:00'),
      ('FIL-2024-016', 'TP-2005', 'JUR-FED', 'PREP-03', 2024, '2025-04-14', 720000.00, 310000.00, 410000.00, 90200.00,90500.00,  300.00, 'Filed', '1120', NULL, '2025-04-14T01:00:00');

  -- 2 new amendments
  INSERT INTO tax.bronze.raw_amendments VALUES
      ('AMD-013', 'FIL-2024-002', 'TP-1002', '2025-04-20', 160000.00, 60000.00, 100000.00, 17200.00, 'Deductions reduced by IRS review', 'PREP-02', '2025-04-20T01:00:00'),
      ('AMD-014', 'FIL-2024-004', 'TP-1005', '2025-04-22', 240000.00, 88000.00, 152000.00, 33440.00, 'Fourth consecutive year audit amendment', 'PREP-03', '2025-04-22T01:00:00');

-- ===================== STEP 3: append_new_filings =====================
-- Only append filings that arrived after the last ingestion.

STEP append_new_filings
  DEPENDS ON (ingest_new_bronze)
  TIMEOUT '5m'
AS
  INSERT INTO tax.silver.filings_immutable
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
      f.filing_type,
      CASE WHEN f.gross_income > 0 THEN ROUND(f.tax_owed / f.gross_income, 4) ELSE 0.0000 END,
      CASE WHEN f.gross_income > 0 THEN ROUND(100.0 * f.deductions / f.gross_income, 2) ELSE 0.00 END,
      CASE WHEN f.gross_income > 0 AND (f.deductions / f.gross_income) > 0.40 THEN true ELSE false END,
      CASE
          WHEN f.gross_income < 50000                    THEN 'Under $50K'
          WHEN f.gross_income BETWEEN 50000 AND 100000   THEN '$50K-$100K'
          WHEN f.gross_income BETWEEN 100001 AND 250000  THEN '$100K-$250K'
          WHEN f.gross_income BETWEEN 250001 AND 500000  THEN '$250K-$500K'
          WHEN f.gross_income BETWEEN 500001 AND 1000000 THEN '$500K-$1M'
          ELSE 'Over $1M'
      END,
      CURRENT_TIMESTAMP
  FROM tax.bronze.raw_filings f
  WHERE {{INCREMENTAL_FILTER(tax.silver.filings_immutable, filing_id, filing_date, 7)}};

-- ===================== STEP 4: apply_new_amendments =====================

STEP apply_new_amendments
  DEPENDS ON (ingest_new_bronze)
  TIMEOUT '3m'
AS
  MERGE INTO tax.silver.amendments_applied AS tgt
  USING (
      SELECT
          a.amendment_id,
          a.original_filing_id,
          a.taxpayer_id,
          CAST(a.amendment_date AS DATE)                             AS amendment_date,
          f.taxable_income                                           AS original_taxable_income,
          a.amended_taxable_income,
          a.amended_taxable_income - f.taxable_income                AS income_delta,
          f.tax_owed                                                 AS original_tax_owed,
          a.amended_tax_owed,
          a.amended_tax_owed - f.tax_owed                            AS tax_delta,
          a.amendment_reason,
          CASE
              WHEN f.taxable_income > 0
              THEN ROUND(100.0 * ABS(a.amended_taxable_income - f.taxable_income) / f.taxable_income, 2)
              ELSE 0.00
          END                                                        AS delta_pct,
          CASE
              WHEN f.taxable_income > 0
               AND ABS(a.amended_taxable_income - f.taxable_income) / f.taxable_income > 0.20
              THEN true ELSE false
          END                                                        AS large_delta_flag,
          a.preparer_id,
          CURRENT_TIMESTAMP                                          AS loaded_at
      FROM tax.bronze.raw_amendments a
      JOIN tax.bronze.raw_filings f ON a.original_filing_id = f.filing_id
      WHERE a.ingested_at > '2025-04-01T00:00:00'
  ) AS src
  ON tgt.amendment_id = src.amendment_id
  WHEN NOT MATCHED THEN INSERT (
      amendment_id, original_filing_id, taxpayer_id, amendment_date,
      original_taxable_income, amended_taxable_income, income_delta,
      original_tax_owed, amended_tax_owed, tax_delta,
      amendment_reason, delta_pct, large_delta_flag, preparer_id, loaded_at
  ) VALUES (
      src.amendment_id, src.original_filing_id, src.taxpayer_id, src.amendment_date,
      src.original_taxable_income, src.amended_taxable_income, src.income_delta,
      src.original_tax_owed, src.amended_tax_owed, src.tax_delta,
      src.amendment_reason, src.delta_pct, src.large_delta_flag, src.preparer_id, src.loaded_at
  );

-- ===================== STEP 5: log_incremental_audit =====================

STEP log_incremental_audit
  DEPENDS ON (append_new_filings, apply_new_amendments)
  TIMEOUT '2m'
AS
  INSERT INTO tax.silver.audit_trail
  SELECT
      (SELECT COALESCE(MAX(audit_id), 0) FROM tax.silver.audit_trail)
          + ROW_NUMBER() OVER (ORDER BY fi.filing_id)               AS audit_id,
      'filings_immutable'                                            AS table_name,
      'INCREMENTAL_INSERT'                                           AS operation,
      fi.filing_id                                                   AS record_key,
      fi.taxpayer_id,
      fi.fiscal_year,
      fi.loaded_at                                                   AS change_timestamp,
      CONCAT('Incremental Gross=', CAST(fi.gross_income AS STRING)) AS details
  FROM tax.silver.filings_immutable fi
  WHERE fi.filing_date > (
      SELECT COALESCE(MAX(CAST(SUBSTRING(details, 18) AS DATE)), CAST('2025-01-01' AS DATE))
      FROM tax.silver.audit_trail
      WHERE table_name = 'filings_immutable' AND operation = 'INSERT'
  );

-- ===================== STEP 6: merge_fact_filings =====================
-- Rebuild fact for new filings only (MERGE to handle re-runs safely).

STEP merge_fact_filings
  DEPENDS ON (log_incremental_audit)
  TIMEOUT '5m'
AS
  MERGE INTO tax.gold.fact_filings AS tgt
  USING (
      SELECT
          (SELECT COALESCE(MAX(filing_key), 0) FROM tax.gold.fact_filings)
              + ROW_NUMBER() OVER (ORDER BY fi.filing_id)           AS filing_key,
          dt.taxpayer_key,
          dj.jurisdiction_key,
          dp.preparer_key,
          dfy.fiscal_year_key,
          fi.fiscal_year,
          fi.filing_date,
          fi.gross_income,
          fi.deductions,
          fi.taxable_income,
          fi.tax_owed,
          fi.tax_paid,
          fi.refund_amount,
          COALESCE(aa.amended_taxable_income, fi.taxable_income)    AS effective_taxable_income,
          COALESCE(aa.amended_tax_owed, fi.tax_owed)                AS effective_tax_owed,
          CASE WHEN aa.amendment_id IS NOT NULL THEN true ELSE false END AS was_amended,
          aa.amendment_reason,
          COALESCE(ac.amendment_count, 0)                            AS amendment_count,
          CASE
              WHEN fi.gross_income > 0
              THEN ROUND(COALESCE(aa.amended_tax_owed, fi.tax_owed) / fi.gross_income, 4)
              ELSE 0.0000
          END                                                        AS effective_tax_rate,
          fi.audit_flag,
          fi.filing_status
      FROM tax.silver.filings_immutable fi
      JOIN tax.gold.dim_taxpayer dt      ON fi.taxpayer_id = dt.taxpayer_id
      JOIN tax.gold.dim_jurisdiction dj  ON fi.jurisdiction_id = dj.jurisdiction_id
      LEFT JOIN tax.gold.dim_preparer dp ON fi.preparer_id = dp.preparer_id
      JOIN tax.gold.dim_fiscal_year dfy  ON fi.fiscal_year = dfy.fiscal_year
      LEFT JOIN (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY original_filing_id ORDER BY amendment_date DESC) AS rn
          FROM tax.silver.amendments_applied
      ) aa ON fi.filing_id = aa.original_filing_id AND aa.rn = 1
      LEFT JOIN (
          SELECT original_filing_id, COUNT(*) AS amendment_count
          FROM tax.silver.amendments_applied
          GROUP BY original_filing_id
      ) ac ON fi.filing_id = ac.original_filing_id
      WHERE fi.filing_date > '2025-04-01'
  ) AS src
  ON tgt.filing_key = src.filing_key
  WHEN NOT MATCHED THEN INSERT (
      filing_key, taxpayer_key, jurisdiction_key, preparer_key, fiscal_year_key,
      fiscal_year, filing_date, gross_income, deductions, taxable_income,
      tax_owed, tax_paid, refund_amount, effective_taxable_income, effective_tax_owed,
      was_amended, amendment_reason, amendment_count, effective_tax_rate, audit_flag, filing_status
  ) VALUES (
      src.filing_key, src.taxpayer_key, src.jurisdiction_key, src.preparer_key, src.fiscal_year_key,
      src.fiscal_year, src.filing_date, src.gross_income, src.deductions, src.taxable_income,
      src.tax_owed, src.tax_paid, src.refund_amount, src.effective_taxable_income, src.effective_tax_owed,
      src.was_amended, src.amendment_reason, src.amendment_count, src.effective_tax_rate,
      src.audit_flag, src.filing_status
  );

-- ===================== STEP 7: rebuild_kpi_revenue =====================

STEP rebuild_kpi_revenue
  DEPENDS ON (merge_fact_filings)
  TIMEOUT '3m'
AS
  DELETE FROM tax.gold.kpi_revenue_analysis WHERE 1=1;

  INSERT INTO tax.gold.kpi_revenue_analysis
  SELECT
      ff.fiscal_year,
      dj.jurisdiction_name,
      COUNT(*),
      SUM(ff.effective_taxable_income),
      SUM(ff.effective_tax_owed),
      SUM(CASE WHEN ff.refund_amount > 0 THEN ff.refund_amount ELSE 0 END),
      ROUND(AVG(ff.effective_tax_rate), 4),
      SUM(CASE WHEN ff.audit_flag = true THEN 1 ELSE 0 END),
      ROUND(100.0 * SUM(CASE WHEN ff.filing_status IN ('Accepted', 'Filed') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
      SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END),
      ROUND(100.0 * SUM(CASE WHEN ff.audit_flag = true AND ff.was_amended = true
                         THEN ff.effective_tax_owed - ff.tax_owed ELSE 0 END)
            / NULLIF(SUM(CASE WHEN ff.audit_flag = true THEN ff.tax_owed ELSE 0 END), 0), 2)
  FROM tax.gold.fact_filings ff
  JOIN tax.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
  GROUP BY ff.fiscal_year, dj.jurisdiction_name;

-- ===================== STEP 8: rebuild_kpi_preparer =====================

STEP rebuild_kpi_preparer
  DEPENDS ON (merge_fact_filings)
  TIMEOUT '3m'
AS
  DELETE FROM tax.gold.kpi_preparer_quality WHERE 1=1;

  INSERT INTO tax.gold.kpi_preparer_quality
  SELECT
      dp.preparer_name,
      dp.firm,
      dp.certification,
      COUNT(*),
      SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END),
      ROUND(100.0 * SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
      SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END),
      ROUND(100.0 * SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
      ROUND(AVG(ff.gross_income), 2),
      ROUND(AVG(CASE WHEN ff.gross_income > 0 THEN 100.0 * ff.deductions / ff.gross_income ELSE 0 END), 2),
      ROUND(AVG(ff.effective_tax_rate), 4)
  FROM tax.gold.fact_filings ff
  JOIN tax.gold.dim_preparer dp ON ff.preparer_key = dp.preparer_key
  GROUP BY dp.preparer_name, dp.firm, dp.certification;

-- ===================== STEP 9: incremental_optimize =====================

STEP incremental_optimize
  DEPENDS ON (rebuild_kpi_revenue, rebuild_kpi_preparer)
  TIMEOUT '5m'
  CONTINUE ON FAILURE
AS
  OPTIMIZE tax.silver.filings_immutable;
  OPTIMIZE tax.gold.fact_filings;
  OPTIMIZE tax.gold.kpi_revenue_analysis;
  OPTIMIZE tax.gold.kpi_preparer_quality;
