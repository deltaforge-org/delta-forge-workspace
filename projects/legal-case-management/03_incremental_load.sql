-- =============================================================================
-- Legal Case Management Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new billing entries, refresh silver/gold layers,
-- re-run graph analytics on updated network
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: "billing_id > 'B070' AND billing_date > '2024-01-12'" or "1=1" if empty
-- ============================================================================

PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.billings_validated, billing_id, billing_date, 3)}};

-- ===================== STEP 1: capture_watermarks =====================

STEP capture_watermarks
  TIMEOUT '2m'
AS
  SELECT COUNT(*) AS pre_silver_billing_count FROM {{zone_prefix}}.silver.billings_validated;
  SELECT COUNT(*) AS pre_fact_count FROM {{zone_prefix}}.gold.fact_billings;
  SELECT MAX(processed_at) AS current_watermark FROM {{zone_prefix}}.silver.billings_validated;

-- ===================== STEP 2: insert_new_bronze =====================
-- 8 new billing entries arriving after the initial load

STEP insert_new_bronze
  DEPENDS ON (capture_watermarks)
AS
  INSERT INTO {{zone_prefix}}.bronze.raw_billings VALUES
      ('B071', 'C001', 'A001', '2024-02-10', 7.0,  650.00, 4550.00,  true,  'billable',  'Summary judgment motion',                '2024-02-15T00:00:00'),
      ('B072', 'C006', 'A001', '2024-02-15', 9.5,  650.00, 6175.00,  true,  'billable',  'Sentencing hearing preparation',          '2024-02-15T00:00:00'),
      ('B073', 'C009', 'A005', '2024-02-20', 5.0,  620.00, 3100.00,  true,  'billable',  'Post-merger integration review',          '2024-02-15T00:00:00'),
      ('B074', 'C012', 'A003', '2024-02-25', 8.0,  580.00, 4640.00,  true,  'billable',  'Trial preparation - patent case',         '2024-02-15T00:00:00'),
      ('B075', 'C015', 'A004', '2024-02-10', 4.5,  395.00, 1777.50,  true,  'billable',  'Demand letter to employer',               '2024-02-15T00:00:00'),
      ('B076', 'C008', 'A001', '2024-02-18', 6.0,  650.00, 3900.00,  true,  'billable',  'Appeal brief finalization',               '2024-02-15T00:00:00'),
      ('B077', 'C011', 'A007', '2024-02-12', 4.0,  520.00, 2080.00,  true,  'billable',  'Shareholder meeting prep',                '2024-02-15T00:00:00'),
      ('B078', 'C005', 'A002', '2024-02-22', 5.5,  475.00, 2612.50,  true,  'billable',  'Closing arguments draft',                 '2024-02-15T00:00:00');

  ASSERT ROW_COUNT = 8
  SELECT COUNT(*) AS new_rows
  FROM {{zone_prefix}}.bronze.raw_billings
  WHERE ingested_at > '2024-02-01T00:00:00';

-- ===================== STEP 3: incremental_enrich_cases =====================
-- Re-compute case enrichment for affected cases

STEP incremental_enrich_cases
  DEPENDS ON (insert_new_bronze)
  TIMEOUT '3m'
AS
  MERGE INTO {{zone_prefix}}.silver.cases_enriched AS target
  USING (
      SELECT
          c.case_id,
          c.case_number,
          c.case_type,
          c.court,
          c.filing_date,
          c.close_date,
          c.status,
          c.priority,
          DATEDIFF(COALESCE(c.close_date, CAST('2024-06-01' AS DATE)), c.filing_date) AS duration_days,
          COUNT(DISTINCT b.attorney_id) AS attorney_count,
          0 AS party_count,
          SUM(b.hours) AS total_billed_hours,
          SUM(CASE WHEN b.billable_flag = true THEN b.amount ELSE 0 END) AS total_billed_amount,
          LEAST(10.0, GREATEST(1.0, ROUND(
              LN(GREATEST(1, SUM(b.hours) + 1))
              * SQRT(CAST(COUNT(DISTINCT b.attorney_id) AS DOUBLE))
              / 1.5
          , 2))) AS complexity_score
      FROM {{zone_prefix}}.bronze.raw_cases c
      JOIN {{zone_prefix}}.bronze.raw_billings b ON c.case_id = b.case_id
      WHERE c.case_id IN (
          SELECT DISTINCT case_id FROM {{zone_prefix}}.bronze.raw_billings
          WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.billings_validated, billing_id, billing_date, 3)}}
      )
      GROUP BY c.case_id, c.case_number, c.case_type, c.court, c.filing_date,
               c.close_date, c.status, c.priority
  ) AS source
  ON target.case_id = source.case_id
  WHEN MATCHED THEN UPDATE SET
      total_billed_hours  = source.total_billed_hours,
      total_billed_amount = source.total_billed_amount,
      complexity_score    = source.complexity_score,
      attorney_count      = source.attorney_count,
      processed_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      case_id, case_number, case_type, court, filing_date, close_date, status,
      priority, duration_days, attorney_count, party_count, total_billed_hours,
      total_billed_amount, complexity_score, processed_at
  ) VALUES (
      source.case_id, source.case_number, source.case_type, source.court,
      source.filing_date, source.close_date, source.status, source.priority,
      source.duration_days, source.attorney_count, source.party_count,
      source.total_billed_hours, source.total_billed_amount, source.complexity_score,
      CURRENT_TIMESTAMP
  );

-- ===================== STEP 4: incremental_validate_billings =====================

STEP incremental_validate_billings
  DEPENDS ON (incremental_enrich_cases)
  TIMEOUT '3m'
AS
  INSERT INTO {{zone_prefix}}.silver.billings_validated
  SELECT
      b.billing_id,
      b.case_id,
      b.attorney_id,
      b.billing_date,
      b.hours,
      b.hourly_rate,
      b.amount,
      b.billable_flag,
      b.billing_type,
      c.case_type,
      a.practice_group,
      a.partner_flag,
      ce.complexity_score,
      CURRENT_TIMESTAMP AS processed_at
  FROM {{zone_prefix}}.bronze.raw_billings b
  JOIN {{zone_prefix}}.bronze.raw_cases c ON b.case_id = c.case_id
  JOIN {{zone_prefix}}.bronze.raw_attorneys a ON b.attorney_id = a.attorney_id
  LEFT JOIN {{zone_prefix}}.silver.cases_enriched ce ON b.case_id = ce.case_id
  WHERE b.hours > 0
    AND b.hourly_rate > 0
    AND {{INCREMENTAL_FILTER({{zone_prefix}}.silver.billings_validated, billing_id, billing_date, 3)}};

  ASSERT ROW_COUNT = 8
  SELECT COUNT(*) AS new_billings
  FROM {{zone_prefix}}.silver.billings_validated
  WHERE processed_at > (
      SELECT MAX(processed_at) - INTERVAL '1' MINUTE
      FROM {{zone_prefix}}.silver.billings_validated
  );

-- ===================== STEP 5: verify_no_duplicates =====================

STEP verify_no_duplicates
  DEPENDS ON (incremental_validate_billings)
AS
  ASSERT ROW_COUNT = 0
  SELECT billing_id, COUNT(*) AS cnt
  FROM {{zone_prefix}}.silver.billings_validated
  GROUP BY billing_id
  HAVING COUNT(*) > 1;

-- ===================== STEP 6: refresh_gold_dims =====================

STEP refresh_gold_dims
  DEPENDS ON (verify_no_duplicates)
  TIMEOUT '3m'
AS
  MERGE INTO {{zone_prefix}}.gold.dim_case AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ce.case_id) AS case_key,
          ce.case_id, ce.case_number, ce.case_type, ce.court, ce.filing_date,
          ce.close_date, ce.status, ce.priority, ce.complexity_score
      FROM {{zone_prefix}}.silver.cases_enriched ce
  ) AS source
  ON target.case_id = source.case_id
  WHEN MATCHED THEN UPDATE SET
      complexity_score = source.complexity_score,
      status           = source.status,
      loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      case_key, case_id, case_number, case_type, court, filing_date,
      close_date, status, priority, complexity_score, loaded_at
  ) VALUES (
      source.case_key, source.case_id, source.case_number, source.case_type,
      source.court, source.filing_date, source.close_date, source.status,
      source.priority, source.complexity_score, CURRENT_TIMESTAMP
  );

-- ===================== STEP 7: refresh_fact_billings =====================

STEP refresh_fact_billings
  DEPENDS ON (refresh_gold_dims)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.gold.fact_billings AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY bv.billing_date, bv.billing_id) AS billing_key,
          dc.case_key,
          da.attorney_key,
          bv.billing_date,
          bv.hours,
          bv.hourly_rate,
          bv.amount,
          bv.billable_flag,
          bv.complexity_score AS case_complexity
      FROM {{zone_prefix}}.silver.billings_validated bv
      JOIN {{zone_prefix}}.gold.dim_case dc ON bv.case_id = dc.case_id
      JOIN {{zone_prefix}}.gold.dim_attorney da ON bv.attorney_id = da.attorney_id
  ) AS source
  ON target.case_key = source.case_key
     AND target.attorney_key = source.attorney_key
     AND target.billing_date = source.billing_date
     AND target.hours = source.hours
  WHEN MATCHED THEN UPDATE SET
      amount          = source.amount,
      case_complexity = source.case_complexity,
      loaded_at       = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      billing_key, case_key, attorney_key, billing_date, hours,
      hourly_rate, amount, billable_flag, case_complexity, loaded_at
  ) VALUES (
      source.billing_key, source.case_key, source.attorney_key,
      source.billing_date, source.hours, source.hourly_rate,
      source.amount, source.billable_flag, source.case_complexity,
      CURRENT_TIMESTAMP
  );

  ASSERT ROW_COUNT >= 78
  SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.gold.fact_billings;

-- ===================== STEP 8: refresh_kpi =====================

STEP refresh_kpi
  DEPENDS ON (refresh_fact_billings)
AS
  DELETE FROM {{zone_prefix}}.gold.kpi_firm_performance WHERE 1 = 1;

  INSERT INTO {{zone_prefix}}.gold.kpi_firm_performance
  SELECT
      da.attorney_id,
      da.attorney_name,
      da.practice_group,
      da.partner_flag,
      SUM(fb.hours) AS total_hours,
      SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END) AS billable_hours,
      ROUND(
          100.0 * SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END)
          / NULLIF(SUM(fb.hours), 0), 2
      ) AS utilization_pct,
      ROUND(SUM(fb.amount), 2) AS total_revenue,
      COUNT(DISTINCT fb.case_key) AS case_count,
      ROUND(AVG(fb.case_complexity), 2) AS avg_case_complexity,
      ROUND(
          SUM(fb.amount) / NULLIF(SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END), 0), 2
      ) AS revenue_per_hour,
      CURRENT_TIMESTAMP AS loaded_at
  FROM {{zone_prefix}}.gold.fact_billings fb
  JOIN {{zone_prefix}}.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
  GROUP BY da.attorney_id, da.attorney_name, da.practice_group, da.partner_flag;

-- ===================== STEP 9: optimize =====================

STEP optimize
  DEPENDS ON (refresh_kpi)
  CONTINUE ON FAILURE
AS
  OPTIMIZE {{zone_prefix}}.gold.fact_billings;
  OPTIMIZE {{zone_prefix}}.gold.kpi_firm_performance;
