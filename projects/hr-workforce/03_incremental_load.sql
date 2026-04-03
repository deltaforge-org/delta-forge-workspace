-- =============================================================================
-- HR Workforce Analytics Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new compensation events, apply SCD2 updates,
-- refresh gold fact and KPIs
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: "event_id > 'CE-055' AND event_date > '2025-03-29'" or "1=1" if empty
-- ============================================================================

PRINT {{INCREMENTAL_FILTER(hr.silver.comp_events_enriched, event_id, event_date, 3)}};

-- ===================== STEP 1: capture_watermark =====================

STEP capture_watermark
  TIMEOUT '2m'
AS
  SELECT MAX(event_date) AS last_watermark
  FROM hr.silver.comp_events_enriched;

  SELECT COUNT(*) AS pre_scd2_count FROM hr.silver.employee_dim;
  SELECT COUNT(*) AS pre_fact_count FROM hr.gold.fact_compensation;

-- ===================== STEP 2: insert_new_bronze_events =====================
-- Simulating Q1 2025 annual review cycle: 4 raises + 2 promotions

STEP insert_new_bronze_events
  DEPENDS ON (capture_watermark)
AS
  INSERT INTO hr.bronze.raw_comp_events VALUES
      ('CE-056', 'EMP-002', 'DEPT-ENG',  'POS-SSE',  '2025-04-01', 'promotion',       140000.00, 10000.00, 4.5, 'Promoted SE2 to SSE',         '2025-04-01T06:00:00'),
      ('CE-057', 'EMP-018', 'DEPT-ENG',  'POS-SE2',  '2025-04-01', 'salary_adjustment',115000.00, 5000.00,  4.0, '6.5% merit increase',         '2025-04-01T06:00:00'),
      ('CE-058', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2025-04-01', 'salary_adjustment', 66150.00, 7000.00,  4.5, '5% + commission bonus',       '2025-04-01T06:00:00'),
      ('CE-059', 'EMP-020', 'DEPT-OPS',  'POS-OM1',  '2025-04-01', 'promotion',        95000.00,  5000.00,  4.0, 'Promoted OA1 to OM1',         '2025-04-01T06:00:00');

  ASSERT ROW_COUNT = 4
  SELECT COUNT(*) AS new_events
  FROM hr.bronze.raw_comp_events
  WHERE ingested_at > '2025-03-01T00:00:00';

-- ===================== STEP 3: scd2_expire_old =====================
-- Two-pass SCD2 MERGE - Pass 1: Expire existing current records

STEP scd2_expire_old
  DEPENDS ON (insert_new_bronze_events)
  TIMEOUT '3m'
AS
  MERGE INTO hr.silver.employee_dim AS tgt
  USING (
      SELECT DISTINCT
          ce.employee_id,
          ce.department_id AS new_dept,
          ce.position_id   AS new_pos,
          ce.base_salary   AS new_salary,
          CAST(ce.event_date AS DATE) AS change_date
      FROM hr.bronze.raw_comp_events ce
      WHERE ce.ingested_at > '2025-03-01T00:00:00'
        AND ce.event_type IN ('promotion', 'salary_adjustment', 'transfer')
  ) AS changes
  ON tgt.employee_id = changes.employee_id
     AND tgt.is_current = true
     AND (tgt.department_id != changes.new_dept
          OR tgt.position_id != changes.new_pos
          OR tgt.base_salary != changes.new_salary)
  WHEN MATCHED THEN UPDATE SET
      valid_to   = changes.change_date,
      is_current = false;

-- ===================== STEP 4: scd2_insert_new =====================
-- Two-pass SCD2 MERGE - Pass 2: Insert new current versions

STEP scd2_insert_new
  DEPENDS ON (scd2_expire_old)
AS
  INSERT INTO hr.silver.employee_dim
  SELECT
      200 + ROW_NUMBER() OVER (ORDER BY ce.employee_id, ce.event_date) AS surrogate_key,
      ce.employee_id,
      TRIM(e.employee_name) AS employee_name,
      e.ssn,
      e.email,
      CAST(e.hire_date AS DATE) AS hire_date,
      CAST(e.termination_date AS DATE) AS termination_date,
      ce.department_id,
      ce.position_id,
      e.education_level,
      e.gender,
      CAST(e.date_of_birth AS DATE) AS date_of_birth,
      ce.base_salary,
      e.status,
      CAST(ce.event_date AS DATE) AS valid_from,
      CAST(NULL AS DATE) AS valid_to,
      true AS is_current
  FROM hr.bronze.raw_comp_events ce
  JOIN hr.bronze.raw_employees e ON ce.employee_id = e.employee_id
  WHERE ce.ingested_at > '2025-03-01T00:00:00'
    AND ce.event_type IN ('promotion', 'salary_adjustment', 'transfer');

  ASSERT ROW_COUNT = 4
  SELECT COUNT(*) AS new_scd2_rows
  FROM hr.silver.employee_dim
  WHERE valid_from = CAST('2025-04-01' AS DATE);

-- ===================== STEP 5: incremental_enrich_comp_events =====================

STEP incremental_enrich_comp_events
  DEPENDS ON (scd2_insert_new)
  TIMEOUT '3m'
AS
  INSERT INTO hr.silver.comp_events_enriched
  SELECT
      ce.event_id,
      ce.employee_id,
      ce.department_id,
      ce.position_id,
      CAST(ce.event_date AS DATE) AS event_date,
      ce.event_type,
      ce.base_salary,
      ce.bonus,
      ce.base_salary + ce.bonus AS total_comp,
      CASE
          WHEN prev.base_salary IS NOT NULL AND prev.base_salary > 0
          THEN ROUND(100.0 * (ce.base_salary - prev.base_salary) / prev.base_salary, 2)
          ELSE 0.00
      END AS salary_change_pct,
      ce.performance_rating,
      ROUND(
          ce.base_salary / NULLIF((p.pay_grade_min + p.pay_grade_max) / 2, 0),
          3
      ) AS compa_ratio,
      CURRENT_TIMESTAMP AS processed_at
  FROM hr.bronze.raw_comp_events ce
  LEFT JOIN (
      SELECT employee_id, base_salary, event_date,
             LEAD(event_date) OVER (PARTITION BY employee_id ORDER BY event_date) AS next_event_date
      FROM hr.bronze.raw_comp_events
  ) prev ON ce.employee_id = prev.employee_id AND ce.event_date = prev.next_event_date
  JOIN hr.bronze.raw_positions p ON ce.position_id = p.position_id
  WHERE ce.ingested_at > '2025-03-01T00:00:00';

  ASSERT ROW_COUNT = 4
  SELECT COUNT(*) AS new_enriched
  FROM hr.silver.comp_events_enriched
  WHERE event_date >= CAST('2025-04-01' AS DATE);

-- ===================== STEP 6: verify_no_duplicates =====================

STEP verify_no_duplicates
  DEPENDS ON (incremental_enrich_comp_events)
AS
  ASSERT ROW_COUNT = 0
  SELECT event_id, COUNT(*) AS cnt
  FROM hr.silver.comp_events_enriched
  GROUP BY event_id
  HAVING COUNT(*) > 1;

-- ===================== STEP 7: refresh_gold_fact =====================

STEP refresh_gold_fact
  DEPENDS ON (verify_no_duplicates)
  TIMEOUT '5m'
AS
  MERGE INTO hr.gold.fact_compensation AS tgt
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ce.event_id) + 55 AS event_key,
          ed.surrogate_key AS employee_key,
          dd.department_key,
          dp.position_key,
          ce.event_date,
          ce.event_type,
          ce.base_salary,
          ce.bonus,
          ce.total_comp,
          ce.salary_change_pct,
          ce.performance_rating,
          ce.compa_ratio
      FROM hr.silver.comp_events_enriched ce
      JOIN hr.silver.employee_dim ed
          ON ce.employee_id = ed.employee_id AND ed.is_current = true
      JOIN hr.gold.dim_department dd ON ce.department_id = dd.department_id
      JOIN hr.gold.dim_position dp ON ce.position_id = dp.position_id
      WHERE ce.event_date >= CAST('2025-04-01' AS DATE)
  ) AS src
  ON tgt.event_key = src.event_key
  WHEN NOT MATCHED THEN INSERT (
      event_key, employee_key, department_key, position_key, event_date,
      event_type, base_salary, bonus, total_comp, salary_change_pct,
      performance_rating, compa_ratio, loaded_at
  ) VALUES (
      src.event_key, src.employee_key, src.department_key, src.position_key,
      src.event_date, src.event_type, src.base_salary, src.bonus, src.total_comp,
      src.salary_change_pct, src.performance_rating, src.compa_ratio, CURRENT_TIMESTAMP
  );

  ASSERT ROW_COUNT >= 59
  SELECT COUNT(*) AS row_count FROM hr.gold.fact_compensation;

-- ===================== STEP 8: optimize =====================

STEP optimize
  DEPENDS ON (refresh_gold_fact)
  CONTINUE ON FAILURE
AS
  OPTIMIZE hr.silver.employee_dim;
  OPTIMIZE hr.silver.comp_events_enriched;
  OPTIMIZE hr.gold.fact_compensation;
