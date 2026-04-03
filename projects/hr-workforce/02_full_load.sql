-- =============================================================================
-- HR Workforce Analytics Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE hr_daily_schedule
    CRON '0 6 * * *'
    TIMEZONE 'America/New_York'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE hr_workforce_pipeline
    DESCRIPTION 'Full load: transform workforce data with SCD2 tracking through medallion layers'
    SCHEDULE 'hr_daily_schedule'
    TAGS 'hr,workforce,scd2,full-load'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP: expire_scd2_employee =====================

STEP expire_scd2_employee
  TIMEOUT '5m'
AS
  -- Build initial SCD2 records from hire events — each employee gets one current record
  INSERT INTO {{zone_prefix}}.silver.dim_employee_scd2
  SELECT
      ROW_NUMBER() OVER (ORDER BY e.employee_id)              AS surrogate_key,
      e.employee_id,
      TRIM(e.employee_name)                                   AS employee_name,
      e.ssn,
      CAST(e.hire_date AS DATE)                               AS hire_date,
      CAST(e.termination_date AS DATE)                        AS termination_date,
      e.department_id,
      e.position_id,
      e.education_level,
      e.gender,
      CONCAT(CAST(FLOOR(EXTRACT(YEAR FROM CAST(e.date_of_birth AS DATE)) / 10) * 10 AS STRING), 's') AS age_band,
      ce.base_salary,
      CAST(e.hire_date AS DATE)                               AS valid_from,
      CAST(NULL AS DATE)                                      AS valid_to,
      true                                                     AS is_current
  FROM {{zone_prefix}}.bronze.raw_employees e
  JOIN (
      SELECT employee_id, base_salary,
             ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date) AS rn
      FROM {{zone_prefix}}.bronze.raw_comp_events
      WHERE event_type = 'Hire'
  ) ce ON e.employee_id = ce.employee_id AND ce.rn = 1;

  ASSERT ROW_COUNT = 20
  SELECT 'row count check' AS status;

  -- Pass 1: Expire existing current records where a change occurred
  MERGE INTO {{zone_prefix}}.silver.dim_employee_scd2 AS tgt
  USING (
      SELECT DISTINCT
          ce.employee_id,
          ce.department_id AS new_dept,
          ce.position_id   AS new_pos,
          ce.base_salary    AS new_salary,
          CAST(ce.event_date AS DATE) AS change_date
      FROM {{zone_prefix}}.bronze.raw_comp_events ce
      WHERE ce.event_type != 'Hire'
        AND ce.event_type != 'Termination'
  ) AS changes
  ON tgt.employee_id = changes.employee_id
     AND tgt.is_current = true
     AND (tgt.department_id != changes.new_dept
          OR tgt.position_id != changes.new_pos
          OR tgt.base_salary != changes.new_salary)
  WHEN MATCHED THEN UPDATE SET
      valid_to = changes.change_date,
      is_current = false;

-- ===================== STEP: insert_scd2_employee =====================

STEP insert_scd2_employee
  DEPENDS ON (expire_scd2_employee)
AS
  -- Pass 2: Insert new current versions for changed employees
  INSERT INTO {{zone_prefix}}.silver.dim_employee_scd2
  SELECT
      20 + ROW_NUMBER() OVER (ORDER BY ce.employee_id, ce.event_date) AS surrogate_key,
      ce.employee_id,
      e.employee_name,
      e.ssn,
      CAST(e.hire_date AS DATE),
      CAST(e.termination_date AS DATE),
      ce.department_id,
      ce.position_id,
      e.education_level,
      e.gender,
      CONCAT(CAST(FLOOR(EXTRACT(YEAR FROM CAST(e.date_of_birth AS DATE)) / 10) * 10 AS STRING), 's') AS age_band,
      ce.base_salary,
      CAST(ce.event_date AS DATE)                             AS valid_from,
      CAST(NULL AS DATE)                                      AS valid_to,
      true                                                     AS is_current
  FROM (
      SELECT employee_id, department_id, position_id, base_salary, event_date,
             ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date DESC) AS rn
      FROM {{zone_prefix}}.bronze.raw_comp_events
      WHERE event_type NOT IN ('Hire', 'Termination')
  ) ce
  JOIN {{zone_prefix}}.bronze.raw_employees e ON ce.employee_id = e.employee_id
  WHERE ce.rn = 1;

-- ===================== STEP: enrich_comp_events =====================

STEP enrich_comp_events
  DEPENDS ON (insert_scd2_employee)
AS
  -- Calculate total comp and salary change percentage
  INSERT INTO {{zone_prefix}}.silver.comp_events_enriched
  SELECT
      ce.event_id,
      ce.employee_id,
      ce.department_id,
      ce.position_id,
      CAST(ce.event_date AS DATE)                             AS event_date,
      ce.event_type,
      ce.base_salary,
      ce.bonus,
      ce.base_salary + ce.bonus                               AS total_comp,
      CASE
          WHEN prev.base_salary IS NOT NULL AND prev.base_salary > 0
          THEN ROUND(100.0 * (ce.base_salary - prev.base_salary) / prev.base_salary, 2)
          ELSE 0.00
      END                                                      AS salary_change_pct,
      ce.performance_rating
  FROM {{zone_prefix}}.bronze.raw_comp_events ce
  LEFT JOIN (
      SELECT employee_id, base_salary, event_date,
             LEAD(event_date) OVER (PARTITION BY employee_id ORDER BY event_date) AS next_event_date
      FROM {{zone_prefix}}.bronze.raw_comp_events
  ) prev ON ce.employee_id = prev.employee_id
         AND ce.event_date = prev.next_event_date;

  ASSERT ROW_COUNT = 55;

-- ===================== STEP: build_dim_department =====================

STEP build_dim_department
  DEPENDS ON (enrich_comp_events)
AS
  INSERT INTO {{zone_prefix}}.gold.dim_department
  SELECT
      ROW_NUMBER() OVER (ORDER BY d.department_id)            AS department_key,
      d.department_name,
      d.division,
      d.cost_center,
      d.head_count_budget,
      d.manager_name
  FROM {{zone_prefix}}.bronze.raw_departments d;

  ASSERT ROW_COUNT = 5;

-- ===================== STEP: build_dim_position =====================

STEP build_dim_position
  DEPENDS ON (enrich_comp_events)
AS
  INSERT INTO {{zone_prefix}}.gold.dim_position
  SELECT
      ROW_NUMBER() OVER (ORDER BY p.position_id)              AS position_key,
      p.title,
      p.job_family,
      p.job_level,
      p.pay_grade_min,
      p.pay_grade_max,
      p.exempt_flag
  FROM {{zone_prefix}}.bronze.raw_positions p;

  ASSERT ROW_COUNT = 8;

  -- dim_employee from SCD2
  INSERT INTO {{zone_prefix}}.gold.dim_employee
  SELECT
      s.surrogate_key,
      s.employee_id,
      s.employee_name         AS name,
      s.hire_date,
      s.termination_date,
      s.education_level,
      s.gender,
      s.age_band,
      s.valid_from,
      s.valid_to,
      s.is_current
  FROM {{zone_prefix}}.silver.dim_employee_scd2 s;

-- ===================== STEP: build_fact_compensation =====================

STEP build_fact_compensation
  DEPENDS ON (build_dim_department, build_dim_position, insert_scd2_employee)
  TIMEOUT '5m'
AS
  INSERT INTO {{zone_prefix}}.gold.fact_compensation_events
  SELECT
      ROW_NUMBER() OVER (ORDER BY ce.event_id)                AS event_key,
      de.surrogate_key                                         AS employee_key,
      dd.department_key,
      dpos.position_key,
      ce.event_date,
      ce.event_type,
      ce.base_salary,
      ce.bonus,
      ce.total_comp,
      ce.salary_change_pct,
      ce.performance_rating
  FROM {{zone_prefix}}.silver.comp_events_enriched ce
  JOIN {{zone_prefix}}.gold.dim_employee de
      ON ce.employee_id = de.employee_id
      AND de.is_current = true
  JOIN {{zone_prefix}}.gold.dim_department dd
      ON ce.department_id = (
          SELECT d.department_id FROM {{zone_prefix}}.bronze.raw_departments d
          WHERE d.department_name = dd.department_name
      )
  JOIN {{zone_prefix}}.gold.dim_position dpos
      ON ce.position_id = (
          SELECT p.position_id FROM {{zone_prefix}}.bronze.raw_positions p
          WHERE p.title = dpos.title
      );

  ASSERT ROW_COUNT = 55;

-- ===================== STEP: compute_workforce_kpi =====================

STEP compute_workforce_kpi
  DEPENDS ON (build_fact_compensation)
AS
  INSERT INTO {{zone_prefix}}.gold.kpi_workforce_analytics
  SELECT
      dd.department_name                                       AS department,
      CONCAT(CAST(EXTRACT(YEAR FROM ce.event_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ce.event_date) AS STRING)) AS quarter,
      COUNT(DISTINCT ce.employee_id)                           AS headcount,
      ROUND(AVG(ce.base_salary), 2)                           AS avg_salary,
      ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ce.base_salary), 2) AS median_salary,
      -- Turnover rate: terminations / headcount
      ROUND(
          100.0 * COUNT(DISTINCT CASE WHEN ce.event_type = 'Termination' THEN ce.employee_id END)
          / NULLIF(COUNT(DISTINCT ce.employee_id), 0),
          2
      )                                                        AS turnover_rate,
      -- Average tenure
      ROUND(AVG(
          DATEDIFF(
              COALESCE(CAST(e.termination_date AS DATE), CAST('2025-01-01' AS DATE)),
              CAST(e.hire_date AS DATE)
          ) / 365.25
      ), 1)                                                    AS avg_tenure_years,
      -- Promotion rate
      ROUND(
          100.0 * COUNT(DISTINCT CASE WHEN ce.event_type = 'Promotion' THEN ce.employee_id END)
          / NULLIF(COUNT(DISTINCT ce.employee_id), 0),
          2
      )                                                        AS promotion_rate,
      -- Gender pay gap: (avg_male - avg_female) / avg_male * 100
      ROUND(
          100.0 * (
              AVG(CASE WHEN e.gender = 'Male' THEN ce.base_salary END)
              - AVG(CASE WHEN e.gender = 'Female' THEN ce.base_salary END)
          ) / NULLIF(AVG(CASE WHEN e.gender = 'Male' THEN ce.base_salary END), 0),
          2
      )                                                        AS gender_pay_gap_pct,
      -- Compa-ratio: actual salary / midpoint of pay grade
      ROUND(AVG(
          ce.base_salary / NULLIF((p.pay_grade_min + p.pay_grade_max) / 2, 0)
      ), 3)                                                    AS compa_ratio
  FROM {{zone_prefix}}.silver.comp_events_enriched ce
  JOIN {{zone_prefix}}.bronze.raw_employees e ON ce.employee_id = e.employee_id
  JOIN {{zone_prefix}}.bronze.raw_departments d ON ce.department_id = d.department_id
  JOIN {{zone_prefix}}.gold.dim_department dd ON d.department_name = dd.department_name
  JOIN {{zone_prefix}}.bronze.raw_positions p ON ce.position_id = p.position_id
  GROUP BY dd.department_name, CONCAT(CAST(EXTRACT(YEAR FROM ce.event_date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM ce.event_date) AS STRING));

  ASSERT ROW_COUNT > 0
  SELECT 'row count check' AS status;

-- ===================== STEP: gdpr_cleanup =====================

STEP gdpr_cleanup
  DEPENDS ON (compute_workforce_kpi)
  CONTINUE ON FAILURE
AS
  -- Deletion Vectors Demo: Terminated employee EMP-005 requests full data erasure
  DELETE FROM {{zone_prefix}}.silver.dim_employee_scd2
  WHERE employee_id = 'EMP-005';

  DELETE FROM {{zone_prefix}}.silver.comp_events_enriched
  WHERE employee_id = 'EMP-005';

  OPTIMIZE {{zone_prefix}}.silver.dim_employee_scd2;
  OPTIMIZE {{zone_prefix}}.silver.comp_events_enriched;
  OPTIMIZE {{zone_prefix}}.gold.fact_compensation_events;
  OPTIMIZE {{zone_prefix}}.gold.dim_employee;
  OPTIMIZE {{zone_prefix}}.gold.kpi_workforce_analytics;
