-- =============================================================================
-- HR Workforce Analytics Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- 14-step DAG: validate_bronze -> build_employee_scd2_initial ->
-- expire_scd2_records -> insert_scd2_current (3-pass) +
-- enrich_comp_events (parallel) -> enable_cdf_org_log ->
-- dim_department + dim_position (parallel) -> build_fact_compensation ->
-- kpi_workforce + kpi_retention_risk (parallel) -> gdpr_erasure_demo ->
-- optimize (CONTINUE ON FAILURE)
-- =============================================================================

PIPELINE hr_workforce_pipeline
  DESCRIPTION 'Full lifecycle workforce pipeline: SCD2 tracking, CDF org log, compa-ratio analysis, gender pay gap, retention risk scoring, GDPR erasure'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'hr,workforce,scd2,cdf,gdpr,compa-ratio'
  SLA 2700
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze =====================

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_employees;

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_comp_events;

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_departments;

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_positions;

-- ===================== build_employee_scd2_initial =====================
-- Pass 1 of 3: Load initial hire records for each employee

DELETE FROM hr.silver.employee_dim WHERE 1=1;

INSERT INTO hr.silver.employee_dim
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_id) AS surrogate_key,
    e.employee_id,
    TRIM(e.employee_name) AS employee_name,
    e.ssn,
    e.email,
    CAST(e.hire_date AS DATE) AS hire_date,
    CAST(e.termination_date AS DATE) AS termination_date,
    e.department_id,
    e.position_id,
    e.education_level,
    e.gender,
    CAST(e.date_of_birth AS DATE) AS date_of_birth,
    ce.base_salary,
    e.status,
    CAST(e.hire_date AS DATE) AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    true AS is_current
FROM hr.bronze.raw_employees e
JOIN (
    SELECT employee_id, base_salary,
           ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date) AS rn
    FROM hr.bronze.raw_comp_events
    WHERE event_type = 'hire'
) ce ON e.employee_id = ce.employee_id AND ce.rn = 1;

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM hr.silver.employee_dim;

-- ===================== expire_scd2_records =====================
-- Pass 2 of 3: Expire current records where a dimension-changing event occurred
-- (promotion, transfer, salary_adjustment that changes dept/position/salary)

MERGE INTO hr.silver.employee_dim AS tgt
USING (
    SELECT employee_id,
           department_id AS new_dept,
           position_id   AS new_pos,
           base_salary   AS new_salary,
           CAST(event_date AS DATE) AS change_date
    FROM (
        SELECT employee_id, department_id, position_id, base_salary, event_date,
               ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date DESC) AS rn
        FROM hr.bronze.raw_comp_events
        WHERE event_type IN ('promotion', 'salary_adjustment', 'transfer')
    ) t
    WHERE rn = 1
) AS changes
ON tgt.employee_id = changes.employee_id
WHEN MATCHED AND tgt.is_current = true
             AND (tgt.department_id != changes.new_dept
                  OR tgt.position_id != changes.new_pos
                  OR tgt.base_salary != changes.new_salary)
THEN UPDATE SET
    valid_to   = changes.change_date,
    is_current = false;

-- ===================== insert_scd2_current =====================
-- Pass 3 of 3: Insert new current version for each changed employee
-- Takes the LATEST change event per employee as the current state

MERGE INTO hr.silver.employee_dim AS tgt
USING (
    SELECT
        20 + ROW_NUMBER() OVER (ORDER BY ce.employee_id, ce.event_date) AS surrogate_key,
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
    FROM (
        SELECT employee_id, department_id, position_id, base_salary, event_date,
               ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date DESC) AS rn
        FROM hr.bronze.raw_comp_events
        WHERE event_type IN ('promotion', 'salary_adjustment', 'transfer')
    ) ce
    JOIN hr.bronze.raw_employees e ON ce.employee_id = e.employee_id
    WHERE ce.rn = 1
) AS src
ON tgt.employee_id = src.employee_id AND tgt.valid_from = src.valid_from AND tgt.is_current = true
WHEN NOT MATCHED THEN INSERT (
    surrogate_key, employee_id, employee_name, ssn, email, hire_date, termination_date,
    department_id, position_id, education_level, gender, date_of_birth, base_salary,
    status, valid_from, valid_to, is_current
) VALUES (
    src.surrogate_key, src.employee_id, src.employee_name, src.ssn, src.email, src.hire_date,
    src.termination_date, src.department_id, src.position_id, src.education_level, src.gender,
    src.date_of_birth, src.base_salary, src.status, src.valid_from, src.valid_to, src.is_current
);

-- SCD2 should have 20 initial + dimension-change rows
ASSERT ROW_COUNT >= 32
SELECT COUNT(*) AS row_count FROM hr.silver.employee_dim;

-- ===================== enrich_comp_events =====================
-- Calculate total comp, salary change pct, compa-ratio

DELETE FROM hr.silver.comp_events_enriched WHERE 1=1;

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
    -- compa_ratio: salary / midpoint(pay_grade_min, pay_grade_max)
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
) prev ON ce.employee_id = prev.employee_id
       AND ce.event_date = prev.next_event_date
JOIN hr.bronze.raw_positions p ON ce.position_id = p.position_id;

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM hr.silver.comp_events_enriched;

-- ===================== enable_cdf_org_log =====================
-- Populate org change log from SCD2 dimension changes via CDF

DELETE FROM hr.silver.org_change_log WHERE 1=1;

INSERT INTO hr.silver.org_change_log
SELECT
    ROW_NUMBER() OVER (ORDER BY expired.employee_id, expired.valid_to) AS change_id,
    expired.employee_id,
    expired.employee_name,
    CASE
        WHEN expired.department_id != current_row.department_id THEN 'department_transfer'
        WHEN expired.position_id != current_row.position_id THEN 'title_change'
        WHEN expired.base_salary != current_row.base_salary THEN 'salary_change'
        ELSE 'other_change'
    END AS change_type,
    CASE
        WHEN expired.department_id != current_row.department_id THEN expired.department_id
        WHEN expired.position_id != current_row.position_id THEN expired.position_id
        ELSE CAST(expired.base_salary AS STRING)
    END AS old_value,
    CASE
        WHEN expired.department_id != current_row.department_id THEN current_row.department_id
        WHEN expired.position_id != current_row.position_id THEN current_row.position_id
        ELSE CAST(current_row.base_salary AS STRING)
    END AS new_value,
    expired.valid_to AS effective_date,
    CURRENT_TIMESTAMP AS captured_at
FROM hr.silver.employee_dim expired
JOIN hr.silver.employee_dim current_row
    ON expired.employee_id = current_row.employee_id
    AND current_row.valid_from = expired.valid_to
WHERE expired.is_current = false;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count FROM hr.silver.org_change_log;

-- ===================== dim_department =====================

DELETE FROM hr.gold.dim_department WHERE 1=1;

INSERT INTO hr.gold.dim_department
SELECT
    ROW_NUMBER() OVER (ORDER BY d.department_id) AS department_key,
    d.department_id,
    d.department_name,
    d.division,
    d.cost_center,
    d.head_count_budget,
    d.annual_budget,
    d.manager_name,
    CURRENT_TIMESTAMP AS loaded_at
FROM hr.bronze.raw_departments d;

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM hr.gold.dim_department;

-- ===================== dim_position =====================

DELETE FROM hr.gold.dim_position WHERE 1=1;

INSERT INTO hr.gold.dim_position
SELECT
    ROW_NUMBER() OVER (ORDER BY p.position_id) AS position_key,
    p.position_id,
    p.title,
    p.job_family,
    p.job_level,
    p.pay_grade_min,
    p.pay_grade_max,
    (p.pay_grade_min + p.pay_grade_max) / 2 AS pay_grade_midpoint,
    p.exempt_flag,
    CURRENT_TIMESTAMP AS loaded_at
FROM hr.bronze.raw_positions p;

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM hr.gold.dim_position;

-- ===================== build_fact_compensation =====================
-- Star schema fact: joins enriched events with dimension keys

DELETE FROM hr.gold.fact_compensation WHERE 1=1;

INSERT INTO hr.gold.fact_compensation
SELECT
    ROW_NUMBER() OVER (ORDER BY ce.event_id) AS event_key,
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
    ce.compa_ratio,
    CURRENT_TIMESTAMP AS loaded_at
FROM hr.silver.comp_events_enriched ce
JOIN hr.silver.employee_dim ed
    ON ce.employee_id = ed.employee_id
    AND ed.is_current = true
JOIN hr.gold.dim_department dd
    ON ce.department_id = dd.department_id
JOIN hr.gold.dim_position dp
    ON ce.position_id = dp.position_id;

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM hr.gold.fact_compensation;

-- ===================== kpi_workforce_analytics =====================
-- Headcount, avg/median salary, turnover, promotion rate, gender pay gap, compa-ratio

DELETE FROM hr.gold.kpi_workforce_analytics WHERE 1=1;

INSERT INTO hr.gold.kpi_workforce_analytics
SELECT
    dd.department_name,
    CONCAT(
        CAST(EXTRACT(YEAR FROM fc.event_date) AS STRING),
        '-Q',
        CAST(EXTRACT(QUARTER FROM fc.event_date) AS STRING)
    ) AS quarter,
    COUNT(DISTINCT fc.employee_key) AS headcount,
    ROUND(AVG(fc.base_salary), 2) AS avg_salary,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fc.base_salary), 2) AS median_salary,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN fc.event_type = 'termination' THEN ed.employee_id END)
        / NULLIF(COUNT(DISTINCT fc.employee_key), 0),
        2
    ) AS turnover_rate,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN fc.event_type = 'promotion' THEN ed.employee_id END)
        / NULLIF(COUNT(DISTINCT fc.employee_key), 0),
        2
    ) AS promotion_rate,
    ROUND(AVG(
        DATEDIFF(
            COALESCE(CAST(e.termination_date AS DATE), CAST('2025-01-01' AS DATE)),
            CAST(e.hire_date AS DATE)
        ) / 365.25
    ), 1) AS avg_tenure_years,
    -- Gender pay gap: (avg_male - avg_female) / avg_male * 100
    ROUND(
        100.0 * (
            AVG(CASE WHEN e.gender = 'Male' THEN fc.base_salary END)
            - AVG(CASE WHEN e.gender = 'Female' THEN fc.base_salary END)
        ) / NULLIF(AVG(CASE WHEN e.gender = 'Male' THEN fc.base_salary END), 0),
        2
    ) AS gender_pay_gap_pct,
    ROUND(AVG(fc.compa_ratio), 3) AS avg_compa_ratio,
    CURRENT_TIMESTAMP AS loaded_at
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.bronze.raw_employees e ON ed.employee_id = e.employee_id
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
GROUP BY
    dd.department_name,
    CONCAT(
        CAST(EXTRACT(YEAR FROM fc.event_date) AS STRING),
        '-Q',
        CAST(EXTRACT(QUARTER FROM fc.event_date) AS STRING)
    );

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count FROM hr.gold.kpi_workforce_analytics;

-- ===================== kpi_retention_risk =====================
-- Risk scoring: tenure < 1yr (25pts) + salary_change < 3% (20pts) +
-- no promotions in 3yr (25pts) + compa_ratio < 0.85 (30pts)

DELETE FROM hr.gold.kpi_retention_risk WHERE 1=1;

INSERT INTO hr.gold.kpi_retention_risk
SELECT
    ed.employee_id,
    ed.employee_name,
    dd.department_name,
    dp.title,
    ROUND(
        DATEDIFF(CAST('2025-01-01' AS DATE), ed.hire_date) / 365.25,
        1
    ) AS tenure_years,
    COALESCE(latest_change.salary_change_pct, 0) AS salary_change_pct,
    COALESCE(promo_count.promotions_in_3yr, 0) AS promotions_in_3yr,
    COALESCE(latest_compa.compa_ratio, 0) AS compa_ratio,
    -- Risk score calculation
    (CASE WHEN DATEDIFF(CAST('2025-01-01' AS DATE), ed.hire_date) / 365.25 < 1 THEN 25 ELSE 0 END)
    + (CASE WHEN COALESCE(latest_change.salary_change_pct, 0) < 3 THEN 20 ELSE 0 END)
    + (CASE WHEN COALESCE(promo_count.promotions_in_3yr, 0) = 0 THEN 25 ELSE 0 END)
    + (CASE WHEN COALESCE(latest_compa.compa_ratio, 0) < 0.85 THEN 30 ELSE 0 END)
    AS risk_score,
    CASE
        WHEN (CASE WHEN DATEDIFF(CAST('2025-01-01' AS DATE), ed.hire_date) / 365.25 < 1 THEN 25 ELSE 0 END)
             + (CASE WHEN COALESCE(latest_change.salary_change_pct, 0) < 3 THEN 20 ELSE 0 END)
             + (CASE WHEN COALESCE(promo_count.promotions_in_3yr, 0) = 0 THEN 25 ELSE 0 END)
             + (CASE WHEN COALESCE(latest_compa.compa_ratio, 0) < 0.85 THEN 30 ELSE 0 END)
             >= 50 THEN 'high'
        WHEN (CASE WHEN DATEDIFF(CAST('2025-01-01' AS DATE), ed.hire_date) / 365.25 < 1 THEN 25 ELSE 0 END)
             + (CASE WHEN COALESCE(latest_change.salary_change_pct, 0) < 3 THEN 20 ELSE 0 END)
             + (CASE WHEN COALESCE(promo_count.promotions_in_3yr, 0) = 0 THEN 25 ELSE 0 END)
             + (CASE WHEN COALESCE(latest_compa.compa_ratio, 0) < 0.85 THEN 30 ELSE 0 END)
             >= 25 THEN 'medium'
        ELSE 'low'
    END AS risk_category,
    CURRENT_TIMESTAMP AS loaded_at
FROM hr.silver.employee_dim ed
JOIN hr.gold.dim_department dd
    ON ed.department_id = dd.department_id
JOIN hr.gold.dim_position dp
    ON ed.position_id = dp.position_id
LEFT JOIN (
    SELECT employee_id, salary_change_pct,
           ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date DESC) AS rn
    FROM hr.silver.comp_events_enriched
    WHERE event_type IN ('salary_adjustment', 'promotion')
) latest_change ON ed.employee_id = latest_change.employee_id AND latest_change.rn = 1
LEFT JOIN (
    SELECT employee_id, COUNT(*) AS promotions_in_3yr
    FROM hr.silver.comp_events_enriched
    WHERE event_type = 'promotion'
      AND event_date >= CAST('2022-01-01' AS DATE)
    GROUP BY employee_id
) promo_count ON ed.employee_id = promo_count.employee_id
LEFT JOIN (
    SELECT employee_id, compa_ratio,
           ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY event_date DESC) AS rn
    FROM hr.silver.comp_events_enriched
) latest_compa ON ed.employee_id = latest_compa.employee_id AND latest_compa.rn = 1
WHERE ed.is_current = true
  AND ed.termination_date IS NULL;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count FROM hr.gold.kpi_retention_risk;

-- ===================== gdpr_erasure_demo =====================
-- DELETE terminated employee EMP-005, VACUUM to prove physical deletion,
-- VERSION AS OF to show prior existence, RESTORE to recover

-- Step A: Show employee exists before erasure
SELECT * FROM hr.silver.employee_dim
WHERE employee_id = 'EMP-005';

-- Step B: Delete all records for terminated employee EMP-005
DELETE FROM hr.silver.employee_dim
WHERE employee_id = 'EMP-005';

DELETE FROM hr.silver.comp_events_enriched
WHERE employee_id = 'EMP-005';

-- Step C: VACUUM to physically remove deleted data
VACUUM hr.silver.employee_dim;
VACUUM hr.silver.comp_events_enriched;

-- Step D: Verify deletion - should return 0 rows
ASSERT ROW_COUNT = 0
SELECT * FROM hr.silver.employee_dim
WHERE employee_id = 'EMP-005';

-- Step E: VERSION AS OF - prove the employee existed before deletion
SELECT * FROM hr.silver.employee_dim VERSION AS OF 1
WHERE employee_id = 'EMP-005';

-- Step F: RESTORE to recover (demonstrates capability)
RESTORE hr.silver.employee_dim TO VERSION 1;

-- Step G: Re-delete after demonstrating restore
DELETE FROM hr.silver.employee_dim
WHERE employee_id = 'EMP-005';

-- ===================== optimize =====================

OPTIMIZE hr.silver.employee_dim;
OPTIMIZE hr.silver.comp_events_enriched;
OPTIMIZE hr.silver.org_change_log;
OPTIMIZE hr.gold.dim_department;
OPTIMIZE hr.gold.dim_position;
OPTIMIZE hr.gold.fact_compensation;
OPTIMIZE hr.gold.kpi_workforce_analytics;
OPTIMIZE hr.gold.kpi_retention_risk;
