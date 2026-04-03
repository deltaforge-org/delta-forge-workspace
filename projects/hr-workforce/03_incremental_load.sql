-- =============================================================================
-- HR Workforce Analytics Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based: process new compensation events, apply SCD2 updates

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "event_id > 'CE-055' AND event_date > '2025-03-29'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.comp_events_enriched, event_id, event_date, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.comp_events_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_comp_events
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.comp_events_enriched, event_id, event_date, 3)}};

-- ===================== STEP 1: Watermark Check =====================

SELECT MAX(event_date) AS last_watermark
FROM {{zone_prefix}}.silver.comp_events_enriched;

-- ===================== STEP 2: New Bronze Compensation Events =====================
-- Simulating Q1 2025 annual review cycle

INSERT INTO {{zone_prefix}}.bronze.raw_comp_events VALUES
    ('CE-056', 'EMP-001', 'DEPT-ENG',  'POS-SE2',  '2025-04-01', 'Annual Raise',110250.00, 7000.00,  4.0, '5% merit increase',    '2025-04-01T06:00:00'),
    ('CE-057', 'EMP-002', 'DEPT-ENG',  'POS-SSE',  '2025-04-01', 'Promotion',   140000.00, 10000.00, 4.5, 'Promoted to SSE',      '2025-04-01T06:00:00'),
    ('CE-058', 'EMP-018', 'DEPT-ENG',  'POS-SE2',  '2025-04-01', 'Promotion',   108000.00, 5000.00,  4.0, 'Promoted to SE2',      '2025-04-01T06:00:00'),
    ('CE-059', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2025-04-01', 'Annual Raise',63669.00,  7000.00,  4.5, '5% + commission bonus','2025-04-01T06:00:00');

-- ===================== STEP 3: SCD2 Two-Pass MERGE — Expire Old Records =====================

MERGE INTO {{zone_prefix}}.silver.dim_employee_scd2 AS tgt
USING (
ASSERT ROW_COUNT = 4
    SELECT DISTINCT
        ce.employee_id,
        ce.department_id AS new_dept,
        ce.position_id   AS new_pos,
        ce.base_salary    AS new_salary,
        CAST(ce.event_date AS DATE) AS change_date
    FROM {{zone_prefix}}.bronze.raw_comp_events ce
    WHERE ce.ingested_at > '2025-01-01T12:00:00'
      AND ce.event_type NOT IN ('Hire', 'Termination')
) AS changes
ON tgt.employee_id = changes.employee_id
   AND tgt.is_current = true
   AND (tgt.department_id != changes.new_dept
        OR tgt.position_id != changes.new_pos
        OR tgt.base_salary != changes.new_salary)
WHEN MATCHED THEN UPDATE SET
    valid_to = changes.change_date,
    is_current = false;

-- ===================== STEP 4: SCD2 Two-Pass MERGE — Insert New Versions =====================

INSERT INTO {{zone_prefix}}.silver.dim_employee_scd2
SELECT
    100 + ROW_NUMBER() OVER (ORDER BY ce.employee_id, ce.event_date) AS surrogate_key,
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
    CAST(ce.event_date AS DATE)     AS valid_from,
    CAST(NULL AS DATE)              AS valid_to,
    true                             AS is_current
FROM {{zone_prefix}}.bronze.raw_comp_events ce
JOIN {{zone_prefix}}.bronze.raw_employees e ON ce.employee_id = e.employee_id
WHERE ce.ingested_at > '2025-01-01T12:00:00'
  AND ce.event_type NOT IN ('Hire', 'Termination');

-- ===================== STEP 5: Incremental Comp Events Enrichment =====================

INSERT INTO {{zone_prefix}}.silver.comp_events_enriched
ASSERT ROW_COUNT = 4
SELECT
    ce.event_id,
    ce.employee_id,
    ce.department_id,
    ce.position_id,
    CAST(ce.event_date AS DATE),
    ce.event_type,
    ce.base_salary,
    ce.bonus,
    ce.base_salary + ce.bonus       AS total_comp,
    CASE
        WHEN prev.base_salary IS NOT NULL AND prev.base_salary > 0
        THEN ROUND(100.0 * (ce.base_salary - prev.base_salary) / prev.base_salary, 2)
        ELSE 0.00
    END                              AS salary_change_pct,
    ce.performance_rating
FROM {{zone_prefix}}.bronze.raw_comp_events ce
LEFT JOIN (
    SELECT employee_id, base_salary, event_date,
           LEAD(event_date) OVER (PARTITION BY employee_id ORDER BY event_date) AS next_event_date
    FROM {{zone_prefix}}.bronze.raw_comp_events
) prev ON ce.employee_id = prev.employee_id AND ce.event_date = prev.next_event_date
WHERE ce.ingested_at > '2025-01-01T12:00:00';

-- ===================== STEP 6: Merge into Gold Fact =====================

MERGE INTO {{zone_prefix}}.gold.fact_compensation_events AS tgt
USING (
ASSERT ROW_COUNT = 4
    SELECT
        ROW_NUMBER() OVER (ORDER BY ce.event_id) + 55  AS event_key,
        de.surrogate_key                                 AS employee_key,
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
    JOIN {{zone_prefix}}.gold.dim_employee de ON ce.employee_id = de.employee_id AND de.is_current = true
    JOIN {{zone_prefix}}.gold.dim_department dd ON ce.department_id = (
        SELECT d.department_id FROM {{zone_prefix}}.bronze.raw_departments d WHERE d.department_name = dd.department_name
    )
    JOIN {{zone_prefix}}.gold.dim_position dpos ON ce.position_id = (
        SELECT p.position_id FROM {{zone_prefix}}.bronze.raw_positions p WHERE p.title = dpos.title
    )
    WHERE ce.event_date > '2025-01-01'
) AS src
ON tgt.event_key = src.event_key
WHEN NOT MATCHED THEN INSERT (
    event_key, employee_key, department_key, position_key, event_date,
    event_type, base_salary, bonus, total_comp, salary_change_pct, performance_rating
) VALUES (
    src.event_key, src.employee_key, src.department_key, src.position_key, src.event_date,
    src.event_type, src.base_salary, src.bonus, src.total_comp, src.salary_change_pct, src.performance_rating
);

-- ===================== STEP 7: Verify =====================

ASSERT VALUE base_salary > 0 WHERE event_type = 'Annual Raise'
SELECT 'base_salary check passed' AS base_salary_status;


OPTIMIZE {{zone_prefix}}.gold.fact_compensation_events;
