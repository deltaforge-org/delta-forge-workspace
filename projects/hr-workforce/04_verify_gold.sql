-- =============================================================================
-- HR Workforce Analytics Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== QUERY 1: Workforce KPI Summary =====================

SELECT
    k.department,
    k.quarter,
    k.headcount,
    k.avg_salary,
    k.median_salary,
    k.turnover_rate,
    k.avg_tenure_years,
    k.promotion_rate,
    k.gender_pay_gap_pct,
    k.compa_ratio
FROM {{zone_prefix}}.gold.kpi_workforce_analytics k
ORDER BY k.department, k.quarter;

-- ===================== QUERY 2: Star Schema Join — Full Compensation Detail =====================

ASSERT ROW_COUNT > 0
SELECT
    f.event_key,
    de.employee_id,
    de.name,
    de.gender,
    dd.department_name,
    dd.division,
    dp.title,
    dp.job_level,
    f.event_date,
    f.event_type,
    f.base_salary,
    f.bonus,
    f.total_comp,
    f.salary_change_pct,
    f.performance_rating
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_department dd   ON f.department_key = dd.department_key
JOIN {{zone_prefix}}.gold.dim_position dp     ON f.position_key = dp.position_key
ORDER BY de.employee_id, f.event_date;

-- ===================== QUERY 3: SCD2 History Verification =====================
-- Show employees with multiple versions (promotions/transfers)

ASSERT VALUE event_key > 0
SELECT
    de.employee_id,
    de.name,
    de.valid_from,
    de.valid_to,
    de.is_current,
    COUNT(*) OVER (PARTITION BY de.employee_id)             AS total_versions
FROM {{zone_prefix}}.gold.dim_employee de
WHERE de.employee_id IN (
    SELECT employee_id FROM {{zone_prefix}}.gold.dim_employee
    GROUP BY employee_id HAVING COUNT(*) > 1
)
ORDER BY de.employee_id, de.valid_from;

-- ===================== QUERY 4: Salary Distribution by Department =====================

ASSERT VALUE total_versions > 1
SELECT
    dd.department_name,
    dp.title,
    COUNT(DISTINCT de.employee_id)                          AS employees,
    ROUND(AVG(f.base_salary), 2)                           AS avg_salary,
    MIN(f.base_salary)                                      AS min_salary,
    MAX(f.base_salary)                                      AS max_salary,
    dp.pay_grade_min,
    dp.pay_grade_max,
    ROUND(AVG(f.base_salary) / ((dp.pay_grade_min + dp.pay_grade_max) / 2), 3) AS compa_ratio
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_department dd   ON f.department_key = dd.department_key
JOIN {{zone_prefix}}.gold.dim_position dp     ON f.position_key = dp.position_key
WHERE f.event_type NOT IN ('Termination')
  AND de.is_current = true
GROUP BY dd.department_name, dp.title, dp.pay_grade_min, dp.pay_grade_max
ORDER BY dd.department_name, dp.title;

-- ===================== QUERY 5: Gender Pay Gap Analysis =====================

ASSERT VALUE avg_salary > 0
SELECT
    dd.department_name,
    de.gender,
    COUNT(DISTINCT de.employee_id)                          AS headcount,
    ROUND(AVG(f.base_salary), 2)                           AS avg_salary,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.base_salary), 2) AS median_salary
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_department dd   ON f.department_key = dd.department_key
WHERE de.is_current = true
  AND f.event_type != 'Termination'
GROUP BY dd.department_name, de.gender
ORDER BY dd.department_name, de.gender;

-- ===================== QUERY 6: Turnover Analysis =====================

ASSERT VALUE headcount > 0
SELECT
    dd.department_name,
    YEAR(f.event_date)                                      AS year,
    COUNT(DISTINCT CASE WHEN f.event_type = 'Termination' THEN de.employee_id END) AS terminations,
    COUNT(DISTINCT de.employee_id)                          AS total_employees,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN f.event_type = 'Termination' THEN de.employee_id END)
        / NULLIF(COUNT(DISTINCT de.employee_id), 0),
        2
    )                                                        AS turnover_rate_pct
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_department dd   ON f.department_key = dd.department_key
GROUP BY dd.department_name, YEAR(f.event_date)
ORDER BY dd.department_name, year;

-- ===================== QUERY 7: Promotion Pipeline by Job Level =====================

ASSERT VALUE total_employees > 0
SELECT
    dp.job_family,
    dp.job_level,
    dp.title,
    COUNT(DISTINCT CASE WHEN f.event_type = 'Promotion' THEN de.employee_id END) AS promotions,
    COUNT(DISTINCT de.employee_id)                          AS employees_at_level,
    ROUND(AVG(f.salary_change_pct), 2)                     AS avg_salary_bump_pct,
    ROUND(AVG(f.performance_rating), 1)                    AS avg_rating_at_promotion
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_position dp     ON f.position_key = dp.position_key
GROUP BY dp.job_family, dp.job_level, dp.title
ORDER BY dp.job_family, dp.job_level;

-- ===================== QUERY 8: Compensation Growth Trajectory =====================
-- Running salary trend per employee using LAG

ASSERT ROW_COUNT = 8
SELECT
    de.employee_id,
    de.name,
    dp.title,
    f.event_date,
    f.event_type,
    f.base_salary,
    LAG(f.base_salary) OVER (PARTITION BY de.employee_id ORDER BY f.event_date) AS prev_salary,
    f.salary_change_pct,
    f.performance_rating
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_position dp ON f.position_key = dp.position_key
WHERE de.is_current = true
ORDER BY de.employee_id, f.event_date;

-- ===================== QUERY 9: Performance Rating Distribution =====================

ASSERT VALUE base_salary > 0
SELECT
    f.performance_rating,
    COUNT(*)                                                AS event_count,
    ROUND(AVG(f.salary_change_pct), 2)                    AS avg_raise_pct,
    ROUND(AVG(f.bonus), 2)                                AS avg_bonus,
    PERCENT_RANK() OVER (ORDER BY f.performance_rating)   AS rating_percentile
FROM {{zone_prefix}}.gold.fact_compensation_events f
WHERE f.event_type IN ('Annual Raise', 'Promotion')
GROUP BY f.performance_rating
ORDER BY f.performance_rating;

-- ===================== QUERY 10: Department Budget vs Actual =====================

ASSERT VALUE event_count > 0
SELECT
    dd.department_name,
    dd.head_count_budget,
    COUNT(DISTINCT de.employee_id)                          AS actual_headcount,
    dd.head_count_budget - COUNT(DISTINCT de.employee_id)  AS open_positions,
    ROUND(SUM(f.base_salary), 2)                           AS total_salary_spend
FROM {{zone_prefix}}.gold.fact_compensation_events f
JOIN {{zone_prefix}}.gold.dim_employee de     ON f.employee_key = de.surrogate_key
JOIN {{zone_prefix}}.gold.dim_department dd   ON f.department_key = dd.department_key
WHERE de.is_current = true
  AND de.termination_date IS NULL
  AND f.event_type != 'Termination'
GROUP BY dd.department_name, dd.head_count_budget
ORDER BY dd.department_name;

ASSERT ROW_COUNT = 5
SELECT 'row count check' AS status;

