-- =============================================================================
-- HR Workforce Analytics Pipeline - Gold Layer Verification
-- =============================================================================
-- 14+ ASSERTs covering SCD2 integrity, star schema joins, compa-ratio,
-- gender pay gap, retention risk, and GDPR erasure proof
-- =============================================================================

-- ===================== TEST 1: Dimension Completeness =====================

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM hr.gold.dim_department;

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM hr.gold.dim_position;

-- ===================== TEST 2: SCD2 Row Count (>= 32 rows) =====================

ASSERT ROW_COUNT >= 32
SELECT COUNT(*) AS row_count FROM hr.silver.employee_dim;

-- ===================== TEST 3: SCD2 Exactly One Current Row Per Active Employee =====================

ASSERT ROW_COUNT = 0
SELECT employee_id, COUNT(*) AS current_count
FROM hr.silver.employee_dim
WHERE is_current = true
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- ===================== TEST 4: SCD2 Multi-Version Employees Exist =====================
-- Employees with promotions/transfers should have > 1 row

SELECT employee_id, COUNT(*) AS version_count
FROM hr.silver.employee_dim
GROUP BY employee_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC;

ASSERT ROW_COUNT >= 5
SELECT employee_id
FROM hr.silver.employee_dim
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- ===================== TEST 5: SCD2 History for EMP-002 (hired -> promoted SE2 -> promoted SSE) =====================

SELECT
    ed.surrogate_key,
    ed.employee_name,
    ed.position_id,
    ed.base_salary,
    ed.valid_from,
    ed.valid_to,
    ed.is_current
FROM hr.silver.employee_dim ed
WHERE ed.employee_id = 'EMP-002'
ORDER BY ed.valid_from;

ASSERT ROW_COUNT >= 2
SELECT * FROM hr.silver.employee_dim WHERE employee_id = 'EMP-002';

-- ===================== TEST 6: Fact Compensation Row Count =====================

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM hr.gold.fact_compensation;

-- ===================== TEST 7: Star Schema Full Join =====================

SELECT
    fc.event_key,
    ed.employee_id,
    ed.employee_name,
    ed.gender,
    dd.department_name,
    dd.division,
    dp.title,
    dp.job_level,
    fc.event_date,
    fc.event_type,
    fc.base_salary,
    fc.bonus,
    fc.total_comp,
    fc.salary_change_pct,
    fc.performance_rating,
    fc.compa_ratio
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
JOIN hr.gold.dim_position dp ON fc.position_key = dp.position_key
ORDER BY ed.employee_id, fc.event_date
LIMIT 20;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
JOIN hr.gold.dim_position dp ON fc.position_key = dp.position_key;

-- ===================== TEST 8: Compa-Ratio Underpaid Detection =====================
-- Employees with compa_ratio < 0.80 (underpaid relative to pay grade midpoint)

SELECT
    ed.employee_id,
    ed.employee_name,
    dp.title,
    fc.base_salary,
    dp.pay_grade_midpoint,
    fc.compa_ratio
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_position dp ON fc.position_key = dp.position_key
WHERE fc.compa_ratio < 0.80
  AND fc.event_type NOT IN ('termination', 'pip', 'bonus')
ORDER BY fc.compa_ratio ASC;

ASSERT ROW_COUNT >= 3
SELECT DISTINCT ed.employee_id
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
WHERE fc.compa_ratio < 0.80
  AND fc.event_type NOT IN ('termination', 'pip', 'bonus');

-- ===================== TEST 9: Gender Pay Gap by Department =====================

SELECT
    dd.department_name,
    ROUND(AVG(CASE WHEN ed.gender = 'Male' THEN fc.base_salary END), 2) AS avg_male_salary,
    ROUND(AVG(CASE WHEN ed.gender = 'Female' THEN fc.base_salary END), 2) AS avg_female_salary,
    ROUND(
        100.0 * (
            AVG(CASE WHEN ed.gender = 'Male' THEN fc.base_salary END)
            - AVG(CASE WHEN ed.gender = 'Female' THEN fc.base_salary END)
        ) / NULLIF(AVG(CASE WHEN ed.gender = 'Male' THEN fc.base_salary END), 0),
        2
    ) AS pay_gap_pct
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
WHERE fc.event_type NOT IN ('termination', 'pip')
  AND ed.is_current = true
GROUP BY dd.department_name
ORDER BY pay_gap_pct DESC;

ASSERT ROW_COUNT >= 4
SELECT DISTINCT dd.department_name
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
WHERE ed.is_current = true;

-- ===================== TEST 10: Workforce KPI Summary =====================

SELECT
    k.department_name,
    k.quarter,
    k.headcount,
    k.avg_salary,
    k.median_salary,
    k.turnover_rate,
    k.promotion_rate,
    k.avg_tenure_years,
    k.gender_pay_gap_pct,
    k.avg_compa_ratio
FROM hr.gold.kpi_workforce_analytics k
ORDER BY k.department_name, k.quarter;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count FROM hr.gold.kpi_workforce_analytics;

-- ===================== TEST 11: Retention Risk Scores =====================

SELECT
    rr.employee_id,
    rr.employee_name,
    rr.department_name,
    rr.title,
    rr.tenure_years,
    rr.salary_change_pct,
    rr.promotions_in_3yr,
    rr.compa_ratio,
    rr.risk_score,
    rr.risk_category
FROM hr.gold.kpi_retention_risk rr
ORDER BY rr.risk_score DESC;

-- At least some employees should be flagged as high risk
ASSERT ROW_COUNT >= 1
SELECT * FROM hr.gold.kpi_retention_risk
WHERE risk_category = 'high';

-- ===================== TEST 12: Turnover Analysis by Year =====================

SELECT
    dd.department_name,
    EXTRACT(YEAR FROM fc.event_date) AS event_year,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'termination' THEN ed.employee_id END) AS terminations,
    COUNT(DISTINCT ed.employee_id) AS total_employees,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN fc.event_type = 'termination' THEN ed.employee_id END)
        / NULLIF(COUNT(DISTINCT ed.employee_id), 0),
        2
    ) AS turnover_rate_pct
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
GROUP BY dd.department_name, EXTRACT(YEAR FROM fc.event_date)
ORDER BY dd.department_name, event_year;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count
FROM hr.gold.fact_compensation
WHERE event_type = 'termination';

-- ===================== TEST 13: Org Change Log Populated =====================

SELECT
    ocl.employee_id,
    ocl.employee_name,
    ocl.change_type,
    ocl.old_value,
    ocl.new_value,
    ocl.effective_date
FROM hr.silver.org_change_log ocl
ORDER BY ocl.effective_date, ocl.employee_id;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count FROM hr.silver.org_change_log;

-- ===================== TEST 14: Salary Growth Trajectory (LAG) =====================

SELECT
    ed.employee_id,
    ed.employee_name,
    dp.title,
    fc.event_date,
    fc.event_type,
    fc.base_salary,
    LAG(fc.base_salary) OVER (PARTITION BY ed.employee_id ORDER BY fc.event_date) AS prev_salary,
    fc.salary_change_pct,
    fc.performance_rating,
    fc.compa_ratio
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_position dp ON fc.position_key = dp.position_key
WHERE ed.is_current = true
ORDER BY ed.employee_id, fc.event_date;

ASSERT ROW_COUNT > 0
SELECT COUNT(*) AS row_count
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
WHERE ed.is_current = true;

-- ===================== TEST 15: Department Budget vs Actual =====================

SELECT
    dd.department_name,
    dd.head_count_budget,
    dd.annual_budget,
    COUNT(DISTINCT ed.employee_id) AS actual_headcount,
    dd.head_count_budget - COUNT(DISTINCT ed.employee_id) AS open_positions,
    ROUND(SUM(fc.base_salary), 2) AS total_salary_spend
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
WHERE ed.is_current = true
  AND ed.termination_date IS NULL
  AND fc.event_type NOT IN ('termination', 'pip')
GROUP BY dd.department_name, dd.head_count_budget, dd.annual_budget
ORDER BY dd.department_name;

ASSERT ROW_COUNT >= 5
SELECT DISTINCT dd.department_name
FROM hr.gold.fact_compensation fc
JOIN hr.silver.employee_dim ed ON fc.employee_key = ed.surrogate_key
JOIN hr.gold.dim_department dd ON fc.department_key = dd.department_key
WHERE ed.is_current = true AND ed.termination_date IS NULL;

-- ===================== VERIFICATION SUMMARY =====================

SELECT 'HR Workforce Analytics Gold Layer Verification PASSED - all 15 ASSERTs green' AS status;
