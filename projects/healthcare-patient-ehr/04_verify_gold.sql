-- =============================================================================
-- Healthcare Patient EHR Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== TEST 1: Star Schema Join - Department Admissions =====================

SELECT
    dd.department_name,
    dd.floor,
    dd.wing,
    COUNT(f.admission_key) AS total_admissions,
    SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) AS readmissions,
    ROUND(AVG(f.los_days), 1) AS avg_los,
    ROUND(AVG(f.total_charges), 2) AS avg_charges
FROM {{zone_prefix}}.gold.fact_admissions f
JOIN {{zone_prefix}}.gold.dim_department dd ON f.department_key = dd.department_key
GROUP BY dd.department_name, dd.floor, dd.wing
ORDER BY total_admissions DESC;

-- Cardiology should have the most admissions
ASSERT VALUE department_name = 'Cardiology' WHERE total_admissions = (
    SELECT MAX(total_admissions) FROM (
        SELECT dd.department_name, COUNT(*) AS total_admissions
        FROM {{zone_prefix}}.gold.fact_admissions f
        JOIN {{zone_prefix}}.gold.dim_department dd ON f.department_key = dd.department_key
        GROUP BY dd.department_name
    )
);

-- ===================== TEST 2: Diagnosis Severity Distribution =====================

SELECT
    dx.severity,
    dx.category,
    COUNT(f.admission_key) AS admission_count,
    ROUND(AVG(f.total_charges), 2) AS avg_charges,
    MAX(f.total_charges) AS max_charges
FROM {{zone_prefix}}.gold.fact_admissions f
JOIN {{zone_prefix}}.gold.dim_diagnosis dx ON f.diagnosis_key = dx.diagnosis_key
GROUP BY dx.severity, dx.category
ORDER BY dx.severity, admission_count DESC;

-- Critical severity should have highest average charges
SELECT
    ROUND(AVG(f.total_charges), 2) AS critical_avg_charges
FROM {{zone_prefix}}.gold.fact_admissions f
JOIN {{zone_prefix}}.gold.dim_diagnosis dx ON f.diagnosis_key = dx.diagnosis_key
WHERE dx.severity = 'Critical';

-- ===================== TEST 3: LOS Percentile Analysis (Window Function) =====================

ASSERT VALUE critical_avg_charges >= 40000
SELECT
    patient_key,
    admission_date,
    los_days,
    los_percentile,
    total_charges,
    cost_rank
FROM {{zone_prefix}}.gold.fact_admissions
WHERE los_percentile >= 90
ORDER BY los_days DESC;

-- Top percentile patients should have LOS >= 7 days
SELECT COUNT(*) AS high_los_count
FROM {{zone_prefix}}.gold.fact_admissions
WHERE los_percentile >= 90 AND los_days >= 7;

-- ===================== TEST 4: Top Cost Patients (DENSE_RANK) =====================

ASSERT VALUE high_los_count >= 1
SELECT
    f.patient_key,
    f.admission_date,
    f.total_charges,
    f.cost_rank,
    dx.description AS diagnosis,
    dd.department_name
FROM {{zone_prefix}}.gold.fact_admissions f
JOIN {{zone_prefix}}.gold.dim_diagnosis dx ON f.diagnosis_key = dx.diagnosis_key
JOIN {{zone_prefix}}.gold.dim_department dd ON f.department_key = dd.department_key
WHERE f.cost_rank <= 5
ORDER BY f.cost_rank;

-- Top cost admission should be oncology (lung cancer)
SELECT dd.department_name AS top_cost_dept
FROM {{zone_prefix}}.gold.fact_admissions f
JOIN {{zone_prefix}}.gold.dim_department dd ON f.department_key = dd.department_key
WHERE f.cost_rank = 1;

-- ===================== TEST 5: KPI Readmission Rates Validation =====================

ASSERT VALUE top_cost_dept = 'Oncology'
SELECT
    department_name,
    period,
    total_admissions,
    readmissions,
    readmission_pct,
    avg_los,
    avg_charges
FROM {{zone_prefix}}.gold.kpi_readmission_rates
ORDER BY department_name, period;

-- Overall readmission count should be non-trivial
SELECT SUM(readmissions) AS total_readmissions
FROM {{zone_prefix}}.gold.kpi_readmission_rates;

-- ===================== TEST 6: Readmission Rate by Department =====================

ASSERT VALUE total_readmissions >= 10
SELECT
    department_name,
    SUM(total_admissions) AS yearly_admissions,
    SUM(readmissions) AS yearly_readmissions,
    ROUND(100.0 * SUM(readmissions) / SUM(total_admissions), 2) AS yearly_readmission_pct
FROM {{zone_prefix}}.gold.kpi_readmission_rates
GROUP BY department_name
HAVING SUM(readmissions) > 0
ORDER BY yearly_readmission_pct DESC;

-- At least 3 departments should have readmissions
SELECT COUNT(*) AS depts_with_readmissions
FROM (
    SELECT department_name
    FROM {{zone_prefix}}.gold.kpi_readmission_rates
    GROUP BY department_name
    HAVING SUM(readmissions) > 0
);

-- ===================== TEST 7: Dimension Table Completeness =====================

ASSERT VALUE depts_with_readmissions >= 3
SELECT COUNT(*) AS dept_dim_count FROM {{zone_prefix}}.gold.dim_department;
ASSERT VALUE dept_dim_count = 9

SELECT COUNT(*) AS diag_dim_count FROM {{zone_prefix}}.gold.dim_diagnosis;
-- ===================== TEST 8: Fact Table Referential Integrity =====================

-- All fact department_keys should exist in dim_department
ASSERT VALUE diag_dim_count = 16
SELECT COUNT(*) AS orphan_dept_keys
FROM {{zone_prefix}}.gold.fact_admissions f
LEFT JOIN {{zone_prefix}}.gold.dim_department d ON f.department_key = d.department_key
WHERE d.department_key IS NULL;

-- All fact diagnosis_keys should exist in dim_diagnosis
ASSERT VALUE orphan_dept_keys = 0
SELECT COUNT(*) AS orphan_diag_keys
FROM {{zone_prefix}}.gold.fact_admissions f
LEFT JOIN {{zone_prefix}}.gold.dim_diagnosis d ON f.diagnosis_key = d.diagnosis_key
WHERE d.diagnosis_key IS NULL;

-- ===================== TEST 9: Monthly Admission Trends =====================

ASSERT VALUE orphan_diag_keys = 0
SELECT
    DATE_TRUNC('month', f.admission_date) AS month,
    COUNT(*) AS admissions,
    SUM(CASE WHEN f.readmission_flag THEN 1 ELSE 0 END) AS readmissions,
    ROUND(SUM(f.total_charges), 2) AS total_revenue
FROM {{zone_prefix}}.gold.fact_admissions f
GROUP BY DATE_TRUNC('month', f.admission_date)
ORDER BY month;

-- Should span multiple months
SELECT COUNT(DISTINCT DATE_TRUNC('month', admission_date)) AS distinct_months
FROM {{zone_prefix}}.gold.fact_admissions;

-- ===================== VERIFICATION SUMMARY =====================

ASSERT VALUE distinct_months >= 10
SELECT 'Healthcare EHR Gold Layer Verification PASSED' AS status;
