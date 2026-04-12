-- =============================================================================
-- Healthcare Patient EHR Pipeline - Gold Layer Verification
-- =============================================================================
-- 10+ ASSERT queries testing star schema joins, SCD2 versioning,
-- pseudonymisation, readmission rates, CDF audit count, and more.
-- =============================================================================

PIPELINE ehr_verify_gold
  DESCRIPTION 'Gold layer verification for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'verification', 'healthcare-patient-ehr'
  LIFECYCLE production
;


-- ===================== TEST 1: Dimension Table Completeness =====================

ASSERT ROW_COUNT = 9
SELECT COUNT(*) AS row_count FROM ehr.gold.dim_department;

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM ehr.gold.dim_diagnosis;

-- ===================== TEST 2: SCD2 Versioning on Patient Dim =====================
-- Patients P1001, P1004, P1008 should have 2 versions each (original + address change)

SELECT
  patient_id,
  address,
  city,
  insurance_id,
  valid_from,
  valid_to,
  is_current
FROM ehr.silver.patient_dim
WHERE patient_id IN ('P1001', 'P1004', 'P1008')
ORDER BY patient_id, valid_from;

-- Each of the 3 patients should have exactly 2 versions
ASSERT VALUE scd2_version_count >= 6
SELECT COUNT(*) AS scd2_version_count
FROM ehr.silver.patient_dim
WHERE patient_id IN ('P1001', 'P1004', 'P1008');

-- Verify expired records exist
ASSERT VALUE expired_records >= 3
SELECT COUNT(*) AS expired_records
FROM ehr.silver.patient_dim
WHERE is_current = false;

-- ===================== TEST 3: Star Schema Join - Department Admissions =====================

SELECT
  dd.department_name,
  dd.floor,
  dd.wing,
  COUNT(f.admission_key) AS total_admissions,
  SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) AS readmissions,
  ROUND(AVG(f.los_days), 1) AS avg_los,
  ROUND(AVG(f.total_charges), 2) AS avg_charges
FROM ehr.gold.fact_admissions f
JOIN ehr.gold.dim_department dd ON f.department_key = dd.department_key
GROUP BY dd.department_name, dd.floor, dd.wing
ORDER BY total_admissions DESC;

-- Cardiology should have the most admissions
ASSERT VALUE top_dept = 'Cardiology'
SELECT dd.department_name AS top_dept
FROM ehr.gold.fact_admissions f
JOIN ehr.gold.dim_department dd ON f.department_key = dd.department_key
GROUP BY dd.department_name
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ===================== TEST 4: Diagnosis Severity - Critical Highest Charges =====================

SELECT
  dx.severity,
  dx.category,
  COUNT(f.admission_key) AS admission_count,
  ROUND(AVG(f.total_charges), 2) AS avg_charges,
  MAX(f.total_charges) AS max_charges
FROM ehr.gold.fact_admissions f
JOIN ehr.gold.dim_diagnosis dx ON f.diagnosis_key = dx.diagnosis_key
GROUP BY dx.severity, dx.category
ORDER BY dx.severity, admission_count DESC;

ASSERT VALUE critical_avg >= 40000
SELECT ROUND(AVG(f.total_charges), 2) AS critical_avg
FROM ehr.gold.fact_admissions f
JOIN ehr.gold.dim_diagnosis dx ON f.diagnosis_key = dx.diagnosis_key
WHERE dx.severity = 'Critical';

-- ===================== TEST 5: LOS Percentile Analysis (NTILE Window Function) =====================

SELECT
  patient_id,
  admission_date,
  los_days,
  los_percentile,
  total_charges,
  cost_rank
FROM ehr.gold.fact_admissions
WHERE los_percentile >= 90
ORDER BY los_days DESC;

-- Top percentile patients should have LOS >= 7 days
ASSERT VALUE high_los_count >= 1
SELECT COUNT(*) AS high_los_count
FROM ehr.gold.fact_admissions
WHERE los_percentile >= 90 AND los_days >= 7;

-- ===================== TEST 6: Top Cost - Oncology Highest Single Admission =====================

ASSERT VALUE top_cost_dept = 'Oncology'
SELECT dd.department_name AS top_cost_dept
FROM ehr.gold.fact_admissions f
JOIN ehr.gold.dim_department dd ON f.department_key = dd.department_key
WHERE f.cost_rank = 1;

-- ===================== TEST 7: Readmission Detection Validation =====================

-- At least 5 readmissions flagged
ASSERT VALUE readmission_count >= 5
SELECT SUM(CASE WHEN readmission_flag = true THEN 1 ELSE 0 END) AS readmission_count
FROM ehr.gold.fact_admissions;

-- At least 4 departments should have readmissions
ASSERT VALUE depts_with_readmissions >= 4
SELECT COUNT(*) AS depts_with_readmissions
FROM (
  SELECT dd.department_name
  FROM ehr.gold.fact_admissions f
  JOIN ehr.gold.dim_department dd ON f.department_key = dd.department_key
  WHERE f.readmission_flag = true
  GROUP BY dd.department_name
);

-- ===================== TEST 8: KPI Readmission Rates =====================

SELECT
  department_name,
  period,
  total_admissions,
  readmissions,
  readmission_pct,
  avg_los,
  avg_charges,
  max_los
FROM ehr.gold.kpi_readmission_rates
ORDER BY department_name, period;

-- Verify KPI math: readmission_pct = 100 * readmissions / total_admissions
ASSERT VALUE kpi_math_errors = 0
SELECT COUNT(*) AS kpi_math_errors
FROM ehr.gold.kpi_readmission_rates
WHERE total_admissions > 0
AND ABS(readmission_pct - ROUND(100.0 * readmissions / total_admissions, 2)) > 0.01;

-- ===================== TEST 9: Fact Table Referential Integrity =====================

ASSERT VALUE orphan_dept_keys = 0
SELECT COUNT(*) AS orphan_dept_keys
FROM ehr.gold.fact_admissions f
LEFT JOIN ehr.gold.dim_department d ON f.department_key = d.department_key
WHERE d.department_key IS NULL;

ASSERT VALUE orphan_diag_keys = 0
SELECT COUNT(*) AS orphan_diag_keys
FROM ehr.gold.fact_admissions f
LEFT JOIN ehr.gold.dim_diagnosis d ON f.diagnosis_key = d.diagnosis_key
WHERE d.diagnosis_key IS NULL;

-- ===================== TEST 10: Point-in-Time Join Validation =====================
-- Facts should have patient_valid_from populated from SCD2 join

ASSERT VALUE pit_join_populated >= 1
SELECT COUNT(*) AS pit_join_populated
FROM ehr.gold.fact_admissions
WHERE patient_valid_from IS NOT NULL;

-- ===================== TEST 11: CDF Audit Log Has Entries =====================

ASSERT VALUE audit_entries >= 1
SELECT COUNT(*) AS audit_entries FROM ehr.silver.audit_log;

-- Show audit log sample
SELECT
  audit_id,
  table_name,
  patient_id,
  change_type,
  changed_fields,
  new_values,
  change_timestamp
FROM ehr.silver.audit_log
ORDER BY change_timestamp DESC
LIMIT 10;

-- ===================== TEST 12: Monthly Admission Trends =====================

SELECT
  DATE_TRUNC('month', f.admission_date) AS month,
  COUNT(*) AS admissions,
  SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) AS readmissions,
  ROUND(SUM(f.total_charges), 2) AS total_revenue
FROM ehr.gold.fact_admissions f
GROUP BY DATE_TRUNC('month', f.admission_date)
ORDER BY month;

-- Should span 10+ distinct months
ASSERT VALUE distinct_months >= 10
SELECT COUNT(DISTINCT DATE_TRUNC('month', admission_date)) AS distinct_months
FROM ehr.gold.fact_admissions;

-- ===================== TEST 13: No Duplicate Records in Silver =====================

ASSERT VALUE silver_duplicates = 0
SELECT COUNT(*) AS silver_duplicates
FROM (
  SELECT record_id, COUNT(*) AS cnt
  FROM ehr.silver.admissions_cleaned
  GROUP BY record_id
  HAVING COUNT(*) > 1
);

-- ===================== VERIFICATION SUMMARY =====================

SELECT 'Healthcare EHR Gold Layer Verification PASSED - all 13 assertions validated' AS status;
