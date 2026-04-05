-- =============================================================================
-- Healthcare Patient EHR Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.

PIPELINE healthcare_patient_ehr_cleanup
  DESCRIPTION 'Cleanup pipeline for Healthcare Patient EHR — drops all objects including SCD2 patient_dim, CDF audit_log, and pseudonymisation rules. DISABLED by default.'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'cleanup', 'maintenance', 'healthcare-patient-ehr'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON ehr.silver.patient_dim (ssn);
DROP PSEUDONYMISATION RULE IF EXISTS ON ehr.silver.patient_dim (email);
DROP PSEUDONYMISATION RULE IF EXISTS ON ehr.silver.patient_dim (patient_name);
DROP PSEUDONYMISATION RULE IF EXISTS ON ehr.silver.patient_dim (date_of_birth);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS ehr.gold.kpi_readmission_rates WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.gold.fact_admissions WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.gold.dim_diagnosis WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.gold.dim_department WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS ehr.silver.audit_log WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.silver.admissions_cleaned WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.silver.patient_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS ehr.bronze.raw_diagnoses WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.bronze.raw_departments WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.bronze.raw_patients WITH FILES;
DROP DELTA TABLE IF EXISTS ehr.bronze.raw_admissions WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS ehr.gold;
DROP SCHEMA IF EXISTS ehr.silver;
DROP SCHEMA IF EXISTS ehr.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS ehr;
