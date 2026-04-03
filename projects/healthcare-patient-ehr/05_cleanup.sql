-- =============================================================================
-- Healthcare Patient EHR Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.

PIPELINE healthcare_patient_ehr_cleanup
  DESCRIPTION 'Cleanup pipeline for Healthcare Patient EHR — drops all objects including SCD2 patient_dim, CDF audit_log, and pseudonymisation rules. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'healthcare-patient-ehr'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patient_dim (ssn);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patient_dim (email);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patient_dim (patient_name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patient_dim (date_of_birth);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_readmission_rates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_admissions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_diagnosis WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_department WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.audit_log WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.admissions_cleaned WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.patient_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_diagnoses WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_departments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_patients WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_admissions WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS {{zone_prefix}};
