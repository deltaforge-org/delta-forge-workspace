-- =============================================================================
-- Healthcare Patient EHR Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE healthcare_patient_ehr_cleanup
  DESCRIPTION 'Cleanup pipeline for Healthcare Patient Ehr — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'healthcare-patient-ehr'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (ssn);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (email);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (patient_name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (ssn);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (email);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (patient_name);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_readmission_rates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_admissions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_diagnosis WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_department WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.patients_deduped WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.admissions_cleaned WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_diagnoses WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_departments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_admissions WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS {{zone_prefix}};
