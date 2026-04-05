-- =============================================================================
-- Healthcare Patient EHR Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS ehr.bronze.raw_admissions (
  record_id           STRING      NOT NULL,
  patient_id          STRING      NOT NULL,
  department_code     STRING,
  diagnosis_code      STRING,
  admission_date      STRING,
  discharge_date      STRING,
  total_charges       DECIMAL(12,2),
  attending_physician STRING,
  notes               STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ehr/bronze/ehr/raw_admissions';

GRANT ADMIN ON TABLE ehr.bronze.raw_admissions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.bronze.raw_patients (
  patient_id          STRING      NOT NULL,
  patient_name        STRING,
  ssn                 STRING,
  date_of_birth       STRING,
  email               STRING,
  address             STRING,
  city                STRING,
  state               STRING,
  insurance_id        STRING,
  insurance_name      STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ehr/bronze/ehr/raw_patients';

GRANT ADMIN ON TABLE ehr.bronze.raw_patients TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.bronze.raw_departments (
  department_code     STRING      NOT NULL,
  department_name     STRING      NOT NULL,
  floor               INT,
  wing                STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ehr/bronze/ehr/raw_departments';

GRANT ADMIN ON TABLE ehr.bronze.raw_departments TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.bronze.raw_diagnoses (
  diagnosis_code      STRING      NOT NULL,
  description         STRING      NOT NULL,
  category            STRING,
  severity            STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ehr/bronze/ehr/raw_diagnoses';

GRANT ADMIN ON TABLE ehr.bronze.raw_diagnoses TO USER admin;
