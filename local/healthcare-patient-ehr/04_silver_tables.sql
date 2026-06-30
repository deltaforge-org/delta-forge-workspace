-- =============================================================================
-- Healthcare Patient EHR Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE ehr_silver_tables
  DESCRIPTION 'Creates silver layer tables for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- SCD2 patient dimension with CDF enabled for audit trail
CREATE DELTA TABLE IF NOT EXISTS ehr.silver.patient_dim (
  patient_id          STRING      NOT NULL,
  patient_name        STRING,
  ssn                 STRING,
  date_of_birth       DATE,
  email               STRING,
  address             STRING,
  city                STRING,
  state               STRING,
  insurance_id        STRING,
  insurance_name      STRING,
  valid_from          DATE        NOT NULL,
  valid_to            DATE,
  is_current          BOOLEAN     NOT NULL,
  updated_at          TIMESTAMP   NOT NULL
) LOCATION 'ehr/silver/ehr/patient_dim'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE ehr.silver.patient_dim TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.silver.admissions_cleaned (
  record_id           STRING      NOT NULL,
  patient_id          STRING      NOT NULL,
  department_code     STRING      NOT NULL,
  diagnosis_code      STRING      NOT NULL,
  admission_date      DATE        NOT NULL,
  discharge_date      DATE,
  los_days            INT,
  total_charges       DECIMAL(12,2),
  attending_physician STRING,
  readmission_flag    BOOLEAN     NOT NULL,
  prev_discharge_date DATE,
  days_since_last_discharge INT,
  los_percentile      INT,
  ingested_at         TIMESTAMP   NOT NULL,
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'ehr/silver/ehr/admissions_cleaned';

GRANT ADMIN ON TABLE ehr.silver.admissions_cleaned TO USER admin;

-- CDF-driven audit log capturing every change to patient_dim
CREATE DELTA TABLE IF NOT EXISTS ehr.silver.audit_log (
  audit_id            BIGINT      NOT NULL,
  table_name          STRING      NOT NULL,
  patient_id          STRING      NOT NULL,
  change_type         STRING      NOT NULL,
  changed_fields      STRING,
  old_values          STRING,
  new_values          STRING,
  change_timestamp    TIMESTAMP   NOT NULL
) LOCATION 'ehr/silver/ehr/audit_log';

GRANT ADMIN ON TABLE ehr.silver.audit_log TO USER admin;
