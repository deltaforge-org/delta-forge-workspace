-- =============================================================================
-- Healthcare Patient EHR Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE ehr_gold_tables
  DESCRIPTION 'Creates gold layer tables for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS ehr.gold.dim_department (
  department_key      INT         NOT NULL,
  department_code     STRING      NOT NULL,
  department_name     STRING      NOT NULL,
  floor               INT,
  wing                STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ehr/gold/ehr/dim_department';

GRANT ADMIN ON TABLE ehr.gold.dim_department TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.gold.dim_diagnosis (
  diagnosis_key       INT         NOT NULL,
  diagnosis_code      STRING      NOT NULL,
  description         STRING      NOT NULL,
  category            STRING,
  severity            STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ehr/gold/ehr/dim_diagnosis';

GRANT ADMIN ON TABLE ehr.gold.dim_diagnosis TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.gold.fact_admissions (
  admission_key       INT         NOT NULL,
  patient_id          STRING      NOT NULL,
  patient_name_hash   STRING,
  department_key      INT         NOT NULL,
  diagnosis_key       INT         NOT NULL,
  admission_date      DATE        NOT NULL,
  discharge_date      DATE,
  los_days            INT,
  total_charges       DECIMAL(12,2),
  readmission_flag    BOOLEAN     NOT NULL,
  los_percentile      INT,
  cost_rank           INT,
  patient_valid_from  DATE,
  patient_valid_to    DATE,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ehr/gold/ehr/fact_admissions';

GRANT ADMIN ON TABLE ehr.gold.fact_admissions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS ehr.gold.kpi_readmission_rates (
  department_name     STRING      NOT NULL,
  period              STRING      NOT NULL,
  total_admissions    INT         NOT NULL,
  readmissions        INT         NOT NULL,
  readmission_pct     DECIMAL(5,2),
  avg_los             DECIMAL(5,2),
  avg_charges         DECIMAL(12,2),
  max_los             INT,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ehr/gold/ehr/kpi_readmission_rates';

GRANT ADMIN ON TABLE ehr.gold.kpi_readmission_rates TO USER admin;
