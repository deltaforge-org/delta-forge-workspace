-- =============================================================================
-- HR Workforce Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS hr.bronze.raw_employees (
  employee_id         STRING      NOT NULL,
  employee_name       STRING      NOT NULL,
  ssn                 STRING,
  email               STRING,
  date_of_birth       STRING,
  gender              STRING,
  hire_date           STRING      NOT NULL,
  termination_date    STRING,
  department_id       STRING      NOT NULL,
  position_id         STRING      NOT NULL,
  education_level     STRING,
  status              STRING      NOT NULL,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'hr/bronze/workforce/raw_employees';

GRANT ADMIN ON TABLE hr.bronze.raw_employees TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.bronze.raw_comp_events (
  event_id            STRING      NOT NULL,
  employee_id         STRING      NOT NULL,
  department_id       STRING      NOT NULL,
  position_id         STRING      NOT NULL,
  event_date          STRING      NOT NULL,
  event_type          STRING      NOT NULL,
  base_salary         DECIMAL(10,2),
  bonus               DECIMAL(10,2),
  performance_rating  DECIMAL(3,1),
  notes               STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'hr/bronze/workforce/raw_comp_events';

GRANT ADMIN ON TABLE hr.bronze.raw_comp_events TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.bronze.raw_departments (
  department_id       STRING      NOT NULL,
  department_name     STRING      NOT NULL,
  division            STRING,
  cost_center         STRING,
  head_count_budget   INT,
  annual_budget       DECIMAL(14,2),
  manager_name        STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'hr/bronze/workforce/raw_departments';

GRANT ADMIN ON TABLE hr.bronze.raw_departments TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.bronze.raw_positions (
  position_id         STRING      NOT NULL,
  title               STRING      NOT NULL,
  job_family          STRING,
  job_level           INT,
  pay_grade_min       DECIMAL(10,2),
  pay_grade_max       DECIMAL(10,2),
  exempt_flag         BOOLEAN,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'hr/bronze/workforce/raw_positions';

GRANT ADMIN ON TABLE hr.bronze.raw_positions TO USER admin;
