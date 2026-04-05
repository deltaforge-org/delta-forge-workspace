-- =============================================================================
-- HR Workforce Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS hr.silver.employee_dim (
  surrogate_key       INT         NOT NULL,
  employee_id         STRING      NOT NULL,
  employee_name       STRING      NOT NULL,
  ssn                 STRING,
  email               STRING,
  hire_date           DATE        NOT NULL,
  termination_date    DATE,
  department_id       STRING      NOT NULL,
  position_id         STRING      NOT NULL,
  education_level     STRING,
  gender              STRING,
  date_of_birth       DATE,
  base_salary         DECIMAL(10,2),
  status              STRING,
  valid_from          DATE        NOT NULL,
  valid_to            DATE,
  is_current          BOOLEAN     NOT NULL
) LOCATION 'hr/silver/workforce/employee_dim'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE hr.silver.employee_dim TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.silver.comp_events_enriched (
  event_id            STRING      NOT NULL,
  employee_id         STRING      NOT NULL,
  department_id       STRING      NOT NULL,
  position_id         STRING      NOT NULL,
  event_date          DATE        NOT NULL,
  event_type          STRING      NOT NULL,
  base_salary         DECIMAL(10,2),
  bonus               DECIMAL(10,2),
  total_comp          DECIMAL(10,2),
  salary_change_pct   DECIMAL(5,2),
  performance_rating  DECIMAL(3,1),
  compa_ratio         DECIMAL(5,3),
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'hr/silver/workforce/comp_events_enriched';

GRANT ADMIN ON TABLE hr.silver.comp_events_enriched TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.silver.org_change_log (
  change_id           INT         NOT NULL,
  employee_id         STRING      NOT NULL,
  employee_name       STRING,
  change_type         STRING      NOT NULL,
  old_value           STRING,
  new_value           STRING,
  effective_date      DATE        NOT NULL,
  captured_at         TIMESTAMP   NOT NULL
) LOCATION 'hr/silver/workforce/org_change_log'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE hr.silver.org_change_log TO USER admin;
