-- =============================================================================
-- HR Workforce Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE 05_gold_tables
  DESCRIPTION 'Creates gold layer tables for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS hr.gold.dim_department (
  department_key      INT         NOT NULL,
  department_id       STRING      NOT NULL,
  department_name     STRING      NOT NULL,
  division            STRING,
  cost_center         STRING,
  head_count_budget   INT,
  annual_budget       DECIMAL(14,2),
  manager_name        STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'hr/gold/workforce/dim_department';

GRANT ADMIN ON TABLE hr.gold.dim_department TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.gold.dim_position (
  position_key        INT         NOT NULL,
  position_id         STRING      NOT NULL,
  title               STRING      NOT NULL,
  job_family          STRING,
  job_level           INT,
  pay_grade_min       DECIMAL(10,2),
  pay_grade_max       DECIMAL(10,2),
  pay_grade_midpoint  DECIMAL(10,2),
  exempt_flag         BOOLEAN,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'hr/gold/workforce/dim_position';

GRANT ADMIN ON TABLE hr.gold.dim_position TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.gold.fact_compensation (
  event_key           INT         NOT NULL,
  employee_key        INT         NOT NULL,
  department_key      INT         NOT NULL,
  position_key        INT         NOT NULL,
  event_date          DATE        NOT NULL,
  event_type          STRING      NOT NULL,
  base_salary         DECIMAL(10,2),
  bonus               DECIMAL(10,2),
  total_comp          DECIMAL(10,2),
  salary_change_pct   DECIMAL(5,2),
  performance_rating  DECIMAL(3,1),
  compa_ratio         DECIMAL(5,3),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'hr/gold/workforce/fact_compensation';

GRANT ADMIN ON TABLE hr.gold.fact_compensation TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.gold.kpi_workforce_analytics (
  department_name     STRING      NOT NULL,
  quarter             STRING      NOT NULL,
  headcount           INT,
  avg_salary          DECIMAL(10,2),
  median_salary       DECIMAL(10,2),
  turnover_rate       DECIMAL(5,2),
  promotion_rate      DECIMAL(5,2),
  avg_tenure_years    DECIMAL(4,1),
  gender_pay_gap_pct  DECIMAL(5,2),
  avg_compa_ratio     DECIMAL(5,3),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'hr/gold/workforce/kpi_workforce_analytics';

GRANT ADMIN ON TABLE hr.gold.kpi_workforce_analytics TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS hr.gold.kpi_retention_risk (
  employee_id         STRING      NOT NULL,
  employee_name       STRING,
  department_name     STRING,
  title               STRING,
  tenure_years        DECIMAL(4,1),
  salary_change_pct   DECIMAL(5,2),
  promotions_in_3yr   INT,
  compa_ratio         DECIMAL(5,3),
  risk_score          INT,
  risk_category       STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'hr/gold/workforce/kpi_retention_risk';

GRANT ADMIN ON TABLE hr.gold.kpi_retention_risk TO USER admin;
