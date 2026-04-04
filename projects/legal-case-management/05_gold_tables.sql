-- =============================================================================
-- Legal Case Management Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE legal_gold_tables
  DESCRIPTION 'Creates gold layer tables for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_case (
  case_key            INT         NOT NULL,
  case_id             STRING      NOT NULL,
  case_number         STRING      NOT NULL,
  case_type           STRING      NOT NULL,
  court               STRING,
  filing_date         DATE,
  close_date          DATE,
  status              STRING,
  priority            STRING,
  complexity_score    DECIMAL(5,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'legal/gold/legal/dim_case';

GRANT ADMIN ON TABLE legal.gold.dim_case TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_attorney (
  attorney_key        INT         NOT NULL,
  attorney_id         STRING      NOT NULL,
  attorney_name       STRING      NOT NULL,
  bar_number          STRING,
  practice_group      STRING,
  partner_flag        BOOLEAN,
  years_experience    INT,
  hourly_rate         DECIMAL(8,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'legal/gold/legal/dim_attorney';

GRANT ADMIN ON TABLE legal.gold.dim_attorney TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_party (
  party_key           INT         NOT NULL,
  party_id            STRING      NOT NULL,
  party_name          STRING      NOT NULL,
  party_type          STRING      NOT NULL,
  organization        STRING,
  jurisdiction        STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'legal/gold/legal/dim_party';

GRANT ADMIN ON TABLE legal.gold.dim_party TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.gold.fact_billings (
  billing_key         INT         NOT NULL,
  case_key            INT         NOT NULL,
  attorney_key        INT         NOT NULL,
  billing_date        DATE        NOT NULL,
  hours               DECIMAL(5,2) NOT NULL,
  hourly_rate         DECIMAL(8,2) NOT NULL,
  amount              DECIMAL(10,2),
  billable_flag       BOOLEAN     NOT NULL,
  case_complexity     DECIMAL(5,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'legal/gold/legal/fact_billings';

GRANT ADMIN ON TABLE legal.gold.fact_billings TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.gold.kpi_firm_performance (
  attorney_id         STRING      NOT NULL,
  attorney_name       STRING      NOT NULL,
  practice_group      STRING,
  partner_flag        BOOLEAN,
  total_hours         DECIMAL(8,2),
  billable_hours      DECIMAL(8,2),
  utilization_pct     DECIMAL(5,2),
  total_revenue       DECIMAL(12,2),
  case_count          INT,
  avg_case_complexity DECIMAL(5,2),
  revenue_per_hour    DECIMAL(8,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'legal/gold/legal/kpi_firm_performance';

GRANT ADMIN ON TABLE legal.gold.kpi_firm_performance TO USER admin;
