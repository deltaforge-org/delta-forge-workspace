-- =============================================================================
-- Legal Case Management Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE legal_case_management_04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS legal.silver.cases_enriched (
  case_id             STRING      NOT NULL,
  case_number         STRING      NOT NULL,
  case_type           STRING      NOT NULL,
  court               STRING,
  filing_date         DATE,
  close_date          DATE,
  status              STRING,
  priority            STRING,
  duration_days       INT,
  attorney_count      INT,
  party_count         INT,
  total_billed_hours  DECIMAL(8,2),
  total_billed_amount DECIMAL(12,2),
  complexity_score    DECIMAL(5,2),
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'legal/silver/legal/cases_enriched';

GRANT ADMIN ON TABLE legal.silver.cases_enriched TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.silver.billings_validated (
  billing_id          STRING      NOT NULL,
  case_id             STRING      NOT NULL,
  attorney_id         STRING      NOT NULL,
  billing_date        DATE        NOT NULL,
  hours               DECIMAL(5,2) NOT NULL CHECK (hours > 0),
  hourly_rate         DECIMAL(8,2) NOT NULL CHECK (hourly_rate > 0),
  amount              DECIMAL(10,2),
  billable_flag       BOOLEAN     NOT NULL,
  billing_type        STRING,
  case_type           STRING,
  practice_group      STRING,
  partner_flag        BOOLEAN,
  complexity_score    DECIMAL(5,2),
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'legal/silver/legal/billings_validated';

GRANT ADMIN ON TABLE legal.silver.billings_validated TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.silver.party_profiles (
  party_id            STRING      NOT NULL,
  party_name          STRING      NOT NULL,
  party_type          STRING      NOT NULL,
  cases_as_plaintiff  INT,
  cases_as_defendant  INT,
  cases_as_witness    INT,
  cases_as_expert     INT,
  total_involvement   INT,
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'legal/silver/legal/party_profiles';

GRANT ADMIN ON TABLE legal.silver.party_profiles TO USER admin;
