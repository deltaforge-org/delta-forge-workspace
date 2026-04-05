-- =============================================================================
-- Legal Case Management Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE legal_case_management_02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_cases (
  case_id             STRING      NOT NULL,
  case_number         STRING      NOT NULL,
  case_type           STRING      NOT NULL,
  court               STRING,
  filing_date         DATE,
  close_date          DATE,
  status              STRING,
  priority            STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'legal/bronze/legal/raw_cases';

GRANT ADMIN ON TABLE legal.bronze.raw_cases TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_parties (
  party_id            STRING      NOT NULL,
  party_name          STRING      NOT NULL,
  party_type          STRING      NOT NULL,
  ssn                 STRING,
  contact_email       STRING,
  contact_phone       STRING,
  organization        STRING,
  jurisdiction        STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'legal/bronze/legal/raw_parties';

GRANT ADMIN ON TABLE legal.bronze.raw_parties TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_attorneys (
  attorney_id         STRING      NOT NULL,
  attorney_name       STRING      NOT NULL,
  bar_number          STRING      NOT NULL,
  practice_group      STRING,
  partner_flag        BOOLEAN,
  years_experience    INT,
  hourly_rate         DECIMAL(8,2),
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'legal/bronze/legal/raw_attorneys';

GRANT ADMIN ON TABLE legal.bronze.raw_attorneys TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_billings (
  billing_id          STRING      NOT NULL,
  case_id             STRING      NOT NULL,
  attorney_id         STRING      NOT NULL,
  billing_date        DATE        NOT NULL,
  hours               DECIMAL(5,2) NOT NULL,
  hourly_rate         DECIMAL(8,2) NOT NULL,
  amount              DECIMAL(10,2),
  billable_flag       BOOLEAN     NOT NULL,
  billing_type        STRING,
  description         STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'legal/bronze/legal/raw_billings';

GRANT ADMIN ON TABLE legal.bronze.raw_billings TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_relationships (
  relationship_id     STRING      NOT NULL,
  source_id           STRING      NOT NULL,
  target_id           STRING      NOT NULL,
  source_type         STRING      NOT NULL,
  target_type         STRING      NOT NULL,
  relationship_type   STRING      NOT NULL,
  weight              DECIMAL(5,2),
  effective_date      DATE,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'legal/bronze/legal/raw_relationships';

GRANT ADMIN ON TABLE legal.bronze.raw_relationships TO USER admin;
