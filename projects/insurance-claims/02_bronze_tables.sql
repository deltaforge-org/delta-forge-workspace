-- =============================================================================
-- Insurance Claims Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS ins.bronze.raw_policies (
  policy_id           STRING      NOT NULL,
  holder_name         STRING      NOT NULL,
  ssn                 STRING,
  coverage_type       STRING      NOT NULL,
  annual_premium      DECIMAL(10,2),
  region              STRING,
  state               STRING,
  risk_score          DECIMAL(4,2),
  effective_date      DATE        NOT NULL,
  change_type         STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/bronze/raw_policies';

CREATE DELTA TABLE IF NOT EXISTS ins.bronze.raw_claims (
  claim_id            STRING      NOT NULL,
  policy_id           STRING      NOT NULL,
  claimant_id         STRING      NOT NULL,
  adjuster_id         STRING      NOT NULL,
  incident_date       DATE        NOT NULL,
  reported_date       DATE        NOT NULL,
  claim_amount        DECIMAL(12,2) NOT NULL,
  approved_amount     DECIMAL(12,2),
  status              STRING      NOT NULL,
  settlement_date     DATE,
  description         STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/bronze/raw_claims';

CREATE DELTA TABLE IF NOT EXISTS ins.bronze.raw_claimants (
  claimant_id         STRING      NOT NULL,
  name                STRING      NOT NULL,
  ssn                 STRING,
  date_of_birth       DATE,
  age_band            STRING,
  state               STRING,
  risk_tier           STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/bronze/raw_claimants';

CREATE DELTA TABLE IF NOT EXISTS ins.bronze.raw_adjusters (
  adjuster_id         STRING      NOT NULL,
  name                STRING      NOT NULL,
  specialization      STRING,
  years_experience    INT,
  region              STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/bronze/raw_adjusters';
