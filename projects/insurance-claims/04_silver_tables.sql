-- =============================================================================
-- Insurance Claims Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- SCD2 policy dimension with surrogate keys, CDF-enabled for actuarial tracking
CREATE DELTA TABLE IF NOT EXISTS ins.silver.policy_dim (
  surrogate_key       INT         NOT NULL,
  policy_id           STRING      NOT NULL,
  holder_name         STRING      NOT NULL,
  coverage_type       STRING      NOT NULL,
  annual_premium      DECIMAL(10,2) CHECK (annual_premium > 0),
  region              STRING,
  state               STRING,
  risk_score          DECIMAL(4,2) CHECK (risk_score >= 0 AND risk_score <= 10),
  valid_from          DATE        NOT NULL,
  valid_to            DATE,
  is_current          INT         NOT NULL CHECK (is_current IN (0, 1)),
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/silver/policy_dim'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Enriched claims with point-in-time policy join and fraud scoring
CREATE DELTA TABLE IF NOT EXISTS ins.silver.claims_enriched (
  claim_id            STRING      NOT NULL,
  policy_id           STRING      NOT NULL,
  claimant_id         STRING      NOT NULL,
  adjuster_id         STRING      NOT NULL,
  incident_date       DATE        NOT NULL,
  reported_date       DATE        NOT NULL,
  claim_amount        DECIMAL(12,2) NOT NULL CHECK (claim_amount > 0),
  approved_amount     DECIMAL(12,2) CHECK (approved_amount >= 0),
  status              STRING      NOT NULL,
  settlement_date     DATE,
  days_to_settle      INT,
  days_to_report      INT,
  coverage_type       STRING,
  annual_premium_at_incident DECIMAL(10,2),
  region              STRING,
  risk_score_at_incident DECIMAL(4,2),
  fraud_risk          STRING,
  fraud_score         DECIMAL(5,2),
  processed_at        TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/silver/claims_enriched'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Actuarial snapshots driven by CDF on claims_enriched
CREATE DELTA TABLE IF NOT EXISTS ins.silver.actuarial_snapshots (
  snapshot_id         STRING      NOT NULL,
  claim_id            STRING      NOT NULL,
  policy_id           STRING      NOT NULL,
  status              STRING      NOT NULL,
  claim_amount        DECIMAL(12,2),
  approved_amount     DECIMAL(12,2),
  coverage_type       STRING,
  change_type         STRING,
  captured_at         TIMESTAMP
) LOCATION 'ins/insurance/silver/actuarial_snapshots';
