-- =============================================================================
-- Insurance Claims Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE 05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS ins.gold.dim_claimant (
  claimant_key        INT         NOT NULL,
  claimant_id         STRING      NOT NULL,
  name                STRING,
  age_band            STRING,
  state               STRING,
  risk_tier           STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/dim_claimant';

CREATE DELTA TABLE IF NOT EXISTS ins.gold.dim_adjuster (
  adjuster_key        INT         NOT NULL,
  adjuster_id         STRING      NOT NULL,
  name                STRING      NOT NULL,
  specialization      STRING,
  years_experience    INT,
  region              STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/dim_adjuster';

CREATE DELTA TABLE IF NOT EXISTS ins.gold.dim_coverage_type (
  coverage_key        STRING      NOT NULL,
  coverage_type       STRING      NOT NULL,
  coverage_category   STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/dim_coverage_type';

CREATE DELTA TABLE IF NOT EXISTS ins.gold.fact_claims (
  claim_key           INT         NOT NULL,
  policy_surrogate_key INT        NOT NULL,
  claimant_key        INT         NOT NULL,
  adjuster_key        INT         NOT NULL,
  coverage_key        STRING      NOT NULL,
  incident_date       DATE        NOT NULL,
  reported_date       DATE        NOT NULL,
  claim_amount        DECIMAL(12,2) NOT NULL,
  approved_amount     DECIMAL(12,2),
  status              STRING      NOT NULL,
  days_to_settle      INT,
  days_to_report      INT,
  fraud_risk          STRING,
  fraud_score         DECIMAL(5,2),
  loss_ratio          DECIMAL(6,4),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/fact_claims';

CREATE DELTA TABLE IF NOT EXISTS ins.gold.kpi_loss_ratios (
  coverage_type       STRING      NOT NULL,
  region              STRING      NOT NULL,
  quarter             STRING      NOT NULL,
  claim_count         INT         NOT NULL,
  total_claimed       DECIMAL(14,2),
  total_approved      DECIMAL(14,2),
  total_premium       DECIMAL(14,2),
  loss_ratio          DECIMAL(6,4),
  avg_days_to_settle  DECIMAL(6,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/kpi_loss_ratios';

CREATE DELTA TABLE IF NOT EXISTS ins.gold.kpi_adjuster_performance (
  adjuster_id         STRING      NOT NULL,
  adjuster_name       STRING,
  specialization      STRING,
  claims_handled      INT,
  claims_settled      INT,
  claims_denied       INT,
  avg_days_to_settle  DECIMAL(6,2),
  avg_claim_amount    DECIMAL(12,2),
  approval_rate_pct   DECIMAL(5,2),
  total_approved      DECIMAL(14,2),
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'ins/insurance/gold/kpi_adjuster_performance';
