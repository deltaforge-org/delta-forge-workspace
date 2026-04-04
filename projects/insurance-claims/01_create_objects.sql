-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Object Creation & Seed Data
-- =============================================================================
-- Narrative: You are a data engineer at a P&C insurer. Policies change frequently
-- (premium adjustments, coverage upgrades, address changes) tracked as SCD2.
-- Claims arrive days/weeks after incidents. The pipeline reconstructs "policy
-- state at time of incident" via point-in-time joins, detects potential fraud
-- via statistical outlier analysis, and supports RESTORE for bad-batch rollback.
-- =============================================================================

-- ===================== SCHEDULE =====================

SCHEDULE ins_weekly_schedule
  CRON '0 7 * * 1'
  TIMEZONE 'America/Chicago'
  RETRIES 2
  TIMEOUT 7200
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE ins_create_objects
  DESCRIPTION 'Creates zones, schemas, tables, seed data, and pseudonymisation rules for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;


-- ===================== ZONE =====================

CREATE ZONE IF NOT EXISTS ins TYPE TEMP
  COMMENT 'Property and casualty insurance project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS ins.bronze COMMENT 'Raw policy, claims, claimant, and adjuster feeds';
CREATE SCHEMA IF NOT EXISTS ins.silver COMMENT 'SCD2 policy dimension, enriched claims, actuarial CDF';
CREATE SCHEMA IF NOT EXISTS ins.gold   COMMENT 'Claims analytics star schema with loss ratio and adjuster KPIs';

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

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON ins.gold.dim_claimant (name) TRANSFORM mask PARAMS (show = 1);
CREATE PSEUDONYMISATION RULE ON ins.bronze.raw_claimants (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');
CREATE PSEUDONYMISATION RULE ON ins.bronze.raw_policies (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE ins.bronze.raw_policies TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_claims TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_claimants TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_adjusters TO USER admin;
GRANT ADMIN ON TABLE ins.silver.policy_dim TO USER admin;
GRANT ADMIN ON TABLE ins.silver.claims_enriched TO USER admin;
GRANT ADMIN ON TABLE ins.silver.actuarial_snapshots TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_claimant TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_adjuster TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_coverage_type TO USER admin;
GRANT ADMIN ON TABLE ins.gold.fact_claims TO USER admin;
GRANT ADMIN ON TABLE ins.gold.kpi_loss_ratios TO USER admin;
GRANT ADMIN ON TABLE ins.gold.kpi_adjuster_performance TO USER admin;

-- =============================================================================
-- SEED DATA: ADJUSTERS (6 rows)
-- =============================================================================

INSERT INTO ins.bronze.raw_adjusters VALUES
  ('ADJ01', 'Margaret O''Brien',  'property',     12, 'Northeast', '2024-01-15T00:00:00'),
  ('ADJ02', 'Richard Townsend',   'auto',          8, 'West',      '2024-01-15T00:00:00'),
  ('ADJ03', 'Yuki Tanaka',        'liability',    15, 'Midwest',   '2024-01-15T00:00:00'),
  ('ADJ04', 'Carlos Mendez',      'health',        6, 'Southeast', '2024-01-15T00:00:00'),
  ('ADJ05', 'Priya Sharma',       'workers_comp', 10, 'South',     '2024-01-15T00:00:00'),
  ('ADJ06', 'James Whitfield',    'multi_peril',  20, 'Northeast', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_adjusters;

-- =============================================================================
-- SEED DATA: CLAIMANTS (12 rows) — with SSN for pseudonymisation demo
-- =============================================================================

INSERT INTO ins.bronze.raw_claimants VALUES
  ('CLM01', 'Albert Donovan',    '123-45-6789', '1975-03-15', '45-54', 'CA', 'standard',  '2024-01-15T00:00:00'),
  ('CLM02', 'Barbara Chen',      '234-56-7890', '1982-08-22', '35-44', 'NY', 'preferred', '2024-01-15T00:00:00'),
  ('CLM03', 'Chris Okafor',      '345-67-8901', '1990-11-10', '25-34', 'TX', 'standard',  '2024-01-15T00:00:00'),
  ('CLM04', 'Diana Petrov',      '456-78-9012', '1966-05-28', '55-64', 'FL', 'high_risk', '2024-01-15T00:00:00'),
  ('CLM05', 'Eduardo Silva',     '567-89-0123', '1984-01-17', '35-44', 'IL', 'preferred', '2024-01-15T00:00:00'),
  ('CLM06', 'Fiona McAllister',  '678-90-1234', '1992-07-03', '25-34', 'WA', 'standard',  '2024-01-15T00:00:00'),
  ('CLM07', 'George Nakamura',   '789-01-2345', '1973-12-20', '45-54', 'CA', 'high_risk', '2024-01-15T00:00:00'),
  ('CLM08', 'Hannah Wright',     '890-12-3456', '1958-09-14', '65+',   'AZ', 'standard',  '2024-01-15T00:00:00'),
  ('CLM09', 'Ivan Popov',        '901-23-4567', '1981-04-25', '35-44', 'NJ', 'preferred', '2024-01-15T00:00:00'),
  ('CLM10', 'Julia Fernandez',   '012-34-5678', '1993-06-11', '25-34', 'CO', 'standard',  '2024-01-15T00:00:00'),
  ('CLM11', 'Kenneth Park',      '111-22-3333', '1970-02-08', '50-59', 'OR', 'preferred', '2024-01-15T00:00:00'),
  ('CLM12', 'Laura Esposito',    '222-33-4444', '1988-10-30', '30-39', 'GA', 'standard',  '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_claimants;

-- =============================================================================
-- SEED DATA: POLICIES — 3 batches totaling 23 rows
-- =============================================================================
-- Batch 1: 15 initial policies (change_type = 'new')
-- Batch 2: 5 premium increases
-- Batch 3: 3 coverage upgrades
-- Result: 23 rows total, only 15 with is_current=1 after SCD2 processing

INSERT INTO ins.bronze.raw_policies VALUES
  -- BATCH 1: 15 initial policies
  ('POL001', 'Albert Donovan',   '123-45-6789', 'auto',         1200.00, 'West',      'CA', 3.2, '2022-01-01', 'new', '2024-01-15T00:00:00'),
  ('POL002', 'Barbara Chen',     '234-56-7890', 'home',         2400.00, 'Northeast', 'NY', 2.1, '2021-06-15', 'new', '2024-01-15T00:00:00'),
  ('POL003', 'Chris Okafor',     '345-67-8901', 'auto',          980.00, 'South',     'TX', 2.8, '2022-03-01', 'new', '2024-01-15T00:00:00'),
  ('POL004', 'Diana Petrov',     '456-78-9012', 'health',       4800.00, 'Southeast', 'FL', 4.5, '2020-01-01', 'new', '2024-01-15T00:00:00'),
  ('POL005', 'Eduardo Silva',    '567-89-0123', 'liability',    3200.00, 'Midwest',   'IL', 2.0, '2021-09-01', 'new', '2024-01-15T00:00:00'),
  ('POL006', 'Fiona McAllister', '678-90-1234', 'auto',         1100.00, 'West',      'WA', 2.5, '2023-01-15', 'new', '2024-01-15T00:00:00'),
  ('POL007', 'George Nakamura',  '789-01-2345', 'home',         3600.00, 'West',      'CA', 4.1, '2020-04-01', 'new', '2024-01-15T00:00:00'),
  ('POL008', 'Hannah Wright',    '890-12-3456', 'health',       5200.00, 'Southwest', 'AZ', 3.6, '2019-01-01', 'new', '2024-01-15T00:00:00'),
  ('POL009', 'Ivan Popov',       '901-23-4567', 'workers_comp', 2800.00, 'Northeast', 'NJ', 2.3, '2022-07-01', 'new', '2024-01-15T00:00:00'),
  ('POL010', 'Julia Fernandez',  '012-34-5678', 'auto',          890.00, 'West',      'CO', 1.9, '2023-05-01', 'new', '2024-01-15T00:00:00'),
  ('POL011', 'Albert Donovan',   '123-45-6789', 'home',         2200.00, 'West',      'CA', 3.0, '2022-06-01', 'new', '2024-01-15T00:00:00'),
  ('POL012', 'Barbara Chen',     '234-56-7890', 'liability',    1800.00, 'Northeast', 'NY', 2.2, '2023-01-01', 'new', '2024-01-15T00:00:00'),
  ('POL013', 'Chris Okafor',     '345-67-8901', 'health',       3400.00, 'South',     'TX', 2.9, '2022-08-01', 'new', '2024-01-15T00:00:00'),
  ('POL014', 'Diana Petrov',     '456-78-9012', 'auto',         1500.00, 'Southeast', 'FL', 4.0, '2021-03-01', 'new', '2024-01-15T00:00:00'),
  ('POL015', 'Eduardo Silva',    '567-89-0123', 'home',         2600.00, 'Midwest',   'IL', 2.1, '2022-11-01', 'new', '2024-01-15T00:00:00'),

  -- BATCH 2: 5 premium increases (expire old -> insert new current)
  ('POL001', 'Albert Donovan',   '123-45-6789', 'auto',         1380.00, 'West',      'CA', 3.8, '2023-01-01', 'premium_increase', '2024-01-15T00:00:00'),
  ('POL002', 'Barbara Chen',     '234-56-7890', 'home',         2640.00, 'Northeast', 'NY', 2.4, '2023-06-15', 'premium_increase', '2024-01-15T00:00:00'),
  ('POL004', 'Diana Petrov',     '456-78-9012', 'health',       5520.00, 'Southeast', 'FL', 4.8, '2022-01-01', 'premium_increase', '2024-01-15T00:00:00'),
  ('POL007', 'George Nakamura',  '789-01-2345', 'home',         4200.00, 'West',      'CA', 4.5, '2023-04-01', 'premium_increase', '2024-01-15T00:00:00'),
  ('POL008', 'Hannah Wright',    '890-12-3456', 'health',       5800.00, 'Southwest', 'AZ', 3.9, '2022-01-01', 'premium_increase', '2024-01-15T00:00:00'),

  -- BATCH 3: 3 coverage upgrades (expire old -> insert new current)
  ('POL001', 'Albert Donovan',   '123-45-6789', 'auto_plus',    1650.00, 'West',      'CA', 3.5, '2024-01-01', 'coverage_upgrade', '2024-01-15T00:00:00'),
  ('POL004', 'Diana Petrov',     '456-78-9012', 'health_plus',  6200.00, 'Southeast', 'FL', 4.2, '2023-07-01', 'coverage_upgrade', '2024-01-15T00:00:00'),
  ('POL007', 'George Nakamura',  '789-01-2345', 'home_plus',    4800.00, 'West',      'CA', 4.3, '2024-01-01', 'coverage_upgrade', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 23
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_policies;

-- =============================================================================
-- SEED DATA: CLAIMS (45 rows) — multiple statuses, 3 outliers for fraud
-- =============================================================================
-- Statuses: filed, under_review, approved, denied, settled
-- 30 settled (with settlement_date), 15 open/under_review/denied
-- 3 deliberately high claims for fraud detection (C0009, C0010, C0037)

INSERT INTO ins.bronze.raw_claims VALUES
  ('C0001', 'POL001', 'CLM01', 'ADJ02', '2023-03-15', '2023-03-16',   8500.00,  7200.00, 'settled',      '2023-05-10', 'Rear-end collision on highway',             '2024-01-15T00:00:00'),
  ('C0002', 'POL001', 'CLM01', 'ADJ02', '2023-08-20', '2023-08-22',   3200.00,  3200.00, 'settled',      '2023-09-15', 'Parking lot fender bender',                  '2024-01-15T00:00:00'),
  ('C0003', 'POL002', 'CLM02', 'ADJ01', '2023-02-10', '2023-02-12',  45000.00, 38000.00, 'settled',      '2023-06-20', 'Water damage from burst pipe',               '2024-01-15T00:00:00'),
  ('C0004', 'POL002', 'CLM02', 'ADJ01', '2023-09-05', '2023-09-06',  12000.00, 10500.00, 'settled',      '2023-11-30', 'Wind damage to roof',                        '2024-01-15T00:00:00'),
  ('C0005', 'POL003', 'CLM03', 'ADJ02', '2023-04-22', '2023-04-25',   5600.00,  4800.00, 'settled',      '2023-06-15', 'Side-impact collision',                      '2024-01-15T00:00:00'),
  ('C0006', 'POL004', 'CLM04', 'ADJ04', '2023-01-08', '2023-01-10',  28000.00, 22000.00, 'settled',      '2023-04-20', 'Hip replacement surgery',                    '2024-01-15T00:00:00'),
  ('C0007', 'POL004', 'CLM04', 'ADJ04', '2023-06-15', '2023-06-18',  15000.00, 12500.00, 'settled',      '2023-09-10', 'Physical therapy post-surgery',              '2024-01-15T00:00:00'),
  ('C0008', 'POL004', 'CLM04', 'ADJ04', '2023-11-01', '2023-11-03',  35000.00,     NULL, 'under_review', NULL,         'Cardiac procedure',                          '2024-01-15T00:00:00'),
  ('C0009', 'POL005', 'CLM05', 'ADJ03', '2023-05-10', '2023-05-12', 185000.00,150000.00, 'settled',      '2023-10-25', 'OUTLIER: massive product liability claim',   '2024-01-15T00:00:00'),
  ('C0010', 'POL005', 'CLM05', 'ADJ03', '2023-12-01', '2023-12-05', 220000.00,     NULL, 'filed',        NULL,         'OUTLIER: workplace injury catastrophic',     '2024-01-15T00:00:00'),
  ('C0011', 'POL006', 'CLM06', 'ADJ02', '2023-06-30', '2023-07-02',   4200.00,  4200.00, 'settled',      '2023-08-10', 'Windshield replacement',                     '2024-01-15T00:00:00'),
  ('C0012', 'POL007', 'CLM07', 'ADJ01', '2023-03-01', '2023-03-03',  22000.00, 18000.00, 'settled',      '2023-07-15', 'Fire damage to kitchen',                     '2024-01-15T00:00:00'),
  ('C0013', 'POL007', 'CLM07', 'ADJ01', '2023-10-15', '2023-10-18',  55000.00, 45000.00, 'settled',      '2024-01-10', 'Foundation structural damage',               '2024-01-15T00:00:00'),
  ('C0014', 'POL008', 'CLM08', 'ADJ04', '2023-02-20', '2023-02-22',   8500.00,  7000.00, 'settled',      '2023-04-10', 'Emergency room visit',                       '2024-01-15T00:00:00'),
  ('C0015', 'POL008', 'CLM08', 'ADJ04', '2023-07-10', '2023-07-12',  42000.00, 35000.00, 'settled',      '2023-11-20', 'Knee replacement surgery',                   '2024-01-15T00:00:00'),
  ('C0016', 'POL008', 'CLM08', 'ADJ04', '2023-12-05', '2023-12-08',   6500.00,     NULL, 'under_review', NULL,         'Post-operative complication',                '2024-01-15T00:00:00'),
  ('C0017', 'POL009', 'CLM09', 'ADJ05', '2023-08-14', '2023-08-16',  18000.00, 15000.00, 'settled',      '2023-11-01', 'Back injury on job site',                    '2024-01-15T00:00:00'),
  ('C0018', 'POL009', 'CLM09', 'ADJ05', '2023-12-20', '2023-12-22',  25000.00,     NULL, 'filed',        NULL,         'Repetitive strain injury',                   '2024-01-15T00:00:00'),
  ('C0019', 'POL010', 'CLM10', 'ADJ02', '2023-07-25', '2023-07-27',   2800.00,  2800.00, 'settled',      '2023-08-20', 'Minor fender bender',                        '2024-01-15T00:00:00'),
  ('C0020', 'POL011', 'CLM01', 'ADJ01', '2023-04-15', '2023-04-18',  15000.00, 12000.00, 'settled',      '2023-08-01', 'Hail damage to roof',                        '2024-01-15T00:00:00'),
  ('C0021', 'POL012', 'CLM02', 'ADJ03', '2023-09-20', '2023-09-22',  50000.00, 40000.00, 'settled',      '2024-01-05', 'Slip and fall liability',                    '2024-01-15T00:00:00'),
  ('C0022', 'POL013', 'CLM03', 'ADJ04', '2023-10-05', '2023-10-08',   9500.00,  8000.00, 'settled',      '2023-12-15', 'Specialist consultation series',             '2024-01-15T00:00:00'),
  ('C0023', 'POL014', 'CLM04', 'ADJ02', '2023-05-18', '2023-05-20',   6800.00,  5500.00, 'settled',      '2023-07-25', 'Intersection collision',                     '2024-01-15T00:00:00'),
  ('C0024', 'POL015', 'CLM05', 'ADJ01', '2023-11-10', '2023-11-13',  32000.00, 28000.00, 'settled',      '2024-01-08', 'Storm damage property',                      '2024-01-15T00:00:00'),
  ('C0025', 'POL001', 'CLM01', 'ADJ02', '2023-12-10', '2023-12-12',  11000.00,     NULL, 'under_review', NULL,         'Multi-vehicle accident',                     '2024-01-15T00:00:00'),
  ('C0026', 'POL003', 'CLM03', 'ADJ02', '2023-11-22', '2023-11-25',   7200.00,     NULL, 'filed',        NULL,         'Hit and run damage',                         '2024-01-15T00:00:00'),
  ('C0027', 'POL006', 'CLM06', 'ADJ02', '2023-12-15', '2023-12-18',   9800.00,     NULL, 'filed',        NULL,         'Theft from vehicle',                         '2024-01-15T00:00:00'),
  ('C0028', 'POL002', 'CLM02', 'ADJ01', '2023-12-20', '2023-12-22',   8500.00,     NULL, 'filed',        NULL,         'Frozen pipe water damage',                   '2024-01-15T00:00:00'),
  ('C0029', 'POL011', 'CLM01', 'ADJ01', '2023-08-25', '2023-08-28',   5200.00,  4500.00, 'settled',      '2023-10-15', 'Window breakage storm',                      '2024-01-15T00:00:00'),
  ('C0030', 'POL013', 'CLM03', 'ADJ04', '2023-06-10', '2023-06-12',   4200.00,  3800.00, 'settled',      '2023-08-05', 'Prescription medication',                    '2024-01-15T00:00:00'),
  ('C0031', 'POL007', 'CLM07', 'ADJ01', '2023-07-20', '2023-07-22',   8900.00,  7500.00, 'settled',      '2023-09-30', 'Plumbing failure damage',                    '2024-01-15T00:00:00'),
  ('C0032', 'POL014', 'CLM04', 'ADJ02', '2023-09-28', '2023-10-01',   4500.00,  4500.00, 'settled',      '2023-11-10', 'Deer collision',                              '2024-01-15T00:00:00'),
  ('C0033', 'POL015', 'CLM05', 'ADJ01', '2023-06-15', '2023-06-18',  18500.00, 15000.00, 'settled',      '2023-09-20', 'Tree fell on house',                          '2024-01-15T00:00:00'),
  ('C0034', 'POL010', 'CLM10', 'ADJ02', '2023-11-05', '2023-11-08',  15200.00,     NULL, 'denied',       NULL,         'Totaled vehicle - coverage lapse',            '2024-01-15T00:00:00'),
  ('C0035', 'POL004', 'CLM04', 'ADJ04', '2023-03-25', '2023-03-28',   5200.00,  4800.00, 'settled',      '2023-05-15', 'Lab work and diagnostics',                    '2024-01-15T00:00:00'),
  ('C0036', 'POL009', 'CLM09', 'ADJ05', '2023-04-10', '2023-04-12',  12000.00, 10000.00, 'settled',      '2023-06-30', 'Hand injury on machinery',                    '2024-01-15T00:00:00'),
  ('C0037', 'POL005', 'CLM05', 'ADJ03', '2023-08-28', '2023-08-30', 175000.00,140000.00, 'settled',      '2023-12-10', 'OUTLIER: professional negligence mega-claim', '2024-01-15T00:00:00'),
  ('C0038', 'POL012', 'CLM02', 'ADJ03', '2023-05-05', '2023-05-08',  22000.00, 18000.00, 'settled',      '2023-08-20', 'Premises liability claim',                    '2024-01-15T00:00:00'),
  ('C0039', 'POL001', 'CLM01', 'ADJ02', '2024-01-05', '2024-01-07',   6200.00,     NULL, 'filed',        NULL,         'Rear-end at traffic light',                   '2024-01-15T00:00:00'),
  ('C0040', 'POL008', 'CLM08', 'ADJ04', '2023-04-15', '2023-04-17',   3200.00,  2800.00, 'settled',      '2023-06-01', 'Outpatient procedure',                        '2024-01-15T00:00:00'),
  ('C0041', 'POL003', 'CLM03', 'ADJ02', '2023-08-10', '2023-08-12',   2100.00,  2100.00, 'settled',      '2023-09-05', 'Glass damage from road debris',               '2024-01-15T00:00:00'),
  ('C0042', 'POL006', 'CLM06', 'ADJ02', '2023-09-18', '2023-09-20',   5400.00,  4800.00, 'settled',      '2023-11-08', 'Side mirror and door damage',                 '2024-01-15T00:00:00'),
  ('C0043', 'POL011', 'CLM11', 'ADJ06', '2023-12-01', '2023-12-04',  28000.00,     NULL, 'under_review', NULL,         'Major water leak damage',                     '2024-01-15T00:00:00'),
  ('C0044', 'POL007', 'CLM07', 'ADJ01', '2023-01-15', '2023-01-18',   4500.00,  4000.00, 'settled',      '2023-03-10', 'Appliance electrical fire',                   '2024-01-15T00:00:00'),
  ('C0045', 'POL014', 'CLM12', 'ADJ06', '2023-02-08', '2023-02-10',   3800.00,  3800.00, 'settled',      '2023-03-25', 'Hail damage to vehicle',                      '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 45
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_claims;
