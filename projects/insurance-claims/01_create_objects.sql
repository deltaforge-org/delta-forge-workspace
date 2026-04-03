-- =============================================================================
-- Insurance Claims Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw claims and policy data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'SCD2 policies and enriched claims';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Claims analytics star schema';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_policies (
    policy_id           STRING      NOT NULL,
    holder_name         STRING      NOT NULL,
    coverage_type       STRING      NOT NULL,
    annual_premium      DECIMAL(10,2),
    region              STRING,
    risk_score          DECIMAL(4,2),
    effective_date      DATE        NOT NULL,
    change_type         STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/claims/raw_policies';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_policies TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_claimants (
    claimant_id         STRING      NOT NULL,
    name                STRING      NOT NULL,
    age_band            STRING,
    state               STRING,
    risk_tier           STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/claims/raw_claimants';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_claimants TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_adjusters (
    adjuster_id         STRING      NOT NULL,
    name                STRING      NOT NULL,
    specialization      STRING,
    years_experience    INT,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/claims/raw_adjusters';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_adjusters TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_claims (
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
) LOCATION '{{data_path}}/bronze/claims/raw_claims';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_claims TO USER {{current_user}};

-- ===================== BRONZE SEED: ADJUSTERS (5 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_adjusters VALUES
    ('ADJ01', 'Margaret O''Brien', 'property', 12, '2024-01-15T00:00:00'),
    ('ADJ02', 'Richard Townsend', 'auto', 8, '2024-01-15T00:00:00'),
    ('ADJ03', 'Yuki Tanaka', 'liability', 15, '2024-01-15T00:00:00'),
    ('ADJ04', 'Carlos Mendez', 'health', 6, '2024-01-15T00:00:00'),
    ('ADJ05', 'Priya Sharma', 'workers_comp', 10, '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_adjusters;


-- ===================== BRONZE SEED: CLAIMANTS (10 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_claimants VALUES
    ('CLM01', 'Albert Donovan', '45-54', 'CA', 'standard', '2024-01-15T00:00:00'),
    ('CLM02', 'Barbara Chen', '35-44', 'NY', 'preferred', '2024-01-15T00:00:00'),
    ('CLM03', 'Chris Okafor', '25-34', 'TX', 'standard', '2024-01-15T00:00:00'),
    ('CLM04', 'Diana Petrov', '55-64', 'FL', 'high_risk', '2024-01-15T00:00:00'),
    ('CLM05', 'Eduardo Silva', '35-44', 'IL', 'preferred', '2024-01-15T00:00:00'),
    ('CLM06', 'Fiona McAllister', '25-34', 'WA', 'standard', '2024-01-15T00:00:00'),
    ('CLM07', 'George Nakamura', '45-54', 'CA', 'high_risk', '2024-01-15T00:00:00'),
    ('CLM08', 'Hannah Wright', '65+', 'AZ', 'standard', '2024-01-15T00:00:00'),
    ('CLM09', 'Ivan Popov', '35-44', 'NJ', 'preferred', '2024-01-15T00:00:00'),
    ('CLM10', 'Julia Fernandez', '25-34', 'CO', 'standard', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_claimants;


-- ===================== BRONZE SEED: POLICIES (22 rows - includes SCD2 changes) =====================
-- Some policies have multiple versions (premium adjustments, coverage upgrades)

INSERT INTO {{zone_prefix}}.bronze.raw_policies VALUES
    ('POL001', 'Albert Donovan', 'auto', 1200.00, 'West', 3.2, '2022-01-01', 'new', '2024-01-15T00:00:00'),
    ('POL001', 'Albert Donovan', 'auto', 1380.00, 'West', 3.8, '2023-01-01', 'premium_increase', '2024-01-15T00:00:00'),
    ('POL001', 'Albert Donovan', 'auto_plus', 1650.00, 'West', 3.5, '2024-01-01', 'coverage_upgrade', '2024-01-15T00:00:00'),
    ('POL002', 'Barbara Chen', 'home', 2400.00, 'Northeast', 2.1, '2021-06-15', 'new', '2024-01-15T00:00:00'),
    ('POL002', 'Barbara Chen', 'home', 2640.00, 'Northeast', 2.4, '2023-06-15', 'premium_increase', '2024-01-15T00:00:00'),
    ('POL003', 'Chris Okafor', 'auto', 980.00, 'South', 2.8, '2022-03-01', 'new', '2024-01-15T00:00:00'),
    ('POL004', 'Diana Petrov', 'health', 4800.00, 'Southeast', 4.5, '2020-01-01', 'new', '2024-01-15T00:00:00'),
    ('POL004', 'Diana Petrov', 'health', 5520.00, 'Southeast', 4.8, '2022-01-01', 'premium_increase', '2024-01-15T00:00:00'),
    ('POL004', 'Diana Petrov', 'health_plus', 6200.00, 'Southeast', 4.2, '2023-07-01', 'coverage_upgrade', '2024-01-15T00:00:00'),
    ('POL005', 'Eduardo Silva', 'liability', 3200.00, 'Midwest', 2.0, '2021-09-01', 'new', '2024-01-15T00:00:00'),
    ('POL006', 'Fiona McAllister', 'auto', 1100.00, 'West', 2.5, '2023-01-15', 'new', '2024-01-15T00:00:00'),
    ('POL007', 'George Nakamura', 'home', 3600.00, 'West', 4.1, '2020-04-01', 'new', '2024-01-15T00:00:00'),
    ('POL007', 'George Nakamura', 'home', 4200.00, 'West', 4.5, '2023-04-01', 'premium_increase', '2024-01-15T00:00:00'),
    ('POL008', 'Hannah Wright', 'health', 5200.00, 'Southwest', 3.6, '2019-01-01', 'new', '2024-01-15T00:00:00'),
    ('POL008', 'Hannah Wright', 'health', 5800.00, 'Southwest', 3.9, '2022-01-01', 'premium_increase', '2024-01-15T00:00:00'),
    ('POL009', 'Ivan Popov', 'workers_comp', 2800.00, 'Northeast', 2.3, '2022-07-01', 'new', '2024-01-15T00:00:00'),
    ('POL010', 'Julia Fernandez', 'auto', 890.00, 'West', 1.9, '2023-05-01', 'new', '2024-01-15T00:00:00'),
    ('POL011', 'Albert Donovan', 'home', 2200.00, 'West', 3.0, '2022-06-01', 'new', '2024-01-15T00:00:00'),
    ('POL012', 'Barbara Chen', 'liability', 1800.00, 'Northeast', 2.2, '2023-01-01', 'new', '2024-01-15T00:00:00'),
    ('POL013', 'Chris Okafor', 'health', 3400.00, 'South', 2.9, '2022-08-01', 'new', '2024-01-15T00:00:00'),
    ('POL014', 'Diana Petrov', 'auto', 1500.00, 'Southeast', 4.0, '2021-03-01', 'new', '2024-01-15T00:00:00'),
    ('POL015', 'Eduardo Silva', 'home', 2600.00, 'Midwest', 2.1, '2022-11-01', 'new', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 22
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_policies;


-- ===================== BRONZE SEED: CLAIMS (52 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_claims VALUES
    ('C0001', 'POL001', 'CLM01', 'ADJ02', '2023-03-15', '2023-03-16', 8500.00, 7200.00, 'settled', '2023-05-10', 'Rear-end collision on highway', '2024-01-15T00:00:00'),
    ('C0002', 'POL001', 'CLM01', 'ADJ02', '2023-08-20', '2023-08-22', 3200.00, 3200.00, 'settled', '2023-09-15', 'Parking lot fender bender', '2024-01-15T00:00:00'),
    ('C0003', 'POL002', 'CLM02', 'ADJ01', '2023-02-10', '2023-02-12', 45000.00, 38000.00, 'settled', '2023-06-20', 'Water damage from burst pipe', '2024-01-15T00:00:00'),
    ('C0004', 'POL002', 'CLM02', 'ADJ01', '2023-09-05', '2023-09-06', 12000.00, 10500.00, 'settled', '2023-11-30', 'Wind damage to roof', '2024-01-15T00:00:00'),
    ('C0005', 'POL003', 'CLM03', 'ADJ02', '2023-04-22', '2023-04-25', 5600.00, 4800.00, 'settled', '2023-06-15', 'Side-impact collision', '2024-01-15T00:00:00'),
    ('C0006', 'POL004', 'CLM04', 'ADJ04', '2023-01-08', '2023-01-10', 28000.00, 22000.00, 'settled', '2023-04-20', 'Hip replacement surgery', '2024-01-15T00:00:00'),
    ('C0007', 'POL004', 'CLM04', 'ADJ04', '2023-06-15', '2023-06-18', 15000.00, 12500.00, 'settled', '2023-09-10', 'Physical therapy post-surgery', '2024-01-15T00:00:00'),
    ('C0008', 'POL004', 'CLM04', 'ADJ04', '2023-11-01', '2023-11-03', 35000.00, NULL, 'under_review', NULL, 'Cardiac procedure', '2024-01-15T00:00:00'),
    ('C0009', 'POL005', 'CLM05', 'ADJ03', '2023-05-10', '2023-05-12', 75000.00, 60000.00, 'settled', '2023-10-25', 'Product liability claim', '2024-01-15T00:00:00'),
    ('C0010', 'POL005', 'CLM05', 'ADJ03', '2023-12-01', '2023-12-05', 120000.00, NULL, 'open', NULL, 'Workplace injury liability', '2024-01-15T00:00:00'),
    ('C0011', 'POL006', 'CLM06', 'ADJ02', '2023-06-30', '2023-07-02', 4200.00, 4200.00, 'settled', '2023-08-10', 'Windshield replacement', '2024-01-15T00:00:00'),
    ('C0012', 'POL007', 'CLM07', 'ADJ01', '2023-03-01', '2023-03-03', 22000.00, 18000.00, 'settled', '2023-07-15', 'Fire damage to kitchen', '2024-01-15T00:00:00'),
    ('C0013', 'POL007', 'CLM07', 'ADJ01', '2023-10-15', '2023-10-18', 55000.00, 45000.00, 'settled', '2024-01-10', 'Foundation structural damage', '2024-01-15T00:00:00'),
    ('C0014', 'POL008', 'CLM08', 'ADJ04', '2023-02-20', '2023-02-22', 8500.00, 7000.00, 'settled', '2023-04-10', 'Emergency room visit', '2024-01-15T00:00:00'),
    ('C0015', 'POL008', 'CLM08', 'ADJ04', '2023-07-10', '2023-07-12', 42000.00, 35000.00, 'settled', '2023-11-20', 'Knee replacement surgery', '2024-01-15T00:00:00'),
    ('C0016', 'POL008', 'CLM08', 'ADJ04', '2023-12-05', '2023-12-08', 6500.00, NULL, 'under_review', NULL, 'Post-operative complication', '2024-01-15T00:00:00'),
    ('C0017', 'POL009', 'CLM09', 'ADJ05', '2023-08-14', '2023-08-16', 18000.00, 15000.00, 'settled', '2023-11-01', 'Back injury on job site', '2024-01-15T00:00:00'),
    ('C0018', 'POL009', 'CLM09', 'ADJ05', '2023-12-20', '2023-12-22', 25000.00, NULL, 'open', NULL, 'Repetitive strain injury', '2024-01-15T00:00:00'),
    ('C0019', 'POL010', 'CLM10', 'ADJ02', '2023-07-25', '2023-07-27', 2800.00, 2800.00, 'settled', '2023-08-20', 'Minor fender bender', '2024-01-15T00:00:00'),
    ('C0020', 'POL011', 'CLM01', 'ADJ01', '2023-04-15', '2023-04-18', 15000.00, 12000.00, 'settled', '2023-08-01', 'Hail damage to roof', '2024-01-15T00:00:00'),
    ('C0021', 'POL012', 'CLM02', 'ADJ03', '2023-09-20', '2023-09-22', 50000.00, 40000.00, 'settled', '2024-01-05', 'Slip and fall liability', '2024-01-15T00:00:00'),
    ('C0022', 'POL013', 'CLM03', 'ADJ04', '2023-10-05', '2023-10-08', 9500.00, 8000.00, 'settled', '2023-12-15', 'Specialist consultation series', '2024-01-15T00:00:00'),
    ('C0023', 'POL014', 'CLM04', 'ADJ02', '2023-05-18', '2023-05-20', 6800.00, 5500.00, 'settled', '2023-07-25', 'Intersection collision', '2024-01-15T00:00:00'),
    ('C0024', 'POL015', 'CLM05', 'ADJ01', '2023-11-10', '2023-11-13', 32000.00, 28000.00, 'settled', '2024-01-08', 'Storm damage property', '2024-01-15T00:00:00'),
    ('C0025', 'POL001', 'CLM01', 'ADJ02', '2023-12-10', '2023-12-12', 11000.00, NULL, 'under_review', NULL, 'Multi-vehicle accident', '2024-01-15T00:00:00'),
    ('C0026', 'POL003', 'CLM03', 'ADJ02', '2023-11-22', '2023-11-25', 7200.00, NULL, 'open', NULL, 'Hit and run damage', '2024-01-15T00:00:00'),
    ('C0027', 'POL006', 'CLM06', 'ADJ02', '2023-12-15', '2023-12-18', 9800.00, NULL, 'open', NULL, 'Theft from vehicle', '2024-01-15T00:00:00'),
    ('C0028', 'POL002', 'CLM02', 'ADJ01', '2023-12-20', '2023-12-22', 8500.00, NULL, 'open', NULL, 'Frozen pipe water damage', '2024-01-15T00:00:00'),
    ('C0029', 'POL011', 'CLM01', 'ADJ01', '2023-08-25', '2023-08-28', 5200.00, 4500.00, 'settled', '2023-10-15', 'Window breakage storm', '2024-01-15T00:00:00'),
    ('C0030', 'POL013', 'CLM03', 'ADJ04', '2023-06-10', '2023-06-12', 4200.00, 3800.00, 'settled', '2023-08-05', 'Prescription medication', '2024-01-15T00:00:00'),
    ('C0031', 'POL007', 'CLM07', 'ADJ01', '2023-07-20', '2023-07-22', 8900.00, 7500.00, 'settled', '2023-09-30', 'Plumbing failure damage', '2024-01-15T00:00:00'),
    ('C0032', 'POL014', 'CLM04', 'ADJ02', '2023-09-28', '2023-10-01', 4500.00, 4500.00, 'settled', '2023-11-10', 'Deer collision', '2024-01-15T00:00:00'),
    ('C0033', 'POL015', 'CLM05', 'ADJ01', '2023-06-15', '2023-06-18', 18500.00, 15000.00, 'settled', '2023-09-20', 'Tree fell on house', '2024-01-15T00:00:00'),
    ('C0034', 'POL010', 'CLM10', 'ADJ02', '2023-11-05', '2023-11-08', 15200.00, NULL, 'denied', NULL, 'Totaled vehicle - coverage lapse', '2024-01-15T00:00:00'),
    ('C0035', 'POL004', 'CLM04', 'ADJ04', '2023-03-25', '2023-03-28', 5200.00, 4800.00, 'settled', '2023-05-15', 'Lab work and diagnostics', '2024-01-15T00:00:00'),
    ('C0036', 'POL009', 'CLM09', 'ADJ05', '2023-04-10', '2023-04-12', 12000.00, 10000.00, 'settled', '2023-06-30', 'Hand injury on machinery', '2024-01-15T00:00:00'),
    ('C0037', 'POL005', 'CLM05', 'ADJ03', '2023-08-28', '2023-08-30', 45000.00, 38000.00, 'settled', '2023-12-10', 'Professional negligence claim', '2024-01-15T00:00:00'),
    ('C0038', 'POL012', 'CLM02', 'ADJ03', '2023-05-05', '2023-05-08', 22000.00, 18000.00, 'settled', '2023-08-20', 'Premises liability claim', '2024-01-15T00:00:00'),
    ('C0039', 'POL001', 'CLM01', 'ADJ02', '2024-01-05', '2024-01-07', 6200.00, NULL, 'open', NULL, 'Rear-end at traffic light', '2024-01-15T00:00:00'),
    ('C0040', 'POL008', 'CLM08', 'ADJ04', '2023-04-15', '2023-04-17', 3200.00, 2800.00, 'settled', '2023-06-01', 'Outpatient procedure', '2024-01-15T00:00:00'),
    ('C0041', 'POL003', 'CLM03', 'ADJ02', '2023-08-10', '2023-08-12', 2100.00, 2100.00, 'settled', '2023-09-05', 'Glass damage from road debris', '2024-01-15T00:00:00'),
    ('C0042', 'POL006', 'CLM06', 'ADJ02', '2023-09-18', '2023-09-20', 5400.00, 4800.00, 'settled', '2023-11-08', 'Side mirror and door damage', '2024-01-15T00:00:00'),
    ('C0043', 'POL011', 'CLM01', 'ADJ01', '2023-12-01', '2023-12-04', 28000.00, NULL, 'under_review', NULL, 'Major water leak damage', '2024-01-15T00:00:00'),
    ('C0044', 'POL007', 'CLM07', 'ADJ01', '2023-01-15', '2023-01-18', 4500.00, 4000.00, 'settled', '2023-03-10', 'Appliance electrical fire', '2024-01-15T00:00:00'),
    ('C0045', 'POL014', 'CLM04', 'ADJ02', '2023-02-08', '2023-02-10', 3800.00, 3800.00, 'settled', '2023-03-25', 'Hail damage to vehicle', '2024-01-15T00:00:00'),
    ('C0046', 'POL013', 'CLM03', 'ADJ04', '2024-01-02', '2024-01-05', 7800.00, NULL, 'open', NULL, 'Annual physical + followup tests', '2024-01-15T00:00:00'),
    ('C0047', 'POL015', 'CLM05', 'ADJ01', '2024-01-08', '2024-01-10', 9200.00, NULL, 'open', NULL, 'Ice dam roof damage', '2024-01-15T00:00:00'),
    ('C0048', 'POL005', 'CLM05', 'ADJ03', '2023-02-14', '2023-02-16', 35000.00, 28000.00, 'settled', '2023-06-30', 'Construction defect liability', '2024-01-15T00:00:00'),
    ('C0049', 'POL002', 'CLM02', 'ADJ01', '2023-06-10', '2023-06-12', 6800.00, 5500.00, 'settled', '2023-08-15', 'Fence damage from neighbor tree', '2024-01-15T00:00:00'),
    ('C0050', 'POL009', 'CLM09', 'ADJ05', '2024-01-10', '2024-01-12', 14000.00, NULL, 'open', NULL, 'Shoulder injury from fall', '2024-01-15T00:00:00'),
    ('C0051', 'POL004', 'CLM04', 'ADJ04', '2023-09-15', '2023-09-18', 19500.00, 16000.00, 'settled', '2023-12-20', 'Cataract surgery bilateral', '2024-01-15T00:00:00'),
    ('C0052', 'POL010', 'CLM10', 'ADJ02', '2024-01-12', '2024-01-14', 3500.00, NULL, 'open', NULL, 'Vandalism to parked car', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 52
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_claims;


-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.dim_policy_scd2 (
    surrogate_key       INT         NOT NULL,
    policy_id           STRING      NOT NULL,
    holder_name         STRING      NOT NULL,
    coverage_type       STRING      NOT NULL,
    annual_premium      DECIMAL(10,2) CHECK (annual_premium > 0),
    region              STRING,
    risk_score          DECIMAL(4,2),
    valid_from          DATE        NOT NULL,
    valid_to            DATE,
    is_current          INT         NOT NULL CHECK (is_current IN (0, 1)),
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/claims/dim_policy_scd2';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.dim_policy_scd2 TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.claims_enriched (
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
    region              STRING,
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/claims/claims_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.claims_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_policy (
    surrogate_key       INT         NOT NULL,
    policy_id           STRING      NOT NULL,
    holder_name         STRING,
    coverage_type       STRING      NOT NULL,
    annual_premium      DECIMAL(10,2),
    region              STRING,
    risk_score          DECIMAL(4,2),
    valid_from          DATE        NOT NULL,
    valid_to            DATE,
    is_current          INT         NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/claims/dim_policy';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_policy TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_claimant (
    claimant_key        INT         NOT NULL,
    claimant_id         STRING      NOT NULL,
    name                STRING,
    age_band            STRING,
    state               STRING,
    risk_tier           STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/claims/dim_claimant';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_claimant TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_adjuster (
    adjuster_key        INT         NOT NULL,
    adjuster_id         STRING      NOT NULL,
    name                STRING      NOT NULL,
    specialization      STRING,
    years_experience    INT,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/claims/dim_adjuster';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_adjuster TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_claims (
    claim_key           INT         NOT NULL,
    policy_key          INT         NOT NULL,
    claimant_key        INT         NOT NULL,
    adjuster_key        INT         NOT NULL,
    incident_date       DATE        NOT NULL,
    reported_date       DATE        NOT NULL,
    claim_amount        DECIMAL(12,2) NOT NULL,
    approved_amount     DECIMAL(12,2),
    status              STRING      NOT NULL,
    days_to_settle      INT,
    days_to_report      INT,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/claims/fact_claims';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_claims TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_loss_ratios (
    coverage_type       STRING      NOT NULL,
    region              STRING      NOT NULL,
    claim_count         INT         NOT NULL,
    total_claimed       DECIMAL(14,2),
    total_approved      DECIMAL(14,2),
    loss_ratio          DECIMAL(6,4),
    avg_days_to_settle  DECIMAL(6,2),
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/claims/kpi_loss_ratios';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_loss_ratios TO USER {{current_user}};

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_claimant (name) TRANSFORM mask PARAMS (show = 1);

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.claims_enriched (claimant_id) TRANSFORM mask PARAMS (show = 3);
