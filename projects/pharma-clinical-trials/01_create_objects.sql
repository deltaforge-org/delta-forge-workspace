-- =============================================================================
-- Pharma Clinical Trials Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: All 4 pseudonymisation transforms, deletion vectors, RESTORE
-- Bronze data: 3 trials, 4 sites, 20 participants, 5 visit types, 60+ observations

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw clinical trial source data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Validated and pseudonymised trial data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Trial efficacy star schema and KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_participants (
    participant_id      STRING      NOT NULL,
    participant_name    STRING,
    ssn                 STRING,
    email               STRING,
    date_of_birth       STRING,
    gender              STRING,
    ethnicity           STRING,
    trial_id            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    enrollment_date     STRING,
    consent_status      STRING,
    withdrawal_date     STRING,
    withdrawal_reason   STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/trials/raw_participants';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_participants TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_trials (
    trial_id            STRING      NOT NULL,
    trial_name          STRING      NOT NULL,
    phase               STRING,
    therapeutic_area    STRING,
    sponsor             STRING,
    start_date          STRING,
    end_date            STRING,
    target_enrollment   INT,
    protocol_version    STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/trials/raw_trials';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_trials TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sites (
    site_id                 STRING      NOT NULL,
    site_name               STRING      NOT NULL,
    city                    STRING,
    country                 STRING,
    principal_investigator  STRING,
    irb_approval_date       STRING,
    max_enrollment          INT,
    ingested_at             TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/trials/raw_sites';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sites TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_visits (
    visit_id            STRING      NOT NULL,
    visit_number        INT         NOT NULL,
    visit_type          STRING,
    scheduled_day       INT,
    window_days         INT,
    trial_id            STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/trials/raw_visits';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_visits TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_observations (
    observation_id      STRING      NOT NULL,
    participant_id      STRING      NOT NULL,
    trial_id            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    visit_number        INT,
    observation_date    STRING,
    biomarker_value     DECIMAL(10,4),
    adverse_event_flag  BOOLEAN,
    adverse_event_desc  STRING,
    severity            STRING,
    dosage_mg           DECIMAL(8,2),
    response_category   STRING,
    protocol_deviation  BOOLEAN,
    deviation_desc      STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/trials/raw_observations';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_observations TO USER {{current_user}};

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.participant_clean (
    participant_id      STRING      NOT NULL,
    participant_name    STRING,
    ssn                 STRING,
    email               STRING,
    date_of_birth       STRING,
    age_band            STRING,
    gender              STRING,
    ethnicity           STRING,
    trial_id            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    enrollment_date     DATE,
    consent_status      STRING,
    withdrawal_date     DATE,
    withdrawal_reason   STRING,
    is_active           BOOLEAN
) LOCATION '{{data_path}}/silver/trials/participant_clean'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.participant_clean TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.observation_enriched (
    observation_id      STRING      NOT NULL,
    participant_id      STRING      NOT NULL,
    trial_id            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    visit_number        INT,
    observation_date    DATE,
    biomarker_value     DECIMAL(10,4),
    baseline_value      DECIMAL(10,4),
    biomarker_change    DECIMAL(10,4),
    adverse_event_flag  BOOLEAN,
    severity            STRING,
    dosage_mg           DECIMAL(8,2),
    response_category   STRING,
    protocol_deviation  BOOLEAN
) LOCATION '{{data_path}}/silver/trials/observation_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.observation_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_observations (
    observation_key     INT         NOT NULL,
    participant_key     INT         NOT NULL,
    trial_key           INT         NOT NULL,
    site_key            INT         NOT NULL,
    visit_key           INT         NOT NULL,
    observation_date    DATE,
    biomarker_value     DECIMAL(10,4),
    adverse_event_flag  BOOLEAN,
    severity            STRING,
    dosage_mg           DECIMAL(8,2),
    response_category   STRING
) LOCATION '{{data_path}}/gold/trials/fact_observations';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_observations TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_participant (
    participant_key     INT         NOT NULL,
    participant_id      STRING      NOT NULL,
    age_band            STRING,
    gender              STRING,
    ethnicity           STRING,
    enrollment_date     DATE,
    consent_status      STRING,
    withdrawal_date     DATE
) LOCATION '{{data_path}}/gold/trials/dim_participant';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_participant TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_trial (
    trial_key           INT         NOT NULL,
    trial_id            STRING      NOT NULL,
    trial_name          STRING,
    phase               STRING,
    therapeutic_area    STRING,
    sponsor             STRING,
    start_date          DATE,
    target_enrollment   INT
) LOCATION '{{data_path}}/gold/trials/dim_trial';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_trial TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_site (
    site_key            INT         NOT NULL,
    site_id             STRING      NOT NULL,
    site_name           STRING,
    city                STRING,
    country             STRING,
    principal_investigator STRING,
    irb_approval_date   DATE
) LOCATION '{{data_path}}/gold/trials/dim_site';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_site TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_visit (
    visit_key           INT         NOT NULL,
    visit_number        INT,
    visit_type          STRING,
    scheduled_day       INT,
    window_days         INT
) LOCATION '{{data_path}}/gold/trials/dim_visit';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_visit TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_trial_efficacy (
    trial_id            STRING      NOT NULL,
    phase               STRING,
    response_rate_pct   DECIMAL(5,2),
    adverse_event_rate  DECIMAL(5,2),
    mean_biomarker_change DECIMAL(10,4),
    enrollment_pct      DECIMAL(5,2),
    screen_fail_rate    DECIMAL(5,2),
    dropout_rate        DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/trials/kpi_trial_efficacy';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_trial_efficacy TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: TRIALS (3 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_trials VALUES
    ('TRL-001', 'CARDIO-PREVENT Phase III',   'Phase III', 'Cardiovascular', 'NovaPharm Inc',     '2023-01-15', '2025-01-15', 120, 'v2.1', '2025-01-01T00:00:00'),
    ('TRL-002', 'ONCO-TARGET Phase II',       'Phase II',  'Oncology',       'BioGenesis Labs',   '2023-03-01', '2025-06-30', 80,  'v1.3', '2025-01-01T00:00:00'),
    ('TRL-003', 'NEURO-REPAIR Phase I',       'Phase I',   'Neurology',      'CerebraTherapy AG', '2023-06-01', '2024-12-31', 40,  'v1.0', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_trials;


-- ===================== BRONZE SEED DATA: SITES (4 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_sites VALUES
    ('SITE-BOS', 'Boston Medical Center',       'Boston',    'USA',    'Dr. Elena Vasquez',  '2022-11-01', 40, '2025-01-01T00:00:00'),
    ('SITE-LON', 'London Royal Hospital',       'London',    'UK',     'Dr. James Whitfield','2022-12-15', 35, '2025-01-01T00:00:00'),
    ('SITE-MUN', 'Munich University Hospital',  'Munich',    'Germany','Dr. Hans Gruber',    '2023-01-10', 30, '2025-01-01T00:00:00'),
    ('SITE-TOK', 'Tokyo Central Clinic',        'Tokyo',     'Japan',  'Dr. Yuki Tanaka',    '2023-02-01', 25, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sites;


-- ===================== BRONZE SEED DATA: VISITS (5 rows per trial = 15 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_visits VALUES
    ('V-001-SCR', 1, 'Screening',    -14, 7, 'TRL-001', '2025-01-01T00:00:00'),
    ('V-001-BAS', 2, 'Baseline',       0, 3, 'TRL-001', '2025-01-01T00:00:00'),
    ('V-001-W04', 3, 'Week 4',        28, 5, 'TRL-001', '2025-01-01T00:00:00'),
    ('V-001-W12', 4, 'Week 12',       84, 7, 'TRL-001', '2025-01-01T00:00:00'),
    ('V-001-EOT', 5, 'End of Trial', 180, 7, 'TRL-001', '2025-01-01T00:00:00'),
    ('V-002-SCR', 1, 'Screening',    -14, 7, 'TRL-002', '2025-01-01T00:00:00'),
    ('V-002-BAS', 2, 'Baseline',       0, 3, 'TRL-002', '2025-01-01T00:00:00'),
    ('V-002-W04', 3, 'Week 4',        28, 5, 'TRL-002', '2025-01-01T00:00:00'),
    ('V-002-W12', 4, 'Week 12',       84, 7, 'TRL-002', '2025-01-01T00:00:00'),
    ('V-002-EOT', 5, 'End of Trial', 180, 7, 'TRL-002', '2025-01-01T00:00:00'),
    ('V-003-SCR', 1, 'Screening',    -14, 7, 'TRL-003', '2025-01-01T00:00:00'),
    ('V-003-BAS', 2, 'Baseline',       0, 3, 'TRL-003', '2025-01-01T00:00:00'),
    ('V-003-W04', 3, 'Week 4',        28, 5, 'TRL-003', '2025-01-01T00:00:00'),
    ('V-003-W12', 4, 'Week 12',       84, 7, 'TRL-003', '2025-01-01T00:00:00'),
    ('V-003-EOT', 5, 'End of Trial', 180, 7, 'TRL-003', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_visits;


-- ===================== BRONZE SEED DATA: PARTICIPANTS (20 rows) =====================
-- Includes screen failures, withdrawals, active, completed across 3 trials and 4 sites

INSERT INTO {{zone_prefix}}.bronze.raw_participants VALUES
    ('PT-1001', 'Alice Morgan',     '111-22-3333', 'alice.morgan@email.com',    '1965-03-14', 'Female', 'Caucasian',        'TRL-001', 'SITE-BOS', '2023-02-01', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-1002', 'Brian Chen',       '222-33-4444', 'brian.chen@email.com',      '1978-07-22', 'Male',   'Asian',            'TRL-001', 'SITE-BOS', '2023-02-10', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-1003', 'Carmen Diaz',      '333-44-5555', 'carmen.diaz@email.com',     '1952-11-08', 'Female', 'Hispanic',         'TRL-001', 'SITE-LON', '2023-02-15', 'Withdrawn', '2023-06-10', 'Adverse event',        '2025-01-01T00:00:00'),
    ('PT-1004', 'David Okafor',     '444-55-6666', 'david.okafor@email.com',    '1989-01-30', 'Male',   'Black',            'TRL-001', 'SITE-LON', '2023-02-20', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-1005', 'Eva Lindqvist',    '555-66-7777', 'eva.lindqvist@email.com',   '1971-09-05', 'Female', 'Caucasian',        'TRL-001', 'SITE-MUN', '2023-03-01', 'Completed', NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-1006', 'Frank Rossi',      '666-77-8888', 'frank.rossi@email.com',     '1984-12-18', 'Male',   'Caucasian',        'TRL-001', 'SITE-MUN', '2023-03-05', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-1007', 'Grace Yamamoto',   '777-88-9999', 'grace.yamamoto@email.com',  '1960-04-25', 'Female', 'Asian',            'TRL-001', 'SITE-TOK', '2023-03-10', 'Screen Fail', NULL,       'Exclusion criteria',   '2025-01-01T00:00:00'),
    ('PT-2001', 'Henry Petrov',     '888-99-0000', 'henry.petrov@email.com',    '1975-06-12', 'Male',   'Caucasian',        'TRL-002', 'SITE-BOS', '2023-04-01', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-2002', 'Irene Nakamura',   '999-00-1111', 'irene.nakamura@email.com',  '1968-02-28', 'Female', 'Asian',            'TRL-002', 'SITE-BOS', '2023-04-05', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-2003', 'James Mbeki',      '100-11-2222', 'james.mbeki@email.com',     '1982-08-17', 'Male',   'Black',            'TRL-002', 'SITE-LON', '2023-04-10', 'Completed', NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-2004', 'Karen Olsen',      '200-22-3333', 'karen.olsen@email.com',     '1957-10-03', 'Female', 'Caucasian',        'TRL-002', 'SITE-LON', '2023-04-15', 'Withdrawn', '2023-08-20', 'Personal reasons',     '2025-01-01T00:00:00'),
    ('PT-2005', 'Luis Fernandez',   '300-33-4444', 'luis.fernandez@email.com',  '1991-05-20', 'Male',   'Hispanic',         'TRL-002', 'SITE-MUN', '2023-04-20', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-2006', 'Mei Wang',         '400-44-5555', 'mei.wang@email.com',        '1973-03-09', 'Female', 'Asian',            'TRL-002', 'SITE-TOK', '2023-05-01', 'Screen Fail', NULL,       'Lab values out of range','2025-01-01T00:00:00'),
    ('PT-2007', 'Nikolai Volkov',   '500-55-6666', 'nikolai.volkov@email.com',  '1986-11-14', 'Male',   'Caucasian',        'TRL-002', 'SITE-TOK', '2023-05-05', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-3001', 'Olivia Schmidt',   '600-66-7777', 'olivia.schmidt@email.com',  '1979-07-07', 'Female', 'Caucasian',        'TRL-003', 'SITE-MUN', '2023-07-01', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-3002', 'Patrick OBrien',   '700-77-8888', 'patrick.obrien@email.com',  '1963-01-19', 'Male',   'Caucasian',        'TRL-003', 'SITE-LON', '2023-07-05', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-3003', 'Rina Patel',       '800-88-9999', 'rina.patel@email.com',      '1995-09-23', 'Female', 'Asian',            'TRL-003', 'SITE-BOS', '2023-07-10', 'Withdrawn', '2023-09-15', 'Lost to follow-up',    '2025-01-01T00:00:00'),
    ('PT-3004', 'Samuel Kone',      '900-99-0000', 'samuel.kone@email.com',     '1988-04-11', 'Male',   'Black',            'TRL-003', 'SITE-BOS', '2023-07-15', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00'),
    ('PT-3005', 'Tanya Johansson',  '010-12-3456', 'tanya.johansson@email.com', '1970-12-02', 'Female', 'Caucasian',        'TRL-003', 'SITE-TOK', '2023-07-20', 'Screen Fail', NULL,       'Concomitant medication','2025-01-01T00:00:00'),
    ('PT-3006', 'Umar Hassan',      '020-23-4567', 'umar.hassan@email.com',     '1981-06-29', 'Male',   'Middle Eastern',   'TRL-003', 'SITE-MUN', '2023-07-25', 'Active',    NULL,         NULL,                   '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_participants;


-- ===================== BRONZE SEED DATA: OBSERVATIONS (65 rows) =====================
-- Biomarker values, adverse events, protocol deviations across visits

INSERT INTO {{zone_prefix}}.bronze.raw_observations VALUES
    -- TRL-001 participants: Cardiovascular biomarker (LDL cholesterol mg/dL range)
    ('OBS-0001', 'PT-1001', 'TRL-001', 'SITE-BOS', 1, '2023-01-20', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0002', 'PT-1001', 'TRL-001', 'SITE-BOS', 2, '2023-02-01', 185.4200, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0003', 'PT-1001', 'TRL-001', 'SITE-BOS', 3, '2023-03-01', 162.3100, false, NULL, NULL,       100.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0004', 'PT-1001', 'TRL-001', 'SITE-BOS', 4, '2023-05-01', 138.7500, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0005', 'PT-1001', 'TRL-001', 'SITE-BOS', 5, '2023-08-01', 121.2000, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0006', 'PT-1002', 'TRL-001', 'SITE-BOS', 1, '2023-01-28', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0007', 'PT-1002', 'TRL-001', 'SITE-BOS', 2, '2023-02-10', 198.6000, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0008', 'PT-1002', 'TRL-001', 'SITE-BOS', 3, '2023-03-10', 189.2300, true,  'Mild headache', 'Mild', 100.00, 'Non-responder', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0009', 'PT-1002', 'TRL-001', 'SITE-BOS', 4, '2023-05-10', 176.8800, false, NULL, NULL,       150.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0010', 'PT-1003', 'TRL-001', 'SITE-LON', 1, '2023-02-05', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0011', 'PT-1003', 'TRL-001', 'SITE-LON', 2, '2023-02-15', 172.1500, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0012', 'PT-1003', 'TRL-001', 'SITE-LON', 3, '2023-03-15', 168.9200, true,  'Severe rash, treatment discontinued', 'Severe', 100.00, 'Non-responder', true, 'AE led to withdrawal', '2025-01-01T00:00:00'),
    ('OBS-0013', 'PT-1004', 'TRL-001', 'SITE-LON', 1, '2023-02-10', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0014', 'PT-1004', 'TRL-001', 'SITE-LON', 2, '2023-02-20', 210.3400, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0015', 'PT-1004', 'TRL-001', 'SITE-LON', 3, '2023-03-20', 187.6100, false, NULL, NULL,       100.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0016', 'PT-1004', 'TRL-001', 'SITE-LON', 4, '2023-05-20', 155.2200, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0017', 'PT-1004', 'TRL-001', 'SITE-LON', 5, '2023-08-20', 132.8900, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0018', 'PT-1005', 'TRL-001', 'SITE-MUN', 1, '2023-02-18', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0019', 'PT-1005', 'TRL-001', 'SITE-MUN', 2, '2023-03-01', 192.7700, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0020', 'PT-1005', 'TRL-001', 'SITE-MUN', 3, '2023-03-29', 164.3300, false, NULL, NULL,       100.00, 'Partial',    true,  'Dose taken 2 hours late', '2025-01-01T00:00:00'),
    ('OBS-0021', 'PT-1005', 'TRL-001', 'SITE-MUN', 4, '2023-05-29', 131.4500, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0022', 'PT-1005', 'TRL-001', 'SITE-MUN', 5, '2023-08-29', 118.9000, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0023', 'PT-1006', 'TRL-001', 'SITE-MUN', 1, '2023-02-22', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0024', 'PT-1006', 'TRL-001', 'SITE-MUN', 2, '2023-03-05', 205.1200, false, NULL, NULL,       100.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0025', 'PT-1006', 'TRL-001', 'SITE-MUN', 3, '2023-04-02', 178.5600, true,  'Nausea', 'Mild', 100.00, 'Partial',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0026', 'PT-1006', 'TRL-001', 'SITE-MUN', 4, '2023-06-02', 149.3400, false, NULL, NULL,       150.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0027', 'PT-1007', 'TRL-001', 'SITE-TOK', 1, '2023-02-28', NULL,     false, NULL, NULL,       NULL,   'Screen Fail',false, NULL, '2025-01-01T00:00:00'),
    -- TRL-002 participants: Oncology biomarker (tumor marker CA-125 U/mL)
    ('OBS-0028', 'PT-2001', 'TRL-002', 'SITE-BOS', 1, '2023-03-20', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0029', 'PT-2001', 'TRL-002', 'SITE-BOS', 2, '2023-04-01', 245.8000, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0030', 'PT-2001', 'TRL-002', 'SITE-BOS', 3, '2023-04-29', 198.3200, true,  'Fatigue grade 2', 'Moderate', 200.00, 'Partial', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0031', 'PT-2001', 'TRL-002', 'SITE-BOS', 4, '2023-06-29', 156.7100, false, NULL, NULL,       250.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0032', 'PT-2002', 'TRL-002', 'SITE-BOS', 1, '2023-03-25', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0033', 'PT-2002', 'TRL-002', 'SITE-BOS', 2, '2023-04-05', 312.4500, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0034', 'PT-2002', 'TRL-002', 'SITE-BOS', 3, '2023-05-03', 289.1200, true,  'Neutropenia grade 3', 'Severe', 200.00, 'Non-responder', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0035', 'PT-2002', 'TRL-002', 'SITE-BOS', 4, '2023-07-03', 267.8800, true,  'Thrombocytopenia', 'Moderate', 150.00, 'Non-responder', true, 'Dose reduced due to AE', '2025-01-01T00:00:00'),
    ('OBS-0036', 'PT-2003', 'TRL-002', 'SITE-LON', 1, '2023-03-28', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0037', 'PT-2003', 'TRL-002', 'SITE-LON', 2, '2023-04-10', 189.2200, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0038', 'PT-2003', 'TRL-002', 'SITE-LON', 3, '2023-05-08', 142.6500, false, NULL, NULL,       200.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0039', 'PT-2003', 'TRL-002', 'SITE-LON', 4, '2023-07-08', 98.3400,  false, NULL, NULL,       250.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0040', 'PT-2003', 'TRL-002', 'SITE-LON', 5, '2023-10-08', 72.1500,  false, NULL, NULL,       250.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0041', 'PT-2004', 'TRL-002', 'SITE-LON', 1, '2023-04-02', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0042', 'PT-2004', 'TRL-002', 'SITE-LON', 2, '2023-04-15', 278.9000, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0043', 'PT-2004', 'TRL-002', 'SITE-LON', 3, '2023-05-13', 265.4400, true,  'Anemia grade 2', 'Moderate', 200.00, 'Non-responder', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0044', 'PT-2005', 'TRL-002', 'SITE-MUN', 1, '2023-04-08', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0045', 'PT-2005', 'TRL-002', 'SITE-MUN', 2, '2023-04-20', 334.7800, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0046', 'PT-2005', 'TRL-002', 'SITE-MUN', 3, '2023-05-18', 301.2200, false, NULL, NULL,       200.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0047', 'PT-2005', 'TRL-002', 'SITE-MUN', 4, '2023-07-18', 258.9100, true,  'Diarrhea grade 1', 'Mild', 250.00, 'Partial', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0048', 'PT-2006', 'TRL-002', 'SITE-TOK', 1, '2023-04-18', NULL,     false, NULL, NULL,       NULL,   'Screen Fail',false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0049', 'PT-2007', 'TRL-002', 'SITE-TOK', 1, '2023-04-22', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0050', 'PT-2007', 'TRL-002', 'SITE-TOK', 2, '2023-05-05', 201.5600, false, NULL, NULL,       200.00, NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0051', 'PT-2007', 'TRL-002', 'SITE-TOK', 3, '2023-06-02', 178.3400, false, NULL, NULL,       200.00, 'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0052', 'PT-2007', 'TRL-002', 'SITE-TOK', 4, '2023-08-02', 145.2200, false, NULL, NULL,       250.00, 'Responder',  false, NULL, '2025-01-01T00:00:00'),
    -- TRL-003 participants: Neurology biomarker (neurofilament light chain pg/mL)
    ('OBS-0053', 'PT-3001', 'TRL-003', 'SITE-MUN', 1, '2023-06-18', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0054', 'PT-3001', 'TRL-003', 'SITE-MUN', 2, '2023-07-01', 42.5000,  false, NULL, NULL,       50.00,  NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0055', 'PT-3001', 'TRL-003', 'SITE-MUN', 3, '2023-07-29', 38.7200,  false, NULL, NULL,       50.00,  'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0056', 'PT-3001', 'TRL-003', 'SITE-MUN', 4, '2023-09-29', 31.4500,  false, NULL, NULL,       75.00,  'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0057', 'PT-3002', 'TRL-003', 'SITE-LON', 1, '2023-06-22', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0058', 'PT-3002', 'TRL-003', 'SITE-LON', 2, '2023-07-05', 55.3400,  false, NULL, NULL,       50.00,  NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0059', 'PT-3002', 'TRL-003', 'SITE-LON', 3, '2023-08-02', 51.8800,  true,  'Dizziness', 'Mild', 50.00, 'Non-responder', false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0060', 'PT-3002', 'TRL-003', 'SITE-LON', 4, '2023-10-02', 48.2100,  false, NULL, NULL,       75.00,  'Partial',    false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0061', 'PT-3003', 'TRL-003', 'SITE-BOS', 1, '2023-06-28', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0062', 'PT-3003', 'TRL-003', 'SITE-BOS', 2, '2023-07-10', 38.1200,  false, NULL, NULL,       50.00,  NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0063', 'PT-3004', 'TRL-003', 'SITE-BOS', 1, '2023-07-02', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0064', 'PT-3004', 'TRL-003', 'SITE-BOS', 2, '2023-07-15', 47.8900,  false, NULL, NULL,       50.00,  NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0065', 'PT-3004', 'TRL-003', 'SITE-BOS', 3, '2023-08-12', 41.2300,  false, NULL, NULL,       50.00,  'Partial',    true,  'Missed scheduled window by 3 days', '2025-01-01T00:00:00'),
    ('OBS-0066', 'PT-3004', 'TRL-003', 'SITE-BOS', 4, '2023-10-12', 34.6700,  false, NULL, NULL,       75.00,  'Responder',  false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0067', 'PT-3005', 'TRL-003', 'SITE-TOK', 1, '2023-07-08', NULL,     false, NULL, NULL,       NULL,   'Screen Fail',false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0068', 'PT-3006', 'TRL-003', 'SITE-MUN', 1, '2023-07-12', NULL,     false, NULL, NULL,       NULL,   'Eligible',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0069', 'PT-3006', 'TRL-003', 'SITE-MUN', 2, '2023-07-25', 61.2400,  false, NULL, NULL,       50.00,  NULL,         false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0070', 'PT-3006', 'TRL-003', 'SITE-MUN', 3, '2023-08-22', 53.1800,  true,  'Insomnia', 'Mild', 50.00, 'Partial',   false, NULL, '2025-01-01T00:00:00'),
    ('OBS-0071', 'PT-3006', 'TRL-003', 'SITE-MUN', 4, '2023-10-22', 44.9500,  false, NULL, NULL,       75.00,  'Responder',  false, NULL, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 71
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_observations;


-- ===================== PSEUDONYMISATION RULES =====================
-- All 4 transforms applied to participant data

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.participant_clean (ssn) TRANSFORM redact PARAMS ('replacement', '[REDACTED]');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.participant_clean (email) TRANSFORM mask PARAMS ('visible_prefix', 2, 'visible_suffix', 4, 'mask_char', '*');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.participant_clean (participant_name) TRANSFORM keyed_hash PARAMS ('algorithm', 'SHA256');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.participant_clean (date_of_birth) TRANSFORM generalize PARAMS ('granularity', 'decade');
