-- =============================================================================
-- Insurance Claims Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE ins_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- SEED DATA: ADJUSTERS (6 rows)

MERGE INTO ins.bronze.raw_adjusters AS target
USING (VALUES
  ('ADJ01', 'Margaret O''Brien',  'property',     12, 'Northeast', '2024-01-15T00:00:00'),
  ('ADJ02', 'Richard Townsend',   'auto',          8, 'West',      '2024-01-15T00:00:00'),
  ('ADJ03', 'Yuki Tanaka',        'liability',    15, 'Midwest',   '2024-01-15T00:00:00'),
  ('ADJ04', 'Carlos Mendez',      'health',        6, 'Southeast', '2024-01-15T00:00:00'),
  ('ADJ05', 'Priya Sharma',       'workers_comp', 10, 'South',     '2024-01-15T00:00:00'),
  ('ADJ06', 'James Whitfield',    'multi_peril',  20, 'Northeast', '2024-01-15T00:00:00')
) AS source(adjuster_id, name, specialization, years_experience, region, ingested_at)
ON target.adjuster_id = source.adjuster_id
WHEN MATCHED THEN UPDATE SET
  name             = source.name,
  specialization   = source.specialization,
  years_experience = source.years_experience,
  region           = source.region,
  ingested_at      = source.ingested_at
WHEN NOT MATCHED THEN INSERT (adjuster_id, name, specialization, years_experience, region, ingested_at)
  VALUES (source.adjuster_id, source.name, source.specialization, source.years_experience, source.region, source.ingested_at);

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_adjusters;

-- SEED DATA: CLAIMANTS (12 rows) — with SSN for pseudonymisation demo

MERGE INTO ins.bronze.raw_claimants AS target
USING (VALUES
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
  ('CLM12', 'Laura Esposito',    '222-33-4444', '1988-10-30', '30-39', 'GA', 'standard',  '2024-01-15T00:00:00')
) AS source(claimant_id, name, ssn, date_of_birth, age_band, state, risk_tier, ingested_at)
ON target.claimant_id = source.claimant_id
WHEN MATCHED THEN UPDATE SET
  name          = source.name,
  ssn           = source.ssn,
  date_of_birth = source.date_of_birth,
  age_band      = source.age_band,
  state         = source.state,
  risk_tier     = source.risk_tier,
  ingested_at   = source.ingested_at
WHEN NOT MATCHED THEN INSERT (claimant_id, name, ssn, date_of_birth, age_band, state, risk_tier, ingested_at)
  VALUES (source.claimant_id, source.name, source.ssn, source.date_of_birth, source.age_band, source.state, source.risk_tier, source.ingested_at);

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_claimants;

-- SEED DATA: POLICIES — 3 batches totaling 23 rows
-- Batch 1: 15 initial policies (change_type = 'new')
-- Batch 2: 5 premium increases
-- Batch 3: 3 coverage upgrades
-- Result: 23 rows total, only 15 with is_current=1 after SCD2 processing

MERGE INTO ins.bronze.raw_policies AS target
USING (VALUES
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
  ('POL007', 'George Nakamura',  '789-01-2345', 'home_plus',    4800.00, 'West',      'CA', 4.3, '2024-01-01', 'coverage_upgrade', '2024-01-15T00:00:00')
) AS source(policy_id, holder_name, ssn, coverage_type, annual_premium, region, state, risk_score, effective_date, change_type, ingested_at)
ON target.policy_id = source.policy_id AND target.effective_date = source.effective_date
WHEN MATCHED THEN UPDATE SET
  holder_name    = source.holder_name,
  ssn            = source.ssn,
  coverage_type  = source.coverage_type,
  annual_premium = source.annual_premium,
  region         = source.region,
  state          = source.state,
  risk_score     = source.risk_score,
  change_type    = source.change_type,
  ingested_at    = source.ingested_at
WHEN NOT MATCHED THEN INSERT (policy_id, holder_name, ssn, coverage_type, annual_premium, region, state, risk_score, effective_date, change_type, ingested_at)
  VALUES (source.policy_id, source.holder_name, source.ssn, source.coverage_type, source.annual_premium, source.region, source.state, source.risk_score, source.effective_date, source.change_type, source.ingested_at);

ASSERT ROW_COUNT = 23
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_policies;

-- SEED DATA: CLAIMS (45 rows) — multiple statuses, 3 outliers for fraud
-- Statuses: filed, under_review, approved, denied, settled
-- 30 settled (with settlement_date), 15 open/under_review/denied
-- 3 deliberately high claims for fraud detection (C0009, C0010, C0037)

MERGE INTO ins.bronze.raw_claims AS target
USING (VALUES
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
  ('C0045', 'POL014', 'CLM12', 'ADJ06', '2023-02-08', '2023-02-10',   3800.00,  3800.00, 'settled',      '2023-03-25', 'Hail damage to vehicle',                      '2024-01-15T00:00:00')
) AS source(claim_id, policy_id, claimant_id, adjuster_id, incident_date, reported_date, claim_amount, approved_amount, status, settlement_date, description, ingested_at)
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_id       = source.policy_id,
  claimant_id     = source.claimant_id,
  adjuster_id     = source.adjuster_id,
  incident_date   = source.incident_date,
  reported_date   = source.reported_date,
  claim_amount    = source.claim_amount,
  approved_amount = source.approved_amount,
  status          = source.status,
  settlement_date = source.settlement_date,
  description     = source.description,
  ingested_at     = source.ingested_at
WHEN NOT MATCHED THEN INSERT (claim_id, policy_id, claimant_id, adjuster_id, incident_date, reported_date, claim_amount, approved_amount, status, settlement_date, description, ingested_at)
  VALUES (source.claim_id, source.policy_id, source.claimant_id, source.adjuster_id, source.incident_date, source.reported_date, source.claim_amount, source.approved_amount, source.status, source.settlement_date, source.description, source.ingested_at);

ASSERT ROW_COUNT = 45
SELECT COUNT(*) AS row_count FROM ins.bronze.raw_claims;
