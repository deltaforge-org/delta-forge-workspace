-- =============================================================================
-- Nonprofit Donations Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw donation and donor feeds';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Tiered donors and enriched donations';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for fundraising analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_donors (
    donor_id           STRING      NOT NULL,
    name               STRING,
    email              STRING,
    donor_type         STRING,
    acquisition_source STRING,
    first_donation_date DATE,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/bronze/raw_donors';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_campaigns (
    campaign_id        STRING      NOT NULL,
    campaign_name      STRING      NOT NULL,
    campaign_type      STRING,
    start_date         DATE,
    end_date           DATE,
    goal_amount        DECIMAL(14,2),
    channel            STRING,
    cost               DECIMAL(12,2),
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/bronze/raw_campaigns';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_funds (
    fund_id            STRING      NOT NULL,
    fund_name          STRING      NOT NULL,
    fund_type          STRING,
    restricted_flag    BOOLEAN,
    purpose            STRING,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/bronze/raw_funds';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_donations (
    donation_id        STRING      NOT NULL,
    donor_id           STRING      NOT NULL,
    campaign_id        STRING      NOT NULL,
    fund_id            STRING      NOT NULL,
    donation_date      DATE        NOT NULL,
    amount             DECIMAL(12,2) NOT NULL,
    payment_method     STRING,
    is_recurring       BOOLEAN,
    tax_deductible_flag BOOLEAN,
    acknowledgment_sent BOOLEAN,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/bronze/raw_donations';

-- ===================== SILVER TABLES =====================

-- CDF enabled to track donor tier changes over time
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.donors_tiered (
    donor_id           STRING      NOT NULL,
    name               STRING,
    email              STRING,
    donor_type         STRING,
    acquisition_source STRING,
    first_donation_date DATE,
    lifetime_amount    DECIMAL(14,2),
    current_tier       STRING,
    tier_since_date    DATE,
    previous_tier      STRING,
    donation_count     INT,
    last_donation_date DATE,
    is_retained        BOOLEAN,
    is_lapsed          BOOLEAN,
    enriched_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/silver/donors_tiered'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.donations_enriched (
    donation_id        STRING      NOT NULL,
    donor_id           STRING      NOT NULL,
    campaign_id        STRING      NOT NULL,
    fund_id            STRING      NOT NULL,
    donation_date      DATE,
    amount             DECIMAL(12,2),
    payment_method     STRING,
    is_recurring       BOOLEAN,
    tax_deductible_flag BOOLEAN,
    acknowledgment_sent BOOLEAN,
    donor_tier         STRING,
    is_major_gift      BOOLEAN,
    enriched_at        TIMESTAMP
) LOCATION '{{data_path}}/npo/silver/donations_enriched';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_donor (
    donor_key          STRING      NOT NULL,
    donor_id           STRING,
    name               STRING,
    email              STRING,
    donor_type         STRING,
    acquisition_source STRING,
    first_donation_date DATE,
    lifetime_amount    DECIMAL(14,2),
    current_tier       STRING,
    tier_since_date    DATE
) LOCATION '{{data_path}}/npo/gold/dim_donor';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_campaign (
    campaign_key       STRING      NOT NULL,
    campaign_id        STRING,
    campaign_name      STRING,
    campaign_type      STRING,
    start_date         DATE,
    end_date           DATE,
    goal_amount        DECIMAL(14,2),
    channel            STRING
) LOCATION '{{data_path}}/npo/gold/dim_campaign';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_fund (
    fund_key           STRING      NOT NULL,
    fund_name          STRING,
    fund_type          STRING,
    restricted_flag    BOOLEAN,
    purpose            STRING
) LOCATION '{{data_path}}/npo/gold/dim_fund';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_donations (
    donation_key       STRING      NOT NULL,
    donor_key          STRING      NOT NULL,
    campaign_key       STRING      NOT NULL,
    fund_key           STRING      NOT NULL,
    donation_date      DATE,
    amount             DECIMAL(12,2),
    payment_method     STRING,
    is_recurring       BOOLEAN,
    tax_deductible_flag BOOLEAN,
    acknowledgment_sent BOOLEAN
) LOCATION '{{data_path}}/npo/gold/fact_donations';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_fundraising (
    campaign_id        STRING      NOT NULL,
    fund_name          STRING,
    period             STRING      NOT NULL,
    total_donations    DECIMAL(14,2),
    unique_donors      INT,
    avg_donation       DECIMAL(10,2),
    recurring_pct      DECIMAL(5,2),
    donor_retention_rate DECIMAL(5,2),
    goal_attainment_pct DECIMAL(7,2),
    cost_per_dollar_raised DECIMAL(8,4),
    major_gift_count   INT
) LOCATION '{{data_path}}/npo/gold/kpi_fundraising';

-- ===================== GRANTS =====================
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_donations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_donors TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_campaigns TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_funds TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.donors_tiered TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.donations_enriched TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_donations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_donor TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_campaign TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_fund TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_fundraising TO USER {{current_user}};

-- ===================== PSEUDONYMISATION =====================
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_donors (email) TRANSFORM mask PARAMS ('mask_char' = '*', 'visible_chars' = 3);
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_donors (name) TRANSFORM mask PARAMS ('mask_char' = '*', 'visible_chars' = 3);

-- ===================== SEED DATA: DONORS (15 donors) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_donors VALUES
('DNR-001', 'Margaret Thornton',     'margaret.t@email.com',     'individual', 'gala_event',     '2020-03-15', '2024-01-01T00:00:00'),
('DNR-002', 'Richard Blackwell',     'r.blackwell@corp.com',     'individual', 'direct_mail',    '2019-06-20', '2024-01-01T00:00:00'),
('DNR-003', 'Greenfield Foundation', 'grants@greenfield.org',    'foundation', 'grant_proposal', '2021-01-10', '2024-01-01T00:00:00'),
('DNR-004', 'Susan Park',           'susanp@email.com',          'individual', 'website',        '2022-09-01', '2024-01-01T00:00:00'),
('DNR-005', 'Apex Industries LLC',   'csr@apex-ind.com',         'corporation','corporate_match', '2020-11-15', '2024-01-01T00:00:00'),
('DNR-006', 'David Martinez',       'dmartinez@email.com',       'individual', 'peer_referral',  '2023-02-14', '2024-01-01T00:00:00'),
('DNR-007', 'Emily Watson',         'ewatson@email.com',         'individual', 'social_media',   '2023-06-01', '2024-01-01T00:00:00'),
('DNR-008', 'Heritage Trust',       'info@heritagetrust.org',    'foundation', 'grant_proposal', '2019-03-20', '2024-01-01T00:00:00'),
('DNR-009', 'Thomas Anderson',      'tanderson@email.com',       'individual', 'gala_event',     '2021-11-05', '2024-01-01T00:00:00'),
('DNR-010', 'Jennifer Liu',         'jliu@email.com',            'individual', 'website',        '2022-04-18', '2024-01-01T00:00:00'),
('DNR-011', 'BioTech Partners Corp','giving@biotech-p.com',      'corporation','sponsorship',    '2021-07-01', '2024-01-01T00:00:00'),
('DNR-012', 'Robert Singh',         'rsingh@email.com',          'individual', 'direct_mail',    '2023-09-10', '2024-01-01T00:00:00'),
('DNR-013', 'Patricia Chambers',    'pchambers@email.com',       'individual', 'peer_referral',  '2020-08-22', '2024-01-01T00:00:00'),
('DNR-014', 'Community First Fund', 'grants@commfirst.org',      'foundation', 'grant_proposal', '2022-02-01', '2024-01-01T00:00:00'),
('DNR-015', 'Michael Torres',       'mtorres@email.com',         'individual', 'social_media',   '2024-01-15', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_donors;


-- ===================== SEED DATA: CAMPAIGNS (5 campaigns) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_campaigns VALUES
('CMP-001', 'Annual Giving 2023',       'annual',     '2023-01-01', '2023-12-31', 500000.00, 'multi_channel', 45000.00, '2024-01-01T00:00:00'),
('CMP-002', 'Spring Gala 2024',         'event',      '2024-03-15', '2024-03-15', 200000.00, 'event',         80000.00, '2024-01-01T00:00:00'),
('CMP-003', 'Year-End Appeal 2023',     'appeal',     '2023-11-01', '2023-12-31', 300000.00, 'direct_mail',   25000.00, '2024-01-01T00:00:00'),
('CMP-004', 'Education Access 2024',    'program',    '2024-01-01', '2024-06-30', 150000.00, 'online',        15000.00, '2024-01-01T00:00:00'),
('CMP-005', 'Capital Campaign 2024',    'capital',    '2024-01-01', '2024-12-31', 1000000.00,'multi_channel', 120000.00,'2024-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_campaigns;


-- ===================== SEED DATA: FUNDS (4 funds) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_funds VALUES
('FND-001', 'General Operating',  'unrestricted', false, 'Day-to-day operations and overhead',    '2024-01-01T00:00:00'),
('FND-002', 'Education Programs', 'restricted',   true,  'Scholarships and educational outreach', '2024-01-01T00:00:00'),
('FND-003', 'Building Fund',      'restricted',   true,  'New community center construction',     '2024-01-01T00:00:00'),
('FND-004', 'Emergency Relief',   'restricted',   true,  'Disaster response and urgent needs',    '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_funds;


-- ===================== SEED DATA: DONATIONS (62 rows, 18 months) =====================
-- Includes recurring monthly gifts, one-time gifts, major gifts (>$10K), lapsed donors
INSERT INTO {{zone_prefix}}.bronze.raw_donations VALUES
-- 2023 Q1-Q2 donations (Annual Giving campaign)
('DON-001', 'DNR-001', 'CMP-001', 'FND-001', '2023-01-15', 500.00,  'credit_card', true,  true, true,  '2023-01-15T00:00:00'),
('DON-002', 'DNR-002', 'CMP-001', 'FND-001', '2023-02-01', 250.00,  'credit_card', true,  true, true,  '2023-02-01T00:00:00'),
('DON-003', 'DNR-003', 'CMP-001', 'FND-002', '2023-02-15', 25000.00,'wire',        false, true, true,  '2023-02-15T00:00:00'),
('DON-004', 'DNR-005', 'CMP-001', 'FND-001', '2023-03-01', 10000.00,'wire',        false, true, true,  '2023-03-01T00:00:00'),
('DON-005', 'DNR-001', 'CMP-001', 'FND-001', '2023-03-15', 500.00,  'credit_card', true,  true, true,  '2023-03-15T00:00:00'),
('DON-006', 'DNR-004', 'CMP-001', 'FND-002', '2023-04-01', 100.00,  'credit_card', false, true, true,  '2023-04-01T00:00:00'),
('DON-007', 'DNR-002', 'CMP-001', 'FND-001', '2023-04-01', 250.00,  'credit_card', true,  true, true,  '2023-04-01T00:00:00'),
('DON-008', 'DNR-009', 'CMP-001', 'FND-001', '2023-05-01', 150.00,  'check',       false, true, true,  '2023-05-01T00:00:00'),
('DON-009', 'DNR-001', 'CMP-001', 'FND-001', '2023-05-15', 500.00,  'credit_card', true,  true, true,  '2023-05-15T00:00:00'),
('DON-010', 'DNR-008', 'CMP-001', 'FND-002', '2023-06-01', 50000.00,'wire',        false, true, true,  '2023-06-01T00:00:00'),
('DON-011', 'DNR-010', 'CMP-001', 'FND-001', '2023-06-15', 200.00,  'credit_card', true,  true, true,  '2023-06-15T00:00:00'),
('DON-012', 'DNR-002', 'CMP-001', 'FND-001', '2023-06-01', 250.00,  'credit_card', true,  true, true,  '2023-06-01T00:00:00'),
-- 2023 Q3-Q4 donations
('DON-013', 'DNR-001', 'CMP-001', 'FND-001', '2023-07-15', 500.00,  'credit_card', true,  true, true,  '2023-07-15T00:00:00'),
('DON-014', 'DNR-011', 'CMP-001', 'FND-003', '2023-07-01', 15000.00,'wire',        false, true, true,  '2023-07-01T00:00:00'),
('DON-015', 'DNR-002', 'CMP-001', 'FND-001', '2023-08-01', 250.00,  'credit_card', true,  true, true,  '2023-08-01T00:00:00'),
('DON-016', 'DNR-006', 'CMP-001', 'FND-004', '2023-08-15', 75.00,   'credit_card', false, true, true,  '2023-08-15T00:00:00'),
('DON-017', 'DNR-013', 'CMP-001', 'FND-001', '2023-09-01', 1000.00, 'check',       false, true, true,  '2023-09-01T00:00:00'),
('DON-018', 'DNR-001', 'CMP-001', 'FND-001', '2023-09-15', 500.00,  'credit_card', true,  true, true,  '2023-09-15T00:00:00'),
('DON-019', 'DNR-010', 'CMP-001', 'FND-001', '2023-09-15', 200.00,  'credit_card', true,  true, true,  '2023-09-15T00:00:00'),
('DON-020', 'DNR-002', 'CMP-001', 'FND-001', '2023-10-01', 250.00,  'credit_card', true,  true, true,  '2023-10-01T00:00:00'),
-- Year-End Appeal 2023
('DON-021', 'DNR-001', 'CMP-003', 'FND-001', '2023-11-15', 1000.00, 'credit_card', false, true, true,  '2023-11-15T00:00:00'),
('DON-022', 'DNR-003', 'CMP-003', 'FND-002', '2023-11-20', 30000.00,'wire',        false, true, true,  '2023-11-20T00:00:00'),
('DON-023', 'DNR-005', 'CMP-003', 'FND-003', '2023-12-01', 25000.00,'wire',        false, true, true,  '2023-12-01T00:00:00'),
('DON-024', 'DNR-002', 'CMP-003', 'FND-001', '2023-12-01', 500.00,  'credit_card', false, true, true,  '2023-12-01T00:00:00'),
('DON-025', 'DNR-009', 'CMP-003', 'FND-004', '2023-12-10', 300.00,  'check',       false, true, true,  '2023-12-10T00:00:00'),
('DON-026', 'DNR-013', 'CMP-003', 'FND-001', '2023-12-15', 2000.00, 'credit_card', false, true, true,  '2023-12-15T00:00:00'),
('DON-027', 'DNR-014', 'CMP-003', 'FND-002', '2023-12-20', 20000.00,'wire',        false, true, true,  '2023-12-20T00:00:00'),
('DON-028', 'DNR-010', 'CMP-003', 'FND-001', '2023-12-25', 500.00,  'credit_card', false, true, true,  '2023-12-25T00:00:00'),
('DON-029', 'DNR-004', 'CMP-003', 'FND-004', '2023-12-28', 150.00,  'credit_card', false, true, true,  '2023-12-28T00:00:00'),
('DON-030', 'DNR-007', 'CMP-003', 'FND-001', '2023-12-30', 50.00,   'credit_card', false, true, true,  '2023-12-30T00:00:00'),
-- 2024 Education Access campaign
('DON-031', 'DNR-001', 'CMP-004', 'FND-002', '2024-01-15', 500.00,  'credit_card', true,  true, true,  '2024-01-15T00:00:00'),
('DON-032', 'DNR-002', 'CMP-004', 'FND-002', '2024-01-20', 250.00,  'credit_card', true,  true, true,  '2024-01-20T00:00:00'),
('DON-033', 'DNR-004', 'CMP-004', 'FND-002', '2024-02-01', 100.00,  'credit_card', false, true, true,  '2024-02-01T00:00:00'),
('DON-034', 'DNR-010', 'CMP-004', 'FND-002', '2024-02-15', 200.00,  'credit_card', true,  true, true,  '2024-02-15T00:00:00'),
('DON-035', 'DNR-015', 'CMP-004', 'FND-002', '2024-01-20', 25.00,   'credit_card', false, true, false, '2024-01-20T00:00:00'),
('DON-036', 'DNR-012', 'CMP-004', 'FND-002', '2024-02-01', 50.00,   'credit_card', false, true, true,  '2024-02-01T00:00:00'),
('DON-037', 'DNR-006', 'CMP-004', 'FND-002', '2024-02-14', 100.00,  'credit_card', false, true, true,  '2024-02-14T00:00:00'),
-- Capital Campaign 2024
('DON-038', 'DNR-005', 'CMP-005', 'FND-003', '2024-01-15', 50000.00,'wire',        false, true, true,  '2024-01-15T00:00:00'),
('DON-039', 'DNR-008', 'CMP-005', 'FND-003', '2024-02-01', 100000.00,'wire',       false, true, true,  '2024-02-01T00:00:00'),
('DON-040', 'DNR-011', 'CMP-005', 'FND-003', '2024-02-15', 25000.00,'wire',        false, true, true,  '2024-02-15T00:00:00'),
('DON-041', 'DNR-003', 'CMP-005', 'FND-003', '2024-03-01', 75000.00,'wire',        false, true, true,  '2024-03-01T00:00:00'),
('DON-042', 'DNR-014', 'CMP-005', 'FND-003', '2024-03-10', 15000.00,'wire',        false, true, true,  '2024-03-10T00:00:00'),
-- Spring Gala 2024
('DON-043', 'DNR-001', 'CMP-002', 'FND-001', '2024-03-15', 5000.00, 'check',       false, true, true,  '2024-03-15T00:00:00'),
('DON-044', 'DNR-002', 'CMP-002', 'FND-001', '2024-03-15', 2500.00, 'credit_card', false, true, true,  '2024-03-15T00:00:00'),
('DON-045', 'DNR-009', 'CMP-002', 'FND-001', '2024-03-15', 1000.00, 'check',       false, true, true,  '2024-03-15T00:00:00'),
('DON-046', 'DNR-013', 'CMP-002', 'FND-001', '2024-03-15', 3000.00, 'credit_card', false, true, true,  '2024-03-15T00:00:00'),
('DON-047', 'DNR-004', 'CMP-002', 'FND-001', '2024-03-15', 500.00,  'credit_card', false, true, true,  '2024-03-15T00:00:00'),
('DON-048', 'DNR-005', 'CMP-002', 'FND-001', '2024-03-15', 10000.00,'wire',        false, true, true,  '2024-03-15T00:00:00'),
('DON-049', 'DNR-010', 'CMP-002', 'FND-001', '2024-03-15', 750.00,  'credit_card', false, true, true,  '2024-03-15T00:00:00'),
-- Continued recurring gifts 2024
('DON-050', 'DNR-001', 'CMP-004', 'FND-001', '2024-03-15', 500.00,  'credit_card', true,  true, true,  '2024-03-15T00:00:00'),
('DON-051', 'DNR-002', 'CMP-004', 'FND-001', '2024-03-01', 250.00,  'credit_card', true,  true, true,  '2024-03-01T00:00:00'),
('DON-052', 'DNR-001', 'CMP-004', 'FND-001', '2024-04-15', 500.00,  'credit_card', true,  true, true,  '2024-04-15T00:00:00'),
('DON-053', 'DNR-002', 'CMP-004', 'FND-001', '2024-04-01', 250.00,  'credit_card', true,  true, true,  '2024-04-01T00:00:00'),
('DON-054', 'DNR-010', 'CMP-004', 'FND-002', '2024-04-15', 200.00,  'credit_card', true,  true, true,  '2024-04-15T00:00:00'),
('DON-055', 'DNR-001', 'CMP-004', 'FND-001', '2024-05-15', 500.00,  'credit_card', true,  true, true,  '2024-05-15T00:00:00'),
('DON-056', 'DNR-002', 'CMP-004', 'FND-001', '2024-05-01', 250.00,  'credit_card', true,  true, true,  '2024-05-01T00:00:00'),
('DON-057', 'DNR-013', 'CMP-004', 'FND-001', '2024-05-01', 500.00,  'check',       false, true, true,  '2024-05-01T00:00:00'),
('DON-058', 'DNR-001', 'CMP-004', 'FND-001', '2024-06-15', 500.00,  'credit_card', true,  true, true,  '2024-06-15T00:00:00'),
('DON-059', 'DNR-002', 'CMP-004', 'FND-001', '2024-06-01', 250.00,  'credit_card', true,  true, true,  '2024-06-01T00:00:00'),
('DON-060', 'DNR-011', 'CMP-005', 'FND-003', '2024-05-01', 20000.00,'wire',        false, true, true,  '2024-05-01T00:00:00');

ASSERT ROW_COUNT = 60
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_donations;

