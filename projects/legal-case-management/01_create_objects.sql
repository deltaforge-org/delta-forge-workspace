-- =============================================================================
-- Legal Case Management Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw attorney, case, client, and billing data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched legal data with utilization metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Legal analytics star schema and network graph';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_attorneys (
    attorney_id         STRING      NOT NULL,
    name                STRING      NOT NULL,
    bar_number          STRING      NOT NULL,
    practice_area       STRING,
    partner_flag        BOOLEAN,
    years_experience    INT,
    hourly_rate         DECIMAL(8,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_attorneys';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_attorneys TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_cases (
    case_id             STRING      NOT NULL,
    case_number         STRING      NOT NULL,
    case_type           STRING      NOT NULL,
    court               STRING,
    filing_date         DATE,
    status              STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_cases';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_cases TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_clients (
    client_id           STRING      NOT NULL,
    client_name         STRING      NOT NULL,
    client_type         STRING,
    industry            STRING,
    jurisdiction        STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_clients';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_clients TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_billings (
    billing_id          STRING      NOT NULL,
    case_id             STRING      NOT NULL,
    attorney_id         STRING      NOT NULL,
    client_id           STRING      NOT NULL,
    billing_date        DATE        NOT NULL,
    hours               DECIMAL(5,2) NOT NULL,
    hourly_rate         DECIMAL(8,2) NOT NULL,
    amount              DECIMAL(10,2),
    billable_flag       BOOLEAN     NOT NULL,
    description         STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_billings';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_billings TO USER {{current_user}};

-- Relationship tables for graph
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_case_attorneys (
    case_id             STRING      NOT NULL,
    attorney_id         STRING      NOT NULL,
    role                STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_case_attorneys';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_case_attorneys TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_case_clients (
    case_id             STRING      NOT NULL,
    client_id           STRING      NOT NULL,
    party_role          STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/legal/raw_case_clients';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_case_clients TO USER {{current_user}};

-- ===================== BRONZE SEED: ATTORNEYS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_attorneys VALUES
    ('ATT01', 'Victoria Sterling', 'BAR-2008-4521', 'corporate', true, 16, 650.00, '2024-01-15T00:00:00'),
    ('ATT02', 'Marcus Chen', 'BAR-2012-7834', 'litigation', false, 12, 475.00, '2024-01-15T00:00:00'),
    ('ATT03', 'Sophia Ramirez', 'BAR-2010-3267', 'intellectual_property', true, 14, 580.00, '2024-01-15T00:00:00'),
    ('ATT04', 'James O''Donnell', 'BAR-2015-9102', 'employment', false, 9, 395.00, '2024-01-15T00:00:00'),
    ('ATT05', 'Aisha Patel', 'BAR-2009-6453', 'real_estate', true, 15, 620.00, '2024-01-15T00:00:00'),
    ('ATT06', 'Robert Kowalski', 'BAR-2014-2178', 'litigation', false, 10, 425.00, '2024-01-15T00:00:00'),
    ('ATT07', 'Elena Volkov', 'BAR-2011-5890', 'corporate', false, 13, 520.00, '2024-01-15T00:00:00'),
    ('ATT08', 'David Nakamura', 'BAR-2016-1345', 'employment', false, 8, 375.00, '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_attorneys;


-- ===================== BRONZE SEED: CASES (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_cases VALUES
    ('CASE01', 'CV-2023-001245', 'merger_acquisition', 'Delaware Chancery', '2023-01-15', 'active', '2024-01-15T00:00:00'),
    ('CASE02', 'CV-2023-002389', 'patent_infringement', 'SDNY Federal', '2023-02-20', 'active', '2024-01-15T00:00:00'),
    ('CASE03', 'CV-2023-003456', 'employment_dispute', 'CA Superior', '2023-03-10', 'settled', '2024-01-15T00:00:00'),
    ('CASE04', 'CV-2023-004578', 'contract_breach', 'IL Circuit', '2023-04-05', 'active', '2024-01-15T00:00:00'),
    ('CASE05', 'CV-2023-005612', 'real_estate_dispute', 'NY Supreme', '2023-05-18', 'settled', '2024-01-15T00:00:00'),
    ('CASE06', 'CV-2023-006789', 'securities_fraud', 'SDNY Federal', '2023-06-01', 'active', '2024-01-15T00:00:00'),
    ('CASE07', 'CV-2023-007890', 'trademark_dispute', 'CDCA Federal', '2023-07-12', 'active', '2024-01-15T00:00:00'),
    ('CASE08', 'CV-2023-008912', 'wrongful_termination', 'TX District', '2023-08-22', 'settled', '2024-01-15T00:00:00'),
    ('CASE09', 'CV-2023-009023', 'merger_acquisition', 'Delaware Chancery', '2023-09-05', 'active', '2024-01-15T00:00:00'),
    ('CASE10', 'CV-2023-010134', 'class_action', 'NDCA Federal', '2023-10-15', 'active', '2024-01-15T00:00:00'),
    ('CASE11', 'CV-2023-011245', 'real_estate_closing', 'NY Supreme', '2023-11-01', 'closed', '2024-01-15T00:00:00'),
    ('CASE12', 'CV-2024-012356', 'employment_dispute', 'WA Superior', '2024-01-05', 'active', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_cases;


-- ===================== BRONZE SEED: CLIENTS (10 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_clients VALUES
    ('CLI01', 'Apex Technologies Inc.', 'corporate', 'technology', 'DE', '2024-01-15T00:00:00'),
    ('CLI02', 'Meridian Healthcare Group', 'corporate', 'healthcare', 'NY', '2024-01-15T00:00:00'),
    ('CLI03', 'Sarah Mitchell', 'individual', 'N/A', 'CA', '2024-01-15T00:00:00'),
    ('CLI04', 'Pinnacle Manufacturing LLC', 'corporate', 'manufacturing', 'IL', '2024-01-15T00:00:00'),
    ('CLI05', 'Harborview Properties', 'corporate', 'real_estate', 'NY', '2024-01-15T00:00:00'),
    ('CLI06', 'Quantum Financial Services', 'corporate', 'finance', 'NY', '2024-01-15T00:00:00'),
    ('CLI07', 'NovaGen Biotech', 'corporate', 'biotech', 'CA', '2024-01-15T00:00:00'),
    ('CLI08', 'Michael Torres', 'individual', 'N/A', 'TX', '2024-01-15T00:00:00'),
    ('CLI09', 'GlobalTrade Partners', 'corporate', 'logistics', 'DE', '2024-01-15T00:00:00'),
    ('CLI10', 'DataStream Analytics', 'corporate', 'technology', 'WA', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_clients;


-- ===================== BRONZE SEED: CASE-ATTORNEY RELATIONSHIPS (20 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_case_attorneys VALUES
    ('CASE01', 'ATT01', 'lead', '2024-01-15T00:00:00'),
    ('CASE01', 'ATT07', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE02', 'ATT03', 'lead', '2024-01-15T00:00:00'),
    ('CASE02', 'ATT02', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE03', 'ATT04', 'lead', '2024-01-15T00:00:00'),
    ('CASE03', 'ATT08', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE04', 'ATT02', 'lead', '2024-01-15T00:00:00'),
    ('CASE04', 'ATT06', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE05', 'ATT05', 'lead', '2024-01-15T00:00:00'),
    ('CASE06', 'ATT01', 'lead', '2024-01-15T00:00:00'),
    ('CASE06', 'ATT02', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE06', 'ATT07', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE07', 'ATT03', 'lead', '2024-01-15T00:00:00'),
    ('CASE08', 'ATT04', 'lead', '2024-01-15T00:00:00'),
    ('CASE09', 'ATT07', 'lead', '2024-01-15T00:00:00'),
    ('CASE09', 'ATT01', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE10', 'ATT06', 'lead', '2024-01-15T00:00:00'),
    ('CASE10', 'ATT02', 'co_counsel', '2024-01-15T00:00:00'),
    ('CASE11', 'ATT05', 'lead', '2024-01-15T00:00:00'),
    ('CASE12', 'ATT08', 'lead', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_case_attorneys;


-- ===================== BRONZE SEED: CASE-CLIENT RELATIONSHIPS (14 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_case_clients VALUES
    ('CASE01', 'CLI01', 'acquirer', '2024-01-15T00:00:00'),
    ('CASE01', 'CLI09', 'target', '2024-01-15T00:00:00'),
    ('CASE02', 'CLI07', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE03', 'CLI03', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE04', 'CLI04', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE05', 'CLI05', 'buyer', '2024-01-15T00:00:00'),
    ('CASE06', 'CLI06', 'defendant', '2024-01-15T00:00:00'),
    ('CASE06', 'CLI01', 'co_defendant', '2024-01-15T00:00:00'),
    ('CASE07', 'CLI07', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE08', 'CLI08', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE09', 'CLI09', 'acquirer', '2024-01-15T00:00:00'),
    ('CASE10', 'CLI02', 'plaintiff', '2024-01-15T00:00:00'),
    ('CASE11', 'CLI05', 'seller', '2024-01-15T00:00:00'),
    ('CASE12', 'CLI10', 'plaintiff', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 14
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_case_clients;


-- ===================== BRONZE SEED: BILLINGS (65 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_billings VALUES
    ('BIL001', 'CASE01', 'ATT01', 'CLI01', '2023-01-20', 6.5, 650.00, 4225.00, true, 'Due diligence review', '2024-01-15T00:00:00'),
    ('BIL002', 'CASE01', 'ATT01', 'CLI01', '2023-02-15', 8.0, 650.00, 5200.00, true, 'Board resolution drafting', '2024-01-15T00:00:00'),
    ('BIL003', 'CASE01', 'ATT07', 'CLI01', '2023-02-15', 5.0, 520.00, 2600.00, true, 'Financial document analysis', '2024-01-15T00:00:00'),
    ('BIL004', 'CASE01', 'ATT01', 'CLI01', '2023-03-10', 4.0, 650.00, 2600.00, true, 'SEC filing preparation', '2024-01-15T00:00:00'),
    ('BIL005', 'CASE01', 'ATT07', 'CLI01', '2023-03-10', 3.5, 520.00, 1820.00, true, 'Antitrust analysis', '2024-01-15T00:00:00'),
    ('BIL006', 'CASE02', 'ATT03', 'CLI07', '2023-03-01', 7.0, 580.00, 4060.00, true, 'Patent claim analysis', '2024-01-15T00:00:00'),
    ('BIL007', 'CASE02', 'ATT03', 'CLI07', '2023-04-05', 9.5, 580.00, 5510.00, true, 'Prior art research', '2024-01-15T00:00:00'),
    ('BIL008', 'CASE02', 'ATT02', 'CLI07', '2023-04-05', 4.0, 475.00, 1900.00, true, 'Expert witness coordination', '2024-01-15T00:00:00'),
    ('BIL009', 'CASE02', 'ATT03', 'CLI07', '2023-05-12', 6.0, 580.00, 3480.00, true, 'Claim construction brief', '2024-01-15T00:00:00'),
    ('BIL010', 'CASE03', 'ATT04', 'CLI03', '2023-03-15', 3.0, 395.00, 1185.00, true, 'Initial client consultation', '2024-01-15T00:00:00'),
    ('BIL011', 'CASE03', 'ATT04', 'CLI03', '2023-04-10', 5.5, 395.00, 2172.50, true, 'Demand letter drafting', '2024-01-15T00:00:00'),
    ('BIL012', 'CASE03', 'ATT08', 'CLI03', '2023-04-10', 2.5, 375.00, 937.50, true, 'Document compilation', '2024-01-15T00:00:00'),
    ('BIL013', 'CASE03', 'ATT04', 'CLI03', '2023-05-20', 4.0, 395.00, 1580.00, true, 'Mediation preparation', '2024-01-15T00:00:00'),
    ('BIL014', 'CASE03', 'ATT04', 'CLI03', '2023-06-15', 2.0, 395.00, 790.00, true, 'Settlement negotiations', '2024-01-15T00:00:00'),
    ('BIL015', 'CASE04', 'ATT02', 'CLI04', '2023-04-10', 5.0, 475.00, 2375.00, true, 'Complaint drafting', '2024-01-15T00:00:00'),
    ('BIL016', 'CASE04', 'ATT02', 'CLI04', '2023-05-15', 7.5, 475.00, 3562.50, true, 'Discovery requests', '2024-01-15T00:00:00'),
    ('BIL017', 'CASE04', 'ATT06', 'CLI04', '2023-05-15', 4.0, 425.00, 1700.00, true, 'Interrogatory preparation', '2024-01-15T00:00:00'),
    ('BIL018', 'CASE04', 'ATT02', 'CLI04', '2023-06-20', 3.0, 475.00, 1425.00, true, 'Deposition preparation', '2024-01-15T00:00:00'),
    ('BIL019', 'CASE05', 'ATT05', 'CLI05', '2023-05-25', 8.0, 620.00, 4960.00, true, 'Title search and review', '2024-01-15T00:00:00'),
    ('BIL020', 'CASE05', 'ATT05', 'CLI05', '2023-06-10', 6.0, 620.00, 3720.00, true, 'Contract negotiations', '2024-01-15T00:00:00'),
    ('BIL021', 'CASE05', 'ATT05', 'CLI05', '2023-07-05', 4.5, 620.00, 2790.00, true, 'Closing preparation', '2024-01-15T00:00:00'),
    ('BIL022', 'CASE06', 'ATT01', 'CLI06', '2023-06-10', 10.0, 650.00, 6500.00, true, 'SEC response preparation', '2024-01-15T00:00:00'),
    ('BIL023', 'CASE06', 'ATT02', 'CLI06', '2023-06-10', 6.0, 475.00, 2850.00, true, 'Document review', '2024-01-15T00:00:00'),
    ('BIL024', 'CASE06', 'ATT07', 'CLI06', '2023-07-15', 5.5, 520.00, 2860.00, true, 'Compliance audit', '2024-01-15T00:00:00'),
    ('BIL025', 'CASE06', 'ATT01', 'CLI06', '2023-07-15', 7.0, 650.00, 4550.00, true, 'Expert report review', '2024-01-15T00:00:00'),
    ('BIL026', 'CASE06', 'ATT02', 'CLI06', '2023-08-10', 4.5, 475.00, 2137.50, true, 'Witness preparation', '2024-01-15T00:00:00'),
    ('BIL027', 'CASE07', 'ATT03', 'CLI07', '2023-07-20', 5.0, 580.00, 2900.00, true, 'Trademark portfolio review', '2024-01-15T00:00:00'),
    ('BIL028', 'CASE07', 'ATT03', 'CLI07', '2023-08-15', 6.5, 580.00, 3770.00, true, 'Opposition brief', '2024-01-15T00:00:00'),
    ('BIL029', 'CASE08', 'ATT04', 'CLI08', '2023-08-28', 3.5, 395.00, 1382.50, true, 'Intake and fact gathering', '2024-01-15T00:00:00'),
    ('BIL030', 'CASE08', 'ATT04', 'CLI08', '2023-09-15', 6.0, 395.00, 2370.00, true, 'EEOC charge response', '2024-01-15T00:00:00'),
    ('BIL031', 'CASE08', 'ATT04', 'CLI08', '2023-10-10', 4.0, 395.00, 1580.00, true, 'Settlement conference', '2024-01-15T00:00:00'),
    ('BIL032', 'CASE09', 'ATT07', 'CLI09', '2023-09-10', 8.0, 520.00, 4160.00, true, 'Due diligence coordination', '2024-01-15T00:00:00'),
    ('BIL033', 'CASE09', 'ATT01', 'CLI09', '2023-09-10', 5.0, 650.00, 3250.00, true, 'Regulatory review', '2024-01-15T00:00:00'),
    ('BIL034', 'CASE09', 'ATT07', 'CLI09', '2023-10-05', 6.5, 520.00, 3380.00, true, 'Purchase agreement draft', '2024-01-15T00:00:00'),
    ('BIL035', 'CASE10', 'ATT06', 'CLI02', '2023-10-20', 8.0, 425.00, 3400.00, true, 'Class certification research', '2024-01-15T00:00:00'),
    ('BIL036', 'CASE10', 'ATT02', 'CLI02', '2023-10-20', 5.0, 475.00, 2375.00, true, 'Medical records review', '2024-01-15T00:00:00'),
    ('BIL037', 'CASE10', 'ATT06', 'CLI02', '2023-11-15', 9.0, 425.00, 3825.00, true, 'Motion for class cert', '2024-01-15T00:00:00'),
    ('BIL038', 'CASE11', 'ATT05', 'CLI05', '2023-11-05', 5.0, 620.00, 3100.00, true, 'Closing document prep', '2024-01-15T00:00:00'),
    ('BIL039', 'CASE11', 'ATT05', 'CLI05', '2023-11-20', 3.0, 620.00, 1860.00, true, 'Final closing execution', '2024-01-15T00:00:00'),
    ('BIL040', 'CASE12', 'ATT08', 'CLI10', '2024-01-08', 3.0, 375.00, 1125.00, true, 'Initial consultation', '2024-01-15T00:00:00'),
    ('BIL041', 'CASE01', 'ATT01', 'CLI01', '2023-04-20', 2.0, 650.00, 0.00, false, 'Internal strategy meeting', '2024-01-15T00:00:00'),
    ('BIL042', 'CASE06', 'ATT01', 'CLI06', '2023-08-20', 1.5, 650.00, 0.00, false, 'Pro bono compliance review', '2024-01-15T00:00:00'),
    ('BIL043', 'CASE02', 'ATT03', 'CLI07', '2023-06-10', 3.0, 580.00, 0.00, false, 'CLE presentation prep', '2024-01-15T00:00:00'),
    ('BIL044', 'CASE04', 'ATT06', 'CLI04', '2023-07-10', 6.0, 425.00, 2550.00, true, 'Motion to compel', '2024-01-15T00:00:00'),
    ('BIL045', 'CASE06', 'ATT07', 'CLI06', '2023-09-05', 4.0, 520.00, 2080.00, true, 'Board presentation prep', '2024-01-15T00:00:00'),
    ('BIL046', 'CASE01', 'ATT07', 'CLI01', '2023-05-10', 7.0, 520.00, 3640.00, true, 'Integration planning', '2024-01-15T00:00:00'),
    ('BIL047', 'CASE09', 'ATT07', 'CLI09', '2023-11-10', 5.0, 520.00, 2600.00, true, 'Shareholder approval docs', '2024-01-15T00:00:00'),
    ('BIL048', 'CASE10', 'ATT06', 'CLI02', '2023-12-10', 7.0, 425.00, 2975.00, true, 'Expert deposition prep', '2024-01-15T00:00:00'),
    ('BIL049', 'CASE02', 'ATT02', 'CLI07', '2023-06-20', 5.0, 475.00, 2375.00, true, 'Markman hearing prep', '2024-01-15T00:00:00'),
    ('BIL050', 'CASE07', 'ATT03', 'CLI07', '2023-09-20', 4.0, 580.00, 2320.00, true, 'TTAB response', '2024-01-15T00:00:00'),
    ('BIL051', 'CASE05', 'ATT05', 'CLI05', '2023-07-20', 2.0, 620.00, 1240.00, true, 'Post-closing adjustments', '2024-01-15T00:00:00'),
    ('BIL052', 'CASE08', 'ATT08', 'CLI08', '2023-09-15', 2.0, 375.00, 0.00, false, 'Admin filing', '2024-01-15T00:00:00'),
    ('BIL053', 'CASE01', 'ATT01', 'CLI01', '2023-06-15', 5.0, 650.00, 3250.00, true, 'Closing conditions review', '2024-01-15T00:00:00'),
    ('BIL054', 'CASE06', 'ATT01', 'CLI06', '2023-09-20', 8.0, 650.00, 5200.00, true, 'Trial preparation', '2024-01-15T00:00:00'),
    ('BIL055', 'CASE04', 'ATT02', 'CLI04', '2023-08-15', 6.0, 475.00, 2850.00, true, 'Summary judgment brief', '2024-01-15T00:00:00'),
    ('BIL056', 'CASE10', 'ATT02', 'CLI02', '2023-12-15', 4.0, 475.00, 1900.00, true, 'Settlement analysis', '2024-01-15T00:00:00'),
    ('BIL057', 'CASE09', 'ATT01', 'CLI09', '2023-11-10', 4.0, 650.00, 2600.00, true, 'Regulatory compliance', '2024-01-15T00:00:00'),
    ('BIL058', 'CASE03', 'ATT08', 'CLI03', '2023-05-20', 1.5, 375.00, 562.50, true, 'Filing assistance', '2024-01-15T00:00:00'),
    ('BIL059', 'CASE12', 'ATT08', 'CLI10', '2024-01-10', 4.0, 375.00, 1500.00, true, 'Complaint drafting', '2024-01-15T00:00:00'),
    ('BIL060', 'CASE07', 'ATT03', 'CLI07', '2023-10-15', 5.5, 580.00, 3190.00, true, 'Damages calculation', '2024-01-15T00:00:00'),
    ('BIL061', 'CASE01', 'ATT07', 'CLI01', '2023-07-10', 3.0, 520.00, 1560.00, true, 'Post-merger filing', '2024-01-15T00:00:00'),
    ('BIL062', 'CASE06', 'ATT02', 'CLI06', '2023-10-05', 5.5, 475.00, 2612.50, true, 'Deposition of CFO', '2024-01-15T00:00:00'),
    ('BIL063', 'CASE11', 'ATT05', 'CLI05', '2023-12-05', 1.0, 620.00, 0.00, false, 'File archival', '2024-01-15T00:00:00'),
    ('BIL064', 'CASE04', 'ATT06', 'CLI04', '2023-09-10', 3.5, 425.00, 1487.50, true, 'Reply brief', '2024-01-15T00:00:00'),
    ('BIL065', 'CASE10', 'ATT06', 'CLI02', '2024-01-05', 6.0, 425.00, 2550.00, true, 'Preliminary hearing prep', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_billings;


-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.billings_enriched (
    billing_id          STRING      NOT NULL,
    case_id             STRING      NOT NULL,
    attorney_id         STRING      NOT NULL,
    client_id           STRING      NOT NULL,
    case_type           STRING,
    practice_area       STRING,
    billing_date        DATE        NOT NULL,
    hours               DECIMAL(5,2) NOT NULL,
    hourly_rate         DECIMAL(8,2) NOT NULL,
    amount              DECIMAL(10,2),
    billable_flag       BOOLEAN     NOT NULL,
    partner_flag        BOOLEAN,
    complexity_score    DECIMAL(5,2),
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/legal/billings_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.billings_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_case (
    case_key            INT         NOT NULL,
    case_id             STRING      NOT NULL,
    case_number         STRING      NOT NULL,
    case_type           STRING      NOT NULL,
    court               STRING,
    filing_date         DATE,
    status              STRING,
    complexity_score    DECIMAL(5,2),
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/legal/dim_case';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_case TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_attorney (
    attorney_key        INT         NOT NULL,
    attorney_id         STRING      NOT NULL,
    name                STRING      NOT NULL,
    bar_number          STRING,
    practice_area       STRING,
    partner_flag        BOOLEAN,
    years_experience    INT,
    hourly_rate         DECIMAL(8,2),
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/legal/dim_attorney';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_attorney TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_client (
    client_key          INT         NOT NULL,
    client_id           STRING      NOT NULL,
    client_name         STRING      NOT NULL,
    client_type         STRING,
    industry            STRING,
    jurisdiction        STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/legal/dim_client';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_client TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_billings (
    billing_key         INT         NOT NULL,
    case_key            INT         NOT NULL,
    attorney_key        INT         NOT NULL,
    client_key          INT         NOT NULL,
    billing_date        DATE        NOT NULL,
    hours               DECIMAL(5,2) NOT NULL,
    hourly_rate         DECIMAL(8,2) NOT NULL,
    amount              DECIMAL(10,2),
    billable_flag       BOOLEAN     NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/legal/fact_billings';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_billings TO USER {{current_user}};

-- ===================== GRAPH =====================

CREATE GRAPH IF NOT EXISTS {{zone_prefix}}.gold.legal_network
    VERTEX TABLE {{zone_prefix}}.gold.dim_attorney KEY (attorney_key)
    VERTEX TABLE {{zone_prefix}}.gold.dim_case KEY (case_key)
    VERTEX TABLE {{zone_prefix}}.gold.dim_client KEY (client_key)
    EDGE TABLE {{zone_prefix}}.bronze.raw_case_attorneys AS REPRESENTS SOURCE KEY (attorney_id) REFERENCES dim_attorney (attorney_id) DESTINATION KEY (case_id) REFERENCES dim_case (case_id)
    EDGE TABLE {{zone_prefix}}.bronze.raw_case_clients AS PARTY_TO SOURCE KEY (client_id) REFERENCES dim_client (client_id) DESTINATION KEY (case_id) REFERENCES dim_case (case_id)
    DIRECTED;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_client (client_name) TRANSFORM redact PARAMS (mask = '[REDACTED]');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_case (case_number) TRANSFORM keyed_hash PARAMS (salt = 'delta_forge_salt_2024');
