-- =============================================================================
-- Government Tax Filing Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: Pseudonymisation (REDACT SSN, MASK taxpayer_name), partitioning by
-- fiscal_year, CDF on silver.filings, audit candidate flagging

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw tax filing source data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Validated and enriched filing data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Filing star schema and revenue KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_taxpayers (
    taxpayer_id         STRING      NOT NULL,
    taxpayer_name       STRING,
    ssn                 STRING,
    filing_type         STRING,
    state               STRING,
    dependent_count     INT,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_taxpayers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_taxpayers TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_jurisdictions (
    jurisdiction_id     STRING      NOT NULL,
    jurisdiction_name   STRING      NOT NULL,
    jurisdiction_level  STRING,
    state               STRING,
    tax_rate            DECIMAL(5,4),
    standard_deduction  DECIMAL(10,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_jurisdictions';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_jurisdictions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_preparers (
    preparer_id         STRING      NOT NULL,
    preparer_name       STRING      NOT NULL,
    firm                STRING,
    certification       STRING,
    years_experience    INT,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_preparers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_preparers TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_filings (
    filing_id           STRING      NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    jurisdiction_id     STRING      NOT NULL,
    preparer_id         STRING,
    fiscal_year         INT         NOT NULL,
    filing_date         STRING,
    gross_income        DECIMAL(12,2),
    deductions          DECIMAL(12,2),
    taxable_income      DECIMAL(12,2),
    tax_owed            DECIMAL(12,2),
    tax_paid            DECIMAL(12,2),
    refund_amount       DECIMAL(12,2),
    filing_status       STRING,
    amended_flag        BOOLEAN,
    notes               STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_filings';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_filings TO USER {{current_user}};

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.filings (
    filing_id           STRING      NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    jurisdiction_id     STRING      NOT NULL,
    preparer_id         STRING,
    fiscal_year         INT         NOT NULL,
    filing_date         DATE,
    gross_income        DECIMAL(12,2),
    deductions          DECIMAL(12,2),
    taxable_income      DECIMAL(12,2),
    tax_owed            DECIMAL(12,2),
    tax_paid            DECIMAL(12,2),
    refund_amount       DECIMAL(12,2),
    filing_status       STRING,
    amended_flag        BOOLEAN,
    effective_tax_rate  DECIMAL(5,4),
    deduction_pct       DECIMAL(5,2),
    audit_flag          BOOLEAN,
    income_bracket      STRING
) LOCATION '{{data_path}}/silver/filing/filings'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.filings TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.taxpayer_enriched (
    taxpayer_id         STRING      NOT NULL,
    taxpayer_name       STRING,
    ssn                 STRING,
    filing_type         STRING,
    state               STRING,
    dependent_count     INT,
    income_bracket      STRING,
    total_filings       INT
) LOCATION '{{data_path}}/silver/filing/taxpayer_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.taxpayer_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_filings (
    filing_key          INT         NOT NULL,
    taxpayer_key        INT         NOT NULL,
    jurisdiction_key    INT         NOT NULL,
    preparer_key        INT,
    fiscal_year         INT,
    filing_date         DATE,
    gross_income        DECIMAL(12,2),
    deductions          DECIMAL(12,2),
    taxable_income      DECIMAL(12,2),
    tax_owed            DECIMAL(12,2),
    tax_paid            DECIMAL(12,2),
    refund_amount       DECIMAL(12,2),
    filing_status       STRING,
    amended_flag        BOOLEAN
) LOCATION '{{data_path}}/gold/filing/fact_filings';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_filings TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_taxpayer (
    taxpayer_key        INT         NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    filing_type         STRING,
    state               STRING,
    income_bracket      STRING,
    dependent_count     INT
) LOCATION '{{data_path}}/gold/filing/dim_taxpayer';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_taxpayer TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_jurisdiction (
    jurisdiction_key    INT         NOT NULL,
    jurisdiction_name   STRING      NOT NULL,
    jurisdiction_level  STRING,
    state               STRING,
    tax_rate            DECIMAL(5,4),
    standard_deduction  DECIMAL(10,2)
) LOCATION '{{data_path}}/gold/filing/dim_jurisdiction';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_jurisdiction TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_preparer (
    preparer_key        INT         NOT NULL,
    preparer_name       STRING      NOT NULL,
    firm                STRING,
    certification       STRING,
    error_rate          DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/filing/dim_preparer';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_preparer TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_revenue_analysis (
    fiscal_year         INT         NOT NULL,
    jurisdiction        STRING      NOT NULL,
    total_filings       INT,
    total_taxable_income DECIMAL(14,2),
    total_tax_collected DECIMAL(14,2),
    total_refunds       DECIMAL(14,2),
    avg_effective_rate  DECIMAL(5,4),
    audit_flag_count    INT,
    compliance_rate     DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/filing/kpi_revenue_analysis';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_revenue_analysis TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: JURISDICTIONS (5 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_jurisdictions VALUES
    ('JUR-FED', 'Federal - IRS',           'Federal', NULL, 0.2200, 13850.00, '2025-01-01T00:00:00'),
    ('JUR-NY',  'New York State',          'State',   'NY', 0.0685, 8000.00,  '2025-01-01T00:00:00'),
    ('JUR-CA',  'California FTB',          'State',   'CA', 0.0930, 5202.00,  '2025-01-01T00:00:00'),
    ('JUR-TX',  'Texas Comptroller',       'State',   'TX', 0.0000, 0.00,     '2025-01-01T00:00:00'),
    ('JUR-FL',  'Florida Dept of Revenue', 'State',   'FL', 0.0000, 0.00,     '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_jurisdictions;


-- ===================== BRONZE SEED DATA: PREPARERS (4 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_preparers VALUES
    ('PREP-01', 'Sarah Mitchell CPA',   'Mitchell & Associates',   'CPA',      12, '2025-01-01T00:00:00'),
    ('PREP-02', 'David Park EA',        'TaxPro Services',         'EA',        8, '2025-01-01T00:00:00'),
    ('PREP-03', 'Jennifer Wu CPA',      'Big Four Accounting LLP', 'CPA',      20, '2025-01-01T00:00:00'),
    ('PREP-04', 'Marcus Brown',         'QuickTax Online',         'RTRP',      3, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_preparers;


-- ===================== BRONZE SEED DATA: TAXPAYERS (15 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_taxpayers VALUES
    ('TP-1001', 'Margaret Henderson',  '111-22-3333', 'Individual', 'NY', 2, '2025-01-01T00:00:00'),
    ('TP-1002', 'William Chang',       '222-33-4444', 'Individual', 'CA', 0, '2025-01-01T00:00:00'),
    ('TP-1003', 'Patricia Kowalski',   '333-44-5555', 'Individual', 'TX', 3, '2025-01-01T00:00:00'),
    ('TP-1004', 'James Rivera',        '444-55-6666', 'Individual', 'FL', 1, '2025-01-01T00:00:00'),
    ('TP-1005', 'Barbara Nguyen',      '555-66-7777', 'Individual', 'NY', 0, '2025-01-01T00:00:00'),
    ('TP-1006', 'Robert Thompson',     '666-77-8888', 'Individual', 'CA', 4, '2025-01-01T00:00:00'),
    ('TP-1007', 'Linda Petrov',        '777-88-9999', 'Individual', 'NY', 1, '2025-01-01T00:00:00'),
    ('TP-1008', 'Michael Garcia',      '888-99-0000', 'Individual', 'TX', 2, '2025-01-01T00:00:00'),
    ('TP-1009', 'Jennifer Adams',      '999-00-1111', 'Individual', 'FL', 0, '2025-01-01T00:00:00'),
    ('TP-1010', 'Richard Kim',         '100-11-2222', 'Individual', 'CA', 1, '2025-01-01T00:00:00'),
    ('TP-2001', 'Henderson & Co LLC',  '200-22-3333', 'Business',   'NY', 0, '2025-01-01T00:00:00'),
    ('TP-2002', 'Pacific Ventures Inc','300-33-4444', 'Business',   'CA', 0, '2025-01-01T00:00:00'),
    ('TP-2003', 'Lone Star Holdings', '400-44-5555', 'Business',   'TX', 0, '2025-01-01T00:00:00'),
    ('TP-2004', 'Sunshine Realty Corp','500-55-6666', 'Business',   'FL', 0, '2025-01-01T00:00:00'),
    ('TP-2005', 'Empire Tech LLC',     '600-66-7777', 'Business',   'NY', 0, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_taxpayers;


-- ===================== BRONZE SEED DATA: FILINGS (55 rows) =====================
-- 3 fiscal years (2022, 2023, 2024), various statuses, amended filings, audit candidates

INSERT INTO {{zone_prefix}}.bronze.raw_filings VALUES
    -- FY 2022 filings
    ('FIL-2022-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2022, '2023-03-15', 95000.00,  18500.00,  76500.00,  12870.00,  14200.00,  1330.00,  'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-002', 'TP-1001', 'JUR-NY',  'PREP-01', 2022, '2023-03-15', 95000.00,  8000.00,   87000.00,  5959.50,   6200.00,   240.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-003', 'TP-1002', 'JUR-FED', 'PREP-02', 2022, '2023-04-10', 142000.00, 62000.00,  80000.00,  13460.00,  13460.00,  0.00,     'Accepted',    false, 'High deductions flagged', '2025-01-01T00:00:00'),
    ('FIL-2022-004', 'TP-1002', 'JUR-CA',  'PREP-02', 2022, '2023-04-10', 142000.00, 5202.00,   136798.00, 12722.21,  13000.00,  277.79,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-005', 'TP-1003', 'JUR-FED', 'PREP-01', 2022, '2023-02-28', 68000.00,  13850.00,  54150.00,  7348.50,   8000.00,   651.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-006', 'TP-1004', 'JUR-FED', 'PREP-04', 2022, '2023-04-14', 55000.00,  13850.00,  41150.00,  4810.50,   5200.00,   389.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-007', 'TP-1005', 'JUR-FED', 'PREP-03', 2022, '2023-03-01', 210000.00, 95000.00,  115000.00, 25300.00,  22000.00,  -3300.00, 'Audited',     false, 'Deductions > 45% of income', '2025-01-01T00:00:00'),
    ('FIL-2022-008', 'TP-1005', 'JUR-NY',  'PREP-03', 2022, '2023-03-01', 210000.00, 8000.00,   202000.00, 13837.00,  14000.00,  163.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-009', 'TP-1006', 'JUR-FED', 'PREP-02', 2022, '2023-04-15', 78000.00,  35000.00,  43000.00,  5110.00,   5500.00,   390.00,   'Accepted',    false, 'Charitable donations high', '2025-01-01T00:00:00'),
    ('FIL-2022-010', 'TP-1006', 'JUR-CA',  'PREP-02', 2022, '2023-04-15', 78000.00,  5202.00,   72798.00,  6770.21,   7000.00,   229.79,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-011', 'TP-2001', 'JUR-FED', 'PREP-03', 2022, '2023-03-15', 850000.00, 320000.00, 530000.00, 116600.00, 120000.00, 3400.00,  'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-012', 'TP-2001', 'JUR-NY',  'PREP-03', 2022, '2023-03-15', 850000.00, 8000.00,   842000.00, 57677.00,  58000.00,  323.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-013', 'TP-2002', 'JUR-FED', 'PREP-03', 2022, '2023-04-01', 1200000.00,550000.00, 650000.00, 143000.00, 140000.00, -3000.00, 'Under Review',false, 'High revenue corp under review', '2025-01-01T00:00:00'),
    ('FIL-2022-014', 'TP-2003', 'JUR-FED', 'PREP-01', 2022, '2023-03-20', 420000.00, 180000.00, 240000.00, 52800.00,  53000.00,  200.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-015', 'TP-1007', 'JUR-FED', 'PREP-04', 2022, '2023-04-15', 62000.00,  13850.00,  48150.00,  5879.50,   6100.00,   220.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-016', 'TP-1008', 'JUR-FED', 'PREP-01', 2022, '2023-03-10', 73000.00,  13850.00,  59150.00,  7707.50,   8000.00,   292.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-017', 'TP-1009', 'JUR-FED', 'PREP-04', 2022, '2023-04-12', 48000.00,  13850.00,  34150.00,  3646.50,   4000.00,   353.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-018', 'TP-1010', 'JUR-FED', 'PREP-02', 2022, '2023-04-05', 115000.00, 52000.00,  63000.00,  10010.00,  10500.00,  490.00,   'Accepted',    false, 'Deductions 45% of income', '2025-01-01T00:00:00'),
    ('FIL-2022-019', 'TP-1010', 'JUR-CA',  'PREP-02', 2022, '2023-04-05', 115000.00, 5202.00,   109798.00, 10211.21,  10500.00,  288.79,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    -- FY 2023 filings
    ('FIL-2023-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2023, '2024-03-20', 102000.00, 19200.00,  82800.00,  14256.00,  15000.00,  744.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-002', 'TP-1001', 'JUR-NY',  'PREP-01', 2023, '2024-03-20', 102000.00, 8000.00,   94000.00,  6439.00,   6800.00,   361.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-003', 'TP-1002', 'JUR-FED', 'PREP-02', 2023, '2024-04-08', 155000.00, 68000.00,  87000.00,  14870.00,  15000.00,  130.00,   'Under Review',false, 'Deductions > 43%', '2025-01-01T00:00:00'),
    ('FIL-2023-004', 'TP-1003', 'JUR-FED', 'PREP-01', 2023, '2024-02-15', 72000.00,  13850.00,  58150.00,  7773.50,   8500.00,   726.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-005', 'TP-1004', 'JUR-FED', 'PREP-04', 2023, '2024-04-15', 58000.00,  27000.00,  31000.00,  3410.00,   3800.00,   390.00,   'Accepted',    false, 'High deduction ratio for income', '2025-01-01T00:00:00'),
    ('FIL-2023-006', 'TP-1005', 'JUR-FED', 'PREP-03', 2023, '2024-03-10', 225000.00, 98000.00,  127000.00, 27940.00,  25000.00,  -2940.00, 'Audited',     false, 'Repeat high deductions', '2025-01-01T00:00:00'),
    ('FIL-2023-007', 'TP-1006', 'JUR-FED', 'PREP-02', 2023, '2024-04-10', 82000.00,  36500.00,  45500.00,  5610.00,   6000.00,   390.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-008', 'TP-2001', 'JUR-FED', 'PREP-03', 2023, '2024-03-15', 920000.00, 345000.00, 575000.00, 126500.00, 130000.00, 3500.00,  'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-009', 'TP-2002', 'JUR-FED', 'PREP-03', 2023, '2024-04-01', 1350000.00,620000.00, 730000.00, 160600.00, 155000.00, -5600.00, 'Under Review',false, 'Large corp audit queue', '2025-01-01T00:00:00'),
    ('FIL-2023-010', 'TP-2003', 'JUR-FED', 'PREP-01', 2023, '2024-03-18', 460000.00, 195000.00, 265000.00, 58300.00,  58500.00,  200.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-011', 'TP-2004', 'JUR-FED', 'PREP-01', 2023, '2024-04-05', 380000.00, 160000.00, 220000.00, 48400.00,  48400.00,  0.00,     'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-012', 'TP-1007', 'JUR-FED', 'PREP-04', 2023, '2024-04-14', 65000.00,  13850.00,  51150.00,  6379.50,   6800.00,   420.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-013', 'TP-1008', 'JUR-FED', 'PREP-01', 2023, '2024-03-12', 76000.00,  13850.00,  62150.00,  8207.50,   8500.00,   292.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-014', 'TP-1009', 'JUR-FED', 'PREP-04', 2023, '2024-04-10', 51000.00,  13850.00,  37150.00,  4146.50,   4500.00,   353.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-015', 'TP-2005', 'JUR-FED', 'PREP-03', 2023, '2024-03-25', 680000.00, 290000.00, 390000.00, 85800.00,  86000.00,  200.00,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    -- FY 2024 filings (some still in progress)
    ('FIL-2024-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2024, '2025-02-20', 108000.00, 20100.00,  87900.00,  15378.00,  15378.00,  0.00,     'Filed',       false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-002', 'TP-1002', 'JUR-FED', 'PREP-02', 2024, '2025-03-05', 160000.00, 72000.00,  88000.00,  15160.00,  15200.00,  40.00,    'Filed',       false, 'Deductions 45% again', '2025-01-01T00:00:00'),
    ('FIL-2024-003', 'TP-1003', 'JUR-FED', 'PREP-01', 2024, '2025-02-10', 75000.00,  13850.00,  61150.00,  8240.50,   8500.00,   259.50,   'Accepted',    false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-004', 'TP-1005', 'JUR-FED', 'PREP-03', 2024, '2025-03-01', 240000.00, 105000.00, 135000.00, 29700.00,  27000.00,  -2700.00, 'Filed',       false, 'Third year high deductions', '2025-01-01T00:00:00'),
    ('FIL-2024-005', 'TP-2001', 'JUR-FED', 'PREP-03', 2024, '2025-03-10', 980000.00, 370000.00, 610000.00, 134200.00, 135000.00, 800.00,   'Filed',       false, NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-006', 'TP-2002', 'JUR-FED', 'PREP-03', 2024, '2025-03-15', 1420000.00,680000.00, 740000.00, 162800.00, 160000.00, -2800.00, 'Filed',       false, NULL, '2025-01-01T00:00:00'),
    -- Amended filing
    ('FIL-2023-003A','TP-1002', 'JUR-FED', 'PREP-02', 2023, '2024-09-15', 155000.00, 55000.00,  100000.00, 18000.00,  15000.00,  -3000.00, 'Amended',     true,  'Reduced deductions after review', '2025-01-01T00:00:00'),
    ('FIL-2022-005A','TP-1005', 'JUR-FED', 'PREP-03', 2022, '2023-10-20', 210000.00, 78000.00,  132000.00, 29040.00,  22000.00,  -7040.00, 'Amended',     true,  'Post-audit amended return', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 42
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_filings;


-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.taxpayer_enriched (ssn) TRANSFORM redact PARAMS (mask = '[REDACTED]');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.taxpayer_enriched (taxpayer_name) TRANSFORM mask PARAMS (show = 1);
