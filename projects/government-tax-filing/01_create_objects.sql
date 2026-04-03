-- =============================================================================
-- Government Tax Filing Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: Append-only immutable filings, separate amendment tracking, CDF-driven
-- audit trail, partitioning by fiscal_year, pseudonymisation (REDACT SSN, MASK name),
-- bloom filter on taxpayer_id, star schema with amendment-aware fact table
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Government tax filing project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw tax filing source data — filings, amendments, taxpayers, jurisdictions, preparers';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Immutable filings, applied amendments, taxpayer profiles, CDF audit trail';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Filing star schema with amendment-aware facts, revenue and preparer KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_filings (
    filing_id           STRING      NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    jurisdiction_id     STRING      NOT NULL,
    preparer_id         STRING,
    fiscal_year         INT         NOT NULL,
    filing_date         STRING      NOT NULL,
    gross_income        DECIMAL(14,2),
    deductions          DECIMAL(14,2),
    taxable_income      DECIMAL(14,2),
    tax_owed            DECIMAL(14,2),
    tax_paid            DECIMAL(14,2),
    refund_amount       DECIMAL(14,2),
    filing_status       STRING,
    filing_type         STRING,
    notes               STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_filings';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_filings TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_amendments (
    amendment_id        STRING      NOT NULL,
    original_filing_id  STRING      NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    amendment_date      STRING      NOT NULL,
    amended_gross_income    DECIMAL(14,2),
    amended_deductions      DECIMAL(14,2),
    amended_taxable_income  DECIMAL(14,2),
    amended_tax_owed        DECIMAL(14,2),
    amendment_reason    STRING,
    preparer_id         STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_amendments';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_amendments TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_taxpayers (
    taxpayer_id         STRING      NOT NULL,
    taxpayer_name       STRING,
    ssn                 STRING,
    filing_type         STRING,
    state               STRING,
    dependent_count     INT,
    annual_income       DECIMAL(14,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/filing/raw_taxpayers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_taxpayers TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_jurisdictions (
    jurisdiction_id     STRING      NOT NULL,
    jurisdiction_name   STRING      NOT NULL,
    jurisdiction_level  STRING,
    state               STRING,
    base_tax_rate       DECIMAL(5,4),
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

-- ===================== SILVER TABLES =====================

-- Append-only: original filings are NEVER updated or deleted.
-- CDF enabled so every INSERT is captured for the audit_trail.
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.filings_immutable (
    filing_id           STRING      NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    jurisdiction_id     STRING      NOT NULL,
    preparer_id         STRING,
    fiscal_year         INT         NOT NULL,
    filing_date         DATE        NOT NULL,
    gross_income        DECIMAL(14,2),
    deductions          DECIMAL(14,2),
    taxable_income      DECIMAL(14,2),
    tax_owed            DECIMAL(14,2),
    tax_paid            DECIMAL(14,2),
    refund_amount       DECIMAL(14,2),
    filing_status       STRING,
    filing_type         STRING,
    effective_tax_rate  DECIMAL(7,4),
    deduction_pct       DECIMAL(5,2),
    audit_flag          BOOLEAN,
    income_bracket      STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/filing/filings_immutable'
PARTITIONED BY (fiscal_year)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.filings_immutable TO USER {{current_user}};

-- Amendments linked to originals with computed deltas
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.amendments_applied (
    amendment_id            STRING      NOT NULL,
    original_filing_id      STRING      NOT NULL,
    taxpayer_id             STRING      NOT NULL,
    amendment_date          DATE        NOT NULL,
    original_taxable_income DECIMAL(14,2),
    amended_taxable_income  DECIMAL(14,2),
    income_delta            DECIMAL(14,2),
    original_tax_owed       DECIMAL(14,2),
    amended_tax_owed        DECIMAL(14,2),
    tax_delta               DECIMAL(14,2),
    amendment_reason        STRING,
    delta_pct               DECIMAL(7,2),
    large_delta_flag        BOOLEAN,
    preparer_id             STRING,
    loaded_at               TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/filing/amendments_applied'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.amendments_applied TO USER {{current_user}};

-- Aggregated taxpayer profile from filings
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.taxpayer_profiles (
    taxpayer_id         STRING      NOT NULL,
    taxpayer_name       STRING,
    ssn                 STRING,
    filing_type         STRING,
    state               STRING,
    dependent_count     INT,
    total_filings       INT,
    total_amendments    INT,
    lifetime_gross_income   DECIMAL(14,2),
    lifetime_tax_paid       DECIMAL(14,2),
    latest_income_bracket   STRING,
    avg_deduction_pct       DECIMAL(5,2),
    ever_audited            BOOLEAN
) LOCATION '{{data_path}}/silver/filing/taxpayer_profiles';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.taxpayer_profiles TO USER {{current_user}};

-- CDF-driven audit trail — captures every change to filings_immutable and amendments
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.audit_trail (
    audit_id            INT         NOT NULL,
    table_name          STRING      NOT NULL,
    operation           STRING      NOT NULL,
    record_key          STRING      NOT NULL,
    taxpayer_id         STRING,
    fiscal_year         INT,
    change_timestamp    TIMESTAMP   NOT NULL,
    details             STRING
) LOCATION '{{data_path}}/silver/filing/audit_trail';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.audit_trail TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_taxpayer (
    taxpayer_key        INT         NOT NULL,
    taxpayer_id         STRING      NOT NULL,
    filing_type         STRING,
    state               STRING,
    income_bracket      STRING,
    dependent_count     INT,
    total_filings       INT,
    ever_audited        BOOLEAN
) LOCATION '{{data_path}}/gold/filing/dim_taxpayer';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_taxpayer TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_jurisdiction (
    jurisdiction_key    INT         NOT NULL,
    jurisdiction_id     STRING      NOT NULL,
    jurisdiction_name   STRING      NOT NULL,
    jurisdiction_level  STRING,
    state               STRING,
    base_tax_rate       DECIMAL(5,4),
    standard_deduction  DECIMAL(10,2)
) LOCATION '{{data_path}}/gold/filing/dim_jurisdiction';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_jurisdiction TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_preparer (
    preparer_key        INT         NOT NULL,
    preparer_id         STRING      NOT NULL,
    preparer_name       STRING      NOT NULL,
    firm                STRING,
    certification       STRING,
    years_experience    INT,
    total_filings_prepared  INT,
    amendment_rate      DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/filing/dim_preparer';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_preparer TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_fiscal_year (
    fiscal_year_key     INT         NOT NULL,
    fiscal_year         INT         NOT NULL,
    filing_deadline     DATE,
    total_filings       INT,
    total_amendments    INT,
    audit_flag_count    INT
) LOCATION '{{data_path}}/gold/filing/dim_fiscal_year';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_fiscal_year TO USER {{current_user}};

-- Star-schema fact: joins originals + amendments via COALESCE
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_filings (
    filing_key              INT         NOT NULL,
    taxpayer_key            INT         NOT NULL,
    jurisdiction_key        INT         NOT NULL,
    preparer_key            INT,
    fiscal_year_key         INT         NOT NULL,
    fiscal_year             INT,
    filing_date             DATE,
    gross_income            DECIMAL(14,2),
    deductions              DECIMAL(14,2),
    taxable_income          DECIMAL(14,2),
    tax_owed                DECIMAL(14,2),
    tax_paid                DECIMAL(14,2),
    refund_amount           DECIMAL(14,2),
    effective_taxable_income DECIMAL(14,2),
    effective_tax_owed       DECIMAL(14,2),
    was_amended             BOOLEAN,
    amendment_reason        STRING,
    amendment_count         INT,
    effective_tax_rate      DECIMAL(7,4),
    audit_flag              BOOLEAN,
    filing_status           STRING
) LOCATION '{{data_path}}/gold/filing/fact_filings';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_filings TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_revenue_analysis (
    fiscal_year         INT         NOT NULL,
    jurisdiction_name   STRING      NOT NULL,
    total_filings       INT,
    total_taxable_income DECIMAL(16,2),
    total_tax_collected DECIMAL(16,2),
    total_refunds       DECIMAL(16,2),
    avg_effective_rate  DECIMAL(7,4),
    audit_flag_count    INT,
    compliance_rate     DECIMAL(5,2),
    amendment_count     INT,
    audit_yield_pct     DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/filing/kpi_revenue_analysis';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_revenue_analysis TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_preparer_quality (
    preparer_name       STRING      NOT NULL,
    firm                STRING,
    certification       STRING,
    total_filings       INT,
    amendment_count     INT,
    amendment_rate_pct  DECIMAL(5,2),
    audit_count         INT,
    audit_rate_pct      DECIMAL(5,2),
    avg_client_income   DECIMAL(14,2),
    avg_deduction_pct   DECIMAL(5,2),
    avg_effective_rate  DECIMAL(7,4)
) LOCATION '{{data_path}}/gold/filing/kpi_preparer_quality';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_preparer_quality TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: JURISDICTIONS (6 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_jurisdictions VALUES
    ('JUR-FED', 'Federal - IRS',           'Federal', NULL, 0.2200, 13850.00, '2025-01-01T00:00:00'),
    ('JUR-NY',  'New York State',          'State',   'NY', 0.0685, 8000.00,  '2025-01-01T00:00:00'),
    ('JUR-CA',  'California FTB',          'State',   'CA', 0.0930, 5202.00,  '2025-01-01T00:00:00'),
    ('JUR-TX',  'Texas Comptroller',       'State',   'TX', 0.0000, 0.00,     '2025-01-01T00:00:00'),
    ('JUR-FL',  'Florida Dept of Revenue', 'State',   'FL', 0.0000, 0.00,     '2025-01-01T00:00:00'),
    ('JUR-IL',  'Illinois Dept of Revenue','State',   'IL', 0.0495, 2425.00,  '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_jurisdictions;

-- ===================== BRONZE SEED DATA: PREPARERS (5 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_preparers VALUES
    ('PREP-01', 'Sarah Mitchell CPA',   'Mitchell & Associates',   'CPA',  12, '2025-01-01T00:00:00'),
    ('PREP-02', 'David Park EA',        'TaxPro Services',         'EA',    8, '2025-01-01T00:00:00'),
    ('PREP-03', 'Jennifer Wu CPA',      'Big Four Accounting LLP', 'CPA',  20, '2025-01-01T00:00:00'),
    ('PREP-04', 'Marcus Brown',         'QuickTax Online',         'RTRP',  3, '2025-01-01T00:00:00'),
    ('PREP-05', 'Angela Torres EA',     'Torres Tax Group',        'EA',   10, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_preparers;

-- ===================== BRONZE SEED DATA: TAXPAYERS (20 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_taxpayers VALUES
    ('TP-1001', 'Margaret Henderson',   '111-22-3333', 'Individual', 'NY', 2, 98000.00,   '2025-01-01T00:00:00'),
    ('TP-1002', 'William Chang',        '222-33-4444', 'Individual', 'CA', 0, 155000.00,  '2025-01-01T00:00:00'),
    ('TP-1003', 'Patricia Kowalski',    '333-44-5555', 'Individual', 'TX', 3, 70000.00,   '2025-01-01T00:00:00'),
    ('TP-1004', 'James Rivera',         '444-55-6666', 'Individual', 'FL', 1, 57000.00,   '2025-01-01T00:00:00'),
    ('TP-1005', 'Barbara Nguyen',       '555-66-7777', 'Individual', 'NY', 0, 225000.00,  '2025-01-01T00:00:00'),
    ('TP-1006', 'Robert Thompson',      '666-77-8888', 'Individual', 'CA', 4, 80000.00,   '2025-01-01T00:00:00'),
    ('TP-1007', 'Linda Petrov',         '777-88-9999', 'Individual', 'NY', 1, 65000.00,   '2025-01-01T00:00:00'),
    ('TP-1008', 'Michael Garcia',       '888-99-0000', 'Individual', 'TX', 2, 76000.00,   '2025-01-01T00:00:00'),
    ('TP-1009', 'Jennifer Adams',       '999-00-1111', 'Individual', 'FL', 0, 50000.00,   '2025-01-01T00:00:00'),
    ('TP-1010', 'Richard Kim',          '100-11-2222', 'Individual', 'CA', 1, 118000.00,  '2025-01-01T00:00:00'),
    ('TP-1011', 'Susan O''Brien',       '110-22-3344', 'Individual', 'IL', 2, 92000.00,   '2025-01-01T00:00:00'),
    ('TP-1012', 'Daniel Vasquez',       '120-33-4455', 'Individual', 'IL', 0, 145000.00,  '2025-01-01T00:00:00'),
    ('TP-2001', 'Henderson & Co LLC',   '200-22-3333', 'Business',   'NY', 0, 860000.00,  '2025-01-01T00:00:00'),
    ('TP-2002', 'Pacific Ventures Inc', '300-33-4444', 'Business',   'CA', 0, 1250000.00, '2025-01-01T00:00:00'),
    ('TP-2003', 'Lone Star Holdings',   '400-44-5555', 'Business',   'TX', 0, 440000.00,  '2025-01-01T00:00:00'),
    ('TP-2004', 'Sunshine Realty Corp', '500-55-6666', 'Business',   'FL', 0, 385000.00,  '2025-01-01T00:00:00'),
    ('TP-2005', 'Empire Tech LLC',      '600-66-7777', 'Business',   'NY', 0, 700000.00,  '2025-01-01T00:00:00'),
    ('TP-2006', 'Midwest Mfg Inc',     '700-77-8888', 'Business',   'IL', 0, 520000.00,  '2025-01-01T00:00:00'),
    ('TP-1013', 'Karen Whitfield',      '130-44-5566', 'Individual', 'TX', 1, 63000.00,   '2025-01-01T00:00:00'),
    ('TP-1014', 'Anthony Russo',        '140-55-6677', 'Individual', 'FL', 0, 88000.00,   '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_taxpayers;

-- ===================== BRONZE SEED DATA: FILINGS (50 rows) =====================
-- 20 FY2022, 18 FY2023, 12 FY2024.  filing_type: 1040/1120/1040X etc.
-- 4 audit-flagged filings: deductions > 40% of gross (TP-1002, TP-1005, TP-1010, TP-1006)

INSERT INTO {{zone_prefix}}.bronze.raw_filings VALUES
    -- ===== FY 2022 (20 filings) =====
    ('FIL-2022-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2022, '2023-03-15', 95000.00,  18500.00,  76500.00,  12870.00, 14200.00,  1330.00,  'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-002', 'TP-1001', 'JUR-NY',  'PREP-01', 2022, '2023-03-15', 95000.00,  8000.00,   87000.00,  5959.50,  6200.00,   240.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-003', 'TP-1002', 'JUR-FED', 'PREP-02', 2022, '2023-04-10', 142000.00, 62000.00,  80000.00,  13460.00, 13460.00,  0.00,     'Accepted', '1040',  'High deductions flagged', '2025-01-01T00:00:00'),
    ('FIL-2022-004', 'TP-1002', 'JUR-CA',  'PREP-02', 2022, '2023-04-10', 142000.00, 5202.00,   136798.00, 12722.21, 13000.00,  277.79,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-005', 'TP-1003', 'JUR-FED', 'PREP-01', 2022, '2023-02-28', 68000.00,  13850.00,  54150.00,  7348.50,  8000.00,   651.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-006', 'TP-1004', 'JUR-FED', 'PREP-04', 2022, '2023-04-14', 55000.00,  13850.00,  41150.00,  4810.50,  5200.00,   389.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-007', 'TP-1005', 'JUR-FED', 'PREP-03', 2022, '2023-03-01', 210000.00, 95000.00,  115000.00, 25300.00, 22000.00,  -3300.00, 'Audited',  '1040',  'Deductions > 45% of income', '2025-01-01T00:00:00'),
    ('FIL-2022-008', 'TP-1005', 'JUR-NY',  'PREP-03', 2022, '2023-03-01', 210000.00, 8000.00,   202000.00, 13837.00, 14000.00,  163.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-009', 'TP-1006', 'JUR-FED', 'PREP-02', 2022, '2023-04-15', 78000.00,  35000.00,  43000.00,  5110.00,  5500.00,   390.00,   'Accepted', '1040',  'Charitable donations high', '2025-01-01T00:00:00'),
    ('FIL-2022-010', 'TP-1006', 'JUR-CA',  'PREP-02', 2022, '2023-04-15', 78000.00,  5202.00,   72798.00,  6770.21,  7000.00,   229.79,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-011', 'TP-2001', 'JUR-FED', 'PREP-03', 2022, '2023-03-15', 850000.00, 320000.00, 530000.00, 116600.00,120000.00, 3400.00,  'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-012', 'TP-2001', 'JUR-NY',  'PREP-03', 2022, '2023-03-15', 850000.00, 8000.00,   842000.00, 57677.00, 58000.00,  323.00,   'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-013', 'TP-2002', 'JUR-FED', 'PREP-03', 2022, '2023-04-01', 1200000.00,550000.00, 650000.00, 143000.00,140000.00, -3000.00, 'Under Review','1120','Large corp under review', '2025-01-01T00:00:00'),
    ('FIL-2022-014', 'TP-2003', 'JUR-FED', 'PREP-01', 2022, '2023-03-20', 420000.00, 180000.00, 240000.00, 52800.00, 53000.00,  200.00,   'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-015', 'TP-1007', 'JUR-FED', 'PREP-04', 2022, '2023-04-15', 62000.00,  13850.00,  48150.00,  5879.50,  6100.00,   220.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-016', 'TP-1008', 'JUR-FED', 'PREP-01', 2022, '2023-03-10', 73000.00,  13850.00,  59150.00,  7707.50,  8000.00,   292.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-017', 'TP-1009', 'JUR-FED', 'PREP-04', 2022, '2023-04-12', 48000.00,  13850.00,  34150.00,  3646.50,  4000.00,   353.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-018', 'TP-1010', 'JUR-FED', 'PREP-02', 2022, '2023-04-05', 115000.00, 52000.00,  63000.00,  10010.00, 10500.00,  490.00,   'Accepted', '1040',  'Deductions 45% of income', '2025-01-01T00:00:00'),
    ('FIL-2022-019', 'TP-1010', 'JUR-CA',  'PREP-02', 2022, '2023-04-05', 115000.00, 5202.00,   109798.00, 10211.21, 10500.00,  288.79,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2022-020', 'TP-1011', 'JUR-FED', 'PREP-05', 2022, '2023-03-22', 90000.00,  18000.00,  72000.00,  11160.00, 11500.00,  340.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    -- ===== FY 2023 (18 filings) =====
    ('FIL-2023-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2023, '2024-03-20', 102000.00, 19200.00,  82800.00,  14256.00, 15000.00,  744.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-002', 'TP-1001', 'JUR-NY',  'PREP-01', 2023, '2024-03-20', 102000.00, 8000.00,   94000.00,  6439.00,  6800.00,   361.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-003', 'TP-1002', 'JUR-FED', 'PREP-02', 2023, '2024-04-08', 155000.00, 68000.00,  87000.00,  14870.00, 15000.00,  130.00,   'Under Review','1040','Deductions > 43%', '2025-01-01T00:00:00'),
    ('FIL-2023-004', 'TP-1003', 'JUR-FED', 'PREP-01', 2023, '2024-02-15', 72000.00,  13850.00,  58150.00,  7773.50,  8500.00,   726.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-005', 'TP-1004', 'JUR-FED', 'PREP-04', 2023, '2024-04-15', 58000.00,  27000.00,  31000.00,  3410.00,  3800.00,   390.00,   'Accepted', '1040',  'High deduction ratio for income', '2025-01-01T00:00:00'),
    ('FIL-2023-006', 'TP-1005', 'JUR-FED', 'PREP-03', 2023, '2024-03-10', 225000.00, 98000.00,  127000.00, 27940.00, 25000.00,  -2940.00, 'Audited',  '1040',  'Repeat high deductions', '2025-01-01T00:00:00'),
    ('FIL-2023-007', 'TP-1006', 'JUR-FED', 'PREP-02', 2023, '2024-04-10', 82000.00,  36500.00,  45500.00,  5610.00,  6000.00,   390.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-008', 'TP-2001', 'JUR-FED', 'PREP-03', 2023, '2024-03-15', 920000.00, 345000.00, 575000.00, 126500.00,130000.00, 3500.00,  'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-009', 'TP-2002', 'JUR-FED', 'PREP-03', 2023, '2024-04-01', 1350000.00,620000.00, 730000.00, 160600.00,155000.00, -5600.00, 'Under Review','1120','Large corp audit queue', '2025-01-01T00:00:00'),
    ('FIL-2023-010', 'TP-2003', 'JUR-FED', 'PREP-01', 2023, '2024-03-18', 460000.00, 195000.00, 265000.00, 58300.00, 58500.00,  200.00,   'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-011', 'TP-2004', 'JUR-FED', 'PREP-05', 2023, '2024-04-05', 380000.00, 160000.00, 220000.00, 48400.00, 48400.00,  0.00,     'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-012', 'TP-1007', 'JUR-FED', 'PREP-04', 2023, '2024-04-14', 65000.00,  13850.00,  51150.00,  6379.50,  6800.00,   420.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-013', 'TP-1008', 'JUR-FED', 'PREP-01', 2023, '2024-03-12', 76000.00,  13850.00,  62150.00,  8207.50,  8500.00,   292.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-014', 'TP-1009', 'JUR-FED', 'PREP-04', 2023, '2024-04-10', 51000.00,  13850.00,  37150.00,  4146.50,  4500.00,   353.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-015', 'TP-2005', 'JUR-FED', 'PREP-03', 2023, '2024-03-25', 680000.00, 290000.00, 390000.00, 85800.00, 86000.00,  200.00,   'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-016', 'TP-1012', 'JUR-FED', 'PREP-05', 2023, '2024-03-30', 145000.00, 62000.00,  83000.00,  14260.00, 14500.00,  240.00,   'Accepted', '1040',  'Deductions 42%', '2025-01-01T00:00:00'),
    ('FIL-2023-017', 'TP-2006', 'JUR-FED', 'PREP-05', 2023, '2024-04-01', 520000.00, 220000.00, 300000.00, 66000.00, 66000.00,  0.00,     'Accepted', '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2023-018', 'TP-1011', 'JUR-FED', 'PREP-05', 2023, '2024-03-18', 94000.00,  18500.00,  75500.00,  11825.00, 12000.00,  175.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    -- ===== FY 2024 (12 filings) =====
    ('FIL-2024-001', 'TP-1001', 'JUR-FED', 'PREP-01', 2024, '2025-02-20', 108000.00, 20100.00,  87900.00,  15378.00, 15378.00,  0.00,     'Filed',    '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-002', 'TP-1002', 'JUR-FED', 'PREP-02', 2024, '2025-03-05', 160000.00, 72000.00,  88000.00,  15160.00, 15200.00,  40.00,    'Filed',    '1040',  'Deductions 45% again', '2025-01-01T00:00:00'),
    ('FIL-2024-003', 'TP-1003', 'JUR-FED', 'PREP-01', 2024, '2025-02-10', 75000.00,  13850.00,  61150.00,  8240.50,  8500.00,   259.50,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-004', 'TP-1005', 'JUR-FED', 'PREP-03', 2024, '2025-03-01', 240000.00, 105000.00, 135000.00, 29700.00, 27000.00,  -2700.00, 'Filed',    '1040',  'Third year high deductions', '2025-01-01T00:00:00'),
    ('FIL-2024-005', 'TP-2001', 'JUR-FED', 'PREP-03', 2024, '2025-03-10', 980000.00, 370000.00, 610000.00, 134200.00,135000.00, 800.00,   'Filed',    '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-006', 'TP-2002', 'JUR-FED', 'PREP-03', 2024, '2025-03-15', 1420000.00,680000.00, 740000.00, 162800.00,160000.00, -2800.00, 'Filed',    '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-007', 'TP-1004', 'JUR-FED', 'PREP-04', 2024, '2025-03-28', 61000.00,  13850.00,  47150.00,  5586.50,  5800.00,   213.50,   'Filed',    '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-008', 'TP-1013', 'JUR-FED', 'PREP-04', 2024, '2025-03-20', 63000.00,  13850.00,  49150.00,  5919.50,  6100.00,   180.50,   'Filed',    '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-009', 'TP-1014', 'JUR-FED', 'PREP-05', 2024, '2025-03-22', 88000.00,  17600.00,  70400.00,  10912.00, 11200.00,  288.00,   'Accepted', '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-010', 'TP-2006', 'JUR-FED', 'PREP-05', 2024, '2025-03-25', 545000.00, 230000.00, 315000.00, 69300.00, 69500.00,  200.00,   'Filed',    '1120',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-011', 'TP-1011', 'JUR-FED', 'PREP-05', 2024, '2025-03-15', 97000.00,  19000.00,  78000.00,  12180.00, 12500.00,  320.00,   'Filed',    '1040',  NULL, '2025-01-01T00:00:00'),
    ('FIL-2024-012', 'TP-1012', 'JUR-FED', 'PREP-05', 2024, '2025-04-01', 150000.00, 65000.00,  85000.00,  14650.00, 15000.00,  350.00,   'Filed',    '1040',  'Deductions 43%', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 50
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_filings;

-- ===================== BRONZE SEED DATA: AMENDMENTS (12 rows) =====================
-- Each amendment references an original filing_id. Includes 2 high-delta amendments.

INSERT INTO {{zone_prefix}}.bronze.raw_amendments VALUES
    ('AMD-001', 'FIL-2022-003', 'TP-1002', '2023-09-10', 142000.00, 48000.00, 94000.00,  16920.00, 'Deductions reduced after audit inquiry',       'PREP-02', '2025-01-01T00:00:00'),
    ('AMD-002', 'FIL-2022-007', 'TP-1005', '2023-10-20', 210000.00, 78000.00, 132000.00, 29040.00, 'Post-audit amended return',                    'PREP-03', '2025-01-01T00:00:00'),
    ('AMD-003', 'FIL-2022-009', 'TP-1006', '2023-11-05', 78000.00,  28000.00, 50000.00,  6300.00,  'Charitable deductions partially disallowed',    'PREP-02', '2025-01-01T00:00:00'),
    ('AMD-004', 'FIL-2022-018', 'TP-1010', '2023-12-15', 115000.00, 42000.00, 73000.00,  11680.00, 'Investment loss claim adjusted',                'PREP-02', '2025-01-01T00:00:00'),
    ('AMD-005', 'FIL-2022-013', 'TP-2002', '2023-08-20', 1200000.00,510000.00,690000.00, 151800.00,'Corporate deduction reclassification',          'PREP-03', '2025-01-01T00:00:00'),
    ('AMD-006', 'FIL-2023-003', 'TP-1002', '2024-09-15', 155000.00, 55000.00, 100000.00, 18000.00, 'Deductions reduced after second review',        'PREP-02', '2025-01-01T00:00:00'),
    ('AMD-007', 'FIL-2023-005', 'TP-1004', '2024-10-01', 58000.00,  20000.00, 38000.00,  4560.00,  'Overstated home office deduction corrected',    'PREP-04', '2025-01-01T00:00:00'),
    ('AMD-008', 'FIL-2023-006', 'TP-1005', '2024-11-10', 225000.00, 82000.00, 143000.00, 31460.00, 'Third consecutive audit amendment',             'PREP-03', '2025-01-01T00:00:00'),
    ('AMD-009', 'FIL-2023-009', 'TP-2002', '2024-08-05', 1350000.00,580000.00,770000.00, 169400.00,'Large corp deduction correction',               'PREP-03', '2025-01-01T00:00:00'),
    ('AMD-010', 'FIL-2023-007', 'TP-1006', '2024-12-01', 82000.00,  30000.00, 52000.00,  6500.00,  'Charitable donation receipt issues',            'PREP-02', '2025-01-01T00:00:00'),
    ('AMD-011', 'FIL-2022-006', 'TP-1004', '2024-01-15', 55000.00,  13850.00, 41150.00,  4950.00,  'Minor math correction on original',             'PREP-04', '2025-01-01T00:00:00'),
    ('AMD-012', 'FIL-2023-016', 'TP-1012', '2025-01-10', 145000.00, 52000.00, 93000.00,  16120.00, 'Investment loss reclassified',                  'PREP-05', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_amendments;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.taxpayer_profiles (ssn) TRANSFORM redact PARAMS (mask = '[REDACTED]');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.taxpayer_profiles (taxpayer_name) TRANSFORM mask PARAMS (show = 1);

-- ===================== BLOOM FILTER INDEX =====================

CREATE BLOOMFILTER INDEX ON {{zone_prefix}}.silver.filings_immutable FOR COLUMNS (taxpayer_id);
