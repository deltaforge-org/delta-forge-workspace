-- =============================================================================
-- HR Workforce Analytics Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: SCD2 (two-pass MERGE), pseudonymisation (REDACT salary, MASK SSN,
-- keyed_hash employee_name), deletion vectors for terminated employee erasure

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw employee and compensation data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'SCD2 employee dimension and enriched events';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Compensation star schema and workforce KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_employees (
    employee_id         STRING      NOT NULL,
    employee_name       STRING,
    ssn                 STRING,
    hire_date           STRING,
    termination_date    STRING,
    department_id       STRING,
    position_id         STRING,
    education_level     STRING,
    gender              STRING,
    date_of_birth       STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/workforce/raw_employees';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_employees TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_departments (
    department_id       STRING      NOT NULL,
    department_name     STRING      NOT NULL,
    division            STRING,
    cost_center         STRING,
    head_count_budget   INT,
    manager_name        STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/workforce/raw_departments';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_departments TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_positions (
    position_id         STRING      NOT NULL,
    title               STRING      NOT NULL,
    job_family          STRING,
    job_level           INT,
    pay_grade_min       DECIMAL(10,2),
    pay_grade_max       DECIMAL(10,2),
    exempt_flag         BOOLEAN,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/workforce/raw_positions';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_positions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_comp_events (
    event_id            STRING      NOT NULL,
    employee_id         STRING      NOT NULL,
    department_id       STRING      NOT NULL,
    position_id         STRING      NOT NULL,
    event_date          STRING,
    event_type          STRING,
    base_salary         DECIMAL(10,2),
    bonus               DECIMAL(10,2),
    performance_rating  DECIMAL(3,1),
    notes               STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/workforce/raw_comp_events';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_comp_events TO USER {{current_user}};

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.dim_employee_scd2 (
    surrogate_key       INT         NOT NULL,
    employee_id         STRING      NOT NULL,
    employee_name       STRING,
    ssn                 STRING,
    hire_date           DATE,
    termination_date    DATE,
    department_id       STRING,
    position_id         STRING,
    education_level     STRING,
    gender              STRING,
    age_band            STRING,
    base_salary         DECIMAL(10,2),
    valid_from          DATE        NOT NULL,
    valid_to            DATE,
    is_current          BOOLEAN     NOT NULL
) LOCATION '{{data_path}}/silver/workforce/dim_employee_scd2'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.dim_employee_scd2 TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.comp_events_enriched (
    event_id            STRING      NOT NULL,
    employee_id         STRING      NOT NULL,
    department_id       STRING      NOT NULL,
    position_id         STRING      NOT NULL,
    event_date          DATE,
    event_type          STRING,
    base_salary         DECIMAL(10,2),
    bonus               DECIMAL(10,2),
    total_comp          DECIMAL(10,2),
    salary_change_pct   DECIMAL(5,2),
    performance_rating  DECIMAL(3,1)
) LOCATION '{{data_path}}/silver/workforce/comp_events_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.comp_events_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_compensation_events (
    event_key           INT         NOT NULL,
    employee_key        INT         NOT NULL,
    department_key      INT         NOT NULL,
    position_key        INT         NOT NULL,
    event_date          DATE,
    event_type          STRING,
    base_salary         DECIMAL(10,2),
    bonus               DECIMAL(10,2),
    total_comp          DECIMAL(10,2),
    salary_change_pct   DECIMAL(5,2),
    performance_rating  DECIMAL(3,1)
) LOCATION '{{data_path}}/gold/workforce/fact_compensation_events';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_compensation_events TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_employee (
    surrogate_key       INT         NOT NULL,
    employee_id         STRING      NOT NULL,
    name                STRING,
    hire_date           DATE,
    termination_date    DATE,
    education_level     STRING,
    gender              STRING,
    age_band            STRING,
    valid_from          DATE,
    valid_to            DATE,
    is_current          BOOLEAN
) LOCATION '{{data_path}}/gold/workforce/dim_employee';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_employee TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_department (
    department_key      INT         NOT NULL,
    department_name     STRING      NOT NULL,
    division            STRING,
    cost_center         STRING,
    head_count_budget   INT,
    manager_name        STRING
) LOCATION '{{data_path}}/gold/workforce/dim_department';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_department TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_position (
    position_key        INT         NOT NULL,
    title               STRING      NOT NULL,
    job_family          STRING,
    job_level           INT,
    pay_grade_min       DECIMAL(10,2),
    pay_grade_max       DECIMAL(10,2),
    exempt_flag         BOOLEAN
) LOCATION '{{data_path}}/gold/workforce/dim_position';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_position TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_workforce_analytics (
    department          STRING      NOT NULL,
    quarter             STRING      NOT NULL,
    headcount           INT,
    avg_salary          DECIMAL(10,2),
    median_salary       DECIMAL(10,2),
    turnover_rate       DECIMAL(5,2),
    avg_tenure_years    DECIMAL(4,1),
    promotion_rate      DECIMAL(5,2),
    gender_pay_gap_pct  DECIMAL(5,2),
    compa_ratio         DECIMAL(5,3)
) LOCATION '{{data_path}}/gold/workforce/kpi_workforce_analytics';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_workforce_analytics TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: DEPARTMENTS (5 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_departments VALUES
    ('DEPT-ENG',  'Engineering',      'Technology',  'CC-1001', 50, 'Diana Torres',    '2025-01-01T00:00:00'),
    ('DEPT-MKT',  'Marketing',        'Revenue',     'CC-2001', 20, 'Kevin Marshall',  '2025-01-01T00:00:00'),
    ('DEPT-FIN',  'Finance',          'Operations',  'CC-3001', 15, 'Sandra Lee',      '2025-01-01T00:00:00'),
    ('DEPT-HR',   'Human Resources',  'Operations',  'CC-4001', 10, 'Angela Russo',    '2025-01-01T00:00:00'),
    ('DEPT-SALE', 'Sales',            'Revenue',     'CC-5001', 30, 'Marcus Johnson',  '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_departments;


-- ===================== BRONZE SEED DATA: POSITIONS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_positions VALUES
    ('POS-SE1',  'Software Engineer I',       'Engineering',    1, 75000.00,  105000.00, true,  '2025-01-01T00:00:00'),
    ('POS-SE2',  'Software Engineer II',      'Engineering',    2, 100000.00, 140000.00, true,  '2025-01-01T00:00:00'),
    ('POS-SSE',  'Senior Software Engineer',  'Engineering',    3, 130000.00, 180000.00, true,  '2025-01-01T00:00:00'),
    ('POS-EM',   'Engineering Manager',       'Engineering',    4, 160000.00, 220000.00, true,  '2025-01-01T00:00:00'),
    ('POS-MA1',  'Marketing Analyst',         'Marketing',      1, 55000.00,  80000.00,  true,  '2025-01-01T00:00:00'),
    ('POS-FA1',  'Financial Analyst',         'Finance',        1, 65000.00,  95000.00,  true,  '2025-01-01T00:00:00'),
    ('POS-HRG',  'HR Generalist',            'Human Resources', 1, 55000.00,  80000.00,  true,  '2025-01-01T00:00:00'),
    ('POS-SR1',  'Sales Representative',      'Sales',          1, 50000.00,  75000.00,  false, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_positions;


-- ===================== BRONZE SEED DATA: EMPLOYEES (20 rows) =====================
-- Across 5 departments, 8 positions. Includes terminated employees.

INSERT INTO {{zone_prefix}}.bronze.raw_employees VALUES
    ('EMP-001', 'Alexis Jordan',     '111-22-3333', '2021-03-15', NULL,          'DEPT-ENG',  'POS-SE1',  'Bachelors',  'Female', '1995-06-12', '2025-01-01T00:00:00'),
    ('EMP-002', 'Brian Matthews',    '222-33-4444', '2020-01-10', NULL,          'DEPT-ENG',  'POS-SE2',  'Masters',    'Male',   '1992-03-08', '2025-01-01T00:00:00'),
    ('EMP-003', 'Catherine Zhao',    '333-44-5555', '2019-06-01', NULL,          'DEPT-ENG',  'POS-SSE',  'Masters',    'Female', '1988-11-22', '2025-01-01T00:00:00'),
    ('EMP-004', 'Daniel Okonkwo',    '444-55-6666', '2018-02-20', NULL,          'DEPT-ENG',  'POS-EM',   'Masters',    'Male',   '1985-07-14', '2025-01-01T00:00:00'),
    ('EMP-005', 'Emily Stanton',     '555-66-7777', '2022-08-01', '2024-06-30', 'DEPT-ENG',  'POS-SE1',  'Bachelors',  'Female', '1997-01-30', '2025-01-01T00:00:00'),
    ('EMP-006', 'Felipe Morales',    '666-77-8888', '2021-05-15', NULL,          'DEPT-MKT',  'POS-MA1',  'Bachelors',  'Male',   '1993-09-17', '2025-01-01T00:00:00'),
    ('EMP-007', 'Grace Sullivan',    '777-88-9999', '2020-09-01', NULL,          'DEPT-MKT',  'POS-MA1',  'Masters',    'Female', '1990-04-05', '2025-01-01T00:00:00'),
    ('EMP-008', 'Hiroshi Tanaka',    '888-99-0000', '2019-11-15', '2024-03-15', 'DEPT-MKT',  'POS-MA1',  'Bachelors',  'Male',   '1991-12-28', '2025-01-01T00:00:00'),
    ('EMP-009', 'Isabella Rossi',    '999-00-1111', '2022-01-10', NULL,          'DEPT-FIN',  'POS-FA1',  'Masters',    'Female', '1994-08-19', '2025-01-01T00:00:00'),
    ('EMP-010', 'James Cooper',      '100-11-2222', '2020-04-01', NULL,          'DEPT-FIN',  'POS-FA1',  'Bachelors',  'Male',   '1989-02-14', '2025-01-01T00:00:00'),
    ('EMP-011', 'Karen Whitfield',   '200-22-3333', '2021-07-01', NULL,          'DEPT-FIN',  'POS-FA1',  'Masters',    'Female', '1987-10-03', '2025-01-01T00:00:00'),
    ('EMP-012', 'Liam Novak',        '300-33-4444', '2023-02-01', NULL,          'DEPT-HR',   'POS-HRG',  'Bachelors',  'Male',   '1996-05-21', '2025-01-01T00:00:00'),
    ('EMP-013', 'Monica Chang',      '400-44-5555', '2020-10-15', NULL,          'DEPT-HR',   'POS-HRG',  'Masters',    'Female', '1991-07-09', '2025-01-01T00:00:00'),
    ('EMP-014', 'Nathan Brooks',     '500-55-6666', '2021-01-15', NULL,          'DEPT-SALE', 'POS-SR1',  'Bachelors',  'Male',   '1993-03-25', '2025-01-01T00:00:00'),
    ('EMP-015', 'Olivia Patel',      '600-66-7777', '2020-06-01', NULL,          'DEPT-SALE', 'POS-SR1',  'Bachelors',  'Female', '1994-11-11', '2025-01-01T00:00:00'),
    ('EMP-016', 'Peter Andersen',    '700-77-8888', '2019-03-15', NULL,          'DEPT-SALE', 'POS-SR1',  'Associates', 'Male',   '1988-06-30', '2025-01-01T00:00:00'),
    ('EMP-017', 'Quinn Murray',      '800-88-9999', '2022-05-01', '2024-09-30', 'DEPT-SALE', 'POS-SR1',  'Bachelors',  'Female', '1996-08-15', '2025-01-01T00:00:00'),
    ('EMP-018', 'Ricardo Silva',     '900-99-0000', '2023-06-15', NULL,          'DEPT-ENG',  'POS-SE1',  'Masters',    'Male',   '1998-02-07', '2025-01-01T00:00:00'),
    ('EMP-019', 'Samantha Wells',    '010-12-3456', '2021-11-01', NULL,          'DEPT-ENG',  'POS-SE2',  'Bachelors',  'Female', '1992-12-18', '2025-01-01T00:00:00'),
    ('EMP-020', 'Thomas Kim',        '020-23-4567', '2022-03-01', NULL,          'DEPT-MKT',  'POS-MA1',  'Masters',    'Male',   '1995-04-22', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_employees;


-- ===================== BRONZE SEED DATA: COMPENSATION EVENTS (55 rows) =====================
-- Multiple events per employee over 2 years: hires, raises, promotions, transfers, terminations

INSERT INTO {{zone_prefix}}.bronze.raw_comp_events VALUES
    -- Initial salaries (hire events)
    ('CE-001', 'EMP-001', 'DEPT-ENG',  'POS-SE1',  '2021-03-15', 'Hire',        82000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-002', 'EMP-002', 'DEPT-ENG',  'POS-SE1',  '2020-01-10', 'Hire',        85000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-003', 'EMP-003', 'DEPT-ENG',  'POS-SE2',  '2019-06-01', 'Hire',        115000.00, 0.00,     3.5, 'Experienced hire',            '2025-01-01T00:00:00'),
    ('CE-004', 'EMP-004', 'DEPT-ENG',  'POS-SSE',  '2018-02-20', 'Hire',        150000.00, 0.00,     4.0, 'Senior hire',                 '2025-01-01T00:00:00'),
    ('CE-005', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2022-08-01', 'Hire',        80000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-006', 'EMP-006', 'DEPT-MKT',  'POS-MA1',  '2021-05-15', 'Hire',        62000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-007', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2020-09-01', 'Hire',        68000.00,  0.00,     3.5, 'Experienced hire',            '2025-01-01T00:00:00'),
    ('CE-008', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2019-11-15', 'Hire',        60000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-009', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2022-01-10', 'Hire',        72000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-010', 'EMP-010', 'DEPT-FIN',  'POS-FA1',  '2020-04-01', 'Hire',        70000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-011', 'EMP-011', 'DEPT-FIN',  'POS-FA1',  '2021-07-01', 'Hire',        75000.00,  0.00,     3.5, 'Experienced hire',            '2025-01-01T00:00:00'),
    ('CE-012', 'EMP-012', 'DEPT-HR',   'POS-HRG',  '2023-02-01', 'Hire',        58000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-013', 'EMP-013', 'DEPT-HR',   'POS-HRG',  '2020-10-15', 'Hire',        62000.00,  0.00,     3.5, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-014', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2021-01-15', 'Hire',        55000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-015', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2020-06-01', 'Hire',        58000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-016', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2019-03-15', 'Hire',        52000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-017', 'EMP-017', 'DEPT-SALE', 'POS-SR1',  '2022-05-01', 'Hire',        54000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-018', 'EMP-018', 'DEPT-ENG',  'POS-SE1',  '2023-06-15', 'Hire',        88000.00,  0.00,     3.0, 'New hire, competitive offer', '2025-01-01T00:00:00'),
    ('CE-019', 'EMP-019', 'DEPT-ENG',  'POS-SE1',  '2021-11-01', 'Hire',        84000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    ('CE-020', 'EMP-020', 'DEPT-MKT',  'POS-MA1',  '2022-03-01', 'Hire',        64000.00,  0.00,     3.0, 'New hire',                    '2025-01-01T00:00:00'),
    -- 2023 annual raises and promotions
    ('CE-021', 'EMP-001', 'DEPT-ENG',  'POS-SE1',  '2023-04-01', 'Annual Raise',86100.00,  4500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-022', 'EMP-002', 'DEPT-ENG',  'POS-SE2',  '2023-04-01', 'Promotion',   108000.00, 8000.00,  4.0, 'Promoted to SE2',             '2025-01-01T00:00:00'),
    ('CE-023', 'EMP-003', 'DEPT-ENG',  'POS-SSE',  '2023-04-01', 'Promotion',   142000.00, 12000.00, 4.5, 'Promoted to SSE',             '2025-01-01T00:00:00'),
    ('CE-024', 'EMP-004', 'DEPT-ENG',  'POS-EM',   '2023-04-01', 'Promotion',   175000.00, 20000.00, 4.5, 'Promoted to Eng Manager',     '2025-01-01T00:00:00'),
    ('CE-025', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2023-04-01', 'Annual Raise',83200.00,  3000.00,  3.0, '4% merit increase',           '2025-01-01T00:00:00'),
    ('CE-026', 'EMP-006', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'Annual Raise',65100.00,  3000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-027', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'Annual Raise',71400.00,  4000.00,  4.0, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-028', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'Annual Raise',62400.00,  2500.00,  2.5, '4% below expectations',       '2025-01-01T00:00:00'),
    ('CE-029', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2023-04-01', 'Annual Raise',75600.00,  3500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-030', 'EMP-010', 'DEPT-FIN',  'POS-FA1',  '2023-04-01', 'Annual Raise',73500.00,  3500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-031', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2023-04-01', 'Annual Raise',57750.00,  5000.00,  4.0, '5% + commission bonus',       '2025-01-01T00:00:00'),
    ('CE-032', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2023-04-01', 'Annual Raise',60900.00,  4500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-033', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2023-04-01', 'Annual Raise',55640.00,  6000.00,  4.0, '7% top performer',            '2025-01-01T00:00:00'),
    -- 2024 annual raises, promotions, transfers, terminations
    ('CE-034', 'EMP-001', 'DEPT-ENG',  'POS-SE2',  '2024-04-01', 'Promotion',   105000.00, 6000.00,  4.0, 'Promoted to SE2',             '2025-01-01T00:00:00'),
    ('CE-035', 'EMP-002', 'DEPT-ENG',  'POS-SE2',  '2024-04-01', 'Annual Raise',113400.00, 7000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-036', 'EMP-003', 'DEPT-ENG',  'POS-SSE',  '2024-04-01', 'Annual Raise',149100.00, 10000.00, 4.0, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-037', 'EMP-004', 'DEPT-ENG',  'POS-EM',   '2024-04-01', 'Annual Raise',183750.00, 22000.00, 4.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-038', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2024-06-30', 'Termination', 83200.00,  0.00,     2.0, 'Voluntary resignation',       '2025-01-01T00:00:00'),
    ('CE-039', 'EMP-006', 'DEPT-MKT',  'POS-MA1',  '2024-04-01', 'Annual Raise',68355.00,  3500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-040', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2024-04-01', 'Annual Raise',74970.00,  5000.00,  4.0, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-041', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2024-03-15', 'Termination', 62400.00,  0.00,     2.0, 'Involuntary - performance',   '2025-01-01T00:00:00'),
    ('CE-042', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2024-04-01', 'Annual Raise',79380.00,  4000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-043', 'EMP-010', 'DEPT-FIN',  'POS-FA1',  '2024-04-01', 'Annual Raise',77175.00,  4000.00,  4.0, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-044', 'EMP-011', 'DEPT-FIN',  'POS-FA1',  '2024-04-01', 'Annual Raise',78750.00,  3500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-045', 'EMP-012', 'DEPT-HR',   'POS-HRG',  '2024-04-01', 'Annual Raise',60900.00,  2500.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-046', 'EMP-013', 'DEPT-HR',   'POS-HRG',  '2024-04-01', 'Annual Raise',65100.00,  3000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-047', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2024-04-01', 'Annual Raise',60637.50,  6000.00,  4.0, '5% + commission bonus',       '2025-01-01T00:00:00'),
    ('CE-048', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2024-04-01', 'Annual Raise',63945.00,  5000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-049', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2024-04-01', 'Annual Raise',58422.00,  6500.00,  4.5, '5% top performer',            '2025-01-01T00:00:00'),
    ('CE-050', 'EMP-017', 'DEPT-SALE', 'POS-SR1',  '2024-09-30', 'Termination', 54000.00,  0.00,     2.5, 'Voluntary resignation',       '2025-01-01T00:00:00'),
    ('CE-051', 'EMP-018', 'DEPT-ENG',  'POS-SE1',  '2024-04-01', 'Annual Raise',92400.00,  4000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    ('CE-052', 'EMP-019', 'DEPT-ENG',  'POS-SE2',  '2024-04-01', 'Promotion',   110000.00, 7000.00,  4.0, 'Promoted to SE2',             '2025-01-01T00:00:00'),
    ('CE-053', 'EMP-020', 'DEPT-MKT',  'POS-MA1',  '2024-04-01', 'Annual Raise',67200.00,  3000.00,  3.5, '5% merit increase',           '2025-01-01T00:00:00'),
    -- Transfer event
    ('CE-054', 'EMP-013', 'DEPT-MKT',  'POS-MA1',  '2024-07-01', 'Transfer',    68000.00,  0.00,     3.5, 'Transferred HR to Marketing', '2025-01-01T00:00:00'),
    ('CE-055', 'EMP-019', 'DEPT-ENG',  'POS-SE2',  '2024-10-01', 'Adjustment',  115000.00, 0.00,     4.0, 'Market adjustment',           '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_comp_events;


-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.dim_employee_scd2 (base_salary) TRANSFORM redact PARAMS ('replacement', '0.00');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.dim_employee_scd2 (ssn) TRANSFORM mask PARAMS ('visible_prefix', 0, 'visible_suffix', 4, 'mask_char', '*');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.dim_employee_scd2 (employee_name) TRANSFORM keyed_hash PARAMS ('algorithm', 'SHA256');
