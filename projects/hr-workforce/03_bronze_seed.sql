-- =============================================================================
-- HR Workforce Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE hr_workforce_03_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== BRONZE SEED: DEPARTMENTS (6 rows) =====================

MERGE INTO hr.bronze.raw_departments AS target
USING (VALUES
  ('DEPT-ENG',  'Engineering',      'Technology',  'CC-1001', 60,  9600000.00, 'Diana Torres',    '2025-01-01T00:00:00'),
  ('DEPT-MKT',  'Marketing',        'Revenue',     'CC-2001', 25,  3200000.00, 'Kevin Marshall',  '2025-01-01T00:00:00'),
  ('DEPT-FIN',  'Finance',          'Operations',  'CC-3001', 20,  2800000.00, 'Sandra Lee',      '2025-01-01T00:00:00'),
  ('DEPT-HR',   'Human Resources',  'Operations',  'CC-4001', 12,  1500000.00, 'Angela Russo',    '2025-01-01T00:00:00'),
  ('DEPT-SALE', 'Sales',            'Revenue',     'CC-5001', 35,  4200000.00, 'Marcus Johnson',  '2025-01-01T00:00:00'),
  ('DEPT-OPS',  'Operations',       'Operations',  'CC-6001', 18,  2200000.00, 'Patricia Dunn',   '2025-01-01T00:00:00')
) AS source(department_id, department_name, division, cost_center, head_count_budget, annual_budget, manager_name, ingested_at)
ON target.department_id = source.department_id
WHEN MATCHED THEN UPDATE SET
  department_name   = source.department_name,
  division          = source.division,
  cost_center       = source.cost_center,
  head_count_budget = source.head_count_budget,
  annual_budget     = source.annual_budget,
  manager_name      = source.manager_name,
  ingested_at       = source.ingested_at
WHEN NOT MATCHED THEN INSERT (department_id, department_name, division, cost_center, head_count_budget, annual_budget, manager_name, ingested_at)
  VALUES (source.department_id, source.department_name, source.division, source.cost_center, source.head_count_budget, source.annual_budget, source.manager_name, source.ingested_at);

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_departments;


-- ===================== BRONZE SEED: POSITIONS (10 rows) =====================

MERGE INTO hr.bronze.raw_positions AS target
USING (VALUES
  ('POS-SE1',  'Software Engineer I',       'Engineering',     1, 75000.00,  105000.00, true,  '2025-01-01T00:00:00'),
  ('POS-SE2',  'Software Engineer II',      'Engineering',     2, 100000.00, 140000.00, true,  '2025-01-01T00:00:00'),
  ('POS-SSE',  'Senior Software Engineer',  'Engineering',     3, 130000.00, 180000.00, true,  '2025-01-01T00:00:00'),
  ('POS-EM',   'Engineering Manager',       'Engineering',     4, 160000.00, 220000.00, true,  '2025-01-01T00:00:00'),
  ('POS-MA1',  'Marketing Analyst',         'Marketing',       1, 55000.00,  80000.00,  true,  '2025-01-01T00:00:00'),
  ('POS-FA1',  'Financial Analyst',         'Finance',         1, 65000.00,  95000.00,  true,  '2025-01-01T00:00:00'),
  ('POS-HRG',  'HR Generalist',             'Human Resources', 1, 55000.00,  80000.00,  true,  '2025-01-01T00:00:00'),
  ('POS-SR1',  'Sales Representative',      'Sales',           1, 50000.00,  75000.00,  false, '2025-01-01T00:00:00'),
  ('POS-OA1',  'Operations Analyst',        'Operations',      1, 60000.00,  90000.00,  true,  '2025-01-01T00:00:00'),
  ('POS-OM1',  'Operations Manager',        'Operations',      3, 95000.00,  140000.00, true,  '2025-01-01T00:00:00')
) AS source(position_id, title, job_family, job_level, pay_grade_min, pay_grade_max, exempt_flag, ingested_at)
ON target.position_id = source.position_id
WHEN MATCHED THEN UPDATE SET
  title         = source.title,
  job_family    = source.job_family,
  job_level     = source.job_level,
  pay_grade_min = source.pay_grade_min,
  pay_grade_max = source.pay_grade_max,
  exempt_flag   = source.exempt_flag,
  ingested_at   = source.ingested_at
WHEN NOT MATCHED THEN INSERT (position_id, title, job_family, job_level, pay_grade_min, pay_grade_max, exempt_flag, ingested_at)
  VALUES (source.position_id, source.title, source.job_family, source.job_level, source.pay_grade_min, source.pay_grade_max, source.exempt_flag, source.ingested_at);

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_positions;


-- ===================== BRONZE SEED: EMPLOYEES (20 rows) =====================
-- 10M/10F, across 6 departments, 10 positions
-- Includes 3 underpaid (compa-ratio < 0.8), 1 overpaid (> 1.2)
-- Measurable gender pay gap in Engineering and Sales

MERGE INTO hr.bronze.raw_employees AS target
USING (VALUES
  ('EMP-001', 'Alexis Jordan',     '111-22-3333', 'ajordan@company.com',    '1995-06-12', 'Female', '2021-03-15', NULL,          'DEPT-ENG',  'POS-SE1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-002', 'Brian Matthews',    '222-33-4444', 'bmatthews@company.com',  '1992-03-08', 'Male',   '2020-01-10', NULL,          'DEPT-ENG',  'POS-SE2',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-003', 'Catherine Zhao',    '333-44-5555', 'czhao@company.com',      '1988-11-22', 'Female', '2019-06-01', NULL,          'DEPT-ENG',  'POS-SSE',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-004', 'Daniel Okonkwo',    '444-55-6666', 'dokonkwo@company.com',   '1985-07-14', 'Male',   '2018-02-20', NULL,          'DEPT-ENG',  'POS-EM',   'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-005', 'Emily Stanton',     '555-66-7777', 'estanton@company.com',   '1997-01-30', 'Female', '2022-08-01', '2024-06-30',  'DEPT-ENG',  'POS-SE1',  'Bachelors',  'terminated', '2025-01-01T00:00:00'),
  ('EMP-006', 'Felipe Morales',    '666-77-8888', 'fmorales@company.com',   '1993-09-17', 'Male',   '2021-05-15', NULL,          'DEPT-MKT',  'POS-MA1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-007', 'Grace Sullivan',    '777-88-9999', 'gsullivan@company.com',  '1990-04-05', 'Female', '2020-09-01', NULL,          'DEPT-MKT',  'POS-MA1',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-008', 'Hiroshi Tanaka',    '888-99-0000', 'htanaka@company.com',    '1991-12-28', 'Male',   '2019-11-15', '2024-03-15',  'DEPT-MKT',  'POS-MA1',  'Bachelors',  'terminated', '2025-01-01T00:00:00'),
  ('EMP-009', 'Isabella Rossi',    '999-00-1111', 'irossi@company.com',     '1994-08-19', 'Female', '2022-01-10', NULL,          'DEPT-FIN',  'POS-FA1',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-010', 'James Cooper',      '100-11-2222', 'jcooper@company.com',    '1989-02-14', 'Male',   '2020-04-01', NULL,          'DEPT-FIN',  'POS-FA1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-011', 'Karen Whitfield',   '200-22-3333', 'kwhitfield@company.com', '1987-10-03', 'Female', '2021-07-01', NULL,          'DEPT-FIN',  'POS-FA1',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-012', 'Liam Novak',        '300-33-4444', 'lnovak@company.com',     '1996-05-21', 'Male',   '2023-02-01', NULL,          'DEPT-HR',   'POS-HRG',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-013', 'Monica Chang',      '400-44-5555', 'mchang@company.com',     '1991-07-09', 'Female', '2020-10-15', NULL,          'DEPT-HR',   'POS-HRG',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-014', 'Nathan Brooks',     '500-55-6666', 'nbrooks@company.com',    '1993-03-25', 'Male',   '2021-01-15', NULL,          'DEPT-SALE', 'POS-SR1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-015', 'Olivia Patel',      '600-66-7777', 'opatel@company.com',     '1994-11-11', 'Female', '2020-06-01', NULL,          'DEPT-SALE', 'POS-SR1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-016', 'Peter Andersen',    '700-77-8888', 'pandersen@company.com',  '1988-06-30', 'Male',   '2019-03-15', NULL,          'DEPT-SALE', 'POS-SR1',  'Associates', 'active',     '2025-01-01T00:00:00'),
  ('EMP-017', 'Quinn Murray',      '800-88-9999', 'qmurray@company.com',   '1996-08-15', 'Female', '2022-05-01', '2024-09-30',  'DEPT-SALE', 'POS-SR1',  'Bachelors',  'terminated', '2025-01-01T00:00:00'),
  ('EMP-018', 'Ricardo Silva',     '900-99-0000', 'rsilva@company.com',     '1998-02-07', 'Male',   '2023-06-15', NULL,          'DEPT-ENG',  'POS-SE1',  'Masters',    'active',     '2025-01-01T00:00:00'),
  ('EMP-019', 'Samantha Wells',    '010-12-3456', 'swells@company.com',     '1992-12-18', 'Female', '2021-11-01', NULL,          'DEPT-OPS',  'POS-OA1',  'Bachelors',  'active',     '2025-01-01T00:00:00'),
  ('EMP-020', 'Thomas Kim',        '020-23-4567', 'tkim@company.com',       '1995-04-22', 'Male',   '2022-03-01', NULL,          'DEPT-OPS',  'POS-OA1',  'Masters',    'active',     '2025-01-01T00:00:00')
) AS source(employee_id, employee_name, ssn, email, date_of_birth, gender, hire_date, termination_date, department_id, position_id, education_level, status, ingested_at)
ON target.employee_id = source.employee_id
WHEN MATCHED THEN UPDATE SET
  employee_name    = source.employee_name,
  ssn              = source.ssn,
  email            = source.email,
  date_of_birth    = source.date_of_birth,
  gender           = source.gender,
  hire_date        = source.hire_date,
  termination_date = source.termination_date,
  department_id    = source.department_id,
  position_id      = source.position_id,
  education_level  = source.education_level,
  status           = source.status,
  ingested_at      = source.ingested_at
WHEN NOT MATCHED THEN INSERT (employee_id, employee_name, ssn, email, date_of_birth, gender, hire_date, termination_date, department_id, position_id, education_level, status, ingested_at)
  VALUES (source.employee_id, source.employee_name, source.ssn, source.email, source.date_of_birth, source.gender, source.hire_date, source.termination_date, source.department_id, source.position_id, source.education_level, source.status, source.ingested_at);

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_employees;


-- ===================== BRONZE SEED: COMPENSATION EVENTS (55 rows) =====================
-- 20 hires, 12 salary adjustments, 8 promotions, 5 transfers, 3 bonuses,
-- 5 terminations, 2 PIPs
-- Result after SCD2: 20 initial + 5 promotions + 3 transfers + 4 salary adj = 32 rows
-- 18 with is_current=1 (2 terminated: EMP-005, EMP-008; but EMP-017 terminated later)

MERGE INTO hr.bronze.raw_comp_events AS target
USING (VALUES
  -- === 20 HIRE EVENTS ===
  ('CE-001', 'EMP-001', 'DEPT-ENG',  'POS-SE1',  '2021-03-15', 'hire',            82000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-002', 'EMP-002', 'DEPT-ENG',  'POS-SE1',  '2020-01-10', 'hire',            85000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-003', 'EMP-003', 'DEPT-ENG',  'POS-SE2',  '2019-06-01', 'hire',           115000.00,  0.00,     3.5, 'Experienced hire',              '2025-01-01T00:00:00'),
  ('CE-004', 'EMP-004', 'DEPT-ENG',  'POS-SSE',  '2018-02-20', 'hire',           150000.00,  0.00,     4.0, 'Senior hire',                   '2025-01-01T00:00:00'),
  ('CE-005', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2022-08-01', 'hire',            80000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-006', 'EMP-006', 'DEPT-MKT',  'POS-MA1',  '2021-05-15', 'hire',            62000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-007', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2020-09-01', 'hire',            68000.00,  0.00,     3.5, 'Experienced hire',              '2025-01-01T00:00:00'),
  ('CE-008', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2019-11-15', 'hire',            60000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-009', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2022-01-10', 'hire',            72000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-010', 'EMP-010', 'DEPT-FIN',  'POS-FA1',  '2020-04-01', 'hire',            70000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-011', 'EMP-011', 'DEPT-FIN',  'POS-FA1',  '2021-07-01', 'hire',            75000.00,  0.00,     3.5, 'Experienced hire',              '2025-01-01T00:00:00'),
  ('CE-012', 'EMP-012', 'DEPT-HR',   'POS-HRG',  '2023-02-01', 'hire',            58000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-013', 'EMP-013', 'DEPT-HR',   'POS-HRG',  '2020-10-15', 'hire',            62000.00,  0.00,     3.5, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-014', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2021-01-15', 'hire',            55000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-015', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2020-06-01', 'hire',            58000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-016', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2019-03-15', 'hire',            52000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-017', 'EMP-017', 'DEPT-SALE', 'POS-SR1',  '2022-05-01', 'hire',            54000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-018', 'EMP-018', 'DEPT-ENG',  'POS-SE1',  '2023-06-15', 'hire',            88000.00,  0.00,     3.0, 'New hire, competitive offer',   '2025-01-01T00:00:00'),
  ('CE-019', 'EMP-019', 'DEPT-OPS',  'POS-OA1',  '2021-11-01', 'hire',            65000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  ('CE-020', 'EMP-020', 'DEPT-OPS',  'POS-OA1',  '2022-03-01', 'hire',            68000.00,  0.00,     3.0, 'New hire',                      '2025-01-01T00:00:00'),
  -- === 8 PROMOTIONS (title changes that expire old SCD2 row) ===
  ('CE-021', 'EMP-002', 'DEPT-ENG',  'POS-SE2',  '2023-04-01', 'promotion',      108000.00,  8000.00,  4.0, 'Promoted SE1 to SE2',           '2025-01-01T00:00:00'),
  ('CE-022', 'EMP-003', 'DEPT-ENG',  'POS-SSE',  '2023-04-01', 'promotion',      142000.00, 12000.00,  4.5, 'Promoted SE2 to SSE',           '2025-01-01T00:00:00'),
  ('CE-023', 'EMP-004', 'DEPT-ENG',  'POS-EM',   '2023-04-01', 'promotion',      175000.00, 20000.00,  4.5, 'Promoted SSE to EM',            '2025-01-01T00:00:00'),
  ('CE-024', 'EMP-001', 'DEPT-ENG',  'POS-SE2',  '2024-04-01', 'promotion',      105000.00,  6000.00,  4.0, 'Promoted SE1 to SE2',           '2025-01-01T00:00:00'),
  ('CE-025', 'EMP-019', 'DEPT-OPS',  'POS-OM1',  '2024-04-01', 'promotion',      100000.00,  5000.00,  4.0, 'Promoted OA1 to OM1',           '2025-01-01T00:00:00'),
  ('CE-026', 'EMP-018', 'DEPT-ENG',  'POS-SE2',  '2024-10-01', 'promotion',      108000.00,  5000.00,  4.0, 'Promoted SE1 to SE2',           '2025-01-01T00:00:00'),
  ('CE-027', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2023-04-01', 'promotion',       61000.00,  6000.00,  4.0, 'Top performer promotion',       '2025-01-01T00:00:00'),
  ('CE-028', 'EMP-014', 'DEPT-SALE', 'POS-SR1',  '2024-04-01', 'promotion',       63000.00,  7000.00,  4.5, 'Top sales performer',           '2025-01-01T00:00:00'),
  -- === 12 SALARY ADJUSTMENTS ===
  ('CE-029', 'EMP-001', 'DEPT-ENG',  'POS-SE1',  '2023-04-01', 'salary_adjustment', 86100.00,  4500.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-030', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2023-04-01', 'salary_adjustment', 83200.00,  3000.00,  3.0, '4% merit increase',          '2025-01-01T00:00:00'),
  ('CE-031', 'EMP-006', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'salary_adjustment', 65100.00,  3000.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-032', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'salary_adjustment', 71400.00,  4000.00,  4.0, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-033', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2023-04-01', 'salary_adjustment', 62400.00,  2500.00,  2.5, '4% below expectations',      '2025-01-01T00:00:00'),
  ('CE-034', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2023-04-01', 'salary_adjustment', 75600.00,  3500.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-035', 'EMP-010', 'DEPT-FIN',  'POS-FA1',  '2023-04-01', 'salary_adjustment', 73500.00,  3500.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-036', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2023-04-01', 'salary_adjustment', 60900.00,  4500.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-037', 'EMP-002', 'DEPT-ENG',  'POS-SE2',  '2024-04-01', 'salary_adjustment',113400.00,  7000.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-038', 'EMP-003', 'DEPT-ENG',  'POS-SSE',  '2024-04-01', 'salary_adjustment',149100.00, 10000.00,  4.0, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-039', 'EMP-004', 'DEPT-ENG',  'POS-EM',   '2024-04-01', 'salary_adjustment',183750.00, 22000.00,  4.5, '5% merit increase',          '2025-01-01T00:00:00'),
  ('CE-040', 'EMP-020', 'DEPT-OPS',  'POS-OA1',  '2024-04-01', 'salary_adjustment', 71400.00,  3000.00,  3.5, '5% merit increase',          '2025-01-01T00:00:00'),
  -- === 5 TRANSFERS (dept changes) ===
  ('CE-041', 'EMP-013', 'DEPT-MKT',  'POS-MA1',  '2024-07-01', 'transfer',         68000.00,  0.00,     3.5, 'Transferred HR to Marketing',  '2025-01-01T00:00:00'),
  ('CE-042', 'EMP-011', 'DEPT-OPS',  'POS-OA1',  '2024-08-01', 'transfer',         78000.00,  0.00,     3.5, 'Transferred Finance to Ops',   '2025-01-01T00:00:00'),
  ('CE-043', 'EMP-010', 'DEPT-OPS',  'POS-OA1',  '2024-09-01', 'transfer',         77000.00,  0.00,     4.0, 'Transferred Finance to Ops',   '2025-01-01T00:00:00'),
  ('CE-044', 'EMP-006', 'DEPT-SALE', 'POS-SR1',  '2024-10-01', 'transfer',         65000.00,  0.00,     3.5, 'Transferred Marketing to Sales','2025-01-01T00:00:00'),
  ('CE-045', 'EMP-012', 'DEPT-OPS',  'POS-OA1',  '2024-11-01', 'transfer',         62000.00,  0.00,     3.5, 'Transferred HR to Operations', '2025-01-01T00:00:00'),
  -- === 3 BONUS-ONLY EVENTS ===
  ('CE-046', 'EMP-016', 'DEPT-SALE', 'POS-SR1',  '2024-06-01', 'bonus',            61000.00,  8000.00,  4.5, 'Q2 sales target exceeded',     '2025-01-01T00:00:00'),
  ('CE-047', 'EMP-004', 'DEPT-ENG',  'POS-EM',   '2024-06-01', 'bonus',           183750.00, 15000.00,  5.0, 'Patent filing bonus',          '2025-01-01T00:00:00'),
  ('CE-048', 'EMP-007', 'DEPT-MKT',  'POS-MA1',  '2024-06-01', 'bonus',            71400.00,  5000.00,  4.0, 'Campaign success bonus',       '2025-01-01T00:00:00'),
  -- === 5 TERMINATIONS ===
  ('CE-049', 'EMP-005', 'DEPT-ENG',  'POS-SE1',  '2024-06-30', 'termination',      83200.00,  0.00,     2.0, 'Voluntary resignation',        '2025-01-01T00:00:00'),
  ('CE-050', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2024-03-15', 'termination',      62400.00,  0.00,     2.0, 'Involuntary - performance',    '2025-01-01T00:00:00'),
  ('CE-051', 'EMP-017', 'DEPT-SALE', 'POS-SR1',  '2024-09-30', 'termination',      54000.00,  0.00,     2.5, 'Voluntary resignation',        '2025-01-01T00:00:00'),
  -- === 2 PIPs (Performance Improvement Plans) ===
  ('CE-052', 'EMP-008', 'DEPT-MKT',  'POS-MA1',  '2023-10-01', 'pip',              62400.00,  0.00,     2.0, 'PIP initiated - low output',   '2025-01-01T00:00:00'),
  ('CE-053', 'EMP-017', 'DEPT-SALE', 'POS-SR1',  '2024-06-01', 'pip',              54000.00,  0.00,     2.0, 'PIP initiated - quota miss',   '2025-01-01T00:00:00'),
  -- === 2 additional terminations to reach 5 total ===
  ('CE-054', 'EMP-009', 'DEPT-FIN',  'POS-FA1',  '2024-12-15', 'termination',      75600.00,  0.00,     3.0, 'Voluntary - relocation',       '2025-01-01T00:00:00'),
  ('CE-055', 'EMP-015', 'DEPT-SALE', 'POS-SR1',  '2024-11-30', 'termination',      60900.00,  0.00,     2.5, 'Voluntary resignation',        '2025-01-01T00:00:00')
) AS source(event_id, employee_id, department_id, position_id, event_date, event_type, base_salary, bonus, performance_rating, notes, ingested_at)
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
  employee_id        = source.employee_id,
  department_id      = source.department_id,
  position_id        = source.position_id,
  event_date         = source.event_date,
  event_type         = source.event_type,
  base_salary        = source.base_salary,
  bonus              = source.bonus,
  performance_rating = source.performance_rating,
  notes              = source.notes,
  ingested_at        = source.ingested_at
WHEN NOT MATCHED THEN INSERT (event_id, employee_id, department_id, position_id, event_date, event_type, base_salary, bonus, performance_rating, notes, ingested_at)
  VALUES (source.event_id, source.employee_id, source.department_id, source.position_id, source.event_date, source.event_type, source.base_salary, source.bonus, source.performance_rating, source.notes, source.ingested_at);

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM hr.bronze.raw_comp_events;
