-- =============================================================================
-- Healthcare Patient EHR Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw electronic health records';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Validated and transformed EHR data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Admissions star schema and KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_admissions (
    record_id           STRING      NOT NULL,
    patient_id          STRING      NOT NULL,
    patient_name        STRING,
    ssn                 STRING,
    email               STRING,
    department_code     STRING,
    diagnosis_code      STRING,
    admission_date      STRING,
    discharge_date      STRING,
    total_charges       DECIMAL(12,2),
    attending_physician STRING,
    notes               STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/ehr/raw_admissions';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_admissions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_departments (
    department_code     STRING      NOT NULL,
    department_name     STRING      NOT NULL,
    floor               INT,
    wing                STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/ehr/raw_departments';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_departments TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_diagnoses (
    diagnosis_code      STRING      NOT NULL,
    description         STRING      NOT NULL,
    category            STRING,
    severity            STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/ehr/raw_diagnoses';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_diagnoses TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: DEPARTMENTS (9 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_departments VALUES
    ('CARD', 'Cardiology', 3, 'East', '2024-01-01T00:00:00'),
    ('ORTH', 'Orthopedics', 2, 'West', '2024-01-01T00:00:00'),
    ('NEUR', 'Neurology', 4, 'East', '2024-01-01T00:00:00'),
    ('PULM', 'Pulmonology', 3, 'West', '2024-01-01T00:00:00'),
    ('GAST', 'Gastroenterology', 2, 'East', '2024-01-01T00:00:00'),
    ('ONCO', 'Oncology', 5, 'North', '2024-01-01T00:00:00'),
    ('ENDO', 'Endocrinology', 4, 'West', '2024-01-01T00:00:00'),
    ('NEPH', 'Nephrology', 3, 'North', '2024-01-01T00:00:00'),
    ('EMER', 'Emergency Medicine', 1, 'South', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 9
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_departments;


-- ===================== BRONZE SEED DATA: DIAGNOSES (16 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_diagnoses VALUES
    ('I21.0', 'Acute ST-elevation myocardial infarction of anterior wall', 'Cardiac', 'Critical', '2024-01-01T00:00:00'),
    ('I25.10', 'Atherosclerotic heart disease of native coronary artery', 'Cardiac', 'High', '2024-01-01T00:00:00'),
    ('I50.9', 'Heart failure, unspecified', 'Cardiac', 'High', '2024-01-01T00:00:00'),
    ('S72.001A', 'Fracture of unspecified part of neck of right femur', 'Orthopedic', 'Medium', '2024-01-01T00:00:00'),
    ('M17.11', 'Primary osteoarthritis, right knee', 'Orthopedic', 'Low', '2024-01-01T00:00:00'),
    ('G45.9', 'Transient cerebral ischemic attack, unspecified', 'Neurological', 'High', '2024-01-01T00:00:00'),
    ('G40.909', 'Epilepsy, unspecified, not intractable', 'Neurological', 'Medium', '2024-01-01T00:00:00'),
    ('J44.1', 'Chronic obstructive pulmonary disease with acute exacerbation', 'Pulmonary', 'High', '2024-01-01T00:00:00'),
    ('J18.9', 'Pneumonia, unspecified organism', 'Pulmonary', 'Medium', '2024-01-01T00:00:00'),
    ('K35.80', 'Unspecified acute appendicitis', 'Gastrointestinal', 'Medium', '2024-01-01T00:00:00'),
    ('K80.20', 'Calculus of gallbladder without cholecystitis', 'Gastrointestinal', 'Low', '2024-01-01T00:00:00'),
    ('C34.90', 'Malignant neoplasm of unspecified part of bronchus or lung', 'Oncological', 'Critical', '2024-01-01T00:00:00'),
    ('E11.65', 'Type 2 diabetes mellitus with hyperglycemia', 'Endocrine', 'Medium', '2024-01-01T00:00:00'),
    ('N18.6', 'End stage renal disease', 'Renal', 'Critical', '2024-01-01T00:00:00'),
    ('N17.9', 'Acute kidney failure, unspecified', 'Renal', 'High', '2024-01-01T00:00:00'),
    ('I48.91', 'Unspecified atrial fibrillation', 'Cardiac', 'Medium', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_diagnoses;


-- ===================== BRONZE SEED DATA: RAW ADMISSIONS (55 rows) =====================
-- Includes: duplicates, inconsistent dates, messy whitespace, readmissions within 30 days

INSERT INTO {{zone_prefix}}.bronze.raw_admissions VALUES
    ('A001', 'P1001', '  John Smith  ', '123-45-6789', 'john.smith@email.com', 'CARD', 'I21.0', '2023-03-15', '2023-03-22', 45200.00, 'Dr. Rivera', 'STEMI anterior wall', '2024-01-15T08:00:00'),
    ('A002', 'P1001', 'John Smith', '123-45-6789', 'john.smith@email.com', 'CARD', 'I25.10', '2023-04-10', '2023-04-14', 12800.00, 'Dr. Rivera', 'Follow-up readmission within 30d', '2024-01-15T08:00:00'),
    ('A003', 'P1002', 'Maria Garcia  ', '234-56-7890', 'maria.garcia@mail.com', 'ORTH', 'S72.001A', '2023-02-20', '2023-03-05', 67500.00, 'Dr. Chen', 'Hip fracture surgical repair', '2024-01-15T08:00:00'),
    ('A004', 'P1003', 'Robert Johnson', '345-67-8901', 'r.johnson@email.com', 'NEUR', 'G45.9', '2023-05-10', '2023-05-13', 18900.00, 'Dr. Patel', 'TIA evaluation', '2024-01-15T08:00:00'),
    ('A005', 'P1003', 'Robert Johnson ', '345-67-8901', 'r.johnson@email.com', 'NEUR', 'G45.9', '2023-06-05', '2023-06-08', 16200.00, 'Dr. Patel', 'Readmission TIA recurrence', '2024-01-15T08:00:00'),
    ('A006', 'P1004', ' Emily Davis', '456-78-9012', 'emily.d@hospital.org', 'PULM', 'J44.1', '2023-04-01', '2023-04-09', 23400.00, 'Dr. Thompson', 'COPD exacerbation', '2024-01-15T08:00:00'),
    ('A007', 'P1004', 'Emily Davis', '456-78-9012', 'emily.d@hospital.org', 'PULM', 'J44.1', '2023-04-28', '2023-05-03', 19800.00, 'Dr. Thompson', 'Readmission COPD within 30d', '2024-01-15T08:00:00'),
    ('A008', 'P1005', 'Michael Brown', '567-89-0123', 'mbrown@webmail.com', 'GAST', 'K35.80', '2023-06-15', '2023-06-18', 31200.00, 'Dr. Nguyen', 'Appendectomy', '2024-01-15T08:00:00'),
    ('A009', 'P1006', 'Sarah Wilson  ', '678-90-1234', 'sarah.w@inbox.com', 'ONCO', 'C34.90', '2023-01-10', '2023-01-25', 89500.00, 'Dr. Kim', 'Lung cancer initial treatment', '2024-01-15T08:00:00'),
    ('A010', 'P1006', 'Sarah Wilson', '678-90-1234', 'sarah.w@inbox.com', 'ONCO', 'C34.90', '2023-03-05', '2023-03-15', 72300.00, 'Dr. Kim', 'Chemo cycle 2', '2024-01-15T08:00:00'),
    ('A001', 'P1001', '  John Smith  ', '123-45-6789', 'john.smith@email.com', 'CARD', 'I21.0', '2023-03-15', '2023-03-22', 45200.00, 'Dr. Rivera', 'STEMI anterior wall DUPLICATE', '2024-01-15T08:05:00'),
    ('A011', 'P1007', 'David Lee ', '789-01-2345', 'dlee@company.com', 'ENDO', 'E11.65', '2023-07-20', '2023-07-24', 8900.00, 'Dr. Adams', 'Diabetic hyperglycemia management', '2024-01-15T08:00:00'),
    ('A012', 'P1008', ' Jennifer Martinez', '890-12-3456', 'jmartinez@email.com', 'NEPH', 'N18.6', '2023-08-01', '2023-08-12', 54600.00, 'Dr. Okafor', 'ESRD dialysis initiation', '2024-01-15T08:00:00'),
    ('A013', 'P1008', 'Jennifer Martinez', '890-12-3456', 'jmartinez@email.com', 'NEPH', 'N17.9', '2023-08-30', '2023-09-05', 38200.00, 'Dr. Okafor', 'Readmission AKI within 30d', '2024-01-15T08:00:00'),
    ('A014', 'P1009', 'William Taylor', '901-23-4567', 'wtaylor@mail.com', 'CARD', 'I50.9', '2023-09-10', '2023-09-18', 35700.00, 'Dr. Rivera', 'Heart failure management', '2024-01-15T08:00:00'),
    ('A015', 'P1009', 'William Taylor ', '901-23-4567', 'wtaylor@mail.com', 'CARD', 'I50.9', '2023-10-05', '2023-10-10', 28900.00, 'Dr. Rivera', 'HF readmission within 30d', '2024-01-15T08:00:00'),
    ('A016', 'P1010', 'Lisa Anderson ', '012-34-5678', 'landerson@webmail.com', 'EMER', 'J18.9', '2023-05-22', '2023-05-28', 21300.00, 'Dr. Park', 'Pneumonia ER admission', '2024-01-15T08:00:00'),
    ('A017', 'P1011', 'James Thomas', '111-22-3333', 'jthomas@email.com', 'ORTH', 'M17.11', '2023-07-01', '2023-07-05', 42100.00, 'Dr. Chen', 'Knee replacement surgery', '2024-01-15T08:00:00'),
    ('A018', 'P1012', '  Patricia White  ', '222-33-4444', 'pwhite@mail.com', 'NEUR', 'G40.909', '2023-08-15', '2023-08-18', 14500.00, 'Dr. Patel', 'Epilepsy evaluation', '2024-01-15T08:00:00'),
    ('A019', 'P1013', 'Charles Harris', '333-44-5555', 'charris@inbox.com', 'GAST', 'K80.20', '2023-09-01', '2023-09-04', 27800.00, 'Dr. Nguyen', 'Cholecystectomy', '2024-01-15T08:00:00'),
    ('A020', 'P1014', 'Nancy Clark  ', '444-55-6666', 'nclark@hospital.org', 'CARD', 'I48.91', '2023-10-20', '2023-10-24', 16700.00, 'Dr. Rivera', 'AFib ablation', '2024-01-15T08:00:00'),
    ('A021', 'P1015', 'Daniel Lewis', '555-66-7777', 'dlewis@email.com', 'PULM', 'J18.9', '2023-11-05', '2023-11-11', 24600.00, 'Dr. Thompson', 'Severe pneumonia', '2024-01-15T08:00:00'),
    ('A022', 'P1015', ' Daniel Lewis', '555-66-7777', 'dlewis@email.com', 'PULM', 'J44.1', '2023-11-28', '2023-12-04', 22100.00, 'Dr. Thompson', 'Readmission COPD exacerbation', '2024-01-15T08:00:00'),
    ('A023', 'P1001', 'John Smith', '123-45-6789', 'john.smith@email.com', 'CARD', 'I50.9', '2023-11-20', '2023-11-27', 33400.00, 'Dr. Rivera', 'HF admission', '2024-01-15T08:00:00'),
    ('A024', 'P1002', 'Maria Garcia', '234-56-7890', 'maria.garcia@mail.com', 'ORTH', 'M17.11', '2023-09-12', '2023-09-17', 39800.00, 'Dr. Chen', 'Knee replacement', '2024-01-15T08:00:00'),
    ('A025', 'P1016', 'Karen Robinson', '666-77-8888', 'krobinson@mail.com', 'ONCO', 'C34.90', '2023-04-15', '2023-04-30', 95200.00, 'Dr. Kim', 'Lung cancer surgery + chemo', '2024-01-15T08:00:00'),
    ('A026', 'P1016', 'Karen Robinson ', '666-77-8888', 'krobinson@mail.com', 'ONCO', 'C34.90', '2023-05-10', '2023-05-18', 68700.00, 'Dr. Kim', 'Readmission post-op complications', '2024-01-15T08:00:00'),
    ('A027', 'P1017', 'Steven Walker', '777-88-9999', 'swalker@company.com', 'EMER', 'I21.0', '2023-12-01', '2023-12-08', 52300.00, 'Dr. Park', 'ER cardiac event', '2024-01-15T08:00:00'),
    ('A028', 'P1017', 'Steven Walker', '777-88-9999', 'swalker@company.com', 'CARD', 'I25.10', '2023-12-20', '2023-12-24', 18500.00, 'Dr. Rivera', 'Readmission cardiac follow-up', '2024-01-15T08:00:00'),
    ('A029', 'P1018', '  Angela Hall  ', '888-99-0000', 'ahall@webmail.com', 'ENDO', 'E11.65', '2023-06-10', '2023-06-14', 7600.00, 'Dr. Adams', 'Diabetes management', '2024-01-15T08:00:00'),
    ('A030', 'P1019', 'Mark Young', '999-00-1111', 'myoung@email.com', 'NEPH', 'N17.9', '2023-10-15', '2023-10-22', 41300.00, 'Dr. Okafor', 'Acute kidney injury', '2024-01-15T08:00:00'),
    ('A031', 'P1019', 'Mark Young ', '999-00-1111', 'myoung@email.com', 'NEPH', 'N18.6', '2023-11-08', '2023-11-18', 49800.00, 'Dr. Okafor', 'Readmission progressed to ESRD', '2024-01-15T08:00:00'),
    ('A032', 'P1020', 'Betty King', '100-20-3040', 'bking@inbox.com', 'GAST', 'K35.80', '2023-03-25', '2023-03-28', 29500.00, 'Dr. Nguyen', 'Appendicitis surgery', '2024-01-15T08:00:00'),
    ('A003', 'P1002', 'Maria Garcia  ', '234-56-7890', 'maria.garcia@mail.com', 'ORTH', 'S72.001A', '2023-02-20', '2023-03-05', 67500.00, 'Dr. Chen', 'DUPLICATE hip fracture', '2024-01-15T08:10:00'),
    ('A033', 'P1010', 'Lisa Anderson', '012-34-5678', 'landerson@webmail.com', 'PULM', 'J44.1', '2023-12-10', '2023-12-16', 20100.00, 'Dr. Thompson', 'COPD management', '2024-01-15T08:00:00'),
    ('A034', 'P1014', 'Nancy Clark', '444-55-6666', 'nclark@hospital.org', 'CARD', 'I48.91', '2023-11-15', '2023-11-18', 14200.00, 'Dr. Rivera', 'AFib follow-up readmission', '2024-01-15T08:00:00'),
    ('A035', 'P1005', 'Michael Brown ', '567-89-0123', 'mbrown@webmail.com', 'GAST', 'K80.20', '2023-12-05', '2023-12-09', 26400.00, 'Dr. Nguyen', 'Gallbladder surgery', '2024-01-15T08:00:00'),
    ('A036', 'P1011', 'James Thomas ', '111-22-3333', 'jthomas@email.com', 'ORTH', 'M17.11', '2023-07-25', '2023-07-28', 15200.00, 'Dr. Chen', 'Readmission post knee surgery', '2024-01-15T08:00:00'),
    ('A037', 'P1013', ' Charles Harris', '333-44-5555', 'charris@inbox.com', 'EMER', 'K35.80', '2023-12-18', '2023-12-21', 33100.00, 'Dr. Park', 'ER abdominal pain', '2024-01-15T08:00:00'),
    ('A038', 'P1004', 'Emily Davis', '456-78-9012', 'emily.d@hospital.org', 'PULM', 'J18.9', '2023-09-15', '2023-09-21', 25800.00, 'Dr. Thompson', 'Pneumonia', '2024-01-15T08:00:00'),
    ('A039', 'P1007', 'David Lee', '789-01-2345', 'dlee@company.com', 'ENDO', 'E11.65', '2023-12-01', '2023-12-04', 9200.00, 'Dr. Adams', 'Diabetes follow-up', '2024-01-15T08:00:00'),
    ('A040', 'P1012', 'Patricia White', '222-33-4444', 'pwhite@mail.com', 'NEUR', 'G45.9', '2023-11-01', '2023-11-05', 19700.00, 'Dr. Patel', 'TIA evaluation', '2024-01-15T08:00:00'),
    ('A041', 'P1020', ' Betty King ', '100-20-3040', 'bking@inbox.com', 'EMER', 'J18.9', '2023-08-10', '2023-08-15', 22800.00, 'Dr. Park', 'ER pneumonia admission', '2024-01-15T08:00:00'),
    ('A042', 'P1006', 'Sarah Wilson', '678-90-1234', 'sarah.w@inbox.com', 'ONCO', 'C34.90', '2023-06-01', '2023-06-12', 78400.00, 'Dr. Kim', 'Chemo cycle 3', '2024-01-15T08:00:00'),
    ('A043', 'P1009', 'William Taylor', '901-23-4567', 'wtaylor@mail.com', 'CARD', 'I48.91', '2024-01-05', '2024-01-09', 17300.00, 'Dr. Rivera', 'AFib episode', '2024-01-15T08:00:00'),
    ('A044', 'P1018', 'Angela Hall', '888-99-0000', 'ahall@webmail.com', 'ENDO', 'E11.65', '2023-12-15', '2023-12-18', 8100.00, 'Dr. Adams', 'Diabetes complications', '2024-01-15T08:00:00'),
    ('A045', 'P1003', 'Robert Johnson', '345-67-8901', 'r.johnson@email.com', 'NEUR', 'G40.909', '2023-10-10', '2023-10-14', 15800.00, 'Dr. Patel', 'Epilepsy monitoring', '2024-01-15T08:00:00'),
    ('A046', 'P1015', 'Daniel Lewis', '555-66-7777', 'dlewis@email.com', 'EMER', 'J18.9', '2024-01-02', '2024-01-07', 23900.00, 'Dr. Park', 'ER pneumonia readmission', '2024-01-15T08:00:00'),
    ('A047', 'P1016', 'Karen Robinson', '666-77-8888', 'krobinson@mail.com', 'ONCO', 'C34.90', '2023-07-20', '2023-07-30', 71200.00, 'Dr. Kim', 'Chemo cycle post-surgery', '2024-01-15T08:00:00'),
    ('A048', 'P1017', 'Steven Walker ', '777-88-9999', 'swalker@company.com', 'CARD', 'I50.9', '2024-01-10', '2024-01-15', 31600.00, 'Dr. Rivera', 'HF management', '2024-01-15T08:00:00'),
    ('A008', 'P1005', 'Michael Brown', '567-89-0123', 'mbrown@webmail.com', 'GAST', 'K35.80', '2023-06-15', '2023-06-18', 31200.00, 'Dr. Nguyen', 'DUPLICATE appendectomy', '2024-01-15T08:12:00'),
    ('A049', 'P1010', 'Lisa Anderson', '012-34-5678', 'landerson@webmail.com', 'EMER', 'J18.9', '2024-01-08', '2024-01-13', 24100.00, 'Dr. Park', 'Pneumonia recurrence', '2024-01-15T08:00:00'),
    ('A050', 'P1019', 'Mark Young', '999-00-1111', 'myoung@email.com', 'NEPH', 'N18.6', '2024-01-05', '2024-01-12', 46500.00, 'Dr. Okafor', 'ESRD dialysis session', '2024-01-15T08:00:00'),
    ('A051', 'P1014', 'Nancy Clark', '444-55-6666', 'nclark@hospital.org', 'CARD', 'I50.9', '2024-01-08', '2024-01-14', 37200.00, 'Dr. Rivera', 'Heart failure new episode', '2024-01-15T08:00:00'),
    ('A052', 'P1011', 'James Thomas', '111-22-3333', 'jthomas@email.com', 'ORTH', 'S72.001A', '2023-11-15', '2023-11-28', 71800.00, 'Dr. Chen', 'Hip fracture', '2024-01-15T08:00:00');

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_admissions;


-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.admissions_cleaned (
    record_id           STRING      NOT NULL,
    patient_id          STRING      NOT NULL,
    patient_name        STRING,
    ssn                 STRING,
    email               STRING,
    department_code     STRING      NOT NULL,
    diagnosis_code      STRING      NOT NULL,
    admission_date      DATE        NOT NULL,
    discharge_date      DATE,
    los_days            INT,
    total_charges       DECIMAL(12,2) CHECK (total_charges >= 0),
    attending_physician STRING,
    readmission_flag    BOOLEAN     NOT NULL,
    prev_discharge_date DATE,
    ingested_at         TIMESTAMP   NOT NULL,
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/ehr/admissions_cleaned';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.admissions_cleaned TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.patients_deduped (
    patient_id          STRING      NOT NULL,
    patient_name        STRING,
    ssn                 STRING,
    email               STRING,
    last_admission_date DATE,
    total_admissions    INT,
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/ehr/patients_deduped';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.patients_deduped TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_department (
    department_key      INT         NOT NULL,
    department_code     STRING      NOT NULL,
    department_name     STRING      NOT NULL,
    floor               INT,
    wing                STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/ehr/dim_department';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_department TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_diagnosis (
    diagnosis_key       INT         NOT NULL,
    diagnosis_code      STRING      NOT NULL,
    description         STRING      NOT NULL,
    category            STRING,
    severity            STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/ehr/dim_diagnosis';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_diagnosis TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_admissions (
    admission_key       INT         NOT NULL,
    patient_key         STRING      NOT NULL,
    department_key      INT         NOT NULL,
    diagnosis_key       INT         NOT NULL,
    admission_date      DATE        NOT NULL,
    discharge_date      DATE,
    los_days            INT,
    total_charges       DECIMAL(12,2),
    readmission_flag    BOOLEAN     NOT NULL,
    los_percentile      INT,
    cost_rank           INT,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/ehr/fact_admissions';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_admissions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_readmission_rates (
    department_name     STRING      NOT NULL,
    period              STRING      NOT NULL,
    total_admissions    INT         NOT NULL,
    readmissions        INT         NOT NULL,
    readmission_pct     DECIMAL(5,2),
    avg_los             DECIMAL(5,2),
    avg_charges         DECIMAL(12,2),
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/ehr/kpi_readmission_rates';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_readmission_rates TO USER {{current_user}};

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (ssn) TRANSFORM redact PARAMS ('replacement' = '***-**-****');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (email) TRANSFORM mask PARAMS ('visible_chars' = '2', 'mask_char' = '*');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.admissions_cleaned (patient_name) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (ssn) TRANSFORM redact PARAMS ('replacement' = '***-**-****');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (email) TRANSFORM mask PARAMS ('visible_chars' = '2', 'mask_char' = '*');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.patients_deduped (patient_name) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');
