-- =============================================================================
-- Healthcare Patient EHR Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE healthcare_patient_ehr_03_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== BRONZE SEED DATA: DEPARTMENTS (9 rows) =====================

MERGE INTO ehr.bronze.raw_departments AS target
USING (VALUES
  ('CARD', 'Cardiology', 3, 'East', '2024-01-01T00:00:00'),
  ('ORTH', 'Orthopedics', 2, 'West', '2024-01-01T00:00:00'),
  ('NEUR', 'Neurology', 4, 'East', '2024-01-01T00:00:00'),
  ('PULM', 'Pulmonology', 3, 'West', '2024-01-01T00:00:00'),
  ('GAST', 'Gastroenterology', 2, 'East', '2024-01-01T00:00:00'),
  ('ONCO', 'Oncology', 5, 'North', '2024-01-01T00:00:00'),
  ('ENDO', 'Endocrinology', 4, 'West', '2024-01-01T00:00:00'),
  ('NEPH', 'Nephrology', 3, 'North', '2024-01-01T00:00:00'),
  ('EMER', 'Emergency Medicine', 1, 'South', '2024-01-01T00:00:00')
) AS source(department_code, department_name, floor, wing, ingested_at)
ON target.department_code = source.department_code
WHEN MATCHED THEN UPDATE SET
  department_name = source.department_name,
  floor = source.floor,
  wing = source.wing,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (department_code, department_name, floor, wing, ingested_at)
  VALUES (source.department_code, source.department_name, source.floor, source.wing, source.ingested_at);

ASSERT ROW_COUNT = 9
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_departments;

-- ===================== BRONZE SEED DATA: DIAGNOSES (16 rows) =====================

MERGE INTO ehr.bronze.raw_diagnoses AS target
USING (VALUES
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
  ('I48.91', 'Unspecified atrial fibrillation', 'Cardiac', 'Medium', '2024-01-01T00:00:00')
) AS source(diagnosis_code, description, category, severity, ingested_at)
ON target.diagnosis_code = source.diagnosis_code
WHEN MATCHED THEN UPDATE SET
  description = source.description,
  category = source.category,
  severity = source.severity,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (diagnosis_code, description, category, severity, ingested_at)
  VALUES (source.diagnosis_code, source.description, source.category, source.severity, source.ingested_at);

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_diagnoses;

-- ===================== BRONZE SEED DATA: RAW PATIENTS (25 rows) =====================
-- 3 patients (P1001, P1004, P1008) have address changes that will drive SCD2 versioning
-- Each appears twice: original row + updated row with new address/insurance

MERGE INTO ehr.bronze.raw_patients AS target
USING (VALUES
  ('P1001', '  John Smith  ', '123-45-6789', '1965-03-12', 'john.smith@email.com', '100 Oak Lane', 'Hartford', 'CT', 'INS-BC-001', 'BlueCross Shield', '2024-01-15T08:00:00'),
  ('P1002', 'Maria Garcia  ', '234-56-7890', '1978-07-25', 'maria.garcia@mail.com', '245 Pine Street', 'New Haven', 'CT', 'INS-AE-002', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1003', 'Robert Johnson', '345-67-8901', '1982-11-03', 'r.johnson@email.com', '78 Maple Drive', 'Stamford', 'CT', 'INS-UH-003', 'UnitedHealth', '2024-01-15T08:00:00'),
  ('P1004', ' Emily Davis', '456-78-9012', '1990-01-18', 'emily.d@hospital.org', '312 Elm Avenue', 'Bridgeport', 'CT', 'INS-BC-004', 'BlueCross Shield', '2024-01-15T08:00:00'),
  ('P1005', 'Michael Brown', '567-89-0123', '1955-09-30', 'mbrown@webmail.com', '567 Cedar Road', 'Waterbury', 'CT', 'INS-CI-005', 'Cigna Health', '2024-01-15T08:00:00'),
  ('P1006', 'Sarah Wilson  ', '678-90-1234', '1971-04-14', 'sarah.w@inbox.com', '890 Birch Lane', 'Danbury', 'CT', 'INS-AE-006', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1007', 'David Lee ', '789-01-2345', '1988-12-07', 'dlee@company.com', '123 Walnut Street', 'Norwalk', 'CT', 'INS-UH-007', 'UnitedHealth', '2024-01-15T08:00:00'),
  ('P1008', ' Jennifer Martinez', '890-12-3456', '1960-06-22', 'jmartinez@email.com', '456 Spruce Way', 'Meriden', 'CT', 'INS-BC-008', 'BlueCross Shield', '2024-01-15T08:00:00'),
  ('P1009', 'William Taylor', '901-23-4567', '1975-08-09', 'wtaylor@mail.com', '789 Hickory Court', 'Bristol', 'CT', 'INS-CI-009', 'Cigna Health', '2024-01-15T08:00:00'),
  ('P1010', 'Lisa Anderson ', '012-34-5678', '1983-02-28', 'landerson@webmail.com', '321 Ash Drive', 'Milford', 'CT', 'INS-AE-010', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1011', 'James Thomas', '111-22-3333', '1968-10-15', 'jthomas@email.com', '654 Poplar Lane', 'Shelton', 'CT', 'INS-UH-011', 'UnitedHealth', '2024-01-15T08:00:00'),
  ('P1012', '  Patricia White  ', '222-33-4444', '1992-05-01', 'pwhite@mail.com', '987 Cypress Blvd', 'Torrington', 'CT', 'INS-BC-012', 'BlueCross Shield', '2024-01-15T08:00:00'),
  ('P1013', 'Charles Harris', '333-44-5555', '1957-03-19', 'charris@inbox.com', '147 Magnolia Way', 'Enfield', 'CT', 'INS-CI-013', 'Cigna Health', '2024-01-15T08:00:00'),
  ('P1014', 'Nancy Clark  ', '444-55-6666', '1980-09-04', 'nclark@hospital.org', '258 Redwood Terr', 'Vernon', 'CT', 'INS-AE-014', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1015', 'Daniel Lewis', '555-66-7777', '1973-12-21', 'dlewis@email.com', '369 Sequoia Place', 'Glastonbury', 'CT', 'INS-UH-015', 'UnitedHealth', '2024-01-15T08:00:00'),
  ('P1016', 'Karen Robinson', '666-77-8888', '1985-06-10', 'krobinson@mail.com', '480 Willow Park', 'Simsbury', 'CT', 'INS-BC-016', 'BlueCross Shield', '2024-01-15T08:00:00'),
  ('P1017', 'Steven Walker', '777-88-9999', '1962-01-27', 'swalker@company.com', '591 Juniper Hill', 'Avon', 'CT', 'INS-CI-017', 'Cigna Health', '2024-01-15T08:00:00'),
  ('P1018', '  Angela Hall  ', '888-99-0000', '1995-08-16', 'ahall@webmail.com', '702 Hazel Court', 'Canton', 'CT', 'INS-AE-018', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1019', 'Mark Young', '999-00-1111', '1970-04-05', 'myoung@email.com', '813 Beech Drive', 'Farmington', 'CT', 'INS-UH-019', 'UnitedHealth', '2024-01-15T08:00:00'),
  ('P1020', 'Betty King', '100-20-3040', '1987-11-13', 'bking@inbox.com', '924 Linden Way', 'Southington', 'CT', 'INS-BC-020', 'BlueCross Shield', '2024-01-15T08:00:00'),
  -- Address change records for SCD2 (3 patients moved)
  ('P1001', 'John Smith', '123-45-6789', '1965-03-12', 'john.smith@email.com', '500 River Road', 'West Hartford', 'CT', 'INS-BC-001', 'BlueCross Shield', '2024-06-01T08:00:00'),
  ('P1004', 'Emily Davis', '456-78-9012', '1990-01-18', 'emily.d@hospital.org', '88 Summit Ave', 'Fairfield', 'CT', 'INS-UH-044', 'UnitedHealth', '2024-06-01T08:00:00'),
  ('P1008', 'Jennifer Martinez', '890-12-3456', '1960-06-22', 'jmartinez@email.com', '220 Harbor View', 'Milford', 'CT', 'INS-CI-088', 'Cigna Health', '2024-06-01T08:00:00'),
  -- Additional patients to reach 25 rows
  ('P1021', 'Rachel Green', '200-30-4050', '1991-02-14', 'rgreen@email.com', '335 Harvest Lane', 'Middletown', 'CT', 'INS-AE-021', 'Aetna Health', '2024-01-15T08:00:00'),
  ('P1022', 'Thomas Baker', '300-40-5060', '1977-07-30', 'tbaker@mail.com', '446 Summit Circle', 'New London', 'CT', 'INS-CI-022', 'Cigna Health', '2024-01-15T08:00:00')
) AS source(patient_id, patient_name, ssn, date_of_birth, email, address, city, state, insurance_id, insurance_name, ingested_at)
ON target.patient_id = source.patient_id AND target.ingested_at = source.ingested_at
WHEN MATCHED THEN UPDATE SET
  patient_name = source.patient_name,
  ssn = source.ssn,
  date_of_birth = source.date_of_birth,
  email = source.email,
  address = source.address,
  city = source.city,
  state = source.state,
  insurance_id = source.insurance_id,
  insurance_name = source.insurance_name
WHEN NOT MATCHED THEN INSERT (patient_id, patient_name, ssn, date_of_birth, email, address, city, state, insurance_id, insurance_name, ingested_at)
  VALUES (source.patient_id, source.patient_name, source.ssn, source.date_of_birth, source.email, source.address, source.city, source.state, source.insurance_id, source.insurance_name, source.ingested_at);

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_patients;


-- ===================== BRONZE SEED DATA: RAW ADMISSIONS (55 rows) =====================
-- 8 duplicates (record_ids A001,A003,A008 appear twice + A009,A016 appear twice), 5 readmissions within 30 days
-- Spans Jan 2023 - Jan 2024

MERGE INTO ehr.bronze.raw_admissions AS target
USING (VALUES
  ('A001', 'P1001', 'CARD', 'I21.0', '2023-03-15', '2023-03-22', 45200.00, 'Dr. Rivera', 'STEMI anterior wall', '2024-01-15T08:00:00'),
  ('A002', 'P1001', 'CARD', 'I25.10', '2023-04-10', '2023-04-14', 12800.00, 'Dr. Rivera', 'Follow-up readmission within 30d', '2024-01-15T08:00:00'),
  ('A003', 'P1002', 'ORTH', 'S72.001A', '2023-02-20', '2023-03-05', 67500.00, 'Dr. Chen', 'Hip fracture surgical repair', '2024-01-15T08:00:00'),
  ('A004', 'P1003', 'NEUR', 'G45.9', '2023-05-10', '2023-05-13', 18900.00, 'Dr. Patel', 'TIA evaluation', '2024-01-15T08:00:00'),
  ('A005', 'P1003', 'NEUR', 'G45.9', '2023-06-05', '2023-06-08', 16200.00, 'Dr. Patel', 'Readmission TIA recurrence', '2024-01-15T08:00:00'),
  ('A006', 'P1004', 'PULM', 'J44.1', '2023-04-01', '2023-04-09', 23400.00, 'Dr. Thompson', 'COPD exacerbation', '2024-01-15T08:00:00'),
  ('A007', 'P1004', 'PULM', 'J44.1', '2023-04-28', '2023-05-03', 19800.00, 'Dr. Thompson', 'Readmission COPD within 30d', '2024-01-15T08:00:00'),
  ('A008', 'P1005', 'GAST', 'K35.80', '2023-06-15', '2023-06-18', 31200.00, 'Dr. Nguyen', 'Appendectomy', '2024-01-15T08:00:00'),
  ('A009', 'P1006', 'ONCO', 'C34.90', '2023-01-10', '2023-01-25', 89500.00, 'Dr. Kim', 'Lung cancer initial treatment', '2024-01-15T08:00:00'),
  ('A010', 'P1006', 'ONCO', 'C34.90', '2023-03-05', '2023-03-15', 72300.00, 'Dr. Kim', 'Chemo cycle 2', '2024-01-15T08:00:00'),
  ('A001', 'P1001', 'CARD', 'I21.0', '2023-03-15', '2023-03-22', 45200.00, 'Dr. Rivera', 'DUPLICATE STEMI', '2024-01-15T08:05:00'),
  ('A011', 'P1007', 'ENDO', 'E11.65', '2023-07-20', '2023-07-24', 8900.00, 'Dr. Adams', 'Diabetic hyperglycemia mgmt', '2024-01-15T08:00:00'),
  ('A012', 'P1008', 'NEPH', 'N18.6', '2023-08-01', '2023-08-12', 54600.00, 'Dr. Okafor', 'ESRD dialysis initiation', '2024-01-15T08:00:00'),
  ('A013', 'P1008', 'NEPH', 'N17.9', '2023-08-30', '2023-09-05', 38200.00, 'Dr. Okafor', 'Readmission AKI within 30d', '2024-01-15T08:00:00'),
  ('A014', 'P1009', 'CARD', 'I50.9', '2023-09-10', '2023-09-18', 35700.00, 'Dr. Rivera', 'Heart failure management', '2024-01-15T08:00:00'),
  ('A015', 'P1009', 'CARD', 'I50.9', '2023-10-05', '2023-10-10', 28900.00, 'Dr. Rivera', 'HF readmission within 30d', '2024-01-15T08:00:00'),
  ('A016', 'P1010', 'EMER', 'J18.9', '2023-05-22', '2023-05-28', 21300.00, 'Dr. Park', 'Pneumonia ER admission', '2024-01-15T08:00:00'),
  ('A017', 'P1011', 'ORTH', 'M17.11', '2023-07-01', '2023-07-05', 42100.00, 'Dr. Chen', 'Knee replacement surgery', '2024-01-15T08:00:00'),
  ('A018', 'P1012', 'NEUR', 'G40.909', '2023-08-15', '2023-08-18', 14500.00, 'Dr. Patel', 'Epilepsy evaluation', '2024-01-15T08:00:00'),
  ('A019', 'P1013', 'GAST', 'K80.20', '2023-09-01', '2023-09-04', 27800.00, 'Dr. Nguyen', 'Cholecystectomy', '2024-01-15T08:00:00'),
  ('A020', 'P1014', 'CARD', 'I48.91', '2023-10-20', '2023-10-24', 16700.00, 'Dr. Rivera', 'AFib ablation', '2024-01-15T08:00:00'),
  ('A021', 'P1015', 'PULM', 'J18.9', '2023-11-05', '2023-11-11', 24600.00, 'Dr. Thompson', 'Severe pneumonia', '2024-01-15T08:00:00'),
  ('A022', 'P1015', 'PULM', 'J44.1', '2023-11-28', '2023-12-04', 22100.00, 'Dr. Thompson', 'Readmission COPD exacerbation', '2024-01-15T08:00:00'),
  ('A023', 'P1001', 'CARD', 'I50.9', '2023-11-20', '2023-11-27', 33400.00, 'Dr. Rivera', 'HF admission', '2024-01-15T08:00:00'),
  ('A024', 'P1002', 'ORTH', 'M17.11', '2023-09-12', '2023-09-17', 39800.00, 'Dr. Chen', 'Knee replacement', '2024-01-15T08:00:00'),
  ('A025', 'P1016', 'ONCO', 'C34.90', '2023-04-15', '2023-04-30', 95200.00, 'Dr. Kim', 'Lung cancer surgery + chemo', '2024-01-15T08:00:00'),
  ('A026', 'P1016', 'ONCO', 'C34.90', '2023-05-10', '2023-05-18', 68700.00, 'Dr. Kim', 'Readmission post-op complications', '2024-01-15T08:00:00'),
  ('A027', 'P1017', 'EMER', 'I21.0', '2023-12-01', '2023-12-08', 52300.00, 'Dr. Park', 'ER cardiac event', '2024-01-15T08:00:00'),
  ('A028', 'P1017', 'CARD', 'I25.10', '2023-12-20', '2023-12-24', 18500.00, 'Dr. Rivera', 'Readmission cardiac follow-up', '2024-01-15T08:00:00'),
  ('A029', 'P1018', 'ENDO', 'E11.65', '2023-06-10', '2023-06-14', 7600.00, 'Dr. Adams', 'Diabetes management', '2024-01-15T08:00:00'),
  ('A030', 'P1019', 'NEPH', 'N17.9', '2023-10-15', '2023-10-22', 41300.00, 'Dr. Okafor', 'Acute kidney injury', '2024-01-15T08:00:00'),
  ('A031', 'P1019', 'NEPH', 'N18.6', '2023-11-08', '2023-11-18', 49800.00, 'Dr. Okafor', 'Readmission progressed to ESRD', '2024-01-15T08:00:00'),
  ('A032', 'P1020', 'GAST', 'K35.80', '2023-03-25', '2023-03-28', 29500.00, 'Dr. Nguyen', 'Appendicitis surgery', '2024-01-15T08:00:00'),
  ('A003', 'P1002', 'ORTH', 'S72.001A', '2023-02-20', '2023-03-05', 67500.00, 'Dr. Chen', 'DUPLICATE hip fracture', '2024-01-15T08:10:00'),
  ('A033', 'P1010', 'PULM', 'J44.1', '2023-12-10', '2023-12-16', 20100.00, 'Dr. Thompson', 'COPD management', '2024-01-15T08:00:00'),
  ('A034', 'P1014', 'CARD', 'I48.91', '2023-11-15', '2023-11-18', 14200.00, 'Dr. Rivera', 'AFib follow-up', '2024-01-15T08:00:00'),
  ('A035', 'P1005', 'GAST', 'K80.20', '2023-12-05', '2023-12-09', 26400.00, 'Dr. Nguyen', 'Gallbladder surgery', '2024-01-15T08:00:00'),
  ('A036', 'P1011', 'ORTH', 'M17.11', '2023-07-25', '2023-07-28', 15200.00, 'Dr. Chen', 'Readmission post knee surgery', '2024-01-15T08:00:00'),
  ('A037', 'P1013', 'EMER', 'K35.80', '2023-12-18', '2023-12-21', 33100.00, 'Dr. Park', 'ER abdominal pain', '2024-01-15T08:00:00'),
  ('A038', 'P1004', 'PULM', 'J18.9', '2023-09-15', '2023-09-21', 25800.00, 'Dr. Thompson', 'Pneumonia', '2024-01-15T08:00:00'),
  ('A039', 'P1007', 'ENDO', 'E11.65', '2023-12-01', '2023-12-04', 9200.00, 'Dr. Adams', 'Diabetes follow-up', '2024-01-15T08:00:00'),
  ('A040', 'P1012', 'NEUR', 'G45.9', '2023-11-01', '2023-11-05', 19700.00, 'Dr. Patel', 'TIA evaluation', '2024-01-15T08:00:00'),
  ('A041', 'P1020', 'EMER', 'J18.9', '2023-08-10', '2023-08-15', 22800.00, 'Dr. Park', 'ER pneumonia admission', '2024-01-15T08:00:00'),
  ('A042', 'P1006', 'ONCO', 'C34.90', '2023-06-01', '2023-06-12', 78400.00, 'Dr. Kim', 'Chemo cycle 3', '2024-01-15T08:00:00'),
  ('A043', 'P1009', 'CARD', 'I48.91', '2024-01-05', '2024-01-09', 17300.00, 'Dr. Rivera', 'AFib episode', '2024-01-15T08:00:00'),
  ('A044', 'P1018', 'ENDO', 'E11.65', '2023-12-15', '2023-12-18', 8100.00, 'Dr. Adams', 'Diabetes complications', '2024-01-15T08:00:00'),
  ('A009', 'P1006', 'ONCO', 'C34.90', '2023-01-10', '2023-01-25', 89500.00, 'Dr. Kim', 'DUPLICATE lung cancer', '2024-01-15T08:12:00'),
  ('A045', 'P1003', 'NEUR', 'G40.909', '2023-10-10', '2023-10-14', 15800.00, 'Dr. Patel', 'Epilepsy monitoring', '2024-01-15T08:00:00'),
  ('A046', 'P1015', 'EMER', 'J18.9', '2024-01-02', '2024-01-07', 23900.00, 'Dr. Park', 'ER pneumonia readmission', '2024-01-15T08:00:00'),
  ('A047', 'P1016', 'ONCO', 'C34.90', '2023-07-20', '2023-07-30', 71200.00, 'Dr. Kim', 'Chemo cycle post-surgery', '2024-01-15T08:00:00'),
  ('A016', 'P1010', 'EMER', 'J18.9', '2023-05-22', '2023-05-28', 21300.00, 'Dr. Park', 'DUPLICATE pneumonia ER', '2024-01-15T08:08:00'),
  ('A048', 'P1017', 'CARD', 'I50.9', '2024-01-10', '2024-01-15', 31600.00, 'Dr. Rivera', 'HF management', '2024-01-15T08:00:00'),
  ('A008', 'P1005', 'GAST', 'K35.80', '2023-06-15', '2023-06-18', 31200.00, 'Dr. Nguyen', 'DUPLICATE appendectomy', '2024-01-15T08:12:00'),
  ('A049', 'P1010', 'EMER', 'J18.9', '2024-01-08', '2024-01-13', 24100.00, 'Dr. Park', 'Pneumonia recurrence', '2024-01-15T08:00:00'),
  ('A050', 'P1019', 'NEPH', 'N18.6', '2024-01-05', '2024-01-12', 46500.00, 'Dr. Okafor', 'ESRD dialysis session', '2024-01-15T08:00:00')
) AS source(record_id, patient_id, department_code, diagnosis_code, admission_date, discharge_date, total_charges, attending_physician, notes, ingested_at)
ON target.record_id = source.record_id AND target.ingested_at = source.ingested_at
WHEN MATCHED THEN UPDATE SET
  patient_id = source.patient_id,
  department_code = source.department_code,
  diagnosis_code = source.diagnosis_code,
  admission_date = source.admission_date,
  discharge_date = source.discharge_date,
  total_charges = source.total_charges,
  attending_physician = source.attending_physician,
  notes = source.notes
WHEN NOT MATCHED THEN INSERT (record_id, patient_id, department_code, diagnosis_code, admission_date, discharge_date, total_charges, attending_physician, notes, ingested_at)
  VALUES (source.record_id, source.patient_id, source.department_code, source.diagnosis_code, source.admission_date, source.discharge_date, source.total_charges, source.attending_physician, source.notes, source.ingested_at);

ASSERT ROW_COUNT = 55
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_admissions;
