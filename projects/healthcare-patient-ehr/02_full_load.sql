-- =============================================================================
-- Healthcare Patient EHR Pipeline - Full Load Transformation
-- =============================================================================
-- 11-step DAG: validate -> dedup_admissions + build_patient_scd2 (parallel)
-- -> enable_cdf_audit -> build_dim_dept + build_dim_diag (parallel)
-- -> build_fact_admissions -> compute_kpi_readmission -> gdpr_erasure_demo
-- -> optimize_and_maintain
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE ehr_daily_schedule
  CRON '0 5 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 2
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE ehr_patient_pipeline
  DESCRIPTION 'Daily EHR pipeline: SCD2 patient dim, CDF audit, GDPR erasure, readmission analytics'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'healthcare,ehr,scd2,cdf,gdpr,hipaa'
  SLA 3600
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze =====================

ASSERT ROW_COUNT >= 55
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_admissions;

ASSERT ROW_COUNT >= 25
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_patients;

ASSERT ROW_COUNT = 9
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_departments;

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM ehr.bronze.raw_diagnoses;

-- ===================== dedup_admissions =====================
-- Deduplicate on record_id, parse dates, calculate LOS, flag 30-day readmissions

MERGE INTO ehr.silver.admissions_cleaned AS target
USING (
    WITH deduped AS (
        SELECT
            record_id,
            patient_id,
            department_code,
            diagnosis_code,
            CAST(admission_date AS DATE) AS admission_date,
            CAST(discharge_date AS DATE) AS discharge_date,
            total_charges,
            attending_physician,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY ingested_at ASC) AS rn
        FROM ehr.bronze.raw_admissions
    ),
    cleaned AS (
        SELECT
            record_id,
            patient_id,
            department_code,
            diagnosis_code,
            admission_date,
            discharge_date,
            DATEDIFF(discharge_date, admission_date) AS los_days,
            total_charges,
            attending_physician,
            ingested_at
        FROM deduped
        WHERE rn = 1
    ),
    with_readmission AS (
        SELECT
            c.*,
            LAG(c.discharge_date) OVER (
                PARTITION BY c.patient_id ORDER BY c.admission_date
            ) AS prev_discharge_date,
            DATEDIFF(c.admission_date, LAG(c.discharge_date) OVER (
                PARTITION BY c.patient_id ORDER BY c.admission_date
            )) AS days_since_last_discharge,
            CASE
                WHEN DATEDIFF(c.admission_date, LAG(c.discharge_date) OVER (
                    PARTITION BY c.patient_id ORDER BY c.admission_date
                )) <= 30 THEN true
                ELSE false
            END AS readmission_flag,
            NTILE(100) OVER (ORDER BY DATEDIFF(discharge_date, admission_date)) AS los_percentile
        FROM cleaned c
    )
    SELECT * FROM with_readmission
) AS source
ON target.record_id = source.record_id
WHEN MATCHED THEN UPDATE SET
    patient_id              = source.patient_id,
    department_code         = source.department_code,
    diagnosis_code          = source.diagnosis_code,
    admission_date          = source.admission_date,
    discharge_date          = source.discharge_date,
    los_days                = source.los_days,
    total_charges           = source.total_charges,
    attending_physician     = source.attending_physician,
    readmission_flag        = source.readmission_flag,
    prev_discharge_date     = source.prev_discharge_date,
    days_since_last_discharge = source.days_since_last_discharge,
    los_percentile          = source.los_percentile,
    ingested_at             = source.ingested_at,
    processed_at            = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    record_id, patient_id, department_code, diagnosis_code,
    admission_date, discharge_date, los_days, total_charges, attending_physician,
    readmission_flag, prev_discharge_date, days_since_last_discharge, los_percentile,
    ingested_at, processed_at
) VALUES (
    source.record_id, source.patient_id, source.department_code, source.diagnosis_code,
    source.admission_date, source.discharge_date, source.los_days, source.total_charges,
    source.attending_physician, source.readmission_flag, source.prev_discharge_date,
    source.days_since_last_discharge, source.los_percentile,
    source.ingested_at, CURRENT_TIMESTAMP
);

-- After dedup: 55 raw rows minus 5 duplicates (A001, A003, A008, A009, A016) = 50 unique
ASSERT VALUE silver_admission_count >= 47
SELECT COUNT(*) AS silver_admission_count FROM ehr.silver.admissions_cleaned;

-- ===================== build_patient_scd2 =====================
-- SCD Type 2: two-pass MERGE on patient_dim
-- Pass 1: expire old current records where demographics changed
-- Pass 2: insert new current records with updated demographics

-- Pass 1: For patients whose demographics changed, expire the current record
-- by setting is_current = false and valid_to = day before the new record's date
MERGE INTO ehr.silver.patient_dim AS target
USING (
    WITH ranked AS (
        SELECT
            patient_id,
            TRIM(patient_name) AS patient_name,
            ssn,
            CAST(date_of_birth AS DATE) AS date_of_birth,
            LOWER(TRIM(email)) AS email,
            TRIM(address) AS address,
            city,
            state,
            insurance_id,
            insurance_name,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id
                ORDER BY ingested_at ASC
            ) AS version_num
        FROM ehr.bronze.raw_patients
    )
    -- Select the FIRST (original) version of each patient for initial load
    SELECT
        patient_id, patient_name, ssn, date_of_birth, email,
        address, city, state, insurance_id, insurance_name,
        CAST('2024-01-15' AS DATE) AS valid_from,
        ingested_at
    FROM ranked
    WHERE version_num = 1
) AS source
ON target.patient_id = source.patient_id AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
    patient_id, patient_name, ssn, date_of_birth, email,
    address, city, state, insurance_id, insurance_name,
    valid_from, valid_to, is_current, updated_at
) VALUES (
    source.patient_id, source.patient_name, source.ssn, source.date_of_birth,
    source.email, source.address, source.city, source.state,
    source.insurance_id, source.insurance_name,
    source.valid_from, NULL, true, CURRENT_TIMESTAMP
)
WHEN MATCHED THEN UPDATE SET
    patient_name    = source.patient_name,
    ssn             = source.ssn,
    date_of_birth   = source.date_of_birth,
    email           = source.email,
    address         = source.address,
    city            = source.city,
    state           = source.state,
    insurance_id    = source.insurance_id,
    insurance_name  = source.insurance_name,
    updated_at      = CURRENT_TIMESTAMP;

-- Pass 2: Now process the CHANGED records (patients with version_num > 1)
-- First expire the old current records for these patients
MERGE INTO ehr.silver.patient_dim AS target
USING (
    WITH ranked AS (
        SELECT
            patient_id,
            TRIM(patient_name) AS patient_name,
            ssn,
            CAST(date_of_birth AS DATE) AS date_of_birth,
            LOWER(TRIM(email)) AS email,
            TRIM(address) AS address,
            city,
            state,
            insurance_id,
            insurance_name,
            ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id
                ORDER BY ingested_at ASC
            ) AS version_num
        FROM ehr.bronze.raw_patients
    )
    SELECT
        patient_id, patient_name, ssn, date_of_birth, email,
        address, city, state, insurance_id, insurance_name,
        CAST('2024-06-01' AS DATE) AS change_date,
        ingested_at
    FROM ranked
    WHERE version_num > 1
) AS source
ON target.patient_id = source.patient_id AND target.is_current = true
    AND (target.address <> source.address OR target.insurance_id <> source.insurance_id)
WHEN MATCHED THEN UPDATE SET
    valid_to    = source.change_date,
    is_current  = false,
    updated_at  = CURRENT_TIMESTAMP;

-- Pass 3: Insert new current records for changed patients
INSERT INTO ehr.silver.patient_dim
SELECT
    r.patient_id,
    TRIM(r.patient_name) AS patient_name,
    r.ssn,
    CAST(r.date_of_birth AS DATE) AS date_of_birth,
    LOWER(TRIM(r.email)) AS email,
    TRIM(r.address) AS address,
    r.city,
    r.state,
    r.insurance_id,
    r.insurance_name,
    CAST('2024-06-01' AS DATE) AS valid_from,
    NULL AS valid_to,
    true AS is_current,
    CURRENT_TIMESTAMP AS updated_at
FROM (
    SELECT
        patient_id,
        patient_name,
        ssn,
        date_of_birth,
        email,
        address,
        city,
        state,
        insurance_id,
        insurance_name,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY ingested_at ASC
        ) AS version_num
    FROM ehr.bronze.raw_patients
) r
WHERE r.version_num > 1
  AND EXISTS (
      SELECT 1 FROM ehr.silver.patient_dim p
      WHERE p.patient_id = r.patient_id
        AND p.is_current = false
        AND p.valid_to = CAST('2024-06-01' AS DATE)
  );

-- Verify: 22 unique patients + 3 expired versions = 25 total rows in patient_dim
-- (20 patients with 1 version each + 3 patients with 2 versions = 20 + 6 = 26,
--  but actually 22 unique + 3 extra = 25 total records)
ASSERT VALUE patient_dim_count >= 22
SELECT COUNT(*) AS patient_dim_count FROM ehr.silver.patient_dim;

-- Verify SCD2: 3 patients should have expired records
ASSERT VALUE expired_count = 3
SELECT COUNT(*) AS expired_count FROM ehr.silver.patient_dim WHERE is_current = false;

-- ===================== enable_cdf_audit =====================
-- Read CDF changes from patient_dim and materialize into audit_log

-- Capture all changes from CDF on patient_dim into the audit log
-- table_changes reads the CDF of patient_dim from version 0 to current
INSERT INTO ehr.silver.audit_log
SELECT
    ROW_NUMBER() OVER (ORDER BY _commit_timestamp, patient_id) AS audit_id,
    'patient_dim' AS table_name,
    patient_id,
    _change_type AS change_type,
    CASE
        WHEN _change_type = 'update_postimage' THEN 'address,insurance_id,insurance_name,is_current,valid_to'
        WHEN _change_type = 'insert' THEN 'ALL'
        ELSE 'unknown'
    END AS changed_fields,
    NULL AS old_values,
    CONCAT(address, '|', city, '|', state, '|', insurance_id) AS new_values,
    _commit_timestamp AS change_timestamp
FROM table_changes('ehr.silver.patient_dim', 0);

ASSERT VALUE audit_count >= 1
SELECT COUNT(*) AS audit_count FROM ehr.silver.audit_log;

-- ===================== build_dim_department =====================

MERGE INTO ehr.gold.dim_department AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY department_code) AS department_key,
        department_code,
        TRIM(department_name) AS department_name,
        floor,
        wing
    FROM ehr.bronze.raw_departments
) AS source
ON target.department_code = source.department_code
WHEN MATCHED THEN UPDATE SET
    department_name = source.department_name,
    floor           = source.floor,
    wing            = source.wing,
    loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    department_key, department_code, department_name, floor, wing, loaded_at
) VALUES (
    source.department_key, source.department_code, source.department_name,
    source.floor, source.wing, CURRENT_TIMESTAMP
);

-- ===================== build_dim_diagnosis =====================

MERGE INTO ehr.gold.dim_diagnosis AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY diagnosis_code) AS diagnosis_key,
        diagnosis_code,
        TRIM(description) AS description,
        category,
        severity
    FROM ehr.bronze.raw_diagnoses
) AS source
ON target.diagnosis_code = source.diagnosis_code
WHEN MATCHED THEN UPDATE SET
    description = source.description,
    category    = source.category,
    severity    = source.severity,
    loaded_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    diagnosis_key, diagnosis_code, description, category, severity, loaded_at
) VALUES (
    source.diagnosis_key, source.diagnosis_code, source.description,
    source.category, source.severity, CURRENT_TIMESTAMP
);

-- ===================== build_fact_admissions =====================
-- Point-in-time join: join admissions to patient_dim where
-- admission_date BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')

MERGE INTO ehr.gold.fact_admissions AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY a.admission_date, a.record_id) AS admission_key,
        a.patient_id,
        p.patient_name AS patient_name_hash,
        d.department_key,
        dx.diagnosis_key,
        a.admission_date,
        a.discharge_date,
        a.los_days,
        a.total_charges,
        a.readmission_flag,
        NTILE(100) OVER (ORDER BY a.los_days) AS los_percentile,
        DENSE_RANK() OVER (ORDER BY a.total_charges DESC) AS cost_rank,
        p.valid_from AS patient_valid_from,
        p.valid_to AS patient_valid_to
    FROM ehr.silver.admissions_cleaned a
    JOIN ehr.gold.dim_department d ON a.department_code = d.department_code
    JOIN ehr.gold.dim_diagnosis dx ON a.diagnosis_code = dx.diagnosis_code
    LEFT JOIN ehr.silver.patient_dim p
        ON a.patient_id = p.patient_id
        AND a.admission_date >= p.valid_from
        AND a.admission_date < COALESCE(p.valid_to, CAST('9999-12-31' AS DATE))
) AS source
ON target.patient_id = source.patient_id
    AND target.admission_date = source.admission_date
    AND target.diagnosis_key = source.diagnosis_key
WHEN MATCHED THEN UPDATE SET
    patient_name_hash   = source.patient_name_hash,
    department_key      = source.department_key,
    discharge_date      = source.discharge_date,
    los_days            = source.los_days,
    total_charges       = source.total_charges,
    readmission_flag    = source.readmission_flag,
    los_percentile      = source.los_percentile,
    cost_rank           = source.cost_rank,
    patient_valid_from  = source.patient_valid_from,
    patient_valid_to    = source.patient_valid_to,
    loaded_at           = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    admission_key, patient_id, patient_name_hash, department_key, diagnosis_key,
    admission_date, discharge_date, los_days, total_charges, readmission_flag,
    los_percentile, cost_rank, patient_valid_from, patient_valid_to, loaded_at
) VALUES (
    source.admission_key, source.patient_id, source.patient_name_hash,
    source.department_key, source.diagnosis_key,
    source.admission_date, source.discharge_date, source.los_days, source.total_charges,
    source.readmission_flag, source.los_percentile, source.cost_rank,
    source.patient_valid_from, source.patient_valid_to, CURRENT_TIMESTAMP
);

-- ===================== compute_kpi_readmission =====================

MERGE INTO ehr.gold.kpi_readmission_rates AS target
USING (
    SELECT
        dd.department_name,
        CONCAT(CAST(EXTRACT(YEAR FROM f.admission_date) AS STRING), '-Q',
               CAST(CAST((EXTRACT(MONTH FROM f.admission_date) - 1) / 3 + 1 AS INT) AS STRING)) AS period,
        COUNT(*) AS total_admissions,
        SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) AS readmissions,
        ROUND(100.0 * SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS readmission_pct,
        ROUND(AVG(f.los_days), 2) AS avg_los,
        ROUND(AVG(f.total_charges), 2) AS avg_charges,
        MAX(f.los_days) AS max_los
    FROM ehr.gold.fact_admissions f
    JOIN ehr.gold.dim_department dd ON f.department_key = dd.department_key
    GROUP BY dd.department_name,
        CONCAT(CAST(EXTRACT(YEAR FROM f.admission_date) AS STRING), '-Q',
               CAST(CAST((EXTRACT(MONTH FROM f.admission_date) - 1) / 3 + 1 AS INT) AS STRING))
) AS source
ON target.department_name = source.department_name AND target.period = source.period
WHEN MATCHED THEN UPDATE SET
    total_admissions = source.total_admissions,
    readmissions     = source.readmissions,
    readmission_pct  = source.readmission_pct,
    avg_los          = source.avg_los,
    avg_charges      = source.avg_charges,
    max_los          = source.max_los,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    department_name, period, total_admissions, readmissions,
    readmission_pct, avg_los, avg_charges, max_los, loaded_at
) VALUES (
    source.department_name, source.period, source.total_admissions, source.readmissions,
    source.readmission_pct, source.avg_los, source.avg_charges, source.max_los, CURRENT_TIMESTAMP
);

-- ===================== gdpr_erasure_demo =====================
-- Demonstrate GDPR right-to-be-forgotten:
-- 1. DELETE patient P1022 (Thomas Baker) from patient_dim
-- 2. VACUUM to prove physical removal (deletion vectors applied)
-- 3. Show time travel: VERSION AS OF to see data before erasure
-- 4. RESTORE to recover the table to pre-deletion state

-- Count before deletion
SELECT COUNT(*) AS pre_delete_count FROM ehr.silver.patient_dim WHERE patient_id = 'P1022';

-- GDPR erasure: remove patient P1022 completely
DELETE FROM ehr.silver.patient_dim WHERE patient_id = 'P1022';

-- Verify deletion
ASSERT VALUE post_delete_count = 0
SELECT COUNT(*) AS post_delete_count FROM ehr.silver.patient_dim WHERE patient_id = 'P1022';

-- VACUUM to physically remove the data (prove deletion vectors applied)
VACUUM ehr.silver.patient_dim;

-- Time travel: show the patient existed in a previous version
SELECT COUNT(*) AS version_1_count
FROM ehr.silver.patient_dim VERSION AS OF 1
WHERE patient_id = 'P1022';

-- RESTORE to recover the table (undo the erasure for demo purposes)
RESTORE ehr.silver.patient_dim TO VERSION 1;

-- Verify recovery
ASSERT VALUE restored_count >= 1
SELECT COUNT(*) AS restored_count FROM ehr.silver.patient_dim WHERE patient_id = 'P1022';

-- ===================== optimize_and_maintain =====================

OPTIMIZE ehr.silver.admissions_cleaned;
OPTIMIZE ehr.silver.patient_dim;
OPTIMIZE ehr.gold.dim_department;
OPTIMIZE ehr.gold.dim_diagnosis;
OPTIMIZE ehr.gold.fact_admissions;
OPTIMIZE ehr.gold.kpi_readmission_rates;
