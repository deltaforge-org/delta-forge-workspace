-- =============================================================================
-- Healthcare Patient EHR Pipeline - Full Load Transformation
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE ehr_daily_schedule CRON '0 5 * * *' TIMEZONE 'America/New_York' RETRIES 2 TIMEOUT 3600 MAX_CONCURRENT 1 ACTIVE;

PIPELINE ehr_patient_pipeline DESCRIPTION 'Daily EHR patient admissions pipeline - bronze to gold medallion transformation' SCHEDULE 'ehr_daily_schedule' TAGS 'healthcare,ehr,admissions,readmission' SLA 3600 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP 1: Validate Bronze Data =====================

-- Verify bronze has data
SELECT COUNT(*) AS raw_admission_count FROM {{zone_prefix}}.bronze.raw_admissions;
ASSERT VALUE raw_admission_count >= 50

SELECT COUNT(*) AS raw_dept_count FROM {{zone_prefix}}.bronze.raw_departments;
ASSERT VALUE raw_dept_count >= 9

SELECT COUNT(*) AS raw_diag_count FROM {{zone_prefix}}.bronze.raw_diagnoses;
ASSERT VALUE raw_diag_count >= 16
SELECT 'raw_diag_count check passed' AS raw_diag_count_status;


-- ===================== STEP 2: Bronze -> Silver (Dedup + Clean + Enrich) =====================

-- Deduplicate admissions: keep the earliest ingestion per record_id
-- Also clean whitespace, parse dates, calculate LOS, flag readmissions
MERGE INTO {{zone_prefix}}.silver.admissions_cleaned AS target
USING (
    WITH deduped AS (
        SELECT
            record_id,
            patient_id,
            TRIM(patient_name) AS patient_name,
            ssn,
            LOWER(TRIM(email)) AS email,
            department_code,
            diagnosis_code,
            CAST(admission_date AS DATE) AS admission_date,
            CAST(discharge_date AS DATE) AS discharge_date,
            total_charges,
            attending_physician,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY ingested_at ASC) AS rn
        FROM {{zone_prefix}}.bronze.raw_admissions
    ),
    cleaned AS (
        SELECT
            record_id,
            patient_id,
            patient_name,
            ssn,
            email,
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
            CASE
                WHEN DATEDIFF(c.admission_date, LAG(c.discharge_date) OVER (
                    PARTITION BY c.patient_id ORDER BY c.admission_date
                )) <= 30 THEN true
                ELSE false
            END AS readmission_flag
        FROM cleaned c
    )
    SELECT * FROM with_readmission
) AS source
ON target.record_id = source.record_id
WHEN MATCHED THEN UPDATE SET
    patient_id          = source.patient_id,
    patient_name        = source.patient_name,
    ssn                 = source.ssn,
    email               = source.email,
    department_code     = source.department_code,
    diagnosis_code      = source.diagnosis_code,
    admission_date      = source.admission_date,
    discharge_date      = source.discharge_date,
    los_days            = source.los_days,
    total_charges       = source.total_charges,
    attending_physician = source.attending_physician,
    readmission_flag    = source.readmission_flag,
    prev_discharge_date = source.prev_discharge_date,
    ingested_at         = source.ingested_at,
    processed_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    record_id, patient_id, patient_name, ssn, email, department_code,
    diagnosis_code, admission_date, discharge_date, los_days, total_charges,
    attending_physician, readmission_flag, prev_discharge_date, ingested_at, processed_at
) VALUES (
    source.record_id, source.patient_id, source.patient_name, source.ssn, source.email,
    source.department_code, source.diagnosis_code, source.admission_date, source.discharge_date,
    source.los_days, source.total_charges, source.attending_physician, source.readmission_flag,
    source.prev_discharge_date, source.ingested_at, CURRENT_TIMESTAMP
);

-- Build patient dedup summary
MERGE INTO {{zone_prefix}}.silver.patients_deduped AS target
USING (
    SELECT
        patient_id,
        FIRST_VALUE(patient_name) OVER (PARTITION BY patient_id ORDER BY admission_date DESC) AS patient_name,
        FIRST_VALUE(ssn) OVER (PARTITION BY patient_id ORDER BY admission_date DESC) AS ssn,
        FIRST_VALUE(email) OVER (PARTITION BY patient_id ORDER BY admission_date DESC) AS email,
        MAX(admission_date) AS last_admission_date,
        COUNT(*) AS total_admissions
    FROM {{zone_prefix}}.silver.admissions_cleaned
    GROUP BY patient_id, patient_name, ssn, email
) AS source
ON target.patient_id = source.patient_id
WHEN MATCHED THEN UPDATE SET
    patient_name        = source.patient_name,
    ssn                 = source.ssn,
    email               = source.email,
    last_admission_date = source.last_admission_date,
    total_admissions    = source.total_admissions,
    processed_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    patient_id, patient_name, ssn, email, last_admission_date, total_admissions, processed_at
) VALUES (
    source.patient_id, source.patient_name, source.ssn, source.email,
    source.last_admission_date, source.total_admissions, CURRENT_TIMESTAMP
);

-- Verify silver dedup removed duplicates
SELECT COUNT(*) AS silver_admission_count FROM {{zone_prefix}}.silver.admissions_cleaned;
-- ===================== STEP 3: Silver -> Gold Dimension Tables =====================

-- dim_department
MERGE INTO {{zone_prefix}}.gold.dim_department AS target
USING (
ASSERT VALUE silver_admission_count = 52
    SELECT
        ROW_NUMBER() OVER (ORDER BY department_code) AS department_key,
        department_code,
        TRIM(department_name) AS department_name,
        floor,
        wing
    FROM {{zone_prefix}}.bronze.raw_departments
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

-- dim_diagnosis
MERGE INTO {{zone_prefix}}.gold.dim_diagnosis AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY diagnosis_code) AS diagnosis_key,
        diagnosis_code,
        TRIM(description) AS description,
        category,
        severity
    FROM {{zone_prefix}}.bronze.raw_diagnoses
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

-- ===================== STEP 4: Silver -> Gold Fact Table =====================

-- fact_admissions with LOS percentiles and cost ranking
MERGE INTO {{zone_prefix}}.gold.fact_admissions AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY a.admission_date, a.record_id) AS admission_key,
        a.patient_id AS patient_key,
        d.department_key,
        dx.diagnosis_key,
        a.admission_date,
        a.discharge_date,
        a.los_days,
        a.total_charges,
        a.readmission_flag,
        NTILE(100) OVER (ORDER BY a.los_days) AS los_percentile,
        DENSE_RANK() OVER (ORDER BY a.total_charges DESC) AS cost_rank
    FROM {{zone_prefix}}.silver.admissions_cleaned a
    JOIN {{zone_prefix}}.gold.dim_department d ON a.department_code = d.department_code
    JOIN {{zone_prefix}}.gold.dim_diagnosis dx ON a.diagnosis_code = dx.diagnosis_code
) AS source
ON target.patient_key = source.patient_key AND target.admission_date = source.admission_date AND target.diagnosis_key = source.diagnosis_key
WHEN MATCHED THEN UPDATE SET
    department_key   = source.department_key,
    discharge_date   = source.discharge_date,
    los_days         = source.los_days,
    total_charges    = source.total_charges,
    readmission_flag = source.readmission_flag,
    los_percentile   = source.los_percentile,
    cost_rank        = source.cost_rank,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    admission_key, patient_key, department_key, diagnosis_key, admission_date,
    discharge_date, los_days, total_charges, readmission_flag, los_percentile,
    cost_rank, loaded_at
) VALUES (
    source.admission_key, source.patient_key, source.department_key, source.diagnosis_key,
    source.admission_date, source.discharge_date, source.los_days, source.total_charges,
    source.readmission_flag, source.los_percentile, source.cost_rank, CURRENT_TIMESTAMP
);

-- ===================== STEP 5: Gold KPI - Readmission Rates =====================

MERGE INTO {{zone_prefix}}.gold.kpi_readmission_rates AS target
USING (
    SELECT
        dd.department_name,
        CONCAT(EXTRACT(YEAR FROM f.admission_date), '-Q', EXTRACT(QUARTER FROM f.admission_date)) AS period,
        COUNT(*) AS total_admissions,
        SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) AS readmissions,
        ROUND(100.0 * SUM(CASE WHEN f.readmission_flag = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS readmission_pct,
        ROUND(AVG(f.los_days), 2) AS avg_los,
        ROUND(AVG(f.total_charges), 2) AS avg_charges
    FROM {{zone_prefix}}.gold.fact_admissions f
    JOIN {{zone_prefix}}.gold.dim_department dd ON f.department_key = dd.department_key
    GROUP BY dd.department_name, CONCAT(EXTRACT(YEAR FROM f.admission_date), '-Q', EXTRACT(QUARTER FROM f.admission_date))
) AS source
ON target.department_name = source.department_name AND target.period = source.period
WHEN MATCHED THEN UPDATE SET
    total_admissions = source.total_admissions,
    readmissions     = source.readmissions,
    readmission_pct  = source.readmission_pct,
    avg_los          = source.avg_los,
    avg_charges      = source.avg_charges,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    department_name, period, total_admissions, readmissions,
    readmission_pct, avg_los, avg_charges, loaded_at
) VALUES (
    source.department_name, source.period, source.total_admissions, source.readmissions,
    source.readmission_pct, source.avg_los, source.avg_charges, CURRENT_TIMESTAMP
);

-- ===================== STEP 6: Optimize Gold Tables =====================

OPTIMIZE {{zone_prefix}}.gold.dim_department;
OPTIMIZE {{zone_prefix}}.gold.dim_diagnosis;
OPTIMIZE {{zone_prefix}}.gold.fact_admissions;
OPTIMIZE {{zone_prefix}}.gold.kpi_readmission_rates;
