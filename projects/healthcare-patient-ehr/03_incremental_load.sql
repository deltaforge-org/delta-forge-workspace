-- =============================================================================
-- Healthcare Patient EHR Pipeline - Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "record_id > 'A052' AND admission_date > '2024-01-15'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.admissions_cleaned, record_id, admission_date, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.admissions_cleaned
-- SELECT * FROM {{zone_prefix}}.bronze.raw_admissions
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.admissions_cleaned, record_id, admission_date, 3)}};

-- ===================== STEP 1: Capture Current Watermark =====================

SELECT MAX(ingested_at) AS current_watermark FROM {{zone_prefix}}.silver.admissions_cleaned;

SELECT COUNT(*) AS pre_silver_count FROM {{zone_prefix}}.silver.admissions_cleaned;

SELECT COUNT(*) AS pre_gold_fact_count FROM {{zone_prefix}}.gold.fact_admissions;

-- ===================== STEP 2: Insert New Bronze Incremental Records =====================

INSERT INTO {{zone_prefix}}.bronze.raw_admissions VALUES
    ('A053', 'P1021', 'Rachel Green  ', '200-30-4050', 'rgreen@email.com', 'CARD', 'I21.0', '2024-01-20', '2024-01-28', 48700.00, 'Dr. Rivera', 'New STEMI patient', '2024-01-20T06:00:00'),
    ('A054', 'P1021', 'Rachel Green', '200-30-4050', 'rgreen@email.com', 'CARD', 'I25.10', '2024-02-12', '2024-02-15', 13200.00, 'Dr. Rivera', 'Readmission within 30d cardiac', '2024-01-20T06:00:00'),
    ('A055', 'P1022', 'Thomas Baker', '300-40-5060', 'tbaker@mail.com', 'NEUR', 'G45.9', '2024-01-25', '2024-01-28', 17800.00, 'Dr. Patel', 'TIA new patient', '2024-01-20T06:00:00'),
    ('A056', 'P1023', ' Monica Reyes ', '400-50-6070', 'mreyes@inbox.com', 'ONCO', 'C34.90', '2024-02-01', '2024-02-14', 91200.00, 'Dr. Kim', 'Lung cancer initial staging', '2024-01-20T06:00:00'),
    ('A057', 'P1001', 'John Smith', '123-45-6789', 'john.smith@email.com', 'CARD', 'I50.9', '2024-02-05', '2024-02-10', 29800.00, 'Dr. Rivera', 'Existing patient new admission', '2024-01-20T06:00:00'),
    ('A058', 'P1024', 'Kevin Huang', '500-60-7080', 'khuang@company.com', 'GAST', 'K80.20', '2024-02-08', '2024-02-11', 25600.00, 'Dr. Nguyen', 'Gallstone surgery', '2024-01-20T06:00:00'),
    ('A059', 'P1008', 'Jennifer Martinez', '890-12-3456', 'jmartinez@email.com', 'NEPH', 'N18.6', '2024-02-10', '2024-02-17', 43900.00, 'Dr. Okafor', 'Existing ESRD patient dialysis', '2024-01-20T06:00:00'),
    ('A053', 'P1021', 'Rachel Green  ', '200-30-4050', 'rgreen@email.com', 'CARD', 'I21.0', '2024-01-20', '2024-01-28', 48700.00, 'Dr. Rivera', 'DUPLICATE new patient', '2024-01-20T06:05:00');

-- ===================== STEP 3: Incremental MERGE to Silver =====================
-- Only process records newer than the watermark

MERGE INTO {{zone_prefix}}.silver.admissions_cleaned AS target
USING (
    WITH new_records AS (
        SELECT *
        FROM {{zone_prefix}}.bronze.raw_admissions
        WHERE ingested_at > (
            SELECT COALESCE(MAX(ingested_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.admissions_cleaned
        )
    ),
    deduped AS (
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
        FROM new_records
    ),
    cleaned AS (
        SELECT
            record_id, patient_id, patient_name, ssn, email,
            department_code, diagnosis_code, admission_date, discharge_date,
            DATEDIFF(discharge_date, admission_date) AS los_days,
            total_charges, attending_physician, ingested_at
        FROM deduped WHERE rn = 1
    ),
    with_history AS (
        SELECT
            c.*,
            LAG(prev_dis.discharge_date) OVER (
                PARTITION BY c.patient_id ORDER BY c.admission_date
            ) AS prev_discharge_date,
            CASE
                WHEN DATEDIFF(c.admission_date, LAG(prev_dis.discharge_date) OVER (
                    PARTITION BY c.patient_id ORDER BY c.admission_date
                )) <= 30 THEN true
                ELSE false
            END AS readmission_flag
        FROM cleaned c
        LEFT JOIN (
            SELECT patient_id, discharge_date
            FROM {{zone_prefix}}.silver.admissions_cleaned
            UNION ALL
            SELECT patient_id, discharge_date FROM cleaned
        ) prev_dis ON c.patient_id = prev_dis.patient_id AND prev_dis.discharge_date < c.admission_date
    )
    SELECT DISTINCT record_id, patient_id, patient_name, ssn, email,
        department_code, diagnosis_code, admission_date, discharge_date,
        los_days, total_charges, attending_physician,
        COALESCE(readmission_flag, false) AS readmission_flag,
        prev_discharge_date, ingested_at
    FROM with_history
) AS source
ON target.record_id = source.record_id
WHEN MATCHED THEN UPDATE SET
    patient_name        = source.patient_name,
    department_code     = source.department_code,
    diagnosis_code      = source.diagnosis_code,
    admission_date      = source.admission_date,
    discharge_date      = source.discharge_date,
    los_days            = source.los_days,
    total_charges       = source.total_charges,
    readmission_flag    = source.readmission_flag,
    prev_discharge_date = source.prev_discharge_date,
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

-- ===================== STEP 4: Verify Incremental Processing =====================

SELECT COUNT(*) AS post_silver_count FROM {{zone_prefix}}.silver.admissions_cleaned;

-- Should have added 7 new unique records (8 inserted minus 1 duplicate)
SELECT COUNT(*) AS new_records_added
FROM {{zone_prefix}}.silver.admissions_cleaned
WHERE processed_at > (SELECT MAX(loaded_at) FROM {{zone_prefix}}.gold.fact_admissions);

-- Verify no duplicates exist in silver
SELECT COUNT(*) AS duplicate_check
FROM (
    SELECT record_id, COUNT(*) AS cnt
    FROM {{zone_prefix}}.silver.admissions_cleaned
    GROUP BY record_id
    HAVING COUNT(*) > 1
);
-- ===================== STEP 5: Refresh Gold Tables (incremental) =====================

-- Re-run gold fact with full rebuild (idempotent via MERGE)
MERGE INTO {{zone_prefix}}.gold.fact_admissions AS target
USING (
ASSERT VALUE duplicate_check = 0
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

SELECT COUNT(*) AS post_gold_fact_count FROM {{zone_prefix}}.gold.fact_admissions;

-- Verify gold grew
SELECT
    post.cnt - pre.cnt AS net_new_facts
FROM
    (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.gold.fact_admissions) post,
    (SELECT 52 AS cnt) pre;

ASSERT VALUE net_new_facts >= 7
SELECT 'net_new_facts check passed' AS net_new_facts_status;

