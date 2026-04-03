-- =============================================================================
-- Healthcare Patient EHR Pipeline - Incremental Load
-- =============================================================================
-- Inserts 8 new admissions + 2 patient address changes, processes via
-- INCREMENTAL_FILTER macro, and verifies only new records flow through.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically with overlap_days for late-arriving
-- data. This eliminates the need to manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "record_id > 'A050' AND admission_date > '2024-01-02'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(ehr.silver.admissions_cleaned, record_id, admission_date, 3)}};

-- ===================== STEP 1: Capture Current Watermarks =====================

SELECT MAX(ingested_at) AS current_watermark FROM ehr.silver.admissions_cleaned;

SELECT COUNT(*) AS pre_silver_count FROM ehr.silver.admissions_cleaned;

SELECT COUNT(*) AS pre_gold_fact_count FROM ehr.gold.fact_admissions;

SELECT COUNT(*) AS pre_patient_dim_count FROM ehr.silver.patient_dim;

-- ===================== STEP 2: Insert New Bronze Incremental Records =====================
-- 8 new admissions including 1 duplicate, 1 readmission within 30 days

INSERT INTO ehr.bronze.raw_admissions VALUES
    ('A051', 'P1021', 'CARD', 'I21.0', '2024-01-20', '2024-01-28', 48700.00, 'Dr. Rivera', 'New STEMI patient Rachel Green', '2024-01-20T06:00:00'),
    ('A052', 'P1021', 'CARD', 'I25.10', '2024-02-12', '2024-02-15', 13200.00, 'Dr. Rivera', 'Readmission within 30d cardiac', '2024-01-20T06:00:00'),
    ('A053', 'P1022', 'NEUR', 'G45.9', '2024-01-25', '2024-01-28', 17800.00, 'Dr. Patel', 'TIA new patient Thomas Baker', '2024-01-20T06:00:00'),
    ('A054', 'P1001', 'CARD', 'I50.9', '2024-02-05', '2024-02-10', 29800.00, 'Dr. Rivera', 'Existing patient John Smith HF', '2024-01-20T06:00:00'),
    ('A055', 'P1008', 'NEPH', 'N18.6', '2024-02-10', '2024-02-17', 43900.00, 'Dr. Okafor', 'Existing ESRD patient dialysis', '2024-01-20T06:00:00'),
    ('A056', 'P1014', 'CARD', 'I50.9', '2024-02-08', '2024-02-14', 37200.00, 'Dr. Rivera', 'Nancy Clark new HF episode', '2024-01-20T06:00:00'),
    ('A057', 'P1016', 'ONCO', 'C34.90', '2024-02-15', '2024-02-28', 82100.00, 'Dr. Kim', 'Karen Robinson chemo cycle 4', '2024-01-20T06:00:00'),
    ('A051', 'P1021', 'CARD', 'I21.0', '2024-01-20', '2024-01-28', 48700.00, 'Dr. Rivera', 'DUPLICATE Rachel Green', '2024-01-20T06:05:00');

-- 2 patient address changes for incremental SCD2 processing
INSERT INTO ehr.bronze.raw_patients VALUES
    ('P1009', 'William Taylor', '901-23-4567', '1975-08-09', 'wtaylor@mail.com', '42 Lakeside Drive', 'Mystic', 'CT', 'INS-BC-099', 'BlueCross Shield', '2024-07-01T08:00:00'),
    ('P1015', 'Daniel Lewis', '555-66-7777', '1973-12-21', 'dlewis@email.com', '18 Mountain Road', 'Litchfield', 'CT', 'INS-AE-155', 'Aetna Health', '2024-07-01T08:00:00');

-- ===================== STEP 3: Incremental MERGE to Silver Admissions =====================
-- Uses INCREMENTAL_FILTER to only process new records with 3-day overlap

MERGE INTO ehr.silver.admissions_cleaned AS target
USING (
    WITH new_records AS (
        SELECT *
        FROM ehr.bronze.raw_admissions
        WHERE {{INCREMENTAL_FILTER(ehr.silver.admissions_cleaned, record_id, admission_date, 3)}}
    ),
    deduped AS (
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
        FROM new_records
    ),
    cleaned AS (
        SELECT
            record_id, patient_id, department_code, diagnosis_code,
            admission_date, discharge_date,
            DATEDIFF(discharge_date, admission_date) AS los_days,
            total_charges, attending_physician, ingested_at
        FROM deduped WHERE rn = 1
    ),
    with_history AS (
        -- Combine existing silver + new cleaned to compute readmission correctly
        SELECT
            c.*,
            LAG(prev_all.discharge_date) OVER (
                PARTITION BY c.patient_id ORDER BY c.admission_date
            ) AS prev_discharge_date,
            DATEDIFF(c.admission_date, LAG(prev_all.discharge_date) OVER (
                PARTITION BY c.patient_id ORDER BY c.admission_date
            )) AS days_since_last_discharge,
            CASE
                WHEN DATEDIFF(c.admission_date, LAG(prev_all.discharge_date) OVER (
                    PARTITION BY c.patient_id ORDER BY c.admission_date
                )) <= 30 THEN true
                ELSE false
            END AS readmission_flag,
            NTILE(100) OVER (ORDER BY DATEDIFF(c.discharge_date, c.admission_date)) AS los_percentile
        FROM cleaned c
        LEFT JOIN (
            SELECT patient_id, discharge_date
            FROM ehr.silver.admissions_cleaned
            UNION ALL
            SELECT patient_id, discharge_date FROM cleaned
        ) prev_all ON c.patient_id = prev_all.patient_id AND prev_all.discharge_date < c.admission_date
    )
    SELECT DISTINCT
        record_id, patient_id, department_code, diagnosis_code,
        admission_date, discharge_date, los_days, total_charges, attending_physician,
        COALESCE(readmission_flag, false) AS readmission_flag,
        prev_discharge_date, days_since_last_discharge, los_percentile, ingested_at
    FROM with_history
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
    readmission_flag        = source.readmission_flag,
    prev_discharge_date     = source.prev_discharge_date,
    days_since_last_discharge = source.days_since_last_discharge,
    los_percentile          = source.los_percentile,
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

-- ===================== STEP 4: Incremental SCD2 on Patient Dim =====================
-- Process the 2 new address changes for P1009 and P1015

-- Expire current records for patients with new demographics
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
            ingested_at
        FROM ehr.bronze.raw_patients
        WHERE ingested_at > '2024-06-15T00:00:00'
    )
    SELECT * FROM ranked
) AS source
ON target.patient_id = source.patient_id AND target.is_current = true
    AND (target.address <> source.address OR target.insurance_id <> source.insurance_id)
WHEN MATCHED THEN UPDATE SET
    valid_to    = CAST('2024-07-01' AS DATE),
    is_current  = false,
    updated_at  = CURRENT_TIMESTAMP;

-- Insert new current version for changed patients
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
    CAST('2024-07-01' AS DATE) AS valid_from,
    NULL AS valid_to,
    true AS is_current,
    CURRENT_TIMESTAMP AS updated_at
FROM ehr.bronze.raw_patients r
WHERE r.ingested_at > '2024-06-15T00:00:00'
  AND EXISTS (
      SELECT 1 FROM ehr.silver.patient_dim p
      WHERE p.patient_id = r.patient_id
        AND p.is_current = false
        AND p.valid_to = CAST('2024-07-01' AS DATE)
  );

-- ===================== STEP 5: Verify Incremental Processing =====================

SELECT COUNT(*) AS post_silver_count FROM ehr.silver.admissions_cleaned;

-- Should have added 7 new unique records (8 inserted minus 1 duplicate)
SELECT COUNT(*) AS new_records_added
FROM ehr.silver.admissions_cleaned
WHERE processed_at > (SELECT MAX(loaded_at) FROM ehr.gold.fact_admissions);

-- Verify no duplicates exist in silver
ASSERT VALUE duplicate_check = 0
SELECT COUNT(*) AS duplicate_check
FROM (
    SELECT record_id, COUNT(*) AS cnt
    FROM ehr.silver.admissions_cleaned
    GROUP BY record_id
    HAVING COUNT(*) > 1
);

-- Verify SCD2 incremental: P1009 and P1015 should now have expired records
ASSERT VALUE incremental_expired >= 2
SELECT COUNT(*) AS incremental_expired
FROM ehr.silver.patient_dim
WHERE is_current = false AND valid_to = CAST('2024-07-01' AS DATE);

-- ===================== STEP 6: Refresh Gold Fact (incremental) =====================

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

SELECT COUNT(*) AS post_gold_fact_count FROM ehr.gold.fact_admissions;

-- Verify gold grew by at least 7 new facts
ASSERT VALUE net_new_facts >= 7
SELECT
    post.cnt - pre.cnt AS net_new_facts
FROM
    (SELECT COUNT(*) AS cnt FROM ehr.gold.fact_admissions) post,
    (SELECT 50 AS cnt) pre;

-- ===================== STEP 7: Refresh KPI =====================

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
