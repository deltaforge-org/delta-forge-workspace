-- =============================================================================
-- Pharma Clinical Trials Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE pharma_daily_schedule
    CRON '0 8 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE pharma_clinical_trials_pipeline
    DESCRIPTION 'Full load: transform clinical trial data through medallion layers'
    SCHEDULE 'pharma_daily_schedule'
    TAGS 'pharma,clinical-trials,full-load'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: Bronze -> Silver (Participant Dimension) =====================
-- Clean participant data, derive age bands via DOB generalization, determine active status

INSERT INTO {{zone_prefix}}.silver.participant_clean
SELECT
    p.participant_id,
    TRIM(p.participant_name)                                    AS participant_name,
    p.ssn,
    LOWER(TRIM(p.email))                                        AS email,
    p.date_of_birth,
    CONCAT(CAST(FLOOR(EXTRACT(YEAR FROM CAST(p.date_of_birth AS DATE)) / 10) * 10 AS STRING), 's') AS age_band,
    p.gender,
    p.ethnicity,
    p.trial_id,
    p.site_id,
    CAST(p.enrollment_date AS DATE)                             AS enrollment_date,
    p.consent_status,
    CAST(p.withdrawal_date AS DATE)                             AS withdrawal_date,
    p.withdrawal_reason,
    CASE
        WHEN p.consent_status IN ('Active', 'Completed') THEN true
        ELSE false
    END                                                          AS is_active
FROM {{zone_prefix}}.bronze.raw_participants p;

-- ===================== STEP 2: Bronze -> Silver (Observation Enrichment) =====================
-- Join baseline values, calculate biomarker change from baseline per participant

INSERT INTO {{zone_prefix}}.silver.observation_enriched
ASSERT ROW_COUNT = 20
SELECT
    o.observation_id,
    o.participant_id,
    o.trial_id,
    o.site_id,
    o.visit_number,
    CAST(o.observation_date AS DATE)                            AS observation_date,
    o.biomarker_value,
    baseline.biomarker_value                                    AS baseline_value,
    CASE
        WHEN o.biomarker_value IS NOT NULL AND baseline.biomarker_value IS NOT NULL
        THEN o.biomarker_value - baseline.biomarker_value
        ELSE NULL
    END                                                          AS biomarker_change,
    o.adverse_event_flag,
    o.severity,
    o.dosage_mg,
    o.response_category,
    o.protocol_deviation
FROM {{zone_prefix}}.bronze.raw_observations o
LEFT JOIN (
    SELECT participant_id, trial_id, biomarker_value
    FROM {{zone_prefix}}.bronze.raw_observations
    WHERE visit_number = 2
      AND biomarker_value IS NOT NULL
) baseline
    ON o.participant_id = baseline.participant_id
   AND o.trial_id = baseline.trial_id;

-- ===================== STEP 3: Silver -> Gold (dim_trial) =====================

INSERT INTO {{zone_prefix}}.gold.dim_trial
ASSERT ROW_COUNT = 71
SELECT
    ROW_NUMBER() OVER (ORDER BY t.trial_id)     AS trial_key,
    t.trial_id,
    t.trial_name,
    t.phase,
    t.therapeutic_area,
    t.sponsor,
    CAST(t.start_date AS DATE)                  AS start_date,
    t.target_enrollment
FROM {{zone_prefix}}.bronze.raw_trials t;

-- ===================== STEP 4: Silver -> Gold (dim_site) =====================

INSERT INTO {{zone_prefix}}.gold.dim_site
ASSERT ROW_COUNT = 3
SELECT
    ROW_NUMBER() OVER (ORDER BY s.site_id)      AS site_key,
    s.site_id,
    s.site_name,
    s.city,
    s.country,
    s.principal_investigator,
    CAST(s.irb_approval_date AS DATE)           AS irb_approval_date
FROM {{zone_prefix}}.bronze.raw_sites s;

-- ===================== STEP 5: Silver -> Gold (dim_visit) =====================

INSERT INTO {{zone_prefix}}.gold.dim_visit
ASSERT ROW_COUNT = 4
SELECT
    ROW_NUMBER() OVER (ORDER BY v.trial_id, v.visit_number)  AS visit_key,
    v.visit_number,
    v.visit_type,
    v.scheduled_day,
    v.window_days
FROM {{zone_prefix}}.bronze.raw_visits v;

-- ===================== STEP 6: Silver -> Gold (dim_participant) =====================

INSERT INTO {{zone_prefix}}.gold.dim_participant
ASSERT ROW_COUNT = 15
SELECT
    ROW_NUMBER() OVER (ORDER BY p.participant_id) AS participant_key,
    p.participant_id,
    p.age_band,
    p.gender,
    p.ethnicity,
    p.enrollment_date,
    p.consent_status,
    p.withdrawal_date
FROM {{zone_prefix}}.silver.participant_clean p;

-- ===================== STEP 7: Silver -> Gold (fact_observations) =====================

INSERT INTO {{zone_prefix}}.gold.fact_observations
ASSERT ROW_COUNT = 20
SELECT
    ROW_NUMBER() OVER (ORDER BY o.observation_id)   AS observation_key,
    dp.participant_key,
    dt.trial_key,
    ds.site_key,
    dv.visit_key,
    o.observation_date,
    o.biomarker_value,
    o.adverse_event_flag,
    o.severity,
    o.dosage_mg,
    o.response_category
FROM {{zone_prefix}}.silver.observation_enriched o
JOIN {{zone_prefix}}.gold.dim_participant dp ON o.participant_id = dp.participant_id
JOIN {{zone_prefix}}.gold.dim_trial dt       ON o.trial_id = dt.trial_id
JOIN {{zone_prefix}}.gold.dim_site ds        ON o.site_id = ds.site_id
JOIN {{zone_prefix}}.bronze.raw_visits rv    ON o.trial_id = rv.trial_id AND o.visit_number = rv.visit_number
JOIN {{zone_prefix}}.gold.dim_visit dv       ON rv.visit_number = dv.visit_number AND rv.visit_type = dv.visit_type;

-- ===================== STEP 8: Silver -> Gold (kpi_trial_efficacy) =====================

INSERT INTO {{zone_prefix}}.gold.kpi_trial_efficacy
ASSERT ROW_COUNT = 71
SELECT
    t.trial_id,
    t.phase,
    -- Response rate: % of participants with at least one 'Responder' observation
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN o.response_category = 'Responder' THEN o.participant_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN p.consent_status NOT IN ('Screen Fail') THEN o.participant_id END), 0),
        2
    )                                                           AS response_rate_pct,
    -- Adverse event rate: % of observations with AE
    ROUND(
        100.0 * SUM(CASE WHEN o.adverse_event_flag = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN o.biomarker_value IS NOT NULL THEN 1 END), 0),
        2
    )                                                           AS adverse_event_rate,
    -- Mean biomarker change from baseline (last visit vs baseline)
    ROUND(AVG(o.biomarker_change), 4)                           AS mean_biomarker_change,
    -- Enrollment percentage: enrolled / target
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN p.consent_status != 'Screen Fail' THEN p.participant_id END)
        / NULLIF(MAX(tr.target_enrollment), 0),
        2
    )                                                           AS enrollment_pct,
    -- Screen fail rate
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN p.consent_status = 'Screen Fail' THEN p.participant_id END)
        / NULLIF(COUNT(DISTINCT p.participant_id), 0),
        2
    )                                                           AS screen_fail_rate,
    -- Dropout rate (withdrawn / enrolled non-screen-fail)
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN p.consent_status = 'Withdrawn' THEN p.participant_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN p.consent_status != 'Screen Fail' THEN p.participant_id END), 0),
        2
    )                                                           AS dropout_rate
FROM {{zone_prefix}}.bronze.raw_trials tr
JOIN {{zone_prefix}}.silver.participant_clean p ON tr.trial_id = p.trial_id
JOIN {{zone_prefix}}.silver.observation_enriched o ON p.participant_id = o.participant_id AND p.trial_id = o.trial_id
JOIN {{zone_prefix}}.gold.dim_trial t ON tr.trial_id = t.trial_id
GROUP BY t.trial_id, t.phase;

ASSERT ROW_COUNT = 3
SELECT 'row count check' AS status;


-- ===================== STEP 9: Deletion Vectors Demo (GDPR Right-to-Erasure) =====================
-- Participant PT-1003 (Carmen Diaz) withdrew due to adverse event and requests data erasure

DELETE FROM {{zone_prefix}}.silver.participant_clean
WHERE participant_id = 'PT-1003';

DELETE FROM {{zone_prefix}}.silver.observation_enriched
WHERE participant_id = 'PT-1003';

-- ===================== STEP 10: RESTORE Demo =====================
-- Oops! We accidentally purged PT-1003 but legal says we must retain for regulatory audit
-- Restore silver participant table to previous version

RESTORE {{zone_prefix}}.silver.participant_clean TO VERSION 0;

-- ===================== OPTIMIZE =====================

OPTIMIZE {{zone_prefix}}.silver.participant_clean;
OPTIMIZE {{zone_prefix}}.silver.observation_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_observations;
OPTIMIZE {{zone_prefix}}.gold.dim_participant;
OPTIMIZE {{zone_prefix}}.gold.kpi_trial_efficacy;
