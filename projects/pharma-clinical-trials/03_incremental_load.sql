-- =============================================================================
-- Pharma Clinical Trials Pipeline - Incremental Load
-- =============================================================================
-- Watermark-based incremental: load only new observations since last run

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "observation_id > 'OBS-0071' AND observation_date > '2023-10-15'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.observation_enriched, observation_id, observation_date, 7)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.observation_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_observations
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.observation_enriched, observation_id, observation_date, 7)}};

-- ===================== STEP 1: Watermark Check =====================
-- Get the maximum ingested_at from silver to determine what's new

SELECT MAX(observation_date) AS last_watermark
FROM {{zone_prefix}}.silver.observation_enriched;

-- ===================== STEP 2: Insert New Bronze Observations =====================
-- Simulating new Week 12 and End-of-Trial observations arriving

INSERT INTO {{zone_prefix}}.bronze.raw_observations VALUES
    -- New Week 12 follow-up for PT-2007 (Oncology)
    ('OBS-0072', 'PT-2007', 'TRL-002', 'SITE-TOK', 5, '2023-11-02', 112.4500, false, NULL, NULL, 250.00, 'Responder', false, NULL, '2025-01-02T08:00:00'),
    -- New Week 4 for PT-3006 (Neurology) — late data entry
    ('OBS-0073', 'PT-3006', 'TRL-003', 'SITE-MUN', 5, '2023-12-22', 38.2100, false, NULL, NULL, 75.00, 'Responder', false, NULL, '2025-01-02T08:00:00'),
    -- New End-of-Trial for PT-1006 (Cardiovascular)
    ('OBS-0074', 'PT-1006', 'TRL-001', 'SITE-MUN', 5, '2023-09-02', 128.7700, false, NULL, NULL, 150.00, 'Responder', false, NULL, '2025-01-02T08:00:00'),
    -- New adverse event for PT-3002 (Neurology)
    ('OBS-0075', 'PT-3002', 'TRL-003', 'SITE-LON', 5, '2023-12-05', 45.6700, true, 'Seizure episode', 'Severe', 75.00, 'Non-responder', true, 'SAE reported', '2025-01-02T08:00:00');

-- ===================== STEP 3: Incremental Silver Enrichment =====================
-- Only process observations newer than watermark

INSERT INTO {{zone_prefix}}.silver.observation_enriched
ASSERT ROW_COUNT = 4
SELECT
    o.observation_id,
    o.participant_id,
    o.trial_id,
    o.site_id,
    o.visit_number,
    CAST(o.observation_date AS DATE)        AS observation_date,
    o.biomarker_value,
    baseline.biomarker_value                AS baseline_value,
    CASE
        WHEN o.biomarker_value IS NOT NULL AND baseline.biomarker_value IS NOT NULL
        THEN o.biomarker_value - baseline.biomarker_value
        ELSE NULL
    END                                      AS biomarker_change,
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
   AND o.trial_id = baseline.trial_id
WHERE o.ingested_at > '2025-01-01T12:00:00';

-- ===================== STEP 4: Refresh Gold Fact (Merge New Observations) =====================

MERGE INTO {{zone_prefix}}.gold.fact_observations AS tgt
USING (
ASSERT ROW_COUNT = 4
    SELECT
        ROW_NUMBER() OVER (ORDER BY o.observation_id) + 71  AS observation_key,
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
    JOIN {{zone_prefix}}.gold.dim_visit dv       ON rv.visit_number = dv.visit_number AND rv.visit_type = dv.visit_type
    WHERE o.observation_date > '2023-10-22'
) AS src
ON tgt.observation_key = src.observation_key
WHEN NOT MATCHED THEN INSERT (
    observation_key, participant_key, trial_key, site_key, visit_key,
    observation_date, biomarker_value, adverse_event_flag, severity, dosage_mg, response_category
) VALUES (
    src.observation_key, src.participant_key, src.trial_key, src.site_key, src.visit_key,
    src.observation_date, src.biomarker_value, src.adverse_event_flag, src.severity, src.dosage_mg, src.response_category
);

-- ===================== STEP 5: Refresh KPI (Full Rebuild) =====================
-- KPIs are small enough to rebuild entirely

DELETE FROM {{zone_prefix}}.gold.kpi_trial_efficacy WHERE 1=1;

INSERT INTO {{zone_prefix}}.gold.kpi_trial_efficacy
SELECT
    t.trial_id,
    t.phase,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN o.response_category = 'Responder' THEN o.participant_id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN p.consent_status NOT IN ('Screen Fail') THEN o.participant_id END), 0),
        2
    )                                                           AS response_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN o.adverse_event_flag = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN o.biomarker_value IS NOT NULL THEN 1 END), 0),
        2
    )                                                           AS adverse_event_rate,
    ROUND(AVG(o.biomarker_change), 4)                           AS mean_biomarker_change,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN p.consent_status != 'Screen Fail' THEN p.participant_id END)
        / NULLIF(MAX(tr.target_enrollment), 0),
        2
    )                                                           AS enrollment_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN p.consent_status = 'Screen Fail' THEN p.participant_id END)
        / NULLIF(COUNT(DISTINCT p.participant_id), 0),
        2
    )                                                           AS screen_fail_rate,
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

-- ===================== STEP 6: Verify Incremental =====================

ASSERT VALUE response_rate_pct > 0 WHERE trial_id = 'TRL-001'
ASSERT VALUE adverse_event_rate > 0 WHERE trial_id = 'TRL-002'
ASSERT ROW_COUNT = 3
SELECT 'adverse_event_rate check passed' AS adverse_event_rate_status;


OPTIMIZE {{zone_prefix}}.gold.fact_observations;
