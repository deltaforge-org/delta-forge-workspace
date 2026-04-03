-- =============================================================================
-- Pharma Clinical Trials Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== QUERY 1: Trial Efficacy Summary =====================
-- Verify KPIs exist for all 3 trials with expected ranges

SELECT
    k.trial_id,
    k.phase,
    k.response_rate_pct,
    k.adverse_event_rate,
    k.mean_biomarker_change,
    k.enrollment_pct,
    k.screen_fail_rate,
    k.dropout_rate
FROM {{zone_prefix}}.gold.kpi_trial_efficacy k
ORDER BY k.trial_id;

-- ===================== QUERY 2: Star Schema Join — Full Observation Detail =====================
-- Join all dimensions to fact table, verify referential integrity

ASSERT ROW_COUNT = 3
SELECT
    f.observation_key,
    dp.participant_id,
    dp.age_band,
    dp.gender,
    dt.trial_name,
    dt.phase,
    ds.site_name,
    ds.country,
    dv.visit_type,
    f.observation_date,
    f.biomarker_value,
    f.adverse_event_flag,
    f.severity,
    f.dosage_mg,
    f.response_category
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_participant dp ON f.participant_key = dp.participant_key
JOIN {{zone_prefix}}.gold.dim_trial dt       ON f.trial_key = dt.trial_key
JOIN {{zone_prefix}}.gold.dim_site ds        ON f.site_key = ds.site_key
JOIN {{zone_prefix}}.gold.dim_visit dv       ON f.visit_key = dv.visit_key
ORDER BY dt.trial_id, dp.participant_id, dv.visit_number;

-- ===================== QUERY 3: Adverse Event Analysis by Trial and Severity =====================

ASSERT VALUE observation_key > 0
SELECT
    dt.trial_name,
    dt.phase,
    f.severity,
    COUNT(*)                                                AS ae_count,
    COUNT(DISTINCT f.participant_key)                       AS affected_participants,
    ROUND(AVG(f.dosage_mg), 2)                             AS avg_dosage_at_ae
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_trial dt ON f.trial_key = dt.trial_key
WHERE f.adverse_event_flag = true
GROUP BY dt.trial_name, dt.phase, f.severity
ORDER BY dt.trial_name, ae_count DESC;

-- ===================== QUERY 4: Site Performance Comparison =====================
-- Enrollment count, AE rate, and response rate per site

ASSERT VALUE ae_count > 0
SELECT
    ds.site_name,
    ds.country,
    COUNT(DISTINCT dp.participant_id)                       AS total_enrolled,
    COUNT(DISTINCT CASE WHEN f.response_category = 'Responder' THEN dp.participant_id END) AS responders,
    ROUND(
        100.0 * SUM(CASE WHEN f.adverse_event_flag = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN f.biomarker_value IS NOT NULL THEN 1 END), 0),
        2
    )                                                       AS ae_rate_pct,
    ROUND(AVG(f.biomarker_value), 2)                       AS avg_biomarker
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_participant dp ON f.participant_key = dp.participant_key
JOIN {{zone_prefix}}.gold.dim_site ds        ON f.site_key = ds.site_key
GROUP BY ds.site_name, ds.country
ORDER BY total_enrolled DESC;

-- ===================== QUERY 5: Enrollment Funnel Analysis =====================
-- Screened -> Enrolled -> Completed/Withdrawn/Active by trial

ASSERT ROW_COUNT = 4
SELECT
    dt.trial_name,
    COUNT(DISTINCT dp.participant_id)                       AS total_screened,
    COUNT(DISTINCT CASE WHEN dp.consent_status != 'Screen Fail' THEN dp.participant_id END) AS enrolled,
    COUNT(DISTINCT CASE WHEN dp.consent_status = 'Screen Fail' THEN dp.participant_id END) AS screen_failed,
    COUNT(DISTINCT CASE WHEN dp.consent_status = 'Active' THEN dp.participant_id END) AS still_active,
    COUNT(DISTINCT CASE WHEN dp.consent_status = 'Completed' THEN dp.participant_id END) AS completed,
    COUNT(DISTINCT CASE WHEN dp.consent_status = 'Withdrawn' THEN dp.participant_id END) AS withdrawn
FROM {{zone_prefix}}.gold.dim_participant dp
JOIN {{zone_prefix}}.gold.fact_observations f ON dp.participant_key = f.participant_key
JOIN {{zone_prefix}}.gold.dim_trial dt        ON f.trial_key = dt.trial_key
GROUP BY dt.trial_name
ORDER BY dt.trial_name;

-- ===================== QUERY 6: Visit Completion Rate (Kaplan-Meier-style) =====================
-- Percentage of enrolled participants who completed each visit type

ASSERT ROW_COUNT = 3
SELECT
    dt.trial_name,
    dv.visit_type,
    dv.visit_number,
    COUNT(DISTINCT f.participant_key)                       AS participants_at_visit,
    MAX(sub.enrolled_count)                                  AS total_enrolled,
    ROUND(
        100.0 * COUNT(DISTINCT f.participant_key) / NULLIF(MAX(sub.enrolled_count), 0),
        2
    )                                                        AS completion_rate_pct
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_trial dt   ON f.trial_key = dt.trial_key
JOIN {{zone_prefix}}.gold.dim_visit dv   ON f.visit_key = dv.visit_key
JOIN (
    SELECT dt2.trial_key, COUNT(DISTINCT f2.participant_key) AS enrolled_count
    FROM {{zone_prefix}}.gold.fact_observations f2
    JOIN {{zone_prefix}}.gold.dim_trial dt2 ON f2.trial_key = dt2.trial_key
    JOIN {{zone_prefix}}.gold.dim_participant dp2 ON f2.participant_key = dp2.participant_key
    WHERE dp2.consent_status != 'Screen Fail'
    GROUP BY dt2.trial_key
) sub ON dt.trial_key = sub.trial_key
GROUP BY dt.trial_name, dv.visit_type, dv.visit_number
ORDER BY dt.trial_name, dv.visit_number;

-- ===================== QUERY 7: Biomarker Change Window Analysis =====================
-- Running biomarker trend per participant using LAG

ASSERT VALUE completion_rate_pct > 0
SELECT
    dp.participant_id,
    dt.trial_name,
    dv.visit_type,
    f.biomarker_value,
    LAG(f.biomarker_value) OVER (PARTITION BY dp.participant_id ORDER BY dv.visit_number) AS prev_biomarker,
    f.biomarker_value - LAG(f.biomarker_value) OVER (PARTITION BY dp.participant_id ORDER BY dv.visit_number) AS visit_delta
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_participant dp ON f.participant_key = dp.participant_key
JOIN {{zone_prefix}}.gold.dim_trial dt       ON f.trial_key = dt.trial_key
JOIN {{zone_prefix}}.gold.dim_visit dv       ON f.visit_key = dv.visit_key
WHERE f.biomarker_value IS NOT NULL
ORDER BY dp.participant_id, dv.visit_number;

-- ===================== QUERY 8: Gender and Ethnicity Distribution by Trial =====================

ASSERT VALUE biomarker_value > 0
SELECT
    dt.trial_name,
    dp.gender,
    dp.ethnicity,
    COUNT(DISTINCT dp.participant_id)                       AS participant_count,
    ROUND(AVG(CASE WHEN f.response_category = 'Responder' THEN 1.0 ELSE 0.0 END) * 100, 2) AS response_pct
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_participant dp ON f.participant_key = dp.participant_key
JOIN {{zone_prefix}}.gold.dim_trial dt       ON f.trial_key = dt.trial_key
WHERE dp.consent_status != 'Screen Fail'
GROUP BY dt.trial_name, dp.gender, dp.ethnicity
ORDER BY dt.trial_name, participant_count DESC;

-- ===================== QUERY 9: Protocol Deviation Summary =====================

ASSERT VALUE participant_count > 0
SELECT
    dt.trial_name,
    ds.site_name,
    COUNT(DISTINCT f.observation_key)                       AS total_observations,
    SUM(CASE WHEN oe.protocol_deviation = true THEN 1 ELSE 0 END) AS deviation_count,
    ROUND(
        100.0 * SUM(CASE WHEN oe.protocol_deviation = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                        AS deviation_rate_pct
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_trial dt           ON f.trial_key = dt.trial_key
JOIN {{zone_prefix}}.gold.dim_site ds            ON f.site_key = ds.site_key
JOIN {{zone_prefix}}.silver.observation_enriched oe ON f.observation_date = oe.observation_date
    AND oe.participant_id = (SELECT participant_id FROM {{zone_prefix}}.gold.dim_participant WHERE participant_key = f.participant_key)
GROUP BY dt.trial_name, ds.site_name
ORDER BY deviation_count DESC;

-- ===================== QUERY 10: Age Band Response Analysis =====================

ASSERT VALUE total_observations > 0
SELECT
    dp.age_band,
    COUNT(DISTINCT dp.participant_id)                       AS participants,
    COUNT(DISTINCT CASE WHEN f.response_category = 'Responder' THEN dp.participant_id END) AS responders,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN f.response_category = 'Responder' THEN dp.participant_id END)
        / NULLIF(COUNT(DISTINCT dp.participant_id), 0),
        2
    )                                                        AS response_rate_by_age
FROM {{zone_prefix}}.gold.fact_observations f
JOIN {{zone_prefix}}.gold.dim_participant dp ON f.participant_key = dp.participant_key
WHERE dp.consent_status NOT IN ('Screen Fail')
GROUP BY dp.age_band
ORDER BY dp.age_band;

ASSERT VALUE participants > 0
SELECT 'participants check passed' AS participants_status;

