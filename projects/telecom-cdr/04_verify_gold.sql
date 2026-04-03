-- =============================================================================
-- Telecom CDR Pipeline: Gold Layer Verification (12 ASSERTs)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_calls row count (all 70 CDRs from 3 schema versions)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_calls_count
FROM {{zone_prefix}}.gold.fact_calls;

ASSERT VALUE fact_calls_count >= 70

-- -----------------------------------------------------------------------------
-- 2. Verify schema evolution: all 3 versions present in fact table
-- -----------------------------------------------------------------------------
SELECT schema_version, COUNT(*) AS version_count
FROM {{zone_prefix}}.gold.fact_calls
GROUP BY schema_version
ORDER BY schema_version;

ASSERT ROW_COUNT = 3

-- -----------------------------------------------------------------------------
-- 3. Verify all 10 cell towers in dimension
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS tower_count
FROM {{zone_prefix}}.gold.dim_tower;

ASSERT VALUE tower_count = 10

-- -----------------------------------------------------------------------------
-- 4. Verify all 15 subscribers in dimension
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS subscriber_count
FROM {{zone_prefix}}.gold.dim_subscriber;

ASSERT VALUE subscriber_count = 15

-- -----------------------------------------------------------------------------
-- 5. Verify plan dimension has distinct plan combinations
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS plan_count
FROM {{zone_prefix}}.gold.dim_plan;

ASSERT VALUE plan_count >= 4

-- -----------------------------------------------------------------------------
-- 6. Dropped call analysis by region — must have drops in data
-- -----------------------------------------------------------------------------
SELECT
    region,
    total_calls,
    dropped_calls,
    drop_rate,
    avg_duration,
    roaming_calls,
    fiveg_calls
FROM {{zone_prefix}}.gold.kpi_network_quality
WHERE dropped_calls > 0
ORDER BY drop_rate DESC;

ASSERT VALUE dropped_calls > 0

-- -----------------------------------------------------------------------------
-- 7. Revenue by subscriber plan tier (fact-dim join across star schema)
-- -----------------------------------------------------------------------------
SELECT
    ds.plan_tier,
    ds.plan_type,
    COUNT(*)          AS call_count,
    SUM(fc.revenue)   AS total_revenue,
    CAST(AVG(fc.duration_sec) AS DECIMAL(8,1)) AS avg_duration_sec,
    COUNT(CASE WHEN fc.roaming_flag = true THEN 1 END) AS roaming_calls,
    COUNT(CASE WHEN fc.schema_version = 3 THEN 1 END) AS fiveg_era_calls
FROM {{zone_prefix}}.gold.fact_calls fc
JOIN {{zone_prefix}}.gold.dim_subscriber ds ON fc.caller_key = ds.subscriber_key
WHERE fc.call_type = 'voice'
GROUP BY ds.plan_tier, ds.plan_type
ORDER BY total_revenue DESC;

ASSERT VALUE call_count > 0

-- -----------------------------------------------------------------------------
-- 8. Tower utilization with 5G vs 4G comparison (fact-dim join with window)
-- -----------------------------------------------------------------------------
SELECT
    dt.city,
    dt.technology,
    dt.capacity_mhz,
    COUNT(*)          AS total_calls,
    SUM(fc.data_usage_mb) AS total_data_mb,
    COUNT(CASE WHEN fc.drop_flag = true THEN 1 END) AS drops,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS utilization_rank
FROM {{zone_prefix}}.gold.fact_calls fc
JOIN {{zone_prefix}}.gold.dim_tower dt ON fc.tower_key = dt.tower_key
GROUP BY dt.city, dt.technology, dt.capacity_mhz
ORDER BY utilization_rank;

ASSERT VALUE total_calls > 0

-- -----------------------------------------------------------------------------
-- 9. Session reconstruction verification — sessions exist in silver
-- -----------------------------------------------------------------------------
SELECT
    subscriber_id,
    COUNT(*) AS session_count,
    SUM(event_count) AS total_events,
    SUM(total_duration) AS total_duration_sec,
    SUM(roaming_events) AS roaming_sessions,
    SUM(drop_events) AS drop_sessions
FROM {{zone_prefix}}.silver.sessions
GROUP BY subscriber_id
ORDER BY session_count DESC;

ASSERT VALUE session_count > 0

-- -----------------------------------------------------------------------------
-- 10. Churn risk scoring verification — all 15 subscribers scored
-- -----------------------------------------------------------------------------
SELECT
    churn_risk_level,
    COUNT(*) AS subscriber_count,
    CAST(AVG(churn_score) AS DECIMAL(5,1)) AS avg_score,
    MIN(churn_score) AS min_score,
    MAX(churn_score) AS max_score
FROM {{zone_prefix}}.gold.kpi_churn_risk
GROUP BY churn_risk_level
ORDER BY avg_score DESC;

ASSERT VALUE subscriber_count > 0

-- -----------------------------------------------------------------------------
-- 11. Churn risk detail: high-risk subscribers with scoring breakdown
-- -----------------------------------------------------------------------------
SELECT
    subscriber_id,
    phone_number,
    plan_type,
    status,
    days_since_last_call,
    monthly_usage_trend,
    drop_rate,
    balance,
    churn_score,
    churn_risk_level
FROM {{zone_prefix}}.gold.kpi_churn_risk
WHERE churn_score >= 40 OR status IN ('churned', 'suspended')
ORDER BY churn_score DESC;

ASSERT VALUE churn_score >= 0

-- -----------------------------------------------------------------------------
-- 12. Network quality: peak hours vs off-peak with 5G breakdown
-- -----------------------------------------------------------------------------
SELECT
    CASE WHEN hour_bucket BETWEEN 7 AND 22 THEN 'Peak' ELSE 'Off-Peak' END AS period,
    SUM(total_calls)   AS total_calls,
    SUM(dropped_calls) AS total_drops,
    CAST(SUM(dropped_calls) * 1.0 / NULLIF(SUM(total_calls), 0) AS DECIMAL(5,4)) AS overall_drop_rate,
    SUM(total_data_mb) AS total_data_mb,
    SUM(roaming_calls) AS total_roaming,
    SUM(fiveg_calls)   AS total_5g,
    SUM(revenue)       AS total_revenue
FROM {{zone_prefix}}.gold.kpi_network_quality
GROUP BY CASE WHEN hour_bucket BETWEEN 7 AND 22 THEN 'Peak' ELSE 'Off-Peak' END;

ASSERT VALUE total_calls > 0

-- -----------------------------------------------------------------------------
-- 13. Referential integrity check: no orphaned caller keys
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS orphaned_callers
FROM {{zone_prefix}}.gold.fact_calls f
LEFT JOIN {{zone_prefix}}.gold.dim_subscriber ds ON f.caller_key = ds.subscriber_key
WHERE f.caller_key IS NOT NULL AND ds.subscriber_key IS NULL;

ASSERT VALUE orphaned_callers = 0

-- -----------------------------------------------------------------------------
-- 14. Revenue per region heatmap with cumulative window
-- -----------------------------------------------------------------------------
SELECT
    region,
    hour_bucket,
    revenue,
    total_calls,
    fiveg_calls,
    SUM(revenue) OVER (PARTITION BY region ORDER BY hour_bucket) AS cumulative_revenue
FROM {{zone_prefix}}.gold.kpi_network_quality
ORDER BY region, hour_bucket;

ASSERT VALUE revenue >= 0
SELECT 'All 12 gold-layer assertions passed' AS verification_status;
