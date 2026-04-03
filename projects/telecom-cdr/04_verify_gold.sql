-- =============================================================================
-- Telecom CDR Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_calls row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_calls_count
FROM {{zone_prefix}}.gold.fact_calls;

-- -----------------------------------------------------------------------------
-- 2. Verify all 8 cell towers in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_calls_count >= 65
SELECT COUNT(*) AS tower_count
FROM {{zone_prefix}}.gold.dim_cell_tower;

-- -----------------------------------------------------------------------------
-- 3. Verify all 12 subscribers in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS subscriber_count
FROM {{zone_prefix}}.gold.dim_subscriber;

-- -----------------------------------------------------------------------------
-- 4. Dropped call analysis by region
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 12
SELECT
    region,
    total_calls,
    dropped_calls,
    drop_rate,
    avg_duration
FROM {{zone_prefix}}.gold.kpi_network_quality
WHERE dropped_calls > 0
ORDER BY drop_rate DESC;

-- -----------------------------------------------------------------------------
-- 5. Revenue by subscriber plan tier (fact-dim join)
-- -----------------------------------------------------------------------------
ASSERT VALUE dropped_calls > 0
SELECT
    ds.plan_tier,
    ds.plan_type,
    COUNT(*)          AS call_count,
    SUM(fc.revenue)   AS total_revenue,
    CAST(AVG(fc.duration_sec) AS DECIMAL(8,1)) AS avg_duration_sec
FROM {{zone_prefix}}.gold.fact_calls fc
JOIN {{zone_prefix}}.gold.dim_subscriber ds ON fc.caller_key = ds.subscriber_key
WHERE fc.call_type = 'voice'
GROUP BY ds.plan_tier, ds.plan_type
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 6. Tower utilization analysis (fact-dim join with window)
-- -----------------------------------------------------------------------------
ASSERT VALUE call_count > 0
SELECT
    dt.city,
    dt.technology,
    dt.capacity_mhz,
    COUNT(*)          AS total_calls,
    SUM(fc.data_usage_mb) AS total_data_mb,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS utilization_rank
FROM {{zone_prefix}}.gold.fact_calls fc
JOIN {{zone_prefix}}.gold.dim_cell_tower dt ON fc.cell_tower_key = dt.tower_key
GROUP BY dt.city, dt.technology, dt.capacity_mhz
ORDER BY utilization_rank;

-- -----------------------------------------------------------------------------
-- 7. Churn signal: subscribers with declining usage (LAG pattern)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    sp.subscriber_id,
    sp.phone_number,
    sp.status,
    sp.total_calls,
    sp.total_revenue,
    sp.drop_rate,
    CASE
        WHEN sp.status = 'churned' THEN 'Already churned'
        WHEN sp.status = 'suspended' THEN 'High risk - suspended'
        WHEN sp.drop_rate > 0.1000 THEN 'High risk - high drop rate'
        WHEN sp.total_calls < 5 THEN 'Medium risk - low activity'
        ELSE 'Low risk'
    END AS churn_signal
FROM {{zone_prefix}}.silver.subscriber_profiles sp
ORDER BY sp.total_revenue ASC;

-- -----------------------------------------------------------------------------
-- 8. Network quality: peak hours vs off-peak
-- -----------------------------------------------------------------------------
ASSERT VALUE subscriber_id IS NOT NULL
SELECT
    CASE WHEN hour_bucket BETWEEN 7 AND 22 THEN 'Peak' ELSE 'Off-Peak' END AS period,
    SUM(total_calls)   AS total_calls,
    SUM(dropped_calls) AS total_drops,
    CAST(SUM(dropped_calls) * 1.0 / NULLIF(SUM(total_calls), 0) AS DECIMAL(5,4)) AS overall_drop_rate,
    SUM(total_data_mb) AS total_data_mb,
    SUM(revenue)       AS total_revenue
FROM {{zone_prefix}}.gold.kpi_network_quality
GROUP BY CASE WHEN hour_bucket BETWEEN 7 AND 22 THEN 'Peak' ELSE 'Off-Peak' END;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity check
-- -----------------------------------------------------------------------------
ASSERT VALUE total_calls > 0
SELECT COUNT(*) AS orphaned_callers
FROM {{zone_prefix}}.gold.fact_calls f
LEFT JOIN {{zone_prefix}}.gold.dim_subscriber ds ON f.caller_key = ds.subscriber_key
WHERE f.caller_key IS NOT NULL AND ds.subscriber_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. Revenue per region per hour heatmap data
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_callers = 0
SELECT
    region,
    hour_bucket,
    revenue,
    total_calls,
    SUM(revenue) OVER (PARTITION BY region ORDER BY hour_bucket) AS cumulative_revenue
FROM {{zone_prefix}}.gold.kpi_network_quality
ORDER BY region, hour_bucket;

ASSERT VALUE revenue >= 0
SELECT 'revenue check passed' AS revenue_status;

