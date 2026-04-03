-- =============================================================================
-- Media Streaming Analytics Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_viewing_events row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_events_count
FROM {{zone_prefix}}.gold.fact_viewing_events;

-- -----------------------------------------------------------------------------
-- 2. Verify all 12 users in dim_user
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_events_count >= 72
SELECT COUNT(*) AS user_count
FROM {{zone_prefix}}.gold.dim_user;

-- -----------------------------------------------------------------------------
-- 3. Verify all 15 content items in dim_content
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS content_count
FROM {{zone_prefix}}.gold.dim_content;

-- -----------------------------------------------------------------------------
-- 4. Content engagement scorecard with star schema join
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 15
SELECT
    dc.title,
    dc.genre,
    dc.content_type,
    COUNT(*) FILTER (WHERE f.event_type = 'play') AS plays,
    COUNT(DISTINCT f.user_key) AS unique_viewers,
    CAST(AVG(f.watch_duration_sec) / 60.0 AS DECIMAL(7,2)) AS avg_watch_min,
    CAST(AVG(f.position_pct) FILTER (WHERE f.event_type IN ('complete', 'abandon'))
        AS DECIMAL(5,2)) AS avg_completion_pct
FROM {{zone_prefix}}.gold.fact_viewing_events f
JOIN {{zone_prefix}}.gold.dim_content dc ON f.content_key = dc.content_key
GROUP BY dc.title, dc.genre, dc.content_type
ORDER BY plays DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 5. Subscription tier engagement analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 5
SELECT
    du.subscription_tier,
    COUNT(DISTINCT du.user_key) AS users,
    COUNT(*) AS total_events,
    CAST(AVG(f.watch_duration_sec) / 60.0 AS DECIMAL(7,2)) AS avg_watch_min,
    COUNT(*) FILTER (WHERE f.event_type = 'abandon') AS abandons,
    CAST(COUNT(*) FILTER (WHERE f.event_type = 'abandon') * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS abandon_rate_pct
FROM {{zone_prefix}}.gold.fact_viewing_events f
JOIN {{zone_prefix}}.gold.dim_user du ON f.user_key = du.user_key
GROUP BY du.subscription_tier
ORDER BY avg_watch_min DESC;

-- -----------------------------------------------------------------------------
-- 6. Device preference analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    dd.device_type,
    dd.os,
    COUNT(DISTINCT f.user_key) AS unique_users,
    COUNT(*) AS events,
    CAST(AVG(f.watch_duration_sec) / 60.0 AS DECIMAL(7,2)) AS avg_watch_min,
    MODE() WITHIN GROUP (ORDER BY f.quality_level) AS most_common_quality
FROM {{zone_prefix}}.gold.fact_viewing_events f
JOIN {{zone_prefix}}.gold.dim_device dd ON f.device_key = dd.device_key
GROUP BY dd.device_type, dd.os
ORDER BY events DESC;

-- -----------------------------------------------------------------------------
-- 7. Binge session detection - sessions with 3+ content items
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 4
SELECT
    ds.session_id,
    du.user_id,
    du.preferred_genre,
    ds.content_count,
    ds.total_watch_sec / 60 AS total_watch_min,
    ds.avg_completion_pct
FROM {{zone_prefix}}.gold.dim_session ds
JOIN {{zone_prefix}}.gold.dim_user du ON ds.user_key = du.user_key
WHERE ds.content_count >= 3
ORDER BY ds.total_watch_sec DESC;

-- -----------------------------------------------------------------------------
-- 8. Churn signal analysis - users with no activity > 14 days
-- -----------------------------------------------------------------------------
ASSERT VALUE content_count >= 3
SELECT
    du.user_id,
    du.subscription_tier,
    du.country,
    MAX(f.event_timestamp) AS last_activity,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE f.event_type = 'abandon') AS abandon_count
FROM {{zone_prefix}}.gold.fact_viewing_events f
JOIN {{zone_prefix}}.gold.dim_user du ON f.user_key = du.user_key
GROUP BY du.user_id, du.subscription_tier, du.country
HAVING MAX(f.event_timestamp) < (
    SELECT MAX(event_timestamp) - INTERVAL '14 days'
    FROM {{zone_prefix}}.gold.fact_viewing_events
)
ORDER BY last_activity ASC;

-- -----------------------------------------------------------------------------
-- 9. Content ROI analysis from KPI table
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 2
SELECT
    k.content_id,
    k.genre,
    k.period,
    k.total_views,
    k.unique_viewers,
    k.avg_completion_pct,
    k.binge_rate,
    k.content_roi
FROM {{zone_prefix}}.gold.kpi_engagement k
WHERE k.content_roi > 0
ORDER BY k.content_roi DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 10. Genre-level engagement summary
-- -----------------------------------------------------------------------------
ASSERT VALUE content_roi > 0
SELECT
    dc.genre,
    COUNT(DISTINCT dc.content_key)      AS titles,
    COUNT(DISTINCT f.user_key)          AS unique_viewers,
    CAST(SUM(f.watch_duration_sec) / 3600.0 AS DECIMAL(10,2)) AS total_watch_hours,
    CAST(AVG(f.position_pct) FILTER (WHERE f.event_type IN ('complete', 'abandon'))
        AS DECIMAL(5,2)) AS avg_completion_pct,
    DENSE_RANK() OVER (ORDER BY SUM(f.watch_duration_sec) DESC) AS genre_rank
FROM {{zone_prefix}}.gold.fact_viewing_events f
JOIN {{zone_prefix}}.gold.dim_content dc ON f.content_key = dc.content_key
GROUP BY dc.genre
ORDER BY genre_rank;

-- -----------------------------------------------------------------------------
-- Verification Summary
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 5
SELECT
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_viewing_events) AS fact_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_user) AS dim_user_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_content) AS dim_content_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_device) AS dim_device_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_session) AS dim_session_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.kpi_engagement) AS kpi_rows;
