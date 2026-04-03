-- =============================================================================
-- Media Streaming Analytics Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE media_3hourly_schedule
    CRON '0 */3 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE media_streaming_pipeline
    DESCRIPTION 'Media streaming pipeline with session reconstruction, engagement scoring, binge/churn detection'
    SCHEDULE 'media_3hourly_schedule'
    TAGS 'media,streaming,engagement,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Session reconstruction using LAG/LEAD
-- =============================================================================
-- A new session starts if the gap between consecutive events for a user is > 30 min.

MERGE INTO {{zone_prefix}}.silver.sessions_reconstructed AS tgt
USING (
    WITH event_gaps AS (
        SELECT
            event_id,
            user_id,
            device_id,
            event_timestamp,
            event_type,
            content_id,
            watch_duration_sec,
            position_pct,
            LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_event_ts,
            CASE
                WHEN LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) IS NULL THEN 1
                WHEN EXTRACT(EPOCH FROM (event_timestamp - LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp))) > 1800 THEN 1
                ELSE 0
            END AS is_new_session
        FROM {{zone_prefix}}.bronze.raw_viewing_events
    ),
    session_numbered AS (
        SELECT
            *,
            SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_num
        FROM event_gaps
    ),
    session_agg AS (
        SELECT
            user_id || '-S' || LPAD(CAST(session_num AS STRING), 4, '0') AS session_id,
            user_id,
            MAX(device_id) AS device_id,
            MIN(event_timestamp) AS start_time,
            MAX(event_timestamp) AS end_time,
            SUM(watch_duration_sec) AS total_watch_sec,
            COUNT(DISTINCT content_id) AS content_count,
            CAST(AVG(CASE WHEN position_pct > 0 THEN position_pct END) AS DECIMAL(5,2)) AS avg_completion_pct,
            -- Binge = 3+ distinct content items of same series in one session
            CASE WHEN COUNT(DISTINCT content_id) >= 3 THEN true ELSE false END AS is_binge,
            MAX(event_timestamp) AS enriched_at
        FROM session_numbered
        GROUP BY user_id, session_num, user_id || '-S' || LPAD(CAST(session_num AS STRING), 4, '0')
    )
    SELECT * FROM session_agg
) AS src
ON tgt.session_id = src.session_id
WHEN MATCHED THEN UPDATE SET
    tgt.end_time           = src.end_time,
    tgt.total_watch_sec    = src.total_watch_sec,
    tgt.content_count      = src.content_count,
    tgt.avg_completion_pct = src.avg_completion_pct,
    tgt.is_binge           = src.is_binge,
    tgt.enriched_at        = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    session_id, user_id, device_id, start_time, end_time, total_watch_sec,
    content_count, avg_completion_pct, is_binge, enriched_at
) VALUES (
    src.session_id, src.user_id, src.device_id, src.start_time, src.end_time,
    src.total_watch_sec, src.content_count, src.avg_completion_pct,
    src.is_binge, src.enriched_at
);

-- =============================================================================
-- STEP 2: SILVER - Engagement scoring with churn signal detection
-- =============================================================================
-- Churn signal: user has no activity in the last 14 days from max event date.
-- Completion pct derived from position_pct on terminal events (complete/abandon).

MERGE INTO {{zone_prefix}}.silver.engagement_scored AS tgt
USING (
    WITH max_event_date AS (
        SELECT MAX(event_timestamp) AS latest_ts
        FROM {{zone_prefix}}.bronze.raw_viewing_events
    ),
    user_last_activity AS (
        SELECT user_id, MAX(event_timestamp) AS last_active
        FROM {{zone_prefix}}.bronze.raw_viewing_events
        GROUP BY user_id
    ),
    session_lookup AS (
        SELECT
            e.event_id,
            s.session_id
        FROM {{zone_prefix}}.bronze.raw_viewing_events e
        JOIN {{zone_prefix}}.silver.sessions_reconstructed s
            ON e.user_id = s.user_id
            AND e.event_timestamp >= s.start_time
            AND e.event_timestamp <= s.end_time
    )
    SELECT
        e.event_id,
        e.user_id,
        e.content_id,
        e.device_id,
        sl.session_id,
        e.event_timestamp,
        e.event_type,
        e.watch_duration_sec,
        e.position_pct,
        e.quality_level,
        CASE
            WHEN e.event_type = 'complete' THEN 100.00
            WHEN e.event_type IN ('abandon', 'pause', 'seek') THEN e.position_pct
            ELSE 0.00
        END AS completion_pct,
        CASE
            WHEN s.is_binge = true THEN true
            ELSE false
        END AS is_binge_event,
        CASE
            WHEN EXTRACT(EPOCH FROM (m.latest_ts - u.last_active)) > 1209600 THEN true
            ELSE false
        END AS churn_signal,
        e.ingested_at AS enriched_at
    FROM {{zone_prefix}}.bronze.raw_viewing_events e
    LEFT JOIN session_lookup sl ON e.event_id = sl.event_id
    LEFT JOIN {{zone_prefix}}.silver.sessions_reconstructed s
        ON sl.session_id = s.session_id
    CROSS JOIN max_event_date m
    JOIN user_last_activity u ON e.user_id = u.user_id
) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED THEN UPDATE SET
    tgt.session_id         = src.session_id,
    tgt.completion_pct     = src.completion_pct,
    tgt.is_binge_event     = src.is_binge_event,
    tgt.churn_signal       = src.churn_signal,
    tgt.enriched_at        = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    event_id, user_id, content_id, device_id, session_id, event_timestamp,
    event_type, watch_duration_sec, position_pct, quality_level,
    completion_pct, is_binge_event, churn_signal, enriched_at
) VALUES (
    src.event_id, src.user_id, src.content_id, src.device_id, src.session_id,
    src.event_timestamp, src.event_type, src.watch_duration_sec, src.position_pct,
    src.quality_level, src.completion_pct, src.is_binge_event, src.churn_signal,
    src.enriched_at
);

-- =============================================================================
-- STEP 3: GOLD - Populate dim_user
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_user AS tgt
USING (
    SELECT user_id AS user_key, user_id, subscription_tier, signup_date,
           country, age_band, preferred_genre
    FROM {{zone_prefix}}.bronze.raw_users
) AS src
ON tgt.user_key = src.user_key
WHEN MATCHED THEN UPDATE SET
    tgt.subscription_tier = src.subscription_tier,
    tgt.country           = src.country,
    tgt.age_band          = src.age_band,
    tgt.preferred_genre   = src.preferred_genre
WHEN NOT MATCHED THEN INSERT (
    user_key, user_id, subscription_tier, signup_date, country, age_band, preferred_genre
) VALUES (
    src.user_key, src.user_id, src.subscription_tier, src.signup_date,
    src.country, src.age_band, src.preferred_genre
);

-- =============================================================================
-- STEP 4: GOLD - Populate dim_content
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_content AS tgt
USING (
    SELECT content_id AS content_key, content_id, title, genre, release_year,
           duration_min, rating, content_type, production_cost
    FROM {{zone_prefix}}.bronze.raw_content
) AS src
ON tgt.content_key = src.content_key
WHEN MATCHED THEN UPDATE SET
    tgt.title           = src.title,
    tgt.genre           = src.genre,
    tgt.release_year    = src.release_year,
    tgt.duration_min    = src.duration_min,
    tgt.rating          = src.rating,
    tgt.content_type    = src.content_type,
    tgt.production_cost = src.production_cost
WHEN NOT MATCHED THEN INSERT (
    content_key, content_id, title, genre, release_year, duration_min, rating, content_type, production_cost
) VALUES (
    src.content_key, src.content_id, src.title, src.genre, src.release_year,
    src.duration_min, src.rating, src.content_type, src.production_cost
);

-- =============================================================================
-- STEP 5: GOLD - Populate dim_device
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_device AS tgt
USING (
    SELECT device_id AS device_key, device_type, os, app_version, screen_resolution
    FROM {{zone_prefix}}.bronze.raw_devices
) AS src
ON tgt.device_key = src.device_key
WHEN MATCHED THEN UPDATE SET
    tgt.device_type       = src.device_type,
    tgt.os                = src.os,
    tgt.app_version       = src.app_version,
    tgt.screen_resolution = src.screen_resolution
WHEN NOT MATCHED THEN INSERT (
    device_key, device_type, os, app_version, screen_resolution
) VALUES (
    src.device_key, src.device_type, src.os, src.app_version, src.screen_resolution
);

-- =============================================================================
-- STEP 6: GOLD - Populate dim_session
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_session AS tgt
USING (
    SELECT
        session_id AS session_key,
        session_id,
        user_id AS user_key,
        start_time,
        end_time,
        total_watch_sec,
        content_count,
        avg_completion_pct
    FROM {{zone_prefix}}.silver.sessions_reconstructed
) AS src
ON tgt.session_key = src.session_key
WHEN MATCHED THEN UPDATE SET
    tgt.end_time           = src.end_time,
    tgt.total_watch_sec    = src.total_watch_sec,
    tgt.content_count      = src.content_count,
    tgt.avg_completion_pct = src.avg_completion_pct
WHEN NOT MATCHED THEN INSERT (
    session_key, session_id, user_key, start_time, end_time, total_watch_sec, content_count, avg_completion_pct
) VALUES (
    src.session_key, src.session_id, src.user_key, src.start_time, src.end_time,
    src.total_watch_sec, src.content_count, src.avg_completion_pct
);

-- =============================================================================
-- STEP 7: GOLD - Populate fact_viewing_events
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_viewing_events AS tgt
USING (
    SELECT
        e.event_id       AS event_key,
        e.user_id        AS user_key,
        e.content_id     AS content_key,
        e.device_id      AS device_key,
        e.session_id     AS session_key,
        e.event_timestamp,
        e.event_type,
        e.watch_duration_sec,
        e.position_pct,
        e.quality_level
    FROM {{zone_prefix}}.silver.engagement_scored e
) AS src
ON tgt.event_key = src.event_key
WHEN MATCHED THEN UPDATE SET
    tgt.session_key        = src.session_key,
    tgt.watch_duration_sec = src.watch_duration_sec,
    tgt.position_pct       = src.position_pct,
    tgt.quality_level      = src.quality_level
WHEN NOT MATCHED THEN INSERT (
    event_key, user_key, content_key, device_key, session_key,
    event_timestamp, event_type, watch_duration_sec, position_pct, quality_level
) VALUES (
    src.event_key, src.user_key, src.content_key, src.device_key, src.session_key,
    src.event_timestamp, src.event_type, src.watch_duration_sec, src.position_pct,
    src.quality_level
);

-- =============================================================================
-- STEP 8: GOLD - KPI Engagement with content ROI, binge rate, churn signals
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_engagement AS tgt
USING (
    WITH content_period AS (
        SELECT
            e.content_id,
            c.genre,
            CAST(EXTRACT(YEAR FROM e.event_timestamp) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM e.event_timestamp) AS STRING), 2, '0') AS period,
            COUNT(*) FILTER (WHERE e.event_type = 'play') AS total_views,
            COUNT(DISTINCT e.user_id) AS unique_viewers,
            CAST(AVG(e.completion_pct) FILTER (WHERE e.event_type IN ('complete', 'abandon')) AS DECIMAL(5,2)) AS avg_completion_pct,
            CAST(AVG(e.watch_duration_sec) / 60.0 AS DECIMAL(7,2)) AS avg_watch_duration_min,
            CAST(
                COUNT(*) FILTER (WHERE e.is_binge_event = true) * 100.0
                / NULLIF(COUNT(*), 0)
            AS DECIMAL(5,2)) AS binge_rate,
            CAST(
                COUNT(DISTINCT e.user_id) FILTER (WHERE e.churn_signal = true) * 100.0
                / NULLIF(COUNT(DISTINCT e.user_id), 0)
            AS DECIMAL(5,2)) AS churn_signal_pct,
            c.production_cost
        FROM {{zone_prefix}}.silver.engagement_scored e
        JOIN {{zone_prefix}}.bronze.raw_content c ON e.content_id = c.content_id
        GROUP BY e.content_id, c.genre, c.production_cost,
            CAST(EXTRACT(YEAR FROM e.event_timestamp) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM e.event_timestamp) AS STRING), 2, '0')
    )
    SELECT
        content_id,
        genre,
        period,
        total_views,
        unique_viewers,
        avg_completion_pct,
        avg_watch_duration_min,
        binge_rate,
        churn_signal_pct,
        CASE
            WHEN production_cost > 0
            THEN CAST((total_views * avg_watch_duration_min) / (production_cost / 1000000.0) AS DECIMAL(10,4))
            ELSE 0.0
        END AS content_roi
    FROM content_period
) AS src
ON tgt.content_id = src.content_id AND tgt.period = src.period
WHEN MATCHED THEN UPDATE SET
    tgt.total_views            = src.total_views,
    tgt.unique_viewers         = src.unique_viewers,
    tgt.avg_completion_pct     = src.avg_completion_pct,
    tgt.avg_watch_duration_min = src.avg_watch_duration_min,
    tgt.binge_rate             = src.binge_rate,
    tgt.churn_signal_pct       = src.churn_signal_pct,
    tgt.content_roi            = src.content_roi
WHEN NOT MATCHED THEN INSERT (
    content_id, genre, period, total_views, unique_viewers, avg_completion_pct,
    avg_watch_duration_min, binge_rate, churn_signal_pct, content_roi
) VALUES (
    src.content_id, src.genre, src.period, src.total_views, src.unique_viewers,
    src.avg_completion_pct, src.avg_watch_duration_min, src.binge_rate,
    src.churn_signal_pct, src.content_roi
);

-- =============================================================================
-- STEP 9: OPTIMIZE compaction
-- =============================================================================

OPTIMIZE {{zone_prefix}}.gold.fact_viewing_events;
OPTIMIZE {{zone_prefix}}.silver.engagement_scored;

VACUUM {{zone_prefix}}.gold.fact_viewing_events RETAIN 168 HOURS;
