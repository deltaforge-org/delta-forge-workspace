-- =============================================================================
-- Media Streaming Analytics Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using INCREMENTAL_FILTER macro.
-- Inserts new viewing events (March 2024 batch), processes incrementally.

-- Show current watermark
SELECT MAX(event_id) AS max_event_id, MAX(enriched_at) AS latest_enriched
FROM {{zone_prefix}}.silver.engagement_scored;

-- Current row counts
SELECT 'silver.engagement_scored' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.engagement_scored
UNION ALL
SELECT 'gold.fact_viewing_events', COUNT(*)
FROM {{zone_prefix}}.gold.fact_viewing_events;

-- =============================================================================
-- Insert 10 new viewing events (March 2024 incremental batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_viewing_events VALUES
(5073, 'USR-001', 'CNT-007', 'DEV-TV-001',  '2024-03-01T20:00:00', 'play',     0,    0.00, '4K',    '2024-03-01T20:00:01'),
(5074, 'USR-001', 'CNT-007', 'DEV-TV-001',  '2024-03-01T22:05:00', 'complete', 7500, 100.00, '4K',   '2024-03-01T22:05:01'),
(5075, 'USR-003', 'CNT-014', 'DEV-TAB-001', '2024-03-02T15:30:00', 'complete', 6600, 100.00, '1080p','2024-03-02T15:30:01'),
(5076, 'USR-005', 'CNT-012', 'DEV-TV-003',  '2024-03-05T09:00:00', 'play',     0,    0.00, '4K',    '2024-03-05T09:00:01'),
(5077, 'USR-005', 'CNT-012', 'DEV-TV-003',  '2024-03-05T10:25:00', 'complete', 5100, 100.00, '4K',   '2024-03-05T10:25:01'),
(5078, 'USR-007', 'CNT-001', 'DEV-MOB-001', '2024-03-08T12:00:00', 'play',     0,    0.00, '720p',  '2024-03-08T12:00:01'),
(5079, 'USR-007', 'CNT-001', 'DEV-MOB-001', '2024-03-08T13:15:00', 'abandon',  4500, 50.68, '720p',  '2024-03-08T13:15:01'),
(5080, 'USR-010', 'CNT-011', 'DEV-WEB-001', '2024-03-10T22:00:00', 'play',     0,    0.00, '1080p', '2024-03-10T22:00:01'),
(5081, 'USR-010', 'CNT-011', 'DEV-WEB-001', '2024-03-10T23:42:00', 'complete', 6120, 100.00, '1080p','2024-03-10T23:42:01'),
(5082, 'USR-012', 'CNT-013', 'DEV-MOB-002', '2024-03-12T18:00:00', 'play',     0,    0.00, '1080p', '2024-03-12T18:00:01');

ASSERT ROW_COUNT = 10
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental session reconstruction
-- Uses INCREMENTAL_FILTER to only process new events
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.sessions_reconstructed AS tgt
USING (
    WITH event_gaps AS (
        SELECT
            event_id,
            user_id,
            device_id,
            event_timestamp,
            content_id,
            watch_duration_sec,
            position_pct,
            CASE
                WHEN LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) IS NULL THEN 1
                WHEN EXTRACT(EPOCH FROM (event_timestamp - LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp))) > 1800 THEN 1
                ELSE 0
            END AS is_new_session
        FROM {{zone_prefix}}.bronze.raw_viewing_events
        WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.sessions_reconstructed, null, start_time, 3)}}
    ),
    session_numbered AS (
        SELECT *, SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_num
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
-- Incremental engagement scoring
-- =============================================================================

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
        SELECT e.event_id, s.session_id
        FROM {{zone_prefix}}.bronze.raw_viewing_events e
        JOIN {{zone_prefix}}.silver.sessions_reconstructed s
            ON e.user_id = s.user_id
            AND e.event_timestamp >= s.start_time AND e.event_timestamp <= s.end_time
    )
    SELECT
        e.event_id, e.user_id, e.content_id, e.device_id, sl.session_id,
        e.event_timestamp, e.event_type, e.watch_duration_sec, e.position_pct,
        e.quality_level,
        CASE WHEN e.event_type = 'complete' THEN 100.00
             WHEN e.event_type IN ('abandon','pause','seek') THEN e.position_pct
             ELSE 0.00 END AS completion_pct,
        COALESCE(s.is_binge, false) AS is_binge_event,
        CASE WHEN EXTRACT(EPOCH FROM (m.latest_ts - u.last_active)) > 1209600 THEN true ELSE false END AS churn_signal,
        e.ingested_at AS enriched_at
    FROM {{zone_prefix}}.bronze.raw_viewing_events e
    LEFT JOIN session_lookup sl ON e.event_id = sl.event_id
    LEFT JOIN {{zone_prefix}}.silver.sessions_reconstructed s ON sl.session_id = s.session_id
    CROSS JOIN max_event_date m
    JOIN user_last_activity u ON e.user_id = u.user_id
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.engagement_scored, event_id, event_timestamp, 3)}}
) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED THEN UPDATE SET
    tgt.session_id     = src.session_id,
    tgt.completion_pct = src.completion_pct,
    tgt.is_binge_event = src.is_binge_event,
    tgt.churn_signal   = src.churn_signal,
    tgt.enriched_at    = src.enriched_at
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
-- Incremental fact refresh
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_viewing_events AS tgt
USING (
    SELECT
        e.event_id AS event_key, e.user_id AS user_key, e.content_id AS content_key,
        e.device_id AS device_key, e.session_id AS session_key,
        e.event_timestamp, e.event_type, e.watch_duration_sec, e.position_pct, e.quality_level
    FROM {{zone_prefix}}.silver.engagement_scored e
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_viewing_events, event_key, event_timestamp, 3)}}
) AS src
ON tgt.event_key = src.event_key
WHEN MATCHED THEN UPDATE SET
    tgt.session_key        = src.session_key,
    tgt.watch_duration_sec = src.watch_duration_sec,
    tgt.position_pct       = src.position_pct
WHEN NOT MATCHED THEN INSERT (
    event_key, user_key, content_key, device_key, session_key,
    event_timestamp, event_type, watch_duration_sec, position_pct, quality_level
) VALUES (
    src.event_key, src.user_key, src.content_key, src.device_key, src.session_key,
    src.event_timestamp, src.event_type, src.watch_duration_sec, src.position_pct,
    src.quality_level
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.engagement_scored;
ASSERT VALUE silver_total = 82

SELECT COUNT(*) AS gold_fact_total FROM {{zone_prefix}}.gold.fact_viewing_events;
-- Verify March events exist
ASSERT VALUE gold_fact_total = 82
SELECT COUNT(*) AS march_events
FROM {{zone_prefix}}.gold.fact_viewing_events
WHERE event_timestamp >= '2024-03-01T00:00:00';

ASSERT VALUE march_events = 10
SELECT 'march_events check passed' AS march_events_status;

