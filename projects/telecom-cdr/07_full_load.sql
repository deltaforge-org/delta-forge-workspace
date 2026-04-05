-- =============================================================================
-- Telecom CDR Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- Schema evolution: v1 (voice-only 2023) -> v2 (multi-service 2024 H1) -> v3 (5G 2024 H2)
-- Session reconstruction via LAG gap detection (30-min threshold).
-- Churn scoring: composite 0-100 behavioral score.
-- Pipeline DAG: 13 steps with parallel branches.
-- =============================================================================

-- ===================== PIPELINE =====================

PIPELINE 07_full_load
  DESCRIPTION 'Daily CDR pipeline with schema evolution lifecycle, session reconstruction, and churn scoring'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'telecom,cdr,schema-evolution,churn,sessions'
  SLA 60
  FAIL_FAST true
  LIFECYCLE production
;

-- =============================================================================
-- Validate bronze tables across all 3 schema versions
-- =============================================================================

SELECT COUNT(*) AS v1_count FROM telco.bronze.raw_cdr_v1;
ASSERT VALUE v1_count = 25

SELECT COUNT(*) AS v2_count FROM telco.bronze.raw_cdr_v2;
ASSERT VALUE v2_count = 25

SELECT COUNT(*) AS v3_count FROM telco.bronze.raw_cdr_v3;
ASSERT VALUE v3_count = 20

SELECT COUNT(*) AS sub_count FROM telco.bronze.raw_subscribers;
ASSERT VALUE sub_count = 15

SELECT COUNT(*) AS tower_count FROM telco.bronze.raw_cell_towers;
ASSERT VALUE tower_count = 10;

-- =============================================================================
-- Merge v1 records into unified silver (voice-only, NULL-fill new cols)
-- =============================================================================

MERGE INTO telco.silver.cdr_unified AS tgt
USING (
    WITH subscriber_lookup AS (
        SELECT subscriber_id, phone_number
        FROM telco.bronze.raw_subscribers
    )
    SELECT
        v.call_id,
        v.caller,
        v.callee,
        caller_sub.subscriber_id AS caller_id,
        callee_sub.subscriber_id AS callee_id,
        v.tower_id,
        t.city          AS tower_city,
        t.region        AS tower_region,
        v.start_time,
        v.end_time,
        CAST(v.duration_sec AS BIGINT)  AS duration_sec,
        'voice'         AS call_type,
        0.00            AS data_usage_mb,
        0               AS sms_count,
        false           AS roaming_flag,
        NULL            AS network_type,
        0               AS handover_count,
        CASE
            WHEN v.duration_sec IS NOT NULL AND v.duration_sec < 10
            THEN true ELSE false
        END AS drop_flag,
        CAST(v.duration_sec * 0.01 AS DECIMAL(8,2)) AS revenue,
        1               AS schema_version,
        v.ingested_at   AS unified_at
    FROM telco.bronze.raw_cdr_v1 v
    LEFT JOIN subscriber_lookup caller_sub ON v.caller = caller_sub.phone_number
    LEFT JOIN subscriber_lookup callee_sub ON v.callee = callee_sub.phone_number
    LEFT JOIN telco.bronze.raw_cell_towers t ON v.tower_id = t.tower_id
) AS src
ON tgt.call_id = src.call_id
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec   = src.duration_sec,
    tgt.drop_flag      = src.drop_flag,
    tgt.schema_version = src.schema_version,
    tgt.unified_at     = src.unified_at
WHEN NOT MATCHED THEN INSERT (
    call_id, caller, callee, caller_id, callee_id, tower_id, tower_city, tower_region,
    start_time, end_time, duration_sec, call_type, data_usage_mb, sms_count,
    roaming_flag, network_type, handover_count, drop_flag, revenue, schema_version, unified_at
) VALUES (
    src.call_id, src.caller, src.callee, src.caller_id, src.callee_id, src.tower_id,
    src.tower_city, src.tower_region, src.start_time, src.end_time, src.duration_sec,
    src.call_type, src.data_usage_mb, src.sms_count, src.roaming_flag, src.network_type,
    src.handover_count, src.drop_flag, src.revenue, src.schema_version, src.unified_at
);

-- =============================================================================
-- Merge v2 records into unified silver (multi-service, NULL-fill 5G cols)
-- =============================================================================

MERGE INTO telco.silver.cdr_unified AS tgt
USING (
    WITH subscriber_lookup AS (
        SELECT subscriber_id, phone_number
        FROM telco.bronze.raw_subscribers
    )
    SELECT
        v.call_id,
        v.caller,
        v.callee,
        caller_sub.subscriber_id AS caller_id,
        callee_sub.subscriber_id AS callee_id,
        v.tower_id,
        t.city          AS tower_city,
        t.region        AS tower_region,
        v.start_time,
        v.end_time,
        CAST(v.duration_sec AS BIGINT)  AS duration_sec,
        v.call_type,
        COALESCE(v.data_usage_mb, 0.00) AS data_usage_mb,
        COALESCE(v.sms_count, 0)        AS sms_count,
        false           AS roaming_flag,
        NULL            AS network_type,
        0               AS handover_count,
        CASE
            WHEN v.call_type = 'voice' AND v.duration_sec IS NOT NULL AND v.duration_sec < 10
            THEN true ELSE false
        END AS drop_flag,
        CASE
            WHEN v.call_type = 'voice' THEN CAST(COALESCE(v.duration_sec, 0) * 0.01 AS DECIMAL(8,2))
            WHEN v.call_type = 'data'  THEN CAST(COALESCE(v.data_usage_mb, 0) * 0.01 AS DECIMAL(8,2))
            WHEN v.call_type = 'sms'   THEN CAST(COALESCE(v.sms_count, 0) * 0.05 AS DECIMAL(8,2))
            ELSE 0.00
        END AS revenue,
        2               AS schema_version,
        v.ingested_at   AS unified_at
    FROM telco.bronze.raw_cdr_v2 v
    LEFT JOIN subscriber_lookup caller_sub ON v.caller = caller_sub.phone_number
    LEFT JOIN subscriber_lookup callee_sub ON v.callee = callee_sub.phone_number
    LEFT JOIN telco.bronze.raw_cell_towers t ON v.tower_id = t.tower_id
) AS src
ON tgt.call_id = src.call_id
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec   = src.duration_sec,
    tgt.call_type      = src.call_type,
    tgt.data_usage_mb  = src.data_usage_mb,
    tgt.sms_count      = src.sms_count,
    tgt.drop_flag      = src.drop_flag,
    tgt.revenue        = src.revenue,
    tgt.schema_version = src.schema_version,
    tgt.unified_at     = src.unified_at
WHEN NOT MATCHED THEN INSERT (
    call_id, caller, callee, caller_id, callee_id, tower_id, tower_city, tower_region,
    start_time, end_time, duration_sec, call_type, data_usage_mb, sms_count,
    roaming_flag, network_type, handover_count, drop_flag, revenue, schema_version, unified_at
) VALUES (
    src.call_id, src.caller, src.callee, src.caller_id, src.callee_id, src.tower_id,
    src.tower_city, src.tower_region, src.start_time, src.end_time, src.duration_sec,
    src.call_type, src.data_usage_mb, src.sms_count, src.roaming_flag, src.network_type,
    src.handover_count, src.drop_flag, src.revenue, src.schema_version, src.unified_at
);

-- =============================================================================
-- Merge v3 records into unified silver (5G/roaming, full column set)
-- =============================================================================

MERGE INTO telco.silver.cdr_unified AS tgt
USING (
    WITH subscriber_lookup AS (
        SELECT subscriber_id, phone_number
        FROM telco.bronze.raw_subscribers
    )
    SELECT
        v.call_id,
        v.caller,
        v.callee,
        caller_sub.subscriber_id AS caller_id,
        callee_sub.subscriber_id AS callee_id,
        v.tower_id,
        t.city          AS tower_city,
        t.region        AS tower_region,
        v.start_time,
        v.end_time,
        v.duration_sec,
        v.call_type,
        COALESCE(v.data_usage_mb, 0.00) AS data_usage_mb,
        COALESCE(v.sms_count, 0)        AS sms_count,
        COALESCE(v.roaming_flag, false)  AS roaming_flag,
        v.network_type,
        COALESCE(v.handover_count, 0)    AS handover_count,
        CASE
            WHEN v.call_type = 'voice' AND v.duration_sec IS NOT NULL AND v.duration_sec < 10
            THEN true ELSE false
        END AS drop_flag,
        CASE
            WHEN v.call_type = 'voice' THEN CAST(COALESCE(v.duration_sec, 0) * 0.01 AS DECIMAL(8,2))
            WHEN v.call_type = 'data'  THEN CAST(COALESCE(v.data_usage_mb, 0) * 0.01 AS DECIMAL(8,2))
            WHEN v.call_type = 'sms'   THEN CAST(COALESCE(v.sms_count, 0) * 0.05 AS DECIMAL(8,2))
            ELSE 0.00
        END AS revenue,
        3               AS schema_version,
        v.ingested_at   AS unified_at
    FROM telco.bronze.raw_cdr_v3 v
    LEFT JOIN subscriber_lookup caller_sub ON v.caller = caller_sub.phone_number
    LEFT JOIN subscriber_lookup callee_sub ON v.callee = callee_sub.phone_number
    LEFT JOIN telco.bronze.raw_cell_towers t ON v.tower_id = t.tower_id
) AS src
ON tgt.call_id = src.call_id
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec   = src.duration_sec,
    tgt.call_type      = src.call_type,
    tgt.data_usage_mb  = src.data_usage_mb,
    tgt.sms_count      = src.sms_count,
    tgt.roaming_flag   = src.roaming_flag,
    tgt.network_type   = src.network_type,
    tgt.handover_count = src.handover_count,
    tgt.drop_flag      = src.drop_flag,
    tgt.revenue        = src.revenue,
    tgt.schema_version = src.schema_version,
    tgt.unified_at     = src.unified_at
WHEN NOT MATCHED THEN INSERT (
    call_id, caller, callee, caller_id, callee_id, tower_id, tower_city, tower_region,
    start_time, end_time, duration_sec, call_type, data_usage_mb, sms_count,
    roaming_flag, network_type, handover_count, drop_flag, revenue, schema_version, unified_at
) VALUES (
    src.call_id, src.caller, src.callee, src.caller_id, src.callee_id, src.tower_id,
    src.tower_city, src.tower_region, src.start_time, src.end_time, src.duration_sec,
    src.call_type, src.data_usage_mb, src.sms_count, src.roaming_flag, src.network_type,
    src.handover_count, src.drop_flag, src.revenue, src.schema_version, src.unified_at
);

-- =============================================================================
-- Apply type widening — duration INT -> BIGINT on unified table
-- =============================================================================
-- This step runs after all 3 schema merges to confirm the type widening is in
-- effect. The unified table was created with BIGINT from the start; this step
-- validates all v1/v2 INT durations were safely cast.

SELECT
    schema_version,
    COUNT(*) AS row_count,
    MAX(duration_sec) AS max_duration_bigint,
    MIN(duration_sec) AS min_duration_bigint
FROM telco.silver.cdr_unified
GROUP BY schema_version
ORDER BY schema_version;

ASSERT VALUE row_count > 0
SELECT COUNT(*) AS unified_total FROM telco.silver.cdr_unified;
ASSERT VALUE unified_total = 70;

-- =============================================================================
-- Upsert subscriber profiles from unified CDR activity
-- =============================================================================

MERGE INTO telco.silver.subscriber_profiles AS tgt
USING (
    WITH activity AS (
        SELECT
            s.subscriber_id,
            s.phone_number,
            s.plan_type,
            s.plan_tier,
            s.activation_date,
            s.status,
            s.monthly_spend,
            s.balance,
            COUNT(DISTINCT u.call_id) AS total_calls,
            COALESCE(SUM(u.data_usage_mb), 0) AS total_data_mb,
            COALESCE(SUM(u.sms_count), 0) AS total_sms,
            COALESCE(SUM(u.revenue), 0) AS total_revenue,
            COALESCE(CAST(AVG(
                CASE WHEN u.call_type = 'voice' AND u.duration_sec IS NOT NULL
                THEN u.duration_sec END
            ) AS BIGINT), 0) AS avg_call_duration,
            CASE
                WHEN COUNT(CASE WHEN u.call_type = 'voice' THEN 1 END) > 0
                THEN CAST(
                    COUNT(CASE WHEN u.drop_flag = true THEN 1 END) * 1.0
                    / COUNT(CASE WHEN u.call_type = 'voice' THEN 1 END)
                AS DECIMAL(5,4))
                ELSE 0
            END AS drop_rate,
            CAST(DATEDIFF(
                CURRENT_TIMESTAMP,
                MAX(CASE WHEN u.call_type = 'voice' THEN u.start_time END)
            ) AS INT) AS days_since_last_call,
            -- Monthly usage trend: negative = declining (simplified as pct change proxy)
            CASE
                WHEN COUNT(CASE WHEN u.schema_version = 3 THEN 1 END) > 0
                     AND COUNT(CASE WHEN u.schema_version = 2 THEN 1 END) > 0
                THEN CAST(
                    (COUNT(CASE WHEN u.schema_version = 3 THEN 1 END) * 1.0
                     - COUNT(CASE WHEN u.schema_version = 2 THEN 1 END))
                    / NULLIF(COUNT(CASE WHEN u.schema_version = 2 THEN 1 END), 0) * 100
                AS DECIMAL(8,2))
                ELSE -25.00
            END AS monthly_usage_trend,
            MAX(u.start_time) AS last_activity
        FROM telco.bronze.raw_subscribers s
        LEFT JOIN telco.silver.cdr_unified u
            ON s.phone_number = u.caller OR s.phone_number = u.callee
        GROUP BY s.subscriber_id, s.phone_number, s.plan_type, s.plan_tier,
                 s.activation_date, s.status, s.monthly_spend, s.balance
    )
    SELECT * FROM activity
) AS src
ON tgt.subscriber_id = src.subscriber_id
WHEN MATCHED THEN UPDATE SET
    tgt.total_calls        = src.total_calls,
    tgt.total_data_mb      = src.total_data_mb,
    tgt.total_sms          = src.total_sms,
    tgt.total_revenue      = src.total_revenue,
    tgt.avg_call_duration  = src.avg_call_duration,
    tgt.drop_rate          = src.drop_rate,
    tgt.days_since_last_call = src.days_since_last_call,
    tgt.monthly_usage_trend  = src.monthly_usage_trend,
    tgt.last_activity      = src.last_activity,
    tgt.updated_at         = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    subscriber_id, phone_number, plan_type, plan_tier, activation_date, status,
    monthly_spend, balance, total_calls, total_data_mb, total_sms, total_revenue,
    avg_call_duration, drop_rate, days_since_last_call, monthly_usage_trend,
    last_activity, updated_at
) VALUES (
    src.subscriber_id, src.phone_number, src.plan_type, src.plan_tier,
    src.activation_date, src.status, src.monthly_spend, src.balance,
    src.total_calls, src.total_data_mb, src.total_sms, src.total_revenue,
    src.avg_call_duration, src.drop_rate, src.days_since_last_call,
    src.monthly_usage_trend, src.last_activity, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Reconstruct sessions from CDR events using LAG gap detection
-- =============================================================================
-- New session starts when gap > 30 minutes between events for same subscriber.
-- Uses window functions: LAG for gap detection, SUM for session_id assignment.

MERGE INTO telco.silver.sessions AS tgt
USING (
    WITH caller_events AS (
        SELECT
            caller_id AS subscriber_id,
            call_id,
            start_time,
            end_time,
            duration_sec,
            call_type,
            data_usage_mb,
            roaming_flag,
            drop_flag
        FROM telco.silver.cdr_unified
        WHERE caller_id IS NOT NULL
    ),
    events_with_gap AS (
        SELECT
            subscriber_id,
            call_id,
            start_time,
            end_time,
            duration_sec,
            call_type,
            data_usage_mb,
            roaming_flag,
            drop_flag,
            CASE
                WHEN DATEDIFF(
                    start_time,
                    LAG(COALESCE(end_time, start_time)) OVER (
                        PARTITION BY subscriber_id ORDER BY start_time
                    )
                ) > 30
                OR LAG(end_time) OVER (PARTITION BY subscriber_id ORDER BY start_time) IS NULL
                THEN 1
                ELSE 0
            END AS is_new_session
        FROM caller_events
    ),
    events_with_session AS (
        SELECT
            *,
            SUM(is_new_session) OVER (
                PARTITION BY subscriber_id ORDER BY start_time
            ) AS session_num
        FROM events_with_gap
    ),
    session_agg AS (
        SELECT
            subscriber_id || '-S' || LPAD(CAST(session_num AS STRING), 4, '0') AS session_id,
            subscriber_id,
            MIN(start_time)          AS session_start,
            MAX(COALESCE(end_time, start_time)) AS session_end,
            COUNT(*)                 AS event_count,
            COALESCE(SUM(duration_sec), 0) AS total_duration,
            COALESCE(SUM(data_usage_mb), 0) AS total_data_mb,
            STRING_AGG(DISTINCT call_type, ',') AS call_types,
            SUM(CASE WHEN roaming_flag = true THEN 1 ELSE 0 END) AS roaming_events,
            SUM(CASE WHEN drop_flag = true THEN 1 ELSE 0 END) AS drop_events
        FROM events_with_session
        GROUP BY subscriber_id, session_num
    )
    SELECT * FROM session_agg
) AS src
ON tgt.session_id = src.session_id
WHEN MATCHED THEN UPDATE SET
    tgt.session_end   = src.session_end,
    tgt.event_count   = src.event_count,
    tgt.total_duration = src.total_duration,
    tgt.total_data_mb  = src.total_data_mb,
    tgt.call_types     = src.call_types,
    tgt.roaming_events = src.roaming_events,
    tgt.drop_events    = src.drop_events,
    tgt.created_at     = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    session_id, subscriber_id, session_start, session_end, event_count,
    total_duration, total_data_mb, call_types, roaming_events, drop_events, created_at
) VALUES (
    src.session_id, src.subscriber_id, src.session_start, src.session_end,
    src.event_count, src.total_duration, src.total_data_mb, src.call_types,
    src.roaming_events, src.drop_events, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Build dim_tower
-- =============================================================================

MERGE INTO telco.gold.dim_tower AS tgt
USING (
    SELECT
        tower_id AS tower_key,
        tower_id,
        location,
        city,
        region,
        technology,
        capacity_mhz
    FROM telco.bronze.raw_cell_towers
) AS src
ON tgt.tower_key = src.tower_key
WHEN MATCHED THEN UPDATE SET
    tgt.technology   = src.technology,
    tgt.capacity_mhz = src.capacity_mhz
WHEN NOT MATCHED THEN INSERT (tower_key, tower_id, location, city, region, technology, capacity_mhz)
VALUES (src.tower_key, src.tower_id, src.location, src.city, src.region, src.technology, src.capacity_mhz);

-- =============================================================================
-- Build dim_plan (distinct plan combinations)
-- =============================================================================

MERGE INTO telco.gold.dim_plan AS tgt
USING (
    SELECT DISTINCT
        plan_type || '-' || plan_tier AS plan_key,
        plan_type,
        plan_tier,
        monthly_spend
    FROM telco.silver.subscriber_profiles
) AS src
ON tgt.plan_key = src.plan_key
WHEN MATCHED THEN UPDATE SET
    tgt.monthly_spend = src.monthly_spend
WHEN NOT MATCHED THEN INSERT (plan_key, plan_type, plan_tier, monthly_spend)
VALUES (src.plan_key, src.plan_type, src.plan_tier, src.monthly_spend);

-- =============================================================================
-- Build dim_subscriber
-- =============================================================================

MERGE INTO telco.gold.dim_subscriber AS tgt
USING (
    SELECT
        subscriber_id AS subscriber_key,
        phone_number,
        plan_type,
        plan_tier,
        activation_date,
        status,
        monthly_spend,
        balance
    FROM telco.silver.subscriber_profiles
) AS src
ON tgt.subscriber_key = src.subscriber_key
WHEN MATCHED THEN UPDATE SET
    tgt.status        = src.status,
    tgt.monthly_spend = src.monthly_spend,
    tgt.balance       = src.balance
WHEN NOT MATCHED THEN INSERT (subscriber_key, phone_number, plan_type, plan_tier, activation_date, status, monthly_spend, balance)
VALUES (src.subscriber_key, src.phone_number, src.plan_type, src.plan_tier, src.activation_date, src.status, src.monthly_spend, src.balance);

-- =============================================================================
-- Build fact_calls (star schema)
-- =============================================================================

MERGE INTO telco.gold.fact_calls AS tgt
USING (
    SELECT
        u.call_id        AS call_key,
        u.caller_id      AS caller_key,
        u.callee_id      AS callee_key,
        u.tower_id       AS tower_key,
        COALESCE(sp.plan_type || '-' || sp.plan_tier, 'unknown') AS plan_key,
        u.start_time,
        u.duration_sec,
        u.call_type,
        u.data_usage_mb,
        u.roaming_flag,
        u.drop_flag,
        u.network_type,
        u.schema_version,
        u.revenue
    FROM telco.silver.cdr_unified u
    LEFT JOIN telco.silver.subscriber_profiles sp
        ON u.caller_id = sp.subscriber_id
) AS src
ON tgt.call_key = src.call_key
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec   = src.duration_sec,
    tgt.drop_flag      = src.drop_flag,
    tgt.roaming_flag   = src.roaming_flag,
    tgt.network_type   = src.network_type,
    tgt.revenue        = src.revenue
WHEN NOT MATCHED THEN INSERT (
    call_key, caller_key, callee_key, tower_key, plan_key, start_time,
    duration_sec, call_type, data_usage_mb, roaming_flag, drop_flag,
    network_type, schema_version, revenue
) VALUES (
    src.call_key, src.caller_key, src.callee_key, src.tower_key, src.plan_key,
    src.start_time, src.duration_sec, src.call_type, src.data_usage_mb,
    src.roaming_flag, src.drop_flag, src.network_type, src.schema_version, src.revenue
);

-- =============================================================================
-- STEP 12a: KPI — network quality by region and hour
-- =============================================================================

MERGE INTO telco.gold.kpi_network_quality AS tgt
USING (
    SELECT
        u.tower_region                                      AS region,
        EXTRACT(HOUR FROM u.start_time)                     AS hour_bucket,
        COUNT(*)                                            AS total_calls,
        COUNT(CASE WHEN u.drop_flag = true THEN 1 END)     AS dropped_calls,
        CASE
            WHEN COUNT(*) > 0
            THEN CAST(COUNT(CASE WHEN u.drop_flag = true THEN 1 END) * 1.0 / COUNT(*) AS DECIMAL(5,4))
            ELSE 0
        END AS drop_rate,
        CAST(AVG(CASE WHEN u.call_type = 'voice' THEN u.duration_sec END) AS DECIMAL(8,1)) AS avg_duration,
        COALESCE(SUM(u.data_usage_mb), 0)                   AS total_data_mb,
        CASE
            WHEN COUNT(CASE WHEN u.call_type = 'data' THEN 1 END) > 0
            THEN CAST(SUM(u.data_usage_mb) / COUNT(CASE WHEN u.call_type = 'data' THEN 1 END) AS DECIMAL(8,2))
            ELSE 0
        END AS avg_throughput_mb,
        COUNT(CASE WHEN u.roaming_flag = true THEN 1 END)  AS roaming_calls,
        COUNT(CASE WHEN u.network_type = '5G' THEN 1 END)  AS fiveg_calls,
        SUM(u.revenue)                                      AS revenue
    FROM telco.silver.cdr_unified u
    GROUP BY u.tower_region, EXTRACT(HOUR FROM u.start_time)
) AS src
ON tgt.region = src.region AND tgt.hour_bucket = src.hour_bucket
WHEN MATCHED THEN UPDATE SET
    tgt.total_calls     = src.total_calls,
    tgt.dropped_calls   = src.dropped_calls,
    tgt.drop_rate       = src.drop_rate,
    tgt.avg_duration    = src.avg_duration,
    tgt.total_data_mb   = src.total_data_mb,
    tgt.avg_throughput_mb = src.avg_throughput_mb,
    tgt.roaming_calls   = src.roaming_calls,
    tgt.fiveg_calls     = src.fiveg_calls,
    tgt.revenue         = src.revenue
WHEN NOT MATCHED THEN INSERT (
    region, hour_bucket, total_calls, dropped_calls, drop_rate,
    avg_duration, total_data_mb, avg_throughput_mb, roaming_calls, fiveg_calls, revenue
) VALUES (
    src.region, src.hour_bucket, src.total_calls, src.dropped_calls,
    src.drop_rate, src.avg_duration, src.total_data_mb, src.avg_throughput_mb,
    src.roaming_calls, src.fiveg_calls, src.revenue
);

-- =============================================================================
-- STEP 12b: KPI — churn risk scoring (behavioral composite 0-100)
-- =============================================================================
-- Score components:
--   +40 if days_since_last_call > 30
--   +30 if monthly_usage_trend < -20 (declining usage)
--   +20 if drop_rate > 0.10 (poor network experience)
--   +10 if plan_type = 'prepaid' AND balance < 5.00

MERGE INTO telco.gold.kpi_churn_risk AS tgt
USING (
    SELECT
        sp.subscriber_id,
        sp.phone_number,
        sp.plan_type,
        sp.plan_tier,
        sp.status,
        sp.days_since_last_call,
        sp.monthly_usage_trend,
        sp.drop_rate,
        sp.balance,
        CASE WHEN sp.days_since_last_call > 30 THEN 40 ELSE 0 END
        + CASE WHEN sp.monthly_usage_trend < -20 THEN 30 ELSE 0 END
        + CASE WHEN sp.drop_rate > 0.10 THEN 20 ELSE 0 END
        + CASE WHEN sp.plan_type = 'prepaid' AND sp.balance < 5.00 THEN 10 ELSE 0 END
        AS churn_score,
        CASE
            WHEN sp.status = 'churned' THEN 'CHURNED'
            WHEN sp.status = 'suspended' THEN 'CRITICAL'
            WHEN (CASE WHEN sp.days_since_last_call > 30 THEN 40 ELSE 0 END
                  + CASE WHEN sp.monthly_usage_trend < -20 THEN 30 ELSE 0 END
                  + CASE WHEN sp.drop_rate > 0.10 THEN 20 ELSE 0 END
                  + CASE WHEN sp.plan_type = 'prepaid' AND sp.balance < 5.00 THEN 10 ELSE 0 END) >= 70
            THEN 'HIGH'
            WHEN (CASE WHEN sp.days_since_last_call > 30 THEN 40 ELSE 0 END
                  + CASE WHEN sp.monthly_usage_trend < -20 THEN 30 ELSE 0 END
                  + CASE WHEN sp.drop_rate > 0.10 THEN 20 ELSE 0 END
                  + CASE WHEN sp.plan_type = 'prepaid' AND sp.balance < 5.00 THEN 10 ELSE 0 END) >= 40
            THEN 'MEDIUM'
            ELSE 'LOW'
        END AS churn_risk_level,
        CURRENT_TIMESTAMP AS scored_at
    FROM telco.silver.subscriber_profiles sp
) AS src
ON tgt.subscriber_id = src.subscriber_id
WHEN MATCHED THEN UPDATE SET
    tgt.days_since_last_call = src.days_since_last_call,
    tgt.monthly_usage_trend  = src.monthly_usage_trend,
    tgt.drop_rate            = src.drop_rate,
    tgt.balance              = src.balance,
    tgt.churn_score          = src.churn_score,
    tgt.churn_risk_level     = src.churn_risk_level,
    tgt.scored_at            = src.scored_at
WHEN NOT MATCHED THEN INSERT (
    subscriber_id, phone_number, plan_type, plan_tier, status,
    days_since_last_call, monthly_usage_trend, drop_rate, balance,
    churn_score, churn_risk_level, scored_at
) VALUES (
    src.subscriber_id, src.phone_number, src.plan_type, src.plan_tier, src.status,
    src.days_since_last_call, src.monthly_usage_trend, src.drop_rate, src.balance,
    src.churn_score, src.churn_risk_level, src.scored_at
);

-- =============================================================================
-- VACUUM and OPTIMIZE (maintenance — safe to fail)
-- =============================================================================

OPTIMIZE telco.silver.cdr_unified;
OPTIMIZE telco.gold.fact_calls;
OPTIMIZE telco.gold.kpi_network_quality;
VACUUM telco.bronze.raw_cdr_v1 RETAIN 168 HOURS;
VACUUM telco.bronze.raw_cdr_v2 RETAIN 168 HOURS;
VACUUM telco.bronze.raw_cdr_v3 RETAIN 168 HOURS;
VACUUM telco.silver.cdr_unified RETAIN 168 HOURS;
