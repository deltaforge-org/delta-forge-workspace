-- =============================================================================
-- Telecom CDR Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE telecom_daily_schedule
    CRON '0 2 * * *'
    TIMEZONE 'UTC'
    RETRIES 3
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE telecom_cdr_pipeline
    DESCRIPTION 'Daily CDR processing with schema evolution, drop detection, and network quality analytics'
    SCHEDULE 'telecom_daily_schedule'
    TAGS 'telecom,cdr,network-quality'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich CDR with subscriber and tower lookups, detect drops
-- =============================================================================
-- Duration calculated from start/end times. Drop flag = voice call < 10 seconds.
-- Join to subscriber and tower reference for enrichment.

MERGE INTO {{zone_prefix}}.silver.cdr_enriched AS tgt
USING (
    WITH subscriber_lookup AS (
        SELECT subscriber_id, phone_number
        FROM {{zone_prefix}}.bronze.raw_subscribers
    ),
    enriched AS (
        SELECT
            c.cdr_id,
            caller_sub.subscriber_id AS caller_id,
            callee_sub.subscriber_id AS callee_id,
            c.caller_number,
            c.callee_number,
            c.tower_id,
            t.city         AS tower_city,
            t.region       AS tower_region,
            c.start_time,
            c.end_time,
            CASE
                WHEN c.call_type = 'voice' AND c.end_time IS NOT NULL
                THEN CAST(EXTRACT(EPOCH FROM (c.end_time - c.start_time)) AS INT)
                WHEN c.call_type = 'sms' THEN 0
                ELSE NULL
            END AS duration_sec,
            c.call_type,
            c.data_usage_mb,
            false AS roaming_flag,
            CASE
                WHEN c.call_type = 'voice' AND c.end_time IS NOT NULL
                    AND CAST(EXTRACT(EPOCH FROM (c.end_time - c.start_time)) AS INT) < 10
                THEN true
                ELSE false
            END AS drop_flag,
            c.revenue,
            c.ingested_at
        FROM {{zone_prefix}}.bronze.raw_cdr c
        LEFT JOIN subscriber_lookup caller_sub ON c.caller_number = caller_sub.phone_number
        LEFT JOIN subscriber_lookup callee_sub ON c.callee_number = callee_sub.phone_number
        LEFT JOIN {{zone_prefix}}.bronze.raw_cell_towers t ON c.tower_id = t.tower_id
    )
    SELECT * FROM enriched
) AS src
ON tgt.cdr_id = src.cdr_id
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec  = src.duration_sec,
    tgt.drop_flag     = src.drop_flag,
    tgt.roaming_flag  = src.roaming_flag,
    tgt.enriched_at   = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    cdr_id, caller_id, callee_id, caller_number, callee_number, tower_id,
    tower_city, tower_region, start_time, end_time, duration_sec, call_type,
    data_usage_mb, roaming_flag, drop_flag, revenue, enriched_at
) VALUES (
    src.cdr_id, src.caller_id, src.callee_id, src.caller_number, src.callee_number,
    src.tower_id, src.tower_city, src.tower_region, src.start_time, src.end_time,
    src.duration_sec, src.call_type, src.data_usage_mb, src.roaming_flag,
    src.drop_flag, src.revenue, src.ingested_at
);

-- =============================================================================
-- STEP 2: SILVER - Upsert subscriber profiles from CDR activity
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.subscriber_profiles AS tgt
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
            COUNT(DISTINCT e.cdr_id) AS total_calls,
            COALESCE(SUM(e.data_usage_mb), 0) AS total_data_mb,
            COALESCE(SUM(e.revenue), 0) AS total_revenue,
            COALESCE(CAST(AVG(CASE WHEN e.call_type = 'voice' THEN e.duration_sec END) AS INT), 0) AS avg_call_duration,
            CASE
                WHEN COUNT(CASE WHEN e.call_type = 'voice' THEN 1 END) > 0
                THEN CAST(
                    COUNT(CASE WHEN e.drop_flag = true THEN 1 END) * 1.0
                    / COUNT(CASE WHEN e.call_type = 'voice' THEN 1 END)
                AS DECIMAL(5,4))
                ELSE 0
            END AS drop_rate,
            MAX(e.start_time) AS last_activity
        FROM {{zone_prefix}}.bronze.raw_subscribers s
        LEFT JOIN {{zone_prefix}}.silver.cdr_enriched e
            ON s.subscriber_id = e.caller_id OR s.subscriber_id = e.callee_id
        GROUP BY s.subscriber_id, s.phone_number, s.plan_type, s.plan_tier,
                 s.activation_date, s.status, s.monthly_spend
    )
    SELECT * FROM activity
) AS src
ON tgt.subscriber_id = src.subscriber_id
WHEN MATCHED THEN UPDATE SET
    tgt.total_calls      = src.total_calls,
    tgt.total_data_mb    = src.total_data_mb,
    tgt.total_revenue    = src.total_revenue,
    tgt.avg_call_duration= src.avg_call_duration,
    tgt.drop_rate        = src.drop_rate,
    tgt.last_activity    = src.last_activity,
    tgt.updated_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    subscriber_id, phone_number, plan_type, plan_tier, activation_date, status,
    monthly_spend, total_calls, total_data_mb, total_revenue, avg_call_duration,
    drop_rate, last_activity, updated_at
) VALUES (
    src.subscriber_id, src.phone_number, src.plan_type, src.plan_tier,
    src.activation_date, src.status, src.monthly_spend, src.total_calls,
    src.total_data_mb, src.total_revenue, src.avg_call_duration, src.drop_rate,
    src.last_activity, CURRENT_TIMESTAMP
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_cell_tower
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_cell_tower AS tgt
USING (
    SELECT
        tower_id AS tower_key,
        tower_id,
        location,
        city,
        region,
        technology,
        capacity_mhz
    FROM {{zone_prefix}}.bronze.raw_cell_towers
) AS src
ON tgt.tower_key = src.tower_key
WHEN MATCHED THEN UPDATE SET
    tgt.technology   = src.technology,
    tgt.capacity_mhz = src.capacity_mhz
WHEN NOT MATCHED THEN INSERT (tower_key, tower_id, location, city, region, technology, capacity_mhz)
VALUES (src.tower_key, src.tower_id, src.location, src.city, src.region, src.technology, src.capacity_mhz);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_subscriber
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_subscriber AS tgt
USING (
    SELECT
        subscriber_id AS subscriber_key,
        phone_number,
        plan_type,
        plan_tier,
        activation_date,
        status,
        monthly_spend
    FROM {{zone_prefix}}.silver.subscriber_profiles
) AS src
ON tgt.subscriber_key = src.subscriber_key
WHEN MATCHED THEN UPDATE SET
    tgt.status        = src.status,
    tgt.monthly_spend = src.monthly_spend
WHEN NOT MATCHED THEN INSERT (subscriber_key, phone_number, plan_type, plan_tier, activation_date, status, monthly_spend)
VALUES (src.subscriber_key, src.phone_number, src.plan_type, src.plan_tier, src.activation_date, src.status, src.monthly_spend);

-- =============================================================================
-- STEP 5: GOLD - Fact: fact_calls
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_calls AS tgt
USING (
    SELECT
        cdr_id        AS call_key,
        caller_id     AS caller_key,
        callee_id     AS callee_key,
        tower_id      AS cell_tower_key,
        start_time,
        duration_sec,
        call_type,
        data_usage_mb,
        roaming_flag,
        drop_flag,
        revenue
    FROM {{zone_prefix}}.silver.cdr_enriched
) AS src
ON tgt.call_key = src.call_key
WHEN MATCHED THEN UPDATE SET
    tgt.duration_sec  = src.duration_sec,
    tgt.drop_flag     = src.drop_flag,
    tgt.roaming_flag  = src.roaming_flag,
    tgt.revenue       = src.revenue
WHEN NOT MATCHED THEN INSERT (
    call_key, caller_key, callee_key, cell_tower_key, start_time,
    duration_sec, call_type, data_usage_mb, roaming_flag, drop_flag, revenue
) VALUES (
    src.call_key, src.caller_key, src.callee_key, src.cell_tower_key,
    src.start_time, src.duration_sec, src.call_type, src.data_usage_mb,
    src.roaming_flag, src.drop_flag, src.revenue
);

-- =============================================================================
-- STEP 6: GOLD - KPI: kpi_network_quality
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_network_quality AS tgt
USING (
    SELECT
        tower_region                                AS region,
        EXTRACT(HOUR FROM start_time)               AS hour_bucket,
        COUNT(*)                                    AS total_calls,
        COUNT(CASE WHEN drop_flag = true THEN 1 END) AS dropped_calls,
        CASE
            WHEN COUNT(*) > 0
            THEN CAST(COUNT(CASE WHEN drop_flag = true THEN 1 END) * 1.0 / COUNT(*) AS DECIMAL(5,4))
            ELSE 0
        END AS drop_rate,
        CAST(AVG(CASE WHEN call_type = 'voice' THEN duration_sec END) AS DECIMAL(8,1)) AS avg_duration,
        COALESCE(SUM(data_usage_mb), 0) AS total_data_mb,
        COUNT(DISTINCT tower_id) AS peak_concurrent,
        SUM(revenue) AS revenue
    FROM {{zone_prefix}}.silver.cdr_enriched
    GROUP BY tower_region, EXTRACT(HOUR FROM start_time)
) AS src
ON tgt.region = src.region AND tgt.hour_bucket = src.hour_bucket
WHEN MATCHED THEN UPDATE SET
    tgt.total_calls     = src.total_calls,
    tgt.dropped_calls   = src.dropped_calls,
    tgt.drop_rate       = src.drop_rate,
    tgt.avg_duration    = src.avg_duration,
    tgt.total_data_mb   = src.total_data_mb,
    tgt.peak_concurrent = src.peak_concurrent,
    tgt.revenue         = src.revenue
WHEN NOT MATCHED THEN INSERT (
    region, hour_bucket, total_calls, dropped_calls, drop_rate,
    avg_duration, total_data_mb, peak_concurrent, revenue
) VALUES (
    src.region, src.hour_bucket, src.total_calls, src.dropped_calls,
    src.drop_rate, src.avg_duration, src.total_data_mb, src.peak_concurrent,
    src.revenue
);

-- =============================================================================
-- OPTIMIZE & VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.cdr_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_calls;
OPTIMIZE {{zone_prefix}}.gold.kpi_network_quality;
VACUUM {{zone_prefix}}.bronze.raw_cdr RETAIN 168 HOURS;
VACUUM {{zone_prefix}}.silver.cdr_enriched RETAIN 168 HOURS;
