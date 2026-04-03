-- =============================================================================
-- Hospitality Reservations Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE hotel_4hourly_schedule
    CRON '0 */4 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE hospitality_reservations_pipeline
    DESCRIPTION 'Hotel reservations pipeline with soft deletes, dedup, RevPAR/ADR analytics, and loyalty analysis'
    SCHEDULE 'hotel_4hourly_schedule'
    TAGS 'hospitality,reservations,revpar,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Dedup bookings and enrich with derived fields
-- =============================================================================
-- Filter out duplicate booking attempts, derive nights, booking lead days,
-- soft delete cancellations (is_cancelled=true, keep row).

MERGE INTO {{zone_prefix}}.silver.bookings_deduped AS tgt
USING (
    SELECT
        b.booking_id,
        b.property_id,
        b.room_type_id,
        b.guest_id,
        b.channel_id,
        b.booking_date,
        b.check_in_date,
        b.check_out_date,
        CAST(EXTRACT(EPOCH FROM (b.check_out_date - b.check_in_date)) / 86400 AS INT) AS nights,
        b.room_rate,
        b.total_amount,
        b.status,
        CASE WHEN b.status IN ('cancelled') THEN true ELSE false END AS is_cancelled,
        CAST(EXTRACT(EPOCH FROM (b.check_in_date - b.booking_date)) / 86400 AS INT) AS booking_lead_days,
        g.loyalty_tier,
        b.ingested_at
    FROM {{zone_prefix}}.bronze.raw_bookings b
    LEFT JOIN {{zone_prefix}}.bronze.raw_guests g ON b.guest_id = g.guest_id
    WHERE b.is_duplicate = false
) AS src
ON tgt.booking_id = src.booking_id
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.status            = src.status,
    tgt.is_cancelled      = src.is_cancelled,
    tgt.total_amount      = src.total_amount,
    tgt.room_rate         = src.room_rate,
    tgt.enriched_at       = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    booking_id, property_id, room_type_id, guest_id, channel_id,
    booking_date, check_in_date, check_out_date, nights, room_rate,
    total_amount, status, is_cancelled, booking_lead_days, loyalty_tier, enriched_at
) VALUES (
    src.booking_id, src.property_id, src.room_type_id, src.guest_id, src.channel_id,
    src.booking_date, src.check_in_date, src.check_out_date, src.nights, src.room_rate,
    src.total_amount, src.status, src.is_cancelled, src.booking_lead_days,
    src.loyalty_tier, src.ingested_at
);

-- =============================================================================
-- STEP 2: GOLD - Populate dim_property
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_property AS tgt
USING (
    SELECT property_id AS property_key, property_id, property_name, brand,
           city, state, country, star_rating, total_rooms, avg_rack_rate
    FROM {{zone_prefix}}.bronze.raw_properties
) AS src
ON tgt.property_key = src.property_key
WHEN MATCHED THEN UPDATE SET
    tgt.property_name = src.property_name,
    tgt.brand         = src.brand,
    tgt.city          = src.city,
    tgt.total_rooms   = src.total_rooms,
    tgt.avg_rack_rate = src.avg_rack_rate
WHEN NOT MATCHED THEN INSERT (
    property_key, property_id, property_name, brand, city, state, country, star_rating, total_rooms, avg_rack_rate
) VALUES (
    src.property_key, src.property_id, src.property_name, src.brand, src.city,
    src.state, src.country, src.star_rating, src.total_rooms, src.avg_rack_rate
);

-- =============================================================================
-- STEP 3: GOLD - Populate dim_room_type
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_room_type AS tgt
USING (
    SELECT room_type_id AS room_type_key, room_type, max_occupancy, base_rate, amenities, view_type
    FROM {{zone_prefix}}.bronze.raw_room_types
) AS src
ON tgt.room_type_key = src.room_type_key
WHEN MATCHED THEN UPDATE SET
    tgt.room_type     = src.room_type,
    tgt.max_occupancy = src.max_occupancy,
    tgt.base_rate     = src.base_rate,
    tgt.amenities     = src.amenities,
    tgt.view_type     = src.view_type
WHEN NOT MATCHED THEN INSERT (
    room_type_key, room_type, max_occupancy, base_rate, amenities, view_type
) VALUES (
    src.room_type_key, src.room_type, src.max_occupancy, src.base_rate, src.amenities, src.view_type
);

-- =============================================================================
-- STEP 4: GOLD - Populate dim_guest
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_guest AS tgt
USING (
    SELECT guest_id AS guest_key, guest_id, name, loyalty_tier,
           lifetime_stays, lifetime_revenue, home_city, home_country
    FROM {{zone_prefix}}.bronze.raw_guests
) AS src
ON tgt.guest_key = src.guest_key
WHEN MATCHED THEN UPDATE SET
    tgt.name             = src.name,
    tgt.loyalty_tier     = src.loyalty_tier,
    tgt.lifetime_stays   = src.lifetime_stays,
    tgt.lifetime_revenue = src.lifetime_revenue
WHEN NOT MATCHED THEN INSERT (
    guest_key, guest_id, name, loyalty_tier, lifetime_stays, lifetime_revenue, home_city, home_country
) VALUES (
    src.guest_key, src.guest_id, src.name, src.loyalty_tier, src.lifetime_stays,
    src.lifetime_revenue, src.home_city, src.home_country
);

-- =============================================================================
-- STEP 5: GOLD - Populate dim_channel
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_channel AS tgt
USING (
    SELECT channel_id AS channel_key, channel_name, channel_type, commission_pct
    FROM {{zone_prefix}}.bronze.raw_channels
) AS src
ON tgt.channel_key = src.channel_key
WHEN MATCHED THEN UPDATE SET
    tgt.channel_name   = src.channel_name,
    tgt.channel_type   = src.channel_type,
    tgt.commission_pct = src.commission_pct
WHEN NOT MATCHED THEN INSERT (channel_key, channel_name, channel_type, commission_pct)
VALUES (src.channel_key, src.channel_name, src.channel_type, src.commission_pct);

-- =============================================================================
-- STEP 6: GOLD - Populate fact_reservations (include cancelled with is_cancelled flag)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_reservations AS tgt
USING (
    SELECT
        b.booking_id      AS reservation_key,
        b.property_id     AS property_key,
        b.room_type_id    AS room_type_key,
        b.guest_id        AS guest_key,
        b.channel_id      AS channel_key,
        b.check_in_date,
        b.check_out_date,
        b.nights,
        b.room_rate,
        b.total_amount,
        b.status,
        b.booking_lead_days,
        b.is_cancelled
    FROM {{zone_prefix}}.silver.bookings_deduped b
) AS src
ON tgt.reservation_key = src.reservation_key
WHEN MATCHED THEN UPDATE SET
    tgt.status            = src.status,
    tgt.is_cancelled      = src.is_cancelled,
    tgt.total_amount      = src.total_amount,
    tgt.room_rate         = src.room_rate
WHEN NOT MATCHED THEN INSERT (
    reservation_key, property_key, room_type_key, guest_key, channel_key,
    check_in_date, check_out_date, nights, room_rate, total_amount,
    status, booking_lead_days, is_cancelled
) VALUES (
    src.reservation_key, src.property_key, src.room_type_key, src.guest_key,
    src.channel_key, src.check_in_date, src.check_out_date, src.nights,
    src.room_rate, src.total_amount, src.status, src.booking_lead_days, src.is_cancelled
);

-- =============================================================================
-- STEP 7: GOLD - KPI Revenue Management
-- =============================================================================
-- RevPAR = Total Revenue / (Total Rooms * Days in Month)
-- ADR = Total Revenue / Room Nights Sold (non-cancelled)
-- Occupancy = Room Nights Sold / Available Room Nights

MERGE INTO {{zone_prefix}}.gold.kpi_revenue_management AS tgt
USING (
    WITH monthly_metrics AS (
        SELECT
            f.property_key AS property_id,
            CAST(EXTRACT(YEAR FROM f.check_in_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM f.check_in_date) AS STRING), 2, '0') AS month,
            dp.total_rooms,
            SUM(CASE WHEN f.is_cancelled = false THEN f.nights ELSE 0 END) AS room_nights_sold,
            CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END) AS DECIMAL(14,2)) AS total_revenue,
            COUNT(*) AS total_bookings,
            COUNT(*) FILTER (WHERE f.is_cancelled = true) AS cancelled_count,
            CAST(AVG(f.booking_lead_days) AS DECIMAL(7,2)) AS avg_lead_time,
            CAST(COUNT(*) FILTER (WHERE dc.channel_type = 'direct') * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS direct_booking_pct,
            CAST(
                SUM(CASE WHEN dg.loyalty_tier IN ('gold','platinum') AND f.is_cancelled = false THEN f.total_amount ELSE 0 END) * 100.0
                / NULLIF(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END), 0)
            AS DECIMAL(5,2)) AS loyalty_revenue_pct,
            CAST(
                COUNT(*) FILTER (WHERE EXTRACT(DOW FROM f.check_in_date) BETWEEN 1 AND 5) * 1.0
                / NULLIF(COUNT(*) FILTER (WHERE EXTRACT(DOW FROM f.check_in_date) IN (0, 6)), 0)
            AS DECIMAL(5,2)) AS weekday_vs_weekend_ratio
        FROM {{zone_prefix}}.gold.fact_reservations f
        JOIN {{zone_prefix}}.gold.dim_property dp ON f.property_key = dp.property_key
        JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
        JOIN {{zone_prefix}}.gold.dim_guest dg ON f.guest_key = dg.guest_key
        GROUP BY f.property_key, dp.total_rooms,
            CAST(EXTRACT(YEAR FROM f.check_in_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM f.check_in_date) AS STRING), 2, '0')
    )
    SELECT
        property_id,
        month,
        CAST(room_nights_sold * 100.0 / NULLIF(total_rooms * 30, 0) AS DECIMAL(5,2)) AS occupancy_rate,
        CAST(total_revenue / NULLIF(room_nights_sold, 0) AS DECIMAL(10,2)) AS adr,
        CAST(total_revenue / NULLIF(total_rooms * 30, 0) AS DECIMAL(10,2)) AS revpar,
        total_revenue,
        CAST(cancelled_count * 100.0 / NULLIF(total_bookings, 0) AS DECIMAL(5,2)) AS cancellation_rate,
        avg_lead_time,
        direct_booking_pct,
        loyalty_revenue_pct,
        weekday_vs_weekend_ratio
    FROM monthly_metrics
) AS src
ON tgt.property_id = src.property_id AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.occupancy_rate           = src.occupancy_rate,
    tgt.adr                      = src.adr,
    tgt.revpar                   = src.revpar,
    tgt.total_revenue            = src.total_revenue,
    tgt.cancellation_rate        = src.cancellation_rate,
    tgt.avg_lead_time            = src.avg_lead_time,
    tgt.direct_booking_pct       = src.direct_booking_pct,
    tgt.loyalty_revenue_pct      = src.loyalty_revenue_pct,
    tgt.weekday_vs_weekend_ratio = src.weekday_vs_weekend_ratio
WHEN NOT MATCHED THEN INSERT (
    property_id, month, occupancy_rate, adr, revpar, total_revenue,
    cancellation_rate, avg_lead_time, direct_booking_pct, loyalty_revenue_pct,
    weekday_vs_weekend_ratio
) VALUES (
    src.property_id, src.month, src.occupancy_rate, src.adr, src.revpar,
    src.total_revenue, src.cancellation_rate, src.avg_lead_time,
    src.direct_booking_pct, src.loyalty_revenue_pct, src.weekday_vs_weekend_ratio
);

-- =============================================================================
-- STEP 8: OPTIMIZE and VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.gold.fact_reservations;
VACUUM {{zone_prefix}}.gold.fact_reservations RETAIN 168 HOURS;
