-- =============================================================================
-- Aviation Flight Operations Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE aviation_daily_4am_schedule
    CRON '0 4 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE aviation_flight_ops_pipeline
    DESCRIPTION 'Aviation flight ops pipeline with composite key MERGE, OTP analysis, fuel efficiency, and route profitability'
    SCHEDULE 'aviation_daily_4am_schedule'
    TAGS 'aviation,flights,otp,medallion'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich flights with derived metrics
-- =============================================================================
-- MERGE on composite key (flight_number, departure_date).
-- Derive: delay_category, load_factor, fuel_per_pax_nm, is_on_time, is_anomaly.

MERGE INTO {{zone_prefix}}.silver.flights_enriched AS tgt
USING (
    SELECT
        f.flight_number,
        f.departure_date,
        f.route_id,
        f.registration,
        f.crew_id,
        f.scheduled_departure,
        f.actual_departure,
        f.scheduled_arrival,
        f.actual_arrival,
        f.delay_minutes,
        f.delay_reason,
        CASE
            WHEN f.flight_status = 'cancelled' THEN 'cancelled'
            WHEN f.delay_minutes = 0 THEN 'on_time'
            WHEN f.delay_minutes <= 15 THEN 'minor_delay'
            WHEN f.delay_minutes <= 60 THEN 'moderate_delay'
            ELSE 'severe_delay'
        END AS delay_category,
        f.pax_count,
        a.seat_capacity,
        CASE WHEN a.seat_capacity > 0 AND f.pax_count > 0
            THEN CAST(f.pax_count * 100.0 / a.seat_capacity AS DECIMAL(5,2))
            ELSE 0.00 END AS load_factor,
        f.fuel_consumed_kg,
        CASE WHEN f.pax_count > 0 AND r.distance_nm > 0
            THEN CAST(f.fuel_consumed_kg / (f.pax_count * r.distance_nm) AS DECIMAL(8,4))
            ELSE 0.0 END AS fuel_per_pax_nm,
        f.revenue,
        f.flight_status,
        CASE WHEN f.delay_minutes <= 15 AND f.flight_status != 'cancelled' THEN true ELSE false END AS is_on_time,
        CASE WHEN f.delay_minutes > 120 THEN true ELSE false END AS is_anomaly,
        f.ingested_at
    FROM {{zone_prefix}}.bronze.raw_flight_ops f
    LEFT JOIN {{zone_prefix}}.bronze.raw_aircraft a ON f.registration = a.registration
    LEFT JOIN {{zone_prefix}}.bronze.raw_routes r ON f.route_id = r.route_id
) AS src
ON tgt.flight_number = src.flight_number AND tgt.departure_date = src.departure_date
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.actual_departure  = src.actual_departure,
    tgt.actual_arrival    = src.actual_arrival,
    tgt.delay_minutes     = src.delay_minutes,
    tgt.delay_reason      = src.delay_reason,
    tgt.delay_category    = src.delay_category,
    tgt.pax_count         = src.pax_count,
    tgt.load_factor       = src.load_factor,
    tgt.fuel_consumed_kg  = src.fuel_consumed_kg,
    tgt.fuel_per_pax_nm   = src.fuel_per_pax_nm,
    tgt.revenue           = src.revenue,
    tgt.flight_status     = src.flight_status,
    tgt.is_on_time        = src.is_on_time,
    tgt.is_anomaly        = src.is_anomaly,
    tgt.enriched_at       = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    flight_number, departure_date, route_id, registration, crew_id,
    scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
    delay_minutes, delay_reason, delay_category, pax_count, seat_capacity,
    load_factor, fuel_consumed_kg, fuel_per_pax_nm, revenue, flight_status,
    is_on_time, is_anomaly, enriched_at
) VALUES (
    src.flight_number, src.departure_date, src.route_id, src.registration, src.crew_id,
    src.scheduled_departure, src.actual_departure, src.scheduled_arrival, src.actual_arrival,
    src.delay_minutes, src.delay_reason, src.delay_category, src.pax_count, src.seat_capacity,
    src.load_factor, src.fuel_consumed_kg, src.fuel_per_pax_nm, src.revenue, src.flight_status,
    src.is_on_time, src.is_anomaly, src.enriched_at
);

-- =============================================================================
-- STEP 2: GOLD - Populate dim_route
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_route AS tgt
USING (
    SELECT route_id AS route_key, origin_iata, origin_city, dest_iata, dest_city,
           distance_nm, block_time_min, domestic_flag
    FROM {{zone_prefix}}.bronze.raw_routes
) AS src
ON tgt.route_key = src.route_key
WHEN MATCHED THEN UPDATE SET
    tgt.origin_iata    = src.origin_iata,
    tgt.origin_city    = src.origin_city,
    tgt.dest_iata      = src.dest_iata,
    tgt.dest_city      = src.dest_city,
    tgt.distance_nm    = src.distance_nm,
    tgt.block_time_min = src.block_time_min,
    tgt.domestic_flag  = src.domestic_flag
WHEN NOT MATCHED THEN INSERT (
    route_key, origin_iata, origin_city, dest_iata, dest_city, distance_nm, block_time_min, domestic_flag
) VALUES (
    src.route_key, src.origin_iata, src.origin_city, src.dest_iata, src.dest_city,
    src.distance_nm, src.block_time_min, src.domestic_flag
);

-- =============================================================================
-- STEP 3: GOLD - Populate dim_aircraft
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_aircraft AS tgt
USING (
    SELECT registration AS aircraft_key, registration, aircraft_type, seat_capacity,
           range_nm, fuel_efficiency, age_years, maintenance_status
    FROM {{zone_prefix}}.bronze.raw_aircraft
) AS src
ON tgt.aircraft_key = src.aircraft_key
WHEN MATCHED THEN UPDATE SET
    tgt.aircraft_type      = src.aircraft_type,
    tgt.seat_capacity      = src.seat_capacity,
    tgt.range_nm           = src.range_nm,
    tgt.fuel_efficiency    = src.fuel_efficiency,
    tgt.age_years          = src.age_years,
    tgt.maintenance_status = src.maintenance_status
WHEN NOT MATCHED THEN INSERT (
    aircraft_key, registration, aircraft_type, seat_capacity, range_nm, fuel_efficiency, age_years, maintenance_status
) VALUES (
    src.aircraft_key, src.registration, src.aircraft_type, src.seat_capacity,
    src.range_nm, src.fuel_efficiency, src.age_years, src.maintenance_status
);

-- =============================================================================
-- STEP 4: GOLD - Populate dim_crew
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_crew AS tgt
USING (
    SELECT crew_id AS crew_key, crew_id, captain_name, first_officer_name,
           crew_base, duty_hours_month
    FROM {{zone_prefix}}.bronze.raw_crew
) AS src
ON tgt.crew_key = src.crew_key
WHEN MATCHED THEN UPDATE SET
    tgt.captain_name       = src.captain_name,
    tgt.first_officer_name = src.first_officer_name,
    tgt.crew_base          = src.crew_base,
    tgt.duty_hours_month   = src.duty_hours_month
WHEN NOT MATCHED THEN INSERT (
    crew_key, crew_id, captain_name, first_officer_name, crew_base, duty_hours_month
) VALUES (
    src.crew_key, src.crew_id, src.captain_name, src.first_officer_name,
    src.crew_base, src.duty_hours_month
);

-- =============================================================================
-- STEP 5: GOLD - Populate dim_date
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_date AS tgt
USING (
    SELECT DISTINCT
        CAST(departure_date AS STRING) AS date_key,
        departure_date AS full_date,
        CASE EXTRACT(DOW FROM departure_date)
            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(MONTH FROM departure_date) AS month,
        EXTRACT(QUARTER FROM departure_date) AS quarter,
        EXTRACT(YEAR FROM departure_date) AS year,
        CASE WHEN EXTRACT(MONTH FROM departure_date) IN (6,7,8,11,12) THEN true ELSE false END AS is_peak_season
    FROM {{zone_prefix}}.silver.flights_enriched
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day_of_week, month, quarter, year, is_peak_season
) VALUES (
    src.date_key, src.full_date, src.day_of_week, src.month, src.quarter, src.year, src.is_peak_season
);

-- =============================================================================
-- STEP 6: GOLD - Populate fact_flights
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_flights AS tgt
USING (
    SELECT
        f.flight_number || '-' || CAST(f.departure_date AS STRING) AS flight_key,
        f.route_id          AS route_key,
        f.registration      AS aircraft_key,
        f.crew_id           AS crew_key,
        CAST(f.departure_date AS STRING) AS date_key,
        f.flight_number,
        f.scheduled_departure,
        f.actual_departure,
        f.scheduled_arrival,
        f.actual_arrival,
        f.delay_minutes,
        f.delay_reason,
        f.pax_count,
        f.fuel_consumed_kg,
        f.revenue
    FROM {{zone_prefix}}.silver.flights_enriched f
) AS src
ON tgt.flight_key = src.flight_key
WHEN MATCHED THEN UPDATE SET
    tgt.actual_departure  = src.actual_departure,
    tgt.actual_arrival    = src.actual_arrival,
    tgt.delay_minutes     = src.delay_minutes,
    tgt.delay_reason      = src.delay_reason,
    tgt.pax_count         = src.pax_count,
    tgt.fuel_consumed_kg  = src.fuel_consumed_kg,
    tgt.revenue           = src.revenue
WHEN NOT MATCHED THEN INSERT (
    flight_key, route_key, aircraft_key, crew_key, date_key, flight_number,
    scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
    delay_minutes, delay_reason, pax_count, fuel_consumed_kg, revenue
) VALUES (
    src.flight_key, src.route_key, src.aircraft_key, src.crew_key, src.date_key,
    src.flight_number, src.scheduled_departure, src.actual_departure,
    src.scheduled_arrival, src.actual_arrival, src.delay_minutes, src.delay_reason,
    src.pax_count, src.fuel_consumed_kg, src.revenue
);

-- =============================================================================
-- STEP 7: GOLD - KPI OTP with route profitability and delay Pareto
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_otp AS tgt
USING (
    WITH route_monthly AS (
        SELECT
            f.route_key AS route,
            CAST(EXTRACT(YEAR FROM dd.full_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM dd.full_date) AS STRING), 2, '0') AS month,
            COUNT(*) AS total_flights,
            COUNT(*) FILTER (WHERE f.delay_minutes <= 15 AND f.pax_count > 0) AS on_time_count,
            COUNT(*) FILTER (WHERE f.delay_minutes > 15) AS delayed_count,
            COUNT(*) FILTER (WHERE f.pax_count = 0 AND f.revenue = 0) AS cancelled_count,
            CAST(
                COUNT(*) FILTER (WHERE f.delay_minutes <= 15 AND f.pax_count > 0) * 100.0
                / NULLIF(COUNT(*), 0)
            AS DECIMAL(5,2)) AS otp_pct,
            CAST(AVG(CASE WHEN f.delay_minutes > 0 THEN f.delay_minutes END) AS DECIMAL(7,2)) AS avg_delay_min,
            CAST(AVG(CASE WHEN f.pax_count > 0 AND dr.distance_nm > 0
                THEN f.fuel_consumed_kg / (f.pax_count * dr.distance_nm) END) AS DECIMAL(8,4)) AS fuel_efficiency_per_pax_nm,
            CAST(SUM(f.revenue) / NULLIF(SUM(CAST(da.seat_capacity AS DECIMAL(10,2)) * dr.distance_nm), 0)
                AS DECIMAL(8,4)) AS revenue_per_asm,
            CAST(AVG(CASE WHEN da.seat_capacity > 0 AND f.pax_count > 0
                THEN f.pax_count * 100.0 / da.seat_capacity END) AS DECIMAL(5,2)) AS load_factor_pct
        FROM {{zone_prefix}}.gold.fact_flights f
        JOIN {{zone_prefix}}.gold.dim_route dr ON f.route_key = dr.route_key
        JOIN {{zone_prefix}}.gold.dim_aircraft da ON f.aircraft_key = da.aircraft_key
        JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
        GROUP BY f.route_key,
            CAST(EXTRACT(YEAR FROM dd.full_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM dd.full_date) AS STRING), 2, '0')
    )
    SELECT * FROM route_monthly
) AS src
ON tgt.route = src.route AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.total_flights              = src.total_flights,
    tgt.on_time_count              = src.on_time_count,
    tgt.delayed_count              = src.delayed_count,
    tgt.cancelled_count            = src.cancelled_count,
    tgt.otp_pct                    = src.otp_pct,
    tgt.avg_delay_min              = src.avg_delay_min,
    tgt.fuel_efficiency_per_pax_nm = src.fuel_efficiency_per_pax_nm,
    tgt.revenue_per_asm            = src.revenue_per_asm,
    tgt.load_factor_pct            = src.load_factor_pct
WHEN NOT MATCHED THEN INSERT (
    route, month, total_flights, on_time_count, delayed_count, cancelled_count,
    otp_pct, avg_delay_min, fuel_efficiency_per_pax_nm, revenue_per_asm, load_factor_pct
) VALUES (
    src.route, src.month, src.total_flights, src.on_time_count, src.delayed_count,
    src.cancelled_count, src.otp_pct, src.avg_delay_min, src.fuel_efficiency_per_pax_nm,
    src.revenue_per_asm, src.load_factor_pct
);

-- =============================================================================
-- STEP 8: OPTIMIZE and VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.gold.fact_flights;
VACUUM {{zone_prefix}}.gold.fact_flights RETAIN 168 HOURS;
VACUUM {{zone_prefix}}.silver.flights_enriched RETAIN 168 HOURS;
