-- =============================================================================
-- Aviation Flight Operations Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing with INCREMENTAL_FILTER macro on
-- composite key (flight_number+departure_date).

-- Show current watermark
SELECT MAX(departure_date) AS max_departure_date, COUNT(*) AS total_flights
FROM {{zone_prefix}}.silver.flights_enriched;

-- Current row counts
SELECT 'silver.flights_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.flights_enriched
UNION ALL
SELECT 'gold.fact_flights', COUNT(*)
FROM {{zone_prefix}}.gold.fact_flights;

-- =============================================================================
-- Insert 8 new flight operations (March 2024 incremental batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_flight_ops VALUES
('AA300', '2024-03-01', 'RT-001', 'N101AA', 'CRW-001', '2024-03-01T08:00:00', '2024-03-01T08:00:00', '2024-03-01T13:30:00', '2024-03-01T13:25:00',  0, NULL,         185, 8150.00,  196000.00, 'arrived',   '2024-03-01T13:30:00'),
('AA301', '2024-03-02', 'RT-002', 'N303CC', 'CRW-002', '2024-03-02T09:00:00', '2024-03-02T09:15:00', '2024-03-02T12:15:00', '2024-03-02T12:35:00', 15, 'ATC',        168, 4250.00,   94000.00, 'arrived',   '2024-03-02T12:40:00'),
('AA302', '2024-03-03', 'RT-006', 'N202BB', 'CRW-001', '2024-03-03T18:00:00', '2024-03-03T18:00:00', '2024-03-04T06:30:00', '2024-03-04T06:22:00',  0, NULL,         285, 27600.00,  455000.00, 'arrived',   '2024-03-04T06:25:00'),
('AA303', '2024-03-04', 'RT-003', 'N505EE', 'CRW-003', '2024-03-04T11:00:00', '2024-03-04T11:40:00', '2024-03-04T13:15:00', '2024-03-04T14:00:00', 40, 'weather',    195, 3200.00,   72000.00, 'arrived',   '2024-03-04T14:05:00'),
('AA304', '2024-03-05', 'RT-004', 'N303CC', 'CRW-005', '2024-03-05T14:00:00', '2024-03-05T14:00:00', '2024-03-05T16:00:00', '2024-03-05T15:50:00',  0, NULL,         176, 2320.00,   61000.00, 'arrived',   '2024-03-05T15:55:00'),
('AA305', '2024-03-06', 'RT-009', 'N505EE', 'CRW-003', '2024-03-06T10:00:00', '2024-03-06T10:00:00', '2024-03-06T12:00:00', '2024-03-06T11:55:00',  0, NULL,         205, 2060.00,   51000.00, 'arrived',   '2024-03-06T12:00:00'),
('AA306', '2024-03-07', 'RT-010', 'N606FF', 'CRW-003', '2024-03-07T06:00:00', '2024-03-07T06:00:00', '2024-03-07T07:30:00', '2024-03-07T07:32:00',  0, NULL,          74, 1410.00,   35500.00, 'arrived',   '2024-03-07T07:35:00'),
('AA307', '2024-03-08', 'RT-007', 'N404DD', 'CRW-004', '2024-03-08T11:00:00', '2024-03-08T11:10:00', '2024-03-08T22:00:00', '2024-03-08T22:15:00', 10, 'ATC',        390, 57000.00,  665000.00, 'arrived',   '2024-03-08T22:20:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE to silver using INCREMENTAL_FILTER on composite key
-- =============================================================================

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
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.flights_enriched, (flight_number, departure_date), departure_date, 3)}}
) AS src
ON tgt.flight_number = src.flight_number AND tgt.departure_date = src.departure_date
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.actual_departure  = src.actual_departure,
    tgt.actual_arrival    = src.actual_arrival,
    tgt.delay_minutes     = src.delay_minutes,
    tgt.delay_reason      = src.delay_reason,
    tgt.delay_category    = src.delay_category,
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
    src.is_on_time, src.is_anomaly, src.ingested_at
);

-- =============================================================================
-- Incremental gold fact refresh
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_flights AS tgt
USING (
    SELECT
        f.flight_number || '-' || CAST(f.departure_date AS STRING) AS flight_key,
        f.route_id AS route_key, f.registration AS aircraft_key, f.crew_id AS crew_key,
        CAST(f.departure_date AS STRING) AS date_key,
        f.flight_number, f.scheduled_departure, f.actual_departure,
        f.scheduled_arrival, f.actual_arrival, f.delay_minutes, f.delay_reason,
        f.pax_count, f.fuel_consumed_kg, f.revenue
    FROM {{zone_prefix}}.silver.flights_enriched f
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_flights, flight_key, departure_date, 3)}}
) AS src
ON tgt.flight_key = src.flight_key
WHEN MATCHED THEN UPDATE SET
    tgt.actual_departure = src.actual_departure,
    tgt.actual_arrival   = src.actual_arrival,
    tgt.delay_minutes    = src.delay_minutes,
    tgt.pax_count        = src.pax_count,
    tgt.revenue          = src.revenue
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

-- Incremental dim_date for March dates
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
    WHERE departure_date >= '2024-03-01'
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day_of_week, month, quarter, year, is_peak_season
) VALUES (
    src.date_key, src.full_date, src.day_of_week, src.month, src.quarter, src.year, src.is_peak_season
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.flights_enriched;
ASSERT VALUE silver_total = 70

SELECT COUNT(*) AS gold_fact_total FROM {{zone_prefix}}.gold.fact_flights;
-- Verify March flights exist
ASSERT VALUE gold_fact_total = 70
SELECT COUNT(*) AS march_flights
FROM {{zone_prefix}}.gold.fact_flights
WHERE date_key >= '2024-03-01';

-- Time travel: compare current vs previous version
ASSERT VALUE march_flights = 8
SELECT COUNT(*) AS prev_version_count
FROM {{zone_prefix}}.silver.flights_enriched VERSION AS OF 1;
