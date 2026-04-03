-- =============================================================================
-- Automotive Telematics Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- New trip telemetry from April 2024 batch is added and processed.

-- Show current state before incremental load
SELECT 'silver.trips_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.trips_enriched
UNION ALL
SELECT 'gold.fact_trips', COUNT(*)
FROM {{zone_prefix}}.gold.fact_trips;

-- =============================================================================
-- Insert 8 new bronze trip records (April 2024 batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_trips VALUES
('T071', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-04-05', '2024-04', 340.00, 248.00, 82.30, 103.00,  95.00, 0, 0, 14.00, 90.80, 105.20, '2024-04-06T00:00:00'),
('T072', '4T1B11HK5JU567890', 'DRV-004', 'RT-003', '2024-04-03', '2024-04', 290.00, 205.00, 84.90, 108.00,  83.00, 1, 0, 12.00, 94.20, 108.00, '2024-04-04T00:00:00'),
('T073', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-04-02', '2024-04', 614.00, 422.00, 87.20, 107.00, 173.00, 0, 0, 24.00, 91.50, 110.00, '2024-04-03T00:00:00'),
('T074', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-04-08', '2024-04', 148.00, 178.00, 49.90,  75.00,  18.50, 1, 0, 38.00, 87.80,  42.50, '2024-04-09T00:00:00'),
('T075', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-04-10', '2024-04', 150.00, 160.00, 56.30,  84.00,   7.80, 3, 2, 28.00, 77.50,  33.20, '2024-04-11T00:00:00'),
('T076', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-04-07', '2024-04', 288.00, 197.00, 87.70,  98.00,   0.00, 0, 1, 6.00,  42.00,  35.00, '2024-04-08T00:00:00'),
('T077', '6FPPX4EP2N2345678', 'DRV-006', 'RT-004', '2024-04-12', '2024-04', 380.00, 300.00, 76.00,  95.00,  50.50, 0, 0, 24.00, 86.50,  37.00, '2024-04-13T00:00:00'),
('T078', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-04-09', '2024-04', 279.00, 228.00, 73.40,  89.00,  36.00, 1, 1, 28.00, 83.00,  35.50, '2024-04-10T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only process new trips using INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.trips_enriched AS tgt
USING (
    SELECT
        t.trip_id,
        t.vin,
        t.driver_id,
        t.route_id,
        v.fleet_id,
        v.vehicle_type,
        t.trip_date,
        t.trip_month,
        t.distance_km,
        t.duration_min,
        t.avg_speed_kmh,
        t.max_speed_kmh,
        t.fuel_consumed_l,
        CASE
            WHEN t.fuel_consumed_l > 0 THEN CAST(t.distance_km / t.fuel_consumed_l AS DECIMAL(8,3))
            ELSE 0.000
        END AS fuel_efficiency_km_per_l,
        t.harsh_brake_count,
        t.harsh_accel_count,
        t.idle_time_min,
        CAST(t.idle_time_min / NULLIF(t.duration_min, 0) * 100.0 AS DECIMAL(5,2)) AS idle_pct,
        t.engine_temp_max_c,
        t.tire_pressure_avg_psi,
        CAST(GREATEST(
            0,
            100.0
            - 5.0 * t.harsh_brake_count
            - 3.0 * t.harsh_accel_count
            - 2.0 * (t.idle_time_min / NULLIF(t.duration_min, 0) * 100.0)
        ) AS DECIMAL(5,2)) AS safety_score,
        CASE
            WHEN v.odometer_km > v.next_service_km THEN true
            ELSE false
        END AS maintenance_due,
        CURRENT_TIMESTAMP AS enriched_at
    FROM {{zone_prefix}}.bronze.raw_trips t
    JOIN {{zone_prefix}}.bronze.raw_vehicles v ON t.vin = v.vin
    WHERE v.is_active = true
      AND {{INCREMENTAL_FILTER({{zone_prefix}}.silver.trips_enriched, trip_id, trip_date, 3)}}
) AS src
ON tgt.trip_id = src.trip_id
WHEN MATCHED THEN UPDATE SET
    tgt.fuel_efficiency_km_per_l = src.fuel_efficiency_km_per_l,
    tgt.idle_pct                 = src.idle_pct,
    tgt.safety_score             = src.safety_score,
    tgt.maintenance_due          = src.maintenance_due,
    tgt.enriched_at              = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    trip_id, vin, driver_id, route_id, fleet_id, vehicle_type, trip_date, trip_month,
    distance_km, duration_min, avg_speed_kmh, max_speed_kmh, fuel_consumed_l,
    fuel_efficiency_km_per_l, harsh_brake_count, harsh_accel_count, idle_time_min,
    idle_pct, engine_temp_max_c, tire_pressure_avg_psi, safety_score,
    maintenance_due, enriched_at
) VALUES (
    src.trip_id, src.vin, src.driver_id, src.route_id, src.fleet_id, src.vehicle_type,
    src.trip_date, src.trip_month, src.distance_km, src.duration_min, src.avg_speed_kmh,
    src.max_speed_kmh, src.fuel_consumed_l, src.fuel_efficiency_km_per_l,
    src.harsh_brake_count, src.harsh_accel_count, src.idle_time_min, src.idle_pct,
    src.engine_temp_max_c, src.tire_pressure_avg_psi, src.safety_score,
    src.maintenance_due, src.enriched_at
);

-- Refresh gold fact incrementally
MERGE INTO {{zone_prefix}}.gold.fact_trips AS tgt
USING (
    SELECT
        trip_id         AS trip_key,
        vin             AS vehicle_key,
        driver_id       AS driver_key,
        route_id        AS route_key,
        trip_date,
        distance_km,
        duration_min,
        avg_speed_kmh,
        max_speed_kmh,
        fuel_consumed_l,
        fuel_efficiency_km_per_l,
        harsh_brake_count,
        harsh_accel_count,
        idle_time_min,
        engine_temp_max_c,
        tire_pressure_avg_psi,
        safety_score
    FROM {{zone_prefix}}.silver.trips_enriched
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_trips, trip_key, trip_date, 3)}}
) AS src
ON tgt.trip_key = src.trip_key
WHEN MATCHED THEN UPDATE SET
    tgt.fuel_efficiency_km_per_l = src.fuel_efficiency_km_per_l,
    tgt.safety_score             = src.safety_score
WHEN NOT MATCHED THEN INSERT (
    trip_key, vehicle_key, driver_key, route_key, trip_date, distance_km, duration_min,
    avg_speed_kmh, max_speed_kmh, fuel_consumed_l, fuel_efficiency_km_per_l,
    harsh_brake_count, harsh_accel_count, idle_time_min, engine_temp_max_c,
    tire_pressure_avg_psi, safety_score
) VALUES (
    src.trip_key, src.vehicle_key, src.driver_key, src.route_key, src.trip_date,
    src.distance_km, src.duration_min, src.avg_speed_kmh, src.max_speed_kmh,
    src.fuel_consumed_l, src.fuel_efficiency_km_per_l, src.harsh_brake_count,
    src.harsh_accel_count, src.idle_time_min, src.engine_temp_max_c,
    src.tire_pressure_avg_psi, src.safety_score
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.trips_enriched;
ASSERT VALUE silver_total >= 70

SELECT COUNT(*) AS gold_total FROM {{zone_prefix}}.gold.fact_trips;
-- Verify new April trips appeared
ASSERT VALUE gold_total >= 70
SELECT COUNT(*) AS april_trips
FROM {{zone_prefix}}.silver.trips_enriched
WHERE trip_month = '2024-04';

ASSERT VALUE april_trips = 8
SELECT 'april_trips check passed' AS april_trips_status;

