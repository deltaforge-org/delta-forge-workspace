-- =============================================================================
-- Automotive Telematics Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE telematics_8hr_schedule
    CRON '0 */8 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 2400
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE automotive_telematics_pipeline
    DESCRIPTION 'Fleet telematics pipeline with safety scoring, fuel efficiency, and predictive maintenance'
    SCHEDULE 'telematics_8hr_schedule'
    TAGS 'automotive,telematics,fleet,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich trips with safety scores and maintenance flags
-- =============================================================================
-- Safety score = 100 - 5*harsh_brakes - 3*harsh_accels - 2*(idle_min/duration_min*100)
-- Maintenance due = odometer_km > next_service_km

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
    WHERE v.is_active = true OR t.trip_date <= '2024-01-31'
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

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_vehicle
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_vehicle AS tgt
USING (
    SELECT
        vin AS vehicle_key,
        vin,
        vehicle_type,
        make,
        model,
        year,
        fuel_type,
        odometer_km,
        last_service_date,
        next_service_km,
        fleet_id
    FROM {{zone_prefix}}.bronze.raw_vehicles
) AS src
ON tgt.vehicle_key = src.vehicle_key
WHEN MATCHED THEN UPDATE SET
    tgt.odometer_km       = src.odometer_km,
    tgt.last_service_date = src.last_service_date,
    tgt.next_service_km   = src.next_service_km
WHEN NOT MATCHED THEN INSERT (
    vehicle_key, vin, vehicle_type, make, model, year, fuel_type,
    odometer_km, last_service_date, next_service_km, fleet_id
) VALUES (
    src.vehicle_key, src.vin, src.vehicle_type, src.make, src.model, src.year,
    src.fuel_type, src.odometer_km, src.last_service_date, src.next_service_km, src.fleet_id
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_driver
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_driver AS tgt
USING (
    SELECT
        driver_id AS driver_key,
        driver_id,
        name,
        license_class,
        hire_date,
        training_level,
        safety_certifications
    FROM {{zone_prefix}}.bronze.raw_drivers
) AS src
ON tgt.driver_key = src.driver_key
WHEN MATCHED THEN UPDATE SET
    tgt.training_level        = src.training_level,
    tgt.safety_certifications = src.safety_certifications
WHEN NOT MATCHED THEN INSERT (
    driver_key, driver_id, name, license_class, hire_date, training_level, safety_certifications
) VALUES (
    src.driver_key, src.driver_id, src.name, src.license_class, src.hire_date,
    src.training_level, src.safety_certifications
);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_route
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_route AS tgt
USING (
    SELECT
        route_id AS route_key,
        route_name,
        origin_city,
        dest_city,
        distance_km,
        road_type,
        avg_traffic_index
    FROM {{zone_prefix}}.bronze.raw_routes
) AS src
ON tgt.route_key = src.route_key
WHEN MATCHED THEN UPDATE SET
    tgt.avg_traffic_index = src.avg_traffic_index
WHEN NOT MATCHED THEN INSERT (
    route_key, route_name, origin_city, dest_city, distance_km, road_type, avg_traffic_index
) VALUES (
    src.route_key, src.route_name, src.origin_city, src.dest_city, src.distance_km,
    src.road_type, src.avg_traffic_index
);

-- =============================================================================
-- STEP 5: GOLD - Fact: fact_trips
-- =============================================================================

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
-- STEP 6: GOLD - KPI: kpi_fleet_performance
-- =============================================================================
-- Fleet performance with utilization %, idle %, harsh event rate, maintenance due.
-- Utilization = (vehicles with trips / total fleet vehicles) * 100

MERGE INTO {{zone_prefix}}.gold.kpi_fleet_performance AS tgt
USING (
    WITH monthly_stats AS (
        SELECT
            te.fleet_id,
            te.vehicle_type,
            te.trip_month AS month,
            SUM(te.distance_km) AS total_distance,
            SUM(te.fuel_consumed_l) AS total_fuel,
            AVG(te.fuel_efficiency_km_per_l) AS avg_fuel_efficiency,
            COUNT(*) AS total_trips,
            CAST(AVG(te.safety_score) AS DECIMAL(5,2)) AS avg_safety_score,
            SUM(CASE WHEN te.maintenance_due = true THEN 1 ELSE 0 END) AS maintenance_due_count,
            CAST(AVG(te.idle_pct) AS DECIMAL(5,2)) AS idle_pct,
            CASE
                WHEN SUM(te.distance_km) > 0
                THEN CAST(
                    (SUM(te.harsh_brake_count) + SUM(te.harsh_accel_count)) * 1.0
                    / (SUM(te.distance_km) / 100.0)
                AS DECIMAL(8,4))
                ELSE 0
            END AS harsh_event_rate,
            COUNT(DISTINCT te.vin) AS active_vehicles
        FROM {{zone_prefix}}.silver.trips_enriched te
        GROUP BY te.fleet_id, te.vehicle_type, te.trip_month
    ),
    fleet_size AS (
        SELECT fleet_id, vehicle_type, COUNT(*) AS total_vehicles
        FROM {{zone_prefix}}.bronze.raw_vehicles
        WHERE is_active = true
        GROUP BY fleet_id, vehicle_type
    )
    SELECT
        ms.fleet_id,
        ms.vehicle_type,
        ms.month,
        CAST(ms.total_distance AS DECIMAL(12,2)),
        CAST(ms.total_fuel AS DECIMAL(12,2)),
        CAST(ms.avg_fuel_efficiency AS DECIMAL(8,3)),
        ms.total_trips,
        ms.avg_safety_score,
        ms.maintenance_due_count,
        ms.idle_pct,
        ms.harsh_event_rate,
        CAST(ms.active_vehicles * 100.0 / NULLIF(fs.total_vehicles, 0) AS DECIMAL(5,2)) AS utilization_pct
    FROM monthly_stats ms
    LEFT JOIN fleet_size fs ON ms.fleet_id = fs.fleet_id AND ms.vehicle_type = fs.vehicle_type
) AS src
ON tgt.fleet_id = src.fleet_id AND tgt.vehicle_type = src.vehicle_type AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.total_distance        = src.total_distance,
    tgt.total_fuel            = src.total_fuel,
    tgt.avg_fuel_efficiency   = src.avg_fuel_efficiency,
    tgt.total_trips           = src.total_trips,
    tgt.avg_safety_score      = src.avg_safety_score,
    tgt.maintenance_due_count = src.maintenance_due_count,
    tgt.idle_pct              = src.idle_pct,
    tgt.harsh_event_rate      = src.harsh_event_rate,
    tgt.utilization_pct       = src.utilization_pct
WHEN NOT MATCHED THEN INSERT (
    fleet_id, vehicle_type, month, total_distance, total_fuel, avg_fuel_efficiency,
    total_trips, avg_safety_score, maintenance_due_count, idle_pct, harsh_event_rate,
    utilization_pct
) VALUES (
    src.fleet_id, src.vehicle_type, src.month, src.total_distance, src.total_fuel,
    src.avg_fuel_efficiency, src.total_trips, src.avg_safety_score,
    src.maintenance_due_count, src.idle_pct, src.harsh_event_rate, src.utilization_pct
);

-- =============================================================================
-- OPTIMIZE & DELETE decommissioned vehicle trips from silver
-- =============================================================================

DELETE FROM {{zone_prefix}}.silver.trips_enriched
WHERE vin IN (
    SELECT vin FROM {{zone_prefix}}.bronze.raw_vehicles WHERE is_active = false
)
AND trip_date > '2024-01-31';

OPTIMIZE {{zone_prefix}}.silver.trips_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_trips;
OPTIMIZE {{zone_prefix}}.gold.kpi_fleet_performance;
