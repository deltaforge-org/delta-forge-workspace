-- =============================================================================
-- Automotive Telematics Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_trips row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_trips_count
FROM {{zone_prefix}}.gold.fact_trips;

-- -----------------------------------------------------------------------------
-- 2. Verify all 10 vehicles loaded into dim_vehicle
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_trips_count >= 65
SELECT COUNT(*) AS vehicle_count
FROM {{zone_prefix}}.gold.dim_vehicle;

-- -----------------------------------------------------------------------------
-- 3. Verify all 8 drivers loaded into dim_driver
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS driver_count
FROM {{zone_prefix}}.gold.dim_driver;

-- -----------------------------------------------------------------------------
-- 4. Fleet fuel efficiency by vehicle type
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    dv.vehicle_type,
    dv.fuel_type,
    COUNT(*) AS trips,
    CAST(AVG(ft.fuel_efficiency_km_per_l) AS DECIMAL(8,3)) AS avg_fuel_efficiency,
    SUM(ft.distance_km) AS total_distance_km,
    SUM(ft.fuel_consumed_l) AS total_fuel_l
FROM {{zone_prefix}}.gold.fact_trips ft
JOIN {{zone_prefix}}.gold.dim_vehicle dv ON ft.vehicle_key = dv.vehicle_key
WHERE dv.fuel_type != 'Electric'
GROUP BY dv.vehicle_type, dv.fuel_type
ORDER BY avg_fuel_efficiency DESC;

-- -----------------------------------------------------------------------------
-- 5. Driver safety rankings with composite scoring
-- -----------------------------------------------------------------------------
ASSERT VALUE trips > 0
SELECT
    dd.name,
    dd.training_level,
    dd.license_class,
    COUNT(*) AS total_trips,
    CAST(AVG(ft.safety_score) AS DECIMAL(5,2)) AS avg_safety_score,
    SUM(ft.harsh_brake_count) AS total_harsh_brakes,
    SUM(ft.harsh_accel_count) AS total_harsh_accels,
    RANK() OVER (ORDER BY AVG(ft.safety_score) DESC) AS safety_rank
FROM {{zone_prefix}}.gold.fact_trips ft
JOIN {{zone_prefix}}.gold.dim_driver dd ON ft.driver_key = dd.driver_key
GROUP BY dd.name, dd.training_level, dd.license_class
ORDER BY safety_rank;

-- -----------------------------------------------------------------------------
-- 6. Route efficiency comparison
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    dr.route_name,
    dr.origin_city,
    dr.dest_city,
    dr.road_type,
    dr.avg_traffic_index,
    COUNT(*) AS trip_count,
    CAST(AVG(ft.avg_speed_kmh) AS DECIMAL(6,2)) AS avg_speed,
    CAST(AVG(ft.fuel_efficiency_km_per_l) AS DECIMAL(8,3)) AS avg_fuel_eff,
    CAST(AVG(ft.safety_score) AS DECIMAL(5,2)) AS avg_safety
FROM {{zone_prefix}}.gold.fact_trips ft
JOIN {{zone_prefix}}.gold.dim_route dr ON ft.route_key = dr.route_key
GROUP BY dr.route_name, dr.origin_city, dr.dest_city, dr.road_type, dr.avg_traffic_index
ORDER BY trip_count DESC;

-- -----------------------------------------------------------------------------
-- 7. Predictive maintenance: odometer trend using LAG window function
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT
    dv.make,
    dv.model,
    dv.vin,
    ft.trip_date,
    ft.distance_km,
    SUM(ft.distance_km) OVER (PARTITION BY ft.vehicle_key ORDER BY ft.trip_date) AS cumulative_distance,
    dv.odometer_km + SUM(ft.distance_km) OVER (PARTITION BY ft.vehicle_key ORDER BY ft.trip_date) AS projected_odometer,
    dv.next_service_km,
    CASE
        WHEN dv.odometer_km + SUM(ft.distance_km) OVER (PARTITION BY ft.vehicle_key ORDER BY ft.trip_date) > dv.next_service_km
        THEN 'OVERDUE'
        WHEN dv.odometer_km + SUM(ft.distance_km) OVER (PARTITION BY ft.vehicle_key ORDER BY ft.trip_date) > dv.next_service_km - 5000
        THEN 'DUE SOON'
        ELSE 'OK'
    END AS maintenance_status
FROM {{zone_prefix}}.gold.fact_trips ft
JOIN {{zone_prefix}}.gold.dim_vehicle dv ON ft.vehicle_key = dv.vehicle_key
WHERE dv.vehicle_type = 'Truck'
ORDER BY dv.vin, ft.trip_date;

-- -----------------------------------------------------------------------------
-- 8. Monthly safety trend with moving average
-- -----------------------------------------------------------------------------
ASSERT VALUE distance_km > 0
SELECT
    trip_month,
    COUNT(*) AS trips,
    CAST(AVG(safety_score) AS DECIMAL(5,2)) AS avg_safety,
    CAST(AVG(AVG(safety_score)) OVER (ORDER BY trip_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS DECIMAL(5,2)) AS moving_avg_safety,
    SUM(harsh_brake_count + harsh_accel_count) AS total_harsh_events
FROM (
    SELECT
        ft.trip_date,
        DATE_TRUNC('month', ft.trip_date) AS trip_month,
        ft.safety_score,
        ft.harsh_brake_count,
        ft.harsh_accel_count
    FROM {{zone_prefix}}.gold.fact_trips ft
) sub
GROUP BY trip_month
ORDER BY trip_month;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT VALUE trips > 0
SELECT COUNT(*) AS orphaned_vehicles
FROM {{zone_prefix}}.gold.fact_trips ft
LEFT JOIN {{zone_prefix}}.gold.dim_vehicle dv ON ft.vehicle_key = dv.vehicle_key
WHERE dv.vehicle_key IS NULL;

ASSERT VALUE orphaned_vehicles = 0

SELECT COUNT(*) AS orphaned_drivers
FROM {{zone_prefix}}.gold.fact_trips ft
LEFT JOIN {{zone_prefix}}.gold.dim_driver dd ON ft.driver_key = dd.driver_key
WHERE dd.driver_key IS NULL;

ASSERT VALUE orphaned_drivers = 0

SELECT COUNT(*) AS orphaned_routes
FROM {{zone_prefix}}.gold.fact_trips ft
LEFT JOIN {{zone_prefix}}.gold.dim_route dr ON ft.route_key = dr.route_key
WHERE dr.route_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. KPI fleet performance - verify fleet utilization and harsh event rates
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_routes = 0
SELECT
    fleet_id,
    vehicle_type,
    month,
    total_trips,
    avg_safety_score,
    harsh_event_rate,
    utilization_pct,
    maintenance_due_count
FROM {{zone_prefix}}.gold.kpi_fleet_performance
ORDER BY fleet_id, month, vehicle_type;

ASSERT VALUE total_trips > 0
SELECT 'total_trips check passed' AS total_trips_status;

