-- =============================================================================
-- Aviation Flight Operations Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_flights row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_flights_count
FROM {{zone_prefix}}.gold.fact_flights;

-- -----------------------------------------------------------------------------
-- 2. Verify all 10 routes in dim_route
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_flights_count >= 62
SELECT COUNT(*) AS route_count
FROM {{zone_prefix}}.gold.dim_route;

-- -----------------------------------------------------------------------------
-- 3. Verify all 6 aircraft in dim_aircraft
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS aircraft_count
FROM {{zone_prefix}}.gold.dim_aircraft;

-- -----------------------------------------------------------------------------
-- 4. OTP by route with star schema join
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT
    dr.origin_iata || '-' || dr.dest_iata AS route_pair,
    dr.domestic_flag,
    COUNT(*) AS flights,
    COUNT(*) FILTER (WHERE f.delay_minutes <= 15 AND f.pax_count > 0) AS on_time,
    CAST(
        COUNT(*) FILTER (WHERE f.delay_minutes <= 15 AND f.pax_count > 0) * 100.0
        / NULLIF(COUNT(*), 0)
    AS DECIMAL(5,2)) AS otp_pct,
    CAST(AVG(CASE WHEN f.delay_minutes > 0 THEN f.delay_minutes END) AS DECIMAL(7,2)) AS avg_delay_min
FROM {{zone_prefix}}.gold.fact_flights f
JOIN {{zone_prefix}}.gold.dim_route dr ON f.route_key = dr.route_key
GROUP BY dr.origin_iata, dr.dest_iata, dr.domestic_flag
ORDER BY otp_pct DESC;

-- -----------------------------------------------------------------------------
-- 5. Delay cause Pareto analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 10
SELECT
    COALESCE(f.delay_reason, 'none') AS delay_cause,
    COUNT(*) AS occurrences,
    SUM(f.delay_minutes) AS total_delay_min,
    CAST(SUM(f.delay_minutes) * 100.0 / NULLIF(SUM(SUM(f.delay_minutes)) OVER (), 0) AS DECIMAL(5,2)) AS pct_of_total,
    CAST(SUM(SUM(f.delay_minutes)) OVER (ORDER BY SUM(f.delay_minutes) DESC) * 100.0
        / NULLIF(SUM(SUM(f.delay_minutes)) OVER (), 0) AS DECIMAL(5,2)) AS cumulative_pct
FROM {{zone_prefix}}.gold.fact_flights f
WHERE f.delay_minutes > 0
GROUP BY f.delay_reason
ORDER BY total_delay_min DESC;

-- -----------------------------------------------------------------------------
-- 6. Aircraft utilization and fuel efficiency
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 3
SELECT
    da.aircraft_type,
    da.registration,
    COUNT(*) AS flights_operated,
    CAST(SUM(f.fuel_consumed_kg) AS DECIMAL(12,2)) AS total_fuel_kg,
    CAST(AVG(f.fuel_consumed_kg / NULLIF(f.pax_count * dr.distance_nm, 0)) AS DECIMAL(8,4)) AS avg_fuel_per_pax_nm,
    CAST(AVG(f.pax_count * 100.0 / NULLIF(da.seat_capacity, 0)) AS DECIMAL(5,2)) AS avg_load_factor
FROM {{zone_prefix}}.gold.fact_flights f
JOIN {{zone_prefix}}.gold.dim_aircraft da ON f.aircraft_key = da.aircraft_key
JOIN {{zone_prefix}}.gold.dim_route dr ON f.route_key = dr.route_key
WHERE f.pax_count > 0
GROUP BY da.aircraft_type, da.registration
ORDER BY avg_fuel_per_pax_nm ASC;

-- -----------------------------------------------------------------------------
-- 7. Crew utilization analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT
    dc.crew_id,
    dc.crew_base,
    dc.duty_hours_month,
    COUNT(*) AS flights_operated,
    SUM(f.delay_minutes) AS total_delay_min,
    CAST(SUM(f.revenue) AS DECIMAL(14,2)) AS total_revenue_generated,
    DENSE_RANK() OVER (ORDER BY SUM(f.revenue) DESC) AS revenue_rank
FROM {{zone_prefix}}.gold.fact_flights f
JOIN {{zone_prefix}}.gold.dim_crew dc ON f.crew_key = dc.crew_key
GROUP BY dc.crew_id, dc.crew_base, dc.duty_hours_month
ORDER BY revenue_rank;

-- -----------------------------------------------------------------------------
-- 8. Route profitability (Revenue per ASM)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    dr.origin_iata || '-' || dr.dest_iata AS route_pair,
    dr.distance_nm,
    COUNT(*) AS flights,
    CAST(SUM(f.revenue) AS DECIMAL(14,2)) AS total_revenue,
    CAST(SUM(f.revenue) / NULLIF(SUM(CAST(da.seat_capacity AS DECIMAL(10,2)) * dr.distance_nm), 0) AS DECIMAL(8,4)) AS revenue_per_asm,
    DENSE_RANK() OVER (ORDER BY SUM(f.revenue) / NULLIF(SUM(CAST(da.seat_capacity AS DECIMAL(10,2)) * dr.distance_nm), 0) DESC) AS profitability_rank
FROM {{zone_prefix}}.gold.fact_flights f
JOIN {{zone_prefix}}.gold.dim_route dr ON f.route_key = dr.route_key
JOIN {{zone_prefix}}.gold.dim_aircraft da ON f.aircraft_key = da.aircraft_key
WHERE f.pax_count > 0
GROUP BY dr.origin_iata, dr.dest_iata, dr.distance_nm
ORDER BY profitability_rank;

-- -----------------------------------------------------------------------------
-- 9. Cancellation analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 8
SELECT
    dr.origin_iata || '-' || dr.dest_iata AS route_pair,
    f.delay_reason AS cancellation_reason,
    dd.month,
    COUNT(*) AS cancelled_flights
FROM {{zone_prefix}}.gold.fact_flights f
JOIN {{zone_prefix}}.gold.dim_route dr ON f.route_key = dr.route_key
JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
WHERE f.pax_count = 0 AND f.revenue = 0
GROUP BY dr.origin_iata, dr.dest_iata, f.delay_reason, dd.month
ORDER BY cancelled_flights DESC;

-- -----------------------------------------------------------------------------
-- 10. KPI OTP dashboard verification
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 2
SELECT
    route,
    month,
    total_flights,
    otp_pct,
    avg_delay_min,
    revenue_per_asm,
    load_factor_pct
FROM {{zone_prefix}}.gold.kpi_otp
ORDER BY route, month;

-- -----------------------------------------------------------------------------
-- Verification Summary
-- -----------------------------------------------------------------------------
ASSERT VALUE otp_pct >= 0
SELECT
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_flights) AS fact_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_route) AS dim_route_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_aircraft) AS dim_aircraft_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_crew) AS dim_crew_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_date) AS dim_date_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.kpi_otp) AS kpi_rows;
