-- =============================================================================
-- Manufacturing IoT Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact table row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_readings_count
FROM {{zone_prefix}}.gold.fact_sensor_readings;

-- -----------------------------------------------------------------------------
-- 2. Verify all 16 sensors in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_readings_count >= 80
SELECT COUNT(*) AS sensor_count
FROM {{zone_prefix}}.gold.dim_sensor;

-- -----------------------------------------------------------------------------
-- 3. Verify 8 production lines in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS line_count
FROM {{zone_prefix}}.gold.dim_production_line;

-- -----------------------------------------------------------------------------
-- 4. Verify 3 shifts in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS shift_count
FROM {{zone_prefix}}.gold.dim_shift;

-- -----------------------------------------------------------------------------
-- 5. Anomaly detection verification
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    ds.sensor_type,
    COUNT(*) AS total_readings,
    COUNT(CASE WHEN f.anomaly_flag = true THEN 1 END) AS anomalous_readings,
    CAST(
        COUNT(CASE WHEN f.anomaly_flag = true THEN 1 END) * 100.0 / COUNT(*)
    AS DECIMAL(5,2)) AS anomaly_pct
FROM {{zone_prefix}}.gold.fact_sensor_readings f
JOIN {{zone_prefix}}.gold.dim_sensor ds ON f.sensor_key = ds.sensor_key
GROUP BY ds.sensor_type
ORDER BY anomaly_pct DESC;

-- -----------------------------------------------------------------------------
-- 6. OEE by plant (Availability x Performance x Quality)
-- -----------------------------------------------------------------------------
ASSERT VALUE total_readings > 0
SELECT
    plant_id,
    line_name,
    shift_name,
    availability_pct,
    performance_pct,
    quality_pct,
    oee_pct,
    total_units,
    defect_units,
    downtime_minutes
FROM {{zone_prefix}}.gold.kpi_oee
ORDER BY oee_pct DESC;

-- -----------------------------------------------------------------------------
-- 7. Predictive maintenance signals using LAG/LEAD
-- -----------------------------------------------------------------------------
ASSERT VALUE oee_pct > 0
SELECT
    v.reading_id,
    v.sensor_id,
    v.plant_id,
    v.reading_time,
    v.temperature_c,
    v.temp_moving_avg,
    LAG(v.temperature_c) OVER (PARTITION BY v.sensor_id ORDER BY v.reading_time) AS prev_temp,
    LEAD(v.temperature_c) OVER (PARTITION BY v.sensor_id ORDER BY v.reading_time) AS next_temp,
    v.anomaly_flag
FROM {{zone_prefix}}.silver.readings_validated v
WHERE v.anomaly_flag = true
ORDER BY v.reading_time;

-- -----------------------------------------------------------------------------
-- 8. Quality score distribution by shift
-- -----------------------------------------------------------------------------
ASSERT VALUE reading_id IS NOT NULL
SELECT
    dsh.shift_name,
    dsh.supervisor,
    COUNT(*) AS readings,
    CAST(AVG(f.quality_score) AS DECIMAL(5,2)) AS avg_quality,
    CAST(MIN(f.quality_score) AS DECIMAL(5,2)) AS min_quality,
    CAST(MAX(f.quality_score) AS DECIMAL(5,2)) AS max_quality
FROM {{zone_prefix}}.gold.fact_sensor_readings f
JOIN {{zone_prefix}}.gold.dim_shift dsh ON f.shift_key = dsh.shift_key
GROUP BY dsh.shift_name, dsh.supervisor
ORDER BY avg_quality DESC;

-- -----------------------------------------------------------------------------
-- 9. Plant-level temperature trend with moving averages
-- -----------------------------------------------------------------------------
ASSERT VALUE readings > 0
SELECT
    v.plant_id,
    v.line_name,
    CAST(v.reading_time AS DATE) AS reading_date,
    CAST(AVG(v.temperature_c) AS DECIMAL(8,2)) AS avg_temp,
    CAST(AVG(v.temp_moving_avg) AS DECIMAL(8,2)) AS avg_smoothed_temp,
    SUM(CASE WHEN v.anomaly_flag = true THEN 1 ELSE 0 END) AS anomaly_count
FROM {{zone_prefix}}.silver.readings_validated v
GROUP BY v.plant_id, v.line_name, CAST(v.reading_time AS DATE)
ORDER BY v.plant_id, reading_date;

-- -----------------------------------------------------------------------------
-- 10. CHECK constraint validation: no readings outside absolute limits
-- -----------------------------------------------------------------------------
ASSERT VALUE avg_temp > 0
SELECT COUNT(*) AS out_of_range
FROM {{zone_prefix}}.bronze.raw_sensor_readings
WHERE temperature_c NOT BETWEEN -50 AND 500
   OR pressure_bar NOT BETWEEN 0 AND 100;

ASSERT VALUE out_of_range = 0
SELECT 'out_of_range check passed' AS out_of_range_status;

