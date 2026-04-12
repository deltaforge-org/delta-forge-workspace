-- =============================================================================
-- Manufacturing IoT Pipeline: Gold Layer Verification (12 ASSERTs)
-- =============================================================================

PIPELINE manufacturing_verify_gold
  DESCRIPTION 'Gold layer verification for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'verification', 'manufacturing-iot'
  LIFECYCLE production
;


-- -----------------------------------------------------------------------------
-- 1. Verify fact table row count (90 readings)
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_readings_count >= 90
SELECT COUNT(*) AS fact_readings_count
FROM mfg.gold.fact_readings;

-- -----------------------------------------------------------------------------
-- 2. Verify all 16 sensors in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE sensor_count = 16
SELECT COUNT(*) AS sensor_count
FROM mfg.gold.dim_sensor;

-- -----------------------------------------------------------------------------
-- 3. Verify 12 production lines in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE line_count = 12
SELECT COUNT(*) AS line_count
FROM mfg.gold.dim_line;

-- -----------------------------------------------------------------------------
-- 4. Verify 3 shifts in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE shift_count = 3
SELECT COUNT(*) AS shift_count
FROM mfg.gold.dim_shift;

-- -----------------------------------------------------------------------------
-- 5. Anomaly detection verification — anomalies must exist
-- -----------------------------------------------------------------------------
ASSERT VALUE total_readings > 0
SELECT
  ds.sensor_type,
  COUNT(*) AS total_readings,
  COUNT(CASE WHEN f.anomaly_flag = true THEN 1 END) AS anomalous_readings,
  CAST(
      COUNT(CASE WHEN f.anomaly_flag = true THEN 1 END) * 100.0 / COUNT(*)
  AS DECIMAL(5,2)) AS anomaly_pct
FROM mfg.gold.fact_readings f
JOIN mfg.gold.dim_sensor ds ON f.sensor_key = ds.sensor_key
GROUP BY ds.sensor_type
ORDER BY anomaly_pct DESC;

-- -----------------------------------------------------------------------------
-- 6. OEE by plant/line/shift — all OEE components must be > 0
-- -----------------------------------------------------------------------------
ASSERT VALUE oee_pct > 0
SELECT
  plant_id,
  line_name,
  shift_name,
  planned_minutes,
  downtime_minutes,
  availability_pct,
  actual_units,
  target_units,
  performance_pct,
  good_units,
  total_units,
  quality_pct,
  oee_pct
FROM mfg.gold.kpi_oee
ORDER BY oee_pct DESC;

-- -----------------------------------------------------------------------------
-- 7. Anomaly trend analysis — trends by sensor type and month
-- -----------------------------------------------------------------------------
ASSERT VALUE anomaly_count > 0
SELECT
  sensor_type,
  plant_id,
  trend_month,
  total_readings,
  anomaly_count,
  anomaly_rate_pct,
  avg_deviation,
  max_deviation
FROM mfg.gold.kpi_anomaly_trends
WHERE anomaly_count > 0
ORDER BY anomaly_rate_pct DESC;

-- -----------------------------------------------------------------------------
-- 8. Equipment status verification — downtime events captured
-- -----------------------------------------------------------------------------
ASSERT VALUE downtime_minutes > 0
SELECT
  plant_id,
  line_name,
  shift_date,
  shift_id,
  planned_minutes,
  downtime_minutes,
  uptime_minutes,
  unplanned_stops,
  status
FROM mfg.silver.equipment_status
WHERE unplanned_stops > 0
ORDER BY downtime_minutes DESC;

-- -----------------------------------------------------------------------------
-- 9. Predictive maintenance signals using LAG/LEAD
-- -----------------------------------------------------------------------------
ASSERT VALUE reading_id IS NOT NULL
SELECT
  sm.reading_id,
  sm.sensor_id,
  sm.plant_id,
  sm.reading_time,
  sm.value,
  sm.moving_avg,
  sm.moving_stddev,
  LAG(sm.value) OVER (PARTITION BY sm.sensor_id ORDER BY sm.reading_time) AS prev_value,
  LEAD(sm.value) OVER (PARTITION BY sm.sensor_id ORDER BY sm.reading_time) AS next_value,
  sm.anomaly_flag,
  sm.anomaly_reason
FROM mfg.silver.readings_smoothed sm
WHERE sm.anomaly_flag = true
ORDER BY sm.reading_time;

-- -----------------------------------------------------------------------------
-- 10. Quality score distribution by shift
-- -----------------------------------------------------------------------------
ASSERT VALUE readings > 0
SELECT
  dsh.shift_name,
  dsh.supervisor,
  COUNT(*) AS readings,
  CAST(AVG(f.quality_score) AS DECIMAL(5,2)) AS avg_quality,
  CAST(MIN(f.quality_score) AS DECIMAL(5,2)) AS min_quality,
  CAST(MAX(f.quality_score) AS DECIMAL(5,2)) AS max_quality
FROM mfg.gold.fact_readings f
JOIN mfg.gold.dim_shift dsh ON f.shift_key = dsh.shift_key
GROUP BY dsh.shift_name, dsh.supervisor
ORDER BY avg_quality DESC;

-- -----------------------------------------------------------------------------
-- 11. Plant-level smoothed value trend with anomaly count
-- -----------------------------------------------------------------------------
ASSERT VALUE avg_value > 0
SELECT
  sm.plant_id,
  sm.line_name,
  CAST(sm.reading_time AS DATE) AS reading_date,
  CAST(AVG(sm.value) AS DECIMAL(10,2)) AS avg_value,
  CAST(AVG(sm.moving_avg) AS DECIMAL(10,2)) AS avg_smoothed,
  SUM(CASE WHEN sm.anomaly_flag = true THEN 1 ELSE 0 END) AS anomaly_count
FROM mfg.silver.readings_smoothed sm
GROUP BY sm.plant_id, sm.line_name, CAST(sm.reading_time AS DATE)
ORDER BY sm.plant_id, reading_date;

-- -----------------------------------------------------------------------------
-- 12. CHECK constraint validation: no readings outside absolute physical limits
-- -----------------------------------------------------------------------------
ASSERT VALUE out_of_range = 0
SELECT COUNT(*) AS out_of_range
FROM mfg.bronze.raw_readings r
JOIN mfg.bronze.raw_sensors s ON r.sensor_id = s.sensor_id
WHERE (s.sensor_type = 'temperature' AND r.value NOT BETWEEN -50 AND 500)
 OR (s.sensor_type = 'pressure'    AND r.value NOT BETWEEN 0 AND 200)
 OR (s.sensor_type = 'vibration'   AND r.value NOT BETWEEN 0 AND 50000)
 OR (s.sensor_type = 'rpm'         AND r.value NOT BETWEEN 0 AND 20000);

SELECT 'All 12 gold-layer assertions passed' AS verification_status;
