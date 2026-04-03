-- =============================================================================
-- Manufacturing IoT Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE manufacturing_2hr_schedule
    CRON '0 */2 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE manufacturing_iot_pipeline
    DESCRIPTION 'IoT sensor pipeline with anomaly detection, moving averages, and OEE calculation'
    SCHEDULE 'manufacturing_2hr_schedule'
    TAGS 'manufacturing,iot,oee,anomaly-detection'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP: validate_readings =====================

STEP validate_readings
  TIMEOUT '5m'
AS
  -- Validate readings, compute moving averages, detect anomalies
  -- 5-reading moving average for smoothing.
  -- Anomaly = reading outside sensor threshold (from raw_sensors lookup).
  -- Quality score = 100 - (defect_units / units_produced * 100).
  MERGE INTO {{zone_prefix}}.silver.readings_validated AS tgt
  USING (
      WITH readings_with_ma AS (
          SELECT
              r.reading_id,
              r.sensor_id,
              s.sensor_type,
              r.plant_id,
              r.line_name,
              r.reading_time,
              CASE
                  WHEN EXTRACT(HOUR FROM r.reading_time) >= 6  AND EXTRACT(HOUR FROM r.reading_time) < 14 THEN 'SHIFT-AM'
                  WHEN EXTRACT(HOUR FROM r.reading_time) >= 14 AND EXTRACT(HOUR FROM r.reading_time) < 22 THEN 'SHIFT-PM'
                  ELSE 'SHIFT-NIGHT'
              END AS shift_id,
              r.temperature_c,
              r.pressure_bar,
              r.vibration_hz,
              r.rpm,
              -- 5-reading moving averages per sensor
              CAST(AVG(r.temperature_c) OVER (
                  PARTITION BY r.sensor_id ORDER BY r.reading_time
                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS DECIMAL(8,2)) AS temp_moving_avg,
              CAST(AVG(r.pressure_bar) OVER (
                  PARTITION BY r.sensor_id ORDER BY r.reading_time
                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS DECIMAL(8,2)) AS press_moving_avg,
              CAST(AVG(r.vibration_hz) OVER (
                  PARTITION BY r.sensor_id ORDER BY r.reading_time
                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS DECIMAL(8,2)) AS vib_moving_avg,
              CAST(AVG(r.rpm) OVER (
                  PARTITION BY r.sensor_id ORDER BY r.reading_time
                  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS DECIMAL(8,2)) AS rpm_moving_avg,
              -- Quality score
              CASE
                  WHEN r.units_produced > 0
                  THEN CAST((1.0 - (r.defect_units * 1.0 / r.units_produced)) * 100 AS DECIMAL(5,2))
                  ELSE 100.00
              END AS quality_score,
              -- Anomaly detection: reading outside sensor thresholds
              CASE
                  WHEN s.sensor_type = 'temperature' AND (r.temperature_c < s.threshold_min OR r.temperature_c > s.threshold_max) THEN true
                  WHEN s.sensor_type = 'pressure'    AND (r.pressure_bar  < s.threshold_min OR r.pressure_bar  > s.threshold_max) THEN true
                  WHEN s.sensor_type = 'vibration'   AND (r.vibration_hz  < s.threshold_min OR r.vibration_hz  > s.threshold_max) THEN true
                  WHEN s.sensor_type = 'rpm'          AND (r.rpm           < s.threshold_min OR r.rpm           > s.threshold_max) THEN true
                  ELSE false
              END AS anomaly_flag,
              CASE
                  WHEN s.sensor_type = 'temperature' AND r.temperature_c > s.threshold_max THEN 'Temperature above threshold: ' || CAST(r.temperature_c AS STRING) || 'C > ' || CAST(s.threshold_max AS STRING) || 'C'
                  WHEN s.sensor_type = 'temperature' AND r.temperature_c < s.threshold_min THEN 'Temperature below threshold'
                  WHEN s.sensor_type = 'pressure'    AND r.pressure_bar  > s.threshold_max THEN 'Pressure above threshold: ' || CAST(r.pressure_bar AS STRING) || ' bar > ' || CAST(s.threshold_max AS STRING) || ' bar'
                  WHEN s.sensor_type = 'vibration'   AND r.vibration_hz  > s.threshold_max THEN 'Vibration above threshold: ' || CAST(r.vibration_hz AS STRING) || ' Hz > ' || CAST(s.threshold_max AS STRING) || ' Hz'
                  WHEN s.sensor_type = 'rpm'          AND r.rpm           > s.threshold_max THEN 'RPM above threshold: ' || CAST(r.rpm AS STRING) || ' > ' || CAST(s.threshold_max AS STRING)
                  ELSE NULL
              END AS anomaly_reason,
              r.units_produced,
              r.defect_units,
              r.downtime_min,
              r.ingested_at
          FROM {{zone_prefix}}.bronze.raw_sensor_readings r
          LEFT JOIN {{zone_prefix}}.bronze.raw_sensors s
              ON r.sensor_id = s.sensor_id
      )
      SELECT * FROM readings_with_ma
  ) AS src
  ON tgt.reading_id = src.reading_id
  WHEN MATCHED THEN UPDATE SET
      tgt.temp_moving_avg  = src.temp_moving_avg,
      tgt.press_moving_avg = src.press_moving_avg,
      tgt.vib_moving_avg   = src.vib_moving_avg,
      tgt.rpm_moving_avg   = src.rpm_moving_avg,
      tgt.quality_score    = src.quality_score,
      tgt.anomaly_flag     = src.anomaly_flag,
      tgt.anomaly_reason   = src.anomaly_reason,
      tgt.validated_at     = src.ingested_at
  WHEN NOT MATCHED THEN INSERT (
      reading_id, sensor_id, sensor_type, plant_id, line_name, reading_time, shift_id,
      temperature_c, pressure_bar, vibration_hz, rpm,
      temp_moving_avg, press_moving_avg, vib_moving_avg, rpm_moving_avg,
      quality_score, anomaly_flag, anomaly_reason,
      units_produced, defect_units, downtime_min, validated_at
  ) VALUES (
      src.reading_id, src.sensor_id, src.sensor_type, src.plant_id, src.line_name,
      src.reading_time, src.shift_id, src.temperature_c, src.pressure_bar,
      src.vibration_hz, src.rpm, src.temp_moving_avg, src.press_moving_avg,
      src.vib_moving_avg, src.rpm_moving_avg, src.quality_score, src.anomaly_flag,
      src.anomaly_reason, src.units_produced, src.defect_units, src.downtime_min,
      src.ingested_at
  );

-- ===================== STEP: detect_anomalies =====================

STEP detect_anomalies
  DEPENDS ON (validate_readings)
AS
  -- Anomaly detection is already performed inline during validate_readings.
  -- This step serves as a DAG checkpoint confirming anomaly flags are populated
  -- before downstream consumers reference them in the fact table.
  SELECT COUNT(*) AS anomaly_count
  FROM {{zone_prefix}}.silver.readings_validated
  WHERE anomaly_flag = true;

-- ===================== STEP: build_dim_sensor =====================

STEP build_dim_sensor
  DEPENDS ON (validate_readings)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_sensor AS tgt
  USING (
      SELECT
          sensor_id       AS sensor_key,
          sensor_id,
          sensor_type,
          manufacturer,
          install_date,
          calibration_date,
          threshold_min,
          threshold_max
      FROM {{zone_prefix}}.bronze.raw_sensors
  ) AS src
  ON tgt.sensor_key = src.sensor_key
  WHEN MATCHED THEN UPDATE SET
      tgt.calibration_date = src.calibration_date,
      tgt.threshold_min    = src.threshold_min,
      tgt.threshold_max    = src.threshold_max
  WHEN NOT MATCHED THEN INSERT (sensor_key, sensor_id, sensor_type, manufacturer, install_date, calibration_date, threshold_min, threshold_max)
  VALUES (src.sensor_key, src.sensor_id, src.sensor_type, src.manufacturer, src.install_date, src.calibration_date, src.threshold_min, src.threshold_max);

-- ===================== STEP: build_dim_line =====================

STEP build_dim_line
  DEPENDS ON (validate_readings)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_production_line AS tgt
  USING (
      SELECT
          line_id                 AS line_key,
          plant_id,
          line_name,
          product_type,
          capacity_units_per_hour
      FROM {{zone_prefix}}.bronze.raw_production_lines
  ) AS src
  ON tgt.line_key = src.line_key
  WHEN NOT MATCHED THEN INSERT (line_key, plant_id, line_name, product_type, capacity_units_per_hour)
  VALUES (src.line_key, src.plant_id, src.line_name, src.product_type, src.capacity_units_per_hour);

-- ===================== STEP: build_dim_shift =====================

STEP build_dim_shift
  DEPENDS ON (validate_readings)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_shift AS tgt
  USING (
      SELECT
          shift_id    AS shift_key,
          shift_name,
          start_hour,
          end_hour,
          supervisor
      FROM {{zone_prefix}}.bronze.raw_shifts
  ) AS src
  ON tgt.shift_key = src.shift_key
  WHEN NOT MATCHED THEN INSERT (shift_key, shift_name, start_hour, end_hour, supervisor)
  VALUES (src.shift_key, src.shift_name, src.start_hour, src.end_hour, src.supervisor);

-- ===================== STEP: build_fact_readings =====================

STEP build_fact_readings
  DEPENDS ON (detect_anomalies, build_dim_sensor, build_dim_line, build_dim_shift)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.gold.fact_sensor_readings AS tgt
  USING (
      SELECT
          reading_id     AS reading_key,
          sensor_id      AS sensor_key,
          plant_id || '-' || line_name AS line_key,
          shift_id       AS shift_key,
          reading_time,
          temperature_c,
          pressure_bar,
          vibration_hz,
          rpm,
          quality_score,
          anomaly_flag
      FROM {{zone_prefix}}.silver.readings_validated
  ) AS src
  ON tgt.reading_key = src.reading_key
  WHEN MATCHED THEN UPDATE SET
      tgt.quality_score = src.quality_score,
      tgt.anomaly_flag  = src.anomaly_flag
  WHEN NOT MATCHED THEN INSERT (
      reading_key, sensor_key, line_key, shift_key, reading_time,
      temperature_c, pressure_bar, vibration_hz, rpm, quality_score, anomaly_flag
  ) VALUES (
      src.reading_key, src.sensor_key, src.line_key, src.shift_key, src.reading_time,
      src.temperature_c, src.pressure_bar, src.vibration_hz, src.rpm,
      src.quality_score, src.anomaly_flag
  );

-- ===================== STEP: compute_oee =====================

STEP compute_oee
  DEPENDS ON (build_fact_readings)
AS
  -- OEE = Availability x Performance x Quality
  -- Availability = (Planned - Downtime) / Planned  (planned = shift hours in minutes)
  -- Performance = Actual Output / (Capacity * Available Hours)
  -- Quality = Good Units / Total Units
  MERGE INTO {{zone_prefix}}.gold.kpi_oee AS tgt
  USING (
      WITH shift_metrics AS (
          SELECT
              v.plant_id,
              v.line_name,
              CAST(v.reading_time AS DATE) AS shift_date,
              sh.shift_name,
              -- Planned time per shift = 8 hours = 480 minutes
              480 AS planned_minutes,
              COALESCE(SUM(v.downtime_min), 0) AS downtime_minutes,
              SUM(v.units_produced) AS total_units,
              SUM(v.defect_units)   AS defect_units,
              COUNT(*) AS reading_count
          FROM {{zone_prefix}}.silver.readings_validated v
          JOIN {{zone_prefix}}.bronze.raw_shifts sh ON v.shift_id = sh.shift_id
          GROUP BY v.plant_id, v.line_name, CAST(v.reading_time AS DATE), sh.shift_name
      ),
      oee_calc AS (
          SELECT
              sm.plant_id,
              sm.line_name,
              sm.shift_date,
              sm.shift_name,
              -- Availability
              CAST((sm.planned_minutes - sm.downtime_minutes) * 100.0 / sm.planned_minutes AS DECIMAL(5,2)) AS availability_pct,
              -- Performance: compare actual vs capacity (using production line capacity)
              CAST(
                  CASE
                      WHEN pl.capacity_units_per_hour > 0
                      THEN sm.total_units * 100.0 / (pl.capacity_units_per_hour * (sm.planned_minutes - sm.downtime_minutes) / 60.0)
                      ELSE 0
                  END
              AS DECIMAL(5,2)) AS performance_pct,
              -- Quality
              CAST(
                  CASE
                      WHEN sm.total_units > 0
                      THEN (sm.total_units - sm.defect_units) * 100.0 / sm.total_units
                      ELSE 100
                  END
              AS DECIMAL(5,2)) AS quality_pct,
              sm.total_units,
              sm.defect_units,
              sm.downtime_minutes
          FROM shift_metrics sm
          LEFT JOIN {{zone_prefix}}.bronze.raw_production_lines pl
              ON sm.plant_id = pl.plant_id AND sm.line_name = pl.line_name
      )
      SELECT
          *,
          CAST(availability_pct * performance_pct * quality_pct / 10000.0 AS DECIMAL(5,2)) AS oee_pct
      FROM oee_calc
  ) AS src
  ON tgt.plant_id = src.plant_id AND tgt.line_name = src.line_name
     AND tgt.shift_date = src.shift_date AND tgt.shift_name = src.shift_name
  WHEN MATCHED THEN UPDATE SET
      tgt.availability_pct = src.availability_pct,
      tgt.performance_pct  = src.performance_pct,
      tgt.quality_pct      = src.quality_pct,
      tgt.oee_pct          = src.oee_pct,
      tgt.total_units      = src.total_units,
      tgt.defect_units     = src.defect_units,
      tgt.downtime_minutes = src.downtime_minutes
  WHEN NOT MATCHED THEN INSERT (
      plant_id, line_name, shift_date, shift_name,
      availability_pct, performance_pct, quality_pct, oee_pct,
      total_units, defect_units, downtime_minutes
  ) VALUES (
      src.plant_id, src.line_name, src.shift_date, src.shift_name,
      src.availability_pct, src.performance_pct, src.quality_pct, src.oee_pct,
      src.total_units, src.defect_units, src.downtime_minutes
  );

-- ===================== STEP: optimize_partitions =====================

STEP optimize_partitions
  DEPENDS ON (compute_oee)
  CONTINUE ON FAILURE
  TIMEOUT '10m'
AS
  OPTIMIZE {{zone_prefix}}.silver.readings_validated;
  OPTIMIZE {{zone_prefix}}.gold.fact_sensor_readings;
  OPTIMIZE {{zone_prefix}}.gold.kpi_oee;
