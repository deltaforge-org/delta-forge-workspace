-- =============================================================================
-- Manufacturing IoT Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- Statistical anomaly detection: 5-reading moving avg + 2-sigma deviation.
-- OEE = Availability x Performance x Quality (non-trivial composite).
-- Equipment status derived from downtime gap analysis.
-- VACUUM RETAIN 2160 HOURS (90-day retention governance).
-- Pipeline DAG: 11 steps with parallel branches.
-- =============================================================================

-- ===================== PIPELINE =====================

PIPELINE manufacturing_iot_pipeline
  DESCRIPTION 'IoT sensor pipeline with 2-sigma anomaly detection, OEE calculation, equipment status, and 90-day VACUUM retention'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'manufacturing,iot,oee,anomaly-detection,vacuum'
  SLA 30
  FAIL_FAST true
  LIFECYCLE production
;

-- =============================================================================
-- Validate bronze tables
-- =============================================================================

SELECT COUNT(*) AS reading_count FROM mfg.bronze.raw_readings;
ASSERT VALUE reading_count = 90

SELECT COUNT(*) AS sensor_count FROM mfg.bronze.raw_sensors;
ASSERT VALUE sensor_count = 16

SELECT COUNT(*) AS line_count FROM mfg.bronze.raw_production_lines;
ASSERT VALUE line_count = 12

SELECT COUNT(*) AS shift_count FROM mfg.bronze.raw_shifts;
ASSERT VALUE shift_count = 3

SELECT COUNT(*) AS target_count FROM mfg.bronze.raw_production_targets;
ASSERT VALUE target_count = 12;

-- =============================================================================
-- STEP 2a: Validate readings — CHECK constraints, out-of-range flagging
-- =============================================================================
-- CHECK constraints: temperature_c BETWEEN -50 AND 500, pressure_bar BETWEEN 0 AND 200,
-- vibration_hz BETWEEN 0 AND 50000, rpm BETWEEN 0 AND 20000.
-- Quality score = 100 - (defect_units / units_produced * 100).

MERGE INTO mfg.silver.readings_validated AS tgt
USING (
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
        r.value,
        s.threshold_min,
        s.threshold_max,
        CASE
            WHEN s.sensor_type = 'temperature' AND r.value BETWEEN -50 AND 500    THEN true
            WHEN s.sensor_type = 'pressure'    AND r.value BETWEEN 0 AND 200      THEN true
            WHEN s.sensor_type = 'vibration'   AND r.value BETWEEN 0 AND 50000    THEN true
            WHEN s.sensor_type = 'rpm'         AND r.value BETWEEN 0 AND 20000    THEN true
            ELSE false
        END AS in_range_flag,
        CASE
            WHEN r.units_produced > 0
            THEN CAST((1.0 - (r.defect_units * 1.0 / r.units_produced)) * 100 AS DECIMAL(5,2))
            ELSE 100.00
        END AS quality_score,
        r.units_produced,
        r.defect_units,
        r.downtime_min,
        r.ingested_at
    FROM mfg.bronze.raw_readings r
    LEFT JOIN mfg.bronze.raw_sensors s ON r.sensor_id = s.sensor_id
) AS src
ON tgt.reading_id = src.reading_id
WHEN MATCHED THEN UPDATE SET
    tgt.in_range_flag  = src.in_range_flag,
    tgt.quality_score  = src.quality_score,
    tgt.validated_at   = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    reading_id, sensor_id, sensor_type, plant_id, line_name, reading_time, shift_id,
    value, threshold_min, threshold_max, in_range_flag, quality_score,
    units_produced, defect_units, downtime_min, validated_at
) VALUES (
    src.reading_id, src.sensor_id, src.sensor_type, src.plant_id, src.line_name,
    src.reading_time, src.shift_id, src.value, src.threshold_min, src.threshold_max,
    src.in_range_flag, src.quality_score, src.units_produced, src.defect_units,
    src.downtime_min, src.ingested_at
);

-- =============================================================================
-- STEP 2b: Build equipment status from downtime gap analysis
-- =============================================================================

MERGE INTO mfg.silver.equipment_status AS tgt
USING (
    WITH shift_downtime AS (
        SELECT
            r.plant_id,
            r.line_name,
            CAST(r.reading_time AS DATE) AS shift_date,
            CASE
                WHEN EXTRACT(HOUR FROM r.reading_time) >= 6  AND EXTRACT(HOUR FROM r.reading_time) < 14 THEN 'SHIFT-AM'
                WHEN EXTRACT(HOUR FROM r.reading_time) >= 14 AND EXTRACT(HOUR FROM r.reading_time) < 22 THEN 'SHIFT-PM'
                ELSE 'SHIFT-NIGHT'
            END AS shift_id,
            480 AS planned_minutes,
            COALESCE(SUM(r.downtime_min), 0) AS downtime_minutes,
            480 - COALESCE(SUM(r.downtime_min), 0) AS uptime_minutes,
            COUNT(CASE WHEN r.downtime_min > 0 THEN 1 END) AS unplanned_stops,
            CASE
                WHEN COALESCE(SUM(r.downtime_min), 0) = 0 THEN 'RUNNING'
                WHEN COALESCE(SUM(r.downtime_min), 0) < 30 THEN 'DEGRADED'
                ELSE 'IMPAIRED'
            END AS status
        FROM mfg.bronze.raw_readings r
        GROUP BY r.plant_id, r.line_name, CAST(r.reading_time AS DATE),
                 CASE
                     WHEN EXTRACT(HOUR FROM r.reading_time) >= 6  AND EXTRACT(HOUR FROM r.reading_time) < 14 THEN 'SHIFT-AM'
                     WHEN EXTRACT(HOUR FROM r.reading_time) >= 14 AND EXTRACT(HOUR FROM r.reading_time) < 22 THEN 'SHIFT-PM'
                     ELSE 'SHIFT-NIGHT'
                 END
    )
    SELECT
        sd.plant_id || '-' || sd.line_name || '-' || CAST(sd.shift_date AS STRING) || '-' || sd.shift_id AS status_id,
        sd.*
    FROM shift_downtime sd
) AS src
ON tgt.status_id = src.status_id
WHEN MATCHED THEN UPDATE SET
    tgt.downtime_minutes = src.downtime_minutes,
    tgt.uptime_minutes   = src.uptime_minutes,
    tgt.unplanned_stops  = src.unplanned_stops,
    tgt.status           = src.status,
    tgt.computed_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    status_id, plant_id, line_name, shift_date, shift_id, planned_minutes,
    downtime_minutes, uptime_minutes, unplanned_stops, status, computed_at
) VALUES (
    src.status_id, src.plant_id, src.line_name, src.shift_date, src.shift_id,
    src.planned_minutes, src.downtime_minutes, src.uptime_minutes,
    src.unplanned_stops, src.status, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Smooth and detect anomalies — 5-reading moving avg + 2-sigma
-- =============================================================================
-- Statistical anomaly detection:
--   moving_avg = AVG(value) OVER (PARTITION BY sensor_id ORDER BY reading_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
--   moving_stddev = STDDEV(value) OVER (same window)
--   anomaly_flag = ABS(value - moving_avg) > 2 * moving_stddev

MERGE INTO mfg.silver.readings_smoothed AS tgt
USING (
    WITH smoothed AS (
        SELECT
            v.reading_id,
            v.sensor_id,
            v.sensor_type,
            v.plant_id,
            v.line_name,
            v.reading_time,
            v.shift_id,
            v.value,
            -- 5-reading moving average
            CAST(AVG(v.value) OVER (
                PARTITION BY v.sensor_id ORDER BY v.reading_time
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(10,2)) AS moving_avg,
            -- Standard deviation over same window
            CAST(STDDEV(v.value) OVER (
                PARTITION BY v.sensor_id ORDER BY v.reading_time
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(10,4)) AS moving_stddev,
            v.validated_at
        FROM mfg.silver.readings_validated v
    )
    SELECT
        s.*,
        -- Flag if current reading deviates > 2 stddev from moving average
        CASE
            WHEN s.moving_stddev IS NOT NULL
                 AND s.moving_stddev > 0
                 AND ABS(s.value - s.moving_avg) > 2 * s.moving_stddev
            THEN true
            ELSE false
        END AS anomaly_flag,
        CASE
            WHEN s.moving_stddev IS NOT NULL
                 AND s.moving_stddev > 0
                 AND ABS(s.value - s.moving_avg) > 2 * s.moving_stddev
            THEN s.sensor_type || ' anomaly: value=' || CAST(s.value AS STRING)
                 || ' deviates ' || CAST(CAST(ABS(s.value - s.moving_avg) / NULLIF(s.moving_stddev, 0) AS DECIMAL(5,1)) AS STRING)
                 || ' sigma from moving_avg=' || CAST(s.moving_avg AS STRING)
            ELSE NULL
        END AS anomaly_reason
    FROM smoothed s
) AS src
ON tgt.reading_id = src.reading_id
WHEN MATCHED THEN UPDATE SET
    tgt.moving_avg     = src.moving_avg,
    tgt.moving_stddev  = src.moving_stddev,
    tgt.anomaly_flag   = src.anomaly_flag,
    tgt.anomaly_reason = src.anomaly_reason,
    tgt.smoothed_at    = src.validated_at
WHEN NOT MATCHED THEN INSERT (
    reading_id, sensor_id, sensor_type, plant_id, line_name, reading_time, shift_id,
    value, moving_avg, moving_stddev, anomaly_flag, anomaly_reason, smoothed_at
) VALUES (
    src.reading_id, src.sensor_id, src.sensor_type, src.plant_id, src.line_name,
    src.reading_time, src.shift_id, src.value, src.moving_avg, src.moving_stddev,
    src.anomaly_flag, src.anomaly_reason, src.validated_at
);

-- =============================================================================
-- STEP 4a: Build dim_sensor
-- =============================================================================

MERGE INTO mfg.gold.dim_sensor AS tgt
USING (
    SELECT
        sensor_id       AS sensor_key,
        sensor_id,
        sensor_type,
        manufacturer,
        install_date,
        calibration_date,
        threshold_min,
        threshold_max,
        plant_id,
        line_name
    FROM mfg.bronze.raw_sensors
) AS src
ON tgt.sensor_key = src.sensor_key
WHEN MATCHED THEN UPDATE SET
    tgt.calibration_date = src.calibration_date,
    tgt.threshold_min    = src.threshold_min,
    tgt.threshold_max    = src.threshold_max
WHEN NOT MATCHED THEN INSERT (
    sensor_key, sensor_id, sensor_type, manufacturer, install_date,
    calibration_date, threshold_min, threshold_max, plant_id, line_name
) VALUES (
    src.sensor_key, src.sensor_id, src.sensor_type, src.manufacturer,
    src.install_date, src.calibration_date, src.threshold_min, src.threshold_max,
    src.plant_id, src.line_name
);

-- =============================================================================
-- STEP 4b: Build dim_line
-- =============================================================================

MERGE INTO mfg.gold.dim_line AS tgt
USING (
    SELECT
        line_id                 AS line_key,
        plant_id,
        line_name,
        product_type,
        capacity_units_per_hour
    FROM mfg.bronze.raw_production_lines
) AS src
ON tgt.line_key = src.line_key
WHEN NOT MATCHED THEN INSERT (line_key, plant_id, line_name, product_type, capacity_units_per_hour)
VALUES (src.line_key, src.plant_id, src.line_name, src.product_type, src.capacity_units_per_hour);

-- =============================================================================
-- STEP 4c: Build dim_shift
-- =============================================================================

MERGE INTO mfg.gold.dim_shift AS tgt
USING (
    SELECT
        shift_id    AS shift_key,
        shift_name,
        start_hour,
        end_hour,
        supervisor
    FROM mfg.bronze.raw_shifts
) AS src
ON tgt.shift_key = src.shift_key
WHEN NOT MATCHED THEN INSERT (shift_key, shift_name, start_hour, end_hour, supervisor)
VALUES (src.shift_key, src.shift_name, src.start_hour, src.end_hour, src.supervisor);

-- =============================================================================
-- Build fact_readings (star schema with smoothed value + anomaly flag)
-- =============================================================================

MERGE INTO mfg.gold.fact_readings AS tgt
USING (
    SELECT
        sm.reading_id     AS reading_key,
        sm.sensor_id      AS sensor_key,
        sm.plant_id || '-' || sm.line_name AS line_key,
        sm.shift_id       AS shift_key,
        sm.reading_time,
        sm.value,
        sm.moving_avg     AS smoothed_value,
        sm.anomaly_flag,
        v.quality_score
    FROM mfg.silver.readings_smoothed sm
    JOIN mfg.silver.readings_validated v ON sm.reading_id = v.reading_id
) AS src
ON tgt.reading_key = src.reading_key
WHEN MATCHED THEN UPDATE SET
    tgt.smoothed_value = src.smoothed_value,
    tgt.anomaly_flag   = src.anomaly_flag,
    tgt.quality_score  = src.quality_score
WHEN NOT MATCHED THEN INSERT (
    reading_key, sensor_key, line_key, shift_key, reading_time,
    value, smoothed_value, anomaly_flag, quality_score
) VALUES (
    src.reading_key, src.sensor_key, src.line_key, src.shift_key, src.reading_time,
    src.value, src.smoothed_value, src.anomaly_flag, src.quality_score
);

-- =============================================================================
-- STEP 6a: Compute OEE = Availability x Performance x Quality
-- =============================================================================
-- Availability = (Planned Production Time - Downtime) / Planned Production Time
-- Performance  = Actual Units / Target Units (from production_targets)
-- Quality      = Good Units / Total Units
-- OEE          = Availability x Performance x Quality / 10000.0

MERGE INTO mfg.gold.kpi_oee AS tgt
USING (
    WITH shift_metrics AS (
        SELECT
            v.plant_id,
            v.line_name,
            CAST(v.reading_time AS DATE) AS shift_date,
            sh.shift_name,
            v.shift_id,
            480 AS planned_minutes,
            COALESCE(SUM(v.downtime_min), 0) AS downtime_minutes,
            SUM(v.units_produced) AS actual_units,
            SUM(v.defect_units)   AS defect_units,
            SUM(v.units_produced) - SUM(v.defect_units) AS good_units
        FROM mfg.silver.readings_validated v
        JOIN mfg.bronze.raw_shifts sh ON v.shift_id = sh.shift_id
        GROUP BY v.plant_id, v.line_name, CAST(v.reading_time AS DATE), sh.shift_name, v.shift_id
    ),
    oee_calc AS (
        SELECT
            sm.plant_id,
            sm.line_name,
            sm.shift_date,
            sm.shift_name,
            sm.planned_minutes,
            sm.downtime_minutes,
            -- Availability
            CAST((sm.planned_minutes - sm.downtime_minutes) * 100.0 / sm.planned_minutes AS DECIMAL(5,2)) AS availability_pct,
            sm.actual_units,
            -- Target from production_targets (if exists)
            COALESCE(pt.target_units_per_shift, sm.actual_units) AS target_units,
            -- Performance
            CAST(
                CASE
                    WHEN COALESCE(pt.target_units_per_shift, 0) > 0
                    THEN sm.actual_units * 100.0 / pt.target_units_per_shift
                    ELSE 100.0
                END
            AS DECIMAL(5,2)) AS performance_pct,
            sm.good_units,
            sm.actual_units AS total_units,
            -- Quality
            CAST(
                CASE
                    WHEN sm.actual_units > 0
                    THEN sm.good_units * 100.0 / sm.actual_units
                    ELSE 100
                END
            AS DECIMAL(5,2)) AS quality_pct,
            sm.defect_units
        FROM shift_metrics sm
        LEFT JOIN mfg.bronze.raw_production_targets pt
            ON sm.plant_id = pt.plant_id AND sm.line_name = pt.line_name AND sm.shift_id = pt.shift_id
    )
    SELECT
        o.*,
        CAST(o.availability_pct * o.performance_pct * o.quality_pct / 10000.0 AS DECIMAL(5,2)) AS oee_pct
    FROM oee_calc o
) AS src
ON tgt.plant_id = src.plant_id AND tgt.line_name = src.line_name
   AND tgt.shift_date = src.shift_date AND tgt.shift_name = src.shift_name
WHEN MATCHED THEN UPDATE SET
    tgt.planned_minutes  = src.planned_minutes,
    tgt.downtime_minutes = src.downtime_minutes,
    tgt.availability_pct = src.availability_pct,
    tgt.actual_units     = src.actual_units,
    tgt.target_units     = src.target_units,
    tgt.performance_pct  = src.performance_pct,
    tgt.good_units       = src.good_units,
    tgt.total_units      = src.total_units,
    tgt.quality_pct      = src.quality_pct,
    tgt.oee_pct          = src.oee_pct
WHEN NOT MATCHED THEN INSERT (
    plant_id, line_name, shift_date, shift_name, planned_minutes, downtime_minutes,
    availability_pct, actual_units, target_units, performance_pct,
    good_units, total_units, quality_pct, oee_pct
) VALUES (
    src.plant_id, src.line_name, src.shift_date, src.shift_name,
    src.planned_minutes, src.downtime_minutes, src.availability_pct,
    src.actual_units, src.target_units, src.performance_pct,
    src.good_units, src.total_units, src.quality_pct, src.oee_pct
);

-- =============================================================================
-- STEP 6b: Compute anomaly trends by sensor type over time
-- =============================================================================

MERGE INTO mfg.gold.kpi_anomaly_trends AS tgt
USING (
    SELECT
        sm.sensor_type,
        sm.plant_id,
        DATE_TRUNC('month', sm.reading_time) AS trend_month,
        COUNT(*) AS total_readings,
        COUNT(CASE WHEN sm.anomaly_flag = true THEN 1 END) AS anomaly_count,
        CAST(
            COUNT(CASE WHEN sm.anomaly_flag = true THEN 1 END) * 100.0 / COUNT(*)
        AS DECIMAL(5,2)) AS anomaly_rate_pct,
        CAST(AVG(
            CASE WHEN sm.anomaly_flag = true
            THEN ABS(sm.value - sm.moving_avg) / NULLIF(sm.moving_stddev, 0)
            END
        ) AS DECIMAL(10,4)) AS avg_deviation,
        CAST(MAX(
            CASE WHEN sm.anomaly_flag = true
            THEN ABS(sm.value - sm.moving_avg) / NULLIF(sm.moving_stddev, 0)
            END
        ) AS DECIMAL(10,4)) AS max_deviation
    FROM mfg.silver.readings_smoothed sm
    GROUP BY sm.sensor_type, sm.plant_id, DATE_TRUNC('month', sm.reading_time)
) AS src
ON tgt.sensor_type = src.sensor_type AND tgt.plant_id = src.plant_id
   AND tgt.trend_month = src.trend_month
WHEN MATCHED THEN UPDATE SET
    tgt.total_readings   = src.total_readings,
    tgt.anomaly_count    = src.anomaly_count,
    tgt.anomaly_rate_pct = src.anomaly_rate_pct,
    tgt.avg_deviation    = src.avg_deviation,
    tgt.max_deviation    = src.max_deviation
WHEN NOT MATCHED THEN INSERT (
    sensor_type, plant_id, trend_month, total_readings, anomaly_count,
    anomaly_rate_pct, avg_deviation, max_deviation
) VALUES (
    src.sensor_type, src.plant_id, src.trend_month, src.total_readings,
    src.anomaly_count, src.anomaly_rate_pct, src.avg_deviation, src.max_deviation
);

-- =============================================================================
-- VACUUM and OPTIMIZE — 90-day retention governance
-- =============================================================================

OPTIMIZE mfg.silver.readings_validated;
OPTIMIZE mfg.silver.readings_smoothed;
OPTIMIZE mfg.gold.fact_readings;
OPTIMIZE mfg.gold.kpi_oee;
VACUUM mfg.bronze.raw_readings RETAIN 2160 HOURS;
VACUUM mfg.silver.readings_validated RETAIN 2160 HOURS;
VACUUM mfg.silver.readings_smoothed RETAIN 2160 HOURS;
