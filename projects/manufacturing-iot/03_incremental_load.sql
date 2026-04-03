-- =============================================================================
-- Manufacturing IoT Pipeline: Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "reading_id > 'R-080' AND reading_time > '2024-06-01T05:00:00'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.readings_validated, reading_id, reading_time, 1)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.readings_validated
-- SELECT * FROM {{zone_prefix}}.bronze.raw_sensor_readings
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.readings_validated, reading_id, reading_time, 1)}};

-- Show current watermark
SELECT MAX(validated_at) AS current_watermark
FROM {{zone_prefix}}.silver.readings_validated;

SELECT 'silver.readings_validated' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.readings_validated
UNION ALL
SELECT 'gold.fact_sensor_readings', COUNT(*)
FROM {{zone_prefix}}.gold.fact_sensor_readings;

-- =============================================================================
-- Insert 10 new sensor readings (June 2, Morning shift)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_sensor_readings VALUES
('R-081', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:00:00', 43.0,  6.2, 28.0, 1855.0, 31, 0, 0,  '2024-06-02T08:00:00'),
('R-082', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:15:00', 43.5,  6.3, 28.5, 1860.0, 30, 1, 0,  '2024-06-02T08:00:00'),
('R-083', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:30:00', 91.0,  6.1, 27.8, 1850.0, 28, 2, 0,  '2024-06-02T08:00:00'),
('R-084', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-02T06:00:00', 55.5,  8.5, 35.0, 2200.0, 15, 0, 0,  '2024-06-02T08:00:00'),
('R-085', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-02T06:15:00', 56.0,  8.6, 35.3, 2210.0, 14, 0, 0,  '2024-06-02T08:00:00'),
('R-086', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-02T06:00:00', 32.5,  4.5, 20.0, 1500.0, 38, 0, 0,  '2024-06-02T08:00:00'),
('R-087', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-02T06:15:00', 33.0,  4.6, 20.5, 1510.0, 37, 1, 0,  '2024-06-02T08:00:00'),
('R-088', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:00:00', 48.0,  7.2, 30.0, 2050.0, 22, 0, 0,  '2024-06-02T08:00:00'),
('R-089', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:15:00', 48.8,  7.3, 30.5, 2060.0, 23, 0, 0,  '2024-06-02T08:00:00'),
('R-090', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:30:00', 49.2,  7.1, 29.8, 2045.0, 22, 1, 5,  '2024-06-02T08:00:00');

ASSERT ROW_COUNT = 10
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only new readings
-- =============================================================================

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
            CAST(AVG(r.temperature_c) OVER (
                PARTITION BY r.sensor_id ORDER BY r.reading_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(8,2)) AS temp_moving_avg,
            CAST(AVG(r.pressure_bar) OVER (
                PARTITION BY r.sensor_id ORDER BY r.reading_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(8,2)) AS press_moving_avg,
            CAST(AVG(r.vibration_hz) OVER (
                PARTITION BY r.sensor_id ORDER BY r.reading_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(8,2)) AS vib_moving_avg,
            CAST(AVG(r.rpm) OVER (
                PARTITION BY r.sensor_id ORDER BY r.reading_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(8,2)) AS rpm_moving_avg,
            CASE
                WHEN r.units_produced > 0
                THEN CAST((1.0 - (r.defect_units * 1.0 / r.units_produced)) * 100 AS DECIMAL(5,2))
                ELSE 100.00
            END AS quality_score,
            CASE
                WHEN s.sensor_type = 'temperature' AND (r.temperature_c < s.threshold_min OR r.temperature_c > s.threshold_max) THEN true
                ELSE false
            END AS anomaly_flag,
            CASE
                WHEN s.sensor_type = 'temperature' AND r.temperature_c > s.threshold_max THEN 'Temperature above threshold'
                ELSE NULL
            END AS anomaly_reason,
            r.units_produced,
            r.defect_units,
            r.downtime_min,
            r.ingested_at
        FROM {{zone_prefix}}.bronze.raw_sensor_readings r
        LEFT JOIN {{zone_prefix}}.bronze.raw_sensors s ON r.sensor_id = s.sensor_id
        WHERE r.ingested_at > (SELECT COALESCE(MAX(validated_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.readings_validated)
    )
    SELECT * FROM readings_with_ma
) AS src
ON tgt.reading_id = src.reading_id
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

-- =============================================================================
-- Verify incremental results
-- =============================================================================

-- Silver should now have 80 + 10 = 90 validated readings
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.readings_validated;
-- Verify anomaly in incremental batch (R-083 temp = 91.0 > threshold 85.0)
ASSERT ROW_COUNT = 90
SELECT reading_id, temperature_c, anomaly_flag, anomaly_reason
FROM {{zone_prefix}}.silver.readings_validated
WHERE reading_id = 'R-083';

-- Verify watermark advanced
ASSERT VALUE anomaly_flag = true
SELECT MAX(validated_at) AS new_watermark
FROM {{zone_prefix}}.silver.readings_validated;
