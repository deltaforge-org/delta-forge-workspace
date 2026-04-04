-- =============================================================================
-- Manufacturing IoT Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing of new sensor readings (June 2 morning)
-- into validated and smoothed silver layers. Uses INCREMENTAL_FILTER macro.
-- RESTORE for point-in-time recovery.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "reading_id > 'R-090' AND reading_time > '2024-06-01T21:00:00'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(mfg.silver.readings_validated, reading_id, reading_time, 1)}};

-- Show current watermark
SELECT MAX(validated_at) AS current_watermark
FROM mfg.silver.readings_validated;

SELECT
  'silver.readings_validated' AS table_name, COUNT(*) AS row_count
FROM mfg.silver.readings_validated
UNION ALL
SELECT 'silver.readings_smoothed', COUNT(*)
FROM mfg.silver.readings_smoothed
UNION ALL
SELECT 'gold.fact_readings', COUNT(*)
FROM mfg.gold.fact_readings;

-- =============================================================================
-- Insert 10 new sensor readings (June 2, Morning shift)
-- =============================================================================
-- Includes 1 temperature anomaly (R-093: 91.0C spike on P1LA),
-- 1 downtime event (R-100: 5min), normal readings across 4 plants.

MERGE INTO mfg.bronze.raw_readings AS tgt
USING (
    VALUES
    ('R-091', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:00:00', 43.00,  31, 0, 0,  '2024-06-02T08:00:00'),
    ('R-092', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:15:00', 43.50,  30, 1, 0,  '2024-06-02T08:00:00'),
    ('R-093', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-02T06:30:00', 91.00,  28, 2, 0,  '2024-06-02T08:00:00'),
    ('R-094', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-02T06:00:00', 55.50,  15, 0, 0,  '2024-06-02T08:00:00'),
    ('R-095', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-02T06:15:00', 56.00,  14, 0, 0,  '2024-06-02T08:00:00'),
    ('R-096', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-02T06:00:00', 32.50,  38, 0, 0,  '2024-06-02T08:00:00'),
    ('R-097', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-02T06:15:00', 33.00,  37, 1, 0,  '2024-06-02T08:00:00'),
    ('R-098', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:00:00', 48.00,  22, 0, 0,  '2024-06-02T08:00:00'),
    ('R-099', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:15:00', 48.80,  23, 0, 0,  '2024-06-02T08:00:00'),
    ('R-100', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-02T06:30:00', 49.20,  22, 1, 5,  '2024-06-02T08:00:00')
) AS src(reading_id, sensor_id, plant_id, line_name, reading_time, value, units_produced, defect_units, downtime_min, ingested_at)
ON tgt.reading_id = src.reading_id
WHEN NOT MATCHED THEN INSERT (
    reading_id, sensor_id, plant_id, line_name, reading_time, value,
    units_produced, defect_units, downtime_min, ingested_at
) VALUES (
    src.reading_id, src.sensor_id, src.plant_id, src.line_name, src.reading_time, src.value,
    src.units_produced, src.defect_units, src.downtime_min, src.ingested_at
);

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS new_reading_count
FROM mfg.bronze.raw_readings
WHERE ingested_at >= '2024-06-02T08:00:00';


-- =============================================================================
-- Incremental MERGE: validate new readings
-- =============================================================================

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
  WHERE {{INCREMENTAL_FILTER(mfg.silver.readings_validated, reading_id, reading_time, 1)}}
) AS src
ON tgt.reading_id = src.reading_id
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
-- Incremental MERGE: smooth new readings and detect anomalies
-- =============================================================================

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
          CAST(AVG(v.value) OVER (
              PARTITION BY v.sensor_id ORDER BY v.reading_time
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ) AS DECIMAL(10,2)) AS moving_avg,
          CAST(STDDEV(v.value) OVER (
              PARTITION BY v.sensor_id ORDER BY v.reading_time
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ) AS DECIMAL(10,4)) AS moving_stddev,
          v.validated_at
      FROM mfg.silver.readings_validated v
  )
  SELECT
      s.*,
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
               || ' deviates from moving_avg=' || CAST(s.moving_avg AS STRING)
          ELSE NULL
      END AS anomaly_reason
  FROM smoothed s
  WHERE s.reading_id IN (SELECT reading_id FROM mfg.silver.readings_validated WHERE validated_at >= '2024-06-02T08:00:00')
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
-- Verify incremental results
-- =============================================================================

-- Validated should now have 90 + 10 = 100
SELECT COUNT(*) AS validated_total FROM mfg.silver.readings_validated;

ASSERT VALUE validated_total = 100
SELECT reading_id, value, in_range_flag
FROM mfg.silver.readings_validated
WHERE reading_id = 'R-093';

-- Verify anomaly detection in incremental batch (R-093 temp = 91.0 spike)
ASSERT VALUE in_range_flag = true
SELECT reading_id, value, anomaly_flag, anomaly_reason
FROM mfg.silver.readings_smoothed
WHERE reading_id = 'R-093';

ASSERT VALUE anomaly_flag = true
SELECT MAX(validated_at) AS new_watermark
FROM mfg.silver.readings_validated;

-- =============================================================================
-- RESTORE demonstration: point-in-time recovery
-- =============================================================================

RESTORE mfg.silver.readings_validated TO VERSION 0;
