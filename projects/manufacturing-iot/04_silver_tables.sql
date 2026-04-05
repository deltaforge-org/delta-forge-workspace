-- =============================================================================
-- Manufacturing IoT Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- Validated readings: CHECK constraints applied, out-of-range flagged
CREATE DELTA TABLE IF NOT EXISTS mfg.silver.readings_validated (
  reading_id       STRING      NOT NULL,
  sensor_id        STRING,
  sensor_type      STRING,
  plant_id         STRING,
  line_name        STRING,
  reading_time     TIMESTAMP,
  shift_id         STRING,
  value            DECIMAL(10,2),
  threshold_min    DECIMAL(10,2),
  threshold_max    DECIMAL(10,2),
  in_range_flag    BOOLEAN     DEFAULT true,
  quality_score    DECIMAL(5,2),
  units_produced   INT,
  defect_units     INT,
  downtime_min     INT,
  validated_at     TIMESTAMP,
  CHECK (value BETWEEN -50 AND 500 OR value BETWEEN 0 AND 200 OR value BETWEEN 0 AND 50000 OR value BETWEEN 0 AND 20000)
) LOCATION 'mfg/manufacturing/silver/readings_validated';

-- Smoothed readings: 5-reading moving avg, stddev, anomaly flag (2-sigma)
CREATE DELTA TABLE IF NOT EXISTS mfg.silver.readings_smoothed (
  reading_id       STRING      NOT NULL,
  sensor_id        STRING,
  sensor_type      STRING,
  plant_id         STRING,
  line_name        STRING,
  reading_time     TIMESTAMP,
  shift_id         STRING,
  value            DECIMAL(10,2),
  moving_avg       DECIMAL(10,2),
  moving_stddev    DECIMAL(10,4),
  anomaly_flag     BOOLEAN     DEFAULT false,
  anomaly_reason   STRING,
  smoothed_at      TIMESTAMP
) LOCATION 'mfg/manufacturing/silver/readings_smoothed';

-- Equipment status: derived uptime/downtime per shift from gap analysis
CREATE DELTA TABLE IF NOT EXISTS mfg.silver.equipment_status (
  status_id        STRING      NOT NULL,
  plant_id         STRING,
  line_name        STRING,
  shift_date       DATE,
  shift_id         STRING,
  planned_minutes  INT         DEFAULT 480,
  downtime_minutes INT,
  uptime_minutes   INT,
  unplanned_stops  INT,
  status           STRING,
  computed_at      TIMESTAMP
) LOCATION 'mfg/manufacturing/silver/equipment_status';
