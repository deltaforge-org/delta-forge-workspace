-- =============================================================================
-- Manufacturing IoT Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE manufacturing_iot_02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS mfg.bronze.raw_sensors (
  sensor_id         STRING      NOT NULL,
  sensor_type       STRING      NOT NULL,
  manufacturer      STRING,
  install_date      DATE,
  calibration_date  DATE,
  threshold_min     DECIMAL(10,2),
  threshold_max     DECIMAL(10,2),
  plant_id          STRING,
  line_name         STRING,
  ingested_at       TIMESTAMP
) LOCATION 'mfg/manufacturing/bronze/raw_sensors';

CREATE DELTA TABLE IF NOT EXISTS mfg.bronze.raw_production_lines (
  line_id              STRING      NOT NULL,
  plant_id             STRING      NOT NULL,
  line_name            STRING,
  product_type         STRING,
  capacity_units_per_hour INT,
  ingested_at          TIMESTAMP
) LOCATION 'mfg/manufacturing/bronze/raw_production_lines';

CREATE DELTA TABLE IF NOT EXISTS mfg.bronze.raw_shifts (
  shift_id     STRING      NOT NULL,
  shift_name   STRING      NOT NULL,
  start_hour   INT,
  end_hour     INT,
  supervisor   STRING,
  ingested_at  TIMESTAMP
) LOCATION 'mfg/manufacturing/bronze/raw_shifts';

CREATE DELTA TABLE IF NOT EXISTS mfg.bronze.raw_production_targets (
  target_id             STRING      NOT NULL,
  plant_id              STRING      NOT NULL,
  line_name             STRING      NOT NULL,
  shift_id              STRING      NOT NULL,
  target_units_per_shift INT,
  max_acceptable_defect_pct DECIMAL(5,2),
  ingested_at           TIMESTAMP
) LOCATION 'mfg/manufacturing/bronze/raw_production_targets';

CREATE DELTA TABLE IF NOT EXISTS mfg.bronze.raw_readings (
  reading_id     STRING      NOT NULL,
  sensor_id      STRING      NOT NULL,
  plant_id       STRING      NOT NULL,
  line_name      STRING      NOT NULL,
  reading_time   TIMESTAMP   NOT NULL,
  value          DECIMAL(10,2),
  units_produced INT,
  defect_units   INT,
  downtime_min   INT         DEFAULT 0,
  ingested_at    TIMESTAMP,
  CHECK (value BETWEEN -50 AND 500 OR value BETWEEN 0 AND 200 OR value BETWEEN 0 AND 50000 OR value BETWEEN 0 AND 20000)
) LOCATION 'mfg/manufacturing/bronze/raw_readings'
PARTITIONED BY (plant_id, line_name);
