-- =============================================================================
-- Manufacturing IoT Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE manufacturing_gold_tables
  DESCRIPTION 'Creates gold layer tables for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.dim_sensor (
  sensor_key       STRING      NOT NULL,
  sensor_id        STRING,
  sensor_type      STRING,
  manufacturer     STRING,
  install_date     DATE,
  calibration_date DATE,
  threshold_min    DECIMAL(10,2),
  threshold_max    DECIMAL(10,2),
  plant_id         STRING,
  line_name        STRING
) LOCATION 'mfg/manufacturing/gold/dim_sensor';

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.dim_line (
  line_key                STRING      NOT NULL,
  plant_id                STRING,
  line_name               STRING,
  product_type            STRING,
  capacity_units_per_hour INT
) LOCATION 'mfg/manufacturing/gold/dim_line';

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.dim_shift (
  shift_key    STRING      NOT NULL,
  shift_name   STRING,
  start_hour   INT,
  end_hour     INT,
  supervisor   STRING
) LOCATION 'mfg/manufacturing/gold/dim_shift';

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.fact_readings (
  reading_key    STRING      NOT NULL,
  sensor_key     STRING,
  line_key       STRING,
  shift_key      STRING,
  reading_time   TIMESTAMP,
  value          DECIMAL(10,2),
  smoothed_value DECIMAL(10,2),
  anomaly_flag   BOOLEAN,
  quality_score  DECIMAL(5,2)
) LOCATION 'mfg/manufacturing/gold/fact_readings';

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.kpi_oee (
  plant_id          STRING,
  line_name         STRING,
  shift_date        DATE,
  shift_name        STRING,
  planned_minutes   INT,
  downtime_minutes  INT,
  availability_pct  DECIMAL(5,2),
  actual_units      INT,
  target_units      INT,
  performance_pct   DECIMAL(5,2),
  good_units        INT,
  total_units       INT,
  quality_pct       DECIMAL(5,2),
  oee_pct           DECIMAL(5,2)
) LOCATION 'mfg/manufacturing/gold/kpi_oee';

CREATE DELTA TABLE IF NOT EXISTS mfg.gold.kpi_anomaly_trends (
  sensor_type       STRING,
  plant_id          STRING,
  trend_month       STRING,
  total_readings    INT,
  anomaly_count     INT,
  anomaly_rate_pct  DECIMAL(5,2),
  avg_deviation     DECIMAL(10,4),
  max_deviation     DECIMAL(10,4)
) LOCATION 'mfg/manufacturing/gold/kpi_anomaly_trends';
