-- =============================================================================
-- Manufacturing IoT Pipeline: Object Creation & Seed Data
-- =============================================================================
-- Multi-plant manufacturer with 4 plants, 3 lines each, 4 sensor types.
-- Statistical anomaly detection (2-sigma from 5-reading moving average).
-- OEE = Availability x Performance x Quality. VACUUM 90-day retention.
-- CHECK constraints on physical thresholds. Bloom filter on sensor_id.
-- =============================================================================

-- ===================== SCHEDULE =====================

SCHEDULE manufacturing_2hr_schedule
  CRON '0 */2 * * *'
  TIMEZONE 'UTC'
  RETRIES 2
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

-- ===================== ZONES =====================

PIPELINE manufacturing_create_objects
  DESCRIPTION 'Creates zones, schemas, tables, seed data, and pseudonymisation rules for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;


CREATE ZONE IF NOT EXISTS mfg TYPE TEMP
  COMMENT 'Manufacturing IoT project zone — anomaly detection, OEE, equipment status';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS mfg.bronze COMMENT 'Raw IoT sensor readings, sensor metadata, production lines, shifts, and targets';
CREATE SCHEMA IF NOT EXISTS mfg.silver COMMENT 'Validated readings with moving averages, anomaly flags, and equipment uptime status';
CREATE SCHEMA IF NOT EXISTS mfg.gold   COMMENT 'Star schema for OEE, anomaly trends, and production analytics';

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

-- ===================== PSEUDONYMISATION =====================

CREATE PSEUDONYMISATION RULE ON mfg.bronze.raw_shifts (supervisor) TRANSFORM mask PARAMS (chars = 4);

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE mfg.bronze.raw_sensors TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_production_lines TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_shifts TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_production_targets TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_readings TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.readings_validated TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.readings_smoothed TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.equipment_status TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_sensor TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_line TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_shift TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.fact_readings TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.kpi_oee TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.kpi_anomaly_trends TO USER admin;

-- ===================== SEED DATA: SENSORS (16 rows) =====================
-- 4 plants x varying lines x 4 sensor types (temperature, pressure, vibration, rpm)
-- Each sensor has physical threshold_min/threshold_max for anomaly detection.

INSERT INTO mfg.bronze.raw_sensors VALUES
('SEN-P1LA-TEMP', 'temperature', 'ThermoTech',  '2023-01-15', '2024-05-01', -50.00, 500.00,  'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1LA-PRES', 'pressure',    'PressurePro', '2023-01-15', '2024-05-01',   0.00, 200.00,  'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1LA-VIB',  'vibration',   'VibraSense',  '2023-03-20', '2024-04-15',   0.00, 50000.00,'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1LA-RPM',  'rpm',         'RotorMax',    '2023-03-20', '2024-04-15',   0.00, 20000.00,'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1LB-TEMP', 'temperature', 'ThermoTech',  '2023-02-10', '2024-05-10', -50.00, 500.00,  'PLANT-01', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P1LB-PRES', 'pressure',    'PressurePro', '2023-02-10', '2024-05-10',   0.00, 200.00,  'PLANT-01', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P2LA-TEMP', 'temperature', 'ThermoTech',  '2023-04-05', '2024-06-01', -50.00, 500.00,  'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2LA-PRES', 'pressure',    'PressurePro', '2023-04-05', '2024-06-01',   0.00, 200.00,  'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2LA-VIB',  'vibration',   'VibraSense',  '2023-04-05', '2024-06-01',   0.00, 50000.00,'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2LA-RPM',  'rpm',         'RotorMax',    '2023-04-05', '2024-06-01',   0.00, 20000.00,'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3LA-TEMP', 'temperature', 'ThermoTech',  '2023-06-15', '2024-03-20', -50.00, 500.00,  'PLANT-03', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3LA-VIB',  'vibration',   'VibraSense',  '2023-06-15', '2024-03-20',   0.00, 50000.00,'PLANT-03', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3LB-TEMP', 'temperature', 'ThermoTech',  '2023-07-01', '2024-04-01', -50.00, 500.00,  'PLANT-03', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P3LB-PRES', 'pressure',    'PressurePro', '2023-07-01', '2024-04-01',   0.00, 200.00,  'PLANT-03', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P4LA-TEMP', 'temperature', 'ThermoTech',  '2023-09-10', '2024-05-15', -50.00, 500.00,  'PLANT-04', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P4LA-RPM',  'rpm',         'RotorMax',    '2023-09-10', '2024-05-15',   0.00, 20000.00,'PLANT-04', 'Line-A', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM mfg.bronze.raw_sensors;


-- ===================== SEED DATA: PRODUCTION LINES (12 rows) =====================
-- 4 plants x 3 lines each

INSERT INTO mfg.bronze.raw_production_lines VALUES
('PL-P1-LA', 'PLANT-01', 'Line-A', 'Automotive Parts',     120, '2024-06-01T00:00:00'),
('PL-P1-LB', 'PLANT-01', 'Line-B', 'Automotive Parts',     100, '2024-06-01T00:00:00'),
('PL-P1-LC', 'PLANT-01', 'Line-C', 'Electronics Boards',    80, '2024-06-01T00:00:00'),
('PL-P2-LA', 'PLANT-02', 'Line-A', 'Aerospace Components',  60, '2024-06-01T00:00:00'),
('PL-P2-LB', 'PLANT-02', 'Line-B', 'Aerospace Components',  55, '2024-06-01T00:00:00'),
('PL-P2-LC', 'PLANT-02', 'Line-C', 'Aerospace Components',  50, '2024-06-01T00:00:00'),
('PL-P3-LA', 'PLANT-03', 'Line-A', 'Consumer Goods',       150, '2024-06-01T00:00:00'),
('PL-P3-LB', 'PLANT-03', 'Line-B', 'Consumer Goods',       140, '2024-06-01T00:00:00'),
('PL-P3-LC', 'PLANT-03', 'Line-C', 'Consumer Goods',       130, '2024-06-01T00:00:00'),
('PL-P4-LA', 'PLANT-04', 'Line-A', 'Medical Devices',       90, '2024-06-01T00:00:00'),
('PL-P4-LB', 'PLANT-04', 'Line-B', 'Medical Devices',       85, '2024-06-01T00:00:00'),
('PL-P4-LC', 'PLANT-04', 'Line-C', 'Medical Devices',       80, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM mfg.bronze.raw_production_lines;


-- ===================== SEED DATA: SHIFTS (3 rows) =====================

INSERT INTO mfg.bronze.raw_shifts VALUES
('SHIFT-AM',   'Morning',   6, 14, 'Sarah Chen',    '2024-06-01T00:00:00'),
('SHIFT-PM',   'Afternoon', 14, 22, 'Mike Johnson',  '2024-06-01T00:00:00'),
('SHIFT-NIGHT','Night',     22,  6, 'Lisa Rodriguez','2024-06-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM mfg.bronze.raw_shifts;


-- ===================== SEED DATA: PRODUCTION TARGETS (12 rows) =====================
-- Target units per shift for each plant/line combination

INSERT INTO mfg.bronze.raw_production_targets VALUES
('TGT-P1LA-AM', 'PLANT-01', 'Line-A', 'SHIFT-AM', 960,  3.00, '2024-06-01T00:00:00'),
('TGT-P1LB-AM', 'PLANT-01', 'Line-B', 'SHIFT-AM', 800,  3.50, '2024-06-01T00:00:00'),
('TGT-P1LA-PM', 'PLANT-01', 'Line-A', 'SHIFT-PM', 960,  3.00, '2024-06-01T00:00:00'),
('TGT-P2LA-AM', 'PLANT-02', 'Line-A', 'SHIFT-AM', 480,  2.00, '2024-06-01T00:00:00'),
('TGT-P2LA-PM', 'PLANT-02', 'Line-A', 'SHIFT-PM', 480,  2.00, '2024-06-01T00:00:00'),
('TGT-P3LA-AM', 'PLANT-03', 'Line-A', 'SHIFT-AM', 1200, 4.00, '2024-06-01T00:00:00'),
('TGT-P3LB-PM', 'PLANT-03', 'Line-B', 'SHIFT-PM', 1120, 4.00, '2024-06-01T00:00:00'),
('TGT-P4LA-AM', 'PLANT-04', 'Line-A', 'SHIFT-AM', 720,  1.50, '2024-06-01T00:00:00'),
('TGT-P4LA-NT', 'PLANT-04', 'Line-A', 'SHIFT-NIGHT', 720, 2.00, '2024-06-01T00:00:00'),
('TGT-P1LA-NT', 'PLANT-01', 'Line-A', 'SHIFT-NIGHT', 960, 3.50, '2024-06-01T00:00:00'),
('TGT-P2LA-NT', 'PLANT-02', 'Line-A', 'SHIFT-NIGHT', 480, 2.50, '2024-06-01T00:00:00'),
('TGT-P3LA-PM', 'PLANT-03', 'Line-A', 'SHIFT-PM', 1200, 4.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM mfg.bronze.raw_production_targets;


-- ===================== SEED DATA: SENSOR READINGS (90 rows) =====================
-- 4 plants x varying lines x ~8 sensors, readings every 15 min.
-- Each row has a single 'value' field — interpretation depends on sensor_type.
-- Includes 6 anomalous readings:
--   2 temperature spikes (R-005, R-023)
--   2 pressure drops (R-016 high pressure, R-080 high pressure)
--   2 vibration spikes (R-034, R-063)
-- Also 3 unplanned downtime events (R-010: 15min, R-069: 20min, R-044: 5min)

INSERT INTO mfg.bronze.raw_readings VALUES
-- PLANT-01, Line-A, Morning shift (temp sensor), June 1
('R-001', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:00:00', 42.50,  30, 0, 0,  '2024-06-01T08:00:00'),
('R-002', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:15:00', 43.10,  31, 1, 0,  '2024-06-01T08:00:00'),
('R-003', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:30:00', 44.00,  29, 0, 0,  '2024-06-01T08:00:00'),
('R-004', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:45:00', 43.80,  30, 0, 0,  '2024-06-01T08:00:00'),
('R-005', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:00:00', 88.50,  28, 2, 0,  '2024-06-01T08:00:00'),
('R-006', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:15:00', 45.20,  31, 0, 0,  '2024-06-01T08:00:00'),
('R-007', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:30:00', 44.80,  30, 0, 0,  '2024-06-01T08:00:00'),
('R-008', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:45:00', 43.50,  29, 1, 0,  '2024-06-01T08:00:00'),
('R-009', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T08:00:00', 44.20,  31, 0, 0,  '2024-06-01T08:00:00'),
('R-010', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T08:15:00', 44.00,  30, 0, 15, '2024-06-01T10:00:00'),

-- PLANT-01, Line-B, Morning shift (temp sensor)
('R-011', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:00:00', 38.20,  25, 0, 0,  '2024-06-01T08:00:00'),
('R-012', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:15:00', 38.50,  24, 1, 0,  '2024-06-01T08:00:00'),
('R-013', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:30:00', 39.00,  25, 0, 0,  '2024-06-01T08:00:00'),
('R-014', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:45:00', 38.80,  26, 0, 0,  '2024-06-01T08:00:00'),
('R-015', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:00:00', 39.20,  25, 0, 0,  '2024-06-01T08:00:00'),
('R-016', 'SEN-P1LB-PRES', 'PLANT-01', 'Line-B', '2024-06-01T07:15:00', 185.50, 23, 2, 0,  '2024-06-01T08:00:00'),
('R-017', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:30:00', 38.60,  26, 0, 0,  '2024-06-01T08:00:00'),
('R-018', 'SEN-P1LB-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:45:00', 39.10,  25, 0, 10, '2024-06-01T08:00:00'),

-- PLANT-01, Line-A, Pressure sensor (morning)
('R-019', 'SEN-P1LA-PRES', 'PLANT-01', 'Line-A', '2024-06-01T06:00:00', 6.20,   30, 0, 0,  '2024-06-01T08:00:00'),
('R-020', 'SEN-P1LA-PRES', 'PLANT-01', 'Line-A', '2024-06-01T06:15:00', 6.30,   31, 0, 0,  '2024-06-01T08:00:00'),
('R-021', 'SEN-P1LA-PRES', 'PLANT-01', 'Line-A', '2024-06-01T06:30:00', 6.10,   29, 0, 0,  '2024-06-01T08:00:00'),
('R-022', 'SEN-P1LA-PRES', 'PLANT-01', 'Line-A', '2024-06-01T06:45:00', 6.40,   30, 0, 0,  '2024-06-01T08:00:00'),

-- PLANT-02, Line-A, Morning shift (temp sensor)
('R-023', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:00:00', 55.00,  15, 0, 0,  '2024-06-01T08:00:00'),
('R-024', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:15:00', 55.80,  14, 0, 0,  '2024-06-01T08:00:00'),
('R-025', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:30:00', 56.20,  15, 1, 0,  '2024-06-01T08:00:00'),
('R-026', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:45:00', 55.50,  14, 0, 0,  '2024-06-01T08:00:00'),
('R-027', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:00:00', 94.00,  13, 2, 0,  '2024-06-01T08:00:00'),
('R-028', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:15:00', 56.00,  15, 0, 0,  '2024-06-01T08:00:00'),
('R-029', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:30:00', 55.30,  14, 0, 0,  '2024-06-01T08:00:00'),
('R-030', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:45:00', 55.70,  15, 0, 0,  '2024-06-01T08:00:00'),
('R-031', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T08:00:00', 56.10,  14, 1, 5,  '2024-06-01T10:00:00'),
('R-032', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T08:15:00', 55.90,  15, 0, 0,  '2024-06-01T10:00:00'),

-- PLANT-02, Line-A, Pressure sensor
('R-033', 'SEN-P2LA-PRES', 'PLANT-02', 'Line-A', '2024-06-01T06:00:00', 8.50,   15, 0, 0,  '2024-06-01T08:00:00'),
('R-034', 'SEN-P2LA-VIB',  'PLANT-02', 'Line-A', '2024-06-01T06:15:00', 35000.00, 14, 0, 0,'2024-06-01T08:00:00'),
('R-035', 'SEN-P2LA-PRES', 'PLANT-02', 'Line-A', '2024-06-01T06:30:00', 8.40,   15, 0, 0,  '2024-06-01T08:00:00'),
('R-036', 'SEN-P2LA-RPM',  'PLANT-02', 'Line-A', '2024-06-01T06:45:00', 2200.00, 14, 0, 0, '2024-06-01T08:00:00'),

-- PLANT-03, Line-A, Morning shift (temp sensor)
('R-037', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:00:00', 32.00,  38, 1, 0,  '2024-06-01T08:00:00'),
('R-038', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:15:00', 32.50,  37, 0, 0,  '2024-06-01T08:00:00'),
('R-039', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:30:00', 33.00,  38, 0, 0,  '2024-06-01T08:00:00'),
('R-040', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:45:00', 32.80,  37, 1, 0,  '2024-06-01T08:00:00'),
('R-041', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:00:00', 32.20,  39, 0, 0,  '2024-06-01T08:00:00'),
('R-042', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:15:00', 33.50,  36, 2, 0,  '2024-06-01T08:00:00'),
('R-043', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:30:00', 32.60,  38, 0, 0,  '2024-06-01T08:00:00'),
('R-044', 'SEN-P3LA-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:45:00', 33.10,  37, 0, 0,  '2024-06-01T08:00:00'),

-- PLANT-04, Line-A, Morning shift (temp sensor)
('R-045', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:00:00', 48.00,  22, 0, 0,  '2024-06-01T08:00:00'),
('R-046', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:15:00', 48.50,  23, 0, 0,  '2024-06-01T08:00:00'),
('R-047', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:30:00', 49.00,  22, 1, 0,  '2024-06-01T08:00:00'),
('R-048', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:45:00', 48.20,  23, 0, 0,  '2024-06-01T08:00:00'),
('R-049', 'SEN-P4LA-RPM',  'PLANT-04', 'Line-A', '2024-06-01T07:00:00', 2050.00, 21, 0, 0, '2024-06-01T08:00:00'),
('R-050', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:15:00', 49.50,  23, 0, 0,  '2024-06-01T08:00:00'),
('R-051', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:30:00', 48.00,  22, 1, 0,  '2024-06-01T08:00:00'),
('R-052', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:45:00', 48.60,  23, 0, 5,  '2024-06-01T08:00:00'),

-- AFTERNOON SHIFT (2pm-10pm), June 1
-- PLANT-01, Line-A (temp)
('R-053', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:00:00', 45.00,  32, 0, 0,  '2024-06-01T16:00:00'),
('R-054', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:15:00', 45.30,  31, 1, 0,  '2024-06-01T16:00:00'),
('R-055', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:30:00', 44.80,  32, 0, 0,  '2024-06-01T16:00:00'),
('R-056', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:45:00', 45.50,  30, 0, 0,  '2024-06-01T16:00:00'),
('R-057', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:00:00', 45.10,  31, 0, 0,  '2024-06-01T16:00:00'),
('R-058', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:15:00', 44.50,  32, 0, 0,  '2024-06-01T16:00:00'),
('R-059', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:30:00', 45.80,  30, 1, 0,  '2024-06-01T16:00:00'),
('R-060', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:45:00', 45.20,  31, 0, 0,  '2024-06-01T16:00:00'),

-- PLANT-02, Line-A, Afternoon (temp)
('R-061', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:00:00', 56.50,  15, 0, 0,  '2024-06-01T16:00:00'),
('R-062', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:15:00', 57.00,  14, 0, 0,  '2024-06-01T16:00:00'),
('R-063', 'SEN-P2LA-VIB',  'PLANT-02', 'Line-A', '2024-06-01T14:30:00', 42000.00, 15, 1, 0,'2024-06-01T16:00:00'),
('R-064', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:45:00', 57.20,  14, 0, 0,  '2024-06-01T16:00:00'),
('R-065', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T15:00:00', 56.30,  15, 0, 0,  '2024-06-01T16:00:00'),
('R-066', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T15:15:00', 56.90,  14, 0, 0,  '2024-06-01T16:00:00'),

-- PLANT-03, Line-B, Afternoon (temp)
('R-067', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:00:00', 34.00,  35, 0, 0,  '2024-06-01T16:00:00'),
('R-068', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:15:00', 34.50,  34, 1, 0,  '2024-06-01T16:00:00'),
('R-069', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:30:00', 35.00,  35, 0, 20, '2024-06-01T16:00:00'),
('R-070', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:45:00', 34.20,  36, 0, 0,  '2024-06-01T16:00:00'),
('R-071', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T15:00:00', 34.80,  33, 3, 0,  '2024-06-01T16:00:00'),
('R-072', 'SEN-P3LB-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T15:15:00', 34.60,  35, 0, 0,  '2024-06-01T16:00:00'),

-- NIGHT SHIFT (10pm-6am), June 1->2
-- PLANT-01, Line-A (temp)
('R-073', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:00:00', 42.00,  28, 0, 0,  '2024-06-02T00:00:00'),
('R-074', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:15:00', 42.30,  29, 0, 0,  '2024-06-02T00:00:00'),
('R-075', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:30:00', 41.80,  28, 1, 0,  '2024-06-02T00:00:00'),
('R-076', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:45:00', 42.50,  29, 0, 0,  '2024-06-02T00:00:00'),
('R-077', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T23:00:00', 42.00,  27, 0, 20, '2024-06-02T00:00:00'),
('R-078', 'SEN-P1LA-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T23:15:00', 41.50,  28, 0, 0,  '2024-06-02T00:00:00'),

-- PLANT-04, Line-A, Night (temp)
('R-079', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:00:00', 46.00,  20, 0, 0,  '2024-06-02T00:00:00'),
('R-080', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:15:00', 46.50,  21, 0, 0,  '2024-06-02T00:00:00'),
('R-081', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:30:00', 46.20,  20, 1, 0,  '2024-06-02T00:00:00'),
('R-082', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:45:00', 46.80,  21, 0, 0,  '2024-06-02T00:00:00'),
('R-083', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T23:00:00', 46.00,  20, 0, 0,  '2024-06-02T00:00:00'),
('R-084', 'SEN-P4LA-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T23:15:00', 47.00,  22, 0, 0,  '2024-06-02T00:00:00'),

-- PLANT-02, Line-A, Night (temp)
('R-085', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:00:00', 53.00,  13, 0, 0,  '2024-06-02T00:00:00'),
('R-086', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:15:00', 53.50,  14, 0, 0,  '2024-06-02T00:00:00'),
('R-087', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:30:00', 53.20,  13, 1, 0,  '2024-06-02T00:00:00'),
('R-088', 'SEN-P2LA-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:45:00', 53.80,  14, 0, 10, '2024-06-02T00:00:00'),

-- PLANT-03, Line-B, Night (pressure sensor)
('R-089', 'SEN-P3LB-PRES', 'PLANT-03', 'Line-B', '2024-06-01T22:00:00', 4.80,   34, 0, 0,  '2024-06-02T00:00:00'),
('R-090', 'SEN-P3LB-PRES', 'PLANT-03', 'Line-B', '2024-06-01T22:15:00', 4.90,   35, 0, 0,  '2024-06-02T00:00:00');

ASSERT ROW_COUNT = 90
SELECT COUNT(*) AS row_count FROM mfg.bronze.raw_readings;
