-- =============================================================================
-- Manufacturing IoT Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw IoT sensor readings and reference data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Validated readings with anomaly detection';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for OEE and production analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sensors (
    sensor_id         STRING      NOT NULL,
    sensor_type       STRING      NOT NULL,
    manufacturer      STRING,
    install_date      DATE,
    calibration_date  DATE,
    threshold_min     DECIMAL(8,2),
    threshold_max     DECIMAL(8,2),
    plant_id          STRING,
    line_name         STRING,
    ingested_at       TIMESTAMP
) LOCATION '{{data_path}}/manufacturing/bronze/raw_sensors';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_production_lines (
    line_id              STRING      NOT NULL,
    plant_id             STRING      NOT NULL,
    line_name            STRING,
    product_type         STRING,
    capacity_units_per_hour INT,
    ingested_at          TIMESTAMP
) LOCATION '{{data_path}}/manufacturing/bronze/raw_production_lines';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_shifts (
    shift_id     STRING      NOT NULL,
    shift_name   STRING      NOT NULL,
    start_hour   INT,
    end_hour     INT,
    supervisor   STRING,
    ingested_at  TIMESTAMP
) LOCATION '{{data_path}}/manufacturing/bronze/raw_shifts';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sensor_readings (
    reading_id     STRING      NOT NULL,
    sensor_id      STRING      NOT NULL,
    plant_id       STRING      NOT NULL,
    line_name      STRING      NOT NULL,
    reading_time   TIMESTAMP   NOT NULL,
    temperature_c  DECIMAL(8,2),
    pressure_bar   DECIMAL(8,2),
    vibration_hz   DECIMAL(8,2),
    rpm            DECIMAL(8,2),
    units_produced INT,
    defect_units   INT,
    downtime_min   INT         DEFAULT 0,
    ingested_at    TIMESTAMP,
    CHECK (temperature_c BETWEEN -50 AND 500),
    CHECK (pressure_bar BETWEEN 0 AND 100)
) LOCATION '{{data_path}}/manufacturing/bronze/raw_sensor_readings';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.readings_validated (
    reading_id       STRING      NOT NULL,
    sensor_id        STRING,
    sensor_type      STRING,
    plant_id         STRING,
    line_name        STRING,
    reading_time     TIMESTAMP,
    shift_id         STRING,
    temperature_c    DECIMAL(8,2),
    pressure_bar     DECIMAL(8,2),
    vibration_hz     DECIMAL(8,2),
    rpm              DECIMAL(8,2),
    temp_moving_avg  DECIMAL(8,2),
    press_moving_avg DECIMAL(8,2),
    vib_moving_avg   DECIMAL(8,2),
    rpm_moving_avg   DECIMAL(8,2),
    quality_score    DECIMAL(5,2),
    anomaly_flag     BOOLEAN     DEFAULT false,
    anomaly_reason   STRING,
    units_produced   INT,
    defect_units     INT,
    downtime_min     INT,
    validated_at     TIMESTAMP
) LOCATION '{{data_path}}/manufacturing/silver/readings_validated';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_sensor (
    sensor_key       STRING      NOT NULL,
    sensor_id        STRING,
    sensor_type      STRING,
    manufacturer     STRING,
    install_date     DATE,
    calibration_date DATE,
    threshold_min    DECIMAL(8,2),
    threshold_max    DECIMAL(8,2)
) LOCATION '{{data_path}}/manufacturing/gold/dim_sensor';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_production_line (
    line_key                STRING      NOT NULL,
    plant_id                STRING,
    line_name               STRING,
    product_type            STRING,
    capacity_units_per_hour INT
) LOCATION '{{data_path}}/manufacturing/gold/dim_production_line';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_shift (
    shift_key    STRING      NOT NULL,
    shift_name   STRING,
    start_hour   INT,
    end_hour     INT,
    supervisor   STRING
) LOCATION '{{data_path}}/manufacturing/gold/dim_shift';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_sensor_readings (
    reading_key    STRING      NOT NULL,
    sensor_key     STRING,
    line_key       STRING,
    shift_key      STRING,
    reading_time   TIMESTAMP,
    temperature_c  DECIMAL(8,2),
    pressure_bar   DECIMAL(8,2),
    vibration_hz   DECIMAL(8,2),
    rpm            DECIMAL(8,2),
    quality_score  DECIMAL(5,2),
    anomaly_flag   BOOLEAN
) LOCATION '{{data_path}}/manufacturing/gold/fact_sensor_readings';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_oee (
    plant_id          STRING,
    line_name         STRING,
    shift_date        DATE,
    shift_name        STRING,
    availability_pct  DECIMAL(5,2),
    performance_pct   DECIMAL(5,2),
    quality_pct       DECIMAL(5,2),
    oee_pct           DECIMAL(5,2),
    total_units       INT,
    defect_units      INT,
    downtime_minutes  INT
) LOCATION '{{data_path}}/manufacturing/gold/kpi_oee';

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sensors TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_production_lines TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_shifts TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sensor_readings TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.readings_validated TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_sensor TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_production_line TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_shift TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_sensor_readings TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_oee TO USER {{current_user}};

-- ===================== SEED DATA: SENSORS (16 rows) =====================
-- 4 plants x 3 lines x 4 sensor types (not all combos - 16 sensors)

INSERT INTO {{zone_prefix}}.bronze.raw_sensors VALUES
('SEN-P1L1-TEMP', 'temperature', 'ThermoTech',  '2023-01-15', '2024-05-01', 15.00, 85.00,  'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1L1-PRES', 'pressure',    'PressurePro', '2023-01-15', '2024-05-01',  2.00, 12.00,  'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1L1-VIB',  'vibration',   'VibraSense',  '2023-03-20', '2024-04-15', 10.00, 60.00,  'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1L1-RPM',  'rpm',         'RotorMax',    '2023-03-20', '2024-04-15', 800.00, 3200.00,'PLANT-01', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P1L2-TEMP', 'temperature', 'ThermoTech',  '2023-02-10', '2024-05-10', 15.00, 85.00,  'PLANT-01', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P1L2-PRES', 'pressure',    'PressurePro', '2023-02-10', '2024-05-10',  2.00, 12.00,  'PLANT-01', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P2L1-TEMP', 'temperature', 'ThermoTech',  '2023-04-05', '2024-06-01', 20.00, 90.00,  'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2L1-PRES', 'pressure',    'PressurePro', '2023-04-05', '2024-06-01',  3.00, 15.00,  'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2L1-VIB',  'vibration',   'VibraSense',  '2023-04-05', '2024-06-01', 12.00, 65.00,  'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P2L1-RPM',  'rpm',         'RotorMax',    '2023-04-05', '2024-06-01', 900.00, 3500.00,'PLANT-02', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3L1-TEMP', 'temperature', 'ThermoTech',  '2023-06-15', '2024-03-20', 10.00, 80.00,  'PLANT-03', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3L1-VIB',  'vibration',   'VibraSense',  '2023-06-15', '2024-03-20',  8.00, 55.00,  'PLANT-03', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P3L2-TEMP', 'temperature', 'ThermoTech',  '2023-07-01', '2024-04-01', 10.00, 80.00,  'PLANT-03', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P3L2-PRES', 'pressure',    'PressurePro', '2023-07-01', '2024-04-01',  2.50, 14.00,  'PLANT-03', 'Line-B', '2024-06-01T00:00:00'),
('SEN-P4L1-TEMP', 'temperature', 'ThermoTech',  '2023-09-10', '2024-05-15', 18.00, 88.00,  'PLANT-04', 'Line-A', '2024-06-01T00:00:00'),
('SEN-P4L1-RPM',  'rpm',         'RotorMax',    '2023-09-10', '2024-05-15', 750.00, 3000.00,'PLANT-04', 'Line-A', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sensors;


-- ===================== SEED DATA: PRODUCTION LINES (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_production_lines VALUES
('PL-P1-LA', 'PLANT-01', 'Line-A', 'Automotive Parts',   120, '2024-06-01T00:00:00'),
('PL-P1-LB', 'PLANT-01', 'Line-B', 'Automotive Parts',   100, '2024-06-01T00:00:00'),
('PL-P1-LC', 'PLANT-01', 'Line-C', 'Electronics Boards',  80, '2024-06-01T00:00:00'),
('PL-P2-LA', 'PLANT-02', 'Line-A', 'Aerospace Components',60, '2024-06-01T00:00:00'),
('PL-P2-LB', 'PLANT-02', 'Line-B', 'Aerospace Components',55, '2024-06-01T00:00:00'),
('PL-P3-LA', 'PLANT-03', 'Line-A', 'Consumer Goods',     150, '2024-06-01T00:00:00'),
('PL-P3-LB', 'PLANT-03', 'Line-B', 'Consumer Goods',     140, '2024-06-01T00:00:00'),
('PL-P4-LA', 'PLANT-04', 'Line-A', 'Medical Devices',     90, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_production_lines;


-- ===================== SEED DATA: SHIFTS (3 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_shifts VALUES
('SHIFT-AM',   'Morning',   6, 14, 'Sarah Chen',    '2024-06-01T00:00:00'),
('SHIFT-PM',   'Afternoon', 14, 22, 'Mike Johnson',  '2024-06-01T00:00:00'),
('SHIFT-NIGHT','Night',     22,  6, 'Lisa Rodriguez','2024-06-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_shifts;


-- ===================== SEED DATA: SENSOR READINGS (80 rows) =====================
-- Readings every ~15 min across 4 plants, multiple lines, 3 shifts.
-- Includes anomalous readings (out of threshold) for anomaly detection.

INSERT INTO {{zone_prefix}}.bronze.raw_sensor_readings VALUES
-- PLANT-01, Line-A, Morning shift (6am-2pm), June 1
('R-001', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:00:00', 42.5,  6.2, 28.3, 1850.0, 30, 0, 0,  '2024-06-01T08:00:00'),
('R-002', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:15:00', 43.1,  6.3, 29.0, 1860.0, 31, 1, 0,  '2024-06-01T08:00:00'),
('R-003', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:30:00', 44.0,  6.1, 27.5, 1870.0, 29, 0, 0,  '2024-06-01T08:00:00'),
('R-004', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T06:45:00', 43.8,  6.4, 28.8, 1855.0, 30, 0, 0,  '2024-06-01T08:00:00'),
('R-005', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:00:00', 88.5,  6.2, 28.0, 1840.0, 28, 2, 0,  '2024-06-01T08:00:00'),
('R-006', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:15:00', 45.2,  6.5, 29.5, 1880.0, 31, 0, 0,  '2024-06-01T08:00:00'),
('R-007', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:30:00', 44.8,  6.3, 28.2, 1865.0, 30, 0, 0,  '2024-06-01T08:00:00'),
('R-008', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T07:45:00', 43.5,  6.1, 27.8, 1850.0, 29, 1, 0,  '2024-06-01T08:00:00'),
('R-009', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T08:00:00', 44.2,  6.4, 28.5, 1875.0, 31, 0, 0,  '2024-06-01T08:00:00'),
('R-010', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T08:15:00', 44.0,  6.2, 28.1, 1860.0, 30, 0, 15, '2024-06-01T10:00:00'),

-- PLANT-01, Line-B, Morning shift
('R-011', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:00:00', 38.2,  5.8, 25.0, 1720.0, 25, 0, 0,  '2024-06-01T08:00:00'),
('R-012', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:15:00', 38.5,  5.9, 25.3, 1730.0, 24, 1, 0,  '2024-06-01T08:00:00'),
('R-013', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:30:00', 39.0,  5.7, 24.8, 1715.0, 25, 0, 0,  '2024-06-01T08:00:00'),
('R-014', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T06:45:00', 38.8,  5.8, 25.1, 1725.0, 26, 0, 0,  '2024-06-01T08:00:00'),
('R-015', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:00:00', 39.2,  5.9, 25.5, 1740.0, 25, 0, 0,  '2024-06-01T08:00:00'),
('R-016', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:15:00', 38.0, 13.5, 24.5, 1700.0, 23, 2, 0,  '2024-06-01T08:00:00'),
('R-017', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:30:00', 38.6,  5.8, 25.2, 1735.0, 26, 0, 0,  '2024-06-01T08:00:00'),
('R-018', 'SEN-P1L2-TEMP', 'PLANT-01', 'Line-B', '2024-06-01T07:45:00', 39.1,  5.7, 25.0, 1720.0, 25, 0, 10, '2024-06-01T08:00:00'),

-- PLANT-02, Line-A, Morning shift
('R-019', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:00:00', 55.0, 8.5, 35.0, 2200.0, 15, 0, 0,  '2024-06-01T08:00:00'),
('R-020', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:15:00', 55.8, 8.7, 35.5, 2210.0, 14, 0, 0,  '2024-06-01T08:00:00'),
('R-021', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:30:00', 56.2, 8.4, 34.8, 2195.0, 15, 1, 0,  '2024-06-01T08:00:00'),
('R-022', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T06:45:00', 55.5, 8.6, 35.2, 2205.0, 14, 0, 0,  '2024-06-01T08:00:00'),
('R-023', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:00:00', 94.0, 8.5, 35.0, 2200.0, 13, 2, 0,  '2024-06-01T08:00:00'),
('R-024', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:15:00', 56.0, 8.8, 36.0, 2220.0, 15, 0, 0,  '2024-06-01T08:00:00'),
('R-025', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:30:00', 55.3, 8.3, 34.5, 2190.0, 14, 0, 0,  '2024-06-01T08:00:00'),
('R-026', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T07:45:00', 55.7, 8.6, 35.3, 2208.0, 15, 0, 0,  '2024-06-01T08:00:00'),
('R-027', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T08:00:00', 56.1, 8.5, 35.1, 2202.0, 14, 1, 5,  '2024-06-01T10:00:00'),
('R-028', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T08:15:00', 55.9, 8.7, 35.4, 2215.0, 15, 0, 0,  '2024-06-01T10:00:00'),

-- PLANT-03, Line-A, Morning shift
('R-029', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:00:00', 32.0, 4.5, 20.0, 1500.0, 38, 1, 0,  '2024-06-01T08:00:00'),
('R-030', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:15:00', 32.5, 4.6, 20.3, 1510.0, 37, 0, 0,  '2024-06-01T08:00:00'),
('R-031', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:30:00', 33.0, 4.4, 19.8, 1495.0, 38, 0, 0,  '2024-06-01T08:00:00'),
('R-032', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T06:45:00', 32.8, 4.5, 20.1, 1505.0, 37, 1, 0,  '2024-06-01T08:00:00'),
('R-033', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:00:00', 32.2, 4.7, 20.5, 1520.0, 39, 0, 0,  '2024-06-01T08:00:00'),
('R-034', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:15:00', 33.5, 4.3, 68.0, 1490.0, 36, 2, 0,  '2024-06-01T08:00:00'),
('R-035', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:30:00', 32.6, 4.6, 20.2, 1508.0, 38, 0, 0,  '2024-06-01T08:00:00'),
('R-036', 'SEN-P3L1-TEMP', 'PLANT-03', 'Line-A', '2024-06-01T07:45:00', 33.1, 4.5, 20.0, 1500.0, 37, 0, 0,  '2024-06-01T08:00:00'),

-- PLANT-04, Line-A, Morning shift
('R-037', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:00:00', 48.0, 7.2, 30.0, 2050.0, 22, 0, 0,  '2024-06-01T08:00:00'),
('R-038', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:15:00', 48.5, 7.3, 30.5, 2060.0, 23, 0, 0,  '2024-06-01T08:00:00'),
('R-039', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:30:00', 49.0, 7.1, 29.8, 2040.0, 22, 1, 0,  '2024-06-01T08:00:00'),
('R-040', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T06:45:00', 48.2, 7.4, 30.2, 2055.0, 23, 0, 0,  '2024-06-01T08:00:00'),
('R-041', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:00:00', 48.8, 7.2, 30.0, 3100.0, 21, 0, 0,  '2024-06-01T08:00:00'),
('R-042', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:15:00', 49.5, 7.5, 31.0, 2070.0, 23, 0, 0,  '2024-06-01T08:00:00'),
('R-043', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:30:00', 48.0, 7.0, 29.5, 2035.0, 22, 1, 0,  '2024-06-01T08:00:00'),
('R-044', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T07:45:00', 48.6, 7.3, 30.3, 2058.0, 23, 0, 5,  '2024-06-01T08:00:00'),

-- AFTERNOON SHIFT (2pm-10pm), June 1
-- PLANT-01, Line-A
('R-045', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:00:00', 45.0, 6.5, 29.0, 1890.0, 32, 0, 0,  '2024-06-01T16:00:00'),
('R-046', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:15:00', 45.3, 6.6, 29.3, 1895.0, 31, 1, 0,  '2024-06-01T16:00:00'),
('R-047', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:30:00', 44.8, 6.4, 28.8, 1880.0, 32, 0, 0,  '2024-06-01T16:00:00'),
('R-048', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T14:45:00', 45.5, 6.7, 29.5, 1900.0, 30, 0, 0,  '2024-06-01T16:00:00'),
('R-049', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:00:00', 45.1, 6.5, 29.1, 1888.0, 31, 0, 0,  '2024-06-01T16:00:00'),
('R-050', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:15:00', 44.5, 6.3, 28.5, 1870.0, 32, 0, 0,  '2024-06-01T16:00:00'),
('R-051', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:30:00', 45.8, 6.8, 29.8, 1910.0, 30, 1, 0,  '2024-06-01T16:00:00'),
('R-052', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T15:45:00', 45.2, 6.5, 29.2, 1892.0, 31, 0, 0,  '2024-06-01T16:00:00'),

-- PLANT-02, Line-A, Afternoon
('R-053', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:00:00', 56.5, 8.8, 36.0, 2225.0, 15, 0, 0,  '2024-06-01T16:00:00'),
('R-054', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:15:00', 57.0, 8.9, 36.5, 2230.0, 14, 0, 0,  '2024-06-01T16:00:00'),
('R-055', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:30:00', 56.8, 8.7, 35.8, 2218.0, 15, 1, 0,  '2024-06-01T16:00:00'),
('R-056', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T14:45:00', 57.2, 9.0, 36.2, 2235.0, 14, 0, 0,  '2024-06-01T16:00:00'),
('R-057', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T15:00:00', 56.3, 8.6, 35.5, 2210.0, 15, 0, 0,  '2024-06-01T16:00:00'),
('R-058', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T15:15:00', 56.9, 8.8, 36.1, 2222.0, 14, 0, 0,  '2024-06-01T16:00:00'),

-- PLANT-03, Line-B, Afternoon
('R-059', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:00:00', 34.0, 4.8, 22.0, 1600.0, 35, 0, 0,  '2024-06-01T16:00:00'),
('R-060', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:15:00', 34.5, 4.9, 22.3, 1610.0, 34, 1, 0,  '2024-06-01T16:00:00'),
('R-061', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:30:00', 35.0, 4.7, 21.8, 1595.0, 35, 0, 0,  '2024-06-01T16:00:00'),
('R-062', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T14:45:00', 34.2, 4.8, 22.1, 1605.0, 36, 0, 0,  '2024-06-01T16:00:00'),
('R-063', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T15:00:00', 85.0, 4.9, 22.5, 1615.0, 33, 3, 0,  '2024-06-01T16:00:00'),
('R-064', 'SEN-P3L2-TEMP', 'PLANT-03', 'Line-B', '2024-06-01T15:15:00', 34.8, 4.7, 21.9, 1598.0, 35, 0, 0,  '2024-06-01T16:00:00'),

-- NIGHT SHIFT (10pm-6am), June 1->2
-- PLANT-01, Line-A
('R-065', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:00:00', 42.0, 6.0, 27.0, 1830.0, 28, 0, 0,  '2024-06-02T00:00:00'),
('R-066', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:15:00', 42.3, 6.1, 27.3, 1835.0, 29, 0, 0,  '2024-06-02T00:00:00'),
('R-067', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:30:00', 41.8, 5.9, 26.8, 1825.0, 28, 1, 0,  '2024-06-02T00:00:00'),
('R-068', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T22:45:00', 42.5, 6.2, 27.5, 1840.0, 29, 0, 0,  '2024-06-02T00:00:00'),
('R-069', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T23:00:00', 42.0, 6.0, 27.0, 1830.0, 27, 0, 20, '2024-06-02T00:00:00'),
('R-070', 'SEN-P1L1-TEMP', 'PLANT-01', 'Line-A', '2024-06-01T23:15:00', 41.5, 5.8, 26.5, 1820.0, 28, 0, 0,  '2024-06-02T00:00:00'),

-- PLANT-04, Line-A, Night
('R-071', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:00:00', 46.0, 6.8, 28.5, 1980.0, 20, 0, 0,  '2024-06-02T00:00:00'),
('R-072', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:15:00', 46.5, 6.9, 28.8, 1990.0, 21, 0, 0,  '2024-06-02T00:00:00'),
('R-073', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:30:00', 46.2, 6.7, 28.3, 1975.0, 20, 1, 0,  '2024-06-02T00:00:00'),
('R-074', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T22:45:00', 46.8, 7.0, 29.0, 2000.0, 21, 0, 0,  '2024-06-02T00:00:00'),
('R-075', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T23:00:00', 46.0, 6.8, 28.5, 1985.0, 20, 0, 0,  '2024-06-02T00:00:00'),
('R-076', 'SEN-P4L1-TEMP', 'PLANT-04', 'Line-A', '2024-06-01T23:15:00', 47.0, 7.1, 29.2, 2010.0, 22, 0, 0,  '2024-06-02T00:00:00'),

-- PLANT-02, Line-A, Night
('R-077', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:00:00', 53.0, 8.0, 33.0, 2150.0, 13, 0, 0,  '2024-06-02T00:00:00'),
('R-078', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:15:00', 53.5, 8.1, 33.5, 2160.0, 14, 0, 0,  '2024-06-02T00:00:00'),
('R-079', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:30:00', 53.2, 7.9, 32.8, 2145.0, 13, 1, 0,  '2024-06-02T00:00:00'),
('R-080', 'SEN-P2L1-TEMP', 'PLANT-02', 'Line-A', '2024-06-01T22:45:00', 53.8, 8.2, 33.8, 2170.0, 14, 0, 10, '2024-06-02T00:00:00');

ASSERT ROW_COUNT = 80
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sensor_readings;

