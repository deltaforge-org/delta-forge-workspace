# Manufacturing IoT Pipeline

## Scenario

A multi-plant manufacturing operation collects sensor data from production lines to monitor equipment health, detect anomalies, and calculate Overall Equipment Effectiveness (OEE). Four plants produce automotive parts, aerospace components, consumer goods, and medical devices across 8 production lines.

Sensors measure temperature, pressure, vibration, and RPM at 15-minute intervals. The pipeline validates readings against sensor-specific thresholds, computes 5-reading moving averages for signal smoothing, flags anomalies, and produces shift-level OEE metrics using the standard Availability x Performance x Quality formula.

## Star Schema

```
+------------------+     +------------------------+     +---------------------+
| dim_sensor       |     | fact_sensor_readings   |     | dim_production_line |
|------------------|     |------------------------|     |---------------------|
| sensor_key       |<----| reading_key            |---->| line_key            |
| sensor_id        |     | sensor_key          FK |     | plant_id            |
| sensor_type      |     | line_key            FK |     | line_name           |
| manufacturer     |     | shift_key           FK |     | product_type        |
| threshold_min    |     | reading_time           |     | capacity_units/hr   |
| threshold_max    |     | temperature_c          |     +---------------------+
+------------------+     | pressure_bar           |
                         | vibration_hz           |     +------------------+
                         | rpm                    |     | dim_shift        |
                         | quality_score          |     |------------------|
                         | anomaly_flag           |---->| shift_key        |
                         +------------------------+     | shift_name       |
                                                        | start_hour       |
+---------------------------+                           | end_hour         |
| kpi_oee                   |                           | supervisor       |
|---------------------------|                           +------------------+
| plant_id                  |
| line_name                 |
| shift_date                |
| availability_pct          |
| performance_pct           |
| quality_pct               |
| oee_pct                   |
| total_units / defect_units|
| downtime_minutes          |
+---------------------------+
```

## Medallion Flow

```
BRONZE                          SILVER                          GOLD
+---------------------+    +------------------------+    +---------------------+
| raw_sensor_readings |--->| readings_validated     |--->| fact_sensor_readings|
| (CHECK constraints) |    | (moving avg, anomaly   |    | dim_sensor          |
+---------------------+    |  detection, quality)   |    | dim_production_line |
| raw_sensors         |    +------------------------+    | dim_shift           |
| raw_production_lines|                                  | kpi_oee             |
| raw_shifts          |                                  +---------------------+
+---------------------+
```

## Features

- **CHECK constraints**: `temperature_c BETWEEN -50 AND 500`, `pressure_bar BETWEEN 0 AND 100`
- **5-reading moving average**: Smooths temperature, pressure, vibration, RPM signals
- **Anomaly detection**: Readings outside sensor-specific thresholds flagged with reason
- **OEE calculation**: Availability x Performance x Quality per shift per line
- **Predictive maintenance**: LAG/LEAD patterns for trend analysis around anomalies
- **Quality scoring**: Per-reading score based on defect rate
- **OPTIMIZE compaction**: Applied to silver and gold tables
- **Shift assignment**: Automatic based on reading timestamp hour

## Seed Data

- **16 sensors** across 4 plants (temperature, pressure, vibration, RPM types)
- **8 production lines** with capacity data for 4 product types
- **3 shifts** (Morning 6-14, Afternoon 14-22, Night 22-6)
- **80 sensor readings** spanning 3 shifts on June 1, 2024
- Includes 5+ anomalous readings for detection testing

## Verification Checklist

- [ ] All 16 sensors in dim_sensor
- [ ] All 8 production lines in dim_production_line
- [ ] All 3 shifts in dim_shift
- [ ] Anomalies detected for out-of-threshold readings
- [ ] OEE metrics calculated per plant/line/shift
- [ ] Moving averages computed correctly (5-reading window)
- [ ] CHECK constraints prevent invalid readings
- [ ] Quality scores reflect defect rates
- [ ] Predictive maintenance signals via LAG/LEAD
- [ ] OPTIMIZE compaction applied
