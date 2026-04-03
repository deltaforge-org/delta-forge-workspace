# Energy Smart Meters Pipeline

## Scenario

A utility company collects hourly smart meter readings from residential and commercial customers across four regions. The pipeline calculates consumption costs using time-of-use tariffs (peak vs off-peak rates), accounts for solar panel generation (net metering), validates readings against meter capacity, and produces regional billing dashboards.

Key analytics include solar offset percentage (how much solar generation reduces net consumption), peak/off-peak consumption patterns, and cumulative revenue forecasting using window functions.

## Star Schema

```
+------------------+     +------------------------+     +------------------+
| dim_meter        |     | fact_meter_readings    |     | dim_tariff       |
|------------------|     |------------------------|     |------------------|
| meter_key        |<----| reading_key            |---->| tariff_key       |
| meter_id         |     | meter_key           FK |     | tariff_name      |
| meter_type       |     | tariff_key          FK |     | rate_per_kwh     |
| customer_name    |     | region_key          FK |     | peak_rate        |
| address          |     | reading_date           |     | off_peak_rate    |
| solar_panel_flag |     | reading_hour           |     | standing_charge  |
| capacity_kw      |     | kwh_consumed           |     +------------------+
+------------------+     | kwh_generated          |
                         | net_kwh                |     +------------------+
                         | cost                   |     | dim_region       |
                         | peak_flag              |     |------------------|
                         +------------------------+---->| region_key       |
                                                        | region_name      |
+-----------------------------+                         | grid_zone        |
| kpi_consumption_billing     |                         | utility_company  |
|-----------------------------|                         | regulatory_body  |
| region / billing_month      |                         +------------------+
| total_meters                |
| total_kwh / total_generated |
| net_consumption             |
| total_revenue / avg_bill    |
| peak_pct / solar_offset_pct |
+-----------------------------+
```

## Medallion Flow

```
BRONZE                         SILVER                          GOLD
+------------------+     +------------------------+     +----------------------+
| raw_readings     |---->| readings_costed        |---->| fact_meter_readings  |
| (hourly kWh)     |     | (peak/off-peak cost,   |     | dim_meter            |
+------------------+     |  net_kwh, validation)  |     | dim_tariff           |
| raw_meters       |     +------------------------+     | dim_region           |
| raw_tariffs      |                                    | kpi_consumption_     |
| raw_regions      |                                    |   billing            |
+------------------+                                    +----------------------+
```

## Features

- **Time-of-use tariffs**: Peak (7am-10pm) and off-peak (11pm-6am) rate differentiation
- **Solar net metering**: net_kwh = consumed - generated; negative values = energy export
- **Capacity validation**: Readings checked against meter rated capacity
- **VACUUM**: Storage governance with 168-hour retention
- **OPTIMIZE**: Compaction on silver and gold tables
- **Regional billing dashboard**: Consumption, revenue, and solar offset by region
- **Cumulative revenue**: Window functions for revenue forecasting
- **Pseudonymisation**: Customer addresses generalized to city+state only

## Seed Data

- **4 regions** with grid zones and utility companies (Northeast, Southeast, Midwest, West)
- **3 tariff types**: Residential Standard, Commercial, Time-of-Use Dynamic
- **12 meters** across 4 regions, 3 tariffs; 5 with solar panels
- **72 hourly readings** spanning May 2024 (both peak and off-peak hours)
- Solar generation data for meters with panels (hours 8-18)

## Verification Checklist

- [ ] All 12 meters in dim_meter
- [ ] All 3 tariffs in dim_tariff
- [ ] All 4 regions in dim_region
- [ ] Peak/off-peak correctly classified (hour 7-22 = peak)
- [ ] Solar offset calculated for solar-enabled meters
- [ ] Net kWh negative for solar meters during peak generation
- [ ] Cost calculated using correct tariff rates
- [ ] KPI dashboard shows regional billing summaries
- [ ] Capacity validation flags readings exceeding meter capacity
- [ ] VACUUM and OPTIMIZE executed successfully
- [ ] Customer addresses pseudonymised (city+state only)
