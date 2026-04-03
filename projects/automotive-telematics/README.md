# Automotive Telematics Pipeline

## Scenario

A fleet management company operates 10 vehicles (trucks, vans, sedans) across 3 regional fleets (East, Central, West). The telematics pipeline ingests real-time trip data including GPS telemetry, fuel consumption, harsh driving events, engine diagnostics, and tire pressure readings. It calculates trip-level safety scores, fuel efficiency metrics, and predictive maintenance indicators.

The system identifies unsafe driving patterns (harsh braking/acceleration, excessive idling), tracks fuel efficiency by vehicle type and route, and predicts maintenance needs based on odometer trends. One vehicle (Ford F-150) has been decommissioned, demonstrating deletion vector handling.

## Star Schema

```
+------------------+     +------------------+     +------------------+
| dim_vehicle      |     | dim_driver       |     | dim_route        |
|------------------|     |------------------|     |------------------|
| vehicle_key      |<-+  | driver_key       |<-+  | route_key        |<-+
| vin (generalized)|  |  | driver_id        |  |  | route_name       |  |
| vehicle_type     |  |  | name             |  |  | origin_city      |  |
| make / model     |  |  | license_class    |  |  | dest_city        |  |
| fuel_type        |  |  | training_level   |  |  | distance_km      |  |
| odometer_km      |  |  | certifications   |  |  | road_type        |  |
| next_service_km  |  |  +------------------+  |  | traffic_index    |  |
| fleet_id         |  |                        |  +------------------+  |
+------------------+  |  +--------------------+ |                       |
                      |  | fact_trips         | |                       |
                      +--| trip_key           | |                       |
                         | vehicle_key     FK |-+                       |
                         | driver_key      FK |-------------------------+
                         | route_key       FK |
                         | trip_date          |
                         | distance_km        |
                         | duration_min       |
                         | fuel_consumed_l    |
                         | fuel_efficiency    |
                         | harsh_brake_count  |
                         | harsh_accel_count  |
                         | idle_time_min      |
                         | safety_score       |
                         +--------------------+
```

## Medallion Flow

```
BRONZE                       SILVER                          GOLD
+------------------+    +------------------------+    +--------------------+
| raw_trips        |--->| trips_enriched (CDF)   |--->| fact_trips         |
| (70+ records)    |    | - fuel efficiency calc  |    | dim_vehicle        |
+------------------+    | - safety score          |    | dim_driver         |
| raw_vehicles     |--->| - idle % derivation     |    | dim_route          |
| (10 vehicles)    |    | - maintenance flags     |    | kpi_fleet_perf     |
+------------------+    | - decommission filter   |    +--------------------+
| raw_drivers      |    +------------------------+
| (8 drivers)      |
+------------------+
| raw_routes       |
| (6 routes)       |
+------------------+
```

## Features

- **Partitioning**: Trips partitioned by vehicle_type + month for efficient querying
- **OPTIMIZE compaction**: Applied to silver and gold tables after each load
- **Schema evolution**: Raw trips table supports adding new sensor columns over time
- **Deletion vectors**: Decommissioned vehicle (F-150) trips deleted from silver
- **Safety scoring**: 100 - 5*harsh_brakes - 3*harsh_accels - 2*(idle%*100)
- **Predictive maintenance**: Projected odometer via cumulative distance window function
- **INCREMENTAL_FILTER macro**: Incremental load uses trip_id + trip_date with 3-day overlap
- **Pseudonymisation**: VIN generalized (first 8 chars + XXXXXXXXX mask)

## Seed Data

- **10 vehicles** across 3 fleets (FL-EAST, FL-CENTRAL, FL-WEST)
- **8 drivers** with varying license classes and training levels
- **6 routes** (highway and mixed road types)
- **70 trip records** spanning January-March 2024
- Includes 1 decommissioned vehicle with historical trips
- Harsh driving events vary by driver experience level
- Electric vehicle (Tesla) with 0 fuel consumption

## Verification Checklist

- [ ] All 10 vehicles in dim_vehicle with VIN pseudonymisation
- [ ] All 8 drivers in dim_driver with training levels
- [ ] All 6 routes in dim_route
- [ ] Safety scores calculated correctly (experienced drivers score higher)
- [ ] Fuel efficiency: Electric vehicle excluded from fuel metrics
- [ ] Maintenance due flags for vehicles past service km
- [ ] Decommissioned vehicle trips removed from silver
- [ ] No orphaned foreign keys in fact table
- [ ] KPI dashboard shows fleet utilization and harsh event rates
- [ ] Incremental load processes only new April trips
