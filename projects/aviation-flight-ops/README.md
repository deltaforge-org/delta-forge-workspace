# Aviation Flight Operations Pipeline

## Scenario

An airline operates 10 routes (7 domestic, 3 international) with a fleet of 6 aircraft (Boeing 737/787/777, Airbus A320neo/A321XLR, Embraer E175) and 5 crew teams based at major US hubs. The pipeline tracks flight operations including delays (weather, mechanical, ATC, crew), cancellations, passenger loads, fuel consumption, and revenue. MERGE on composite key (flight_number + departure_date) handles flight updates as operational data evolves. Time travel enables comparison of schedule versions for audit and planning.

## Star Schema

```
                    +----------------+
                    | dim_crew       |
                    |----------------|
                    | crew_key       |<------+
                    | captain_name   |       |
                    | first_officer  |       |
                    | crew_base      |       |
                    +----------------+       |
                                             |
+----------------+  +-------------------+    |    +----------------+
| dim_route      |  | fact_flights      |    |    | dim_date       |
|----------------|  |-------------------|    |    |----------------|
| route_key      |<-| flight_key        |    +--->| date_key       |
| origin_iata    |  | route_key      FK |         | full_date      |
| dest_iata      |  | aircraft_key   FK |         | day_of_week    |
| distance_nm    |  | crew_key       FK |         | is_peak_season |
| domestic_flag  |  | date_key       FK |         +----------------+
+----------------+  | flight_number     |
                    | delay_minutes     |    +----------------+
                    | delay_reason      |    | dim_aircraft   |
                    | pax_count         |    |----------------|
                    | fuel_consumed_kg  |--->| aircraft_key   |
                    | revenue           |    | aircraft_type  |
                    +-------------------+    | seat_capacity  |
                                             | fuel_efficiency|
                    +------------------------+ +----------------+
                    | kpi_otp               |
                    |------------------------|
                    | route, month          |
                    | otp_pct, avg_delay    |
                    | revenue_per_asm       |
                    | load_factor_pct       |
                    +------------------------+
```

## Medallion Flow

```
BRONZE                      SILVER                          GOLD
+-----------------+    +-------------------------+    +---------------------+
| raw_flight_ops  |--->| flights_enriched        |--->| fact_flights        |
| (62+ flights)   |    | (composite key MERGE)   |    | dim_route           |
+-----------------+    | (delay_cat, load_factor)|    | dim_aircraft        |
| raw_routes      |    +-------------------------+    | dim_crew            |
+-----------------+                                   | dim_date            |
| raw_aircraft    |                                   | kpi_otp             |
+-----------------+                                   +---------------------+
| raw_crew        |
+-----------------+
```

## Features

- **Composite key MERGE**: Flight updates merged on (flight_number, departure_date)
- **Z-ordering**: fact_flights optimized on route_key for range queries
- **Time travel**: Compare schedule versions across table versions
- **OTP analysis**: On-time performance by route and month
- **Delay Pareto**: Cumulative delay analysis by cause (weather, ATC, mechanical, crew)
- **Route profitability**: Revenue per Available Seat Mile (ASM)
- **Fuel efficiency**: Fuel per passenger-nautical mile tracking
- **Pseudonymisation**: MASK on captain_name and first_officer_name

## Seed Data

- **10 routes**: 7 domestic (JFK-LAX, ORD-MIA, etc.) + 3 international (JFK-LHR, LAX-NRT, ORD-CDG)
- **6 aircraft**: Boeing 737/787/777, Airbus A320neo/A321XLR, Embraer E175
- **5 crew teams** based at JFK, ORD, ATL, LAX, SFO
- **62 flight operations** across Jan-Feb 2024 with 3 cancellations, delays, and diversions

## Verification Checklist

- [ ] All 10 routes in dim_route with distance and block time
- [ ] All 6 aircraft in dim_aircraft
- [ ] All 5 crew teams in dim_crew with masked names
- [ ] 3 cancellations identified (weather, mechanical)
- [ ] OTP percentage calculated per route
- [ ] Delay Pareto shows cumulative percentages
- [ ] Revenue per ASM ranking across routes
- [ ] Fuel efficiency varies by aircraft type
- [ ] Incremental load adds March flights via INCREMENTAL_FILTER
- [ ] Time travel shows previous version count
