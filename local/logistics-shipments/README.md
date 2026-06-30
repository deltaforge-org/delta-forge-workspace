# Logistics Shipments Pipeline

## Scenario

A global logistics company tracks shipments across 8 carriers and 15 hub locations. Tracking events arrive from multiple carrier systems, often duplicated (15 exact duplicates in seed data) and sometimes out of order (5 events with timestamps arriving non-sequentially). The pipeline implements event sourcing with idempotent composite-key deduplication, reconstructs shipment timelines using window functions, detects SLA violations against carrier contracts, and enables route optimization via Z-ordering.

Schema evolution is demonstrated by adding a `customs_cleared` column during incremental processing as the company expands to cross-border shipments. Time travel via CDF supports dispute resolution for exception shipments.

## Star Schema

```
+------------------+     +--------------------+     +------------------+
| dim_carrier      |     | fact_shipments     |     | dim_location     |
|------------------|     |--------------------|     |------------------|
| carrier_key   PK |<----| shipment_key    PK |---->| location_key  PK |
| carrier_name     |     | carrier_key     FK |     | hub_name         |
| carrier_type     |     | origin_key      FK |     | city / state     |
| fleet_size       |     | destination_key FK |     | region           |
| headquarters     |     | customer_key    FK |     | hub_type         |
| on_time_rating   |     | route_key       FK |     | lat / lon        |
| cost_per_kg      |     | service_level      |     +------------------+
+------------------+     | ship/delivery/     |
                         |   promised_date    |     +------------------+
+------------------+     | weight / volume    |     | dim_customer     |
| dim_route        |     | cost / revenue     |     |------------------|
|------------------|     | margin             |     | customer_key  PK |
| route_key     PK |<----| on_time_flag       |---->| customer_name    |
| origin_hub       |     | transit_days       |     | tier / industry  |
| destination_hub  |     | event_count        |     | city / country   |
| distance_km      |     | sla_violated       |     | account_manager  |
| avg_transit_days |     | penalty_amount     |     +------------------+
| shipment_count   |     +--------------------+
| primary_mode     |
+------------------+     +---------------------------+     +---------------------------+
                         | kpi_delivery_performance  |     | kpi_sla_compliance        |
                         |---------------------------|     |---------------------------|
                         | carrier / route / month   |     | carrier / service / month |
                         | total_shipments           |     | violated_count            |
                         | on_time_count / pct       |     | violation_rate            |
                         | avg_transit_days           |     | total_penalty             |
                         | avg_cost_per_kg            |     | worst_violation           |
                         | total_margin               |     +---------------------------+
                         +---------------------------+
```

## Medallion Flow

```
BRONZE                          SILVER                              GOLD
+---------------------+   +------------------------+   +--------------------+
| raw_tracking_events |-->| events_deduped (CDF)   |-->| fact_shipments     |
| (80 rows, 15 dupes, |   | (composite-key dedup)  |   | dim_carrier        |
|  5 out-of-order)    |   +------------------------+   | dim_location       |
+---------------------+   | shipment_status        |   | dim_customer       |
| raw_carriers (8)    |   | (timeline reconstruct) |   | dim_route          |
| raw_locations (15)  |   +------------------------+   | kpi_delivery_perf  |
| raw_customers (20)  |   | sla_violations         |   | kpi_sla_compliance |
| raw_sla_contracts(8)|   | (contract comparison)  |   +--------------------+
+---------------------+   +------------------------+
```

## Pipeline DAG

```
validate_bronze
       |
  dedup_events  <-- idempotent composite-key MERGE
       |
  +----+--------------------+
reconstruct_timelines    detect_sla_violations  <-- parallel
       |                        |
  +----+----------+             |
dim_carrier  dim_location  dim_customer  <-- parallel
       |        |              |
       +--------+--------------+
                |
    build_dim_route  <-- derived from location pairs
                |
    build_fact_shipments
                |
    +-----------+-----------+
kpi_delivery  kpi_sla_compliance  <-- parallel
    |                  |
    +--------+---------+
  optimize_zorder (CONTINUE ON FAILURE)  <-- Z-ORDER BY (origin, dest)
```

## Features

- **Idempotent composite-key MERGE**: Dedup on (shipment_id + event_type + event_timestamp); same event arriving 3 times produces exactly one row
- **Timeline reconstruction**: ROW_NUMBER + LAG window functions rebuild shipment lifecycle from individual events
- **SLA violation detection**: Compare actual transit vs contracted max_transit_days with penalty calculation
- **Schema evolution**: `customs_cleared` BOOLEAN column added during incremental load
- **Z-ordering**: Route-based query optimization for origin+destination lookups
- **CDF (Change Data Feed)**: Enabled on events_deduped for time travel dispute resolution
- **Out-of-order handling**: Events with non-sequential timestamps correctly ordered by event_timestamp
- **Star schema**: Full dimensional model with carrier, location, customer, route dimensions
- **Margin analysis**: Revenue - cost calculated per shipment and aggregated by KPI

## Seed Data

- **8 carriers** across 6 types (LTL, Express, Ocean, Rail, Air, Regional)
- **15 locations** with hub types (distribution, port, cross-dock, warehouse)
- **20 customers** across 3 tiers and 10 industries
- **8 SLA contracts** (carrier x service_level with penalty rates)
- **80 tracking events** for 25 shipments spanning April-May 2024
- **15 exact duplicates** (same composite key)
- **5 out-of-order events** (delivered before in_transit timestamps)
- **3 SLA violations** (S005, S011, S014)
- **2 exceptions** (S019 lost, S024 damaged)

## Verification Checklist

- [ ] 80 raw events deduplicated to 65 unique events
- [ ] All 8 carriers in dim_carrier
- [ ] All 15 locations in dim_location
- [ ] All 20 customers in dim_customer
- [ ] Routes derived with Haversine distance estimates
- [ ] At least 3 SLA violations detected with penalty amounts
- [ ] Late deliveries identified with days_late calculation
- [ ] Idempotent: re-running MERGE produces same results
- [ ] Schema evolution: customs_cleared column accepted
- [ ] Out-of-order events correctly sequenced
- [ ] No orphaned foreign keys in fact table
- [ ] CDF enabled for dispute resolution time travel
