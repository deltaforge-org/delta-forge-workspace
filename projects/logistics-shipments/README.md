# Logistics Shipments Pipeline

## Scenario

A logistics platform tracks shipments across a network of carriers, warehouses, and delivery destinations. Each shipment generates multiple tracking events (picked_up, in_transit, at_hub, out_for_delivery, delivered, exception). The pipeline deduplicates these events idempotently, derives delivery metrics (transit days, on-time flag), and produces carrier performance scorecards and route optimization analytics.

Schema evolution is demonstrated by adding a `tracking_url` column during incremental processing. The gold layer uses Z-ordering concepts for efficient route-based queries.

## Star Schema

```
+------------------+     +--------------------+     +------------------+
| dim_carrier      |     | fact_shipments     |     | dim_location     |
|------------------|     |--------------------|     |------------------|
| carrier_key      |<----| shipment_key       |---->| location_key     |
| carrier_name     |     | carrier_key     FK |     | city             |
| carrier_type     |     | origin_key      FK |     | state            |
| fleet_size       |     | destination_key FK |     | region           |
| on_time_rating   |     | customer_key    FK |     | warehouse_flag   |
| cost_per_kg      |     | ship_date          |     | latitude         |
+------------------+     | delivery_date      |     | longitude        |
                         | promised_date      |     +------------------+
+------------------+     | weight_kg          |
| dim_route        |     | volume_m3          |
|------------------|     | cost               |
| route_key        |     | on_time_flag       |
| origin_city      |     | transit_days       |
| destination_city |     +--------------------+
| distance_km      |
| avg_transit_days |     +---------------------------+
| mode             |     | kpi_delivery_performance  |
+------------------+     |---------------------------|
                         | carrier / route / month   |
                         | total_shipments           |
                         | on_time_count / on_time_% |
                         | avg_transit_days           |
                         | avg_cost_per_kg            |
                         | total_weight / revenue     |
                         +---------------------------+
```

## Medallion Flow

```
BRONZE                          SILVER                         GOLD
+-------------------+     +-----------------------+     +--------------------+
| raw_events        |---->| shipments_deduped     |---->| fact_shipments     |
| (multi-event/ship)|     | (idempotent dedup,    |     | dim_carrier        |
+-------------------+     |  transit_days,        |     | dim_location       |
| raw_carriers      |     |  on_time_flag)        |     | dim_route          |
| raw_locations     |     +-----------------------+     | kpi_delivery_perf  |
| raw_customers     |                                   +--------------------+
+-------------------+
```

## Features

- **Idempotent MERGE**: Dedup on shipment_id; same shipment with newer events updates, duplicates skipped
- **Schema evolution**: `tracking_url` column added during incremental load
- **Z-ordering**: Route-based query optimization for origin+destination lookups
- **Carrier scorecards**: On-time performance vs contracted SLA
- **Route analysis**: Distance, mode, and transit time per route
- **Late delivery analysis**: Days late calculation with carrier attribution
- **SLA compliance**: Per-carrier on-time percentage tracking

## Seed Data

- **6 carriers** across 5 types (LTL, Express, Ocean, Rail, Air, Regional)
- **12 locations** with warehouse flags and coordinates
- **20 customers** across 3 tiers (Enterprise, Mid-Market, SMB)
- **68 tracking events** for 21 shipments spanning April-May 2024
- Includes 3 late deliveries, 2 exceptions, 1 undelivered shipment

## Verification Checklist

- [ ] All 6 carriers in dim_carrier
- [ ] All 12 locations in dim_location
- [ ] Routes derived with distance estimates
- [ ] Late deliveries identified (3 shipments)
- [ ] Idempotent: re-running MERGE produces same results
- [ ] Schema evolution: tracking_url column accepted
- [ ] Carrier SLA compliance calculated
- [ ] Referential integrity: no orphaned foreign keys
