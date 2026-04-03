# Telecom CDR Pipeline

## Scenario

A telecommunications provider processes Call Detail Records (CDR) from its network to analyze network quality, subscriber behavior, and revenue patterns. The pipeline ingests raw CDRs from switching equipment, enriches them with subscriber and cell tower metadata, detects dropped calls (voice calls under 10 seconds), and produces network quality dashboards.

A key feature is schema evolution: the bronze CDR table initially lacks a `roaming_flag` column, which is added during incremental processing to demonstrate how the pipeline adapts to new data fields without breaking existing transformations.

## Star Schema

```
+------------------+     +-------------------+     +------------------+
| dim_subscriber   |     | fact_calls        |     | dim_cell_tower   |
|------------------|     |-------------------|     |------------------|
| subscriber_key   |<----| call_key          |---->| tower_key        |
| phone_number     |     | caller_key     FK |     | tower_id         |
| plan_type        |     | callee_key     FK |     | location         |
| plan_tier        |     | cell_tower_key FK |     | city             |
| activation_date  |     | start_time        |     | region           |
| status           |     | duration_sec      |     | technology       |
| monthly_spend    |     | call_type         |     | capacity_mhz     |
+------------------+     | data_usage_mb     |     +------------------+
                         | roaming_flag      |
                         | drop_flag         |
                         | revenue           |
                         +-------------------+

+------------------------+
| kpi_network_quality    |
|------------------------|
| region                 |
| hour_bucket            |
| total_calls            |
| dropped_calls          |
| drop_rate              |
| avg_duration           |
| total_data_mb          |
| peak_concurrent        |
| revenue                |
+------------------------+
```

## Medallion Flow

```
BRONZE                        SILVER                          GOLD
+------------------+    +-----------------------+    +------------------+
| raw_cdr          |--->| cdr_enriched          |--->| fact_calls       |
| (schema evolves) |    | (drop detection,      |    | dim_subscriber   |
+------------------+    |  tower enrichment)    |    | dim_cell_tower   |
| raw_subscribers  |--->| subscriber_profiles   |--->| kpi_network_     |
+------------------+    | (activity aggregation)|    |   quality        |
| raw_cell_towers  |    +-----------------------+    +------------------+
+------------------+
```

## Features

- **Schema evolution**: `roaming_flag` column added to bronze CDR table during incremental load
- **MERGE upsert**: Subscriber profiles updated from CDR activity data
- **Dropped call detection**: Voice calls < 10 seconds flagged as dropped
- **VACUUM**: Storage governance with 168-hour retention on CDR tables
- **OPTIMIZE**: Compaction on silver and gold tables
- **Network quality KPI**: Drop rate by region and hour bucket
- **Churn detection**: Subscriber risk scoring based on activity patterns
- **Pseudonymisation**: Phone numbers generalized (last 4 digits masked)

## Seed Data

- **12 subscribers** across 4 plan tiers (platinum, gold, silver, bronze)
- **8 cell towers** across 4 regions (Northeast, West, Central, South) with 5G/4G technology
- **65 CDR records** spanning 3 days with voice, SMS, and data call types
- Includes 10+ dropped calls (voice < 10s) for network quality analysis

## Verification Checklist

- [ ] All 8 towers in dim_cell_tower
- [ ] All 12 subscribers in dim_subscriber
- [ ] Dropped calls detected (duration < 10s for voice)
- [ ] Network quality KPI shows drop rates by region
- [ ] Schema evolution: roaming_flag column added and populated
- [ ] Subscriber profiles reflect CDR activity
- [ ] VACUUM and OPTIMIZE executed successfully
- [ ] Referential integrity: no orphaned caller keys
