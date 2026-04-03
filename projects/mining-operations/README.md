# Mining Operations Pipeline

## Scenario

A mining conglomerate operates 3 mine sites across Australia (copper), Brazil (iron ore), and Canada (gold). The pipeline tracks extraction operations across 6 pits, 8 pieces of heavy equipment, and 3 daily shifts. It calculates strip ratios, equipment utilization rates, cost per tonne, and cumulative year-to-date ore extraction with reserve depletion tracking.

The system identifies equipment with low utilization (breakdowns), high-cost extractions, and safety incidents. The gold underground mine (Site 3) has a developing shaft (Pit 6) with higher waste ratios, while the iron ore operation (Site 2) has one haul truck under maintenance with degraded performance.

## Star Schema

```
+------------------+     +------------------+     +------------------+
| dim_site         |     | dim_pit          |     | dim_equipment    |
|------------------|     |------------------|     |------------------|
| site_key         |<-+  | pit_key          |<-+  | equipment_key    |<-+
| site_name        |  |  | pit_name         |  |  | equipment_type   |  |
| mineral_type     |  |  | site_id          |  |  | manufacturer     |  |
| country          |  |  | current_depth_m  |  |  | capacity_tonnes  |  |
| reserve_est_mt   |  |  | target_depth_m   |  |  | operating_hours  |  |
| mine_type        |  |  | status           |  |  | maint_status     |  |
| env_rating       |  |  +------------------+  |  +------------------+  |
+------------------+  |                         |                       |
                      |  +--------------------+ |  +------------------+ |
                      |  | fact_extraction    | |  | dim_shift        | |
                      +--| extraction_key     | |  |------------------| |
                         | site_key        FK |-+  | shift_key        |<+
                         | pit_key         FK |    | shift_type       | |
                         | equipment_key   FK |--->| start/end_hour   | |
                         | shift_key       FK |--->| crew_size        | |
                         | extraction_date    |    | supervisor       | |
                         | ore_tonnes         |    +------------------+ |
                         | waste_tonnes       |                         |
                         | strip_ratio        |                         |
                         | grade_pct          |                         |
                         | recovery_pct       |                         |
                         | equip_util_pct     |                         |
                         +--------------------+                         |
                                                                        |
                         +--------------------+                         |
                         | kpi_production     |                         |
                         |--------------------|                         |
                         | site_id / pit_id   |                         |
                         | month              |                         |
                         | total_ore/waste    |                         |
                         | cumulative_ore_ytd |                         |
                         | reserve_depletion  |                         |
                         | cost_per_tonne     |                         |
                         | safety_incidents   |                         |
                         +--------------------+                         |
```

## Medallion Flow

```
BRONZE                       SILVER                          GOLD
+------------------+    +------------------------+    +--------------------+
| raw_extractions  |--->| extractions_enriched   |--->| fact_extraction    |
| (70+ records)    |    | - strip ratio calc     |    | dim_site           |
+------------------+    | - cost per tonne       |    | dim_pit            |
| raw_sites        |--->| - equip utilization    |    | dim_equipment      |
| raw_pits         |    | - flag low grade       |    | dim_shift          |
| raw_equipment    |    +------------------------+    | kpi_production     |
| raw_shifts       |                                  | (cumulative YTD)   |
+------------------+                                  +--------------------+
```

## Features

- **Partitioning**: Extractions partitioned by site_id + shift for efficient querying
- **Cumulative YTD**: Running SUM window function for year-to-date ore extraction
- **Reserve depletion**: Tracks percentage of total reserves extracted
- **VACUUM**: Applied with 168-hour retention for data governance
- **Equipment utilization**: Derived from actual vs available hours per shift
- **Cost per tonne**: Calculated from fuel cost + equipment hourly rate
- **Strip ratio analysis**: waste/ore ratio tracked per pit and equipment
- **INCREMENTAL_FILTER macro**: Incremental load uses extraction_id + extraction_date with 3-day overlap

## Seed Data

- **3 mine sites** (copper/Australia, iron ore/Brazil, gold/Canada)
- **6 pits** including 1 developing shaft
- **8 equipment** pieces (excavators, haul trucks, dozers, drill rig)
- **3 shifts** per day (day, swing, night)
- **70 extraction records** spanning January-February 2024
- Equipment breakdown examples (EQ-005 in maintenance with low utilization)
- Safety incidents tracked per extraction

## Verification Checklist

- [ ] All 3 sites in dim_site with reserve estimates
- [ ] All 6 pits in dim_pit with depth progress
- [ ] All 8 equipment in dim_equipment with maintenance status
- [ ] All 3 shifts in dim_shift
- [ ] Fact table has 70+ extraction records
- [ ] Strip ratios: gold mine lowest, copper mine moderate, iron ore highest
- [ ] Equipment utilization varies (maintenance equipment < 50%)
- [ ] Cumulative YTD ore calculated correctly per site-pit
- [ ] Reserve depletion percentages computed
- [ ] No orphaned foreign keys in fact table
