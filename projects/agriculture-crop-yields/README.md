# Agriculture Crop Yields Pipeline

## Scenario

A multi-region agriculture operation tracks crop yields across 8 fields spanning 3 regions (Midwest, Southeast, West Coast). The pipeline ingests harvest records, weather station data, and soil sensor readings to build a comprehensive crop analytics platform. It calculates yield per hectare, compares against expected benchmarks, identifies underperforming fields (< 70% of expected yield), and correlates weather conditions (drought index, rainfall) with crop performance.

The system tracks 6 crop types (Corn, Soybeans, Wheat, Rice, Cotton, Almonds, Grapes) across 3 seasons with varying weather conditions, enabling seasonal trend analysis and field-level performance scorecards.

## Star Schema

```
+----------------+     +----------------+     +----------------+
| dim_field      |     | dim_crop       |     | dim_season     |
|----------------|     |----------------|     |----------------|
| field_key      |<-+  | crop_key       |<-+  | season_key     |<-+
| field_id       |  |  | crop_type      |  |  | season_name    |  |
| field_name     |  |  | variety        |  |  | year           |  |
| farm_name      |  |  | growth_days    |  |  | planting_start |  |
| region         |  |  | seed_cost/ha   |  |  | harvest_start  |  |
| soil_type      |  |  | expected_yield |  |  | frost_risk     |  |
| irrigation     |  |  +----------------+  |  +----------------+  |
| total_hectares |  |                      |                      |
| gps_lat/lon    |  |  +------------------+|  +----------------+  |
+----------------+  |  | fact_harvest     ||  | dim_weather    |  |
                    |  |------------------|+  |----------------|  |
                    +--| harvest_key      |   | weather_key    |<-+
                       | field_key     FK |   | station_id     |  |
                       | crop_key      FK |-->| avg_temp_c     |  |
                       | season_key    FK |-->| rainfall_mm    |  |
                       | weather_key   FK |-->| sunshine_hours |  |
                       | harvest_date     |   | drought_index  |  |
                       | area_hectares    |   +----------------+  |
                       | yield_tonnes     |                       |
                       | moisture_pct     |                       |
                       | quality_grade    |                       |
                       | input_cost       |                       |
                       | revenue          |                       |
                       | profit_per_ha    |                       |
                       +------------------+                       |
                                                                  |
                       +------------------+                       |
                       | kpi_yield_analysis|                      |
                       |------------------|                       |
                       | region            |                      |
                       | crop_type         |                      |
                       | season         -->+-----------------------+
                       | avg_yield_per_ha  |
                       | yield_vs_expected |
                       | total_revenue     |
                       | total_cost        |
                       | avg_profit_per_ha |
                       | best/worst_field  |
                       | weather_corr      |
                       +------------------+
```

## Medallion Flow

```
BRONZE                       SILVER                          GOLD
+------------------+    +------------------------+    +--------------------+
| raw_harvests     |--->| harvests_enriched (CDF)|    | fact_harvest       |
| (65+ records)    |    | - yield/ha calc        |--->| dim_field          |
+------------------+    | - profit/ha            |    | dim_crop           |
| raw_fields       |    | - vs expected %        |    | dim_season         |
| (8 fields)       |--->| - underperformer flag  |    | dim_weather        |
+------------------+    | - sensor enrichment    |    | kpi_yield_analysis |
| raw_weather      |    +------------------------+    +--------------------+
| (9 station recs) |
+------------------+
| raw_sensors      |
| (24 readings)    |
+------------------+
```

## Features

- **Schema evolution**: Sensor table supports adding new sensor types across growing seasons
- **CHECK constraints**: yield >= 0, moisture 0-100, area > 0 validated at bronze
- **Partitioning**: Harvest data partitioned by region + crop_type for efficient querying
- **Weather correlation**: Drought index and rainfall mapped to yield performance
- **Underperformer detection**: Fields yielding < 70% of expected flagged automatically
- **Seasonal trend analysis**: LAG window functions compare yield across seasons
- **Field scorecards**: RANK-based profitability scoring across all fields
- **INCREMENTAL_FILTER macro**: Incremental load uses harvest_id + harvest_date with 7-day overlap

## Seed Data

- **8 fields** across 3 regions (Midwest, Southeast, West Coast)
- **6 crop types** with multiple varieties
- **3 seasons** (Spring 2023, Summer 2023, Spring 2024)
- **65 harvest records** with realistic yield variations
- **9 weather station records** with drought conditions on West Coast
- **24 soil sensor readings** (moisture, pH, NPK nutrients)

## Verification Checklist

- [ ] All 8 fields in dim_field with GPS coordinates
- [ ] 6+ crop types in dim_crop with expected yields
- [ ] All season-year combinations in dim_season
- [ ] Weather dimension populated with drought indices
- [ ] Fact table has 65+ harvest records
- [ ] Regional yield benchmarks show Midwest > West Coast for corn
- [ ] Underperformers detected (Summer 2023 drought crops)
- [ ] Weather correlation shows drought impact on West Coast
- [ ] No orphaned foreign keys in fact table
- [ ] KPI analysis identifies best/worst fields per region-crop-season
