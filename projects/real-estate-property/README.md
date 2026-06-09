# Real Estate Property Pipeline

## Scenario

A county assessor's office tracks property assessments that change annually (SCD2 tracking). Assessment batches sometimes contain errors that need rollback (RESTORE). The pipeline tracks property value history across 3 annual assessment batches, calculates neighborhood trends, compares assessed values to actual sale prices for accuracy analysis, and enables fast parcel lookups via bloom filters.

## Unique Combination

SCD2 with 3 assessment batches (2022/2023/2024) + RESTORE correction workflow + bloom filters on parcel_id + time travel for "value as of assessment date" + partition by county + window functions for YoY assessment trends + neighborhood benchmarking + assessment accuracy (assessed_value vs sale_price) with COD calculation

## Star Schema

```
+---------------------+       +--------------------+
| dim_neighborhood    |       | dim_agent          |
|---------------------|       |--------------------|
| neighborhood_key(PK)|       | agent_key (PK)     |
| neighborhood_id     |       | agent_id           |
| neighborhood_name   |  +----+ agent_name        |
| city/county/state   |  |    | brokerage          |
| median_income       |  |    | license_number     |
| school_rating       |  |    | years_experience   |
| crime_index         |  |    | specialization     |
| walkability_score   |  |    | county             |
+---------+-----------+  |    +--------------------+
          |              |
     +----+----+----+----+    +--------------------+
     | fact_transactions |    | dim_property_type  |
     |------------------|    |--------------------|
     | transaction_key  +----+ property_type_key  |
     | parcel_id        |    | property_type      |
     | neighborhood_key |    | avg_sqft           |
     | agent_key        |    | avg_assessed_value |
     | property_type_key|    | property_count     |
     | transaction_date |    +--------------------+
     | sale_price       |
     | assessed_value_  |
     |   at_sale        |
     | price_per_sqft   |
     | days_on_market   |
     | over_asking_pct  |
     | assessed_vs_     |
     |   sale_ratio     |
     | assessment_      |
     |   outlier        |
     | financing_type   |
     +------------------+

+------------------------------+   +------------------------------+
| kpi_market_trends            |   | kpi_assessment_accuracy      |
|------------------------------|   |------------------------------|
| city                         |   | county                       |
| property_type                |   | property_type                |
| sale_quarter                 |   | assessment_year              |
| median_sale_price            |   | total_sales                  |
| avg_price_per_sqft           |   | avg_assessed_value           |
| avg_days_on_market           |   | avg_sale_price               |
| total_transactions           |   | avg_ratio                    |
| avg_over_asking_pct          |   | median_ratio                 |
| inventory_months             |   | outlier_count                |
| yoy_price_change_pct         |   | outlier_rate_pct             |
+------------------------------+   | cod (coeff of dispersion)    |
                                   +------------------------------+
```

## Pipeline DAG

```
validate_bronze
       |
  +----+-------------------+
load_scd2_batch1    load_transactions       <-- parallel (first assessment + sales)
       |                    |
load_scd2_batch2            |
       |                    |
load_scd2_batch3            |
       |                    |
       +---------+----------+
                 |
  enrich_transactions_point_in_time        <-- assessed value at time of sale
                 |
  +--------------+---------------+
build_dim_    build_dim_    build_dim_
 neighborhood   agent       property_type   <-- parallel
  |              |              |
  +--------------+--------------+
                 |
      build_fact_transactions
                 |
      +----------+----------+
kpi_market_   kpi_assessment_
  trends        accuracy                    <-- parallel
      |              |
      +------+-------+
             |
  restore_correction_demo                   <-- insert bad batch, RESTORE, verify
             |
  bloom_and_optimize (CONTINUE ON FAILURE)
```

## SCD2 with 3 Batches

- **Batch 1**: 2022 assessments for all 18 properties (18 rows, all is_current=true)
- **Batch 2**: 2023 assessments for 12 properties reassessed (expire old, insert new)
- **Batch 3**: 2024 assessments for 8 properties reassessed (expire old, insert new)
- **Result**: 18 + 12 + 8 = 38 rows in property_dim, only 18 with is_current=true
- **3 significant value changes**: PRC-001 ($480K->$525K->$560K), PRC-004 ($800K->$875K->$920K), PRC-011 ($700K->$785K->$830K)

## RESTORE Demo

1. Insert bad batch: all 18 properties assessed at $1
2. Verify damage: 56 rows exist (38 + 18 bad)
3. `RESTORE silver.property_dim TO VERSION 6`
4. Verify recovery: back to 38 rows
5. Log correction in correction_log table

## Features Demonstrated

| Feature | Description |
|---|---|
| **SCD Type 2** | 3 annual assessment batches with expire-insert pattern |
| **RESTORE** | Rollback of bad assessment batch to prior version |
| **Bloom Filter** | Fast parcel_id lookup on property_dim |
| **Pseudonymisation (keyed_hash)** | Buyer and seller names hashed |
| **Point-in-Time Join** | Assessed value at time of sale via SCD2 temporal join |
| **Assessment Accuracy** | assessed_vs_sale_ratio with outlier flagging (|ratio-1|>0.20) |
| **COD** | Coefficient of Dispersion for assessment uniformity |
| **Partitioning** | property_dim partitioned by county |
| **LAG Window** | YoY assessment appreciation and quarter-over-quarter price momentum |
| **PERCENTILE_CONT** | Median sale price and median assessment ratio |
| **Repeat Sales** | Same-parcel sold multiple times with appreciation tracking |

## Data Profile

- **18 properties**: Across Austin TX (Travis), Denver CO (Denver/Jefferson), Phoenix AZ (Maricopa)
- **40 assessments**: 3 annual batches (18 + 12 + 8)
- **25 sales transactions**: Over 2022-2024 with conventional, FHA, VA, jumbo financing
- **6 neighborhoods**: With school ratings, crime indices, walkability scores, county assignment
- **8 agents**: From 3 brokerages across 3 counties
- **3 significant value changes**: Properties with 3 SCD2 versions each
- **2 assessment outliers**: Properties where assessed/sale ratio deviates > 20%

## Verification Checklist

- [ ] SCD2 property_dim has 38 total rows with 18 is_current=true
- [ ] Properties PRC-001, PRC-004, PRC-011 have 3 assessment versions each
- [ ] Star schema joins produce 25+ transaction records with all dimensions
- [ ] Assessment accuracy KPI shows avg_ratio, outlier_count, COD by county
- [ ] Assessment outliers correctly flagged (|ratio - 1.0| > 0.20)
- [ ] Market trend KPIs generated for all city/property_type/quarter combinations
- [ ] Agent performance scorecards show total volume, average DOM, over-asking rates
- [ ] Neighborhood benchmark includes school ratings, walkability, avg assessment ratio
- [ ] Price momentum LAG analysis shows quarter-over-quarter trends
- [ ] Repeat sales analysis shows appreciation percentages using LAG
- [ ] RESTORE correction log has at least 1 entry documenting rollback
- [ ] Property type comparison shows avg_sqft, avg_price, median via PERCENTILE_CONT
