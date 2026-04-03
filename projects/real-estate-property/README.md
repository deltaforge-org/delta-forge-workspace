# Real Estate Property Pipeline

## Scenario

A real estate analytics platform tracks property listings, price changes, and sales transactions across three cities (Austin TX, Denver CO, Phoenix AZ) in five neighborhoods. The pipeline implements SCD Type 2 to track listing price changes over time (each price reduction creates a new version), uses RESTORE to roll back bad data imports, and applies pseudonymisation to buyer and seller names.

The gold layer provides market trend analysis with median prices, days on market (DOM), over-asking percentages, agent performance scorecards, neighborhood valuation comparisons, and price momentum analysis using LAG window functions.

## Star Schema

```
+------------------+         +------------------+
| dim_property     |  (SCD2) | dim_buyer        |
|------------------|         |------------------|
| surrogate_key(PK)|         | buyer_key (PK)   |
| property_id      |         | name             |
| address          |    +----+ buyer_type       |
| city             |    |    | pre_approved     |
| state/zip        |    |    | budget_range     |
| property_type    |    |    +------------------+
| bed/bath/sqft    |    |
| lot_acres        |    |
| year_built       |    |
| list_price       |    |    +------------------+
| valid_from/to    |    |    | dim_agent        |
| is_current       |    |    |------------------|
+--------+---------+    |    | agent_key (PK)   |
         |              |    | name             |
    +----+--------+-----+ +--+ brokerage       |
    |   fact_     |        |  | license_number  |
    | transactions+--------+  | years_experience|
    +-------------+           | specialization  |
    | transaction_key  |      +------------------+
    | property_key     |
    | buyer_key        |      +-------------------+
    | seller_key       |      | dim_neighborhood  |
    | agent_key        |      |-------------------|
    | transaction_date |      | neighborhood_key  |
    | list_price       |      | name              |
    | sale_price       |      | city/state        |
    | price_per_sqft   |      | median_income     |
    | days_on_market   |      | school_rating     |
    | over_asking_pct  |      | crime_index       |
    +------------------+      | walkability_score |
                              +-------------------+

+----------------------------+
|   kpi_market_trends        |
|----------------------------|
| city                       |
| property_type              |
| quarter                    |
| median_sale_price          |
| avg_price_per_sqft         |
| avg_days_on_market         |
| total_transactions         |
| over_asking_pct            |
| inventory_months           |
| price_change_yoy_pct       |
+----------------------------+
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **SCD Type 2** | Property listing price changes tracked as new versions (4 properties with price reductions) |
| **Bloom Filters** | Fast property_id lookup optimization on large property tables |
| **RESTORE** | Rollback of accidentally imported bad data row to previous version |
| **Pseudonymisation (keyed_hash)** | Buyer and seller names hashed with SHA256 |
| **Days on Market** | Calculated as DATEDIFF between transaction_date and list_date |
| **Over-Asking Analysis** | Percentage above/below list price for bidding war detection |
| **Price per SqFt** | Normalized comparison metric across property types |
| **LAG Window** | Quarter-over-quarter price momentum and repeat sale appreciation |
| **PERCENTILE_CONT** | Median sale price calculation for market trends |

## Data Profile

- **15 properties**: Across Austin TX, Denver CO, Phoenix AZ in 5 neighborhoods
- **12 buyers**: First-time (5), Move-up (3), Investor (3) with budget ranges
- **6 agents**: From 3 brokerages with 4-15 years experience
- **5 neighborhoods**: With school ratings, crime indices, walkability scores
- **22+ transactions**: Over 2023-2024 with conventional, FHA, VA, jumbo financing
- **4 price reductions**: SCD2 tracking of listing price changes
- **7 repeat properties**: Sold multiple times for appreciation analysis

## Verification Checklist

- [ ] Market trend KPIs generated for all city/property_type/quarter combinations
- [ ] Star schema joins produce complete transaction records with all dimensions
- [ ] SCD2 property dimension shows multiple versions for price-reduced listings
- [ ] Agent performance scorecards show total volume, average DOM, over-asking rates
- [ ] Neighborhood valuation comparison includes school ratings and walkability
- [ ] Price momentum LAG analysis shows quarter-over-quarter trends
- [ ] Bidding war detection correctly categorizes over/under asking transactions
- [ ] Buyer type analysis differentiates first-time, move-up, and investor patterns
- [ ] Repeat sales analysis shows appreciation percentages using LAG
- [ ] RESTORE demo successfully rolls back bad data import
