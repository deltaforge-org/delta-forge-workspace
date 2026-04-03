# Retail POS Sales Pipeline

## Scenario

A multi-format retail chain operates 6 stores across 3 US regions (Midwest, Northeast, West) in flagship, standard, and express formats. The POS system captures every line item including returns (negative quantities), discounts, and multiple payment methods (credit card, debit card, cash, mobile pay). The pipeline enriches raw transactions with product data, computes basket metrics, flags anomalies (stores with return rates exceeding 20%), and builds a gold-layer star schema for store performance analytics with YoY growth tracking and regional ranking.

## Star Schema

```
                    +----------------+
                    | dim_cashier    |
                    |----------------|
                    | cashier_key    |<------+
                    | employee_id    |       |
                    | name           |       |
                    | certification  |       |
                    +----------------+       |
                                             |
+----------------+  +-------------------+    |    +----------------+
| dim_product    |  | fact_sales        |    |    | dim_date       |
|----------------|  |-------------------|    |    |----------------|
| product_key    |<-| sale_key          |    +--->| date_key       |
| sku            |  | store_key      FK |         | full_date      |
| product_name   |  | product_key    FK |         | day_of_week    |
| category       |  | cashier_key    FK |         | month          |
| brand          |  | date_key       FK |         | is_weekend     |
| unit_cost      |  | receipt_id        |         +----------------+
+----------------+  | quantity          |
                    | unit_price        |    +----------------+
                    | discount_pct      |    | dim_store      |
                    | line_total        |    |----------------|
                    | payment_method    |--->| store_key      |
                    | basket_id         |    | store_name     |
                    +-------------------+    | region         |
                                             | format         |
                    +------------------------+ sqft           |
                    | kpi_store_performance  | +----------------+
                    |------------------------|
                    | store_id, region       |
                    | month, total_revenue   |
                    | yoy_growth_pct         |
                    | rank_in_region         |
                    +------------------------+
```

## Medallion Flow

```
BRONZE                      SILVER                          GOLD
+-----------------+    +-------------------------+    +---------------------+
| raw_transactions|--->| transactions_enriched   |--->| fact_sales          |
| (75+ POS lines) |    | (line totals, returns)  |    | dim_store           |
+-----------------+    +-------------------------+    | dim_product         |
| raw_stores      |    | basket_metrics          |    | dim_date            |
+-----------------+    | (items/basket, value)   |    | dim_cashier         |
| raw_products    |    +-------------------------+    | kpi_store_perf      |
+-----------------+                                   +---------------------+
| raw_cashiers    |
+-----------------+
```

## Features

- **Bloom filters**: Fast lookup on receipt_id for receipt-level queries
- **Z-ordering**: fact_sales optimized on store_key + date_key for range scans
- **Partition pruning**: Region-based filtering for regional reports
- **Window functions**: LAG for YoY growth, DENSE_RANK for regional store ranking, running totals
- **Basket analysis**: Items per basket, basket value, return detection
- **Anomaly detection**: Flags stores where returns exceed 20% of sales
- **Incremental processing**: INCREMENTAL_FILTER macro for watermark-based MERGE

## Seed Data

- **6 stores** across 3 regions (Midwest: Chicago/Naperville, Northeast: NYC/Boston, West: LA/SF)
- **20 products** across 4 categories (Groceries, Electronics, Apparel) with realistic brands
- **8 cashiers** with varying certification levels and shift preferences
- **75 transaction lines** spanning January-March 2024 with returns, discounts, multiple payment methods

## Verification Checklist

- [ ] All 6 stores present in dim_store with correct regions
- [ ] All 20 products in dim_product with unit_cost for margin calculation
- [ ] All 8 cashiers in dim_cashier
- [ ] Returns (negative qty) included in fact_sales with negative line_total
- [ ] 3 regions present in revenue-by-region query
- [ ] KPI store performance has regional rankings (rank_in_region)
- [ ] Monthly trend shows running totals and MoM changes
- [ ] Incremental load processes only new April rows (INCREMENTAL_FILTER macro)
