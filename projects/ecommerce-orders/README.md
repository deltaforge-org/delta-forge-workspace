# E-Commerce Orders Pipeline

## Scenario

A multi-channel e-commerce platform ingests order data from three sources: web (desktop), mobile (iOS/Android), and marketplace (Amazon/eBay). The pipeline merges these feeds into a unified order model, handles soft deletes for cancelled orders (preserving them with `is_deleted=true` rather than physical deletion), and enables Change Data Feed (CDF) on the silver orders table for downstream sync to fulfillment and analytics systems.

Customer segmentation uses RFM (Recency, Frequency, Monetary) scoring with NTILE window functions to classify customers into actionable segments like Champion, Loyal, Big Spender, At Risk, and Lost.

## Star Schema

```
                    +----------------+
                    | dim_customer   |
                    |----------------|
                    | customer_key   |<------+
                    | email          |       |
                    | segment        |       |
                    | rfm_segment    |       |
                    | lifetime_orders|       |
                    +----------------+       |
                                             |
+----------------+  +-------------------+    |    +----------------+
| dim_product    |  | fact_order_lines  |    |    | dim_channel    |
|----------------|  |-------------------|    |    |----------------|
| product_key    |<-| order_line_key    |    +--->| channel_key    |
| sku            |  | order_key         |         | channel_name   |
| product_name   |  | product_key    FK |         | platform       |
| category       |  | customer_key   FK |         | region         |
| brand          |  | channel_key    FK |         +----------------+
| unit_cost      |  | order_date        |
+----------------+  | quantity          |
                    | unit_price        |
                    | discount_pct      |
                    | line_total        |
                    | shipping_cost     |
                    +-------------------+
```

## Medallion Flow

```
BRONZE                      SILVER                         GOLD
+-----------------+    +---------------------+    +--------------------+
| raw_orders      |--->| orders_merged (CDF) |--->| fact_order_lines   |
| (web+mobile+mkt)|    | (soft-delete aware) |    | dim_customer       |
+-----------------+    +---------------------+    | dim_product        |
| raw_customers   |--->| customer_rfm        |--->| dim_channel        |
+-----------------+    | (NTILE scoring)     |    | kpi_sales_dashboard|
| raw_products    |    +---------------------+    +--------------------+
+-----------------+
```

## Features

- **Multi-source MERGE**: Web, mobile, and marketplace feeds merged on `order_line_id`
- **Soft delete**: Cancelled orders set `is_deleted=true` instead of physical deletion
- **Change Data Feed (CDF)**: Enabled on `silver.orders_merged` for downstream consumers
- **RFM segmentation**: NTILE(4) quartile scoring on recency, frequency, monetary value
- **Cancellation trend analysis**: KPI dashboard tracks cancellation rate by channel and date
- **Incremental processing**: Watermark-based MERGE processes only new rows
- **Pseudonymisation**: Customer email and address REDACTED for PII compliance

## Seed Data

- **15 customers** across 5 segments (Premium, Standard, Enterprise)
- **20 products** across 4 categories (Electronics, Home, Sports, Office) with 6 brands
- **72 order lines** spanning January-June 2024 across 3 channels
- Includes 6 cancelled orders, 3 returns, 2 partial shipments

## Verification Checklist

- [ ] All 3 channels present in dim_channel
- [ ] All 20 products in dim_product with unit_cost
- [ ] All 15 customers in dim_customer with RFM segments
- [ ] Cancelled orders excluded from fact_order_lines
- [ ] No orphaned foreign keys in fact table
- [ ] KPI dashboard shows per-channel cancellation rates
- [ ] Incremental load processes only new rows (watermark advances)
- [ ] CDF captures change history on silver.orders_merged
