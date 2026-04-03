# Banking Transactions Pipeline

## Scenario

You are a data engineer at a retail bank. Your pipeline processes transaction feeds every 4 hours, scores fraud risk using a multi-rule engine (high-value, crypto merchants, velocity clustering, unusual hours, suspicious flags), tracks customer tier changes (bronze/silver/gold) via SCD Type 2, materializes CDF changes into balance_snapshots for regulatory audit, and maintains a star schema for analytics with a tier migration matrix and 7-day moving averages.

## Table Schemas

### Bronze Layer
- **raw_transactions** (70 rows): transaction_id, account_id, merchant_id, transaction_date, amount, transaction_type, channel, is_suspicious, ingested_at -- includes 5 high-fraud, 3 velocity clusters, 2 late-night transactions
- **raw_accounts** (12 rows): account_id, account_number, account_type, customer_name, branch, open_date, status, current_balance, customer_tier (bronze/silver/gold), ingested_at -- 3 customers with tier upgrades for SCD2
- **raw_merchants** (15 rows): merchant_id, merchant_name, category (groceries/dining/travel/fuel/online/crypto/etc.), city, country, risk_level, ingested_at

### Silver Layer
- **customer_dim** (SCD2, CDF-enabled): customer_id, account_id, account_number, account_type, customer_name, branch, open_date, status, customer_tier, valid_from, valid_to, is_current, updated_at -- tracks tier upgrades with valid_from/valid_to/is_current
- **transactions_enriched**: transaction_id, account_id, merchant_id, merchant_category, merchant_risk, transaction_date, amount, transaction_type, channel, running_balance, fraud_score, is_suspicious, prev_txn_amount, time_since_prev_sec, txn_velocity_5, ingested_at, processed_at
- **balance_snapshots** (CDF-driven): snapshot_id, account_id, customer_name, old_tier, new_tier, change_type, snapshot_timestamp -- materialized from customer_dim CDF changes

### Gold Layer (Star Schema)
- **dim_account** (12 rows): account_key, account_id, account_number, account_type, customer_name, branch, open_date, status, customer_tier, loaded_at
- **dim_merchant** (15 rows): merchant_key, merchant_id, merchant_name, category, city, country, risk_level, loaded_at
- **dim_date**: date_key, full_date, day_of_week, day_name, month, month_name, quarter, year, is_weekend, loaded_at
- **fact_transactions**: transaction_key, account_key, merchant_key, date_key, transaction_date, amount, transaction_type, running_balance, fraud_score, channel, loaded_at
- **kpi_daily_volumes**: transaction_date, total_txns, total_amount, avg_amount, fraud_flagged_count, fraud_pct, running_7d_avg_txns, running_7d_avg_amount, loaded_at
- **kpi_customer_health**: from_tier, to_tier, migration_count, period, avg_balance, loaded_at -- tier migration matrix

## Pipeline DAG (11 steps)

```
validate_bronze
     |
  +--+----------+-------------------+
  |             |                   |
build_customer_scd2  enrich_transactions  build_dim_merchant  <-- parallel
     |                    |                    |
materialize_cdf_snapshots |                    |
     |                    |                    |
build_dim_account         |                    |
     |                    |                    |
build_dim_date            |                    |
     |                    |                    |
     +----------+---------+-------------------+
                |
     build_fact_transactions
                |
     +----------+----------+
     |                     |
compute_daily_kpi  compute_customer_health  <-- parallel
     |                     |
     +----------+----------+
                |
     optimize_and_vacuum (CONTINUE ON FAILURE)
```

## Star Schema

```
+-------------------+    +---------------------+    +-------------------+
| dim_account       |    | fact_transactions   |    | dim_merchant      |
|-------------------|    |---------------------|    |-------------------|
| account_key    PK |----| transaction_key  PK |----| merchant_key   PK |
| account_number    |    | account_key      FK |    | merchant_name     |
| account_type      |    | merchant_key     FK |    | category          |
| customer_name     |    | date_key         FK |    | city, country     |
| customer_tier     |    | transaction_date    |    | risk_level        |
| branch            |    | amount              |    +-------------------+
+-------------------+    | transaction_type    |
                         | running_balance     |    +-------------------+
+-------------------+    | fraud_score         |    | kpi_daily_volumes |
| dim_date          |    | channel             |    |-------------------|
|-------------------|    +---------------------+    | transaction_date  |
| date_key       PK |----                           | total_txns        |
| full_date         |    +-------------------------+| fraud_pct         |
| day_name          |    | kpi_customer_health    || running_7d_avg    |
| month, quarter    |    |-------------------------+-------------------+
| is_weekend        |    | from_tier, to_tier     |
+-------------------+    | migration_count        |
                         | period, avg_balance    |
                         +-------------------------+
```

## Key Features

- **SCD Type 2** on customer_dim: tracks tier upgrades (bronze->silver, silver->gold) with valid_from/valid_to/is_current
- **Change Data Feed (CDF)** on customer_dim materialized into balance_snapshots -- every tier change captured
- **Tier migration matrix**: gold KPI counting customers who moved up/down/stayed using CDF _change_type
- **Fraud scoring** (0-100) multi-rule engine: +30 high value (>5000), +25 crypto merchant, +20 velocity (5+ txns in 1 day), +15 suspicious flag, +10 unusual hours (1-5 AM)
- **Running balance** via window SUM partitioned by account
- **7-day moving average** on daily transaction volumes and amounts
- **dim_date** dimension for temporal analytics
- **Pseudonymisation**: keyed_hash on account_number and customer_name
- **OPTIMIZE** with Z-ORDER on fraud_score, bloom filters on account_id
- **Time travel**: regulatory audit via VERSION AS OF queries

## Verification Checklist

- [ ] 12 accounts in dim_account, 15 merchants in dim_merchant
- [ ] Business accounts have highest total spend
- [ ] Travel category total >= $5,000
- [ ] At least 5 high-fraud transactions flagged (score >= 40)
- [ ] Daily volumes span 10+ distinct days
- [ ] 7-day moving average between 1 and 20
- [ ] No orphan account keys in fact table
- [ ] CDF balance_snapshots has tier change entries
- [ ] SCD2 customer_dim has multiple versions for upgraded customers
- [ ] Tier migration matrix shows bronze->silver upgrades
- [ ] dim_date correctly populated
- [ ] Running balances are monotonically consistent per account
