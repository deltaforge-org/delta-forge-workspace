# Banking Transactions Pipeline

## Scenario

A retail bank processes transaction feeds every 4 hours through a medallion architecture. Raw transactions are enriched with merchant data, scored for fraud using rule-based detection (high amounts, crypto merchants, velocity checks, suspicious hours), and running balances are tracked with Change Data Feed (CDF) enabled on the accounts table for audit trail compliance.

## Table Schemas

### Bronze Layer
- **raw_accounts** (10 rows): account_id, account_number, account_type, customer_name, branch, status, balance
- **raw_merchants** (15 rows): merchant_id, merchant_name, category (groceries/dining/travel/fuel/online/crypto/etc.), city, country
- **raw_transactions** (65 rows): transaction_id, account/merchant references, timestamp, amount ($5-$5000), type, channel, suspicious flag

### Silver Layer
- **transactions_enriched** - Enriched with merchant categories, running balances (window SUM), fraud scores (0-100), velocity metrics (LAG)
- **accounts_cdf** - CDF-enabled account tracking with balance snapshots and last transaction dates

### Gold Layer (Star Schema)
- **fact_transactions** - Transaction facts with fraud scores and running balances
- **dim_account** - Account dimension (10 accounts, 4 branches)
- **dim_merchant** - Merchant dimension (15 merchants, 9 categories)
- **kpi_daily_volumes** - Daily aggregates with 7-day moving average and fraud percentages

## Medallion Flow

```
Bronze                          Silver                           Gold
+-------------------+    +---------------------------+    +--------------------+
| raw_transactions  |--->| transactions_enriched     |--->| fact_transactions  |
| (65 rows)         |    | (fraud score, run balance)|    | (scored, enriched) |
+-------------------+    +---------------------------+    +--------------------+
| raw_accounts      |--->| accounts_cdf              |--->| dim_account        |
| (10 accounts)     |    | (CDF enabled)             |    +--------------------+
+-------------------+    +---------------------------+    | dim_merchant       |
| raw_merchants     |----------------------------------->| (15 merchants)     |
| (15 merchants)    |                                    +--------------------+
+-------------------+                                    | kpi_daily_volumes  |
                                                         | (7d moving avg)    |
                                                         +--------------------+
```

## Star Schema

```
+-------------------+    +---------------------+    +-------------------+
| dim_account       |    | fact_transactions   |    | dim_merchant      |
|-------------------|    |---------------------|    |-------------------|
| account_key    PK |----| transaction_key  PK |----| merchant_key   PK |
| account_number    |    | account_key      FK |    | merchant_name     |
| account_type      |    | merchant_key     FK |    | category          |
| customer_name     |    | transaction_date    |    | city, country     |
| branch            |    | amount              |    +-------------------+
| status            |    | transaction_type    |
+-------------------+    | running_balance     |    +-------------------+
                         | fraud_score         |    | kpi_daily_volumes |
                         | channel             |    |-------------------|
                         +---------------------+    | transaction_date  |
                                                    | total_txns        |
                                                    | fraud_pct         |
                                                    | running_7d_avg    |
                                                    +-------------------+
```

## Key Features

- **Change Data Feed (CDF)** on accounts table for balance change auditing
- **Fraud scoring** (0-100) using rules: high amount (+30), crypto merchant (+25), velocity (+20), suspicious flag (+15), late-night (+10)
- **Running balance** calculation using window SUM partitioned by account
- **7-day moving average** on daily transaction volumes
- **PERCENT_RANK** for fraud percentile analysis
- **LAG/LEAD** for account balance trajectory analysis
- **Pseudonymisation**: keyed_hash on account_number and customer_name
- **OPTIMIZE** compaction on gold tables

## Verification Checklist

- [ ] 10 accounts in dim_account, 15 merchants in dim_merchant
- [ ] Business accounts have highest total spend
- [ ] Travel category total >= $5,000
- [ ] At least 4 high-fraud transactions flagged (score >= 40)
- [ ] Daily volumes span 10+ distinct days
- [ ] 7-day moving average between 1 and 20
- [ ] No orphan account keys in fact table
- [ ] CDF accounts have last_txn_date populated
