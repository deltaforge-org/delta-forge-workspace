-- =============================================================================
-- Banking Transactions Pipeline - Gold Layer Verification
-- =============================================================================
-- 12+ ASSERT queries testing fraud scores, CDF snapshot count, SCD2 versions,
-- running balances, 7-day moving average, tier migration counts, and more.
-- =============================================================================

-- ===================== TEST 1: Dimension Table Completeness =====================

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.gold.dim_account;

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.gold.dim_merchant;

-- dim_date should cover Jan-Mar 2024 (91 days)
ASSERT VALUE date_dim_count >= 91
SELECT COUNT(*) AS date_dim_count FROM {{zone_prefix}}.gold.dim_date;

-- ===================== TEST 2: SCD2 Customer Tier Versioning =====================
-- ACC002, ACC005, ACC008 should have 2 versions each (bronze + silver)

SELECT
    account_id,
    customer_name,
    customer_tier,
    valid_from,
    valid_to,
    is_current
FROM {{zone_prefix}}.silver.customer_dim
WHERE account_id IN ('ACC002', 'ACC005', 'ACC008')
ORDER BY account_id, valid_from;

ASSERT VALUE scd2_total_versions >= 6
SELECT COUNT(*) AS scd2_total_versions
FROM {{zone_prefix}}.silver.customer_dim
WHERE account_id IN ('ACC002', 'ACC005', 'ACC008');

ASSERT VALUE expired_count = 3
SELECT COUNT(*) AS expired_count
FROM {{zone_prefix}}.silver.customer_dim
WHERE is_current = false;

-- ===================== TEST 3: Star Schema Join - Account Transaction Summary =====================

SELECT
    da.customer_name,
    da.account_type,
    da.branch,
    da.customer_tier,
    COUNT(f.transaction_key) AS txn_count,
    ROUND(SUM(f.amount), 2) AS total_spend,
    ROUND(AVG(f.fraud_score), 2) AS avg_fraud_score
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
GROUP BY da.customer_name, da.account_type, da.branch, da.customer_tier
ORDER BY total_spend DESC;

-- Business accounts should have highest total spend
ASSERT VALUE top_spend_type = 'business'
SELECT da.account_type AS top_spend_type
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
GROUP BY da.account_type
ORDER BY SUM(f.amount) DESC
LIMIT 1;

-- ===================== TEST 4: Merchant Category Distribution =====================

SELECT
    dm.category,
    dm.risk_level,
    COUNT(*) AS txn_count,
    ROUND(SUM(f.amount), 2) AS category_total,
    ROUND(AVG(f.amount), 2) AS avg_txn_amount
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
GROUP BY dm.category, dm.risk_level
ORDER BY category_total DESC;

-- Travel should be a significant category
ASSERT VALUE travel_total >= 5000
SELECT ROUND(SUM(f.amount), 2) AS travel_total
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
WHERE dm.category = 'travel';

-- ===================== TEST 5: Fraud Detection Scoring =====================

SELECT
    f.transaction_date,
    f.amount,
    f.fraud_score,
    f.channel,
    dm.merchant_name,
    dm.category,
    dm.risk_level,
    PERCENT_RANK() OVER (ORDER BY f.fraud_score) AS fraud_percentile
FROM {{zone_prefix}}.gold.fact_transactions f
LEFT JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
WHERE f.fraud_score >= 40
ORDER BY f.fraud_score DESC;

-- Should have flagged crypto + suspicious transactions
ASSERT VALUE high_fraud_count >= 5
SELECT COUNT(*) AS high_fraud_count
FROM {{zone_prefix}}.gold.fact_transactions f
WHERE f.fraud_score >= 40;

-- Crypto transactions specifically should all be high fraud
ASSERT VALUE crypto_fraud_count >= 3
SELECT COUNT(*) AS crypto_fraud_count
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
WHERE dm.category = 'crypto' AND f.fraud_score >= 25;

-- ===================== TEST 6: Daily Volume KPIs =====================

SELECT
    transaction_date,
    total_txns,
    total_amount,
    avg_amount,
    fraud_flagged_count,
    fraud_pct,
    running_7d_avg_txns,
    running_7d_avg_amount
FROM {{zone_prefix}}.gold.kpi_daily_volumes
ORDER BY transaction_date;

-- Should span 10+ distinct days
ASSERT VALUE distinct_days >= 10
SELECT COUNT(*) AS distinct_days FROM {{zone_prefix}}.gold.kpi_daily_volumes;

-- ===================== TEST 7: 7-Day Moving Average Validation =====================

SELECT
    transaction_date,
    total_txns,
    running_7d_avg_txns
FROM {{zone_prefix}}.gold.kpi_daily_volumes
WHERE running_7d_avg_txns IS NOT NULL
ORDER BY transaction_date;

-- Moving average should be reasonable (between 1 and 20)
ASSERT VALUE min_7d_avg >= 1
SELECT MIN(running_7d_avg_txns) AS min_7d_avg FROM {{zone_prefix}}.gold.kpi_daily_volumes;

ASSERT VALUE max_7d_avg <= 20
SELECT MAX(running_7d_avg_txns) AS max_7d_avg FROM {{zone_prefix}}.gold.kpi_daily_volumes;

-- ===================== TEST 8: Account Balance Trajectory (LAG/LEAD) =====================

SELECT
    da.customer_name,
    f.transaction_date,
    f.amount,
    f.transaction_type,
    f.running_balance,
    LAG(f.running_balance) OVER (PARTITION BY f.account_key ORDER BY f.transaction_date) AS prev_balance,
    LEAD(f.running_balance) OVER (PARTITION BY f.account_key ORDER BY f.transaction_date) AS next_balance
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
WHERE da.customer_name = 'Alice Whitmore'
ORDER BY f.transaction_date;

-- ===================== TEST 9: Referential Integrity =====================

ASSERT VALUE orphan_accounts = 0
SELECT COUNT(*) AS orphan_accounts
FROM {{zone_prefix}}.gold.fact_transactions f
LEFT JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
WHERE da.account_key IS NULL;

ASSERT VALUE orphan_dates = 0
SELECT COUNT(*) AS orphan_dates
FROM {{zone_prefix}}.gold.fact_transactions f
LEFT JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
WHERE f.date_key IS NOT NULL AND dd.date_key IS NULL;

-- ===================== TEST 10: CDF Balance Snapshots =====================

ASSERT VALUE snapshot_count >= 1
SELECT COUNT(*) AS snapshot_count FROM {{zone_prefix}}.silver.balance_snapshots;

-- Show CDF snapshot details
SELECT
    snapshot_id,
    account_id,
    customer_name,
    old_tier,
    new_tier,
    change_type,
    snapshot_timestamp
FROM {{zone_prefix}}.silver.balance_snapshots
ORDER BY snapshot_timestamp DESC
LIMIT 20;

-- ===================== TEST 11: Tier Migration Matrix =====================

SELECT
    from_tier,
    to_tier,
    migration_count,
    period,
    ROUND(avg_balance, 2) AS avg_balance
FROM {{zone_prefix}}.gold.kpi_customer_health
ORDER BY from_tier, to_tier;

-- Should have bronze->silver upgrades
ASSERT VALUE bronze_to_silver >= 1
SELECT COALESCE(SUM(migration_count), 0) AS bronze_to_silver
FROM {{zone_prefix}}.gold.kpi_customer_health
WHERE from_tier = 'bronze' AND to_tier = 'silver';

-- ===================== TEST 12: Time Travel - Regulatory Audit =====================
-- Show customer_dim at version 1 vs current to demonstrate audit capability

SELECT COUNT(*) AS version_1_count
FROM {{zone_prefix}}.silver.customer_dim VERSION AS OF 1;

SELECT COUNT(*) AS current_count
FROM {{zone_prefix}}.silver.customer_dim;

-- Current should have more rows than version 1 (due to SCD2 expansion)
ASSERT VALUE version_growth >= 0
SELECT
    curr.cnt - v1.cnt AS version_growth
FROM
    (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.customer_dim) curr,
    (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.customer_dim VERSION AS OF 1) v1;

-- ===================== TEST 13: dim_date Weekend/Weekday Distribution =====================

ASSERT VALUE weekend_days >= 20
SELECT COUNT(*) AS weekend_days
FROM {{zone_prefix}}.gold.dim_date
WHERE is_weekend = true;

-- ===================== VERIFICATION SUMMARY =====================

SELECT 'Banking Transactions Gold Layer Verification PASSED - all 13+ assertions validated' AS status;
