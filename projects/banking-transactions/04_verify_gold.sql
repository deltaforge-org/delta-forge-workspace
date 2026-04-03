-- =============================================================================
-- Banking Transactions Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== TEST 1: Star Schema Join - Account Transaction Summary =====================

SELECT
    da.customer_name,
    da.account_type,
    da.branch,
    COUNT(f.transaction_key) AS txn_count,
    ROUND(SUM(f.amount), 2) AS total_spend,
    ROUND(AVG(f.fraud_score), 2) AS avg_fraud_score
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
GROUP BY da.customer_name, da.account_type, da.branch
ORDER BY total_spend DESC;

-- Business accounts should have highest total spend
SELECT da.account_type AS top_spend_type
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
GROUP BY da.account_type
ORDER BY SUM(f.amount) DESC
LIMIT 1;

-- ===================== TEST 2: Merchant Category Distribution =====================

ASSERT VALUE top_spend_type = 'business'
SELECT
    dm.category,
    COUNT(*) AS txn_count,
    ROUND(SUM(f.amount), 2) AS category_total,
    ROUND(AVG(f.amount), 2) AS avg_txn_amount
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
GROUP BY dm.category
ORDER BY category_total DESC;

-- Travel should be a significant category
SELECT ROUND(SUM(f.amount), 2) AS travel_total
FROM {{zone_prefix}}.gold.fact_transactions f
JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
WHERE dm.category = 'travel';

-- ===================== TEST 3: Fraud Detection Scoring =====================

ASSERT VALUE travel_total >= 5000
SELECT
    f.transaction_date,
    f.amount,
    f.fraud_score,
    f.channel,
    dm.merchant_name,
    dm.category,
    PERCENT_RANK() OVER (ORDER BY f.fraud_score) AS fraud_percentile
FROM {{zone_prefix}}.gold.fact_transactions f
LEFT JOIN {{zone_prefix}}.gold.dim_merchant dm ON f.merchant_key = dm.merchant_key
WHERE f.fraud_score >= 40
ORDER BY f.fraud_score DESC;

-- Should have flagged crypto transactions
SELECT COUNT(*) AS high_fraud_count
FROM {{zone_prefix}}.gold.fact_transactions f
WHERE f.fraud_score >= 40;

-- ===================== TEST 4: Daily Volume KPIs =====================

ASSERT VALUE high_fraud_count >= 4
SELECT
    transaction_date,
    total_txns,
    total_amount,
    avg_amount,
    fraud_flagged_count,
    fraud_pct,
    running_7d_avg_txns
FROM {{zone_prefix}}.gold.kpi_daily_volumes
ORDER BY transaction_date;

-- Should span multiple days
SELECT COUNT(*) AS distinct_days FROM {{zone_prefix}}.gold.kpi_daily_volumes;
-- ===================== TEST 5: 7-Day Moving Average Validation =====================

ASSERT VALUE distinct_days >= 10
SELECT
    transaction_date,
    total_txns,
    running_7d_avg_txns
FROM {{zone_prefix}}.gold.kpi_daily_volumes
WHERE running_7d_avg_txns IS NOT NULL
ORDER BY transaction_date;

-- Moving average should be reasonable (between 1 and 20)
SELECT
    MIN(running_7d_avg_txns) AS min_7d_avg,
    MAX(running_7d_avg_txns) AS max_7d_avg
FROM {{zone_prefix}}.gold.kpi_daily_volumes;

ASSERT VALUE min_7d_avg >= 1
-- ===================== TEST 6: Account Balance Trajectory (LAG/LEAD) =====================

ASSERT VALUE max_7d_avg <= 20
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

-- ===================== TEST 7: Dimension Completeness =====================

SELECT COUNT(*) AS acct_dim_count FROM {{zone_prefix}}.gold.dim_account;
ASSERT VALUE acct_dim_count = 10

SELECT COUNT(*) AS merch_dim_count FROM {{zone_prefix}}.gold.dim_merchant;
-- ===================== TEST 8: Referential Integrity =====================

ASSERT VALUE merch_dim_count = 15
SELECT COUNT(*) AS orphan_accounts
FROM {{zone_prefix}}.gold.fact_transactions f
LEFT JOIN {{zone_prefix}}.gold.dim_account da ON f.account_key = da.account_key
WHERE da.account_key IS NULL;

-- ===================== TEST 9: CDF Tracking Verification =====================

ASSERT VALUE orphan_accounts = 0
SELECT
    account_id,
    customer_name,
    current_balance,
    last_txn_date,
    updated_at
FROM {{zone_prefix}}.silver.accounts_cdf
ORDER BY last_txn_date DESC;

-- All active accounts with transactions should have last_txn_date set
SELECT COUNT(*) AS accounts_with_txns
FROM {{zone_prefix}}.silver.accounts_cdf
WHERE last_txn_date IS NOT NULL;

-- ===================== VERIFICATION SUMMARY =====================

ASSERT VALUE accounts_with_txns >= 9
SELECT 'Banking Transactions Gold Layer Verification PASSED' AS status;
