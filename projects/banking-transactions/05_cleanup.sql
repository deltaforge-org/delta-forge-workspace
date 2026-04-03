-- =============================================================================
-- Banking Transactions Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.

PIPELINE banking_transactions_cleanup
  DESCRIPTION 'Cleanup pipeline for Banking Transactions — drops all objects including SCD2 customer_dim, CDF balance_snapshots, and pseudonymisation rules. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'banking-transactions'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON bank.silver.customer_dim (account_number);
DROP PSEUDONYMISATION RULE ON bank.silver.customer_dim (customer_name);
DROP PSEUDONYMISATION RULE ON bank.gold.dim_account (account_number);
DROP PSEUDONYMISATION RULE ON bank.gold.dim_account (customer_name);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS bank.gold.kpi_customer_health WITH FILES;
DROP DELTA TABLE IF EXISTS bank.gold.kpi_daily_volumes WITH FILES;
DROP DELTA TABLE IF EXISTS bank.gold.fact_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS bank.gold.dim_date WITH FILES;
DROP DELTA TABLE IF EXISTS bank.gold.dim_merchant WITH FILES;
DROP DELTA TABLE IF EXISTS bank.gold.dim_account WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS bank.silver.balance_snapshots WITH FILES;
DROP DELTA TABLE IF EXISTS bank.silver.transactions_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS bank.silver.customer_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS bank.bronze.raw_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS bank.bronze.raw_merchants WITH FILES;
DROP DELTA TABLE IF EXISTS bank.bronze.raw_accounts WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS bank.gold;
DROP SCHEMA IF EXISTS bank.silver;
DROP SCHEMA IF EXISTS bank.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS bank;
