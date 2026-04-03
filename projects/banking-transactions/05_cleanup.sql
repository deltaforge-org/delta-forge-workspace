-- =============================================================================
-- Banking Transactions Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE banking_transactions_cleanup
  DESCRIPTION 'Cleanup pipeline for Banking Transactions — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'banking-transactions'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.accounts_cdf (account_number);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.accounts_cdf (customer_name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_account (account_number);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_account (customer_name);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_daily_volumes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_merchant WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_account WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.transactions_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.accounts_cdf WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_merchants WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_accounts WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS {{zone_prefix}};
