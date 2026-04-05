-- =============================================================================
-- Banking Transactions Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE banking_transactions_05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS bank.gold.dim_account (
  account_key         INT             NOT NULL,
  account_id          STRING          NOT NULL,
  account_number      STRING          NOT NULL,
  account_type        STRING          NOT NULL,
  customer_name       STRING,
  branch              STRING,
  open_date           DATE,
  status              STRING,
  customer_tier       STRING,
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/dim_account';

GRANT ADMIN ON TABLE bank.gold.dim_account TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.gold.dim_merchant (
  merchant_key        INT             NOT NULL,
  merchant_id         STRING          NOT NULL,
  merchant_name       STRING          NOT NULL,
  category            STRING,
  city                STRING,
  country             STRING,
  risk_level          STRING,
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/dim_merchant';

GRANT ADMIN ON TABLE bank.gold.dim_merchant TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.gold.dim_date (
  date_key            INT             NOT NULL,
  full_date           DATE            NOT NULL,
  day_of_week         INT,
  day_name            STRING,
  month               INT,
  month_name          STRING,
  quarter             INT,
  year                INT,
  is_weekend          BOOLEAN,
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/dim_date';

GRANT ADMIN ON TABLE bank.gold.dim_date TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.gold.fact_transactions (
  transaction_key     INT             NOT NULL,
  account_key         INT             NOT NULL,
  merchant_key        INT,
  date_key            INT,
  transaction_date    TIMESTAMP       NOT NULL,
  amount              DECIMAL(12,2)   NOT NULL,
  transaction_type    STRING          NOT NULL,
  running_balance     DECIMAL(14,2),
  fraud_score         INT,
  channel             STRING,
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/fact_transactions';

GRANT ADMIN ON TABLE bank.gold.fact_transactions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.gold.kpi_daily_volumes (
  transaction_date    DATE            NOT NULL,
  total_txns          INT             NOT NULL,
  total_amount        DECIMAL(14,2),
  avg_amount          DECIMAL(12,2),
  fraud_flagged_count INT,
  fraud_pct           DECIMAL(5,2),
  running_7d_avg_txns DECIMAL(10,2),
  running_7d_avg_amount DECIMAL(14,2),
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/kpi_daily_volumes';

GRANT ADMIN ON TABLE bank.gold.kpi_daily_volumes TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.gold.kpi_customer_health (
  from_tier           STRING          NOT NULL,
  to_tier             STRING          NOT NULL,
  migration_count     INT             NOT NULL,
  period              STRING          NOT NULL,
  avg_balance         DECIMAL(14,2),
  loaded_at           TIMESTAMP       NOT NULL
) LOCATION 'bank/gold/txn/kpi_customer_health';

GRANT ADMIN ON TABLE bank.gold.kpi_customer_health TO USER admin;
