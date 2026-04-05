-- =============================================================================
-- Banking Transactions Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS bank.bronze.raw_accounts (
  account_id          STRING      NOT NULL,
  account_number      STRING      NOT NULL,
  account_type        STRING      NOT NULL,
  customer_name       STRING,
  branch              STRING,
  open_date           DATE,
  status              STRING,
  current_balance     DECIMAL(14,2),
  customer_tier       STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'bank/bronze/txn/raw_accounts';

GRANT ADMIN ON TABLE bank.bronze.raw_accounts TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.bronze.raw_merchants (
  merchant_id         STRING      NOT NULL,
  merchant_name       STRING      NOT NULL,
  category            STRING,
  city                STRING,
  country             STRING,
  risk_level          STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'bank/bronze/txn/raw_merchants';

GRANT ADMIN ON TABLE bank.bronze.raw_merchants TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.bronze.raw_transactions (
  transaction_id      STRING      NOT NULL,
  account_id          STRING      NOT NULL,
  merchant_id         STRING,
  transaction_date    TIMESTAMP   NOT NULL,
  amount              DECIMAL(12,2) NOT NULL,
  transaction_type    STRING      NOT NULL,
  channel             STRING,
  is_suspicious       BOOLEAN,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'bank/bronze/txn/raw_transactions';

GRANT ADMIN ON TABLE bank.bronze.raw_transactions TO USER admin;
