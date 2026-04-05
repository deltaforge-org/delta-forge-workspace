-- =============================================================================
-- Banking Transactions Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE banking_transactions_04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- SCD2 customer dimension with CDF for tier change tracking
CREATE DELTA TABLE IF NOT EXISTS bank.silver.customer_dim (
  customer_id         BIGINT      NOT NULL,
  account_id          STRING      NOT NULL,
  account_number      STRING      NOT NULL,
  account_type        STRING      NOT NULL,
  customer_name       STRING,
  branch              STRING,
  open_date           DATE,
  status              STRING,
  customer_tier       STRING,
  valid_from          DATE        NOT NULL,
  valid_to            DATE,
  is_current          BOOLEAN     NOT NULL,
  updated_at          TIMESTAMP   NOT NULL
) LOCATION 'bank/silver/txn/customer_dim'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE bank.silver.customer_dim TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS bank.silver.transactions_enriched (
  transaction_id      STRING          NOT NULL,
  account_id          STRING          NOT NULL,
  merchant_id         STRING,
  merchant_category   STRING,
  merchant_risk       STRING,
  transaction_date    TIMESTAMP       NOT NULL,
  amount              DECIMAL(12,2)   NOT NULL,
  transaction_type    STRING          NOT NULL,
  channel             STRING,
  running_balance     DECIMAL(14,2),
  fraud_score         INT,
  is_suspicious       BOOLEAN,
  prev_txn_amount     DECIMAL(12,2),
  time_since_prev_sec BIGINT,
  txn_velocity_5      INT,
  ingested_at         TIMESTAMP       NOT NULL,
  processed_at        TIMESTAMP       NOT NULL
) LOCATION 'bank/silver/txn/transactions_enriched';

GRANT ADMIN ON TABLE bank.silver.transactions_enriched TO USER admin;

-- CDF-driven balance snapshots: materialized from customer_dim changes
CREATE DELTA TABLE IF NOT EXISTS bank.silver.balance_snapshots (
  snapshot_id         BIGINT      NOT NULL,
  account_id          STRING      NOT NULL,
  customer_name       STRING,
  old_tier            STRING,
  new_tier            STRING,
  change_type         STRING      NOT NULL,
  snapshot_timestamp  TIMESTAMP   NOT NULL
) LOCATION 'bank/silver/txn/balance_snapshots';

GRANT ADMIN ON TABLE bank.silver.balance_snapshots TO USER admin;
