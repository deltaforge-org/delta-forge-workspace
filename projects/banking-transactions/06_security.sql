-- =============================================================================
-- Banking Transactions Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE 06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON bank.silver.customer_dim (account_number);
CREATE PSEUDONYMISATION RULE ON bank.silver.customer_dim (account_number) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'bank_pii_salt_2024');

DROP PSEUDONYMISATION RULE IF EXISTS ON bank.silver.customer_dim (customer_name);
CREATE PSEUDONYMISATION RULE ON bank.silver.customer_dim (customer_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'bank_pii_salt_2024');

DROP PSEUDONYMISATION RULE IF EXISTS ON bank.gold.dim_account (account_number);
CREATE PSEUDONYMISATION RULE ON bank.gold.dim_account (account_number) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'bank_pii_salt_2024');

DROP PSEUDONYMISATION RULE IF EXISTS ON bank.gold.dim_account (customer_name);
CREATE PSEUDONYMISATION RULE ON bank.gold.dim_account (customer_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'bank_pii_salt_2024');
