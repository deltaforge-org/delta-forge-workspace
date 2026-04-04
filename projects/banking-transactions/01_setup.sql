-- =============================================================================
-- Banking Transactions Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE bank_4h_schedule
  CRON '0 */4 * * *'
  TIMEZONE 'UTC'
  RETRIES 3
  TIMEOUT 1800
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE bank_setup
  DESCRIPTION 'Creates zones and schemas for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS bank TYPE TEMP
  COMMENT 'Banking transactions pipeline zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS bank.bronze COMMENT 'Raw transaction feeds, accounts, and merchant reference data';
CREATE SCHEMA IF NOT EXISTS bank.silver COMMENT 'SCD2 customer dimension, enriched transactions, CDF balance snapshots';
CREATE SCHEMA IF NOT EXISTS bank.gold COMMENT 'Transaction analytics star schema with fraud scoring and tier migration KPIs';
