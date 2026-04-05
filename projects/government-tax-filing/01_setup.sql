-- =============================================================================
-- Government Tax Filing Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE tax_daily_schedule
  CRON '0 1 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE 01_setup
  DESCRIPTION 'Creates zones and schemas for Government Tax Filing'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'setup', 'government-tax-filing'
  LIFECYCLE production
;

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS tax TYPE TEMP
  COMMENT 'Government tax filing project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS tax.bronze COMMENT 'Raw tax filing source data — filings, amendments, taxpayers, jurisdictions, preparers';
CREATE SCHEMA IF NOT EXISTS tax.silver COMMENT 'Immutable filings, applied amendments, taxpayer profiles, CDF audit trail';
CREATE SCHEMA IF NOT EXISTS tax.gold   COMMENT 'Filing star schema with amendment-aware facts, revenue and preparer KPIs';
