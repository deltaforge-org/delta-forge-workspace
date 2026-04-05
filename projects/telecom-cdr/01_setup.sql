-- =============================================================================
-- Telecom CDR Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE telecom_daily_schedule
  CRON '0 2 * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE 01_setup
  DESCRIPTION 'Creates zones and schemas for Telecom CDR'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'setup', 'telecom-cdr'
  LIFECYCLE production
;

CREATE ZONE IF NOT EXISTS telco TYPE TEMP
  COMMENT 'Telecom CDR project zone — schema evolution, session reconstruction, churn scoring';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS telco.bronze COMMENT 'Raw CDR feeds across 3 schema versions and reference data';
CREATE SCHEMA IF NOT EXISTS telco.silver COMMENT 'Unified CDR with schema evolution merge, subscriber profiles, reconstructed sessions';
CREATE SCHEMA IF NOT EXISTS telco.gold   COMMENT 'Star schema for network quality, churn risk, and revenue analytics';
