-- =============================================================================
-- Legal Case Management Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE legal_daily_schedule
  CRON '0 7 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 2
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE legal_setup
  DESCRIPTION 'Creates zones and schemas for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

CREATE ZONE IF NOT EXISTS legal TYPE TEMP
  COMMENT 'Litigation analytics pipeline zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS legal.bronze COMMENT 'Raw cases, parties, attorneys, billings, and relationship edges';
CREATE SCHEMA IF NOT EXISTS legal.silver COMMENT 'Enriched cases with complexity scores, validated billings, party profiles';
CREATE SCHEMA IF NOT EXISTS legal.gold COMMENT 'Star schema, firm KPIs, and legal network property graph';
