-- =============================================================================
-- Insurance Claims Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE ins_weekly_schedule
  CRON '0 7 * * 1'
  TIMEZONE 'America/Chicago'
  RETRIES 0
  TIMEOUT 7200
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE 01_setup
  DESCRIPTION 'Creates zones and schemas for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- ===================== ZONE =====================

CREATE ZONE IF NOT EXISTS ins TYPE TEMP
  COMMENT 'Property and casualty insurance project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS ins.bronze COMMENT 'Raw policy, claims, claimant, and adjuster feeds';
CREATE SCHEMA IF NOT EXISTS ins.silver COMMENT 'SCD2 policy dimension, enriched claims, actuarial CDF';
CREATE SCHEMA IF NOT EXISTS ins.gold   COMMENT 'Claims analytics star schema with loss ratio and adjuster KPIs';
