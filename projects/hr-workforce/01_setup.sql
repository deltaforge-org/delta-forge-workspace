-- =============================================================================
-- HR Workforce Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE hr_daily_schedule
  CRON '0 6 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE 01_setup
  DESCRIPTION 'Creates zones and schemas for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS hr TYPE TEMP
  COMMENT 'HR workforce analytics pipeline zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS hr.bronze COMMENT 'Raw employee, compensation event, department, and position data';
CREATE SCHEMA IF NOT EXISTS hr.silver COMMENT 'SCD2 employee dimension, enriched comp events, CDF org change log';
CREATE SCHEMA IF NOT EXISTS hr.gold COMMENT 'Compensation star schema, workforce KPIs, and retention risk scores';
