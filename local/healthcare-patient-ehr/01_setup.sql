-- =============================================================================
-- Healthcare Patient EHR Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE ehr_daily_schedule
  CRON '0 5 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE ehr_setup
  DESCRIPTION 'Creates zones and schemas for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS ehr TYPE TEMP
  COMMENT 'Healthcare EHR pipeline zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS ehr.bronze COMMENT 'Raw electronic health records and reference data';
CREATE SCHEMA IF NOT EXISTS ehr.silver COMMENT 'SCD2 patient dimension, cleaned admissions, CDF audit log';
CREATE SCHEMA IF NOT EXISTS ehr.gold COMMENT 'Admissions star schema with point-in-time joins and KPIs';
