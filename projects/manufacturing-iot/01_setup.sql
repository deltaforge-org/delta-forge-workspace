-- =============================================================================
-- Manufacturing IoT Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE manufacturing_2hr_schedule
  CRON '0 */2 * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE manufacturing_setup
  DESCRIPTION 'Creates zones and schemas for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;

CREATE ZONE IF NOT EXISTS mfg TYPE TEMP
  COMMENT 'Manufacturing IoT project zone — anomaly detection, OEE, equipment status';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS mfg.bronze COMMENT 'Raw IoT sensor readings, sensor metadata, production lines, shifts, and targets';
CREATE SCHEMA IF NOT EXISTS mfg.silver COMMENT 'Validated readings with moving averages, anomaly flags, and equipment uptime status';
CREATE SCHEMA IF NOT EXISTS mfg.gold   COMMENT 'Star schema for OEE, anomaly trends, and production analytics';
