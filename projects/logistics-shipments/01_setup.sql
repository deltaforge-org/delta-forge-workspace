-- =============================================================================
-- Logistics Shipments Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE logistics_6hr_schedule
  CRON '0 */6 * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE logistics_shipments_01_setup
  DESCRIPTION 'Creates zones and schemas for Logistics Shipments'
  SCHEDULE 'logistics_6hr_schedule'
  TAGS 'setup', 'logistics-shipments'
  LIFECYCLE production
;

CREATE ZONE IF NOT EXISTS logi TYPE TEMP
  COMMENT 'Global logistics shipment tracking zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS logi.bronze COMMENT 'Raw tracking events, carrier reference data, SLA contracts';
CREATE SCHEMA IF NOT EXISTS logi.silver COMMENT 'Deduplicated events, reconstructed timelines, SLA violations';
CREATE SCHEMA IF NOT EXISTS logi.gold   COMMENT 'Star schema for delivery performance and SLA compliance analytics';
