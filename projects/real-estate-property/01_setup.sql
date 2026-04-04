-- =============================================================================
-- Real Estate Property Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE realty_daily_schedule
  CRON '0 9 * * *'
  TIMEZONE 'America/New_York'
  RETRIES 2
  TIMEOUT 3600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE realty_setup
  DESCRIPTION 'Creates zones and schemas for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

CREATE ZONE IF NOT EXISTS realty TYPE TEMP
  COMMENT 'County assessor real estate project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS realty.bronze COMMENT 'Raw property, assessment, transaction, neighborhood, and agent data';
CREATE SCHEMA IF NOT EXISTS realty.silver COMMENT 'SCD2 property dimension, enriched transactions, correction log';
CREATE SCHEMA IF NOT EXISTS realty.gold   COMMENT 'Property star schema, market trends, and assessment accuracy KPIs';
