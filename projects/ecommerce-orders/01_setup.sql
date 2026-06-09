-- =============================================================================
-- E-commerce Orders Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE ecommerce_30min_schedule
  CRON '*/30 * * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 1800
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE ecommerce_setup
  DESCRIPTION 'Creates zones and schemas for E-commerce Orders'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'setup', 'ecommerce-orders'
  LIFECYCLE production
;

-- ===================== ZONE =====================

CREATE ZONE IF NOT EXISTS ecom TYPE TEMP
  COMMENT 'Omnichannel e-commerce project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS ecom.bronze COMMENT 'Raw order feeds from web, mobile, POS, browsing events';
CREATE SCHEMA IF NOT EXISTS ecom.silver COMMENT 'Unified orders, RFM scoring, inventory CDF, sessions';
CREATE SCHEMA IF NOT EXISTS ecom.gold   COMMENT 'Star schema for sales, funnel, and channel analytics';
