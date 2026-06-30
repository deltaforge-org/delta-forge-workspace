-- =============================================================================
-- Cybersecurity Incidents Pipeline - Zone & Schema Setup
-- =============================================================================

SCHEDULE cyber_15min_schedule
  CRON '*/15 * * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 600
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE cyber_setup
  DESCRIPTION 'Creates zones and schemas for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS cyber TYPE TEMP
  COMMENT 'Security Operations Center SIEM analytics zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS cyber.bronze COMMENT 'Raw SIEM alert feeds from 3 sources plus threat intelligence';
CREATE SCHEMA IF NOT EXISTS cyber.silver COMMENT 'Deduplicated alerts, correlated incidents, threat-enriched data';
CREATE SCHEMA IF NOT EXISTS cyber.gold   COMMENT 'Threat dashboard star schema with MITRE classification';
