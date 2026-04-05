-- =============================================================================
-- Cybersecurity Incidents Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== BRONZE: 3 ALERT SOURCE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS cyber.bronze.raw_firewall_alerts (
  alert_id         STRING      NOT NULL,
  source_ip        STRING      NOT NULL,
  target_host      STRING      NOT NULL,
  rule_id          STRING      NOT NULL,
  detected_at      TIMESTAMP   NOT NULL,
  severity_score   INT         NOT NULL,
  severity         STRING      NOT NULL,
  bytes_transferred BIGINT,
  protocol         STRING,
  raw_log          STRING,
  ingested_at      TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/bronze/raw_firewall_alerts';

CREATE DELTA TABLE IF NOT EXISTS cyber.bronze.raw_ids_alerts (
  alert_id         STRING      NOT NULL,
  source_ip        STRING      NOT NULL,
  target_host      STRING      NOT NULL,
  rule_id          STRING      NOT NULL,
  detected_at      TIMESTAMP   NOT NULL,
  severity_score   INT         NOT NULL,
  severity         STRING      NOT NULL,
  signature_id     STRING,
  protocol         STRING,
  raw_log          STRING,
  ingested_at      TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/bronze/raw_ids_alerts';

CREATE DELTA TABLE IF NOT EXISTS cyber.bronze.raw_endpoint_alerts (
  alert_id         STRING      NOT NULL,
  source_ip        STRING      NOT NULL,
  target_host      STRING      NOT NULL,
  rule_id          STRING      NOT NULL,
  detected_at      TIMESTAMP   NOT NULL,
  severity_score   INT         NOT NULL,
  severity         STRING      NOT NULL,
  process_name     STRING,
  file_hash        STRING,
  raw_log          STRING,
  ingested_at      TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/bronze/raw_endpoint_alerts';

-- ===================== BRONZE: THREAT INTELLIGENCE =====================

CREATE DELTA TABLE IF NOT EXISTS cyber.bronze.raw_threat_intel (
  ip_address       STRING      NOT NULL,
  subnet           STRING,
  geo_country      STRING,
  geo_city         STRING,
  threat_score     INT         NOT NULL,
  threat_category  STRING,
  is_known_bad     BOOLEAN     NOT NULL,
  first_seen       DATE,
  last_seen        DATE,
  ingested_at      TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/bronze/raw_threat_intel';

CREATE DELTA TABLE IF NOT EXISTS cyber.bronze.raw_mitre_techniques (
  technique_id     STRING      NOT NULL,
  technique_name   STRING      NOT NULL,
  tactic           STRING      NOT NULL,
  tactic_id        STRING,
  description      STRING,
  severity_weight  INT,
  ingested_at      TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/bronze/raw_mitre_techniques';
