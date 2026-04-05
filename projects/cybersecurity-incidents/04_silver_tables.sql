-- =============================================================================
-- Cybersecurity Incidents Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS cyber.silver.alerts_deduped (
  alert_id         STRING      NOT NULL,
  source           STRING      NOT NULL,
  source_ip        STRING      NOT NULL,
  target_host      STRING      NOT NULL,
  rule_id          STRING      NOT NULL,
  detected_at      TIMESTAMP   NOT NULL,
  severity_score   INT         NOT NULL,
  severity         STRING      NOT NULL,
  protocol         STRING,
  raw_log          STRING,
  dedup_bucket     STRING,
  deduped_at       TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/silver/alerts_deduped'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE DELTA TABLE IF NOT EXISTS cyber.silver.incidents_correlated (
  incident_id       STRING      NOT NULL,
  source_ip         STRING      NOT NULL,
  primary_target    STRING      NOT NULL,
  primary_rule_id   STRING      NOT NULL,
  first_detected_at TIMESTAMP   NOT NULL,
  last_detected_at  TIMESTAMP   NOT NULL,
  max_severity      STRING      NOT NULL,
  max_severity_score INT        NOT NULL,
  alert_count       INT         NOT NULL,
  distinct_targets  INT,
  distinct_rules    INT,
  duration_minutes  INT,
  status            STRING      NOT NULL,
  correlated_at     TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/silver/incidents_correlated';

CREATE DELTA TABLE IF NOT EXISTS cyber.silver.threat_enriched (
  incident_id       STRING      NOT NULL,
  source_ip         STRING      NOT NULL,
  primary_target    STRING,
  primary_rule_id   STRING,
  first_detected_at TIMESTAMP,
  max_severity      STRING,
  max_severity_score INT,
  alert_count       INT,
  threat_score      INT,
  threat_category   STRING,
  is_known_bad      BOOLEAN,
  mitre_tactic      STRING,
  mitre_technique   STRING,
  technique_name    STRING,
  combined_risk_score INT,
  enriched_at       TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/silver/threat_enriched';
