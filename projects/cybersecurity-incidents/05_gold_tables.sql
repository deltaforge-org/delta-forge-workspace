-- =============================================================================
-- Cybersecurity Incidents Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE cybersecurity_incidents_05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.dim_source_ip (
  source_ip_key    INT         NOT NULL,
  ip_address       STRING      NOT NULL,
  subnet           STRING,
  geo_country      STRING,
  geo_city         STRING,
  threat_score     INT,
  threat_category  STRING,
  is_known_bad     BOOLEAN,
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/dim_source_ip';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.dim_target (
  target_key       INT         NOT NULL,
  hostname         STRING      NOT NULL,
  environment      STRING,
  criticality      STRING,
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/dim_target';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.dim_rule (
  rule_key         INT         NOT NULL,
  rule_id          STRING      NOT NULL,
  rule_name        STRING,
  category         STRING,
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/dim_rule';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.dim_mitre (
  mitre_key        INT         NOT NULL,
  technique_id     STRING      NOT NULL,
  technique_name   STRING      NOT NULL,
  tactic           STRING      NOT NULL,
  tactic_id        STRING,
  severity_weight  INT,
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/dim_mitre';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.fact_incidents (
  incident_key        INT         NOT NULL,
  source_ip_key       INT         NOT NULL,
  target_key          INT         NOT NULL,
  rule_key            INT         NOT NULL,
  mitre_key           INT,
  detected_at         TIMESTAMP   NOT NULL,
  severity            STRING      NOT NULL,
  severity_score      INT         NOT NULL,
  alert_count         INT         NOT NULL,
  distinct_targets    INT,
  distinct_rules      INT,
  duration_minutes    INT,
  threat_score        INT,
  combined_risk_score INT,
  mitre_tactic        STRING,
  status              STRING      NOT NULL,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/fact_incidents';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.kpi_threat_dashboard (
  hour_bucket      TIMESTAMP   NOT NULL,
  total_alerts     INT         NOT NULL,
  unique_sources   INT,
  unique_targets   INT,
  critical_count   INT,
  high_count       INT,
  medium_count     INT,
  low_count        INT,
  avg_severity_score DECIMAL(5,1),
  top_mitre_tactic STRING,
  top_source_ip    STRING,
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/kpi_threat_dashboard';

CREATE DELTA TABLE IF NOT EXISTS cyber.gold.kpi_response_metrics (
  severity         STRING      NOT NULL,
  period           STRING      NOT NULL,
  incident_count   INT,
  avg_alert_count  DECIMAL(5,1),
  avg_duration_min DECIMAL(8,1),
  max_duration_min INT,
  escalated_count  INT,
  escalated_pct    DECIMAL(5,2),
  avg_threat_score DECIMAL(5,1),
  loaded_at        TIMESTAMP   NOT NULL
) LOCATION 'cyber/cyber/gold/kpi_response_metrics';

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE cyber.bronze.raw_firewall_alerts TO USER admin;
GRANT ADMIN ON TABLE cyber.bronze.raw_ids_alerts TO USER admin;
GRANT ADMIN ON TABLE cyber.bronze.raw_endpoint_alerts TO USER admin;
GRANT ADMIN ON TABLE cyber.bronze.raw_threat_intel TO USER admin;
GRANT ADMIN ON TABLE cyber.bronze.raw_mitre_techniques TO USER admin;
GRANT ADMIN ON TABLE cyber.silver.alerts_deduped TO USER admin;
GRANT ADMIN ON TABLE cyber.silver.incidents_correlated TO USER admin;
GRANT ADMIN ON TABLE cyber.silver.threat_enriched TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.dim_source_ip TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.dim_target TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.dim_rule TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.dim_mitre TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.fact_incidents TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.kpi_threat_dashboard TO USER admin;
GRANT ADMIN ON TABLE cyber.gold.kpi_response_metrics TO USER admin;
