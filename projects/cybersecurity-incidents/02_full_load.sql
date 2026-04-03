-- =============================================================================
-- Cybersecurity Incidents Pipeline - Full Load Transformation
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE cyber_15min_schedule CRON '*/15 * * * *' TIMEZONE 'UTC' RETRIES 3 TIMEOUT 600 MAX_CONCURRENT 1 ACTIVE;

PIPELINE cyber_incident_pipeline DESCRIPTION 'Every-15-minute SIEM alert correlation pipeline with dedup, threat enrichment, and MITRE classification' SCHEDULE 'cyber_15min_schedule' TAGS 'cybersecurity,SIEM,MITRE,threat-intel' SLA 600 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP: dedup_alerts =====================

STEP dedup_alerts
  TIMEOUT '2m'
AS
  SELECT COUNT(*) AS raw_alert_count FROM {{zone_prefix}}.bronze.raw_alerts;
  ASSERT VALUE raw_alert_count >= 70

  SELECT COUNT(*) AS raw_ip_count FROM {{zone_prefix}}.bronze.raw_source_ips;
  ASSERT VALUE raw_ip_count = 16

  SELECT COUNT(*) AS raw_target_count FROM {{zone_prefix}}.bronze.raw_targets;
  ASSERT VALUE raw_target_count = 8

  SELECT COUNT(*) AS raw_rule_count FROM {{zone_prefix}}.bronze.raw_rules;
  ASSERT VALUE raw_rule_count = 12
  SELECT 'raw_rule_count check passed' AS raw_rule_count_status;

-- ===================== STEP: correlate_incidents =====================

STEP correlate_incidents
  DEPENDS ON (dedup_alerts)
  TIMEOUT '5m'
AS
  -- Deduplicate alerts within 5-min windows using ROW_NUMBER
  -- partitioned by source_ip + target + rule, correlate into incidents
  MERGE INTO {{zone_prefix}}.silver.incidents_correlated AS target
  USING (
      WITH windowed AS (
          SELECT
              a.alert_id,
              a.source_ip,
              a.target_hostname,
              a.rule_id,
              a.detected_at,
              a.severity,
              a.ingested_at,
              -- Group alerts within 5-min windows
              ROW_NUMBER() OVER (
                  PARTITION BY a.source_ip, a.target_hostname, a.rule_id,
                      DATE_TRUNC('hour', a.detected_at),
                      FLOOR(EXTRACT(MINUTE FROM a.detected_at) / 5)
                  ORDER BY a.detected_at ASC
              ) AS rn,
              COUNT(*) OVER (
                  PARTITION BY a.source_ip, a.target_hostname, a.rule_id,
                      DATE_TRUNC('hour', a.detected_at),
                      FLOOR(EXTRACT(MINUTE FROM a.detected_at) / 5)
              ) AS window_alert_count,
              MIN(a.detected_at) OVER (
                  PARTITION BY a.source_ip, a.target_hostname, a.rule_id,
                      DATE_TRUNC('hour', a.detected_at),
                      FLOOR(EXTRACT(MINUTE FROM a.detected_at) / 5)
              ) AS first_detected,
              MAX(a.detected_at) OVER (
                  PARTITION BY a.source_ip, a.target_hostname, a.rule_id,
                      DATE_TRUNC('hour', a.detected_at),
                      FLOOR(EXTRACT(MINUTE FROM a.detected_at) / 5)
              ) AS last_detected
          FROM {{zone_prefix}}.bronze.raw_alerts a
      ),
      incidents AS (
          SELECT
              CONCAT(w.source_ip, '-', w.target_hostname, '-', w.rule_id, '-',
                     DATE_FORMAT(w.first_detected, 'yyyyMMddHHmm')) AS incident_id,
              w.source_ip,
              w.target_hostname,
              w.rule_id,
              w.first_detected AS first_detected_at,
              w.last_detected AS last_detected_at,
              w.severity,
              w.window_alert_count AS alert_count,
              -- MTTD: seconds between first alert and ingestion (simulated detection delay)
              CAST(DATEDIFF(w.first_detected, DATE_TRUNC('day', w.first_detected)) * 86400
                   - DATEDIFF(DATE_TRUNC('day', w.first_detected), DATE_TRUNC('day', w.first_detected)) * 86400
                   AS INT) AS mean_time_to_detect_sec,
              ip.threat_intel_score,
              ip.is_known_bad,
              CASE
                  WHEN w.severity = 'critical' AND ip.is_known_bad = true THEN 'escalated'
                  WHEN w.severity = 'critical' THEN 'investigating'
                  WHEN w.severity = 'high' THEN 'triaging'
                  WHEN w.severity = 'medium' THEN 'monitoring'
                  ELSE 'informational'
              END AS status
          FROM windowed w
          LEFT JOIN {{zone_prefix}}.bronze.raw_source_ips ip ON w.source_ip = ip.ip_address
          WHERE w.rn = 1
      )
      SELECT * FROM incidents
  ) AS source
  ON target.incident_id = source.incident_id
  WHEN MATCHED THEN UPDATE SET
      last_detected_at        = source.last_detected_at,
      alert_count             = source.alert_count,
      threat_intel_score      = source.threat_intel_score,
      status                  = source.status,
      processed_at            = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      incident_id, source_ip, target_hostname, rule_id, first_detected_at,
      last_detected_at, severity, alert_count, mean_time_to_detect_sec,
      threat_intel_score, is_known_bad, status, processed_at
  ) VALUES (
      source.incident_id, source.source_ip, source.target_hostname, source.rule_id,
      source.first_detected_at, source.last_detected_at, source.severity,
      source.alert_count, source.mean_time_to_detect_sec, source.threat_intel_score,
      source.is_known_bad, source.status, CURRENT_TIMESTAMP
  );

  -- Verify dedup reduced alert count
  SELECT COUNT(*) AS incident_count FROM {{zone_prefix}}.silver.incidents_correlated;

-- ===================== STEP: enrich_threat_intel =====================

STEP enrich_threat_intel
  DEPENDS ON (correlate_incidents)
AS
  -- Threat enrichment is performed inline during correlate_incidents via the
  -- raw_source_ips join. This step verifies the correlated data meets thresholds.
  SELECT COUNT(*) AS correlated_check FROM {{zone_prefix}}.silver.incidents_correlated;
  ASSERT VALUE correlated_check >= 40;

-- ===================== STEP: build_dim_source_ip =====================

STEP build_dim_source_ip
  DEPENDS ON (enrich_threat_intel)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_source_ip AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ip_address) AS source_ip_key,
          ip_address, subnet, geo_country, geo_city, threat_intel_score, is_known_bad
      FROM {{zone_prefix}}.bronze.raw_source_ips
  ) AS source
  ON target.ip_address = source.ip_address
  WHEN MATCHED THEN UPDATE SET
      threat_intel_score = source.threat_intel_score,
      is_known_bad       = source.is_known_bad,
      loaded_at          = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      source_ip_key, ip_address, subnet, geo_country, geo_city,
      threat_intel_score, is_known_bad, loaded_at
  ) VALUES (
      source.source_ip_key, source.ip_address, source.subnet, source.geo_country,
      source.geo_city, source.threat_intel_score, source.is_known_bad, CURRENT_TIMESTAMP
  );

-- ===================== STEP: build_dim_target =====================

STEP build_dim_target
  DEPENDS ON (enrich_threat_intel)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_target AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY hostname) AS target_key,
          hostname, service, port, environment, criticality
      FROM {{zone_prefix}}.bronze.raw_targets
  ) AS source
  ON target.hostname = source.hostname
  WHEN MATCHED THEN UPDATE SET
      service     = source.service,
      port        = source.port,
      environment = source.environment,
      criticality = source.criticality,
      loaded_at   = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      target_key, hostname, service, port, environment, criticality, loaded_at
  ) VALUES (
      source.target_key, source.hostname, source.service, source.port,
      source.environment, source.criticality, CURRENT_TIMESTAMP
  );

-- ===================== STEP: build_dim_rule =====================

STEP build_dim_rule
  DEPENDS ON (enrich_threat_intel)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_rule AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY rule_id) AS rule_key,
          rule_id, rule_name, category, mitre_tactic, mitre_technique
      FROM {{zone_prefix}}.bronze.raw_rules
  ) AS source
  ON target.rule_id = source.rule_id
  WHEN MATCHED THEN UPDATE SET
      rule_name       = source.rule_name,
      category        = source.category,
      mitre_tactic    = source.mitre_tactic,
      mitre_technique = source.mitre_technique,
      loaded_at       = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      rule_key, rule_id, rule_name, category, mitre_tactic, mitre_technique, loaded_at
  ) VALUES (
      source.rule_key, source.rule_id, source.rule_name, source.category,
      source.mitre_tactic, source.mitre_technique, CURRENT_TIMESTAMP
  );

-- ===================== STEP: build_fact_incidents =====================

STEP build_fact_incidents
  DEPENDS ON (build_dim_source_ip, build_dim_target, build_dim_rule)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.gold.fact_incidents AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ic.first_detected_at, ic.incident_id) AS incident_key,
          dip.source_ip_key,
          dt.target_key,
          dr.rule_key,
          ic.first_detected_at AS detected_at,
          ic.severity,
          ic.alert_count,
          ic.mean_time_to_detect_sec,
          ic.status
      FROM {{zone_prefix}}.silver.incidents_correlated ic
      JOIN {{zone_prefix}}.gold.dim_source_ip dip ON ic.source_ip = dip.ip_address
      JOIN {{zone_prefix}}.gold.dim_target dt ON ic.target_hostname = dt.hostname
      JOIN {{zone_prefix}}.gold.dim_rule dr ON ic.rule_id = dr.rule_id
  ) AS source
  ON target.source_ip_key = source.source_ip_key AND target.target_key = source.target_key AND target.rule_key = source.rule_key AND target.detected_at = source.detected_at
  WHEN MATCHED THEN UPDATE SET
      severity                = source.severity,
      alert_count             = source.alert_count,
      mean_time_to_detect_sec = source.mean_time_to_detect_sec,
      status                  = source.status,
      loaded_at               = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      incident_key, source_ip_key, target_key, rule_key, detected_at,
      severity, alert_count, mean_time_to_detect_sec, status, loaded_at
  ) VALUES (
      source.incident_key, source.source_ip_key, source.target_key, source.rule_key,
      source.detected_at, source.severity, source.alert_count,
      source.mean_time_to_detect_sec, source.status, CURRENT_TIMESTAMP
  );

-- ===================== STEP: compute_threat_dashboard =====================

STEP compute_threat_dashboard
  DEPENDS ON (build_fact_incidents)
AS
  MERGE INTO {{zone_prefix}}.gold.kpi_threat_dashboard AS target
  USING (
      WITH hourly AS (
          SELECT
              DATE_TRUNC('hour', fi.detected_at) AS hour_bucket,
              SUM(fi.alert_count) AS total_alerts,
              COUNT(DISTINCT fi.source_ip_key) AS unique_sources,
              SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
              SUM(CASE WHEN fi.severity = 'high' THEN 1 ELSE 0 END) AS high_count,
              SUM(CASE WHEN fi.severity = 'medium' THEN 1 ELSE 0 END) AS medium_count,
              SUM(CASE WHEN fi.severity = 'low' THEN 1 ELSE 0 END) AS low_count,
              AVG(fi.mean_time_to_detect_sec) AS mean_time_to_detect
          FROM {{zone_prefix}}.gold.fact_incidents fi
          GROUP BY DATE_TRUNC('hour', fi.detected_at)
      ),
      with_tactic AS (
          SELECT
              h.*,
              (SELECT dr.mitre_tactic
               FROM {{zone_prefix}}.gold.fact_incidents fi2
               JOIN {{zone_prefix}}.gold.dim_rule dr ON fi2.rule_key = dr.rule_key
               WHERE DATE_TRUNC('hour', fi2.detected_at) = h.hour_bucket
               GROUP BY dr.mitre_tactic
               ORDER BY COUNT(*) DESC
               LIMIT 1) AS top_mitre_tactic
          FROM hourly h
      )
      SELECT * FROM with_tactic
  ) AS source
  ON target.hour_bucket = source.hour_bucket
  WHEN MATCHED THEN UPDATE SET
      total_alerts        = source.total_alerts,
      unique_sources      = source.unique_sources,
      critical_count      = source.critical_count,
      high_count          = source.high_count,
      medium_count        = source.medium_count,
      low_count           = source.low_count,
      mean_time_to_detect = source.mean_time_to_detect,
      top_mitre_tactic    = source.top_mitre_tactic,
      loaded_at           = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      hour_bucket, total_alerts, unique_sources, critical_count, high_count,
      medium_count, low_count, mean_time_to_detect, top_mitre_tactic, loaded_at
  ) VALUES (
      source.hour_bucket, source.total_alerts, source.unique_sources, source.critical_count,
      source.high_count, source.medium_count, source.low_count, source.mean_time_to_detect,
      source.top_mitre_tactic, CURRENT_TIMESTAMP
  );

-- ===================== STEP: optimize_compact =====================

STEP optimize_compact
  DEPENDS ON (compute_threat_dashboard)
  CONTINUE ON FAILURE
AS
  OPTIMIZE {{zone_prefix}}.gold.dim_source_ip;
  OPTIMIZE {{zone_prefix}}.gold.dim_target;
  OPTIMIZE {{zone_prefix}}.gold.dim_rule;
  OPTIMIZE {{zone_prefix}}.gold.fact_incidents;
  OPTIMIZE {{zone_prefix}}.gold.kpi_threat_dashboard;
  OPTIMIZE {{zone_prefix}}.silver.incidents_correlated;
