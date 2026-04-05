-- =============================================================================
-- Cybersecurity Incidents Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- Multi-source SIEM pipeline: 3 alert feeds -> dedup 5-min windows ->
-- correlate incidents via gap detection -> enrich threat intel + MITRE ->
-- star schema with 4 dimensions + threat dashboard + response metrics.
-- =============================================================================

PIPELINE 07_full_load
  DESCRIPTION 'Every-15-minute SIEM pipeline: 3-source ingestion, 5-min dedup, incident correlation, MITRE classification, threat dashboard'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'cybersecurity,SIEM,MITRE,threat-intel,multi-source'
  SLA 600
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze_3_sources =====================
-- Verify all 3 alert sources and reference tables have expected counts.

ASSERT ROW_COUNT = 30
SELECT COUNT(*) AS fw_count FROM cyber.bronze.raw_firewall_alerts;

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS ids_count FROM cyber.bronze.raw_ids_alerts;

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS ep_count FROM cyber.bronze.raw_endpoint_alerts;

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS threat_count FROM cyber.bronze.raw_threat_intel;

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS mitre_count FROM cyber.bronze.raw_mitre_techniques;

-- ===================== merge_firewall =====================
-- Ingest firewall alerts into unified deduped table.

MERGE INTO cyber.silver.alerts_deduped AS target
USING (
    SELECT
        alert_id,
        'firewall' AS source,
        source_ip,
        target_host,
        rule_id,
        detected_at,
        severity_score,
        severity,
        protocol,
        raw_log,
        CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
               DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
               CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_firewall_alerts
) AS source
ON target.alert_id = source.alert_id AND target.source = 'firewall'
WHEN NOT MATCHED THEN INSERT (
    alert_id, source, source_ip, target_host, rule_id, detected_at,
    severity_score, severity, protocol, raw_log, dedup_bucket, deduped_at
) VALUES (
    source.alert_id, source.source, source.source_ip, source.target_host,
    source.rule_id, source.detected_at, source.severity_score, source.severity,
    source.protocol, source.raw_log, source.dedup_bucket, CURRENT_TIMESTAMP
);

-- ===================== merge_ids =====================
-- Ingest IDS alerts into unified deduped table.

MERGE INTO cyber.silver.alerts_deduped AS target
USING (
    SELECT
        alert_id,
        'ids' AS source,
        source_ip,
        target_host,
        rule_id,
        detected_at,
        severity_score,
        severity,
        protocol,
        raw_log,
        CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
               DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
               CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_ids_alerts
) AS source
ON target.alert_id = source.alert_id AND target.source = 'ids'
WHEN NOT MATCHED THEN INSERT (
    alert_id, source, source_ip, target_host, rule_id, detected_at,
    severity_score, severity, protocol, raw_log, dedup_bucket, deduped_at
) VALUES (
    source.alert_id, source.source, source.source_ip, source.target_host,
    source.rule_id, source.detected_at, source.severity_score, source.severity,
    source.protocol, source.raw_log, source.dedup_bucket, CURRENT_TIMESTAMP
);

-- ===================== merge_endpoint =====================
-- Ingest endpoint alerts into unified deduped table.

MERGE INTO cyber.silver.alerts_deduped AS target
USING (
    SELECT
        alert_id,
        'endpoint' AS source,
        source_ip,
        target_host,
        rule_id,
        detected_at,
        severity_score,
        severity,
        NULL AS protocol,
        raw_log,
        CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
               DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
               CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_endpoint_alerts
) AS source
ON target.alert_id = source.alert_id AND target.source = 'endpoint'
WHEN NOT MATCHED THEN INSERT (
    alert_id, source, source_ip, target_host, rule_id, detected_at,
    severity_score, severity, protocol, raw_log, dedup_bucket, deduped_at
) VALUES (
    source.alert_id, source.source, source.source_ip, source.target_host,
    source.rule_id, source.detected_at, source.severity_score, source.severity,
    source.protocol, source.raw_log, source.dedup_bucket, CURRENT_TIMESTAMP
);

-- ===================== dedup_5min_windows =====================
-- Deduplicate within 5-min windows using ROW_NUMBER partitioned by
-- source_ip + target_host + rule_id within 5-min time buckets.
-- Only keep the first alert per window. This transforms the raw
-- unified alerts into a clean deduplicated set.
-- Note: the dedup is applied as a view/filter in downstream steps
-- by selecting rn=1 from windowed alerts. The alerts_deduped table
-- already has all raw merged alerts; the dedup logic filters them.

-- Verify total alerts loaded from 3 sources
SELECT COUNT(*) AS total_merged_alerts FROM cyber.silver.alerts_deduped;

-- Count unique after 5-min window dedup
SELECT COUNT(*) AS unique_after_dedup FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY source_ip, target_host, rule_id, dedup_bucket
            ORDER BY detected_at ASC
        ) AS rn
    FROM cyber.silver.alerts_deduped
) WHERE rn = 1;

-- ===================== correlate_incidents =====================
-- Group alerts into incidents: same source_ip within 15-min window.
-- Uses window-based gap detection: if time between consecutive alerts
-- from same source exceeds 15 min, start a new incident.
-- Severity escalation: incident severity = MAX of constituent alerts.

MERGE INTO cyber.silver.incidents_correlated AS target
USING (
    WITH deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY source_ip, target_host, rule_id, dedup_bucket
                ORDER BY detected_at ASC
            ) AS rn
        FROM cyber.silver.alerts_deduped
    ),
    clean_alerts AS (
        SELECT * FROM deduped WHERE rn = 1
    ),
    gap_detect AS (
        SELECT
            source_ip,
            target_host,
            rule_id,
            detected_at,
            severity,
            severity_score,
            CASE
                WHEN DATEDIFF(detected_at,
                    LAG(detected_at) OVER (PARTITION BY source_ip ORDER BY detected_at)
                ) * 24 * 60 > 15
                OR LAG(detected_at) OVER (PARTITION BY source_ip ORDER BY detected_at) IS NULL
                THEN 1
                ELSE 0
            END AS new_incident_flag
        FROM clean_alerts
    ),
    incident_groups AS (
        SELECT
            source_ip,
            target_host,
            rule_id,
            detected_at,
            severity,
            severity_score,
            SUM(new_incident_flag) OVER (
                PARTITION BY source_ip ORDER BY detected_at
            ) AS incident_group_id
        FROM gap_detect
    ),
    incidents AS (
        SELECT
            CONCAT(source_ip, '-INC-', CAST(incident_group_id AS STRING)) AS incident_id,
            source_ip,
            FIRST_VALUE(target_host) OVER (
                PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC
            ) AS primary_target,
            FIRST_VALUE(rule_id) OVER (
                PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC
            ) AS primary_rule_id,
            MIN(detected_at) OVER (PARTITION BY source_ip, incident_group_id) AS first_detected_at,
            MAX(detected_at) OVER (PARTITION BY source_ip, incident_group_id) AS last_detected_at,
            FIRST_VALUE(severity) OVER (
                PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC
            ) AS max_severity,
            MAX(severity_score) OVER (PARTITION BY source_ip, incident_group_id) AS max_severity_score,
            COUNT(*) OVER (PARTITION BY source_ip, incident_group_id) AS alert_count,
            COUNT(DISTINCT target_host) OVER (PARTITION BY source_ip, incident_group_id) AS distinct_targets,
            COUNT(DISTINCT rule_id) OVER (PARTITION BY source_ip, incident_group_id) AS distinct_rules,
            CAST(DATEDIFF(
                MAX(detected_at) OVER (PARTITION BY source_ip, incident_group_id),
                MIN(detected_at) OVER (PARTITION BY source_ip, incident_group_id)
            ) * 24 * 60 AS INT) AS duration_minutes,
            ROW_NUMBER() OVER (
                PARTITION BY source_ip, incident_group_id ORDER BY detected_at ASC
            ) AS rn_in_group
        FROM incident_groups
    )
    SELECT
        incident_id, source_ip, primary_target, primary_rule_id,
        first_detected_at, last_detected_at, max_severity, max_severity_score,
        alert_count, distinct_targets, distinct_rules, duration_minutes,
        'open' AS status
    FROM incidents
    WHERE rn_in_group = 1
) AS source
ON target.incident_id = source.incident_id
WHEN MATCHED THEN UPDATE SET
    target.last_detected_at  = source.last_detected_at,
    target.alert_count       = source.alert_count,
    target.distinct_targets  = source.distinct_targets,
    target.distinct_rules    = source.distinct_rules,
    target.duration_minutes  = source.duration_minutes,
    target.max_severity_score= source.max_severity_score,
    target.correlated_at     = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    incident_id, source_ip, primary_target, primary_rule_id,
    first_detected_at, last_detected_at, max_severity, max_severity_score,
    alert_count, distinct_targets, distinct_rules, duration_minutes,
    status, correlated_at
) VALUES (
    source.incident_id, source.source_ip, source.primary_target,
    source.primary_rule_id, source.first_detected_at, source.last_detected_at,
    source.max_severity, source.max_severity_score, source.alert_count,
    source.distinct_targets, source.distinct_rules, source.duration_minutes,
    source.status, CURRENT_TIMESTAMP
);

-- ===================== enrich_threat_intel =====================
-- Join incidents with threat_intel + MITRE techniques.
-- Compute combined_risk_score = (max_severity_score * 10) + threat_score.
-- Assign status: escalated / investigating / triaging / monitoring / informational.

MERGE INTO cyber.silver.threat_enriched AS target
USING (
    SELECT
        ic.incident_id,
        ic.source_ip,
        ic.primary_target,
        ic.primary_rule_id,
        ic.first_detected_at,
        ic.max_severity,
        ic.max_severity_score,
        ic.alert_count,
        COALESCE(ti.threat_score, 0) AS threat_score,
        ti.threat_category,
        COALESCE(ti.is_known_bad, false) AS is_known_bad,
        mt.tactic AS mitre_tactic,
        mt.technique_id AS mitre_technique,
        mt.technique_name,
        (ic.max_severity_score * 10) + COALESCE(ti.threat_score, 0) AS combined_risk_score
    FROM cyber.silver.incidents_correlated ic
    LEFT JOIN cyber.bronze.raw_threat_intel ti ON ic.source_ip = ti.ip_address
    LEFT JOIN (
        -- Map rule_id patterns to MITRE techniques
        SELECT 'R-SQL-INJ' AS rule_pattern, technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1190'
        UNION ALL SELECT 'R-BRUTE-SSH', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1110.001'
        UNION ALL SELECT 'R-POWERSHELL', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1059.001'
        UNION ALL SELECT 'R-REV-SHELL', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1059.004'
        UNION ALL SELECT 'R-MALWARE', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1204.002'
        UNION ALL SELECT 'R-PERSIST', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1547.001'
        UNION ALL SELECT 'R-PRIV-ESC', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1068'
        UNION ALL SELECT 'R-CRED-DUMP', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1003.001'
        UNION ALL SELECT 'R-LAT-MOV', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1021.002'
        UNION ALL SELECT 'R-PORTSCAN', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1046'
        UNION ALL SELECT 'R-DNS-TUN', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1048.001'
        UNION ALL SELECT 'R-EXFIL', technique_id, technique_name, tactic FROM cyber.bronze.raw_mitre_techniques WHERE technique_id = 'T1041'
    ) mt ON ic.primary_rule_id = mt.rule_pattern
) AS source
ON target.incident_id = source.incident_id
WHEN MATCHED THEN UPDATE SET
    target.threat_score        = source.threat_score,
    target.threat_category     = source.threat_category,
    target.is_known_bad        = source.is_known_bad,
    target.mitre_tactic        = source.mitre_tactic,
    target.mitre_technique     = source.mitre_technique,
    target.combined_risk_score = source.combined_risk_score,
    target.enriched_at         = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    incident_id, source_ip, primary_target, primary_rule_id,
    first_detected_at, max_severity, max_severity_score, alert_count,
    threat_score, threat_category, is_known_bad, mitre_tactic,
    mitre_technique, technique_name, combined_risk_score, enriched_at
) VALUES (
    source.incident_id, source.source_ip, source.primary_target,
    source.primary_rule_id, source.first_detected_at, source.max_severity,
    source.max_severity_score, source.alert_count, source.threat_score,
    source.threat_category, source.is_known_bad, source.mitre_tactic,
    source.mitre_technique, source.technique_name, source.combined_risk_score,
    CURRENT_TIMESTAMP
);

-- Update incident status based on enrichment
MERGE INTO cyber.silver.incidents_correlated AS target
USING (
    SELECT
        incident_id,
        CASE
            WHEN max_severity_score >= 9 AND is_known_bad = true THEN 'escalated'
            WHEN max_severity_score >= 9 THEN 'investigating'
            WHEN max_severity_score >= 7 THEN 'triaging'
            WHEN max_severity_score >= 4 THEN 'monitoring'
            ELSE 'informational'
        END AS new_status
    FROM cyber.silver.threat_enriched
) AS source
ON target.incident_id = source.incident_id
WHEN MATCHED THEN UPDATE SET
    target.status = source.new_status,
    target.correlated_at = CURRENT_TIMESTAMP;

-- ===================== build dimensions (parallel) =====================

MERGE INTO cyber.gold.dim_source_ip AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ip_address) AS source_ip_key,
        ip_address, subnet, geo_country, geo_city,
        threat_score, threat_category, is_known_bad
    FROM cyber.bronze.raw_threat_intel
) AS source
ON target.ip_address = source.ip_address
WHEN MATCHED THEN UPDATE SET
    target.threat_score    = source.threat_score,
    target.threat_category = source.threat_category,
    target.is_known_bad    = source.is_known_bad,
    target.loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    source_ip_key, ip_address, subnet, geo_country, geo_city,
    threat_score, threat_category, is_known_bad, loaded_at
) VALUES (
    source.source_ip_key, source.ip_address, source.subnet,
    source.geo_country, source.geo_city, source.threat_score,
    source.threat_category, source.is_known_bad, CURRENT_TIMESTAMP
);

MERGE INTO cyber.gold.dim_target AS target
USING (
    WITH targets AS (
        SELECT DISTINCT target_host AS hostname FROM cyber.silver.alerts_deduped
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY hostname) AS target_key,
        hostname,
        CASE
            WHEN hostname LIKE '%prod%' THEN 'production'
            WHEN hostname LIKE '%dev%' THEN 'development'
            WHEN hostname IN ('vpn-gw-01', 'jump-01') THEN 'dmz'
            ELSE 'unknown'
        END AS environment,
        CASE
            WHEN hostname LIKE 'db-%' THEN 'critical'
            WHEN hostname LIKE 'web-prod%' OR hostname LIKE 'api-prod%' THEN 'critical'
            WHEN hostname LIKE 'mail-%' OR hostname LIKE 'vpn-%' THEN 'high'
            WHEN hostname = 'jump-01' THEN 'high'
            ELSE 'medium'
        END AS criticality
    FROM targets
) AS source
ON target.hostname = source.hostname
WHEN MATCHED THEN UPDATE SET
    target.environment = source.environment,
    target.criticality = source.criticality,
    target.loaded_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    target_key, hostname, environment, criticality, loaded_at
) VALUES (
    source.target_key, source.hostname, source.environment,
    source.criticality, CURRENT_TIMESTAMP
);

MERGE INTO cyber.gold.dim_rule AS target
USING (
    WITH rules AS (
        SELECT DISTINCT rule_id FROM cyber.silver.alerts_deduped
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY rule_id) AS rule_key,
        rule_id,
        CASE rule_id
            WHEN 'R-SQL-INJ'   THEN 'SQL Injection Detection'
            WHEN 'R-BRUTE-SSH' THEN 'Brute Force SSH/VPN'
            WHEN 'R-POWERSHELL' THEN 'Suspicious PowerShell'
            WHEN 'R-REV-SHELL' THEN 'Reverse Shell Detection'
            WHEN 'R-MALWARE'   THEN 'Malware Signature Match'
            WHEN 'R-PERSIST'   THEN 'Persistence Mechanism'
            WHEN 'R-PRIV-ESC'  THEN 'Privilege Escalation'
            WHEN 'R-CRED-DUMP' THEN 'Credential Dumping'
            WHEN 'R-LAT-MOV'   THEN 'Lateral Movement SMB'
            WHEN 'R-PORTSCAN'  THEN 'Port Scan Detection'
            WHEN 'R-DNS-TUN'   THEN 'DNS Tunneling Exfil'
            WHEN 'R-EXFIL'     THEN 'Data Exfiltration'
            ELSE rule_id
        END AS rule_name,
        CASE rule_id
            WHEN 'R-SQL-INJ'   THEN 'web-attack'
            WHEN 'R-BRUTE-SSH' THEN 'authentication'
            WHEN 'R-POWERSHELL' THEN 'execution'
            WHEN 'R-REV-SHELL' THEN 'command-control'
            WHEN 'R-MALWARE'   THEN 'malware'
            WHEN 'R-PERSIST'   THEN 'persistence'
            WHEN 'R-PRIV-ESC'  THEN 'privilege-escalation'
            WHEN 'R-CRED-DUMP' THEN 'credential-access'
            WHEN 'R-LAT-MOV'   THEN 'lateral-movement'
            WHEN 'R-PORTSCAN'  THEN 'reconnaissance'
            WHEN 'R-DNS-TUN'   THEN 'exfiltration'
            WHEN 'R-EXFIL'     THEN 'exfiltration'
            ELSE 'other'
        END AS category
    FROM rules
) AS source
ON target.rule_id = source.rule_id
WHEN MATCHED THEN UPDATE SET
    target.rule_name = source.rule_name,
    target.category  = source.category,
    target.loaded_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    rule_key, rule_id, rule_name, category, loaded_at
) VALUES (
    source.rule_key, source.rule_id, source.rule_name,
    source.category, CURRENT_TIMESTAMP
);

MERGE INTO cyber.gold.dim_mitre AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY technique_id) AS mitre_key,
        technique_id, technique_name, tactic, tactic_id, severity_weight
    FROM cyber.bronze.raw_mitre_techniques
) AS source
ON target.technique_id = source.technique_id
WHEN MATCHED THEN UPDATE SET
    target.technique_name = source.technique_name,
    target.tactic         = source.tactic,
    target.severity_weight= source.severity_weight,
    target.loaded_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    mitre_key, technique_id, technique_name, tactic, tactic_id,
    severity_weight, loaded_at
) VALUES (
    source.mitre_key, source.technique_id, source.technique_name,
    source.tactic, source.tactic_id, source.severity_weight, CURRENT_TIMESTAMP
);

-- ===================== build_fact_incidents =====================

MERGE INTO cyber.gold.fact_incidents AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY te.first_detected_at, te.incident_id) AS incident_key,
        dip.source_ip_key,
        dt.target_key,
        dr.rule_key,
        dm.mitre_key,
        te.first_detected_at AS detected_at,
        te.max_severity AS severity,
        te.max_severity_score AS severity_score,
        ic.alert_count,
        ic.distinct_targets,
        ic.distinct_rules,
        ic.duration_minutes,
        te.threat_score,
        te.combined_risk_score,
        te.mitre_tactic,
        ic.status
    FROM cyber.silver.threat_enriched te
    JOIN cyber.silver.incidents_correlated ic ON te.incident_id = ic.incident_id
    JOIN cyber.gold.dim_source_ip dip ON te.source_ip = dip.ip_address
    JOIN cyber.gold.dim_target dt ON te.primary_target = dt.hostname
    JOIN cyber.gold.dim_rule dr ON te.primary_rule_id = dr.rule_id
    LEFT JOIN cyber.gold.dim_mitre dm ON te.mitre_technique = dm.technique_id
) AS source
ON target.source_ip_key = source.source_ip_key
   AND target.target_key = source.target_key
   AND target.rule_key = source.rule_key
   AND target.detected_at = source.detected_at
WHEN MATCHED THEN UPDATE SET
    target.severity            = source.severity,
    target.severity_score      = source.severity_score,
    target.alert_count         = source.alert_count,
    target.distinct_targets    = source.distinct_targets,
    target.distinct_rules      = source.distinct_rules,
    target.duration_minutes    = source.duration_minutes,
    target.threat_score        = source.threat_score,
    target.combined_risk_score = source.combined_risk_score,
    target.mitre_tactic        = source.mitre_tactic,
    target.status              = source.status,
    target.loaded_at           = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    incident_key, source_ip_key, target_key, rule_key, mitre_key,
    detected_at, severity, severity_score, alert_count, distinct_targets,
    distinct_rules, duration_minutes, threat_score, combined_risk_score,
    mitre_tactic, status, loaded_at
) VALUES (
    source.incident_key, source.source_ip_key, source.target_key,
    source.rule_key, source.mitre_key, source.detected_at, source.severity,
    source.severity_score, source.alert_count, source.distinct_targets,
    source.distinct_rules, source.duration_minutes, source.threat_score,
    source.combined_risk_score, source.mitre_tactic, source.status,
    CURRENT_TIMESTAMP
);

-- ===================== kpi_threat_dashboard =====================

MERGE INTO cyber.gold.kpi_threat_dashboard AS target
USING (
    WITH hourly AS (
        SELECT
            DATE_TRUNC('hour', fi.detected_at) AS hour_bucket,
            SUM(fi.alert_count) AS total_alerts,
            COUNT(DISTINCT fi.source_ip_key) AS unique_sources,
            COUNT(DISTINCT fi.target_key) AS unique_targets,
            SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
            SUM(CASE WHEN fi.severity = 'high' THEN 1 ELSE 0 END) AS high_count,
            SUM(CASE WHEN fi.severity = 'medium' THEN 1 ELSE 0 END) AS medium_count,
            SUM(CASE WHEN fi.severity = 'low' THEN 1 ELSE 0 END) AS low_count,
            CAST(AVG(fi.severity_score) AS DECIMAL(5,1)) AS avg_severity_score
        FROM cyber.gold.fact_incidents fi
        GROUP BY DATE_TRUNC('hour', fi.detected_at)
    ),
    with_top AS (
        SELECT
            h.*,
            (SELECT fi2.mitre_tactic
             FROM cyber.gold.fact_incidents fi2
             WHERE DATE_TRUNC('hour', fi2.detected_at) = h.hour_bucket
               AND fi2.mitre_tactic IS NOT NULL
             GROUP BY fi2.mitre_tactic
             ORDER BY COUNT(*) DESC LIMIT 1) AS top_mitre_tactic,
            (SELECT dip.ip_address
             FROM cyber.gold.fact_incidents fi3
             JOIN cyber.gold.dim_source_ip dip ON fi3.source_ip_key = dip.source_ip_key
             WHERE DATE_TRUNC('hour', fi3.detected_at) = h.hour_bucket
             GROUP BY dip.ip_address
             ORDER BY COUNT(*) DESC LIMIT 1) AS top_source_ip
        FROM hourly h
    )
    SELECT * FROM with_top
) AS source
ON target.hour_bucket = source.hour_bucket
WHEN MATCHED THEN UPDATE SET
    target.total_alerts      = source.total_alerts,
    target.unique_sources    = source.unique_sources,
    target.unique_targets    = source.unique_targets,
    target.critical_count    = source.critical_count,
    target.high_count        = source.high_count,
    target.medium_count      = source.medium_count,
    target.low_count         = source.low_count,
    target.avg_severity_score= source.avg_severity_score,
    target.top_mitre_tactic  = source.top_mitre_tactic,
    target.top_source_ip     = source.top_source_ip,
    target.loaded_at         = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    hour_bucket, total_alerts, unique_sources, unique_targets,
    critical_count, high_count, medium_count, low_count,
    avg_severity_score, top_mitre_tactic, top_source_ip, loaded_at
) VALUES (
    source.hour_bucket, source.total_alerts, source.unique_sources,
    source.unique_targets, source.critical_count, source.high_count,
    source.medium_count, source.low_count, source.avg_severity_score,
    source.top_mitre_tactic, source.top_source_ip, CURRENT_TIMESTAMP
);

-- ===================== kpi_response_metrics =====================
-- MTTD/MTTR equivalent: duration_minutes per severity level.

MERGE INTO cyber.gold.kpi_response_metrics AS target
USING (
    SELECT
        fi.severity,
        DATE_FORMAT(fi.detected_at, 'yyyy-MM-dd') AS period,
        COUNT(*) AS incident_count,
        CAST(AVG(fi.alert_count) AS DECIMAL(5,1)) AS avg_alert_count,
        CAST(AVG(fi.duration_minutes) AS DECIMAL(8,1)) AS avg_duration_min,
        MAX(fi.duration_minutes) AS max_duration_min,
        SUM(CASE WHEN fi.status = 'escalated' THEN 1 ELSE 0 END) AS escalated_count,
        CAST(
            SUM(CASE WHEN fi.status = 'escalated' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        AS DECIMAL(5,2)) AS escalated_pct,
        CAST(AVG(fi.threat_score) AS DECIMAL(5,1)) AS avg_threat_score
    FROM cyber.gold.fact_incidents fi
    GROUP BY fi.severity, DATE_FORMAT(fi.detected_at, 'yyyy-MM-dd')
) AS source
ON target.severity = source.severity AND target.period = source.period
WHEN MATCHED THEN UPDATE SET
    target.incident_count  = source.incident_count,
    target.avg_alert_count = source.avg_alert_count,
    target.avg_duration_min= source.avg_duration_min,
    target.max_duration_min= source.max_duration_min,
    target.escalated_count = source.escalated_count,
    target.escalated_pct   = source.escalated_pct,
    target.avg_threat_score= source.avg_threat_score,
    target.loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    severity, period, incident_count, avg_alert_count, avg_duration_min,
    max_duration_min, escalated_count, escalated_pct, avg_threat_score, loaded_at
) VALUES (
    source.severity, source.period, source.incident_count,
    source.avg_alert_count, source.avg_duration_min, source.max_duration_min,
    source.escalated_count, source.escalated_pct, source.avg_threat_score,
    CURRENT_TIMESTAMP
);

-- ===================== bloom_and_optimize =====================
-- Bloom filter on source_ip for fast IP lookups. OPTIMIZE compaction.

CREATE BLOOMFILTER INDEX ON cyber.silver.alerts_deduped FOR COLUMNS (source_ip);
CREATE BLOOMFILTER INDEX ON cyber.gold.fact_incidents FOR COLUMNS (source_ip_key);
OPTIMIZE cyber.silver.alerts_deduped;
OPTIMIZE cyber.silver.incidents_correlated;
OPTIMIZE cyber.silver.threat_enriched;
OPTIMIZE cyber.gold.fact_incidents;
OPTIMIZE cyber.gold.kpi_threat_dashboard;
