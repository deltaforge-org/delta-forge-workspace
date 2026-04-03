-- =============================================================================
-- Cybersecurity Incidents Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates:
--   1. INCREMENTAL_FILTER macro for watermark-based filtering
--   2. New alerts from all 3 sources arriving in next batch
--   3. Incremental dedup, correlation, enrichment, and gold refresh
--   4. Multi-alert incident escalation (5+ alerts from same source)
--   5. Exfiltration chain detection
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: "alert_id > 'FW-030' AND deduped_at > '2024-01-16'"
--         or "1=1" if the target is empty (first run = full load)
-- ============================================================================

PRINT {{INCREMENTAL_FILTER(cyber.silver.alerts_deduped, alert_id, deduped_at, 0)}};

-- ===================== Capture Current State =====================

SELECT MAX(deduped_at) AS current_dedup_watermark FROM cyber.silver.alerts_deduped;
SELECT MAX(correlated_at) AS current_correlation_watermark FROM cyber.silver.incidents_correlated;

SELECT 'alerts_deduped' AS table_name, COUNT(*) AS row_count FROM cyber.silver.alerts_deduped
UNION ALL SELECT 'incidents_correlated', COUNT(*) FROM cyber.silver.incidents_correlated
UNION ALL SELECT 'threat_enriched', COUNT(*) FROM cyber.silver.threat_enriched
UNION ALL SELECT 'fact_incidents', COUNT(*) FROM cyber.gold.fact_incidents;

-- ===================== Insert New Bronze Alerts (10 across 3 sources) =====================

-- Firewall: 3 new alerts (1 duplicate within 5-min window)
INSERT INTO cyber.bronze.raw_firewall_alerts VALUES
('FW-031', '185.220.101.34', 'web-prod-01', 'R-SQL-INJ',   '2024-01-16T06:00:00', 8, 'high',     51000,  'TCP', 'Morning SQL injection probe from Russia',               '2024-01-16T06:00:30'),
('FW-032', '185.220.101.34', 'web-prod-01', 'R-SQL-INJ',   '2024-01-16T06:02:00', 8, 'high',     48000,  'TCP', 'SQL injection DUPLICATE',                               '2024-01-16T06:02:30'),
('FW-033', '5.188.86.22',    'db-prod-01',  'R-EXFIL',     '2024-01-16T06:30:00', 9, 'critical', 310000, 'TCP', 'Massive exfiltration from APT source',                   '2024-01-16T06:30:30');

-- IDS: 4 new alerts (1 duplicate)
INSERT INTO cyber.bronze.raw_ids_alerts VALUES
('IDS-026', '112.85.42.187', 'db-prod-01',  'R-CRED-DUMP', '2024-01-16T07:00:00', 9, 'critical', 'SIG-7001', 'TCP', 'New credential dump from Shanghai',                '2024-01-16T07:00:30'),
('IDS-027', '112.85.42.187', 'db-prod-01',  'R-CRED-DUMP', '2024-01-16T07:03:00', 9, 'critical', 'SIG-7001', 'TCP', 'Credential dump DUPLICATE',                        '2024-01-16T07:03:30'),
('IDS-028', '91.215.85.102', 'jump-01',     'R-BRUTE-SSH', '2024-01-16T08:00:00', 8, 'high',     'SIG-3006', 'TCP', 'SSH brute force new day from Ukraine',             '2024-01-16T08:00:30'),
('IDS-029', '23.129.64.201', 'api-prod-01', 'R-PORTSCAN',  '2024-01-16T08:30:00', 4, 'medium',   'SIG-1009', 'TCP', 'API server port scan from Tor',                    '2024-01-16T08:30:30');

-- Endpoint: 3 new alerts
INSERT INTO cyber.bronze.raw_endpoint_alerts VALUES
('EP-021', '46.166.139.111', 'web-prod-02', 'R-MALWARE',   '2024-01-16T07:30:00', 9, 'critical',  'dropper.exe',  's1t2u3v4w5', 'New malware dropper from Romania',           '2024-01-16T07:30:30'),
('EP-022', '10.0.1.50',      'api-prod-01', 'R-DNS-TUN',   '2024-01-16T09:00:00', 9, 'critical',  'dns.exe',      'x6y7z8a9b0', 'DNS tunneling exfiltration continued',       '2024-01-16T09:00:30'),
('EP-023', '94.102.49.193',  'web-prod-01', 'R-REV-SHELL', '2024-01-16T09:30:00', 9, 'critical',  'bash',         'c1d2e3f4g5', 'Reverse shell from botnet C2',               '2024-01-16T09:30:30');

-- ===================== Incremental MERGE: 3 sources -> alerts_deduped =====================

MERGE INTO cyber.silver.alerts_deduped AS target
USING (
    SELECT alert_id, 'firewall' AS source, source_ip, target_host, rule_id,
           detected_at, severity_score, severity, protocol, raw_log,
           CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
                  DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
                  CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_firewall_alerts
    WHERE {{INCREMENTAL_FILTER(cyber.silver.alerts_deduped, alert_id, deduped_at, 0)}}
    UNION ALL
    SELECT alert_id, 'ids' AS source, source_ip, target_host, rule_id,
           detected_at, severity_score, severity, protocol, raw_log,
           CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
                  DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
                  CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_ids_alerts
    WHERE {{INCREMENTAL_FILTER(cyber.silver.alerts_deduped, alert_id, deduped_at, 0)}}
    UNION ALL
    SELECT alert_id, 'endpoint' AS source, source_ip, target_host, rule_id,
           detected_at, severity_score, severity, NULL AS protocol, raw_log,
           CONCAT(source_ip, '|', target_host, '|', rule_id, '|',
                  DATE_FORMAT(DATE_TRUNC('hour', detected_at), 'yyyyMMddHH'), '|',
                  CAST(FLOOR(EXTRACT(MINUTE FROM detected_at) / 5) AS STRING)) AS dedup_bucket
    FROM cyber.bronze.raw_endpoint_alerts
    WHERE {{INCREMENTAL_FILTER(cyber.silver.alerts_deduped, alert_id, deduped_at, 0)}}
) AS source
ON target.alert_id = source.alert_id AND target.source = source.source
WHEN NOT MATCHED THEN INSERT (
    alert_id, source, source_ip, target_host, rule_id, detected_at,
    severity_score, severity, protocol, raw_log, dedup_bucket, deduped_at
) VALUES (
    source.alert_id, source.source, source.source_ip, source.target_host,
    source.rule_id, source.detected_at, source.severity_score, source.severity,
    source.protocol, source.raw_log, source.dedup_bucket, CURRENT_TIMESTAMP
);

-- ===================== Incremental Correlation =====================

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
    clean_alerts AS (SELECT * FROM deduped WHERE rn = 1),
    gap_detect AS (
        SELECT source_ip, target_host, rule_id, detected_at, severity, severity_score,
            CASE
                WHEN DATEDIFF(detected_at,
                    LAG(detected_at) OVER (PARTITION BY source_ip ORDER BY detected_at)
                ) * 24 * 60 > 15
                OR LAG(detected_at) OVER (PARTITION BY source_ip ORDER BY detected_at) IS NULL
                THEN 1 ELSE 0
            END AS new_incident_flag
        FROM clean_alerts
    ),
    incident_groups AS (
        SELECT *, SUM(new_incident_flag) OVER (PARTITION BY source_ip ORDER BY detected_at) AS incident_group_id
        FROM gap_detect
    ),
    incidents AS (
        SELECT
            CONCAT(source_ip, '-INC-', CAST(incident_group_id AS STRING)) AS incident_id,
            source_ip,
            FIRST_VALUE(target_host) OVER (PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC) AS primary_target,
            FIRST_VALUE(rule_id) OVER (PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC) AS primary_rule_id,
            MIN(detected_at) OVER (PARTITION BY source_ip, incident_group_id) AS first_detected_at,
            MAX(detected_at) OVER (PARTITION BY source_ip, incident_group_id) AS last_detected_at,
            FIRST_VALUE(severity) OVER (PARTITION BY source_ip, incident_group_id ORDER BY severity_score DESC) AS max_severity,
            MAX(severity_score) OVER (PARTITION BY source_ip, incident_group_id) AS max_severity_score,
            COUNT(*) OVER (PARTITION BY source_ip, incident_group_id) AS alert_count,
            COUNT(DISTINCT target_host) OVER (PARTITION BY source_ip, incident_group_id) AS distinct_targets,
            COUNT(DISTINCT rule_id) OVER (PARTITION BY source_ip, incident_group_id) AS distinct_rules,
            CAST(DATEDIFF(MAX(detected_at) OVER (PARTITION BY source_ip, incident_group_id),
                          MIN(detected_at) OVER (PARTITION BY source_ip, incident_group_id)) * 24 * 60 AS INT) AS duration_minutes,
            ROW_NUMBER() OVER (PARTITION BY source_ip, incident_group_id ORDER BY detected_at ASC) AS rn_in_group
        FROM incident_groups
    )
    SELECT incident_id, source_ip, primary_target, primary_rule_id,
           first_detected_at, last_detected_at, max_severity, max_severity_score,
           alert_count, distinct_targets, distinct_rules, duration_minutes, 'open' AS status
    FROM incidents WHERE rn_in_group = 1
) AS source
ON target.incident_id = source.incident_id
WHEN MATCHED THEN UPDATE SET
    target.last_detected_at   = source.last_detected_at,
    target.alert_count        = source.alert_count,
    target.distinct_targets   = source.distinct_targets,
    target.distinct_rules     = source.distinct_rules,
    target.duration_minutes   = source.duration_minutes,
    target.max_severity_score = source.max_severity_score,
    target.correlated_at      = CURRENT_TIMESTAMP
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

-- ===================== Verify Incremental Results =====================

SELECT COUNT(*) AS post_alert_count FROM cyber.silver.alerts_deduped;
SELECT COUNT(*) AS post_incident_count FROM cyber.silver.incidents_correlated;

-- No duplicate incident_ids
SELECT COUNT(*) AS dup_check
FROM (
    SELECT incident_id, COUNT(*) AS cnt
    FROM cyber.silver.incidents_correlated
    GROUP BY incident_id
    HAVING COUNT(*) > 1
);

ASSERT VALUE dup_check = 0
SELECT 'No duplicate incidents detected' AS status;

-- Multi-alert incidents (5+ alerts from same source)
SELECT incident_id, source_ip, alert_count, distinct_targets, distinct_rules, duration_minutes
FROM cyber.silver.incidents_correlated
WHERE alert_count >= 5
ORDER BY alert_count DESC;

-- ===================== Refresh Gold Layer =====================

-- Re-run enrichment for new/updated incidents
MERGE INTO cyber.silver.threat_enriched AS target
USING (
    SELECT
        ic.incident_id, ic.source_ip, ic.primary_target, ic.primary_rule_id,
        ic.first_detected_at, ic.max_severity, ic.max_severity_score, ic.alert_count,
        COALESCE(ti.threat_score, 0) AS threat_score,
        ti.threat_category,
        COALESCE(ti.is_known_bad, false) AS is_known_bad,
        mt.tactic AS mitre_tactic, mt.technique_id AS mitre_technique, mt.technique_name,
        (ic.max_severity_score * 10) + COALESCE(ti.threat_score, 0) AS combined_risk_score
    FROM cyber.silver.incidents_correlated ic
    LEFT JOIN cyber.bronze.raw_threat_intel ti ON ic.source_ip = ti.ip_address
    LEFT JOIN (
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
    WHERE ic.correlated_at > (SELECT COALESCE(MAX(enriched_at), '1970-01-01T00:00:00') FROM cyber.silver.threat_enriched)
       OR ic.incident_id IN (SELECT incident_id FROM cyber.silver.threat_enriched)
) AS source
ON target.incident_id = source.incident_id
WHEN MATCHED THEN UPDATE SET
    target.threat_score        = source.threat_score,
    target.combined_risk_score = source.combined_risk_score,
    target.alert_count         = source.alert_count,
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

-- Refresh fact_incidents
MERGE INTO cyber.gold.fact_incidents AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY te.first_detected_at, te.incident_id) AS incident_key,
        dip.source_ip_key, dt.target_key, dr.rule_key, dm.mitre_key,
        te.first_detected_at AS detected_at, te.max_severity AS severity,
        te.max_severity_score AS severity_score, ic.alert_count,
        ic.distinct_targets, ic.distinct_rules, ic.duration_minutes,
        te.threat_score, te.combined_risk_score, te.mitre_tactic, ic.status
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
    target.alert_count = source.alert_count,
    target.status      = source.status,
    target.loaded_at   = CURRENT_TIMESTAMP
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

SELECT COUNT(*) AS post_fact_count FROM cyber.gold.fact_incidents;

-- Verify watermark advanced
SELECT MAX(deduped_at) AS new_dedup_watermark FROM cyber.silver.alerts_deduped;
