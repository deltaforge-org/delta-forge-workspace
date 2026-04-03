-- =============================================================================
-- Cybersecurity Incidents Pipeline - Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "alert_id > 'ALT00075' AND detected_at > '2024-01-15T23:00:00'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.incidents_correlated, incident_id, first_detected_at, 0)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.incidents_correlated
-- SELECT * FROM {{zone_prefix}}.bronze.raw_alerts
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.incidents_correlated, incident_id, first_detected_at, 0)}};

-- ===================== STEP 1: Capture Current State =====================

SELECT MAX(processed_at) AS current_watermark FROM {{zone_prefix}}.silver.incidents_correlated;
SELECT COUNT(*) AS pre_incident_count FROM {{zone_prefix}}.silver.incidents_correlated;
SELECT COUNT(*) AS pre_fact_count FROM {{zone_prefix}}.gold.fact_incidents;

-- ===================== STEP 2: Insert New Bronze Alerts =====================

INSERT INTO {{zone_prefix}}.bronze.raw_alerts VALUES
    ('ALT00076', '185.220.101.34', 'web-prod-01', 'R002', '2024-01-16T06:00:00', 'high', 'Morning SQL injection probe', '2024-01-16T06:00:30'),
    ('ALT00077', '185.220.101.34', 'web-prod-01', 'R002', '2024-01-16T06:02:00', 'high', 'SQL injection DUPLICATE', '2024-01-16T06:02:30'),
    ('ALT00078', '112.85.42.187', 'db-prod-01', 'R008', '2024-01-16T06:30:00', 'critical', 'New credential dump attempt', '2024-01-16T06:30:30'),
    ('ALT00079', '112.85.42.187', 'db-prod-01', 'R008', '2024-01-16T06:33:00', 'critical', 'Credential dump DUPLICATE', '2024-01-16T06:33:30'),
    ('ALT00080', '10.0.1.50', 'api-prod-01', 'R006', '2024-01-16T07:00:00', 'critical', 'DNS tunneling exfiltration', '2024-01-16T07:00:30'),
    ('ALT00081', '46.166.139.111', 'web-prod-02', 'R004', '2024-01-16T07:30:00', 'critical', 'New reverse shell from Romania', '2024-01-16T07:30:30'),
    ('ALT00082', '91.215.85.102', 'jump-01', 'R001', '2024-01-16T08:00:00', 'high', 'SSH brute force new day', '2024-01-16T08:00:30'),
    ('ALT00083', '91.215.85.102', 'jump-01', 'R001', '2024-01-16T08:04:00', 'high', 'SSH brute force DUPLICATE', '2024-01-16T08:04:30'),
    ('ALT00084', '23.129.64.201', 'api-prod-01', 'R010', '2024-01-16T08:30:00', 'medium', 'API server port scan from Tor', '2024-01-16T08:30:30'),
    ('ALT00085', '103.235.46.78', 'web-prod-02', 'R011', '2024-01-16T09:00:00', 'critical', 'Malware dropper via web upload', '2024-01-16T09:00:30');

-- ===================== STEP 3: Incremental MERGE to Silver =====================

MERGE INTO {{zone_prefix}}.silver.incidents_correlated AS target
USING (
    WITH new_alerts AS (
        SELECT *
        FROM {{zone_prefix}}.bronze.raw_alerts
        WHERE ingested_at > (
            SELECT COALESCE(MAX(processed_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.incidents_correlated
        )
    ),
    windowed AS (
        SELECT
            a.alert_id, a.source_ip, a.target_hostname, a.rule_id,
            a.detected_at, a.severity, a.ingested_at,
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
        FROM new_alerts a
    ),
    incidents AS (
        SELECT
            CONCAT(w.source_ip, '-', w.target_hostname, '-', w.rule_id, '-',
                   DATE_FORMAT(w.first_detected, 'yyyyMMddHHmm')) AS incident_id,
            w.source_ip, w.target_hostname, w.rule_id,
            w.first_detected AS first_detected_at,
            w.last_detected AS last_detected_at,
            w.severity, w.window_alert_count AS alert_count,
            30 AS mean_time_to_detect_sec,
            ip.threat_intel_score, ip.is_known_bad,
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
    last_detected_at   = source.last_detected_at,
    alert_count        = source.alert_count,
    status             = source.status,
    processed_at       = CURRENT_TIMESTAMP
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

-- ===================== STEP 4: Verify Incremental =====================

SELECT COUNT(*) AS post_incident_count FROM {{zone_prefix}}.silver.incidents_correlated;

-- 10 new alerts should produce ~7 unique incidents (3 are dupes)
SELECT post.cnt - pre.cnt AS new_incidents
FROM (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.incidents_correlated) post,
     (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.incidents_correlated WHERE processed_at < CURRENT_TIMESTAMP) pre;

-- No duplicate incident_ids
SELECT COUNT(*) AS dup_check
FROM (
    SELECT incident_id, COUNT(*) AS cnt
    FROM {{zone_prefix}}.silver.incidents_correlated
    GROUP BY incident_id
    HAVING COUNT(*) > 1
);
-- ===================== STEP 5: Refresh Gold Fact =====================

MERGE INTO {{zone_prefix}}.gold.fact_incidents AS target
USING (
ASSERT VALUE dup_check = 0
    SELECT
        ROW_NUMBER() OVER (ORDER BY ic.first_detected_at, ic.incident_id) AS incident_key,
        dip.source_ip_key, dt.target_key, dr.rule_key,
        ic.first_detected_at AS detected_at, ic.severity, ic.alert_count,
        ic.mean_time_to_detect_sec, ic.status
    FROM {{zone_prefix}}.silver.incidents_correlated ic
    JOIN {{zone_prefix}}.gold.dim_source_ip dip ON ic.source_ip = dip.ip_address
    JOIN {{zone_prefix}}.gold.dim_target dt ON ic.target_hostname = dt.hostname
    JOIN {{zone_prefix}}.gold.dim_rule dr ON ic.rule_id = dr.rule_id
) AS source
ON target.source_ip_key = source.source_ip_key AND target.target_key = source.target_key AND target.rule_key = source.rule_key AND target.detected_at = source.detected_at
WHEN MATCHED THEN UPDATE SET
    alert_count = source.alert_count,
    status      = source.status,
    loaded_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    incident_key, source_ip_key, target_key, rule_key, detected_at,
    severity, alert_count, mean_time_to_detect_sec, status, loaded_at
) VALUES (
    source.incident_key, source.source_ip_key, source.target_key, source.rule_key,
    source.detected_at, source.severity, source.alert_count,
    source.mean_time_to_detect_sec, source.status, CURRENT_TIMESTAMP
);

SELECT COUNT(*) AS post_fact_count FROM {{zone_prefix}}.gold.fact_incidents;

-- Refresh KPI dashboard
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
        SELECT h.*,
            (SELECT dr.mitre_tactic
             FROM {{zone_prefix}}.gold.fact_incidents fi2
             JOIN {{zone_prefix}}.gold.dim_rule dr ON fi2.rule_key = dr.rule_key
             WHERE DATE_TRUNC('hour', fi2.detected_at) = h.hour_bucket
             GROUP BY dr.mitre_tactic ORDER BY COUNT(*) DESC LIMIT 1) AS top_mitre_tactic
        FROM hourly h
    )
    SELECT * FROM with_tactic
) AS source
ON target.hour_bucket = source.hour_bucket
WHEN MATCHED THEN UPDATE SET
    total_alerts = source.total_alerts, unique_sources = source.unique_sources,
    critical_count = source.critical_count, high_count = source.high_count,
    medium_count = source.medium_count, low_count = source.low_count,
    mean_time_to_detect = source.mean_time_to_detect,
    top_mitre_tactic = source.top_mitre_tactic, loaded_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    hour_bucket, total_alerts, unique_sources, critical_count, high_count,
    medium_count, low_count, mean_time_to_detect, top_mitre_tactic, loaded_at
) VALUES (
    source.hour_bucket, source.total_alerts, source.unique_sources, source.critical_count,
    source.high_count, source.medium_count, source.low_count, source.mean_time_to_detect,
    source.top_mitre_tactic, CURRENT_TIMESTAMP
);
