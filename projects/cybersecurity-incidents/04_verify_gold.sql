-- =============================================================================
-- Cybersecurity Incidents Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== TEST 1: Star Schema - Top Attacking Source IPs =====================

SELECT
    dip.ip_address,
    dip.geo_country,
    dip.geo_city,
    dip.threat_intel_score,
    dip.is_known_bad,
    COUNT(fi.incident_key) AS incident_count,
    SUM(fi.alert_count) AS total_alerts
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
GROUP BY dip.ip_address, dip.geo_country, dip.geo_city, dip.threat_intel_score, dip.is_known_bad
ORDER BY incident_count DESC;

-- Known bad IPs should dominate top attackers
SELECT COUNT(DISTINCT fi.source_ip_key) AS known_bad_sources
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.is_known_bad = true;

-- ===================== TEST 2: MITRE ATT&CK Tactic Distribution =====================

ASSERT VALUE known_bad_sources >= 5
SELECT
    dr.mitre_tactic,
    dr.mitre_technique,
    COUNT(fi.incident_key) AS incident_count,
    SUM(fi.alert_count) AS total_alerts,
    SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_rule dr ON fi.rule_key = dr.rule_key
GROUP BY dr.mitre_tactic, dr.mitre_technique
ORDER BY incident_count DESC;

-- initial-access should be most common tactic (SQL injection + brute force)
SELECT dr.mitre_tactic AS top_tactic
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_rule dr ON fi.rule_key = dr.rule_key
GROUP BY dr.mitre_tactic
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ===================== TEST 3: Target Vulnerability Heat Map =====================

ASSERT VALUE top_tactic = 'initial-access'
SELECT
    dt.hostname,
    dt.service,
    dt.environment,
    dt.criticality,
    COUNT(fi.incident_key) AS incidents_targeting,
    SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_hits,
    SUM(fi.alert_count) AS total_alerts
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_target dt ON fi.target_key = dt.target_key
GROUP BY dt.hostname, dt.service, dt.environment, dt.criticality
ORDER BY incidents_targeting DESC;

-- Production critical targets should have the most incidents
SELECT dt.environment AS most_targeted_env
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_target dt ON fi.target_key = dt.target_key
GROUP BY dt.environment
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ===================== TEST 4: Severity Distribution =====================

ASSERT VALUE most_targeted_env = 'production'
SELECT
    fi.severity,
    COUNT(*) AS incident_count,
    SUM(fi.alert_count) AS raw_alert_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_incidents), 2) AS pct_of_total
FROM {{zone_prefix}}.gold.fact_incidents fi
GROUP BY fi.severity
ORDER BY CASE fi.severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
END;

-- Should have all 4 severity levels
SELECT COUNT(DISTINCT severity) AS severity_levels FROM {{zone_prefix}}.gold.fact_incidents;
-- ===================== TEST 5: Hourly Threat Dashboard =====================

ASSERT VALUE severity_levels = 4
SELECT
    hour_bucket,
    total_alerts,
    unique_sources,
    critical_count,
    high_count,
    medium_count,
    low_count,
    mean_time_to_detect,
    top_mitre_tactic
FROM {{zone_prefix}}.gold.kpi_threat_dashboard
ORDER BY hour_bucket;

-- Should span multiple hours
SELECT COUNT(*) AS hour_buckets FROM {{zone_prefix}}.gold.kpi_threat_dashboard;
-- ===================== TEST 6: Alert Velocity (alerts per hour trend) =====================

ASSERT VALUE hour_buckets >= 10
SELECT
    hour_bucket,
    total_alerts,
    LAG(total_alerts) OVER (ORDER BY hour_bucket) AS prev_hour_alerts,
    total_alerts - COALESCE(LAG(total_alerts) OVER (ORDER BY hour_bucket), 0) AS alert_delta
FROM {{zone_prefix}}.gold.kpi_threat_dashboard
ORDER BY hour_bucket;

-- ===================== TEST 7: Incident Status Distribution =====================

SELECT
    fi.status,
    COUNT(*) AS count,
    ROUND(AVG(fi.alert_count), 1) AS avg_alerts_per_incident
FROM {{zone_prefix}}.gold.fact_incidents fi
GROUP BY fi.status
ORDER BY count DESC;

-- Escalated incidents should exist (critical + known_bad)
SELECT COUNT(*) AS escalated_count
FROM {{zone_prefix}}.gold.fact_incidents
WHERE status = 'escalated';

-- ===================== TEST 8: Dimension Completeness =====================

ASSERT VALUE escalated_count >= 5
SELECT COUNT(*) AS ip_dim_count FROM {{zone_prefix}}.gold.dim_source_ip;
ASSERT VALUE ip_dim_count = 16

SELECT COUNT(*) AS target_dim_count FROM {{zone_prefix}}.gold.dim_target;
ASSERT VALUE target_dim_count = 8

SELECT COUNT(*) AS rule_dim_count FROM {{zone_prefix}}.gold.dim_rule;
-- ===================== TEST 9: Referential Integrity =====================

ASSERT VALUE rule_dim_count = 12
SELECT COUNT(*) AS orphan_ips
FROM {{zone_prefix}}.gold.fact_incidents fi
LEFT JOIN {{zone_prefix}}.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.source_ip_key IS NULL;

ASSERT VALUE orphan_ips = 0

SELECT COUNT(*) AS orphan_targets
FROM {{zone_prefix}}.gold.fact_incidents fi
LEFT JOIN {{zone_prefix}}.gold.dim_target dt ON fi.target_key = dt.target_key
WHERE dt.target_key IS NULL;

ASSERT VALUE orphan_targets = 0

SELECT COUNT(*) AS orphan_rules
FROM {{zone_prefix}}.gold.fact_incidents fi
LEFT JOIN {{zone_prefix}}.gold.dim_rule dr ON fi.rule_key = dr.rule_key
WHERE dr.rule_key IS NULL;

-- ===================== TEST 10: Geo Distribution of Threats =====================

ASSERT VALUE orphan_rules = 0
SELECT
    dip.geo_country,
    COUNT(fi.incident_key) AS incidents,
    SUM(fi.alert_count) AS total_alerts,
    ROUND(AVG(dip.threat_intel_score), 1) AS avg_threat_score
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
GROUP BY dip.geo_country
ORDER BY incidents DESC;

-- External countries should have higher threat scores than internal
SELECT ROUND(AVG(dip.threat_intel_score), 1) AS external_avg_score
FROM {{zone_prefix}}.gold.fact_incidents fi
JOIN {{zone_prefix}}.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.geo_country != 'INTERNAL';

-- ===================== VERIFICATION SUMMARY =====================

ASSERT VALUE external_avg_score >= 50
SELECT 'Cybersecurity Incidents Gold Layer Verification PASSED' AS status;
