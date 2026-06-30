-- =============================================================================
-- Cybersecurity Incidents Pipeline: Gold Layer Verification
-- =============================================================================

PIPELINE cyber_verify_gold
  DESCRIPTION 'Gold layer verification for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'verification', 'cybersecurity-incidents'
  LIFECYCLE production
;


-- -----------------------------------------------------------------------------
-- 1. Top Attacking Source IPs (star schema query)
-- -----------------------------------------------------------------------------
SELECT
  dip.ip_address,
  dip.geo_country,
  dip.geo_city,
  dip.threat_score,
  dip.threat_category,
  dip.is_known_bad,
  COUNT(fi.incident_key) AS incident_count,
  SUM(fi.alert_count) AS total_alerts,
  CAST(AVG(fi.combined_risk_score) AS DECIMAL(5,1)) AS avg_risk_score
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
GROUP BY dip.ip_address, dip.geo_country, dip.geo_city, dip.threat_score,
       dip.threat_category, dip.is_known_bad
ORDER BY incident_count DESC;

-- Known bad IPs should dominate top attackers
ASSERT VALUE known_bad_sources >= 5
SELECT COUNT(DISTINCT fi.source_ip_key) AS known_bad_sources
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.is_known_bad = true;

SELECT 'Known bad source check passed' AS status;

-- -----------------------------------------------------------------------------
-- 2. MITRE ATT&CK Tactic Distribution
-- -----------------------------------------------------------------------------
SELECT
  dm.tactic,
  dm.technique_name,
  dm.severity_weight,
  COUNT(fi.incident_key) AS incident_count,
  SUM(fi.alert_count) AS total_alerts,
  SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_mitre dm ON fi.mitre_key = dm.mitre_key
GROUP BY dm.tactic, dm.technique_name, dm.severity_weight
ORDER BY incident_count DESC;

-- initial-access should be most common tactic (SQL injection + brute force)
ASSERT VALUE top_tactic = 'initial-access'
SELECT dm.tactic AS top_tactic
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_mitre dm ON fi.mitre_key = dm.mitre_key
WHERE dm.tactic IS NOT NULL
GROUP BY dm.tactic
ORDER BY COUNT(*) DESC
LIMIT 1;

SELECT 'MITRE tactic check passed' AS status;

-- -----------------------------------------------------------------------------
-- 3. Target Vulnerability Heat Map
-- -----------------------------------------------------------------------------
SELECT
  dt.hostname,
  dt.environment,
  dt.criticality,
  COUNT(fi.incident_key) AS incidents_targeting,
  SUM(CASE WHEN fi.severity = 'critical' THEN 1 ELSE 0 END) AS critical_hits,
  SUM(fi.alert_count) AS total_alerts,
  CAST(AVG(fi.combined_risk_score) AS DECIMAL(5,1)) AS avg_risk
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_target dt ON fi.target_key = dt.target_key
GROUP BY dt.hostname, dt.environment, dt.criticality
ORDER BY incidents_targeting DESC;

-- Production environment should be most targeted
ASSERT VALUE most_targeted_env = 'production'
SELECT dt.environment AS most_targeted_env
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_target dt ON fi.target_key = dt.target_key
GROUP BY dt.environment
ORDER BY COUNT(*) DESC
LIMIT 1;

SELECT 'Target environment check passed' AS status;

-- -----------------------------------------------------------------------------
-- 4. Severity Distribution
-- -----------------------------------------------------------------------------
SELECT
  fi.severity,
  COUNT(*) AS incident_count,
  SUM(fi.alert_count) AS raw_alert_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cyber.gold.fact_incidents), 2) AS pct_of_total,
  CAST(AVG(fi.threat_score) AS DECIMAL(5,1)) AS avg_threat_score
FROM cyber.gold.fact_incidents fi
GROUP BY fi.severity
ORDER BY CASE fi.severity
  WHEN 'critical' THEN 1
  WHEN 'high' THEN 2
  WHEN 'medium' THEN 3
  WHEN 'low' THEN 4
END;

ASSERT VALUE severity_levels = 4
SELECT COUNT(DISTINCT severity) AS severity_levels FROM cyber.gold.fact_incidents;

SELECT 'All 4 severity levels present' AS status;

-- -----------------------------------------------------------------------------
-- 5. Hourly Threat Dashboard
-- -----------------------------------------------------------------------------
SELECT
  hour_bucket,
  total_alerts,
  unique_sources,
  unique_targets,
  critical_count,
  high_count,
  medium_count,
  low_count,
  avg_severity_score,
  top_mitre_tactic,
  top_source_ip
FROM cyber.gold.kpi_threat_dashboard
ORDER BY hour_bucket;

ASSERT VALUE hour_buckets >= 10
SELECT COUNT(*) AS hour_buckets FROM cyber.gold.kpi_threat_dashboard;

SELECT 'Dashboard spans sufficient hours' AS status;

-- -----------------------------------------------------------------------------
-- 6. Alert Velocity Trend (window function over dashboard)
-- -----------------------------------------------------------------------------
SELECT
  hour_bucket,
  total_alerts,
  LAG(total_alerts) OVER (ORDER BY hour_bucket) AS prev_hour_alerts,
  total_alerts - COALESCE(LAG(total_alerts) OVER (ORDER BY hour_bucket), 0) AS alert_delta,
  SUM(total_alerts) OVER (ORDER BY hour_bucket) AS cumulative_alerts
FROM cyber.gold.kpi_threat_dashboard
ORDER BY hour_bucket;

-- -----------------------------------------------------------------------------
-- 7. Response Metrics by Severity
-- -----------------------------------------------------------------------------
SELECT
  severity,
  SUM(incident_count) AS total_incidents,
  CAST(AVG(avg_alert_count) AS DECIMAL(5,1)) AS avg_alerts_per_incident,
  CAST(AVG(avg_duration_min) AS DECIMAL(8,1)) AS avg_duration,
  MAX(max_duration_min) AS worst_duration,
  SUM(escalated_count) AS total_escalated,
  CAST(AVG(escalated_pct) AS DECIMAL(5,2)) AS avg_escalated_pct,
  CAST(AVG(avg_threat_score) AS DECIMAL(5,1)) AS avg_threat_score
FROM cyber.gold.kpi_response_metrics
GROUP BY severity
ORDER BY CASE severity
  WHEN 'critical' THEN 1 WHEN 'high' THEN 2
  WHEN 'medium' THEN 3 WHEN 'low' THEN 4
END;

-- -----------------------------------------------------------------------------
-- 8. Incident Status Distribution
-- -----------------------------------------------------------------------------
SELECT
  fi.status,
  COUNT(*) AS count,
  ROUND(AVG(fi.alert_count), 1) AS avg_alerts,
  CAST(AVG(fi.combined_risk_score) AS DECIMAL(5,1)) AS avg_risk_score
FROM cyber.gold.fact_incidents fi
GROUP BY fi.status
ORDER BY count DESC;

-- Escalated incidents should exist (critical + known_bad)
ASSERT VALUE escalated_count >= 5
SELECT COUNT(*) AS escalated_count
FROM cyber.gold.fact_incidents
WHERE status = 'escalated';

SELECT 'Escalation threshold met' AS status;

-- -----------------------------------------------------------------------------
-- 9. Dimension Completeness
-- -----------------------------------------------------------------------------
ASSERT VALUE ip_dim_count = 20
SELECT COUNT(*) AS ip_dim_count FROM cyber.gold.dim_source_ip;

ASSERT VALUE target_dim_count >= 7
SELECT COUNT(*) AS target_dim_count FROM cyber.gold.dim_target;

ASSERT VALUE rule_dim_count >= 10
SELECT COUNT(*) AS rule_dim_count FROM cyber.gold.dim_rule;

ASSERT VALUE mitre_dim_count = 15
SELECT COUNT(*) AS mitre_dim_count FROM cyber.gold.dim_mitre;

SELECT 'Dimension counts verified' AS status;

-- -----------------------------------------------------------------------------
-- 10. Referential Integrity
-- -----------------------------------------------------------------------------
ASSERT VALUE orphan_ips = 0
SELECT COUNT(*) AS orphan_ips
FROM cyber.gold.fact_incidents fi
LEFT JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.source_ip_key IS NULL;

ASSERT VALUE orphan_targets = 0
SELECT COUNT(*) AS orphan_targets
FROM cyber.gold.fact_incidents fi
LEFT JOIN cyber.gold.dim_target dt ON fi.target_key = dt.target_key
WHERE dt.target_key IS NULL;

ASSERT VALUE orphan_rules = 0
SELECT COUNT(*) AS orphan_rules
FROM cyber.gold.fact_incidents fi
LEFT JOIN cyber.gold.dim_rule dr ON fi.rule_key = dr.rule_key
WHERE dr.rule_key IS NULL;

SELECT 'Referential integrity verified' AS status;

-- -----------------------------------------------------------------------------
-- 11. Geo Distribution of Threats
-- -----------------------------------------------------------------------------
SELECT
  dip.geo_country,
  COUNT(fi.incident_key) AS incidents,
  SUM(fi.alert_count) AS total_alerts,
  ROUND(AVG(dip.threat_score), 1) AS avg_threat_score,
  SUM(CASE WHEN dip.is_known_bad = true THEN 1 ELSE 0 END) AS known_bad_incidents
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
GROUP BY dip.geo_country
ORDER BY incidents DESC;

-- External countries should have higher threat scores than internal
ASSERT VALUE external_avg_score >= 50
SELECT ROUND(AVG(dip.threat_score), 1) AS external_avg_score
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
WHERE dip.geo_country != 'INTERNAL';

SELECT 'External threat score threshold met' AS status;

-- -----------------------------------------------------------------------------
-- 12. Multi-alert Incident Analysis (5+ alerts = high-value incidents)
-- -----------------------------------------------------------------------------
SELECT
  ic.incident_id,
  ic.source_ip,
  dip.geo_country,
  ic.alert_count,
  ic.distinct_targets,
  ic.distinct_rules,
  ic.duration_minutes,
  ic.max_severity,
  ic.status
FROM cyber.silver.incidents_correlated ic
JOIN cyber.gold.dim_source_ip dip ON ic.source_ip = dip.ip_address
WHERE ic.alert_count >= 5
ORDER BY ic.alert_count DESC;

-- Should have at least 3 multi-alert incidents
ASSERT VALUE multi_alert_count >= 3
SELECT COUNT(*) AS multi_alert_count
FROM cyber.silver.incidents_correlated
WHERE alert_count >= 5;

SELECT 'Multi-alert incident check passed' AS status;

-- -----------------------------------------------------------------------------
-- 13. Exfiltration Detection Summary
-- -----------------------------------------------------------------------------
SELECT
  fi.mitre_tactic,
  dm.technique_name,
  COUNT(*) AS exfil_incidents,
  SUM(fi.alert_count) AS total_alerts,
  CAST(AVG(fi.combined_risk_score) AS DECIMAL(5,1)) AS avg_risk
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_mitre dm ON fi.mitre_key = dm.mitre_key
WHERE fi.mitre_tactic = 'exfiltration'
GROUP BY fi.mitre_tactic, dm.technique_name;

-- Should have at least 2 exfiltration incidents
ASSERT VALUE exfil_count >= 2
SELECT COUNT(*) AS exfil_count
FROM cyber.gold.fact_incidents fi
WHERE fi.mitre_tactic = 'exfiltration';

SELECT 'Exfiltration detection check passed' AS status;

-- -----------------------------------------------------------------------------
-- 14. Lateral Movement Chain Detection
-- -----------------------------------------------------------------------------
SELECT
  dip.ip_address AS source,
  dt.hostname AS target,
  dr.rule_name,
  fi.severity,
  fi.alert_count,
  fi.duration_minutes
FROM cyber.gold.fact_incidents fi
JOIN cyber.gold.dim_source_ip dip ON fi.source_ip_key = dip.source_ip_key
JOIN cyber.gold.dim_target dt ON fi.target_key = dt.target_key
JOIN cyber.gold.dim_rule dr ON fi.rule_key = dr.rule_key
WHERE fi.mitre_tactic = 'lateral-movement'
ORDER BY fi.detected_at;

-- -----------------------------------------------------------------------------
-- 15. Verification Summary
-- -----------------------------------------------------------------------------
SELECT 'Cybersecurity Incidents Gold Layer Verification PASSED' AS status;
