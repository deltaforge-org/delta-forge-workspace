# Cybersecurity Incidents Pipeline

## Scenario

A Security Operations Center (SOC) processes SIEM alert feeds every 15 minutes. Raw alerts contain many duplicates (same source IP + target + rule within 5-minute windows) that must be correlated into unique incidents. The pipeline enriches incidents with threat intelligence scores, classifies by MITRE ATT&CK framework, calculates mean-time-to-detect (MTTD), and produces an hourly threat dashboard.

## Table Schemas

### Bronze Layer
- **raw_alerts** (75 rows): alert_id, source_ip, target_hostname, rule_id, detected_at, severity, raw_log -- includes ~25 duplicate alerts within 5-min windows
- **raw_source_ips** (16 rows): IP addresses with subnet, geo location, threat intelligence scores (0-100), known_bad flag
- **raw_targets** (8 rows): hostnames with service, port, environment (production/dmz/development), criticality
- **raw_rules** (12 rows): detection rules mapped to MITRE ATT&CK tactics and techniques

### Silver Layer
- **incidents_correlated** - Deduplicated alerts correlated into incidents with alert counts, MTTD, threat scores, and status assignment

### Gold Layer (Star Schema)
- **fact_incidents** - Incident facts with source IP / target / rule keys, severity, alert counts, MTTD, status
- **dim_source_ip** - Source IP dimension (16 IPs, 8 countries, threat intel scores)
- **dim_target** - Target dimension (8 hosts, 4 services, 3 environments)
- **dim_rule** - Detection rule dimension (12 rules, 7 MITRE tactics)
- **kpi_threat_dashboard** - Hourly threat metrics with alert velocity, severity breakdown, top tactic

## Medallion Flow

```
Bronze                          Silver                           Gold
+-------------------+    +---------------------------+    +--------------------+
| raw_alerts        |--->| incidents_correlated      |--->| fact_incidents     |
| (75 rows, dupes)  |    | (dedup 5-min windows,     |    +--------------------+
+-------------------+    |  threat enrich, MTTD)     |    | dim_source_ip      |
| raw_source_ips    |--->|                           |--->| dim_target         |
| raw_targets       |----+---------------------------+    | dim_rule           |
| raw_rules         |------------------------------------| kpi_threat_dashboard|
+-------------------+                                    +--------------------+
```

## Star Schema

```
+-------------------+    +-------------------+    +-------------------+
| dim_source_ip     |    | fact_incidents    |    | dim_target        |
|-------------------|    |-------------------|    |-------------------|
| source_ip_key  PK |----| incident_key   PK |----| target_key     PK |
| ip_address        |    | source_ip_key  FK |    | hostname          |
| subnet            |    | target_key     FK |    | service, port     |
| geo_country/city  |    | rule_key       FK |    | environment       |
| threat_intel_score|    | detected_at       |    | criticality       |
| is_known_bad      |    | severity          |    +-------------------+
+-------------------+    | alert_count       |
                         | mttd_sec          |    +-------------------+
+-------------------+    | status            |    | kpi_threat_       |
| dim_rule          |    +-------------------+    | dashboard         |
|-------------------|----+                        |-------------------|
| rule_key       PK |                             | hour_bucket       |
| rule_name         |                             | total_alerts      |
| category          |                             | severity breakdown|
| mitre_tactic      |                             | top_mitre_tactic  |
| mitre_technique   |                             +-------------------+
+-------------------+
```

## Key Features

- **Alert deduplication** using ROW_NUMBER partitioned by source_ip + target + rule within 5-min windows
- **MITRE ATT&CK classification**: 7 tactics (initial-access, execution, persistence, credential-access, lateral-movement, exfiltration, reconnaissance)
- **Threat intelligence enrichment**: scores 0-100 with known_bad flag
- **Mean Time to Detect (MTTD)** calculation per incident
- **Status assignment**: escalated (critical + known_bad), investigating, triaging, monitoring, informational
- **Hourly threat dashboard** with alert velocity and severity breakdown
- **OPTIMIZE compaction** on all gold tables
- **Pseudonymisation**: GENERALIZE IP addresses to /24 subnet

## Verification Checklist

- [ ] At least 5 known-bad source IPs generated incidents
- [ ] initial-access is the most common MITRE tactic
- [ ] Production environment is most targeted
- [ ] All 4 severity levels present (critical/high/medium/low)
- [ ] 10+ hourly buckets in threat dashboard
- [ ] At least 5 escalated incidents
- [ ] 16 source IPs, 8 targets, 12 rules in dimensions
- [ ] No orphan keys in fact table
- [ ] External IPs average threat score >= 50
- [ ] Deduplication reduced 75 raw alerts to ~50 incidents
