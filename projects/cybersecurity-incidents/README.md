# Cybersecurity Incidents Pipeline

## Scenario

A Security Operations Center (SOC) ingests SIEM alerts every 15 minutes from 3 sources: firewall (30 alerts), IDS (25 alerts), and endpoint detection (20 alerts). Raw alerts contain ~20 duplicates within 5-minute windows that must be deduplicated. The pipeline correlates multi-alert sequences into incidents using window-based gap detection (15-min threshold), enriches with threat intelligence scores and MITRE ATT&CK classification (15 techniques across 7 tactics), and produces a real-time threat dashboard with response metrics.

## Table Schemas

### Bronze Layer (5 tables)
- **raw_firewall_alerts** (30 rows): Network-level alerts with bytes_transferred, protocol
- **raw_ids_alerts** (25 rows): Intrusion detection with signature_id, protocol
- **raw_endpoint_alerts** (20 rows): Host-based alerts with process_name, file_hash
- **raw_threat_intel** (20 rows): IP addresses with threat scores (5 critical, 10 high, 5 medium), categories (apt, malware, tor-exit, scanner, botnet, insider, benign)
- **raw_mitre_techniques** (15 rows): MITRE ATT&CK techniques across 7 tactics

### Silver Layer (3 tables)
- **alerts_deduped** (CDF enabled): 3-source UNION with ROW_NUMBER dedup within 5-min windows
- **incidents_correlated**: Window-based gap detection groups alerts into incidents, severity escalation
- **threat_enriched**: Joined with threat_intel + MITRE, combined_risk_score calculated

### Gold Layer (6 tables, Star Schema)
- **fact_incidents**: Incident facts with 4 dimension keys, severity, alert_count, duration, risk scores, MITRE tactic, status
- **dim_source_ip** (20 IPs): IP dimension with geo, threat scores, pseudonymised to /24
- **dim_target** (8 hosts): Target dimension across 3 environments (prod/dmz/dev)
- **dim_rule** (12 rules): Detection rule metadata with categories
- **dim_mitre** (15 techniques): MITRE ATT&CK tactic + technique hierarchy
- **kpi_threat_dashboard**: Hourly metrics with alert velocity, severity distribution, top tactic/IP
- **kpi_response_metrics**: Per-severity metrics: avg duration, escalation rate, avg threat score

## Medallion Flow

```
BRONZE                              SILVER                              GOLD
+-----------------------+   +------------------------+   +---------------------+
| raw_firewall_alerts   |-->|                        |   | fact_incidents      |
| (30 rows)             |   | alerts_deduped (CDF)   |-->| dim_source_ip       |
+-----------------------+   | (3-source UNION +      |   | dim_target          |
| raw_ids_alerts        |-->|  5-min window dedup)   |   | dim_rule            |
| (25 rows)             |   +------------------------+   | dim_mitre           |
+-----------------------+   | incidents_correlated   |-->| kpi_threat_dashboard|
| raw_endpoint_alerts   |-->| (gap detection grouping|   | kpi_response_metrics|
| (20 rows)             |   |  severity escalation)  |   +---------------------+
+-----------------------+   +------------------------+
| raw_threat_intel (20) |-->| threat_enriched        |
| raw_mitre_tech   (15) |-->| (intel + MITRE join)   |
+-----------------------+   +------------------------+
```

## Star Schema

```
+-------------------+    +---------------------+    +-------------------+
| dim_source_ip     |    | fact_incidents      |    | dim_target        |
|-------------------|    |---------------------|    |-------------------|
| source_ip_key  PK |----| incident_key     PK |----| target_key     PK |
| ip_address        |    | source_ip_key    FK |    | hostname          |
| subnet (/24)      |    | target_key       FK |    | environment       |
| geo_country/city  |    | rule_key         FK |    | criticality       |
| threat_score      |    | mitre_key        FK |    +-------------------+
| threat_category   |    | detected_at         |
| is_known_bad      |    | severity / score    |    +-------------------+
+-------------------+    | alert_count         |    | dim_mitre         |
                         | distinct_targets    |    |-------------------|
+-------------------+    | duration_minutes    |----| mitre_key      PK |
| dim_rule          |    | threat_score        |    | technique_id      |
|-------------------|    | combined_risk_score |    | technique_name    |
| rule_key       PK |----| mitre_tactic        |    | tactic            |
| rule_id           |    | status              |    | severity_weight   |
| rule_name         |    +---------------------+    +-------------------+
| category          |
+-------------------+    +---------------------------+  +-------------------------+
                         | kpi_threat_dashboard      |  | kpi_response_metrics    |
                         |---------------------------|  |-------------------------|
                         | hour_bucket               |  | severity / period       |
                         | total_alerts              |  | incident_count          |
                         | unique_sources/targets    |  | avg_alert_count         |
                         | severity breakdown        |  | avg/max_duration_min    |
                         | avg_severity_score        |  | escalated_count / pct   |
                         | top_mitre_tactic          |  | avg_threat_score        |
                         | top_source_ip             |  +-------------------------+
                         +---------------------------+
```

## Pipeline DAG

```
validate_bronze_3_sources
       |
  +----+----------------+
merge_firewall  merge_ids  merge_endpoint  <-- parallel ingestion
  |              |            |
  +------+-------+------------+
         |
  dedup_5min_windows
         |
  correlate_incidents  <-- window-based gap detection
         |
  enrich_threat_intel  <-- join threat_intel + MITRE
         |
  +------+--------------------+
dim_source_ip  dim_target  dim_rule  dim_mitre  <-- parallel
  |              |           |         |
  +--------------+-----------+---------+
                    |
         build_fact_incidents
                    |
         +----------+----------+
  kpi_threat_dashboard  kpi_response_metrics  <-- parallel
         |                     |
         +----------+----------+
  bloom_and_optimize (CONTINUE ON FAILURE)  <-- bloom filter on source_ip
```

## Key Features

- **Multi-source ingestion**: 3 parallel MERGE steps for firewall, IDS, endpoint alerts
- **5-min window dedup**: ROW_NUMBER partitioned by source_ip + target + rule within 5-min buckets
- **Incident correlation**: Window-based gap detection (15-min threshold) groups alerts into incidents
- **Severity escalation**: Incident severity = MAX of constituent alert severities
- **MITRE ATT&CK classification**: 15 techniques across 7 tactics (initial-access, execution, persistence, privilege-escalation, credential-access, lateral-movement, exfiltration, defense-evasion, command-and-control, reconnaissance)
- **Threat intelligence enrichment**: Scores 0-100 with categories and combined_risk_score
- **Status assignment**: escalated (critical + known_bad), investigating, triaging, monitoring, informational
- **Bloom filter**: On source_ip for fast IP lookups across large datasets
- **OPTIMIZE compaction**: All silver and gold tables
- **Pseudonymisation**: GENERALIZE IP addresses to /24 subnet
- **CDF**: Enabled on alerts_deduped for audit trail
- **Response metrics**: Duration, escalation rate, threat score averages per severity

## Seed Data

- **30 firewall alerts** with bytes_transferred and protocol
- **25 IDS alerts** with signature_id and protocol
- **20 endpoint alerts** with process_name and file_hash
- **~20 duplicates** within 5-min windows across all 3 sources
- **20 threat intel IPs**: 5 critical (apt), 10 high (malware/tor/scanner/botnet), 5 medium/low (benign/insider)
- **15 MITRE techniques** across 7 tactics
- **8 target hosts** across 3 environments (production, dmz, development)
- **12 detection rules** covering web-attack, authentication, execution, malware, persistence, privilege-escalation, credential-access, lateral-movement, reconnaissance, exfiltration
- **3 multi-alert incidents** (5+ alerts from same source)
- **2 exfiltration attempts** (DNS tunneling + HTTPS data transfer)
- **1 lateral movement chain** (SMB movement across hosts)

## Verification Checklist

- [ ] At least 5 known-bad source IPs generated incidents
- [ ] initial-access is most common MITRE tactic
- [ ] Production environment is most targeted
- [ ] All 4 severity levels present (critical/high/medium/low)
- [ ] 10+ hourly buckets in threat dashboard
- [ ] At least 5 escalated incidents
- [ ] 20 source IPs, 7+ targets, 10+ rules, 15 MITRE techniques in dimensions
- [ ] No orphan keys in fact table
- [ ] External IPs average threat score >= 50
- [ ] At least 3 multi-alert incidents (5+ alerts)
- [ ] At least 2 exfiltration incidents detected
- [ ] Lateral movement chain identified
- [ ] Bloom filter created on source_ip
