-- =============================================================================
-- Cybersecurity Incidents Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw SIEM alert data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Correlated incident data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Threat dashboard star schema';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_source_ips (
    ip_address          STRING      NOT NULL,
    subnet              STRING,
    geo_country         STRING,
    geo_city            STRING,
    threat_intel_score  INT,
    is_known_bad        BOOLEAN,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/siem/raw_source_ips';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_source_ips TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_targets (
    hostname            STRING      NOT NULL,
    service             STRING      NOT NULL,
    port                INT,
    environment         STRING,
    criticality         STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/siem/raw_targets';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_targets TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_rules (
    rule_id             STRING      NOT NULL,
    rule_name           STRING      NOT NULL,
    category            STRING,
    mitre_tactic        STRING,
    mitre_technique     STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/siem/raw_rules';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_rules TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_alerts (
    alert_id            STRING      NOT NULL,
    source_ip           STRING      NOT NULL,
    target_hostname     STRING      NOT NULL,
    rule_id             STRING      NOT NULL,
    detected_at         TIMESTAMP   NOT NULL,
    severity            STRING      NOT NULL,
    raw_log             STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/siem/raw_alerts';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_alerts TO USER {{current_user}};

-- ===================== BRONZE SEED: SOURCE IPS (16 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_source_ips VALUES
    ('185.220.101.34', '185.220.101.0/24', 'RU', 'Moscow', 92, true, '2024-01-15T00:00:00'),
    ('45.33.32.156', '45.33.32.0/24', 'US', 'Fremont', 15, false, '2024-01-15T00:00:00'),
    ('103.235.46.78', '103.235.46.0/24', 'CN', 'Beijing', 88, true, '2024-01-15T00:00:00'),
    ('198.51.100.23', '198.51.100.0/24', 'US', 'Ashburn', 10, false, '2024-01-15T00:00:00'),
    ('91.215.85.102', '91.215.85.0/24', 'UA', 'Kyiv', 78, true, '2024-01-15T00:00:00'),
    ('203.0.113.45', '203.0.113.0/24', 'AU', 'Sydney', 5, false, '2024-01-15T00:00:00'),
    ('77.247.181.163', '77.247.181.0/24', 'NL', 'Amsterdam', 85, true, '2024-01-15T00:00:00'),
    ('10.0.1.50', '10.0.1.0/24', 'INTERNAL', 'HQ-Floor3', 20, false, '2024-01-15T00:00:00'),
    ('10.0.2.100', '10.0.2.0/24', 'INTERNAL', 'HQ-Floor5', 12, false, '2024-01-15T00:00:00'),
    ('178.128.220.67', '178.128.220.0/24', 'DE', 'Frankfurt', 72, true, '2024-01-15T00:00:00'),
    ('159.89.164.240', '159.89.164.0/24', 'SG', 'Singapore', 65, false, '2024-01-15T00:00:00'),
    ('192.168.1.105', '192.168.1.0/24', 'INTERNAL', 'Branch-NY', 8, false, '2024-01-15T00:00:00'),
    ('23.129.64.201', '23.129.64.0/24', 'US', 'Seattle', 90, true, '2024-01-15T00:00:00'),
    ('46.166.139.111', '46.166.139.0/24', 'RO', 'Bucharest', 82, true, '2024-01-15T00:00:00'),
    ('10.0.3.200', '10.0.3.0/24', 'INTERNAL', 'DC-Primary', 25, false, '2024-01-15T00:00:00'),
    ('112.85.42.187', '112.85.42.0/24', 'CN', 'Shanghai', 95, true, '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 16
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_source_ips;


-- ===================== BRONZE SEED: TARGETS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_targets VALUES
    ('web-prod-01', 'nginx', 443, 'production', 'critical', '2024-01-15T00:00:00'),
    ('web-prod-02', 'nginx', 443, 'production', 'critical', '2024-01-15T00:00:00'),
    ('api-prod-01', 'nodejs', 8080, 'production', 'critical', '2024-01-15T00:00:00'),
    ('db-prod-01', 'postgresql', 5432, 'production', 'critical', '2024-01-15T00:00:00'),
    ('mail-prod-01', 'postfix', 25, 'production', 'high', '2024-01-15T00:00:00'),
    ('vpn-gw-01', 'openvpn', 1194, 'dmz', 'high', '2024-01-15T00:00:00'),
    ('jump-01', 'sshd', 22, 'dmz', 'high', '2024-01-15T00:00:00'),
    ('dev-app-01', 'flask', 5000, 'development', 'medium', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_targets;


-- ===================== BRONZE SEED: DETECTION RULES (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_rules VALUES
    ('R001', 'Brute Force SSH Login', 'authentication', 'initial-access', 'T1110.001', '2024-01-15T00:00:00'),
    ('R002', 'SQL Injection Attempt', 'web-attack', 'initial-access', 'T1190', '2024-01-15T00:00:00'),
    ('R003', 'Suspicious PowerShell Execution', 'execution', 'execution', 'T1059.001', '2024-01-15T00:00:00'),
    ('R004', 'Reverse Shell Detection', 'command-control', 'execution', 'T1059.004', '2024-01-15T00:00:00'),
    ('R005', 'Registry Persistence Mechanism', 'persistence', 'persistence', 'T1547.001', '2024-01-15T00:00:00'),
    ('R006', 'Unusual DNS Tunneling', 'exfiltration', 'exfiltration', 'T1048.001', '2024-01-15T00:00:00'),
    ('R007', 'Large Data Transfer Outbound', 'exfiltration', 'exfiltration', 'T1041', '2024-01-15T00:00:00'),
    ('R008', 'Credential Dumping Detected', 'credential-access', 'credential-access', 'T1003.001', '2024-01-15T00:00:00'),
    ('R009', 'Lateral Movement via SMB', 'lateral-movement', 'lateral-movement', 'T1021.002', '2024-01-15T00:00:00'),
    ('R010', 'Port Scan Detected', 'reconnaissance', 'reconnaissance', 'T1046', '2024-01-15T00:00:00'),
    ('R011', 'Malware Signature Match', 'malware', 'execution', 'T1204.002', '2024-01-15T00:00:00'),
    ('R012', 'Privilege Escalation Attempt', 'privilege-escalation', 'privilege-escalation', 'T1068', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_rules;


-- ===================== BRONZE SEED: RAW ALERTS (75 rows) =====================
-- Includes duplicate events within 5-min windows for dedup testing

INSERT INTO {{zone_prefix}}.bronze.raw_alerts VALUES
    ('ALT00001', '185.220.101.34', 'web-prod-01', 'R002', '2024-01-15T02:14:30', 'high', 'SQL injection payload detected in POST /api/login', '2024-01-15T02:15:00'),
    ('ALT00002', '185.220.101.34', 'web-prod-01', 'R002', '2024-01-15T02:15:10', 'high', 'SQL injection payload detected in POST /api/login DUPLICATE', '2024-01-15T02:15:30'),
    ('ALT00003', '185.220.101.34', 'web-prod-01', 'R002', '2024-01-15T02:16:45', 'high', 'SQL injection payload in GET /api/users DUPLICATE', '2024-01-15T02:17:00'),
    ('ALT00004', '103.235.46.78', 'jump-01', 'R001', '2024-01-15T03:20:00', 'critical', 'Brute force: 500 failed SSH attempts in 60s', '2024-01-15T03:20:30'),
    ('ALT00005', '103.235.46.78', 'jump-01', 'R001', '2024-01-15T03:22:15', 'critical', 'Brute force: continued SSH attempts DUPLICATE', '2024-01-15T03:22:30'),
    ('ALT00006', '91.215.85.102', 'web-prod-02', 'R002', '2024-01-15T04:05:00', 'high', 'SQL injection in search parameter', '2024-01-15T04:05:30'),
    ('ALT00007', '91.215.85.102', 'web-prod-02', 'R002', '2024-01-15T04:07:30', 'high', 'SQL injection in search DUPLICATE', '2024-01-15T04:08:00'),
    ('ALT00008', '10.0.1.50', 'db-prod-01', 'R003', '2024-01-15T05:30:00', 'medium', 'PowerShell download cradle detected', '2024-01-15T05:30:30'),
    ('ALT00009', '10.0.1.50', 'db-prod-01', 'R008', '2024-01-15T05:32:00', 'critical', 'Mimikatz credential dump detected', '2024-01-15T05:32:30'),
    ('ALT00010', '10.0.1.50', 'db-prod-01', 'R008', '2024-01-15T05:33:00', 'critical', 'Credential dump continued DUPLICATE', '2024-01-15T05:33:30'),
    ('ALT00011', '77.247.181.163', 'vpn-gw-01', 'R001', '2024-01-15T06:10:00', 'high', 'Brute force VPN login attempts', '2024-01-15T06:10:30'),
    ('ALT00012', '77.247.181.163', 'vpn-gw-01', 'R001', '2024-01-15T06:12:30', 'high', 'VPN brute force continued DUPLICATE', '2024-01-15T06:13:00'),
    ('ALT00013', '178.128.220.67', 'api-prod-01', 'R002', '2024-01-15T07:00:00', 'high', 'SQL injection in REST API endpoint', '2024-01-15T07:00:30'),
    ('ALT00014', '10.0.2.100', 'db-prod-01', 'R009', '2024-01-15T07:30:00', 'high', 'SMB lateral movement to database server', '2024-01-15T07:30:30'),
    ('ALT00015', '10.0.2.100', 'db-prod-01', 'R009', '2024-01-15T07:32:00', 'high', 'SMB lateral movement continued DUPLICATE', '2024-01-15T07:32:30'),
    ('ALT00016', '23.129.64.201', 'web-prod-01', 'R010', '2024-01-15T08:00:00', 'medium', 'Full port scan from Tor exit node', '2024-01-15T08:00:30'),
    ('ALT00017', '23.129.64.201', 'web-prod-02', 'R010', '2024-01-15T08:02:00', 'medium', 'Port scan targeting web servers', '2024-01-15T08:02:30'),
    ('ALT00018', '23.129.64.201', 'api-prod-01', 'R010', '2024-01-15T08:04:00', 'medium', 'Port scan targeting API server', '2024-01-15T08:04:30'),
    ('ALT00019', '46.166.139.111', 'mail-prod-01', 'R011', '2024-01-15T09:15:00', 'critical', 'Emotet malware in email attachment', '2024-01-15T09:15:30'),
    ('ALT00020', '46.166.139.111', 'mail-prod-01', 'R011', '2024-01-15T09:17:00', 'critical', 'Emotet second wave DUPLICATE', '2024-01-15T09:17:30'),
    ('ALT00021', '112.85.42.187', 'web-prod-01', 'R002', '2024-01-15T10:00:00', 'critical', 'Advanced SQL injection bypass attempt', '2024-01-15T10:00:30'),
    ('ALT00022', '112.85.42.187', 'web-prod-01', 'R002', '2024-01-15T10:02:00', 'critical', 'SQL injection continued DUPLICATE', '2024-01-15T10:02:30'),
    ('ALT00023', '112.85.42.187', 'api-prod-01', 'R002', '2024-01-15T10:05:00', 'critical', 'API SQL injection from same source', '2024-01-15T10:05:30'),
    ('ALT00024', '10.0.1.50', 'jump-01', 'R004', '2024-01-15T10:30:00', 'critical', 'Reverse shell connection established', '2024-01-15T10:30:30'),
    ('ALT00025', '10.0.3.200', 'db-prod-01', 'R012', '2024-01-15T11:00:00', 'high', 'Privilege escalation via kernel exploit', '2024-01-15T11:00:30'),
    ('ALT00026', '10.0.3.200', 'db-prod-01', 'R012', '2024-01-15T11:02:00', 'high', 'Priv esc continued DUPLICATE', '2024-01-15T11:02:30'),
    ('ALT00027', '159.89.164.240', 'web-prod-02', 'R010', '2024-01-15T11:30:00', 'low', 'Port scan from cloud provider', '2024-01-15T11:30:30'),
    ('ALT00028', '185.220.101.34', 'api-prod-01', 'R002', '2024-01-15T12:00:00', 'high', 'SQL injection round 2 from known attacker', '2024-01-15T12:00:30'),
    ('ALT00029', '185.220.101.34', 'api-prod-01', 'R002', '2024-01-15T12:03:00', 'high', 'Continued SQL injection DUPLICATE', '2024-01-15T12:03:30'),
    ('ALT00030', '103.235.46.78', 'vpn-gw-01', 'R001', '2024-01-15T12:30:00', 'critical', 'VPN brute force from China', '2024-01-15T12:30:30'),
    ('ALT00031', '10.0.1.50', 'db-prod-01', 'R006', '2024-01-15T13:00:00', 'critical', 'DNS tunneling exfiltration detected', '2024-01-15T13:00:30'),
    ('ALT00032', '10.0.1.50', 'db-prod-01', 'R006', '2024-01-15T13:03:00', 'critical', 'DNS tunneling continued DUPLICATE', '2024-01-15T13:03:30'),
    ('ALT00033', '10.0.2.100', 'api-prod-01', 'R007', '2024-01-15T13:30:00', 'critical', 'Large data transfer 2GB outbound', '2024-01-15T13:30:30'),
    ('ALT00034', '77.247.181.163', 'web-prod-01', 'R002', '2024-01-15T14:00:00', 'high', 'SQL injection from Tor node', '2024-01-15T14:00:30'),
    ('ALT00035', '198.51.100.23', 'dev-app-01', 'R010', '2024-01-15T14:30:00', 'low', 'Vulnerability scan from security vendor', '2024-01-15T14:30:30'),
    ('ALT00036', '45.33.32.156', 'dev-app-01', 'R010', '2024-01-15T14:35:00', 'low', 'Nmap scan from known scanner', '2024-01-15T14:35:30'),
    ('ALT00037', '112.85.42.187', 'db-prod-01', 'R003', '2024-01-15T15:00:00', 'high', 'Remote PowerShell execution attempt', '2024-01-15T15:00:30'),
    ('ALT00038', '112.85.42.187', 'db-prod-01', 'R003', '2024-01-15T15:02:00', 'high', 'PowerShell execution DUPLICATE', '2024-01-15T15:02:30'),
    ('ALT00039', '91.215.85.102', 'jump-01', 'R001', '2024-01-15T15:30:00', 'high', 'SSH brute force from Ukraine', '2024-01-15T15:30:30'),
    ('ALT00040', '91.215.85.102', 'jump-01', 'R001', '2024-01-15T15:33:00', 'high', 'SSH brute force continued DUPLICATE', '2024-01-15T15:33:30'),
    ('ALT00041', '46.166.139.111', 'web-prod-01', 'R011', '2024-01-15T16:00:00', 'critical', 'QakBot malware download attempt', '2024-01-15T16:00:30'),
    ('ALT00042', '10.0.3.200', 'jump-01', 'R005', '2024-01-15T16:30:00', 'high', 'Registry run key persistence added', '2024-01-15T16:30:30'),
    ('ALT00043', '10.0.3.200', 'jump-01', 'R005', '2024-01-15T16:33:00', 'high', 'Registry persistence DUPLICATE', '2024-01-15T16:33:30'),
    ('ALT00044', '178.128.220.67', 'web-prod-02', 'R002', '2024-01-15T17:00:00', 'high', 'Automated SQL injection scanner', '2024-01-15T17:00:30'),
    ('ALT00045', '178.128.220.67', 'web-prod-02', 'R002', '2024-01-15T17:03:00', 'high', 'SQL injection scan DUPLICATE', '2024-01-15T17:03:30'),
    ('ALT00046', '185.220.101.34', 'web-prod-02', 'R002', '2024-01-15T17:30:00', 'high', 'SQL injection probe web-prod-02', '2024-01-15T17:30:30'),
    ('ALT00047', '23.129.64.201', 'vpn-gw-01', 'R001', '2024-01-15T18:00:00', 'high', 'VPN brute force from Tor', '2024-01-15T18:00:30'),
    ('ALT00048', '23.129.64.201', 'vpn-gw-01', 'R001', '2024-01-15T18:03:00', 'high', 'VPN brute force DUPLICATE', '2024-01-15T18:03:30'),
    ('ALT00049', '10.0.1.50', 'api-prod-01', 'R007', '2024-01-15T18:30:00', 'critical', 'Large data exfiltration via HTTPS', '2024-01-15T18:30:30'),
    ('ALT00050', '103.235.46.78', 'web-prod-01', 'R002', '2024-01-15T19:00:00', 'high', 'SQL injection from Beijing', '2024-01-15T19:00:30'),
    ('ALT00051', '103.235.46.78', 'web-prod-01', 'R002', '2024-01-15T19:03:00', 'high', 'SQL injection DUPLICATE', '2024-01-15T19:03:30'),
    ('ALT00052', '192.168.1.105', 'dev-app-01', 'R003', '2024-01-15T19:30:00', 'medium', 'Suspicious PowerShell from branch office', '2024-01-15T19:30:30'),
    ('ALT00053', '46.166.139.111', 'api-prod-01', 'R004', '2024-01-15T20:00:00', 'critical', 'Reverse shell callback detected', '2024-01-15T20:00:30'),
    ('ALT00054', '46.166.139.111', 'api-prod-01', 'R004', '2024-01-15T20:02:00', 'critical', 'Reverse shell DUPLICATE', '2024-01-15T20:02:30'),
    ('ALT00055', '10.0.2.100', 'db-prod-01', 'R008', '2024-01-15T20:30:00', 'critical', 'Second credential dump attempt', '2024-01-15T20:30:30'),
    ('ALT00056', '112.85.42.187', 'web-prod-01', 'R011', '2024-01-15T21:00:00', 'critical', 'Cobalt Strike beacon detected', '2024-01-15T21:00:30'),
    ('ALT00057', '112.85.42.187', 'web-prod-01', 'R011', '2024-01-15T21:02:00', 'critical', 'Cobalt Strike DUPLICATE', '2024-01-15T21:02:30'),
    ('ALT00058', '77.247.181.163', 'jump-01', 'R001', '2024-01-15T21:30:00', 'high', 'SSH brute force from Netherlands', '2024-01-15T21:30:30'),
    ('ALT00059', '185.220.101.34', 'db-prod-01', 'R003', '2024-01-15T22:00:00', 'high', 'Remote PowerShell on DB server', '2024-01-15T22:00:30'),
    ('ALT00060', '185.220.101.34', 'db-prod-01', 'R003', '2024-01-15T22:03:00', 'high', 'PowerShell execution DUPLICATE', '2024-01-15T22:03:30'),
    ('ALT00061', '10.0.1.50', 'mail-prod-01', 'R007', '2024-01-15T22:30:00', 'critical', 'Email exfil: large attachment outbound', '2024-01-15T22:30:30'),
    ('ALT00062', '203.0.113.45', 'web-prod-01', 'R010', '2024-01-15T23:00:00', 'low', 'Routine port scan from Australia', '2024-01-15T23:00:30'),
    ('ALT00063', '91.215.85.102', 'api-prod-01', 'R004', '2024-01-15T23:15:00', 'critical', 'Reverse shell from Ukraine IP', '2024-01-15T23:15:30'),
    ('ALT00064', '91.215.85.102', 'api-prod-01', 'R004', '2024-01-15T23:17:00', 'critical', 'Reverse shell DUPLICATE', '2024-01-15T23:17:30'),
    ('ALT00065', '10.0.3.200', 'api-prod-01', 'R012', '2024-01-15T23:30:00', 'high', 'Priv esc on API server', '2024-01-15T23:30:30'),
    ('ALT00066', '159.89.164.240', 'mail-prod-01', 'R010', '2024-01-15T23:45:00', 'low', 'SMTP banner grab scan', '2024-01-15T23:45:30'),
    ('ALT00067', '23.129.64.201', 'web-prod-01', 'R002', '2024-01-15T23:50:00', 'high', 'Late night SQL injection from Tor', '2024-01-15T23:50:30'),
    ('ALT00068', '10.0.2.100', 'jump-01', 'R009', '2024-01-15T23:55:00', 'high', 'Lateral movement via jump server', '2024-01-15T23:55:30'),
    ('ALT00069', '178.128.220.67', 'vpn-gw-01', 'R001', '2024-01-16T00:15:00', 'high', 'Next day VPN brute force', '2024-01-16T00:15:30'),
    ('ALT00070', '103.235.46.78', 'db-prod-01', 'R008', '2024-01-16T01:00:00', 'critical', 'Credential harvesting on DB', '2024-01-16T01:00:30'),
    ('ALT00071', '103.235.46.78', 'db-prod-01', 'R008', '2024-01-16T01:03:00', 'critical', 'Credential harvest DUPLICATE', '2024-01-16T01:03:30'),
    ('ALT00072', '46.166.139.111', 'web-prod-02', 'R011', '2024-01-16T02:00:00', 'critical', 'Malware dropper detected', '2024-01-16T02:00:30'),
    ('ALT00073', '112.85.42.187', 'vpn-gw-01', 'R001', '2024-01-16T03:00:00', 'critical', 'VPN brute force from Shanghai', '2024-01-16T03:00:30'),
    ('ALT00074', '112.85.42.187', 'vpn-gw-01', 'R001', '2024-01-16T03:04:00', 'critical', 'VPN brute force DUPLICATE', '2024-01-16T03:04:30'),
    ('ALT00075', '185.220.101.34', 'web-prod-01', 'R004', '2024-01-16T04:00:00', 'critical', 'Reverse shell callback from Russia', '2024-01-16T04:00:30');

ASSERT ROW_COUNT = 75
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_alerts;


-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.incidents_correlated (
    incident_id         STRING      NOT NULL,
    source_ip           STRING      NOT NULL,
    target_hostname     STRING      NOT NULL,
    rule_id             STRING      NOT NULL,
    first_detected_at   TIMESTAMP   NOT NULL,
    last_detected_at    TIMESTAMP   NOT NULL,
    severity            STRING      NOT NULL,
    alert_count         INT         NOT NULL,
    mean_time_to_detect_sec INT,
    threat_intel_score  INT,
    is_known_bad        BOOLEAN,
    status              STRING      NOT NULL,
    processed_at        TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/silver/siem/incidents_correlated';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.incidents_correlated TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_source_ip (
    source_ip_key       INT         NOT NULL,
    ip_address          STRING      NOT NULL,
    subnet              STRING,
    geo_country         STRING,
    geo_city            STRING,
    threat_intel_score  INT,
    is_known_bad        BOOLEAN,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/siem/dim_source_ip';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_source_ip TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_target (
    target_key          INT         NOT NULL,
    hostname            STRING      NOT NULL,
    service             STRING,
    port                INT,
    environment         STRING,
    criticality         STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/siem/dim_target';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_target TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_rule (
    rule_key            INT         NOT NULL,
    rule_id             STRING      NOT NULL,
    rule_name           STRING      NOT NULL,
    category            STRING,
    mitre_tactic        STRING,
    mitre_technique     STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/siem/dim_rule';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_rule TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_incidents (
    incident_key        INT         NOT NULL,
    source_ip_key       INT         NOT NULL,
    target_key          INT         NOT NULL,
    rule_key            INT         NOT NULL,
    detected_at         TIMESTAMP   NOT NULL,
    severity            STRING      NOT NULL,
    alert_count         INT         NOT NULL,
    mean_time_to_detect_sec INT,
    status              STRING      NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/siem/fact_incidents';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_incidents TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_threat_dashboard (
    hour_bucket         TIMESTAMP   NOT NULL,
    total_alerts        INT         NOT NULL,
    unique_sources      INT,
    critical_count      INT,
    high_count          INT,
    medium_count        INT,
    low_count           INT,
    mean_time_to_detect INT,
    top_mitre_tactic    STRING,
    loaded_at           TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/gold/siem/kpi_threat_dashboard';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_threat_dashboard TO USER {{current_user}};

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_source_ip (ip_address) TRANSFORM generalize PARAMS (range = 24);

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.incidents_correlated (source_ip) TRANSFORM generalize PARAMS (range = 24);
