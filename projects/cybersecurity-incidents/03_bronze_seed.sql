-- =============================================================================
-- Cybersecurity Incidents Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE 03_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== SEED: THREAT INTELLIGENCE (20 IPs) =====================
-- 5 critical, 10 high, 5 medium threat scores

MERGE INTO cyber.bronze.raw_threat_intel AS target
USING (VALUES
('185.220.101.34',  '185.220.101.0/24', 'RU', 'Moscow',     95, 'apt',         true,  '2023-06-15', '2024-01-14', '2024-01-15T00:00:00'),
('103.235.46.78',   '103.235.46.0/24',  'CN', 'Beijing',    92, 'apt',         true,  '2023-08-01', '2024-01-14', '2024-01-15T00:00:00'),
('112.85.42.187',   '112.85.42.0/24',   'CN', 'Shanghai',   97, 'apt',         true,  '2023-05-20', '2024-01-14', '2024-01-15T00:00:00'),
('46.166.139.111',  '46.166.139.0/24',  'RO', 'Bucharest',  90, 'malware',     true,  '2023-09-10', '2024-01-14', '2024-01-15T00:00:00'),
('23.129.64.201',   '23.129.64.0/24',   'US', 'Seattle',    93, 'tor-exit',    true,  '2023-07-01', '2024-01-14', '2024-01-15T00:00:00'),
('91.215.85.102',   '91.215.85.0/24',   'UA', 'Kyiv',       82, 'bruteforce',  true,  '2023-10-05', '2024-01-14', '2024-01-15T00:00:00'),
('77.247.181.163',  '77.247.181.0/24',  'NL', 'Amsterdam',  85, 'tor-exit',    true,  '2023-11-12', '2024-01-14', '2024-01-15T00:00:00'),
('178.128.220.67',  '178.128.220.0/24', 'DE', 'Frankfurt',  78, 'scanner',     true,  '2023-12-01', '2024-01-14', '2024-01-15T00:00:00'),
('159.89.164.240',  '159.89.164.0/24',  'SG', 'Singapore',  65, 'scanner',     false, '2023-11-20', '2024-01-14', '2024-01-15T00:00:00'),
('198.51.100.23',   '198.51.100.0/24',  'US', 'Ashburn',    15, 'benign',      false, '2024-01-01', '2024-01-14', '2024-01-15T00:00:00'),
('45.33.32.156',    '45.33.32.0/24',    'US', 'Fremont',    12, 'scanner',     false, '2024-01-05', '2024-01-14', '2024-01-15T00:00:00'),
('203.0.113.45',    '203.0.113.0/24',   'AU', 'Sydney',      8, 'benign',      false, '2024-01-10', '2024-01-14', '2024-01-15T00:00:00'),
('10.0.1.50',       '10.0.1.0/24',      'INTERNAL', 'HQ-Floor3',  22, 'insider', false, '2024-01-01', '2024-01-14', '2024-01-15T00:00:00'),
('10.0.2.100',      '10.0.2.0/24',      'INTERNAL', 'HQ-Floor5',  18, 'insider', false, '2024-01-01', '2024-01-14', '2024-01-15T00:00:00'),
('10.0.3.200',      '10.0.3.0/24',      'INTERNAL', 'DC-Primary', 28, 'insider', false, '2024-01-01', '2024-01-14', '2024-01-15T00:00:00'),
('192.168.1.105',   '192.168.1.0/24',   'INTERNAL', 'Branch-NY',  10, 'benign',  false, '2024-01-01', '2024-01-14', '2024-01-15T00:00:00'),
('64.233.160.100',  '64.233.160.0/24',  'US', 'Mountain View', 5, 'benign',    false, '2024-01-12', '2024-01-14', '2024-01-15T00:00:00'),
('94.102.49.193',   '94.102.49.0/24',   'NL', 'Amsterdam',  88, 'botnet',      true,  '2023-08-15', '2024-01-14', '2024-01-15T00:00:00'),
('5.188.86.22',     '5.188.86.0/24',    'RU', 'St Petersburg',91,'apt',        true,  '2023-07-20', '2024-01-14', '2024-01-15T00:00:00'),
('45.155.205.233',  '45.155.205.0/24',  'DE', 'Berlin',     75, 'scanner',     false, '2023-12-15', '2024-01-14', '2024-01-15T00:00:00')
) AS source(ip_address, subnet, geo_country, geo_city, threat_score, threat_category, is_known_bad, first_seen, last_seen, ingested_at)
ON target.ip_address = source.ip_address
WHEN MATCHED THEN UPDATE SET
  subnet         = source.subnet,
  geo_country    = source.geo_country,
  geo_city       = source.geo_city,
  threat_score   = source.threat_score,
  threat_category = source.threat_category,
  is_known_bad   = source.is_known_bad,
  first_seen     = source.first_seen,
  last_seen      = source.last_seen,
  ingested_at    = source.ingested_at
WHEN NOT MATCHED THEN INSERT (ip_address, subnet, geo_country, geo_city, threat_score, threat_category, is_known_bad, first_seen, last_seen, ingested_at)
  VALUES (source.ip_address, source.subnet, source.geo_country, source.geo_city, source.threat_score, source.threat_category, source.is_known_bad, source.first_seen, source.last_seen, source.ingested_at);

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM cyber.bronze.raw_threat_intel;

-- ===================== SEED: MITRE ATT&CK TECHNIQUES (15 across 7 tactics) =====================

MERGE INTO cyber.bronze.raw_mitre_techniques AS target
USING (VALUES
('T1190',     'Exploit Public-Facing Application', 'initial-access',      'TA0001', 'Adversary exploits a vulnerability in an internet-facing application', 8, '2024-01-15T00:00:00'),
('T1110.001', 'Password Guessing',                 'initial-access',      'TA0001', 'Brute force password guessing against authentication services',        6, '2024-01-15T00:00:00'),
('T1059.001', 'PowerShell',                         'execution',           'TA0002', 'Use of PowerShell for command execution',                              7, '2024-01-15T00:00:00'),
('T1059.004', 'Unix Shell',                         'execution',           'TA0002', 'Reverse shell via bash/sh',                                            9, '2024-01-15T00:00:00'),
('T1204.002', 'Malicious File',                     'execution',           'TA0002', 'User executes malicious file (malware dropper)',                       8, '2024-01-15T00:00:00'),
('T1547.001', 'Registry Run Keys',                  'persistence',         'TA0003', 'Persistence via registry run keys or startup folder',                  7, '2024-01-15T00:00:00'),
('T1068',     'Exploitation for Privilege Escalation','privilege-escalation','TA0004', 'Kernel or service exploit for elevated privileges',                  9, '2024-01-15T00:00:00'),
('T1003.001', 'LSASS Memory',                       'credential-access',   'TA0006', 'Credential dumping from LSASS process memory',                        9, '2024-01-15T00:00:00'),
('T1021.002', 'SMB/Windows Admin Shares',            'lateral-movement',    'TA0008', 'Lateral movement via SMB to admin shares',                            8, '2024-01-15T00:00:00'),
('T1046',     'Network Service Discovery',           'reconnaissance',      'TA0043', 'Port scanning and service enumeration',                               4, '2024-01-15T00:00:00'),
('T1048.001', 'Exfiltration Over Symmetric Encrypted Non-C2', 'exfiltration', 'TA0010', 'DNS tunneling for data exfiltration',                              10, '2024-01-15T00:00:00'),
('T1041',     'Exfiltration Over C2 Channel',        'exfiltration',        'TA0010', 'Large data transfer over HTTPS C2 channel',                           9, '2024-01-15T00:00:00'),
('T1566.001', 'Spearphishing Attachment',            'initial-access',      'TA0001', 'Phishing email with malicious attachment',                             7, '2024-01-15T00:00:00'),
('T1027',     'Obfuscated Files or Information',     'defense-evasion',     'TA0005', 'Obfuscation of payloads to evade detection',                          6, '2024-01-15T00:00:00'),
('T1071.001', 'Web Protocols',                       'command-and-control', 'TA0011', 'C2 communication over HTTP/HTTPS',                                    7, '2024-01-15T00:00:00')
) AS source(technique_id, technique_name, tactic, tactic_id, description, severity_weight, ingested_at)
ON target.technique_id = source.technique_id
WHEN MATCHED THEN UPDATE SET
  technique_name  = source.technique_name,
  tactic          = source.tactic,
  tactic_id       = source.tactic_id,
  description     = source.description,
  severity_weight = source.severity_weight,
  ingested_at     = source.ingested_at
WHEN NOT MATCHED THEN INSERT (technique_id, technique_name, tactic, tactic_id, description, severity_weight, ingested_at)
  VALUES (source.technique_id, source.technique_name, source.tactic, source.tactic_id, source.description, source.severity_weight, source.ingested_at);

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM cyber.bronze.raw_mitre_techniques;

-- ===================== SEED: FIREWALL ALERTS (30 rows) =====================
-- Includes ~8 duplicates within 5-min windows

MERGE INTO cyber.bronze.raw_firewall_alerts AS target
USING (VALUES
('FW-001', '185.220.101.34', 'web-prod-01', 'R-SQL-INJ',   '2024-01-15T02:14:30', 8, 'high',     45200,  'TCP', 'SQL injection payload in POST /api/login',               '2024-01-15T02:15:00'),
('FW-002', '185.220.101.34', 'web-prod-01', 'R-SQL-INJ',   '2024-01-15T02:16:45', 8, 'high',     38100,  'TCP', 'SQL injection in GET /api/users DUPLICATE',               '2024-01-15T02:17:00'),
('FW-003', '103.235.46.78',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T03:20:00', 9, 'critical', 1200,   'TCP', 'Brute force: 500 failed SSH attempts in 60s',             '2024-01-15T03:20:30'),
('FW-004', '103.235.46.78',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T03:22:15', 9, 'critical', 980,    'TCP', 'Brute force continued DUPLICATE',                         '2024-01-15T03:22:30'),
('FW-005', '91.215.85.102',  'web-prod-02', 'R-SQL-INJ',   '2024-01-15T04:05:00', 8, 'high',     52000,  'TCP', 'SQL injection in search parameter',                       '2024-01-15T04:05:30'),
('FW-006', '91.215.85.102',  'web-prod-02', 'R-SQL-INJ',   '2024-01-15T04:07:30', 8, 'high',     48000,  'TCP', 'SQL injection DUPLICATE',                                 '2024-01-15T04:08:00'),
('FW-007', '77.247.181.163', 'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T06:10:00', 8, 'high',     2100,   'TCP', 'Brute force VPN login attempts',                          '2024-01-15T06:10:30'),
('FW-008', '77.247.181.163', 'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T06:12:30', 8, 'high',     1800,   'TCP', 'VPN brute force DUPLICATE',                               '2024-01-15T06:13:00'),
('FW-009', '178.128.220.67', 'api-prod-01', 'R-SQL-INJ',   '2024-01-15T07:00:00', 8, 'high',     61000,  'TCP', 'SQL injection in REST API endpoint',                      '2024-01-15T07:00:30'),
('FW-010', '185.220.101.34', 'api-prod-01', 'R-SQL-INJ',   '2024-01-15T12:00:00', 8, 'high',     42000,  'TCP', 'SQL injection round 2 from known attacker',               '2024-01-15T12:00:30'),
('FW-011', '185.220.101.34', 'api-prod-01', 'R-SQL-INJ',   '2024-01-15T12:03:00', 8, 'high',     39000,  'TCP', 'Continued SQL injection DUPLICATE',                       '2024-01-15T12:03:30'),
('FW-012', '112.85.42.187',  'web-prod-01', 'R-SQL-INJ',   '2024-01-15T10:00:00', 9, 'critical', 78000,  'TCP', 'Advanced SQL injection bypass attempt',                   '2024-01-15T10:00:30'),
('FW-013', '112.85.42.187',  'web-prod-01', 'R-SQL-INJ',   '2024-01-15T10:02:00', 9, 'critical', 71000,  'TCP', 'SQL injection continued DUPLICATE',                       '2024-01-15T10:02:30'),
('FW-014', '112.85.42.187',  'api-prod-01', 'R-SQL-INJ',   '2024-01-15T10:05:00', 9, 'critical', 65000,  'TCP', 'API SQL injection from same source',                      '2024-01-15T10:05:30'),
('FW-015', '103.235.46.78',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T12:30:00', 9, 'critical', 1500,   'TCP', 'VPN brute force from China',                              '2024-01-15T12:30:30'),
('FW-016', '77.247.181.163', 'web-prod-01', 'R-SQL-INJ',   '2024-01-15T14:00:00', 8, 'high',     55000,  'TCP', 'SQL injection from Tor node',                             '2024-01-15T14:00:30'),
('FW-017', '185.220.101.34', 'web-prod-02', 'R-SQL-INJ',   '2024-01-15T17:30:00', 8, 'high',     47000,  'TCP', 'SQL injection probe web-prod-02',                         '2024-01-15T17:30:30'),
('FW-018', '178.128.220.67', 'web-prod-02', 'R-SQL-INJ',   '2024-01-15T17:00:00', 8, 'high',     59000,  'TCP', 'Automated SQL injection scanner',                         '2024-01-15T17:00:30'),
('FW-019', '178.128.220.67', 'web-prod-02', 'R-SQL-INJ',   '2024-01-15T17:03:00', 8, 'high',     53000,  'TCP', 'SQL injection scan DUPLICATE',                            '2024-01-15T17:03:30'),
('FW-020', '103.235.46.78',  'web-prod-01', 'R-SQL-INJ',   '2024-01-15T19:00:00', 8, 'high',     44000,  'TCP', 'SQL injection from Beijing',                              '2024-01-15T19:00:30'),
('FW-021', '103.235.46.78',  'web-prod-01', 'R-SQL-INJ',   '2024-01-15T19:03:00', 8, 'high',     41000,  'TCP', 'SQL injection DUPLICATE',                                 '2024-01-15T19:03:30'),
('FW-022', '5.188.86.22',    'web-prod-01', 'R-SQL-INJ',   '2024-01-15T20:30:00', 9, 'critical', 82000,  'TCP', 'Advanced persistent SQL injection from Russia',           '2024-01-15T20:30:30'),
('FW-023', '5.188.86.22',    'api-prod-01', 'R-SQL-INJ',   '2024-01-15T20:33:00', 9, 'critical', 76000,  'TCP', 'API attack from same APT source',                         '2024-01-15T20:33:30'),
('FW-024', '94.102.49.193',  'web-prod-02', 'R-SQL-INJ',   '2024-01-15T21:15:00', 8, 'high',     49000,  'TCP', 'SQL injection from botnet node',                          '2024-01-15T21:15:30'),
('FW-025', '185.220.101.34', 'db-prod-01',  'R-EXFIL',     '2024-01-15T22:00:00', 9, 'critical', 250000, 'TCP', 'Large outbound transfer from DB server',                  '2024-01-15T22:00:30'),
('FW-026', '185.220.101.34', 'db-prod-01',  'R-EXFIL',     '2024-01-15T22:03:00', 9, 'critical', 220000, 'TCP', 'Exfiltration continued DUPLICATE',                        '2024-01-15T22:03:30'),
('FW-027', '23.129.64.201',  'web-prod-01', 'R-SQL-INJ',   '2024-01-15T23:50:00', 8, 'high',     38000,  'TCP', 'Late night SQL injection from Tor',                       '2024-01-15T23:50:30'),
('FW-028', '45.155.205.233', 'web-prod-01', 'R-PORTSCAN',  '2024-01-16T01:00:00', 4, 'low',      800,    'TCP', 'Port scan from known scanner',                            '2024-01-16T01:00:30'),
('FW-029', '94.102.49.193',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-16T02:30:00', 8, 'high',     1600,   'TCP', 'VPN brute force from botnet',                             '2024-01-16T02:30:30'),
('FW-030', '5.188.86.22',    'web-prod-01', 'R-SQL-INJ',   '2024-01-16T03:00:00', 9, 'critical', 88000,  'TCP', 'Persistent APT SQL injection next day',                   '2024-01-16T03:00:30')
) AS source(alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, bytes_transferred, protocol, raw_log, ingested_at)
ON target.alert_id = source.alert_id
WHEN MATCHED THEN UPDATE SET
  source_ip         = source.source_ip,
  target_host       = source.target_host,
  rule_id           = source.rule_id,
  detected_at       = source.detected_at,
  severity_score    = source.severity_score,
  severity          = source.severity,
  bytes_transferred = source.bytes_transferred,
  protocol          = source.protocol,
  raw_log           = source.raw_log,
  ingested_at       = source.ingested_at
WHEN NOT MATCHED THEN INSERT (alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, bytes_transferred, protocol, raw_log, ingested_at)
  VALUES (source.alert_id, source.source_ip, source.target_host, source.rule_id, source.detected_at, source.severity_score, source.severity, source.bytes_transferred, source.protocol, source.raw_log, source.ingested_at);

ASSERT ROW_COUNT = 30
SELECT COUNT(*) AS row_count FROM cyber.bronze.raw_firewall_alerts;

-- ===================== SEED: IDS ALERTS (25 rows) =====================
-- Includes ~7 duplicates within 5-min windows

MERGE INTO cyber.bronze.raw_ids_alerts AS target
USING (VALUES
('IDS-001', '23.129.64.201',  'web-prod-01', 'R-PORTSCAN',  '2024-01-15T08:00:00', 4, 'medium',   'SIG-1001', 'TCP', 'Full port scan from Tor exit node',               '2024-01-15T08:00:30'),
('IDS-002', '23.129.64.201',  'web-prod-02', 'R-PORTSCAN',  '2024-01-15T08:02:00', 4, 'medium',   'SIG-1001', 'TCP', 'Port scan targeting web cluster',                  '2024-01-15T08:02:30'),
('IDS-003', '23.129.64.201',  'api-prod-01', 'R-PORTSCAN',  '2024-01-15T08:04:00', 4, 'medium',   'SIG-1001', 'TCP', 'Port scan targeting API server',                   '2024-01-15T08:04:30'),
('IDS-004', '159.89.164.240', 'web-prod-02', 'R-PORTSCAN',  '2024-01-15T11:30:00', 3, 'low',      'SIG-1002', 'TCP', 'Port scan from cloud provider',                    '2024-01-15T11:30:30'),
('IDS-005', '198.51.100.23',  'dev-app-01',  'R-PORTSCAN',  '2024-01-15T14:30:00', 2, 'low',      'SIG-1003', 'TCP', 'Vulnerability scan from security vendor',          '2024-01-15T14:30:30'),
('IDS-006', '45.33.32.156',   'dev-app-01',  'R-PORTSCAN',  '2024-01-15T14:35:00', 2, 'low',      'SIG-1004', 'TCP', 'Nmap scan from known scanner',                     '2024-01-15T14:35:30'),
('IDS-007', '10.0.2.100',     'db-prod-01',  'R-LAT-MOV',   '2024-01-15T07:30:00', 8, 'high',     'SIG-2001', 'SMB', 'SMB lateral movement to database server',          '2024-01-15T07:30:30'),
('IDS-008', '10.0.2.100',     'db-prod-01',  'R-LAT-MOV',   '2024-01-15T07:32:00', 8, 'high',     'SIG-2001', 'SMB', 'SMB lateral movement DUPLICATE',                   '2024-01-15T07:32:30'),
('IDS-009', '91.215.85.102',  'jump-01',     'R-BRUTE-SSH', '2024-01-15T15:30:00', 8, 'high',     'SIG-3001', 'TCP', 'SSH brute force from Ukraine',                     '2024-01-15T15:30:30'),
('IDS-010', '91.215.85.102',  'jump-01',     'R-BRUTE-SSH', '2024-01-15T15:33:00', 8, 'high',     'SIG-3001', 'TCP', 'SSH brute force DUPLICATE',                        '2024-01-15T15:33:30'),
('IDS-011', '23.129.64.201',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T18:00:00', 8, 'high',     'SIG-3002', 'TCP', 'VPN brute force from Tor',                        '2024-01-15T18:00:30'),
('IDS-012', '23.129.64.201',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-15T18:03:00', 8, 'high',     'SIG-3002', 'TCP', 'VPN brute force DUPLICATE',                       '2024-01-15T18:03:30'),
('IDS-013', '10.0.2.100',     'jump-01',     'R-LAT-MOV',   '2024-01-15T23:55:00', 8, 'high',     'SIG-2001', 'SMB', 'Lateral movement via jump server',                 '2024-01-15T23:55:30'),
('IDS-014', '159.89.164.240', 'mail-prod-01','R-PORTSCAN',  '2024-01-15T23:45:00', 3, 'low',      'SIG-1005', 'TCP', 'SMTP banner grab scan',                            '2024-01-15T23:45:30'),
('IDS-015', '203.0.113.45',   'web-prod-01', 'R-PORTSCAN',  '2024-01-15T23:00:00', 2, 'low',      'SIG-1006', 'TCP', 'Routine port scan from Australia',                 '2024-01-15T23:00:30'),
('IDS-016', '77.247.181.163', 'jump-01',     'R-BRUTE-SSH', '2024-01-15T21:30:00', 8, 'high',     'SIG-3003', 'TCP', 'SSH brute force from Netherlands',                 '2024-01-15T21:30:30'),
('IDS-017', '178.128.220.67', 'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-16T00:15:00', 8, 'high',     'SIG-3004', 'TCP', 'Next day VPN brute force',                        '2024-01-16T00:15:30'),
('IDS-018', '112.85.42.187',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-16T03:00:00', 9, 'critical', 'SIG-3005', 'TCP', 'VPN brute force from Shanghai',                   '2024-01-16T03:00:30'),
('IDS-019', '112.85.42.187',  'vpn-gw-01',   'R-BRUTE-SSH', '2024-01-16T03:04:00', 9, 'critical', 'SIG-3005', 'TCP', 'VPN brute force DUPLICATE',                       '2024-01-16T03:04:30'),
('IDS-020', '10.0.2.100',     'api-prod-01', 'R-EXFIL',     '2024-01-15T13:30:00', 9, 'critical', 'SIG-4001', 'TCP', 'Large data transfer 2GB outbound',                '2024-01-15T13:30:30'),
('IDS-021', '91.215.85.102',  'api-prod-01', 'R-REV-SHELL', '2024-01-15T23:15:00', 9, 'critical', 'SIG-5001', 'TCP', 'Reverse shell from Ukraine IP',                   '2024-01-15T23:15:30'),
('IDS-022', '91.215.85.102',  'api-prod-01', 'R-REV-SHELL', '2024-01-15T23:17:00', 9, 'critical', 'SIG-5001', 'TCP', 'Reverse shell DUPLICATE',                         '2024-01-15T23:17:30'),
('IDS-023', '64.233.160.100', 'web-prod-01', 'R-PORTSCAN',  '2024-01-15T16:00:00', 2, 'low',      'SIG-1007', 'TCP', 'Benign scan from Google IP',                      '2024-01-15T16:00:30'),
('IDS-024', '94.102.49.193',  'api-prod-01', 'R-SQL-INJ',   '2024-01-15T22:00:00', 8, 'high',     'SIG-6001', 'TCP', 'Botnet SQL injection attempt',                    '2024-01-15T22:00:30'),
('IDS-025', '45.155.205.233', 'dev-app-01',  'R-PORTSCAN',  '2024-01-16T01:05:00', 3, 'low',      'SIG-1008', 'TCP', 'Dev server scan from Berlin',                     '2024-01-16T01:05:30')
) AS source(alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, signature_id, protocol, raw_log, ingested_at)
ON target.alert_id = source.alert_id
WHEN MATCHED THEN UPDATE SET
  source_ip      = source.source_ip,
  target_host    = source.target_host,
  rule_id        = source.rule_id,
  detected_at    = source.detected_at,
  severity_score = source.severity_score,
  severity       = source.severity,
  signature_id   = source.signature_id,
  protocol       = source.protocol,
  raw_log        = source.raw_log,
  ingested_at    = source.ingested_at
WHEN NOT MATCHED THEN INSERT (alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, signature_id, protocol, raw_log, ingested_at)
  VALUES (source.alert_id, source.source_ip, source.target_host, source.rule_id, source.detected_at, source.severity_score, source.severity, source.signature_id, source.protocol, source.raw_log, source.ingested_at);

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM cyber.bronze.raw_ids_alerts;

-- ===================== SEED: ENDPOINT ALERTS (20 rows) =====================
-- Includes ~5 duplicates within 5-min windows

MERGE INTO cyber.bronze.raw_endpoint_alerts AS target
USING (VALUES
('EP-001', '10.0.1.50',      'db-prod-01',  'R-POWERSHELL', '2024-01-15T05:30:00', 6, 'medium',   'powershell.exe', 'a1b2c3d4e5', 'PowerShell download cradle detected',           '2024-01-15T05:30:30'),
('EP-002', '10.0.1.50',      'db-prod-01',  'R-CRED-DUMP', '2024-01-15T05:32:00', 9, 'critical',  'mimikatz.exe',   'f6g7h8i9j0', 'Mimikatz credential dump detected',             '2024-01-15T05:32:30'),
('EP-003', '10.0.1.50',      'db-prod-01',  'R-CRED-DUMP', '2024-01-15T05:33:00', 9, 'critical',  'mimikatz.exe',   'f6g7h8i9j0', 'Credential dump DUPLICATE',                     '2024-01-15T05:33:30'),
('EP-004', '10.0.1.50',      'jump-01',     'R-REV-SHELL', '2024-01-15T10:30:00', 9, 'critical',  'nc.exe',         'k1l2m3n4o5', 'Reverse shell connection established',           '2024-01-15T10:30:30'),
('EP-005', '10.0.3.200',     'db-prod-01',  'R-PRIV-ESC',  '2024-01-15T11:00:00', 8, 'high',      'exploit.exe',    'p6q7r8s9t0', 'Privilege escalation via kernel exploit',        '2024-01-15T11:00:30'),
('EP-006', '10.0.3.200',     'db-prod-01',  'R-PRIV-ESC',  '2024-01-15T11:02:00', 8, 'high',      'exploit.exe',    'p6q7r8s9t0', 'Priv esc DUPLICATE',                            '2024-01-15T11:02:30'),
('EP-007', '46.166.139.111', 'mail-prod-01','R-MALWARE',   '2024-01-15T09:15:00', 9, 'critical',  'emotet.dll',     'u1v2w3x4y5', 'Emotet malware in email attachment',             '2024-01-15T09:15:30'),
('EP-008', '46.166.139.111', 'mail-prod-01','R-MALWARE',   '2024-01-15T09:17:00', 9, 'critical',  'emotet.dll',     'u1v2w3x4y5', 'Emotet second wave DUPLICATE',                  '2024-01-15T09:17:30'),
('EP-009', '46.166.139.111', 'web-prod-01', 'R-MALWARE',   '2024-01-15T16:00:00', 9, 'critical',  'qakbot.exe',     'z6a7b8c9d0', 'QakBot malware download attempt',               '2024-01-15T16:00:30'),
('EP-010', '10.0.3.200',     'jump-01',     'R-PERSIST',   '2024-01-15T16:30:00', 8, 'high',      'reg.exe',        'e1f2g3h4i5', 'Registry run key persistence added',             '2024-01-15T16:30:30'),
('EP-011', '10.0.3.200',     'jump-01',     'R-PERSIST',   '2024-01-15T16:33:00', 8, 'high',      'reg.exe',        'e1f2g3h4i5', 'Registry persistence DUPLICATE',                '2024-01-15T16:33:30'),
('EP-012', '10.0.1.50',      'db-prod-01',  'R-DNS-TUN',   '2024-01-15T13:00:00', 9, 'critical',  'dns.exe',        'j6k7l8m9n0', 'DNS tunneling exfiltration detected',            '2024-01-15T13:00:30'),
('EP-013', '10.0.1.50',      'db-prod-01',  'R-DNS-TUN',   '2024-01-15T13:03:00', 9, 'critical',  'dns.exe',        'j6k7l8m9n0', 'DNS tunneling DUPLICATE',                       '2024-01-15T13:03:30'),
('EP-014', '10.0.1.50',      'api-prod-01', 'R-EXFIL',     '2024-01-15T18:30:00', 9, 'critical',  'curl.exe',       'o1p2q3r4s5', 'Large data exfiltration via HTTPS',              '2024-01-15T18:30:30'),
('EP-015', '10.0.1.50',      'mail-prod-01','R-EXFIL',     '2024-01-15T22:30:00', 9, 'critical',  'outlook.exe',    't6u7v8w9x0', 'Email exfil: large attachment outbound',         '2024-01-15T22:30:30'),
('EP-016', '192.168.1.105',  'dev-app-01',  'R-POWERSHELL', '2024-01-15T19:30:00', 5, 'medium',   'powershell.exe', 'y1z2a3b4c5', 'Suspicious PowerShell from branch office',      '2024-01-15T19:30:30'),
('EP-017', '46.166.139.111', 'api-prod-01', 'R-REV-SHELL', '2024-01-15T20:00:00', 9, 'critical',  'bash',           'd6e7f8g9h0', 'Reverse shell callback detected',                '2024-01-15T20:00:30'),
('EP-018', '46.166.139.111', 'api-prod-01', 'R-REV-SHELL', '2024-01-15T20:02:00', 9, 'critical',  'bash',           'd6e7f8g9h0', 'Reverse shell DUPLICATE',                       '2024-01-15T20:02:30'),
('EP-019', '10.0.2.100',     'db-prod-01',  'R-CRED-DUMP', '2024-01-15T20:30:00', 9, 'critical',  'procdump.exe',   'i1j2k3l4m5', 'Second credential dump attempt',                '2024-01-15T20:30:30'),
('EP-020', '112.85.42.187',  'web-prod-01', 'R-MALWARE',   '2024-01-15T21:00:00', 9, 'critical',  'cobalt.exe',     'n6o7p8q9r0', 'Cobalt Strike beacon detected',                 '2024-01-15T21:00:30')
) AS source(alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, process_name, file_hash, raw_log, ingested_at)
ON target.alert_id = source.alert_id
WHEN MATCHED THEN UPDATE SET
  source_ip      = source.source_ip,
  target_host    = source.target_host,
  rule_id        = source.rule_id,
  detected_at    = source.detected_at,
  severity_score = source.severity_score,
  severity       = source.severity,
  process_name   = source.process_name,
  file_hash      = source.file_hash,
  raw_log        = source.raw_log,
  ingested_at    = source.ingested_at
WHEN NOT MATCHED THEN INSERT (alert_id, source_ip, target_host, rule_id, detected_at, severity_score, severity, process_name, file_hash, raw_log, ingested_at)
  VALUES (source.alert_id, source.source_ip, source.target_host, source.rule_id, source.detected_at, source.severity_score, source.severity, source.process_name, source.file_hash, source.raw_log, source.ingested_at);

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM cyber.bronze.raw_endpoint_alerts;
