-- =============================================================================
-- Legal Case Management Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE 03_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== BRONZE SEED: CASES (15 rows) =====================
-- 5 civil, 3 criminal, 3 corporate, 2 IP, 2 employment

MERGE INTO legal.bronze.raw_cases AS target
USING (VALUES
  ('C001', 'CV-2023-001', 'civil',      'NY Supreme',        '2023-01-10', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C002', 'CV-2023-002', 'civil',      'CA Superior',       '2023-02-18', '2024-03-01', 'settled',  'medium', '2024-01-15T00:00:00'),
  ('C003', 'CV-2023-003', 'civil',      'IL Circuit',        '2023-03-22', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C004', 'CV-2023-004', 'civil',      'TX District',       '2023-04-15', '2024-06-15', 'closed',   'low',    '2024-01-15T00:00:00'),
  ('C005', 'CV-2023-005', 'civil',      'FL Circuit',        '2023-05-30', NULL,         'active',   'medium', '2024-01-15T00:00:00'),
  ('C006', 'CR-2023-006', 'criminal',   'SDNY Federal',      '2023-02-01', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C007', 'CR-2023-007', 'criminal',   'EDNY Federal',      '2023-06-12', '2024-01-20', 'closed',   'medium', '2024-01-15T00:00:00'),
  ('C008', 'CR-2023-008', 'criminal',   'DC District',       '2023-08-05', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C009', 'CO-2023-009', 'corporate',  'Delaware Chancery', '2023-03-01', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C010', 'CO-2023-010', 'corporate',  'Delaware Chancery', '2023-07-20', '2024-02-10', 'closed',   'medium', '2024-01-15T00:00:00'),
  ('C011', 'CO-2023-011', 'corporate',  'NY Supreme',        '2023-09-15', NULL,         'active',   'medium', '2024-01-15T00:00:00'),
  ('C012', 'IP-2023-012', 'ip',         'CDCA Federal',      '2023-04-08', NULL,         'active',   'high',   '2024-01-15T00:00:00'),
  ('C013', 'IP-2023-013', 'ip',         'NDCA Federal',      '2023-10-01', NULL,         'active',   'medium', '2024-01-15T00:00:00'),
  ('C014', 'EM-2023-014', 'employment', 'WA Superior',       '2023-05-15', '2024-04-01', 'settled',  'low',    '2024-01-15T00:00:00'),
  ('C015', 'EM-2024-015', 'employment', 'CA Superior',       '2024-01-05', NULL,         'active',   'medium', '2024-01-15T00:00:00')
) AS source(case_id, case_number, case_type, court, filing_date, close_date, status, priority, ingested_at)
ON target.case_id = source.case_id
WHEN MATCHED THEN UPDATE SET
  case_number = source.case_number,
  case_type   = source.case_type,
  court       = source.court,
  filing_date = source.filing_date,
  close_date  = source.close_date,
  status      = source.status,
  priority    = source.priority,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (case_id, case_number, case_type, court, filing_date, close_date, status, priority, ingested_at)
  VALUES (source.case_id, source.case_number, source.case_type, source.court, source.filing_date, source.close_date, source.status, source.priority, source.ingested_at);

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM legal.bronze.raw_cases;


-- ===================== BRONZE SEED: PARTIES (25 rows) =====================
-- plaintiffs, defendants, witnesses, experts

MERGE INTO legal.bronze.raw_parties AS target
USING (VALUES
  ('P001', 'James Whitfield',       'plaintiff',  '111-22-3333', 'j.whitfield@email.com',   '212-555-0101', NULL,                       'NY', '2024-01-15T00:00:00'),
  ('P002', 'Apex Technologies Inc', 'plaintiff',  NULL,          'legal@apextech.com',       '415-555-0201', 'Apex Technologies Inc',    'CA', '2024-01-15T00:00:00'),
  ('P003', 'Maria Santos',          'defendant',  '222-33-4444', 'm.santos@email.com',       '312-555-0301', NULL,                       'IL', '2024-01-15T00:00:00'),
  ('P004', 'GlobalCorp LLC',        'defendant',  NULL,          'counsel@globalcorp.com',    '713-555-0401', 'GlobalCorp LLC',           'TX', '2024-01-15T00:00:00'),
  ('P005', 'Sarah Chen',            'plaintiff',  '333-44-5555', 's.chen@email.com',          '305-555-0501', NULL,                       'FL', '2024-01-15T00:00:00'),
  ('P006', 'Robert Nakamura',       'defendant',  '444-55-6666', 'r.nakamura@email.com',      '202-555-0601', NULL,                       'DC', '2024-01-15T00:00:00'),
  ('P007', 'Pinnacle Holdings',     'defendant',  NULL,          'legal@pinnacle.com',        '302-555-0701', 'Pinnacle Holdings',        'DE', '2024-01-15T00:00:00'),
  ('P008', 'TechVenture Capital',   'plaintiff',  NULL,          'deals@techvc.com',          '646-555-0801', 'TechVenture Capital',      'NY', '2024-01-15T00:00:00'),
  ('P009', 'David Okonkwo',         'plaintiff',  '555-66-7777', 'd.okonkwo@email.com',       '206-555-0901', NULL,                       'WA', '2024-01-15T00:00:00'),
  ('P010', 'NovaBio Sciences',      'defendant',  NULL,          'ip@novabio.com',            '858-555-1001', 'NovaBio Sciences',         'CA', '2024-01-15T00:00:00'),
  ('P011', 'Elena Volkov',          'witness',    '666-77-8888', 'e.volkov@email.com',        '917-555-1101', NULL,                       'NY', '2024-01-15T00:00:00'),
  ('P012', 'Dr. Alan Frost',        'expert',     '777-88-9999', 'a.frost@forensics.com',     '310-555-1201', 'Frost Forensics LLC',      'CA', '2024-01-15T00:00:00'),
  ('P013', 'Linda Park',            'witness',    '888-99-0000', 'l.park@email.com',          '773-555-1301', NULL,                       'IL', '2024-01-15T00:00:00'),
  ('P014', 'Dr. Rachel Torres',     'expert',     '999-00-1111', 'r.torres@ipexperts.com',    '650-555-1401', 'IP Experts Group',         'CA', '2024-01-15T00:00:00'),
  ('P015', 'Marcus Thompson',       'plaintiff',  '100-11-2222', 'm.thompson@email.com',      '404-555-1501', NULL,                       'GA', '2024-01-15T00:00:00'),
  ('P016', 'Quantum Financial',     'defendant',  NULL,          'legal@quantumfin.com',      '212-555-1601', 'Quantum Financial',        'NY', '2024-01-15T00:00:00'),
  ('P017', 'Angela Rivera',         'witness',    '200-22-3333', 'a.rivera@email.com',        '512-555-1701', NULL,                       'TX', '2024-01-15T00:00:00'),
  ('P018', 'Dr. Kevin Walsh',       'expert',     '300-33-4444', 'k.walsh@econ-expert.com',   '617-555-1801', 'Walsh Economics LLC',      'MA', '2024-01-15T00:00:00'),
  ('P019', 'Harborview Properties', 'plaintiff',  NULL,          'office@harborview.com',     '718-555-1901', 'Harborview Properties',    'NY', '2024-01-15T00:00:00'),
  ('P020', 'Patricia Dunn',         'defendant',  '400-44-5555', 'p.dunn@email.com',          '503-555-2001', NULL,                       'OR', '2024-01-15T00:00:00'),
  ('P021', 'Silicon Dynamics Corp', 'plaintiff',  NULL,          'ip@sildyn.com',             '408-555-2101', 'Silicon Dynamics Corp',    'CA', '2024-01-15T00:00:00'),
  ('P022', 'William Huang',         'witness',    '500-55-6666', 'w.huang@email.com',         '213-555-2201', NULL,                       'CA', '2024-01-15T00:00:00'),
  ('P023', 'DataStream Analytics',  'plaintiff',  NULL,          'legal@datastream.com',      '206-555-2301', 'DataStream Analytics',     'WA', '2024-01-15T00:00:00'),
  ('P024', 'MegaCorp Industries',   'defendant',  NULL,          'counsel@megacorp.com',      '312-555-2401', 'MegaCorp Industries',      'IL', '2024-01-15T00:00:00'),
  ('P025', 'Frank Morales',         'witness',    '600-66-7777', 'f.morales@email.com',       '305-555-2501', NULL,                       'FL', '2024-01-15T00:00:00')
) AS source(party_id, party_name, party_type, ssn, contact_email, contact_phone, organization, jurisdiction, ingested_at)
ON target.party_id = source.party_id
WHEN MATCHED THEN UPDATE SET
  party_name    = source.party_name,
  party_type    = source.party_type,
  ssn           = source.ssn,
  contact_email = source.contact_email,
  contact_phone = source.contact_phone,
  organization  = source.organization,
  jurisdiction  = source.jurisdiction,
  ingested_at   = source.ingested_at
WHEN NOT MATCHED THEN INSERT (party_id, party_name, party_type, ssn, contact_email, contact_phone, organization, jurisdiction, ingested_at)
  VALUES (source.party_id, source.party_name, source.party_type, source.ssn, source.contact_email, source.contact_phone, source.organization, source.jurisdiction, source.ingested_at);

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM legal.bronze.raw_parties;


-- ===================== BRONZE SEED: ATTORNEYS (10 rows) =====================
-- 3 practice groups: litigation, corporate, ip

MERGE INTO legal.bronze.raw_attorneys AS target
USING (VALUES
  ('A001', 'Victoria Sterling',  'BAR-2008-4521', 'litigation', true,  16, 650.00, '2024-01-15T00:00:00'),
  ('A002', 'Marcus Chen',        'BAR-2012-7834', 'litigation', false, 12, 475.00, '2024-01-15T00:00:00'),
  ('A003', 'Sophia Ramirez',     'BAR-2010-3267', 'ip',         true,  14, 580.00, '2024-01-15T00:00:00'),
  ('A004', 'James O''Donnell',   'BAR-2015-9102', 'litigation', false,  9, 395.00, '2024-01-15T00:00:00'),
  ('A005', 'Aisha Patel',        'BAR-2009-6453', 'corporate',  true,  15, 620.00, '2024-01-15T00:00:00'),
  ('A006', 'Robert Kowalski',    'BAR-2014-2178', 'litigation', false, 10, 425.00, '2024-01-15T00:00:00'),
  ('A007', 'Elena Volkov',       'BAR-2011-5890', 'corporate',  false, 13, 520.00, '2024-01-15T00:00:00'),
  ('A008', 'David Nakamura',     'BAR-2016-1345', 'ip',         false,  8, 375.00, '2024-01-15T00:00:00'),
  ('A009', 'Catherine Park',     'BAR-2013-8901', 'corporate',  false, 11, 490.00, '2024-01-15T00:00:00'),
  ('A010', 'Thomas Grant',       'BAR-2017-2345', 'litigation', false,  7, 350.00, '2024-01-15T00:00:00')
) AS source(attorney_id, attorney_name, bar_number, practice_group, partner_flag, years_experience, hourly_rate, ingested_at)
ON target.attorney_id = source.attorney_id
WHEN MATCHED THEN UPDATE SET
  attorney_name    = source.attorney_name,
  bar_number       = source.bar_number,
  practice_group   = source.practice_group,
  partner_flag     = source.partner_flag,
  years_experience = source.years_experience,
  hourly_rate      = source.hourly_rate,
  ingested_at      = source.ingested_at
WHEN NOT MATCHED THEN INSERT (attorney_id, attorney_name, bar_number, practice_group, partner_flag, years_experience, hourly_rate, ingested_at)
  VALUES (source.attorney_id, source.attorney_name, source.bar_number, source.practice_group, source.partner_flag, source.years_experience, source.hourly_rate, source.ingested_at);

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM legal.bronze.raw_attorneys;


-- ===================== BRONZE SEED: BILLINGS (70 rows) =====================
-- Some non-billable: admin, pro-bono. 3 high-complexity cases (C001, C006, C009)

MERGE INTO legal.bronze.raw_billings AS target
USING (VALUES
  ('B001', 'C001', 'A001', '2023-01-15', 6.5,  650.00, 4225.00,  true,  'billable',  'Complaint drafting and filing',          '2024-01-15T00:00:00'),
  ('B002', 'C001', 'A001', '2023-02-10', 8.0,  650.00, 5200.00,  true,  'billable',  'Motion for preliminary injunction',      '2024-01-15T00:00:00'),
  ('B003', 'C001', 'A002', '2023-02-10', 5.0,  475.00, 2375.00,  true,  'billable',  'Legal research re: standing',            '2024-01-15T00:00:00'),
  ('B004', 'C001', 'A004', '2023-03-05', 4.0,  395.00, 1580.00,  true,  'billable',  'Witness interview preparation',          '2024-01-15T00:00:00'),
  ('B005', 'C001', 'A001', '2023-04-12', 9.0,  650.00, 5850.00,  true,  'billable',  'Deposition of key witness',              '2024-01-15T00:00:00'),
  ('B006', 'C001', 'A002', '2023-05-20', 3.0,  475.00, 1425.00,  true,  'billable',  'Document review batch 1',                '2024-01-15T00:00:00'),
  ('B007', 'C001', 'A001', '2023-06-15', 2.0,  650.00, 0.00,     false, 'admin',     'Internal case strategy meeting',         '2024-01-15T00:00:00'),
  ('B008', 'C002', 'A002', '2023-02-25', 5.0,  475.00, 2375.00,  true,  'billable',  'Initial case assessment',                '2024-01-15T00:00:00'),
  ('B009', 'C002', 'A006', '2023-03-15', 7.0,  425.00, 2975.00,  true,  'billable',  'Discovery requests drafting',            '2024-01-15T00:00:00'),
  ('B010', 'C002', 'A002', '2023-04-20', 6.0,  475.00, 2850.00,  true,  'billable',  'Interrogatory responses',                '2024-01-15T00:00:00'),
  ('B011', 'C002', 'A006', '2023-06-10', 4.5,  425.00, 1912.50,  true,  'billable',  'Settlement negotiation prep',            '2024-01-15T00:00:00'),
  ('B012', 'C003', 'A004', '2023-04-01', 5.5,  395.00, 2172.50,  true,  'billable',  'Breach of contract analysis',            '2024-01-15T00:00:00'),
  ('B013', 'C003', 'A010', '2023-04-15', 4.0,  350.00, 1400.00,  true,  'billable',  'Damages calculation',                    '2024-01-15T00:00:00'),
  ('B014', 'C003', 'A004', '2023-05-20', 6.0,  395.00, 2370.00,  true,  'billable',  'Motion for summary judgment',            '2024-01-15T00:00:00'),
  ('B015', 'C004', 'A006', '2023-05-01', 3.0,  425.00, 1275.00,  true,  'billable',  'Small claims review',                    '2024-01-15T00:00:00'),
  ('B016', 'C004', 'A010', '2023-06-15', 2.5,  350.00, 875.00,   true,  'billable',  'Settlement documentation',               '2024-01-15T00:00:00'),
  ('B017', 'C005', 'A002', '2023-06-05', 5.0,  475.00, 2375.00,  true,  'billable',  'Insurance dispute filing',               '2024-01-15T00:00:00'),
  ('B018', 'C005', 'A004', '2023-07-10', 4.0,  395.00, 1580.00,  true,  'billable',  'Mediation preparation',                  '2024-01-15T00:00:00'),
  ('B019', 'C005', 'A002', '2023-08-20', 3.5,  475.00, 1662.50,  true,  'billable',  'Expert witness coordination',            '2024-01-15T00:00:00'),
  ('B020', 'C006', 'A001', '2023-02-15', 10.0, 650.00, 6500.00,  true,  'billable',  'Grand jury preparation',                 '2024-01-15T00:00:00'),
  ('B021', 'C006', 'A002', '2023-03-10', 7.0,  475.00, 3325.00,  true,  'billable',  'Evidence review and cataloging',         '2024-01-15T00:00:00'),
  ('B022', 'C006', 'A001', '2023-04-20', 8.5,  650.00, 5525.00,  true,  'billable',  'Pre-trial motions',                      '2024-01-15T00:00:00'),
  ('B023', 'C006', 'A004', '2023-05-15', 6.0,  395.00, 2370.00,  true,  'billable',  'Witness preparation',                    '2024-01-15T00:00:00'),
  ('B024', 'C006', 'A001', '2023-06-25', 9.0,  650.00, 5850.00,  true,  'billable',  'Trial day 1 - opening arguments',        '2024-01-15T00:00:00'),
  ('B025', 'C006', 'A002', '2023-07-15', 5.5,  475.00, 2612.50,  true,  'billable',  'Cross-examination prep',                 '2024-01-15T00:00:00'),
  ('B026', 'C006', 'A001', '2023-08-10', 1.5,  650.00, 0.00,     false, 'pro_bono',  'Community legal aid consult',            '2024-01-15T00:00:00'),
  ('B027', 'C007', 'A004', '2023-06-20', 4.0,  395.00, 1580.00,  true,  'billable',  'Arraignment representation',             '2024-01-15T00:00:00'),
  ('B028', 'C007', 'A010', '2023-07-15', 3.5,  350.00, 1225.00,  true,  'billable',  'Plea bargain research',                  '2024-01-15T00:00:00'),
  ('B029', 'C007', 'A004', '2023-08-25', 5.0,  395.00, 1975.00,  true,  'billable',  'Sentencing memo',                        '2024-01-15T00:00:00'),
  ('B030', 'C008', 'A001', '2023-08-15', 7.0,  650.00, 4550.00,  true,  'billable',  'Federal investigation response',         '2024-01-15T00:00:00'),
  ('B031', 'C008', 'A006', '2023-09-10', 6.0,  425.00, 2550.00,  true,  'billable',  'Evidence suppression motion',            '2024-01-15T00:00:00'),
  ('B032', 'C008', 'A001', '2023-10-05', 8.0,  650.00, 5200.00,  true,  'billable',  'Expert testimony coordination',          '2024-01-15T00:00:00'),
  ('B033', 'C009', 'A005', '2023-03-10', 8.0,  620.00, 4960.00,  true,  'billable',  'Due diligence review',                   '2024-01-15T00:00:00'),
  ('B034', 'C009', 'A007', '2023-03-25', 6.5,  520.00, 3380.00,  true,  'billable',  'Financial statement analysis',           '2024-01-15T00:00:00'),
  ('B035', 'C009', 'A005', '2023-04-15', 7.0,  620.00, 4340.00,  true,  'billable',  'Board resolution drafting',              '2024-01-15T00:00:00'),
  ('B036', 'C009', 'A009', '2023-05-01', 5.0,  490.00, 2450.00,  true,  'billable',  'Regulatory compliance check',            '2024-01-15T00:00:00'),
  ('B037', 'C009', 'A007', '2023-06-10', 5.5,  520.00, 2860.00,  true,  'billable',  'SEC filing preparation',                 '2024-01-15T00:00:00'),
  ('B038', 'C009', 'A005', '2023-07-20', 6.0,  620.00, 3720.00,  true,  'billable',  'Shareholder approval docs',              '2024-01-15T00:00:00'),
  ('B039', 'C009', 'A007', '2023-08-15', 2.0,  520.00, 0.00,     false, 'admin',     'Internal strategy session',              '2024-01-15T00:00:00'),
  ('B040', 'C010', 'A007', '2023-07-25', 5.0,  520.00, 2600.00,  true,  'billable',  'Merger agreement review',                '2024-01-15T00:00:00'),
  ('B041', 'C010', 'A009', '2023-08-10', 4.0,  490.00, 1960.00,  true,  'billable',  'Antitrust analysis',                     '2024-01-15T00:00:00'),
  ('B042', 'C010', 'A005', '2023-09-05', 3.5,  620.00, 2170.00,  true,  'billable',  'Closing documentation',                  '2024-01-15T00:00:00'),
  ('B043', 'C011', 'A005', '2023-09-20', 6.0,  620.00, 3720.00,  true,  'billable',  'Securities compliance audit',            '2024-01-15T00:00:00'),
  ('B044', 'C011', 'A009', '2023-10-15', 5.0,  490.00, 2450.00,  true,  'billable',  'Corporate governance review',            '2024-01-15T00:00:00'),
  ('B045', 'C011', 'A007', '2023-11-10', 4.0,  520.00, 2080.00,  true,  'billable',  'Board meeting preparation',              '2024-01-15T00:00:00'),
  ('B046', 'C012', 'A003', '2023-04-15', 7.5,  580.00, 4350.00,  true,  'billable',  'Patent claim construction',              '2024-01-15T00:00:00'),
  ('B047', 'C012', 'A008', '2023-05-10', 5.0,  375.00, 1875.00,  true,  'billable',  'Prior art search',                       '2024-01-15T00:00:00'),
  ('B048', 'C012', 'A003', '2023-06-20', 9.0,  580.00, 5220.00,  true,  'billable',  'Markman hearing brief',                  '2024-01-15T00:00:00'),
  ('B049', 'C012', 'A008', '2023-07-15', 4.5,  375.00, 1687.50,  true,  'billable',  'Technical expert coordination',          '2024-01-15T00:00:00'),
  ('B050', 'C012', 'A003', '2023-08-25', 6.0,  580.00, 3480.00,  true,  'billable',  'Infringement analysis update',           '2024-01-15T00:00:00'),
  ('B051', 'C013', 'A003', '2023-10-10', 5.0,  580.00, 2900.00,  true,  'billable',  'Trademark opposition filing',            '2024-01-15T00:00:00'),
  ('B052', 'C013', 'A008', '2023-11-05', 4.0,  375.00, 1500.00,  true,  'billable',  'TTAB response drafting',                 '2024-01-15T00:00:00'),
  ('B053', 'C013', 'A003', '2023-12-10', 3.5,  580.00, 2030.00,  true,  'billable',  'Settlement evaluation',                  '2024-01-15T00:00:00'),
  ('B054', 'C014', 'A004', '2023-05-20', 3.0,  395.00, 1185.00,  true,  'billable',  'Initial discrimination claim review',    '2024-01-15T00:00:00'),
  ('B055', 'C014', 'A010', '2023-06-15', 4.5,  350.00, 1575.00,  true,  'billable',  'EEOC charge preparation',                '2024-01-15T00:00:00'),
  ('B056', 'C014', 'A004', '2023-07-25', 5.0,  395.00, 1975.00,  true,  'billable',  'Mediation session',                      '2024-01-15T00:00:00'),
  ('B057', 'C014', 'A010', '2023-09-10', 2.5,  350.00, 875.00,   true,  'billable',  'Settlement finalization',                '2024-01-15T00:00:00'),
  ('B058', 'C015', 'A004', '2024-01-10', 3.5,  395.00, 1382.50,  true,  'billable',  'Wrongful termination intake',            '2024-01-15T00:00:00'),
  ('B059', 'C015', 'A010', '2024-01-12', 2.0,  350.00, 700.00,   true,  'billable',  'Document compilation',                   '2024-01-15T00:00:00'),
  ('B060', 'C001', 'A004', '2023-07-20', 3.5,  395.00, 1382.50,  true,  'billable',  'Expert report review',                   '2024-01-15T00:00:00'),
  ('B061', 'C006', 'A004', '2023-09-15', 2.0,  395.00, 0.00,     false, 'pro_bono',  'Pro bono defendant consult',             '2024-01-15T00:00:00'),
  ('B062', 'C009', 'A009', '2023-09-01', 4.5,  490.00, 2205.00,  true,  'billable',  'Integration planning',                   '2024-01-15T00:00:00'),
  ('B063', 'C003', 'A010', '2023-06-25', 3.0,  350.00, 1050.00,  true,  'billable',  'Appeal research',                        '2024-01-15T00:00:00'),
  ('B064', 'C005', 'A006', '2023-09-15', 4.0,  425.00, 1700.00,  true,  'billable',  'Trial exhibit preparation',              '2024-01-15T00:00:00'),
  ('B065', 'C008', 'A006', '2023-11-01', 5.5,  425.00, 2337.50,  true,  'billable',  'Bail hearing brief',                     '2024-01-15T00:00:00'),
  ('B066', 'C011', 'A005', '2023-12-05', 3.0,  620.00, 1860.00,  true,  'billable',  'Year-end compliance filing',             '2024-01-15T00:00:00'),
  ('B067', 'C012', 'A008', '2023-10-01', 3.0,  375.00, 1125.00,  true,  'billable',  'Damages expert report',                  '2024-01-15T00:00:00'),
  ('B068', 'C008', 'A010', '2023-12-15', 4.0,  350.00, 1400.00,  true,  'billable',  'Appellate brief draft',                  '2024-01-15T00:00:00'),
  ('B069', 'C003', 'A004', '2023-08-10', 1.5,  395.00, 0.00,     false, 'admin',     'Case file organization',                 '2024-01-15T00:00:00'),
  ('B070', 'C006', 'A002', '2023-09-25', 3.0,  475.00, 0.00,     false, 'admin',     'Team coordination call',                 '2024-01-15T00:00:00')
) AS source(billing_id, case_id, attorney_id, billing_date, hours, hourly_rate, amount, billable_flag, billing_type, description, ingested_at)
ON target.billing_id = source.billing_id
WHEN MATCHED THEN UPDATE SET
  case_id       = source.case_id,
  attorney_id   = source.attorney_id,
  billing_date  = source.billing_date,
  hours         = source.hours,
  hourly_rate   = source.hourly_rate,
  amount        = source.amount,
  billable_flag = source.billable_flag,
  billing_type  = source.billing_type,
  description   = source.description,
  ingested_at   = source.ingested_at
WHEN NOT MATCHED THEN INSERT (billing_id, case_id, attorney_id, billing_date, hours, hourly_rate, amount, billable_flag, billing_type, description, ingested_at)
  VALUES (source.billing_id, source.case_id, source.attorney_id, source.billing_date, source.hours, source.hourly_rate, source.amount, source.billable_flag, source.billing_type, source.description, source.ingested_at);

ASSERT ROW_COUNT = 70
SELECT COUNT(*) AS row_count FROM legal.bronze.raw_billings;


-- ===================== BRONZE SEED: RELATIONSHIPS (40 rows) =====================
-- Types: represents, opposes, witnesses_for, co_counsel, referral
-- Includes: 2 attorneys who co-counsel frequently (A001-A002, A005-A007)
-- Includes: 1 potential conflict (A009 connected to opposing parties via referral chain)

MERGE INTO legal.bronze.raw_relationships AS target
USING (VALUES
  -- represents (attorney->party)
  ('R001', 'A001', 'P001', 'attorney', 'party', 'represents',    1.00, '2023-01-10', '2024-01-15T00:00:00'),
  ('R002', 'A002', 'P002', 'attorney', 'party', 'represents',    1.00, '2023-02-18', '2024-01-15T00:00:00'),
  ('R003', 'A004', 'P003', 'attorney', 'party', 'represents',    1.00, '2023-03-22', '2024-01-15T00:00:00'),
  ('R004', 'A006', 'P005', 'attorney', 'party', 'represents',    1.00, '2023-05-30', '2024-01-15T00:00:00'),
  ('R005', 'A001', 'P006', 'attorney', 'party', 'represents',    1.00, '2023-02-01', '2024-01-15T00:00:00'),
  ('R006', 'A005', 'P007', 'attorney', 'party', 'represents',    1.00, '2023-03-01', '2024-01-15T00:00:00'),
  ('R007', 'A003', 'P021', 'attorney', 'party', 'represents',    1.00, '2023-04-08', '2024-01-15T00:00:00'),
  ('R008', 'A004', 'P009', 'attorney', 'party', 'represents',    1.00, '2023-05-15', '2024-01-15T00:00:00'),
  ('R009', 'A007', 'P008', 'attorney', 'party', 'represents',    1.00, '2023-09-15', '2024-01-15T00:00:00'),
  ('R010', 'A009', 'P016', 'attorney', 'party', 'represents',    1.00, '2023-07-20', '2024-01-15T00:00:00'),
  -- opposes (party->party across cases)
  ('R011', 'P001', 'P003', 'party', 'party', 'opposes',          0.80, '2023-01-10', '2024-01-15T00:00:00'),
  ('R012', 'P002', 'P004', 'party', 'party', 'opposes',          0.70, '2023-02-18', '2024-01-15T00:00:00'),
  ('R013', 'P005', 'P020', 'party', 'party', 'opposes',          0.60, '2023-05-30', '2024-01-15T00:00:00'),
  ('R014', 'P006', 'P015', 'party', 'party', 'opposes',          0.90, '2023-02-01', '2024-01-15T00:00:00'),
  ('R015', 'P008', 'P007', 'party', 'party', 'opposes',          0.75, '2023-09-15', '2024-01-15T00:00:00'),
  ('R016', 'P021', 'P010', 'party', 'party', 'opposes',          0.85, '2023-04-08', '2024-01-15T00:00:00'),
  ('R017', 'P009', 'P024', 'party', 'party', 'opposes',          0.50, '2023-05-15', '2024-01-15T00:00:00'),
  ('R018', 'P023', 'P024', 'party', 'party', 'opposes',          0.65, '2024-01-05', '2024-01-15T00:00:00'),
  -- witnesses_for (witness/expert->party)
  ('R019', 'P011', 'P001', 'party', 'party', 'witnesses_for',    0.90, '2023-03-01', '2024-01-15T00:00:00'),
  ('R020', 'P012', 'P006', 'party', 'party', 'witnesses_for',    0.95, '2023-04-01', '2024-01-15T00:00:00'),
  ('R021', 'P013', 'P003', 'party', 'party', 'witnesses_for',    0.85, '2023-04-15', '2024-01-15T00:00:00'),
  ('R022', 'P014', 'P021', 'party', 'party', 'witnesses_for',    0.90, '2023-05-10', '2024-01-15T00:00:00'),
  ('R023', 'P017', 'P004', 'party', 'party', 'witnesses_for',    0.80, '2023-06-01', '2024-01-15T00:00:00'),
  ('R024', 'P018', 'P016', 'party', 'party', 'witnesses_for',    0.85, '2023-07-01', '2024-01-15T00:00:00'),
  ('R025', 'P022', 'P010', 'party', 'party', 'witnesses_for',    0.75, '2023-08-01', '2024-01-15T00:00:00'),
  ('R026', 'P025', 'P005', 'party', 'party', 'witnesses_for',    0.80, '2023-09-01', '2024-01-15T00:00:00'),
  -- co_counsel (attorney->attorney): A001-A002 frequent, A005-A007 frequent
  ('R027', 'A001', 'A002', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-01-10', '2024-01-15T00:00:00'),
  ('R028', 'A001', 'A002', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-02-01', '2024-01-15T00:00:00'),
  ('R029', 'A005', 'A007', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-03-01', '2024-01-15T00:00:00'),
  ('R030', 'A005', 'A007', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-07-25', '2024-01-15T00:00:00'),
  ('R031', 'A005', 'A009', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-03-01', '2024-01-15T00:00:00'),
  ('R032', 'A003', 'A008', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-04-08', '2024-01-15T00:00:00'),
  ('R033', 'A004', 'A010', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-06-20', '2024-01-15T00:00:00'),
  ('R034', 'A001', 'A004', 'attorney', 'attorney', 'co_counsel', 1.00, '2023-02-01', '2024-01-15T00:00:00'),
  -- referral (attorney->party): conflict-of-interest chain via A009
  -- A009 represents P016 (defendant) and was referred P008 (plaintiff opposing P007)
  -- P007 is represented by A005, A009's co-counsel partner
  ('R035', 'A009', 'P008', 'attorney', 'party', 'referral',      0.50, '2023-08-01', '2024-01-15T00:00:00'),
  ('R036', 'A005', 'P019', 'attorney', 'party', 'referral',      0.60, '2023-06-01', '2024-01-15T00:00:00'),
  ('R037', 'A002', 'P023', 'attorney', 'party', 'referral',      0.40, '2023-11-01', '2024-01-15T00:00:00'),
  ('R038', 'A007', 'P016', 'attorney', 'party', 'referral',      0.55, '2023-05-01', '2024-01-15T00:00:00'),
  ('R039', 'A001', 'P015', 'attorney', 'party', 'referral',      0.45, '2023-07-01', '2024-01-15T00:00:00'),
  ('R040', 'A003', 'P010', 'attorney', 'party', 'referral',      0.50, '2023-09-01', '2024-01-15T00:00:00')
) AS source(relationship_id, source_id, target_id, source_type, target_type, relationship_type, weight, effective_date, ingested_at)
ON target.relationship_id = source.relationship_id
WHEN MATCHED THEN UPDATE SET
  source_id         = source.source_id,
  target_id         = source.target_id,
  source_type       = source.source_type,
  target_type       = source.target_type,
  relationship_type = source.relationship_type,
  weight            = source.weight,
  effective_date    = source.effective_date,
  ingested_at       = source.ingested_at
WHEN NOT MATCHED THEN INSERT (relationship_id, source_id, target_id, source_type, target_type, relationship_type, weight, effective_date, ingested_at)
  VALUES (source.relationship_id, source.source_id, source.target_id, source.source_type, source.target_type, source.relationship_type, source.weight, source.effective_date, source.ingested_at);

ASSERT ROW_COUNT = 40
SELECT COUNT(*) AS row_count FROM legal.bronze.raw_relationships;
