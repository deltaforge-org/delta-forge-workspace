-- =============================================================================
-- Real Estate Property Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE realty_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== BRONZE SEED DATA: NEIGHBORHOODS (6 rows) =====================

INSERT INTO realty.bronze.raw_neighborhoods VALUES
  ('NBH-OHP', 'Oak Hill Park',     'Austin',  'Travis',    'TX', 125000.00, 9, 1.20, 82, '2025-01-01T00:00:00'),
  ('NBH-LSQ', 'Lakeside Square',   'Austin',  'Travis',    'TX', 98000.00,  7, 2.10, 71, '2025-01-01T00:00:00'),
  ('NBH-MVW', 'Mountain View',     'Denver',  'Denver',    'CO', 110000.00, 8, 1.50, 75, '2025-01-01T00:00:00'),
  ('NBH-RVB', 'Riverbank Estates', 'Denver',  'Jefferson', 'CO', 145000.00, 9, 0.80, 65, '2025-01-01T00:00:00'),
  ('NBH-SUN', 'Sunset Ridge',      'Phoenix', 'Maricopa',  'AZ', 85000.00,  6, 2.50, 58, '2025-01-01T00:00:00'),
  ('NBH-DWN', 'Downtown Core',     'Austin',  'Travis',    'TX', 135000.00, 8, 1.80, 92, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_neighborhoods;

-- ===================== BRONZE SEED DATA: AGENTS (8 rows) =====================

INSERT INTO realty.bronze.raw_agents VALUES
  ('AGT-01', 'Jennifer Walsh',    'Premier Realty',       'TX-RE-44521', 15, 'Luxury',       'Travis',    '2025-01-01T00:00:00'),
  ('AGT-02', 'Michael Chen',      'Premier Realty',       'TX-RE-33892', 8,  'Residential',  'Travis',    '2025-01-01T00:00:00'),
  ('AGT-03', 'Sarah Martinez',    'Horizon Properties',   'CO-RE-55673', 12, 'Residential',  'Denver',    '2025-01-01T00:00:00'),
  ('AGT-04', 'David Park',        'Horizon Properties',   'CO-RE-22104', 6,  'Condos',       'Jefferson', '2025-01-01T00:00:00'),
  ('AGT-05', 'Lisa Thompson',     'Desert Sun Realty',    'AZ-RE-88745', 10, 'Investment',   'Maricopa',  '2025-01-01T00:00:00'),
  ('AGT-06', 'Robert Kim',        'Desert Sun Realty',    'AZ-RE-99123', 4,  'Residential',  'Maricopa',  '2025-01-01T00:00:00'),
  ('AGT-07', 'Amanda Foster',     'Premier Realty',       'TX-RE-77234', 7,  'First-time',   'Travis',    '2025-01-01T00:00:00'),
  ('AGT-08', 'Carlos Mendez',     'Horizon Properties',   'CO-RE-66890', 9,  'Commercial',   'Denver',    '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_agents;

-- ===================== BRONZE SEED DATA: PROPERTIES (18 rows) =====================

INSERT INTO realty.bronze.raw_properties VALUES
  ('PRC-001', '142 Oak Hill Dr',      'Austin',  'Travis',    'TX', '78745', 'NBH-OHP', 'Single Family', 4, 3.0, 2850, 0.280, 2018, '2025-01-01T00:00:00'),
  ('PRC-002', '88 Lakeside Blvd',     'Austin',  'Travis',    'TX', '78748', 'NBH-LSQ', 'Single Family', 3, 2.0, 1950, 0.180, 2005, '2025-01-01T00:00:00'),
  ('PRC-003', '520 Mountain View Ct', 'Denver',  'Denver',    'CO', '80220', 'NBH-MVW', 'Single Family', 4, 2.5, 2400, 0.220, 2015, '2025-01-01T00:00:00'),
  ('PRC-004', '15 Riverbank Ln',      'Denver',  'Jefferson', 'CO', '80210', 'NBH-RVB', 'Single Family', 5, 3.5, 3500, 0.450, 2020, '2025-01-01T00:00:00'),
  ('PRC-005', '301 Sunset Ridge Way', 'Phoenix', 'Maricopa',  'AZ', '85044', 'NBH-SUN', 'Single Family', 3, 2.0, 1800, 0.150, 2010, '2025-01-01T00:00:00'),
  ('PRC-006', '78 Oak Hill Ct',       'Austin',  'Travis',    'TX', '78745', 'NBH-OHP', 'Townhouse',     3, 2.5, 2100, 0.100, 2019, '2025-01-01T00:00:00'),
  ('PRC-007', '225 Lakeside Ave',     'Austin',  'Travis',    'TX', '78748', 'NBH-LSQ', 'Condo',         2, 2.0, 1200, 0.000, 2021, '2025-01-01T00:00:00'),
  ('PRC-008', '900 Mountain View Dr', 'Denver',  'Denver',    'CO', '80220', 'NBH-MVW', 'Single Family', 3, 2.0, 2000, 0.200, 2008, '2025-01-01T00:00:00'),
  ('PRC-009', '42 Riverbank Ct',      'Denver',  'Jefferson', 'CO', '80210', 'NBH-RVB', 'Townhouse',     3, 2.5, 2200, 0.120, 2022, '2025-01-01T00:00:00'),
  ('PRC-010', '567 Sunset Blvd',      'Phoenix', 'Maricopa',  'AZ', '85044', 'NBH-SUN', 'Single Family', 4, 3.0, 2600, 0.250, 2016, '2025-01-01T00:00:00'),
  ('PRC-011', '33 Oak Hill Ln',       'Austin',  'Travis',    'TX', '78745', 'NBH-OHP', 'Single Family', 5, 4.0, 3800, 0.500, 2021, '2025-01-01T00:00:00'),
  ('PRC-012', '110 Mountain View Pl', 'Denver',  'Denver',    'CO', '80220', 'NBH-MVW', 'Condo',         2, 1.5, 1100, 0.000, 2023, '2025-01-01T00:00:00'),
  ('PRC-013', '88 Sunset Ct',         'Phoenix', 'Maricopa',  'AZ', '85044', 'NBH-SUN', 'Condo',         2, 2.0, 1050, 0.000, 2020, '2025-01-01T00:00:00'),
  ('PRC-014', '450 Lakeside Dr',      'Austin',  'Travis',    'TX', '78748', 'NBH-LSQ', 'Single Family', 3, 2.5, 2200, 0.200, 2012, '2025-01-01T00:00:00'),
  ('PRC-015', '72 Riverbank Way',     'Denver',  'Jefferson', 'CO', '80210', 'NBH-RVB', 'Single Family', 4, 3.0, 2900, 0.350, 2017, '2025-01-01T00:00:00'),
  ('PRC-016', '200 Downtown Ave',     'Austin',  'Travis',    'TX', '78701', 'NBH-DWN', 'Condo',         2, 2.0, 1350, 0.000, 2022, '2025-01-01T00:00:00'),
  ('PRC-017', '55 Sunset Hills Rd',   'Phoenix', 'Maricopa',  'AZ', '85044', 'NBH-SUN', 'Townhouse',     3, 2.0, 1700, 0.080, 2019, '2025-01-01T00:00:00'),
  ('PRC-018', '610 Mountain Peak Dr', 'Denver',  'Denver',    'CO', '80220', 'NBH-MVW', 'Townhouse',     3, 2.5, 1900, 0.100, 2021, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_properties;

-- ===================== BRONZE SEED DATA: ASSESSMENTS (40 rows) =====================
-- 3 annual batches: 2022 (18 properties), 2023 (12 reassessed), 2024 (8 reassessed + 2 new)
-- 3 properties with significant value changes for SCD2 tracking: PRC-001, PRC-004, PRC-011

INSERT INTO realty.bronze.raw_assessments VALUES
  -- ===== Batch 1: 2022 assessments — all 18 properties =====
  ('ASM-2022-001', 'PRC-001', 2022, 480000.00,  180000.00, 300000.00, 0.02150, 10320.00, '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-002', 'PRC-002', 2022, 350000.00,  120000.00, 230000.00, 0.02150, 7525.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-003', 'PRC-003', 2022, 560000.00,  200000.00, 360000.00, 0.00770, 4312.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-004', 'PRC-004', 2022, 800000.00,  350000.00, 450000.00, 0.00770, 6160.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-005', 'PRC-005', 2022, 310000.00,  100000.00, 210000.00, 0.00620, 1922.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-006', 'PRC-006', 2022, 390000.00,  130000.00, 260000.00, 0.02150, 8385.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-007', 'PRC-007', 2022, 260000.00,   80000.00, 180000.00, 0.02150, 5590.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-008', 'PRC-008', 2022, 440000.00,  160000.00, 280000.00, 0.00770, 3388.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-009', 'PRC-009', 2022, 540000.00,  190000.00, 350000.00, 0.00770, 4158.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-010', 'PRC-010', 2022, 380000.00,  130000.00, 250000.00, 0.00620, 2356.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-011', 'PRC-011', 2022, 700000.00,  300000.00, 400000.00, 0.02150, 15050.00, '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-012', 'PRC-012', 2022, 330000.00,  110000.00, 220000.00, 0.00770, 2541.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-013', 'PRC-013', 2022, 220000.00,   75000.00, 145000.00, 0.00620, 1364.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-014', 'PRC-014', 2022, 370000.00,  130000.00, 240000.00, 0.02150, 7955.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-015', 'PRC-015', 2022, 660000.00,  280000.00, 380000.00, 0.00770, 5082.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-016', 'PRC-016', 2022, 420000.00,  150000.00, 270000.00, 0.02150, 9030.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-017', 'PRC-017', 2022, 290000.00,   95000.00, 195000.00, 0.00620, 1798.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2022-018', 'PRC-018', 2022, 410000.00,  140000.00, 270000.00, 0.00770, 3157.00,  '2022-01-15', NULL,                        '2025-01-01T00:00:00'),
  -- ===== Batch 2: 2023 assessments — 12 properties reassessed (value changes) =====
  ('ASM-2023-001', 'PRC-001', 2023, 525000.00,  195000.00, 330000.00, 0.02180, 11445.00, '2023-01-15', 'Market appreciation +9.4%', '2025-01-01T00:00:00'),
  ('ASM-2023-002', 'PRC-002', 2023, 365000.00,  125000.00, 240000.00, 0.02180, 7957.00,  '2023-01-15', 'Minor appreciation',        '2025-01-01T00:00:00'),
  ('ASM-2023-003', 'PRC-003', 2023, 610000.00,  220000.00, 390000.00, 0.00790, 4819.00,  '2023-01-15', 'Strong market demand',      '2025-01-01T00:00:00'),
  ('ASM-2023-004', 'PRC-004', 2023, 875000.00,  380000.00, 495000.00, 0.00790, 6912.50,  '2023-01-15', 'Premium location +9.4%',   '2025-01-01T00:00:00'),
  ('ASM-2023-005', 'PRC-005', 2023, 330000.00,  108000.00, 222000.00, 0.00640, 2112.00,  '2023-01-15', 'Slight increase',           '2025-01-01T00:00:00'),
  ('ASM-2023-006', 'PRC-008', 2023, 470000.00,  170000.00, 300000.00, 0.00790, 3713.00,  '2023-01-15', 'Renovation uplift',         '2025-01-01T00:00:00'),
  ('ASM-2023-007', 'PRC-010', 2023, 405000.00,  140000.00, 265000.00, 0.00640, 2592.00,  '2023-01-15', 'Desert market growth',      '2025-01-01T00:00:00'),
  ('ASM-2023-008', 'PRC-011', 2023, 785000.00,  330000.00, 455000.00, 0.02180, 17113.00, '2023-01-15', 'New construction premium +12.1%', '2025-01-01T00:00:00'),
  ('ASM-2023-009', 'PRC-012', 2023, 355000.00,  118000.00, 237000.00, 0.00790, 2804.50,  '2023-01-15', NULL,                        '2025-01-01T00:00:00'),
  ('ASM-2023-010', 'PRC-014', 2023, 400000.00,  142000.00, 258000.00, 0.02180, 8720.00,  '2023-01-15', 'Neighborhood improvement',  '2025-01-01T00:00:00'),
  ('ASM-2023-011', 'PRC-016', 2023, 450000.00,  165000.00, 285000.00, 0.02180, 9810.00,  '2023-01-15', 'Downtown premium',          '2025-01-01T00:00:00'),
  ('ASM-2023-012', 'PRC-018', 2023, 440000.00,  150000.00, 290000.00, 0.00790, 3476.00,  '2023-01-15', NULL,                        '2025-01-01T00:00:00'),
  -- ===== Batch 3: 2024 assessments — 8 properties reassessed =====
  ('ASM-2024-001', 'PRC-001', 2024, 560000.00,  210000.00, 350000.00, 0.02200, 12320.00, '2024-01-15', 'Continued appreciation +6.7%', '2025-01-01T00:00:00'),
  ('ASM-2024-002', 'PRC-004', 2024, 920000.00,  400000.00, 520000.00, 0.00810, 7452.00,  '2024-01-15', 'Riverbank luxury premium +5.1%', '2025-01-01T00:00:00'),
  ('ASM-2024-003', 'PRC-005', 2024, 345000.00,  112000.00, 233000.00, 0.00650, 2242.50,  '2024-01-15', 'Steady growth',             '2025-01-01T00:00:00'),
  ('ASM-2024-004', 'PRC-008', 2024, 490000.00,  178000.00, 312000.00, 0.00810, 3969.00,  '2024-01-15', 'Post-renovation value',     '2025-01-01T00:00:00'),
  ('ASM-2024-005', 'PRC-011', 2024, 830000.00,  355000.00, 475000.00, 0.02200, 18260.00, '2024-01-15', 'Premium lot +5.7%',         '2025-01-01T00:00:00'),
  ('ASM-2024-006', 'PRC-014', 2024, 420000.00,  150000.00, 270000.00, 0.02200, 9240.00,  '2024-01-15', 'Lakeside area growth',      '2025-01-01T00:00:00'),
  ('ASM-2024-007', 'PRC-016', 2024, 475000.00,  175000.00, 300000.00, 0.02200, 10450.00, '2024-01-15', 'Downtown demand surge',     '2025-01-01T00:00:00'),
  ('ASM-2024-008', 'PRC-017', 2024, 315000.00,  102000.00, 213000.00, 0.00650, 2047.50,  '2024-01-15', NULL,                        '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 40
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_assessments;

-- ===================== BRONZE SEED DATA: TRANSACTIONS (25 rows) =====================
-- Sales across 2022-2024. Some over asking (bidding wars), some under.
-- 2 assessment outliers: PRC-013 sells far above assessed, PRC-004 sells far below.

INSERT INTO realty.bronze.raw_transactions VALUES
  -- 2022 sales
  ('TXN-001', 'PRC-001', 'Thomas Wright',     'Margaret Henderson', 'AGT-01', '2022-05-15', 540000.00, 'Conventional', 12500.00, 45, '2025-01-01T00:00:00'),
  ('TXN-002', 'PRC-002', 'Amanda Foster',      'William Chang',     'AGT-02', '2022-07-20', 365000.00, 'FHA',          8800.00,  62, '2025-01-01T00:00:00'),
  ('TXN-003', 'PRC-005', 'Jessica Nguyen',     'Barbara Nguyen',    'AGT-05', '2022-09-10', 322000.00, 'VA',           7500.00,  55, '2025-01-01T00:00:00'),
  ('TXN-004', 'PRC-007', 'Daniel Lee',         'Linda Petrov',      'AGT-07', '2022-05-30', 275000.00, 'Conventional', 7000.00,  38, '2025-01-01T00:00:00'),
  ('TXN-005', 'PRC-010', 'Olivia Robinson',    'Richard Kim',       'AGT-05', '2022-12-20', 395000.00, 'Conventional', 9500.00,  70, '2025-01-01T00:00:00'),
  -- 2023 sales
  ('TXN-006', 'PRC-003', 'Megan Clark',        'Patricia Kowalski', 'AGT-03', '2023-04-10', 645000.00, 'Conventional', 15200.00, 30, '2025-01-01T00:00:00'),
  ('TXN-007', 'PRC-004', 'William Taylor',     'James Rivera',      'AGT-03', '2023-07-30', 700000.00, 'Jumbo',        22000.00, 52, '2025-01-01T00:00:00'),
  ('TXN-008', 'PRC-006', 'Sophia Anderson',    'Robert Thompson',   'AGT-02', '2023-09-25', 435000.00, 'Conventional', 10500.00, 40, '2025-01-01T00:00:00'),
  ('TXN-009', 'PRC-008', 'James Wilson',       'Michael Garcia',    'AGT-04', '2023-11-15', 458000.00, 'Conventional', 11200.00, 65, '2025-01-01T00:00:00'),
  ('TXN-010', 'PRC-009', 'Ryan Patel',         'Jennifer Adams',    'AGT-03', '2023-06-15', 610000.00, 'Conventional', 14800.00, 28, '2025-01-01T00:00:00'),
  ('TXN-011', 'PRC-013', 'Daniel Lee',         'Nathan Brooks',     'AGT-06', '2023-08-10', 285000.00, 'Conventional', 6800.00,  42, '2025-01-01T00:00:00'),
  ('TXN-012', 'PRC-017', 'Emily Garcia',       'Jake Morrison',     'AGT-06', '2023-10-05', 305000.00, 'FHA',          7400.00,  48, '2025-01-01T00:00:00'),
  -- 2024 sales
  ('TXN-013', 'PRC-011', 'William Taylor',     'Karen Singh',       'AGT-01', '2024-03-20', 810000.00, 'Jumbo',        20500.00, 22, '2025-01-01T00:00:00'),
  ('TXN-014', 'PRC-012', 'Emily Garcia',       'Leo Yamamoto',      'AGT-04', '2024-04-15', 372000.00, 'FHA',          9000.00,  50, '2025-01-01T00:00:00'),
  ('TXN-015', 'PRC-014', 'Christopher Brown',  'Olivia Schmidt',    'AGT-02', '2024-06-20', 425000.00, 'Conventional', 10200.00, 35, '2025-01-01T00:00:00'),
  ('TXN-016', 'PRC-015', 'Megan Clark',        'Paul Rivera',       'AGT-03', '2024-07-30', 748000.00, 'Conventional', 18200.00, 25, '2025-01-01T00:00:00'),
  ('TXN-017', 'PRC-001', 'Ryan Patel',         'Thomas Wright',     'AGT-01', '2024-08-15', 575000.00, 'Conventional', 13500.00, 30, '2025-01-01T00:00:00'),
  ('TXN-018', 'PRC-003', 'James Wilson',       'Megan Clark',       'AGT-08', '2024-09-01', 680000.00, 'Conventional', 16500.00, 32, '2025-01-01T00:00:00'),
  ('TXN-019', 'PRC-006', 'Amanda Foster',      'Sophia Anderson',   'AGT-02', '2024-10-10', 455000.00, 'Conventional', 11000.00, 38, '2025-01-01T00:00:00'),
  ('TXN-020', 'PRC-009', 'Christopher Brown',  'Ryan Patel',        'AGT-04', '2024-11-15', 635000.00, 'Conventional', 15500.00, 20, '2025-01-01T00:00:00'),
  ('TXN-021', 'PRC-005', 'Emily Garcia',       'Jessica Nguyen',    'AGT-06', '2024-07-01', 355000.00, 'VA',           8200.00,  44, '2025-01-01T00:00:00'),
  ('TXN-022', 'PRC-010', 'Jessica Nguyen',     'Olivia Robinson',   'AGT-05', '2024-12-01', 418000.00, 'Conventional', 10100.00, 55, '2025-01-01T00:00:00'),
  ('TXN-023', 'PRC-016', 'Sophia Anderson',    'Owner LLC',         'AGT-07', '2024-05-20', 470000.00, 'Conventional', 11400.00, 18, '2025-01-01T00:00:00'),
  ('TXN-024', 'PRC-018', 'Thomas Wright',      'Builder Corp',      'AGT-08', '2024-06-10', 465000.00, 'Conventional', 11200.00, 27, '2025-01-01T00:00:00'),
  ('TXN-025', 'PRC-007', 'Sophia Anderson',    'Daniel Lee',        'AGT-07', '2024-05-15', 298000.00, 'FHA',          7200.00,  33, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM realty.bronze.raw_transactions;
