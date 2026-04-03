-- =============================================================================
-- Food Safety Inspections Pipeline: Create Objects & Seed Data
-- =============================================================================

-- =============================================================================
-- ZONES
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw inspection, establishment, inspector, and violation data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Scored inspections with grade assignment';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Star schema for compliance analytics';

-- =============================================================================
-- BRONZE TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_establishments (
    establishment_id    STRING      NOT NULL,
    name                STRING      NOT NULL,
    cuisine_type        STRING      NOT NULL,
    seating_capacity    INT,
    license_date        DATE        NOT NULL,
    owner_name          STRING      NOT NULL,
    chain_flag          BOOLEAN     NOT NULL,
    district            STRING      NOT NULL,
    city                STRING      NOT NULL,
    state               STRING      NOT NULL,
    risk_category       STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/food_safety/bronze/raw_establishments';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_establishments TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_inspectors (
    inspector_id        STRING      NOT NULL,
    name                STRING      NOT NULL,
    certification_level STRING      NOT NULL,
    years_experience    INT         NOT NULL,
    district            STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/food_safety/bronze/raw_inspectors';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_inspectors TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_violations (
    violation_code      STRING      NOT NULL,
    description         STRING      NOT NULL,
    category            STRING      NOT NULL,
    severity            STRING      NOT NULL,
    points_deducted     INT         NOT NULL,
    corrective_action   STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/food_safety/bronze/raw_violations';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_violations TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_inspections (
    inspection_id       STRING      NOT NULL,
    establishment_id    STRING      NOT NULL,
    inspector_id        STRING      NOT NULL,
    district            STRING      NOT NULL,
    inspection_date     DATE        NOT NULL,
    inspection_type     STRING      NOT NULL,
    violation_codes     STRING,
    follow_up_required  BOOLEAN     NOT NULL,
    closure_ordered     BOOLEAN     NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/food_safety/bronze/raw_inspections'
PARTITIONED BY (district STRING);

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_inspections TO USER {{current_user}};

-- =============================================================================
-- SILVER TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.inspections_scored (
    inspection_id           STRING      NOT NULL,
    establishment_id        STRING      NOT NULL,
    inspector_id            STRING      NOT NULL,
    district                STRING      NOT NULL,
    inspection_date         DATE        NOT NULL,
    inspection_type         STRING      NOT NULL,
    score                   INT         CHECK (score >= 0 AND score <= 100),
    grade                   STRING,
    critical_violations     INT,
    non_critical_violations INT,
    total_points_deducted   INT,
    follow_up_required      BOOLEAN,
    closure_ordered         BOOLEAN,
    scored_at               TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/food_safety/silver/inspections_scored'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.inspections_scored TO USER {{current_user}};

-- =============================================================================
-- GOLD TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_establishment (
    establishment_key   STRING      NOT NULL,
    establishment_id    STRING      NOT NULL,
    name                STRING      NOT NULL,
    cuisine_type        STRING,
    seating_capacity    INT,
    license_date        DATE,
    owner_name          STRING,
    chain_flag          BOOLEAN,
    previous_score      INT,
    risk_category       STRING
) LOCATION '{{data_path}}/food_safety/gold/dim_establishment';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_establishment TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_inspector (
    inspector_key           STRING      NOT NULL,
    inspector_id            STRING      NOT NULL,
    name                    STRING      NOT NULL,
    certification_level     STRING,
    years_experience        INT,
    avg_score_given         DECIMAL(5,2),
    inspection_count        INT
) LOCATION '{{data_path}}/food_safety/gold/dim_inspector';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_inspector TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_district (
    district_key        STRING      NOT NULL,
    district_name       STRING      NOT NULL,
    city                STRING,
    state               STRING,
    population          INT,
    establishment_count INT,
    avg_score           DECIMAL(5,2)
) LOCATION '{{data_path}}/food_safety/gold/dim_district';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_district TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_violation (
    violation_key       STRING      NOT NULL,
    violation_code      STRING      NOT NULL,
    description         STRING,
    category            STRING,
    severity            STRING,
    points_deducted     INT,
    corrective_action   STRING
) LOCATION '{{data_path}}/food_safety/gold/dim_violation';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_violation TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_inspections (
    inspection_key          STRING      NOT NULL,
    establishment_key       STRING      NOT NULL,
    inspector_key           STRING      NOT NULL,
    district_key            STRING      NOT NULL,
    violation_key           STRING,
    inspection_date         DATE        NOT NULL,
    inspection_type         STRING,
    score                   INT,
    grade                   STRING,
    critical_violations     INT,
    non_critical_violations INT,
    follow_up_required      BOOLEAN,
    closure_ordered         BOOLEAN
) LOCATION '{{data_path}}/food_safety/gold/fact_inspections';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_inspections TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_compliance (
    district                    STRING      NOT NULL,
    cuisine_type                STRING      NOT NULL,
    quarter                     STRING      NOT NULL,
    total_inspections           INT,
    avg_score                   DECIMAL(5,2),
    pass_rate                   DECIMAL(5,2),
    critical_violation_rate     DECIMAL(5,4),
    closure_rate                DECIMAL(5,4),
    repeat_offender_count       INT,
    inspector_consistency_score DECIMAL(5,2),
    improvement_trend           STRING
) LOCATION '{{data_path}}/food_safety/gold/kpi_compliance';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_compliance TO USER {{current_user}};

-- =============================================================================
-- PSEUDONYMISATION: Mask inspector names
-- =============================================================================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_inspector (name)
    TRANSFORM mask
    PARAMS ('character' = '*', 'preserve_length' = 'true');

-- =============================================================================
-- SEED DATA: raw_violations (12 violation types)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_violations VALUES
('V001', 'Improper food temperature (hot holding below 140F)',       'Temperature Control', 'Critical',     15, 'Immediate correction required',        '2024-01-01T00:00:00'),
('V002', 'Improper food temperature (cold holding above 41F)',       'Temperature Control', 'Critical',     15, 'Immediate correction required',        '2024-01-01T00:00:00'),
('V003', 'Cross-contamination risk (raw/ready-to-eat contact)',      'Food Handling',       'Critical',     20, 'Dispose affected food, retrain staff', '2024-01-01T00:00:00'),
('V004', 'Inadequate handwashing facilities or practices',           'Personal Hygiene',    'Critical',     15, 'Install/repair facilities, retrain',   '2024-01-01T00:00:00'),
('V005', 'Pest evidence (rodent droppings, insects)',                'Facility',            'Critical',     25, 'Pest control service required',        '2024-01-01T00:00:00'),
('V006', 'Improper food storage (no labeling or dating)',            'Food Handling',       'Non-Critical',  5, 'Label and date all stored items',      '2024-01-01T00:00:00'),
('V007', 'Dirty food contact surfaces',                             'Sanitation',          'Non-Critical', 10, 'Clean and sanitize surfaces',          '2024-01-01T00:00:00'),
('V008', 'Missing or expired food handler permits',                 'Documentation',       'Non-Critical',  5, 'Obtain permits within 30 days',        '2024-01-01T00:00:00'),
('V009', 'Inadequate ventilation or lighting',                      'Facility',            'Non-Critical',  5, 'Repair within 30 days',                '2024-01-01T00:00:00'),
('V010', 'Improper waste disposal or overflowing bins',             'Sanitation',          'Non-Critical',  5, 'Increase pickup frequency',            '2024-01-01T00:00:00'),
('V011', 'No certified food safety manager on premises',            'Documentation',       'Critical',     10, 'Designate certified manager',          '2024-01-01T00:00:00'),
('V012', 'Sewage or wastewater backup',                             'Facility',            'Critical',     30, 'Immediate closure until resolved',     '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_violations;


-- =============================================================================
-- SEED DATA: raw_inspectors (5 inspectors)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_inspectors VALUES
('INS-001', 'Maria Garcia',    'Senior',       12, 'Downtown',      '2024-01-01T00:00:00'),
('INS-002', 'James Chen',      'Senior',        8, 'Midtown',       '2024-01-01T00:00:00'),
('INS-003', 'Patricia Brown',  'Intermediate',  5, 'Uptown',        '2024-01-01T00:00:00'),
('INS-004', 'Robert Johnson',  'Junior',        2, 'Harbor',        '2024-01-01T00:00:00'),
('INS-005', 'Aisha Williams',  'Intermediate',  6, 'Downtown',      '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_inspectors;


-- =============================================================================
-- SEED DATA: raw_establishments (15 establishments across 4 districts)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_establishments VALUES
('EST-001', 'Bella Italia Ristorante',  'Italian',    80, '2018-03-15', 'Marco Bellini',     false, 'Downtown', 'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-002', 'Golden Dragon Palace',     'Chinese',   120, '2015-09-01', 'Wei Zhang',         false, 'Downtown', 'New York', 'NY', 'High',   '2024-01-01T00:00:00'),
('EST-003', 'Burger Barn Express',      'American',   60, '2020-06-10', 'FastFood Corp',     true,  'Downtown', 'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-004', 'Sakura Sushi House',       'Japanese',   45, '2019-11-20', 'Yuki Tanaka',       false, 'Midtown',  'New York', 'NY', 'High',   '2024-01-01T00:00:00'),
('EST-005', 'Taco Fiesta Truck',        'Mexican',     0, '2021-04-05', 'Carlos Mendez',     false, 'Midtown',  'New York', 'NY', 'High',   '2024-01-01T00:00:00'),
('EST-006', 'Le Petit Bistro',          'French',     40, '2017-01-15', 'Chantal Dupont',    false, 'Midtown',  'New York', 'NY', 'Low',    '2024-01-01T00:00:00'),
('EST-007', 'Sunrise Bakery & Cafe',    'Bakery',     30, '2022-02-28', 'Emma Stevens',      false, 'Uptown',   'New York', 'NY', 'Low',    '2024-01-01T00:00:00'),
('EST-008', 'BBQ Smokehouse',           'American',   90, '2016-08-12', 'Tom Wilson',        false, 'Uptown',   'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-009', 'Thai Orchid Kitchen',      'Thai',       55, '2020-10-01', 'Saowalak Patel',    false, 'Uptown',   'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-010', 'Fresh Catch Seafood',      'Seafood',    70, '2019-05-20', 'Harbor Foods LLC',  false, 'Harbor',   'New York', 'NY', 'High',   '2024-01-01T00:00:00'),
('EST-011', 'Harbor Grill & Bar',       'American',  100, '2014-12-01', 'Michael OBrien',    false, 'Harbor',   'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-012', 'Mama Rosa Catering',       'Italian',     0, '2021-07-15', 'Rosa Calabrese',    false, 'Harbor',   'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-013', 'Pizza Palace Chain',       'Italian',    50, '2019-03-10', 'FastFood Corp',     true,  'Downtown', 'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-014', 'Spice Route Indian',       'Indian',     65, '2018-11-25', 'Priya Sharma',      false, 'Midtown',  'New York', 'NY', 'Medium', '2024-01-01T00:00:00'),
('EST-015', 'Green Leaf Vegan',         'Vegetarian', 35, '2023-01-10', 'Sarah Mitchell',    false, 'Uptown',   'New York', 'NY', 'Low',    '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_establishments;


-- =============================================================================
-- SEED DATA: raw_inspections (65 inspections including re-inspections)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_inspections VALUES
-- Q1 2024: Initial inspections
('I001', 'EST-001', 'INS-001', 'Downtown', '2024-01-10', 'Routine',     'V006,V009',          false, false, '2024-01-11T00:00:00'),
('I002', 'EST-002', 'INS-001', 'Downtown', '2024-01-12', 'Routine',     'V001,V003,V007',     true,  false, '2024-01-13T00:00:00'),
('I003', 'EST-003', 'INS-005', 'Downtown', '2024-01-15', 'Routine',     'V006',               false, false, '2024-01-16T00:00:00'),
('I004', 'EST-013', 'INS-005', 'Downtown', '2024-01-17', 'Routine',     'V007,V008',          false, false, '2024-01-18T00:00:00'),
('I005', 'EST-004', 'INS-002', 'Midtown',  '2024-01-11', 'Routine',     'V001,V002,V004,V007',true,  false, '2024-01-12T00:00:00'),
('I006', 'EST-005', 'INS-002', 'Midtown',  '2024-01-14', 'Routine',     'V003,V005,V011',     true,  true,  '2024-01-15T00:00:00'),
('I007', 'EST-006', 'INS-002', 'Midtown',  '2024-01-18', 'Routine',     NULL,                 false, false, '2024-01-19T00:00:00'),
('I008', 'EST-014', 'INS-002', 'Midtown',  '2024-01-22', 'Routine',     'V001,V006',          true,  false, '2024-01-23T00:00:00'),
('I009', 'EST-007', 'INS-003', 'Uptown',   '2024-01-09', 'Routine',     NULL,                 false, false, '2024-01-10T00:00:00'),
('I010', 'EST-008', 'INS-003', 'Uptown',   '2024-01-11', 'Routine',     'V001,V007,V010',     true,  false, '2024-01-12T00:00:00'),
('I011', 'EST-009', 'INS-003', 'Uptown',   '2024-01-16', 'Routine',     'V006,V008',          false, false, '2024-01-17T00:00:00'),
('I012', 'EST-015', 'INS-003', 'Uptown',   '2024-01-19', 'Routine',     'V009',               false, false, '2024-01-20T00:00:00'),
('I013', 'EST-010', 'INS-004', 'Harbor',   '2024-01-10', 'Routine',     'V001,V002,V003,V005',true,  true,  '2024-01-11T00:00:00'),
('I014', 'EST-011', 'INS-004', 'Harbor',   '2024-01-15', 'Routine',     'V007,V010',          false, false, '2024-01-16T00:00:00'),
('I015', 'EST-012', 'INS-004', 'Harbor',   '2024-01-18', 'Routine',     'V004,V006,V008',     true,  false, '2024-01-19T00:00:00'),
-- Q1 2024: Follow-up / Re-inspections
('I016', 'EST-002', 'INS-001', 'Downtown', '2024-02-05', 'Follow-Up',   'V007',               false, false, '2024-02-06T00:00:00'),
('I017', 'EST-005', 'INS-002', 'Midtown',  '2024-02-08', 'Follow-Up',   'V011',               true,  false, '2024-02-09T00:00:00'),
('I018', 'EST-004', 'INS-002', 'Midtown',  '2024-02-10', 'Follow-Up',   'V002',               false, false, '2024-02-11T00:00:00'),
('I019', 'EST-010', 'INS-004', 'Harbor',   '2024-02-12', 'Follow-Up',   'V001,V002',          true,  false, '2024-02-13T00:00:00'),
('I020', 'EST-008', 'INS-003', 'Uptown',   '2024-02-14', 'Follow-Up',   'V010',               false, false, '2024-02-15T00:00:00'),
('I021', 'EST-012', 'INS-004', 'Harbor',   '2024-02-16', 'Follow-Up',   'V006',               false, false, '2024-02-17T00:00:00'),
('I022', 'EST-014', 'INS-002', 'Midtown',  '2024-02-20', 'Follow-Up',   NULL,                 false, false, '2024-02-21T00:00:00'),
-- Q1 2024: Additional routine
('I023', 'EST-001', 'INS-005', 'Downtown', '2024-02-25', 'Routine',     'V007',               false, false, '2024-02-26T00:00:00'),
('I024', 'EST-003', 'INS-001', 'Downtown', '2024-03-01', 'Routine',     NULL,                 false, false, '2024-03-02T00:00:00'),
('I025', 'EST-006', 'INS-002', 'Midtown',  '2024-03-04', 'Routine',     'V009',               false, false, '2024-03-05T00:00:00'),
('I026', 'EST-009', 'INS-003', 'Uptown',   '2024-03-06', 'Routine',     'V006',               false, false, '2024-03-07T00:00:00'),
('I027', 'EST-011', 'INS-004', 'Harbor',   '2024-03-08', 'Routine',     'V008,V010',          false, false, '2024-03-09T00:00:00'),
('I028', 'EST-007', 'INS-003', 'Uptown',   '2024-03-11', 'Routine',     NULL,                 false, false, '2024-03-12T00:00:00'),
('I029', 'EST-013', 'INS-001', 'Downtown', '2024-03-13', 'Routine',     'V001,V006',          true,  false, '2024-03-14T00:00:00'),
('I030', 'EST-015', 'INS-003', 'Uptown',   '2024-03-15', 'Routine',     NULL,                 false, false, '2024-03-16T00:00:00'),
-- Q2 2024: Second round of routine inspections
('I031', 'EST-001', 'INS-001', 'Downtown', '2024-04-08', 'Routine',     'V006',               false, false, '2024-04-09T00:00:00'),
('I032', 'EST-002', 'INS-005', 'Downtown', '2024-04-10', 'Routine',     'V001,V005,V007',     true,  false, '2024-04-11T00:00:00'),
('I033', 'EST-003', 'INS-005', 'Downtown', '2024-04-12', 'Routine',     'V008',               false, false, '2024-04-13T00:00:00'),
('I034', 'EST-013', 'INS-001', 'Downtown', '2024-04-15', 'Follow-Up',   'V006',               false, false, '2024-04-16T00:00:00'),
('I035', 'EST-004', 'INS-002', 'Midtown',  '2024-04-09', 'Routine',     'V001,V007',          true,  false, '2024-04-10T00:00:00'),
('I036', 'EST-005', 'INS-002', 'Midtown',  '2024-04-11', 'Follow-Up',   'V003,V004',          true,  false, '2024-04-12T00:00:00'),
('I037', 'EST-006', 'INS-002', 'Midtown',  '2024-04-16', 'Routine',     NULL,                 false, false, '2024-04-17T00:00:00'),
('I038', 'EST-014', 'INS-002', 'Midtown',  '2024-04-18', 'Routine',     'V006,V009',          false, false, '2024-04-19T00:00:00'),
('I039', 'EST-007', 'INS-003', 'Uptown',   '2024-04-08', 'Routine',     NULL,                 false, false, '2024-04-09T00:00:00'),
('I040', 'EST-008', 'INS-003', 'Uptown',   '2024-04-10', 'Routine',     'V001,V007',          true,  false, '2024-04-11T00:00:00'),
('I041', 'EST-009', 'INS-003', 'Uptown',   '2024-04-14', 'Routine',     'V010',               false, false, '2024-04-15T00:00:00'),
('I042', 'EST-015', 'INS-003', 'Uptown',   '2024-04-17', 'Routine',     NULL,                 false, false, '2024-04-18T00:00:00'),
('I043', 'EST-010', 'INS-004', 'Harbor',   '2024-04-09', 'Routine',     'V001,V003',          true,  false, '2024-04-10T00:00:00'),
('I044', 'EST-011', 'INS-004', 'Harbor',   '2024-04-12', 'Routine',     'V006',               false, false, '2024-04-13T00:00:00'),
('I045', 'EST-012', 'INS-004', 'Harbor',   '2024-04-15', 'Routine',     'V004,V007,V008',     true,  false, '2024-04-16T00:00:00'),
-- Q2 2024: Follow-ups and closures
('I046', 'EST-002', 'INS-001', 'Downtown', '2024-05-02', 'Follow-Up',   'V005',               true,  false, '2024-05-03T00:00:00'),
('I047', 'EST-005', 'INS-002', 'Midtown',  '2024-05-06', 'Follow-Up',   NULL,                 false, false, '2024-05-07T00:00:00'),
('I048', 'EST-004', 'INS-002', 'Midtown',  '2024-05-08', 'Follow-Up',   'V007',               false, false, '2024-05-09T00:00:00'),
('I049', 'EST-010', 'INS-004', 'Harbor',   '2024-05-10', 'Follow-Up',   'V001',               true,  false, '2024-05-11T00:00:00'),
('I050', 'EST-008', 'INS-003', 'Uptown',   '2024-05-13', 'Follow-Up',   NULL,                 false, false, '2024-05-14T00:00:00'),
('I051', 'EST-012', 'INS-004', 'Harbor',   '2024-05-15', 'Follow-Up',   'V008',               false, false, '2024-05-16T00:00:00'),
-- Q2 2024: Complaint-driven inspections
('I052', 'EST-002', 'INS-001', 'Downtown', '2024-05-20', 'Complaint',   'V003,V005,V012',     true,  true,  '2024-05-21T00:00:00'),
('I053', 'EST-010', 'INS-004', 'Harbor',   '2024-05-22', 'Complaint',   'V001,V002,V005',     true,  true,  '2024-05-23T00:00:00'),
('I054', 'EST-005', 'INS-002', 'Midtown',  '2024-05-25', 'Complaint',   'V004,V011',          true,  false, '2024-05-26T00:00:00'),
-- Q2 2024: End of quarter routine
('I055', 'EST-001', 'INS-005', 'Downtown', '2024-06-03', 'Routine',     NULL,                 false, false, '2024-06-04T00:00:00'),
('I056', 'EST-006', 'INS-002', 'Midtown',  '2024-06-05', 'Routine',     'V009',               false, false, '2024-06-06T00:00:00'),
('I057', 'EST-007', 'INS-003', 'Uptown',   '2024-06-07', 'Routine',     NULL,                 false, false, '2024-06-08T00:00:00'),
('I058', 'EST-009', 'INS-003', 'Uptown',   '2024-06-10', 'Routine',     'V006',               false, false, '2024-06-11T00:00:00'),
('I059', 'EST-011', 'INS-004', 'Harbor',   '2024-06-12', 'Routine',     NULL,                 false, false, '2024-06-13T00:00:00'),
('I060', 'EST-013', 'INS-001', 'Downtown', '2024-06-14', 'Routine',     'V006,V007',          false, false, '2024-06-15T00:00:00'),
('I061', 'EST-014', 'INS-002', 'Midtown',  '2024-06-17', 'Routine',     'V001',               true,  false, '2024-06-18T00:00:00'),
('I062', 'EST-015', 'INS-003', 'Uptown',   '2024-06-19', 'Routine',     NULL,                 false, false, '2024-06-20T00:00:00'),
('I063', 'EST-003', 'INS-005', 'Downtown', '2024-06-21', 'Routine',     'V010',               false, false, '2024-06-22T00:00:00'),
('I064', 'EST-012', 'INS-004', 'Harbor',   '2024-06-24', 'Routine',     'V004,V006,V010',     true,  false, '2024-06-25T00:00:00'),
('I065', 'EST-008', 'INS-003', 'Uptown',   '2024-06-26', 'Routine',     'V001,V006',          true,  false, '2024-06-27T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_inspections;

