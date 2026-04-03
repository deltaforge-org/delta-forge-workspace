-- =============================================================================
-- Mining Operations Pipeline: Create Objects & Seed Data
-- =============================================================================

-- =============================================================================
-- ZONES
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw extraction, site, pit, and equipment data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched extraction data with derived metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Star schema for mining production analytics';

-- =============================================================================
-- BRONZE TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sites (
    site_id             STRING      NOT NULL,
    site_name           STRING      NOT NULL,
    mineral_type        STRING      NOT NULL,
    country             STRING      NOT NULL,
    region              STRING      NOT NULL,
    reserve_estimate_mt DECIMAL(12,3) NOT NULL,
    mine_type           STRING      NOT NULL,
    environmental_rating STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/bronze/raw_sites';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sites TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_pits (
    pit_id              STRING      NOT NULL,
    pit_name            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    current_depth_m     DECIMAL(8,2) NOT NULL,
    target_depth_m      DECIMAL(8,2) NOT NULL,
    bench_height_m      DECIMAL(6,2) NOT NULL,
    status              STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/bronze/raw_pits';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_pits TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_equipment (
    equipment_id        STRING      NOT NULL,
    equipment_type      STRING      NOT NULL,
    manufacturer        STRING      NOT NULL,
    capacity_tonnes     DECIMAL(8,2) NOT NULL,
    operating_hours     INT         NOT NULL,
    maintenance_status  STRING      NOT NULL,
    fuel_rate_l_per_hr  DECIMAL(6,2) NOT NULL,
    site_id             STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/bronze/raw_equipment';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_equipment TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_shifts (
    shift_id            STRING      NOT NULL,
    shift_type          STRING      NOT NULL,
    start_hour          INT         NOT NULL,
    end_hour            INT         NOT NULL,
    crew_size           INT         NOT NULL,
    supervisor          STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/bronze/raw_shifts';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_shifts TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_extractions (
    extraction_id       STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    pit_id              STRING      NOT NULL,
    equipment_id        STRING      NOT NULL,
    shift_id            STRING      NOT NULL,
    extraction_date     DATE        NOT NULL,
    ore_tonnes          DECIMAL(10,3) NOT NULL,
    waste_tonnes        DECIMAL(10,3) NOT NULL,
    grade_pct           DECIMAL(6,3) NOT NULL,
    recovery_pct        DECIMAL(6,2) NOT NULL,
    haul_distance_km    DECIMAL(6,2) NOT NULL,
    fuel_litres         DECIMAL(8,2) NOT NULL,
    cycle_time_min      DECIMAL(6,2) NOT NULL,
    equipment_hours     DECIMAL(6,2) NOT NULL,
    safety_incidents    INT         NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/bronze/raw_extractions'
PARTITIONED BY (site_id STRING, shift_id STRING);

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_extractions TO USER {{current_user}};

-- =============================================================================
-- SILVER TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.extractions_enriched (
    extraction_id           STRING      NOT NULL,
    site_id                 STRING      NOT NULL,
    pit_id                  STRING      NOT NULL,
    equipment_id            STRING      NOT NULL,
    shift_id                STRING      NOT NULL,
    extraction_date         DATE        NOT NULL,
    ore_tonnes              DECIMAL(10,3),
    waste_tonnes            DECIMAL(10,3),
    strip_ratio             DECIMAL(8,3),
    grade_pct               DECIMAL(6,3),
    recovery_pct            DECIMAL(6,2),
    haul_distance_km        DECIMAL(6,2),
    fuel_litres             DECIMAL(8,2),
    cycle_time_min          DECIMAL(6,2),
    equipment_hours         DECIMAL(6,2),
    equipment_utilization_pct DECIMAL(5,2),
    cost_per_tonne          DECIMAL(10,2),
    safety_incidents        INT,
    enriched_at             TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/mining/silver/extractions_enriched'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.extractions_enriched TO USER {{current_user}};

-- =============================================================================
-- GOLD TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_site (
    site_key                STRING      NOT NULL,
    site_id                 STRING      NOT NULL,
    site_name               STRING      NOT NULL,
    mineral_type            STRING      NOT NULL,
    country                 STRING      NOT NULL,
    region                  STRING      NOT NULL,
    reserve_estimate_mt     DECIMAL(12,3),
    mine_type               STRING,
    environmental_rating    STRING
) LOCATION '{{data_path}}/mining/gold/dim_site';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_site TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_pit (
    pit_key             STRING      NOT NULL,
    pit_id              STRING      NOT NULL,
    pit_name            STRING      NOT NULL,
    site_id             STRING      NOT NULL,
    current_depth_m     DECIMAL(8,2),
    target_depth_m      DECIMAL(8,2),
    bench_height_m      DECIMAL(6,2),
    status              STRING
) LOCATION '{{data_path}}/mining/gold/dim_pit';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_pit TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_equipment (
    equipment_key       STRING      NOT NULL,
    equipment_id        STRING      NOT NULL,
    equipment_type      STRING      NOT NULL,
    manufacturer        STRING      NOT NULL,
    capacity_tonnes     DECIMAL(8,2),
    operating_hours     INT,
    maintenance_status  STRING,
    fuel_rate_l_per_hr  DECIMAL(6,2)
) LOCATION '{{data_path}}/mining/gold/dim_equipment';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_equipment TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_shift (
    shift_key           STRING      NOT NULL,
    shift_type          STRING      NOT NULL,
    start_hour          INT,
    end_hour            INT,
    crew_size           INT,
    supervisor          STRING
) LOCATION '{{data_path}}/mining/gold/dim_shift';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_shift TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_extraction (
    extraction_key          STRING      NOT NULL,
    site_key                STRING      NOT NULL,
    pit_key                 STRING      NOT NULL,
    equipment_key           STRING      NOT NULL,
    shift_key               STRING      NOT NULL,
    extraction_date         DATE        NOT NULL,
    ore_tonnes              DECIMAL(10,3),
    waste_tonnes            DECIMAL(10,3),
    strip_ratio             DECIMAL(8,3),
    grade_pct               DECIMAL(6,3),
    recovery_pct            DECIMAL(6,2),
    haul_distance_km        DECIMAL(6,2),
    fuel_litres             DECIMAL(8,2),
    cycle_time_min          DECIMAL(6,2),
    equipment_utilization_pct DECIMAL(5,2)
) LOCATION '{{data_path}}/mining/gold/fact_extraction';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_extraction TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_production (
    site_id                     STRING      NOT NULL,
    pit_id                      STRING      NOT NULL,
    month                       STRING      NOT NULL,
    total_ore_tonnes            DECIMAL(12,3),
    total_waste_tonnes          DECIMAL(12,3),
    avg_strip_ratio             DECIMAL(8,3),
    avg_grade                   DECIMAL(6,3),
    avg_recovery                DECIMAL(6,2),
    cumulative_ore_ytd          DECIMAL(14,3),
    reserve_depletion_pct       DECIMAL(8,4),
    cost_per_tonne              DECIMAL(10,2),
    equipment_utilization_avg   DECIMAL(5,2),
    safety_incidents            INT
) LOCATION '{{data_path}}/mining/gold/kpi_production';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_production TO USER {{current_user}};

-- =============================================================================
-- SEED DATA: raw_sites (3 mine sites)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_sites VALUES
('SITE-001', 'Mount Copper Ridge',   'Copper',   'Australia', 'Western Australia',  850.000, 'Open Pit',    'A',  '2024-01-01T00:00:00'),
('SITE-002', 'Iron Valley Complex',  'Iron Ore', 'Brazil',    'Para',              2200.000, 'Open Pit',    'B',  '2024-01-01T00:00:00'),
('SITE-003', 'Golden Creek Mine',    'Gold',     'Canada',    'Ontario',            125.000, 'Underground', 'A+', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sites;


-- =============================================================================
-- SEED DATA: raw_pits (6 pits across 3 sites)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_pits VALUES
('PIT-001', 'Main Pit Alpha',  'SITE-001', 185.00, 350.00, 12.00, 'Active',     '2024-01-01T00:00:00'),
('PIT-002', 'Extension Beta',  'SITE-001',  95.00, 280.00, 10.00, 'Active',     '2024-01-01T00:00:00'),
('PIT-003', 'North Pit',       'SITE-002', 220.00, 400.00, 15.00, 'Active',     '2024-01-01T00:00:00'),
('PIT-004', 'South Pit',       'SITE-002', 160.00, 380.00, 15.00, 'Active',     '2024-01-01T00:00:00'),
('PIT-005', 'Shaft Level 1',   'SITE-003', 450.00, 800.00,  5.00, 'Active',     '2024-01-01T00:00:00'),
('PIT-006', 'Shaft Level 2',   'SITE-003', 280.00, 600.00,  5.00, 'Developing', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_pits;


-- =============================================================================
-- SEED DATA: raw_equipment (8 pieces)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_equipment VALUES
('EQ-001', 'Excavator',   'Caterpillar', 150.00, 12500, 'Operational', 85.00, 'SITE-001', '2024-01-01T00:00:00'),
('EQ-002', 'Haul Truck',  'Komatsu',     220.00, 18000, 'Operational', 120.00,'SITE-001', '2024-01-01T00:00:00'),
('EQ-003', 'Dozer',       'Caterpillar',  80.00, 9500,  'Operational', 65.00, 'SITE-001', '2024-01-01T00:00:00'),
('EQ-004', 'Excavator',   'Hitachi',     180.00, 15000, 'Operational', 95.00, 'SITE-002', '2024-01-01T00:00:00'),
('EQ-005', 'Haul Truck',  'Caterpillar', 250.00, 22000, 'Maintenance', 135.00,'SITE-002', '2024-01-01T00:00:00'),
('EQ-006', 'Drill Rig',   'Sandvik',      50.00, 8000,  'Operational', 45.00, 'SITE-002', '2024-01-01T00:00:00'),
('EQ-007', 'Excavator',   'Liebherr',    120.00, 6000,  'Operational', 75.00, 'SITE-003', '2024-01-01T00:00:00'),
('EQ-008', 'Haul Truck',  'Komatsu',     180.00, 11000, 'Operational', 100.00,'SITE-003', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_equipment;


-- =============================================================================
-- SEED DATA: raw_shifts (3 shifts per day)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_shifts VALUES
('SH-DAY',   'Day',   6,  14, 25, 'Mike Henderson',  '2024-01-01T00:00:00'),
('SH-SWING', 'Swing', 14, 22, 20, 'Sarah Kowalski',  '2024-01-01T00:00:00'),
('SH-NIGHT', 'Night', 22,  6, 18, 'James Blackwood',  '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_shifts;


-- =============================================================================
-- SEED DATA: raw_extractions (70 records - 3 sites, 6 pits, 8 equipment, 3 shifts, 2 months)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_extractions VALUES
-- SITE-001: Copper - PIT-001 - January 2024
('EX-001', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-01-03', 850.000,  2100.000, 1.250, 88.50, 4.20, 680.00, 32.50, 7.50, 0, '2024-01-04T00:00:00'),
('EX-002', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-DAY',   '2024-01-03', 920.000,  2350.000, 1.180, 89.20, 4.50, 960.00, 28.00, 8.00, 0, '2024-01-04T00:00:00'),
('EX-003', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-SWING', '2024-01-03', 780.000,  1980.000, 1.300, 87.80, 4.20, 640.00, 34.00, 7.00, 0, '2024-01-04T00:00:00'),
('EX-004', 'SITE-001', 'PIT-001', 'EQ-003', 'SH-NIGHT', '2024-01-03', 420.000,  1200.000, 1.450, 85.00, 3.80, 390.00, 38.00, 6.00, 0, '2024-01-04T00:00:00'),
('EX-005', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-01-10', 880.000,  2200.000, 1.220, 89.00, 4.30, 700.00, 31.50, 7.80, 0, '2024-01-11T00:00:00'),
('EX-006', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-SWING', '2024-01-10', 900.000,  2280.000, 1.200, 88.70, 4.40, 920.00, 29.00, 7.50, 1, '2024-01-11T00:00:00'),
('EX-007', 'SITE-001', 'PIT-001', 'EQ-003', 'SH-NIGHT', '2024-01-10', 380.000,  1100.000, 1.500, 84.50, 3.90, 370.00, 40.00, 5.50, 0, '2024-01-11T00:00:00'),
('EX-008', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-01-17', 910.000,  2250.000, 1.190, 89.50, 4.25, 710.00, 30.50, 8.00, 0, '2024-01-18T00:00:00'),
('EX-009', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-DAY',   '2024-01-24', 860.000,  2150.000, 1.240, 88.00, 4.35, 890.00, 31.00, 7.20, 0, '2024-01-25T00:00:00'),
-- SITE-001: Copper - PIT-002 - January 2024
('EX-010', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-DAY',   '2024-01-05', 520.000,  1800.000, 1.650, 82.50, 5.10, 520.00, 42.00, 6.50, 0, '2024-01-06T00:00:00'),
('EX-011', 'SITE-001', 'PIT-002', 'EQ-001', 'SH-SWING', '2024-01-05', 600.000,  2000.000, 1.580, 84.00, 5.00, 580.00, 36.00, 7.00, 0, '2024-01-06T00:00:00'),
('EX-012', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-DAY',   '2024-01-12', 480.000,  1750.000, 1.720, 81.00, 5.20, 500.00, 44.00, 6.00, 1, '2024-01-13T00:00:00'),
('EX-013', 'SITE-001', 'PIT-002', 'EQ-001', 'SH-DAY',   '2024-01-19', 550.000,  1900.000, 1.600, 83.50, 5.05, 560.00, 38.00, 7.00, 0, '2024-01-20T00:00:00'),
-- SITE-001: February 2024
('EX-014', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-02-01', 920.000,  2300.000, 1.180, 90.00, 4.20, 720.00, 30.00, 8.00, 0, '2024-02-02T00:00:00'),
('EX-015', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-SWING', '2024-02-01', 890.000,  2200.000, 1.210, 89.50, 4.30, 900.00, 29.50, 7.50, 0, '2024-02-02T00:00:00'),
('EX-016', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-02-08', 940.000,  2350.000, 1.170, 90.20, 4.15, 730.00, 29.00, 8.00, 0, '2024-02-09T00:00:00'),
('EX-017', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-DAY',   '2024-02-08', 500.000,  1800.000, 1.680, 82.00, 5.10, 510.00, 43.00, 6.20, 0, '2024-02-09T00:00:00'),
('EX-018', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-DAY',   '2024-02-15', 930.000,  2280.000, 1.190, 89.80, 4.25, 910.00, 28.50, 8.00, 0, '2024-02-16T00:00:00'),
('EX-019', 'SITE-001', 'PIT-002', 'EQ-001', 'SH-SWING', '2024-02-15', 570.000,  1950.000, 1.620, 83.80, 5.00, 570.00, 37.00, 7.20, 0, '2024-02-16T00:00:00'),
-- SITE-002: Iron Ore - PIT-003 - January 2024
('EX-020', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-01-02', 1800.000, 3200.000, 0.850, 65.20, 6.50, 1140.00, 25.00, 8.00, 0, '2024-01-03T00:00:00'),
('EX-021', 'SITE-002', 'PIT-003', 'EQ-005', 'SH-DAY',   '2024-01-02', 2100.000, 3800.000, 0.820, 66.00, 7.00, 1620.00, 22.00, 3.50, 0, '2024-01-03T00:00:00'),
('EX-022', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-SWING', '2024-01-02', 1650.000, 3000.000, 0.880, 64.80, 6.30, 1050.00, 26.50, 7.50, 0, '2024-01-03T00:00:00'),
('EX-023', 'SITE-002', 'PIT-003', 'EQ-006', 'SH-NIGHT', '2024-01-02', 800.000,  1500.000, 0.950, 62.00, 5.80, 360.00,  35.00, 6.00, 0, '2024-01-03T00:00:00'),
('EX-024', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-01-09', 1850.000, 3300.000, 0.840, 65.50, 6.40, 1160.00, 24.50, 8.00, 0, '2024-01-10T00:00:00'),
('EX-025', 'SITE-002', 'PIT-003', 'EQ-005', 'SH-SWING', '2024-01-09', 1950.000, 3500.000, 0.830, 65.80, 6.80, 1500.00, 23.00, 3.00, 0, '2024-01-10T00:00:00'),
('EX-026', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-01-16', 1780.000, 3150.000, 0.860, 64.90, 6.50, 1120.00, 25.50, 7.80, 1, '2024-01-17T00:00:00'),
('EX-027', 'SITE-002', 'PIT-003', 'EQ-006', 'SH-DAY',   '2024-01-23', 820.000,  1550.000, 0.940, 63.00, 5.90, 375.00,  34.00, 6.20, 0, '2024-01-24T00:00:00'),
-- SITE-002: Iron Ore - PIT-004 - January 2024
('EX-028', 'SITE-002', 'PIT-004', 'EQ-004', 'SH-DAY',   '2024-01-04', 1500.000, 2800.000, 0.900, 63.50, 7.20, 1080.00, 27.00, 7.50, 0, '2024-01-05T00:00:00'),
('EX-029', 'SITE-002', 'PIT-004', 'EQ-005', 'SH-SWING', '2024-01-04', 1700.000, 3100.000, 0.870, 64.20, 7.50, 1400.00, 24.00, 3.20, 0, '2024-01-05T00:00:00'),
('EX-030', 'SITE-002', 'PIT-004', 'EQ-006', 'SH-NIGHT', '2024-01-04', 700.000,  1400.000, 0.980, 61.50, 6.00, 330.00,  36.00, 5.80, 0, '2024-01-05T00:00:00'),
('EX-031', 'SITE-002', 'PIT-004', 'EQ-004', 'SH-DAY',   '2024-01-11', 1550.000, 2900.000, 0.890, 64.00, 7.10, 1100.00, 26.50, 7.80, 0, '2024-01-12T00:00:00'),
('EX-032', 'SITE-002', 'PIT-004', 'EQ-005', 'SH-DAY',   '2024-01-18', 1600.000, 3000.000, 0.880, 64.50, 7.30, 1350.00, 25.00, 2.80, 0, '2024-01-19T00:00:00'),
-- SITE-002: February 2024
('EX-033', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-02-02', 1900.000, 3400.000, 0.830, 66.20, 6.40, 1180.00, 24.00, 8.00, 0, '2024-02-03T00:00:00'),
('EX-034', 'SITE-002', 'PIT-003', 'EQ-005', 'SH-SWING', '2024-02-02', 2050.000, 3700.000, 0.810, 66.50, 6.90, 1580.00, 22.50, 3.50, 0, '2024-02-03T00:00:00'),
('EX-035', 'SITE-002', 'PIT-003', 'EQ-006', 'SH-NIGHT', '2024-02-02', 850.000,  1600.000, 0.920, 63.50, 5.70, 380.00,  33.00, 6.50, 0, '2024-02-03T00:00:00'),
('EX-036', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-02-09', 1880.000, 3350.000, 0.840, 65.80, 6.45, 1170.00, 24.50, 7.80, 0, '2024-02-10T00:00:00'),
('EX-037', 'SITE-002', 'PIT-004', 'EQ-005', 'SH-DAY',   '2024-02-09', 1680.000, 3050.000, 0.870, 64.80, 7.20, 1380.00, 24.00, 3.00, 1, '2024-02-10T00:00:00'),
('EX-038', 'SITE-002', 'PIT-004', 'EQ-004', 'SH-SWING', '2024-02-16', 1520.000, 2850.000, 0.900, 63.80, 7.10, 1090.00, 27.50, 7.50, 0, '2024-02-17T00:00:00'),
('EX-039', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-02-16', 1920.000, 3420.000, 0.835, 66.00, 6.50, 1190.00, 24.00, 8.00, 0, '2024-02-17T00:00:00'),
-- SITE-003: Gold - PIT-005 (Underground) - January 2024
('EX-040', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-01-03', 120.000,   80.000, 8.500, 92.00, 2.50, 225.00, 55.00, 7.00, 0, '2024-01-04T00:00:00'),
('EX-041', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-DAY',   '2024-01-03', 150.000,   95.000, 8.200, 91.50, 3.00, 300.00, 48.00, 7.50, 0, '2024-01-04T00:00:00'),
('EX-042', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-SWING', '2024-01-03', 100.000,   70.000, 8.800, 93.00, 2.30, 200.00, 58.00, 6.50, 0, '2024-01-04T00:00:00'),
('EX-043', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-01-10', 130.000,   85.000, 8.400, 91.80, 2.40, 230.00, 54.00, 7.20, 0, '2024-01-11T00:00:00'),
('EX-044', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-SWING', '2024-01-10', 140.000,   90.000, 8.300, 92.20, 2.80, 280.00, 50.00, 7.00, 0, '2024-01-11T00:00:00'),
('EX-045', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-01-17', 135.000,   82.000, 8.600, 92.50, 2.50, 235.00, 53.00, 7.50, 0, '2024-01-18T00:00:00'),
('EX-046', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-DAY',   '2024-01-24', 145.000,   88.000, 8.350, 91.00, 2.90, 290.00, 49.00, 7.80, 1, '2024-01-25T00:00:00'),
-- SITE-003: Gold - PIT-006 (Developing) - January 2024
('EX-047', 'SITE-003', 'PIT-006', 'EQ-007', 'SH-DAY',   '2024-01-08', 60.000,   150.000, 9.200, 88.00, 3.50, 180.00, 65.00, 5.50, 0, '2024-01-09T00:00:00'),
('EX-048', 'SITE-003', 'PIT-006', 'EQ-008', 'SH-SWING', '2024-01-08', 50.000,   140.000, 9.500, 86.50, 3.80, 200.00, 70.00, 5.00, 0, '2024-01-09T00:00:00'),
('EX-049', 'SITE-003', 'PIT-006', 'EQ-007', 'SH-DAY',   '2024-01-15', 65.000,   160.000, 9.100, 88.50, 3.40, 185.00, 64.00, 5.80, 0, '2024-01-16T00:00:00'),
('EX-050', 'SITE-003', 'PIT-006', 'EQ-008', 'SH-DAY',   '2024-01-22', 55.000,   145.000, 9.300, 87.00, 3.60, 195.00, 68.00, 5.20, 0, '2024-01-23T00:00:00'),
-- SITE-003: February 2024
('EX-051', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-02-01', 140.000,   85.000, 8.500, 92.50, 2.45, 230.00, 52.00, 7.50, 0, '2024-02-02T00:00:00'),
('EX-052', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-SWING', '2024-02-01', 155.000,   92.000, 8.100, 91.00, 2.85, 295.00, 47.00, 8.00, 0, '2024-02-02T00:00:00'),
('EX-053', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-02-08', 138.000,   83.000, 8.550, 93.00, 2.50, 228.00, 53.00, 7.20, 0, '2024-02-09T00:00:00'),
('EX-054', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-DAY',   '2024-02-15', 148.000,   90.000, 8.250, 91.50, 2.80, 285.00, 49.00, 7.80, 0, '2024-02-16T00:00:00'),
('EX-055', 'SITE-003', 'PIT-006', 'EQ-007', 'SH-DAY',   '2024-02-05', 70.000,   155.000, 9.000, 89.00, 3.30, 182.00, 62.00, 6.00, 0, '2024-02-06T00:00:00'),
('EX-056', 'SITE-003', 'PIT-006', 'EQ-008', 'SH-SWING', '2024-02-05', 58.000,   142.000, 9.400, 87.50, 3.70, 198.00, 67.00, 5.20, 0, '2024-02-06T00:00:00'),
('EX-057', 'SITE-003', 'PIT-006', 'EQ-007', 'SH-DAY',   '2024-02-12', 72.000,   158.000, 8.950, 89.50, 3.25, 180.00, 61.00, 6.20, 0, '2024-02-13T00:00:00'),
-- Additional records for weight
('EX-058', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-01-31', 895.000,  2220.000, 1.200, 89.20, 4.25, 705.00, 31.00, 7.80, 0, '2024-02-01T00:00:00'),
('EX-059', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-SWING', '2024-01-26', 510.000,  1820.000, 1.660, 82.80, 5.15, 525.00, 41.00, 6.50, 0, '2024-01-27T00:00:00'),
('EX-060', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-01-30', 1870.000, 3280.000, 0.845, 65.70, 6.45, 1155.00, 24.50, 8.00, 0, '2024-01-31T00:00:00'),
('EX-061', 'SITE-002', 'PIT-004', 'EQ-006', 'SH-NIGHT', '2024-01-25', 720.000,  1450.000, 0.970, 62.00, 6.10, 340.00,  35.50, 6.00, 0, '2024-01-26T00:00:00'),
('EX-062', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-NIGHT', '2024-01-17', 95.000,    65.000, 8.900, 93.50, 2.20, 190.00, 60.00, 6.00, 0, '2024-01-18T00:00:00'),
('EX-063', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-NIGHT', '2024-02-08', 750.000,  1900.000, 1.280, 87.50, 4.40, 820.00, 33.00, 6.80, 0, '2024-02-09T00:00:00'),
('EX-064', 'SITE-002', 'PIT-004', 'EQ-004', 'SH-DAY',   '2024-02-23', 1560.000, 2880.000, 0.895, 64.20, 7.15, 1095.00, 26.50, 7.80, 0, '2024-02-24T00:00:00'),
('EX-065', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-NIGHT', '2024-02-08', 110.000,   75.000, 8.650, 92.80, 2.60, 260.00, 56.00, 6.50, 0, '2024-02-09T00:00:00'),
('EX-066', 'SITE-001', 'PIT-001', 'EQ-003', 'SH-DAY',   '2024-02-22', 450.000,  1250.000, 1.420, 86.00, 3.90, 400.00, 37.00, 6.00, 0, '2024-02-23T00:00:00'),
('EX-067', 'SITE-002', 'PIT-003', 'EQ-006', 'SH-DAY',   '2024-02-23', 870.000,  1620.000, 0.930, 63.80, 5.85, 390.00, 32.50, 6.50, 0, '2024-02-24T00:00:00'),
('EX-068', 'SITE-003', 'PIT-006', 'EQ-008', 'SH-DAY',   '2024-02-19', 62.000,   148.000, 9.300, 88.00, 3.55, 192.00, 66.00, 5.50, 0, '2024-02-20T00:00:00'),
-- Equipment breakdown examples (low utilization)
('EX-069', 'SITE-002', 'PIT-003', 'EQ-005', 'SH-DAY',   '2024-02-16', 800.000,  1500.000, 0.920, 63.00, 6.00, 810.00,  40.00, 1.50, 0, '2024-02-17T00:00:00'),
('EX-070', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-DAY',   '2024-02-22', 350.000,  1100.000, 1.800, 80.00, 5.30, 380.00, 48.00, 3.00, 1, '2024-02-23T00:00:00');

ASSERT ROW_COUNT = 70
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_extractions;

