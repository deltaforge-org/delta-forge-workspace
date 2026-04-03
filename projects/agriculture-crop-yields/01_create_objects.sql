-- =============================================================================
-- Agriculture Crop Yields Pipeline: Create Objects & Seed Data
-- =============================================================================

-- =============================================================================
-- ZONES
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw crop, field, and weather data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched harvest data with yield calculations';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Star schema for crop yield analytics';

-- =============================================================================
-- BRONZE TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_fields (
    field_id        STRING      NOT NULL,
    field_name      STRING      NOT NULL,
    farm_name       STRING      NOT NULL,
    region          STRING      NOT NULL,
    soil_type       STRING      NOT NULL,
    irrigation_type STRING      NOT NULL,
    total_hectares  DECIMAL(8,2) NOT NULL,
    gps_lat         DECIMAL(9,6),
    gps_lon         DECIMAL(9,6),
    ingested_at     TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/agriculture/bronze/raw_fields';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_fields TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_weather (
    station_id      STRING      NOT NULL,
    region          STRING      NOT NULL,
    season          STRING      NOT NULL,
    year            INT         NOT NULL,
    avg_temp_c      DECIMAL(5,2),
    total_rainfall_mm DECIMAL(8,2),
    sunshine_hours  DECIMAL(8,2),
    drought_index   DECIMAL(5,3),
    ingested_at     TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/agriculture/bronze/raw_weather';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_weather TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_harvests (
    harvest_id      STRING      NOT NULL,
    field_id        STRING      NOT NULL,
    crop_type       STRING      NOT NULL,
    variety         STRING      NOT NULL,
    season          STRING      NOT NULL,
    year            INT         NOT NULL,
    station_id      STRING      NOT NULL,
    harvest_date    DATE        NOT NULL,
    area_hectares   DECIMAL(8,2) NOT NULL CHECK (area_hectares > 0),
    yield_tonnes    DECIMAL(10,3) NOT NULL CHECK (yield_tonnes >= 0),
    moisture_pct    DECIMAL(5,2) CHECK (moisture_pct >= 0 AND moisture_pct <= 100),
    quality_grade   STRING      NOT NULL,
    input_cost      DECIMAL(12,2) NOT NULL,
    revenue         DECIMAL(12,2) NOT NULL,
    ingested_at     TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/agriculture/bronze/raw_harvests'
PARTITIONED BY (region STRING, crop_type STRING);

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_harvests TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sensors (
    sensor_id       STRING      NOT NULL,
    field_id        STRING      NOT NULL,
    reading_date    DATE        NOT NULL,
    soil_moisture   DECIMAL(5,2) CHECK (soil_moisture >= 0 AND soil_moisture <= 100),
    soil_ph         DECIMAL(4,2),
    nitrogen_ppm    DECIMAL(8,2),
    phosphorus_ppm  DECIMAL(8,2),
    potassium_ppm   DECIMAL(8,2),
    ingested_at     TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/agriculture/bronze/raw_sensors';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sensors TO USER {{current_user}};

-- =============================================================================
-- SILVER TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.harvests_enriched (
    harvest_id          STRING      NOT NULL,
    field_id            STRING      NOT NULL,
    crop_type           STRING      NOT NULL,
    variety             STRING      NOT NULL,
    season              STRING      NOT NULL,
    year                INT         NOT NULL,
    station_id          STRING      NOT NULL,
    region              STRING      NOT NULL,
    harvest_date        DATE        NOT NULL,
    area_hectares       DECIMAL(8,2),
    yield_tonnes        DECIMAL(10,3),
    yield_per_hectare   DECIMAL(10,3),
    expected_yield_per_hectare DECIMAL(10,3),
    yield_vs_expected_pct DECIMAL(8,2),
    moisture_pct        DECIMAL(5,2),
    quality_grade       STRING,
    input_cost          DECIMAL(12,2),
    revenue             DECIMAL(12,2),
    profit              DECIMAL(12,2),
    profit_per_hectare  DECIMAL(12,2),
    is_underperformer   BOOLEAN,
    avg_soil_moisture   DECIMAL(5,2),
    avg_soil_ph         DECIMAL(4,2),
    enriched_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/agriculture/silver/harvests_enriched'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.harvests_enriched TO USER {{current_user}};

-- =============================================================================
-- GOLD TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_field (
    field_key       STRING      NOT NULL,
    field_id        STRING      NOT NULL,
    field_name      STRING      NOT NULL,
    farm_name       STRING      NOT NULL,
    region          STRING      NOT NULL,
    soil_type       STRING      NOT NULL,
    irrigation_type STRING      NOT NULL,
    total_hectares  DECIMAL(8,2),
    gps_lat         DECIMAL(9,6),
    gps_lon         DECIMAL(9,6)
) LOCATION '{{data_path}}/agriculture/gold/dim_field';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_field TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_crop (
    crop_key                    STRING      NOT NULL,
    crop_type                   STRING      NOT NULL,
    variety                     STRING      NOT NULL,
    growth_days                 INT,
    seed_cost_per_hectare       DECIMAL(10,2),
    expected_yield_per_hectare  DECIMAL(10,3)
) LOCATION '{{data_path}}/agriculture/gold/dim_crop';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_crop TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_season (
    season_key      STRING      NOT NULL,
    season_name     STRING      NOT NULL,
    year            INT         NOT NULL,
    planting_start  DATE,
    harvest_start   DATE,
    frost_risk_level STRING
) LOCATION '{{data_path}}/agriculture/gold/dim_season';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_season TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_weather (
    weather_key       STRING      NOT NULL,
    station_id        STRING      NOT NULL,
    avg_temp_c        DECIMAL(5,2),
    total_rainfall_mm DECIMAL(8,2),
    sunshine_hours    DECIMAL(8,2),
    drought_index     DECIMAL(5,3)
) LOCATION '{{data_path}}/agriculture/gold/dim_weather';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_weather TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_harvest (
    harvest_key         STRING      NOT NULL,
    field_key           STRING      NOT NULL,
    crop_key            STRING      NOT NULL,
    season_key          STRING      NOT NULL,
    weather_key         STRING      NOT NULL,
    harvest_date        DATE        NOT NULL,
    area_hectares       DECIMAL(8,2),
    yield_tonnes        DECIMAL(10,3),
    moisture_pct        DECIMAL(5,2),
    quality_grade       STRING,
    input_cost          DECIMAL(12,2),
    revenue             DECIMAL(12,2),
    profit_per_hectare  DECIMAL(12,2)
) LOCATION '{{data_path}}/agriculture/gold/fact_harvest';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_harvest TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_yield_analysis (
    region                  STRING      NOT NULL,
    crop_type               STRING      NOT NULL,
    season                  STRING      NOT NULL,
    avg_yield_per_hectare   DECIMAL(10,3),
    yield_vs_expected_pct   DECIMAL(8,2),
    total_revenue           DECIMAL(14,2),
    total_cost              DECIMAL(14,2),
    avg_profit_per_hectare  DECIMAL(12,2),
    best_field              STRING,
    worst_field             STRING,
    weather_correlation     STRING
) LOCATION '{{data_path}}/agriculture/gold/kpi_yield_analysis';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_yield_analysis TO USER {{current_user}};

-- =============================================================================
-- SEED DATA: raw_fields (8 fields across 3 regions)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_fields VALUES
('F001', 'North Prairie',      'Greenfield Farms',  'Midwest',    'Loam',       'Center Pivot',  120.50, 41.878114, -93.097702, '2024-01-01T00:00:00'),
('F002', 'Riverside Plot',     'Greenfield Farms',  'Midwest',    'Clay Loam',  'Drip',           85.00, 41.661129, -91.530167, '2024-01-01T00:00:00'),
('F003', 'Hilltop East',       'Sunrise Agriculture','Southeast', 'Sandy Loam', 'Sprinkler',      95.75, 33.748995, -84.387982, '2024-01-01T00:00:00'),
('F004', 'Bottomland South',   'Sunrise Agriculture','Southeast', 'Silt Loam',  'Flood',         150.00, 32.776475, -79.931051, '2024-01-01T00:00:00'),
('F005', 'Dryland West',       'Pacific Growers',   'West Coast', 'Sandy',      'None',           60.25, 36.778261, -119.417932,'2024-01-01T00:00:00'),
('F006', 'Irrigated Valley',   'Pacific Growers',   'West Coast', 'Loam',       'Drip',          110.00, 37.774929, -122.419416,'2024-01-01T00:00:00'),
('F007', 'Central Block',      'Greenfield Farms',  'Midwest',    'Silt Loam',  'Center Pivot',  200.00, 40.417287, -86.878243, '2024-01-01T00:00:00'),
('F008', 'Coastal Strip',      'Sunrise Agriculture','Southeast', 'Clay',       'Sprinkler',      75.50, 30.267153, -97.743061, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_fields;


-- =============================================================================
-- SEED DATA: raw_weather (9 station-season records across 3 seasons)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_weather VALUES
('WS-MW-01', 'Midwest',    'Spring', 2023, 18.50, 320.00, 1450.00, 0.120, '2024-01-01T00:00:00'),
('WS-MW-01', 'Midwest',    'Summer', 2023, 28.30, 180.00, 1820.00, 0.350, '2024-01-01T00:00:00'),
('WS-MW-01', 'Midwest',    'Spring', 2024, 17.80, 380.00, 1380.00, 0.080, '2024-01-01T00:00:00'),
('WS-SE-01', 'Southeast',  'Spring', 2023, 22.10, 410.00, 1600.00, 0.050, '2024-01-01T00:00:00'),
('WS-SE-01', 'Southeast',  'Summer', 2023, 31.50, 250.00, 1950.00, 0.280, '2024-01-01T00:00:00'),
('WS-SE-01', 'Southeast',  'Spring', 2024, 21.60, 450.00, 1550.00, 0.040, '2024-01-01T00:00:00'),
('WS-WC-01', 'West Coast', 'Spring', 2023, 16.20,  95.00, 1900.00, 0.620, '2024-01-01T00:00:00'),
('WS-WC-01', 'West Coast', 'Summer', 2023, 24.80,  40.00, 2100.00, 0.780, '2024-01-01T00:00:00'),
('WS-WC-01', 'West Coast', 'Spring', 2024, 15.90, 130.00, 1850.00, 0.540, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 9
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_weather;


-- =============================================================================
-- SEED DATA: raw_harvests (65 rows across 8 fields, 6 crop types, 3 seasons)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_harvests VALUES
-- Midwest - Spring 2023 - Corn
('H001', 'F001', 'Corn',     'Pioneer P1197',    'Spring', 2023, 'WS-MW-01', '2023-09-15', 80.00,  760.000, 14.20, 'A', 48000.00,  91200.00, '2024-01-01T00:00:00'),
('H002', 'F002', 'Corn',     'Pioneer P1197',    'Spring', 2023, 'WS-MW-01', '2023-09-18', 50.00,  425.000, 15.10, 'B', 30000.00,  51000.00, '2024-01-01T00:00:00'),
('H003', 'F007', 'Corn',     'DeKalb DKC64',     'Spring', 2023, 'WS-MW-01', '2023-09-20',150.00, 1485.000, 13.80, 'A', 90000.00, 178200.00, '2024-01-01T00:00:00'),
-- Midwest - Spring 2023 - Soybeans
('H004', 'F001', 'Soybeans', 'Asgrow AG36X6',    'Spring', 2023, 'WS-MW-01', '2023-10-05', 40.00,  120.000, 12.50, 'A', 16000.00,  43200.00, '2024-01-01T00:00:00'),
('H005', 'F002', 'Soybeans', 'Asgrow AG36X6',    'Spring', 2023, 'WS-MW-01', '2023-10-08', 35.00,   87.500, 13.20, 'B', 14000.00,  31500.00, '2024-01-01T00:00:00'),
-- Midwest - Summer 2023 - Wheat (poor drought season)
('H006', 'F001', 'Wheat',    'SY Wolverine',     'Summer', 2023, 'WS-MW-01', '2023-07-20', 80.00,  240.000, 11.80, 'C', 32000.00,  40800.00, '2024-01-01T00:00:00'),
('H007', 'F007', 'Wheat',    'SY Wolverine',     'Summer', 2023, 'WS-MW-01', '2023-07-22',120.00,  300.000, 12.40, 'D', 48000.00,  48000.00, '2024-01-01T00:00:00'),
('H008', 'F002', 'Wheat',    'WestBred WB4303',  'Summer', 2023, 'WS-MW-01', '2023-07-25', 40.00,  100.000, 12.90, 'C', 16000.00,  17000.00, '2024-01-01T00:00:00'),
-- Midwest - Spring 2024 - Corn (good season with higher rainfall)
('H009', 'F001', 'Corn',     'Pioneer P1197',    'Spring', 2024, 'WS-MW-01', '2024-09-12', 80.00,  824.000, 13.50, 'A', 50000.00, 102176.00, '2024-09-15T00:00:00'),
('H010', 'F002', 'Corn',     'DeKalb DKC64',     'Spring', 2024, 'WS-MW-01', '2024-09-14', 50.00,  520.000, 14.00, 'A', 31250.00,  64480.00, '2024-09-15T00:00:00'),
('H011', 'F007', 'Corn',     'DeKalb DKC64',     'Spring', 2024, 'WS-MW-01', '2024-09-16',150.00, 1590.000, 13.20, 'A', 93750.00, 197160.00, '2024-09-18T00:00:00'),
-- Midwest - Spring 2024 - Soybeans
('H012', 'F001', 'Soybeans', 'Asgrow AG36X6',    'Spring', 2024, 'WS-MW-01', '2024-10-03', 40.00,  132.000, 12.00, 'A', 16800.00,  47520.00, '2024-10-05T00:00:00'),
('H013', 'F002', 'Soybeans', 'Pioneer P22T41',   'Spring', 2024, 'WS-MW-01', '2024-10-05', 35.00,  101.500, 12.80, 'B', 14700.00,  36540.00, '2024-10-06T00:00:00'),
-- Southeast - Spring 2023 - Rice
('H014', 'F003', 'Rice',     'CL153',            'Spring', 2023, 'WS-SE-01', '2023-08-25', 60.00,  420.000, 18.50, 'A', 30000.00,  63000.00, '2024-01-01T00:00:00'),
('H015', 'F004', 'Rice',     'CL153',            'Spring', 2023, 'WS-SE-01', '2023-08-28',100.00,  720.000, 19.20, 'A', 50000.00, 108000.00, '2024-01-01T00:00:00'),
('H016', 'F008', 'Rice',     'Rex',              'Spring', 2023, 'WS-SE-01', '2023-09-01', 50.00,  325.000, 20.10, 'B', 25000.00,  48750.00, '2024-01-01T00:00:00'),
-- Southeast - Spring 2023 - Cotton
('H017', 'F003', 'Cotton',   'DP 1646 B2XF',     'Spring', 2023, 'WS-SE-01', '2023-10-15', 35.00,   52.500, 8.30,  'A', 21000.00,  44625.00, '2024-01-01T00:00:00'),
('H018', 'F004', 'Cotton',   'DP 1646 B2XF',     'Spring', 2023, 'WS-SE-01', '2023-10-18', 50.00,   65.000, 8.80,  'B', 30000.00,  55250.00, '2024-01-01T00:00:00'),
('H019', 'F008', 'Cotton',   'PHY 350 W3FE',     'Spring', 2023, 'WS-SE-01', '2023-10-20', 25.00,   25.000, 9.50,  'D', 15000.00,  16250.00, '2024-01-01T00:00:00'),
-- Southeast - Summer 2023 - Soybeans (drought stress)
('H020', 'F003', 'Soybeans', 'Credenz CZ 4105',  'Summer', 2023, 'WS-SE-01', '2023-11-05', 50.00,  100.000, 13.80, 'C', 20000.00,  36000.00, '2024-01-01T00:00:00'),
('H021', 'F004', 'Soybeans', 'Credenz CZ 4105',  'Summer', 2023, 'WS-SE-01', '2023-11-08', 80.00,  184.000, 14.20, 'B', 32000.00,  66240.00, '2024-01-01T00:00:00'),
('H022', 'F008', 'Soybeans', 'Asgrow AG46X6',    'Summer', 2023, 'WS-SE-01', '2023-11-10', 30.00,   48.000, 15.00, 'D', 12000.00,  17280.00, '2024-01-01T00:00:00'),
-- Southeast - Spring 2024 - Rice
('H023', 'F003', 'Rice',     'CL153',            'Spring', 2024, 'WS-SE-01', '2024-08-22', 60.00,  450.000, 17.80, 'A', 31200.00,  67500.00, '2024-08-25T00:00:00'),
('H024', 'F004', 'Rice',     'Rex',              'Spring', 2024, 'WS-SE-01', '2024-08-25',100.00,  750.000, 18.50, 'A', 52000.00, 112500.00, '2024-08-28T00:00:00'),
('H025', 'F008', 'Rice',     'Rex',              'Spring', 2024, 'WS-SE-01', '2024-08-28', 50.00,  340.000, 19.00, 'B', 26000.00,  51000.00, '2024-09-01T00:00:00'),
-- Southeast - Spring 2024 - Cotton
('H026', 'F003', 'Cotton',   'DP 1646 B2XF',     'Spring', 2024, 'WS-SE-01', '2024-10-12', 35.00,   56.000, 7.90,  'A', 22050.00,  47600.00, '2024-10-14T00:00:00'),
('H027', 'F004', 'Cotton',   'PHY 350 W3FE',     'Spring', 2024, 'WS-SE-01', '2024-10-15', 50.00,   72.500, 8.20,  'A', 31500.00,  61625.00, '2024-10-17T00:00:00'),
-- West Coast - Spring 2023 - Almonds (drought year)
('H028', 'F005', 'Almonds',  'Nonpareil',        'Spring', 2023, 'WS-WC-01', '2023-08-10', 40.00,   64.000, 5.50,  'B', 28000.00,  57600.00, '2024-01-01T00:00:00'),
('H029', 'F006', 'Almonds',  'Nonpareil',        'Spring', 2023, 'WS-WC-01', '2023-08-12', 80.00,  152.000, 5.20,  'A', 56000.00, 136800.00, '2024-01-01T00:00:00'),
-- West Coast - Spring 2023 - Grapes
('H030', 'F005', 'Grapes',   'Cabernet Sauv.',   'Spring', 2023, 'WS-WC-01', '2023-09-20', 20.00,  140.000, 22.50, 'A', 16000.00,  56000.00, '2024-01-01T00:00:00'),
('H031', 'F006', 'Grapes',   'Chardonnay',       'Spring', 2023, 'WS-WC-01', '2023-09-22', 30.00,  195.000, 23.10, 'A', 24000.00,  78000.00, '2024-01-01T00:00:00'),
-- West Coast - Summer 2023 - Almonds (severe drought)
('H032', 'F005', 'Almonds',  'Nonpareil',        'Summer', 2023, 'WS-WC-01', '2023-08-25', 40.00,   44.000, 6.10,  'D', 28000.00,  39600.00, '2024-01-01T00:00:00'),
('H033', 'F006', 'Almonds',  'Carmel',           'Summer', 2023, 'WS-WC-01', '2023-08-28', 80.00,  104.000, 5.80,  'C', 56000.00,  93600.00, '2024-01-01T00:00:00'),
-- West Coast - Summer 2023 - Grapes
('H034', 'F005', 'Grapes',   'Cabernet Sauv.',   'Summer', 2023, 'WS-WC-01', '2023-10-01', 20.00,  110.000, 24.00, 'B', 16000.00,  44000.00, '2024-01-01T00:00:00'),
('H035', 'F006', 'Grapes',   'Chardonnay',       'Summer', 2023, 'WS-WC-01', '2023-10-03', 30.00,  150.000, 23.80, 'B', 24000.00,  60000.00, '2024-01-01T00:00:00'),
-- West Coast - Spring 2024 - Almonds (better water year)
('H036', 'F005', 'Almonds',  'Nonpareil',        'Spring', 2024, 'WS-WC-01', '2024-08-08', 40.00,   76.000, 5.30,  'A', 29000.00,  68400.00, '2024-08-10T00:00:00'),
('H037', 'F006', 'Almonds',  'Nonpareil',        'Spring', 2024, 'WS-WC-01', '2024-08-11', 80.00,  168.000, 5.10,  'A', 58000.00, 151200.00, '2024-08-13T00:00:00'),
-- West Coast - Spring 2024 - Grapes
('H038', 'F005', 'Grapes',   'Cabernet Sauv.',   'Spring', 2024, 'WS-WC-01', '2024-09-18', 20.00,  148.000, 21.80, 'A', 16500.00,  59200.00, '2024-09-20T00:00:00'),
('H039', 'F006', 'Grapes',   'Chardonnay',       'Spring', 2024, 'WS-WC-01', '2024-09-20', 30.00,  207.000, 22.50, 'A', 25000.00,  82800.00, '2024-09-22T00:00:00'),
-- Additional Midwest records for depth
('H040', 'F007', 'Soybeans', 'Pioneer P22T41',   'Spring', 2023, 'WS-MW-01', '2023-10-10',100.00,  280.000, 13.00, 'B', 40000.00, 100800.00, '2024-01-01T00:00:00'),
('H041', 'F007', 'Wheat',    'WestBred WB4303',  'Spring', 2024, 'WS-MW-01', '2024-07-18',120.00,  456.000, 11.50, 'A', 50400.00,  77520.00, '2024-07-20T00:00:00'),
('H042', 'F001', 'Wheat',    'SY Wolverine',     'Spring', 2024, 'WS-MW-01', '2024-07-15', 60.00,  222.000, 11.90, 'B', 25200.00,  37740.00, '2024-07-18T00:00:00'),
-- Additional Southeast records
('H043', 'F004', 'Soybeans', 'Credenz CZ 4105',  'Spring', 2024, 'WS-SE-01', '2024-11-02', 80.00,  216.000, 13.50, 'A', 33600.00,  77760.00, '2024-11-04T00:00:00'),
('H044', 'F003', 'Soybeans', 'Asgrow AG46X6',    'Spring', 2024, 'WS-SE-01', '2024-11-05', 50.00,  127.500, 14.00, 'B', 21000.00,  45900.00, '2024-11-07T00:00:00'),
('H045', 'F008', 'Cotton',   'PHY 350 W3FE',     'Spring', 2024, 'WS-SE-01', '2024-10-20', 25.00,   28.750, 9.10,  'C', 15750.00,  23000.00, '2024-10-22T00:00:00'),
-- Additional poor-performing entries for underperformer analysis
('H046', 'F008', 'Soybeans', 'Asgrow AG46X6',    'Spring', 2024, 'WS-SE-01', '2024-11-08', 30.00,   42.000, 15.50, 'D', 12600.00,  15120.00, '2024-11-10T00:00:00'),
('H047', 'F005', 'Almonds',  'Carmel',           'Summer', 2023, 'WS-WC-01', '2023-09-05', 20.00,   18.000, 6.50,  'D', 14000.00,  16200.00, '2024-01-01T00:00:00'),
-- More Midwest Spring 2024 for statistical weight
('H048', 'F007', 'Soybeans', 'Asgrow AG36X6',    'Spring', 2024, 'WS-MW-01', '2024-10-08',100.00,  310.000, 12.20, 'A', 42000.00, 111600.00, '2024-10-10T00:00:00'),
('H049', 'F001', 'Corn',     'DeKalb DKC64',     'Summer', 2023, 'WS-MW-01', '2023-09-25', 40.00,  280.000, 15.50, 'C', 24000.00,  33600.00, '2024-01-01T00:00:00'),
('H050', 'F002', 'Corn',     'Pioneer P1197',    'Summer', 2023, 'WS-MW-01', '2023-09-28', 35.00,  210.000, 16.00, 'D', 21000.00,  25200.00, '2024-01-01T00:00:00'),
-- Southeast additional harvest records for variety
('H051', 'F003', 'Rice',     'Rex',              'Summer', 2023, 'WS-SE-01', '2023-09-08', 40.00,  260.000, 19.80, 'B', 20000.00,  39000.00, '2024-01-01T00:00:00'),
('H052', 'F004', 'Rice',     'CL153',            'Summer', 2023, 'WS-SE-01', '2023-09-10', 60.00,  402.000, 18.90, 'B', 30000.00,  60300.00, '2024-01-01T00:00:00'),
('H053', 'F004', 'Cotton',   'DP 1646 B2XF',     'Summer', 2023, 'WS-SE-01', '2023-11-15', 40.00,   40.000, 9.20,  'D', 24000.00,  26000.00, '2024-01-01T00:00:00'),
-- West Coast additional
('H054', 'F006', 'Grapes',   'Cabernet Sauv.',   'Spring', 2024, 'WS-WC-01', '2024-09-25', 20.00,  136.000, 22.00, 'A', 16500.00,  54400.00, '2024-09-27T00:00:00'),
('H055', 'F005', 'Grapes',   'Chardonnay',       'Spring', 2024, 'WS-WC-01', '2024-09-28', 10.00,   62.000, 22.80, 'B', 8500.00,   24800.00, '2024-09-30T00:00:00'),
-- Cross-region variety
('H056', 'F003', 'Cotton',   'PHY 350 W3FE',     'Summer', 2023, 'WS-SE-01', '2023-11-12', 30.00,   30.000, 9.00,  'C', 18000.00,  21000.00, '2024-01-01T00:00:00'),
('H057', 'F007', 'Corn',     'Pioneer P1197',    'Summer', 2023, 'WS-MW-01', '2023-09-22',100.00,  650.000, 15.80, 'C', 60000.00,  78000.00, '2024-01-01T00:00:00'),
('H058', 'F001', 'Soybeans', 'Pioneer P22T41',   'Summer', 2023, 'WS-MW-01', '2023-10-20', 30.00,   60.000, 14.50, 'D', 12000.00,  21600.00, '2024-01-01T00:00:00'),
('H059', 'F006', 'Almonds',  'Carmel',           'Spring', 2024, 'WS-WC-01', '2024-08-15', 40.00,   80.000, 5.40,  'B', 29000.00,  72000.00, '2024-08-17T00:00:00'),
('H060', 'F008', 'Rice',     'CL153',            'Spring', 2024, 'WS-SE-01', '2024-09-03', 30.00,  195.000, 18.80, 'A', 15600.00,  29250.00, '2024-09-05T00:00:00'),
('H061', 'F002', 'Soybeans', 'Pioneer P22T41',   'Summer', 2023, 'WS-MW-01', '2023-10-18', 20.00,   36.000, 14.80, 'D', 8000.00,   12960.00, '2024-01-01T00:00:00'),
('H062', 'F004', 'Rice',     'Rex',              'Spring', 2024, 'WS-SE-01', '2024-09-05', 40.00,  280.000, 18.20, 'A', 20800.00,  42000.00, '2024-09-07T00:00:00'),
('H063', 'F003', 'Cotton',   'DP 1646 B2XF',     'Spring', 2024, 'WS-SE-01', '2024-10-25', 20.00,   32.000, 8.00,  'A', 12600.00,  27200.00, '2024-10-27T00:00:00'),
('H064', 'F006', 'Grapes',   'Cabernet Sauv.',   'Summer', 2023, 'WS-WC-01', '2023-10-08', 15.00,   97.500, 23.50, 'B', 12000.00,  39000.00, '2024-01-01T00:00:00'),
('H065', 'F007', 'Soybeans', 'Asgrow AG36X6',    'Summer', 2023, 'WS-MW-01', '2023-10-25', 60.00,  108.000, 14.20, 'D', 24000.00,  38880.00, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_harvests;


-- =============================================================================
-- SEED DATA: raw_sensors (24 soil sensor readings)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_sensors VALUES
('SNS-001', 'F001', '2023-04-15', 42.30, 6.80, 45.20, 22.10, 180.50, '2024-01-01T00:00:00'),
('SNS-002', 'F001', '2023-07-20', 28.50, 6.75, 38.40, 20.30, 165.00, '2024-01-01T00:00:00'),
('SNS-003', 'F002', '2023-04-18', 55.10, 7.10, 52.00, 25.60, 195.00, '2024-01-01T00:00:00'),
('SNS-004', 'F002', '2023-07-22', 31.20, 7.05, 44.80, 23.10, 178.00, '2024-01-01T00:00:00'),
('SNS-005', 'F003', '2023-04-20', 60.80, 6.50, 40.10, 18.50, 150.20, '2024-01-01T00:00:00'),
('SNS-006', 'F003', '2023-07-25', 38.40, 6.45, 35.60, 16.80, 140.00, '2024-01-01T00:00:00'),
('SNS-007', 'F004', '2023-05-01', 68.20, 6.90, 55.30, 28.40, 210.00, '2024-01-01T00:00:00'),
('SNS-008', 'F004', '2023-08-10', 45.60, 6.85, 48.70, 26.00, 195.50, '2024-01-01T00:00:00'),
('SNS-009', 'F005', '2023-04-10', 15.30, 7.50, 28.00, 12.40, 120.00, '2024-01-01T00:00:00'),
('SNS-010', 'F005', '2023-08-01', 8.20,  7.45, 22.50, 10.10, 105.00, '2024-01-01T00:00:00'),
('SNS-011', 'F006', '2023-04-12', 35.60, 7.20, 42.80, 20.50, 170.00, '2024-01-01T00:00:00'),
('SNS-012', 'F006', '2023-08-05', 22.10, 7.15, 36.20, 18.00, 155.00, '2024-01-01T00:00:00'),
('SNS-013', 'F007', '2023-04-22', 48.90, 6.95, 50.10, 24.80, 200.00, '2024-01-01T00:00:00'),
('SNS-014', 'F007', '2023-07-28', 30.50, 6.90, 42.00, 22.00, 185.00, '2024-01-01T00:00:00'),
('SNS-015', 'F008', '2023-05-05', 52.40, 6.30, 32.50, 14.20, 130.00, '2024-01-01T00:00:00'),
('SNS-016', 'F008', '2023-08-15', 34.80, 6.25, 28.00, 12.50, 118.00, '2024-01-01T00:00:00'),
-- 2024 season readings
('SNS-017', 'F001', '2024-04-12', 46.50, 6.82, 47.00, 23.00, 182.00, '2024-04-15T00:00:00'),
('SNS-018', 'F002', '2024-04-15', 57.30, 7.12, 53.50, 26.20, 198.00, '2024-04-18T00:00:00'),
('SNS-019', 'F003', '2024-04-18', 62.10, 6.52, 41.50, 19.00, 152.00, '2024-04-20T00:00:00'),
('SNS-020', 'F004', '2024-05-02', 70.50, 6.92, 56.80, 29.10, 215.00, '2024-05-05T00:00:00'),
('SNS-021', 'F005', '2024-04-08', 18.60, 7.48, 30.20, 13.00, 125.00, '2024-04-10T00:00:00'),
('SNS-022', 'F006', '2024-04-10', 38.20, 7.22, 44.50, 21.20, 175.00, '2024-04-12T00:00:00'),
('SNS-023', 'F007', '2024-04-20', 50.80, 6.98, 51.50, 25.50, 205.00, '2024-04-22T00:00:00'),
('SNS-024', 'F008', '2024-05-08', 55.00, 6.32, 34.00, 15.00, 135.00, '2024-05-10T00:00:00');

ASSERT ROW_COUNT = 24
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sensors;

