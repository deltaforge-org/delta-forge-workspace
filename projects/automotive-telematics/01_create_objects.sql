-- =============================================================================
-- Automotive Telematics Pipeline: Create Objects & Seed Data
-- =============================================================================

-- =============================================================================
-- ZONES
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw vehicle, driver, and trip telematics data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched trip data with safety and efficiency metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Star schema for fleet performance analytics';

-- =============================================================================
-- BRONZE TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_vehicles (
    vin                 STRING      NOT NULL,
    vehicle_type        STRING      NOT NULL,
    make                STRING      NOT NULL,
    model               STRING      NOT NULL,
    year                INT         NOT NULL,
    fuel_type           STRING      NOT NULL,
    odometer_km         INT         NOT NULL,
    last_service_date   DATE,
    next_service_km     INT,
    fleet_id            STRING      NOT NULL,
    is_active           BOOLEAN     NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/automotive/bronze/raw_vehicles';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_vehicles TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_drivers (
    driver_id               STRING      NOT NULL,
    name                    STRING      NOT NULL,
    license_class           STRING      NOT NULL,
    hire_date               DATE        NOT NULL,
    training_level          STRING      NOT NULL,
    safety_certifications   STRING,
    fleet_id                STRING      NOT NULL,
    ingested_at             TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/automotive/bronze/raw_drivers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_drivers TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_routes (
    route_id            STRING      NOT NULL,
    route_name          STRING      NOT NULL,
    origin_city         STRING      NOT NULL,
    dest_city           STRING      NOT NULL,
    distance_km         DECIMAL(8,2) NOT NULL,
    road_type           STRING      NOT NULL,
    avg_traffic_index   DECIMAL(4,2) NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/automotive/bronze/raw_routes';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_routes TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_trips (
    trip_id             STRING      NOT NULL,
    vin                 STRING      NOT NULL,
    driver_id           STRING      NOT NULL,
    route_id            STRING      NOT NULL,
    trip_date           DATE        NOT NULL,
    trip_month          STRING      NOT NULL,
    distance_km         DECIMAL(8,2) NOT NULL,
    duration_min        DECIMAL(8,2) NOT NULL,
    avg_speed_kmh       DECIMAL(6,2),
    max_speed_kmh       DECIMAL(6,2),
    fuel_consumed_l     DECIMAL(8,2) NOT NULL,
    harsh_brake_count   INT         NOT NULL,
    harsh_accel_count   INT         NOT NULL,
    idle_time_min       DECIMAL(8,2) NOT NULL,
    engine_temp_max_c   DECIMAL(6,2),
    tire_pressure_avg_psi DECIMAL(5,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/automotive/bronze/raw_trips'
PARTITIONED BY (vehicle_type STRING, trip_month STRING);

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_trips TO USER {{current_user}};

-- =============================================================================
-- SILVER TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.trips_enriched (
    trip_id                 STRING      NOT NULL,
    vin                     STRING      NOT NULL,
    driver_id               STRING      NOT NULL,
    route_id                STRING      NOT NULL,
    fleet_id                STRING      NOT NULL,
    vehicle_type            STRING      NOT NULL,
    trip_date               DATE        NOT NULL,
    trip_month              STRING      NOT NULL,
    distance_km             DECIMAL(8,2),
    duration_min            DECIMAL(8,2),
    avg_speed_kmh           DECIMAL(6,2),
    max_speed_kmh           DECIMAL(6,2),
    fuel_consumed_l         DECIMAL(8,2),
    fuel_efficiency_km_per_l DECIMAL(8,3),
    harsh_brake_count       INT,
    harsh_accel_count       INT,
    idle_time_min           DECIMAL(8,2),
    idle_pct                DECIMAL(5,2),
    engine_temp_max_c       DECIMAL(6,2),
    tire_pressure_avg_psi   DECIMAL(5,2),
    safety_score            DECIMAL(5,2),
    maintenance_due         BOOLEAN,
    enriched_at             TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/automotive/silver/trips_enriched'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.trips_enriched TO USER {{current_user}};

-- =============================================================================
-- GOLD TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_vehicle (
    vehicle_key         STRING      NOT NULL,
    vin                 STRING      NOT NULL,
    vehicle_type        STRING      NOT NULL,
    make                STRING      NOT NULL,
    model               STRING      NOT NULL,
    year                INT,
    fuel_type           STRING,
    odometer_km         INT,
    last_service_date   DATE,
    next_service_km     INT,
    fleet_id            STRING
) LOCATION '{{data_path}}/automotive/gold/dim_vehicle';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_vehicle TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_driver (
    driver_key              STRING      NOT NULL,
    driver_id               STRING      NOT NULL,
    name                    STRING      NOT NULL,
    license_class           STRING,
    hire_date               DATE,
    training_level          STRING,
    safety_certifications   STRING
) LOCATION '{{data_path}}/automotive/gold/dim_driver';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_driver TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_route (
    route_key           STRING      NOT NULL,
    route_name          STRING      NOT NULL,
    origin_city         STRING      NOT NULL,
    dest_city           STRING      NOT NULL,
    distance_km         DECIMAL(8,2),
    road_type           STRING,
    avg_traffic_index   DECIMAL(4,2)
) LOCATION '{{data_path}}/automotive/gold/dim_route';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_route TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_trips (
    trip_key                    STRING      NOT NULL,
    vehicle_key                 STRING      NOT NULL,
    driver_key                  STRING      NOT NULL,
    route_key                   STRING      NOT NULL,
    trip_date                   DATE        NOT NULL,
    distance_km                 DECIMAL(8,2),
    duration_min                DECIMAL(8,2),
    avg_speed_kmh               DECIMAL(6,2),
    max_speed_kmh               DECIMAL(6,2),
    fuel_consumed_l             DECIMAL(8,2),
    fuel_efficiency_km_per_l    DECIMAL(8,3),
    harsh_brake_count           INT,
    harsh_accel_count           INT,
    idle_time_min               DECIMAL(8,2),
    engine_temp_max_c           DECIMAL(6,2),
    tire_pressure_avg_psi       DECIMAL(5,2),
    safety_score                DECIMAL(5,2)
) LOCATION '{{data_path}}/automotive/gold/fact_trips';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_trips TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_fleet_performance (
    fleet_id                STRING      NOT NULL,
    vehicle_type            STRING      NOT NULL,
    month                   STRING      NOT NULL,
    total_distance          DECIMAL(12,2),
    total_fuel              DECIMAL(12,2),
    avg_fuel_efficiency     DECIMAL(8,3),
    total_trips             INT,
    avg_safety_score        DECIMAL(5,2),
    maintenance_due_count   INT,
    idle_pct                DECIMAL(5,2),
    harsh_event_rate        DECIMAL(8,4),
    utilization_pct         DECIMAL(5,2)
) LOCATION '{{data_path}}/automotive/gold/kpi_fleet_performance';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_fleet_performance TO USER {{current_user}};

-- =============================================================================
-- PSEUDONYMISATION: Generalize VIN (first 8 chars visible, rest masked)
-- =============================================================================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_vehicle (vin)
    TRANSFORM generalize
    PARAMS (range = 10);

-- =============================================================================
-- SEED DATA: raw_vehicles (10 vehicles across 3 fleets)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_vehicles VALUES
('1HGBH41JXMN109186', 'Truck',  'Freightliner', 'Cascadia',     2022, 'Diesel',   125000, '2024-01-15', 140000, 'FL-EAST',  true,  '2024-01-01T00:00:00'),
('2T1BURHE5JC123456', 'Sedan',  'Toyota',       'Camry',        2023, 'Hybrid',    35000, '2024-02-01',  45000, 'FL-EAST',  true,  '2024-01-01T00:00:00'),
('3VWDX7AJ1DM012345', 'Van',    'Mercedes',     'Sprinter',     2021, 'Diesel',    89000, '2023-12-10', 100000, 'FL-EAST',  true,  '2024-01-01T00:00:00'),
('4T1B11HK5JU567890', 'Truck',  'Kenworth',     'T680',         2020, 'Diesel',   210000, '2023-11-20', 220000, 'FL-CENTRAL', true, '2024-01-01T00:00:00'),
('5YJSA1E26MF098765', 'Sedan',  'Tesla',        'Model S',      2024, 'Electric',  12000, '2024-03-01',  25000, 'FL-CENTRAL', true, '2024-01-01T00:00:00'),
('6FPPX4EP2N2345678', 'Van',    'Ford',         'Transit',      2022, 'Gasoline',  67000, '2024-01-05',  75000, 'FL-CENTRAL', true, '2024-01-01T00:00:00'),
('7WDBRF61XNT654321', 'Truck',  'Volvo',        'VNL 860',      2021, 'Diesel',   185000, '2023-10-15', 195000, 'FL-WEST',  true,  '2024-01-01T00:00:00'),
('8GCGG25K371876543', 'Van',    'Chevrolet',    'Express',      2023, 'Gasoline',  42000, '2024-02-20',  55000, 'FL-WEST',  true,  '2024-01-01T00:00:00'),
('9BWSE61J984765432', 'Sedan',  'Honda',        'Accord',       2022, 'Hybrid',    55000, '2024-01-10',  65000, 'FL-WEST',  true,  '2024-01-01T00:00:00'),
('1FTFW1ET2DFC23456', 'Truck',  'Ford',         'F-150',        2023, 'Gasoline', 148000, '2023-09-01', 155000, 'FL-EAST',  false, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_vehicles;


-- =============================================================================
-- SEED DATA: raw_drivers (8 drivers)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_drivers VALUES
('DRV-001', 'Marcus Thompson',   'Class A CDL', '2019-03-15', 'Advanced',     'HazMat,Tanker',         'FL-EAST',    '2024-01-01T00:00:00'),
('DRV-002', 'Sarah Chen',        'Class B CDL', '2020-06-01', 'Intermediate', 'Passenger',             'FL-EAST',    '2024-01-01T00:00:00'),
('DRV-003', 'James Rodriguez',   'Class C',     '2021-01-10', 'Basic',        NULL,                    'FL-EAST',    '2024-01-01T00:00:00'),
('DRV-004', 'Aisha Patel',       'Class A CDL', '2018-08-20', 'Advanced',     'HazMat,Doubles',        'FL-CENTRAL', '2024-01-01T00:00:00'),
('DRV-005', 'David Kim',         'Class C',     '2022-04-15', 'Basic',        NULL,                    'FL-CENTRAL', '2024-01-01T00:00:00'),
('DRV-006', 'Elena Volkov',      'Class B CDL', '2020-11-01', 'Intermediate', 'Passenger,Air Brake',   'FL-CENTRAL', '2024-01-01T00:00:00'),
('DRV-007', 'Robert Williams',   'Class A CDL', '2017-02-28', 'Advanced',     'HazMat,Tanker,Doubles', 'FL-WEST',    '2024-01-01T00:00:00'),
('DRV-008', 'Lisa Nakamura',     'Class B CDL', '2021-07-15', 'Intermediate', 'Air Brake',             'FL-WEST',    '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_drivers;


-- =============================================================================
-- SEED DATA: raw_routes (6 routes)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_routes VALUES
('RT-001', 'Eastern Corridor',   'New York',     'Boston',        340.00, 'Highway',    3.20, '2024-01-01T00:00:00'),
('RT-002', 'Metro Distribution', 'Philadelphia', 'Newark',         150.00, 'Mixed',      4.50, '2024-01-01T00:00:00'),
('RT-003', 'Central Haul',       'Chicago',      'Indianapolis',   290.00, 'Highway',    2.80, '2024-01-01T00:00:00'),
('RT-004', 'Southern Link',      'Dallas',       'Houston',        380.00, 'Highway',    2.50, '2024-01-01T00:00:00'),
('RT-005', 'Pacific Route',      'Los Angeles',  'San Francisco',  615.00, 'Highway',    3.00, '2024-01-01T00:00:00'),
('RT-006', 'Urban Delivery',     'Seattle',      'Portland',       280.00, 'Mixed',      3.80, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_routes;


-- =============================================================================
-- SEED DATA: raw_trips (70 trip records across 10 vehicles, 8 drivers, 6 routes)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_trips VALUES
-- FL-EAST: Truck (Cascadia) - DRV-001 - Long haul trips
('T001', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-01-05', '2024-01', 338.50, 245.00, 82.90, 105.00, 95.20,  1, 0, 18.50, 92.30, 105.50, '2024-01-06T00:00:00'),
('T002', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-01-12', '2024-01', 342.00, 260.00, 78.90, 102.00, 98.50,  0, 1, 22.00, 91.80, 104.80, '2024-01-13T00:00:00'),
('T003', '1HGBH41JXMN109186', 'DRV-001', 'RT-002', '2024-01-19', '2024-01', 148.00, 135.00, 65.80,  88.00, 52.30,  2, 1, 28.00, 93.50, 106.20, '2024-01-20T00:00:00'),
('T004', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-02-02', '2024-02', 340.00, 250.00, 81.60, 103.00, 96.00,  0, 0, 15.00, 90.50, 105.00, '2024-02-03T00:00:00'),
('T005', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-02-16', '2024-02', 339.00, 255.00, 79.80, 101.00, 97.80,  1, 0, 20.00, 91.20, 104.50, '2024-02-17T00:00:00'),
('T006', '1HGBH41JXMN109186', 'DRV-001', 'RT-002', '2024-02-23', '2024-02', 152.00, 140.00, 65.10,  90.00, 55.00,  3, 2, 32.00, 94.10, 105.80, '2024-02-24T00:00:00'),
('T007', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-03-08', '2024-03', 341.00, 248.00, 82.50, 104.00, 94.50,  0, 0, 14.00, 90.00, 105.20, '2024-03-09T00:00:00'),
-- FL-EAST: Sedan (Camry) - DRV-003 - Metro trips
('T008', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-01-08', '2024-01', 148.50, 165.00, 54.00,  85.00, 8.20,   3, 2, 35.00, 78.50,  33.20, '2024-01-09T00:00:00'),
('T009', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-01-15', '2024-01', 150.00, 158.00, 57.00,  82.00, 7.80,   4, 3, 30.00, 77.00,  33.00, '2024-01-16T00:00:00'),
('T010', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-01-22', '2024-01', 149.00, 170.00, 52.60,  88.00, 8.50,   5, 4, 42.00, 79.20,  33.50, '2024-01-23T00:00:00'),
('T011', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-02-05', '2024-02', 151.00, 160.00, 56.60,  84.00, 7.90,   2, 1, 28.00, 76.80,  33.10, '2024-02-06T00:00:00'),
('T012', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-02-19', '2024-02', 149.50, 155.00, 57.90,  80.00, 7.60,   1, 1, 22.00, 77.50,  33.30, '2024-02-20T00:00:00'),
('T013', '2T1BURHE5JC123456', 'DRV-003', 'RT-001', '2024-03-04', '2024-03', 340.00, 290.00, 70.30,  95.00, 16.50,  2, 2, 25.00, 80.10,  33.00, '2024-03-05T00:00:00'),
-- FL-EAST: Van (Sprinter) - DRV-002 - Delivery runs
('T014', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-01-10', '2024-01', 145.00, 180.00, 48.30,  75.00, 18.50,  1, 0, 40.00, 88.20,  42.50, '2024-01-11T00:00:00'),
('T015', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-01-17', '2024-01', 152.00, 190.00, 48.00,  72.00, 19.80,  0, 1, 45.00, 87.50,  42.00, '2024-01-18T00:00:00'),
('T016', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-01-24', '2024-01', 148.00, 175.00, 50.70,  78.00, 18.00,  1, 0, 38.00, 89.00,  43.00, '2024-01-25T00:00:00'),
('T017', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-02-07', '2024-02', 150.00, 185.00, 48.60,  74.00, 19.20,  0, 0, 42.00, 88.00,  42.20, '2024-02-08T00:00:00'),
('T018', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-001', '2024-02-14', '2024-02', 338.00, 310.00, 65.40,  90.00, 42.00,  2, 1, 35.00, 90.50,  43.50, '2024-02-15T00:00:00'),
('T019', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-03-01', '2024-03', 149.00, 178.00, 50.20,  76.00, 18.20,  0, 0, 36.00, 87.80,  42.80, '2024-03-02T00:00:00'),
-- FL-CENTRAL: Truck (Kenworth) - DRV-004 - Highway haul
('T020', '4T1B11HK5JU567890', 'DRV-004', 'RT-003', '2024-01-04', '2024-01', 288.00, 200.00, 86.40, 110.00, 82.00,  0, 0, 12.00, 95.80, 108.00, '2024-01-05T00:00:00'),
('T021', '4T1B11HK5JU567890', 'DRV-004', 'RT-004', '2024-01-11', '2024-01', 378.00, 265.00, 85.60, 108.00, 108.50, 1, 0, 18.00, 96.20, 107.50, '2024-01-12T00:00:00'),
('T022', '4T1B11HK5JU567890', 'DRV-004', 'RT-003', '2024-01-18', '2024-01', 290.00, 205.00, 84.90, 106.00, 84.00,  0, 1, 15.00, 94.50, 108.20, '2024-01-19T00:00:00'),
('T023', '4T1B11HK5JU567890', 'DRV-004', 'RT-004', '2024-02-01', '2024-02', 380.00, 270.00, 84.40, 112.00, 110.00, 2, 1, 22.00, 97.00, 107.00, '2024-02-02T00:00:00'),
('T024', '4T1B11HK5JU567890', 'DRV-004', 'RT-003', '2024-02-15', '2024-02', 291.00, 210.00, 83.10, 105.00, 85.50,  0, 0, 10.00, 93.80, 108.50, '2024-02-16T00:00:00'),
('T025', '4T1B11HK5JU567890', 'DRV-004', 'RT-004', '2024-03-01', '2024-03', 375.00, 258.00, 87.20, 115.00, 106.00, 1, 2, 20.00, 98.50, 106.80, '2024-03-02T00:00:00'),
('T026', '4T1B11HK5JU567890', 'DRV-004', 'RT-003', '2024-03-15', '2024-03', 292.00, 208.00, 84.20, 107.00, 83.50,  0, 0, 11.00, 94.00, 108.00, '2024-03-16T00:00:00'),
-- FL-CENTRAL: Tesla - DRV-005 - City trips (electric)
('T027', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-01-09', '2024-01', 285.00, 195.00, 87.70,  98.00, 0.00,   1, 2, 8.00,  42.00,  35.00, '2024-01-10T00:00:00'),
('T028', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-01-23', '2024-01', 290.00, 200.00, 87.00,  99.00, 0.00,   2, 3, 12.00, 43.50,  35.20, '2024-01-24T00:00:00'),
('T029', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-02-06', '2024-02', 288.00, 198.00, 87.30,  97.00, 0.00,   0, 1, 6.00,  41.80,  35.00, '2024-02-07T00:00:00'),
('T030', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-02-20', '2024-02', 291.00, 202.00, 86.40,  96.00, 0.00,   3, 4, 15.00, 44.00,  34.80, '2024-02-21T00:00:00'),
('T031', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-03-05', '2024-03', 287.00, 196.00, 87.90, 100.00, 0.00,   1, 1, 7.00,  42.20,  35.10, '2024-03-06T00:00:00'),
-- FL-CENTRAL: Van (Transit) - DRV-006 - Mixed deliveries
('T032', '6FPPX4EP2N2345678', 'DRV-006', 'RT-003', '2024-01-08', '2024-01', 288.00, 240.00, 72.00,  92.00, 38.40,  1, 1, 30.00, 85.00,  36.50, '2024-01-09T00:00:00'),
('T033', '6FPPX4EP2N2345678', 'DRV-006', 'RT-004', '2024-01-15', '2024-01', 375.00, 300.00, 75.00,  95.00, 50.00,  0, 0, 25.00, 86.50,  37.00, '2024-01-16T00:00:00'),
('T034', '6FPPX4EP2N2345678', 'DRV-006', 'RT-003', '2024-01-29', '2024-01', 290.00, 245.00, 71.00,  90.00, 39.50,  2, 1, 35.00, 85.80,  36.00, '2024-01-30T00:00:00'),
('T035', '6FPPX4EP2N2345678', 'DRV-006', 'RT-004', '2024-02-12', '2024-02', 378.00, 305.00, 74.40,  94.00, 51.50,  1, 0, 28.00, 87.00,  37.20, '2024-02-13T00:00:00'),
('T036', '6FPPX4EP2N2345678', 'DRV-006', 'RT-003', '2024-02-26', '2024-02', 289.00, 242.00, 71.60,  91.00, 39.00,  0, 1, 32.00, 85.50,  36.30, '2024-02-27T00:00:00'),
('T037', '6FPPX4EP2N2345678', 'DRV-006', 'RT-004', '2024-03-11', '2024-03', 380.00, 298.00, 76.50,  96.00, 49.00,  1, 0, 22.00, 86.00,  37.50, '2024-03-12T00:00:00'),
-- FL-WEST: Truck (Volvo) - DRV-007 - Pacific route (experienced driver)
('T038', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-01-03', '2024-01', 612.00, 420.00, 87.40, 108.00, 172.00, 0, 0, 25.00, 92.00, 110.00, '2024-01-04T00:00:00'),
('T039', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-01-10', '2024-01', 615.00, 425.00, 86.80, 107.00, 175.00, 0, 0, 28.00, 91.50, 109.50, '2024-01-11T00:00:00'),
('T040', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-01-17', '2024-01', 610.00, 418.00, 87.60, 106.00, 170.00, 1, 0, 22.00, 92.50, 110.20, '2024-01-18T00:00:00'),
('T041', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-01-31', '2024-01', 618.00, 430.00, 86.20, 109.00, 178.00, 0, 1, 30.00, 93.00, 109.80, '2024-02-01T00:00:00'),
('T042', '7WDBRF61XNT654321', 'DRV-007', 'RT-006', '2024-02-07', '2024-02', 278.00, 210.00, 79.40,  95.00, 78.00,  0, 0, 15.00, 89.00, 110.50, '2024-02-08T00:00:00'),
('T043', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-02-14', '2024-02', 614.00, 422.00, 87.30, 108.00, 173.00, 0, 0, 26.00, 91.80, 110.00, '2024-02-15T00:00:00'),
('T044', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-02-28', '2024-02', 616.00, 428.00, 86.40, 107.00, 176.00, 1, 0, 24.00, 92.20, 109.70, '2024-03-01T00:00:00'),
('T045', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-03-07', '2024-03', 611.00, 420.00, 87.30, 106.00, 171.00, 0, 0, 23.00, 91.50, 110.30, '2024-03-08T00:00:00'),
('T046', '7WDBRF61XNT654321', 'DRV-007', 'RT-006', '2024-03-14', '2024-03', 280.00, 215.00, 78.10,  94.00, 79.50,  0, 0, 18.00, 89.50, 110.00, '2024-03-15T00:00:00'),
-- FL-WEST: Van (Express) - DRV-008 - Mixed routes
('T047', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-01-06', '2024-01', 275.00, 225.00, 73.30,  88.00, 35.00,  1, 1, 28.00, 82.00,  35.50, '2024-01-07T00:00:00'),
('T048', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-01-13', '2024-01', 280.00, 230.00, 73.00,  90.00, 36.50,  2, 2, 32.00, 83.50,  35.00, '2024-01-14T00:00:00'),
('T049', '8GCGG25K371876543', 'DRV-008', 'RT-005', '2024-01-20', '2024-01', 610.00, 480.00, 76.30,  92.00, 78.00,  1, 0, 45.00, 85.00,  36.00, '2024-01-21T00:00:00'),
('T050', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-02-03', '2024-02', 278.00, 228.00, 73.20,  89.00, 35.80,  0, 1, 26.00, 82.50,  35.30, '2024-02-04T00:00:00'),
('T051', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-02-17', '2024-02', 282.00, 232.00, 72.90,  87.00, 37.00,  1, 0, 30.00, 83.00,  35.80, '2024-02-18T00:00:00'),
('T052', '8GCGG25K371876543', 'DRV-008', 'RT-005', '2024-03-02', '2024-03', 612.00, 475.00, 77.30,  93.00, 77.00,  0, 1, 40.00, 84.50,  36.20, '2024-03-03T00:00:00'),
-- FL-WEST: Sedan (Accord) - DRV-008 (shared driver)
('T053', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-01-07', '2024-01', 278.00, 210.00, 79.40,  92.00, 18.50,  1, 1, 15.00, 75.00,  33.50, '2024-01-08T00:00:00'),
('T054', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-01-21', '2024-01', 280.00, 215.00, 78.10,  90.00, 19.00,  0, 0, 12.00, 74.50,  33.80, '2024-01-22T00:00:00'),
('T055', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-02-04', '2024-02', 276.00, 208.00, 79.60,  91.00, 18.20,  0, 1, 14.00, 75.20,  33.50, '2024-02-05T00:00:00'),
('T056', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-02-18', '2024-02', 281.00, 218.00, 77.30,  89.00, 19.30,  1, 0, 16.00, 76.00,  34.00, '2024-02-19T00:00:00'),
('T057', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-03-03', '2024-03', 279.00, 212.00, 78.90,  91.00, 18.60,  0, 0, 13.00, 74.80,  33.60, '2024-03-04T00:00:00'),
-- FL-EAST: Decommissioned truck (F-150) - some historical trips before decommission
('T058', '1FTFW1ET2DFC23456', 'DRV-001', 'RT-002', '2024-01-03', '2024-01', 150.00, 145.00, 62.10,  85.00, 22.50,  3, 2, 25.00, 96.00,  35.00, '2024-01-04T00:00:00'),
('T059', '1FTFW1ET2DFC23456', 'DRV-002', 'RT-002', '2024-01-10', '2024-01', 148.00, 150.00, 59.20,  82.00, 23.00,  4, 3, 30.00, 97.50,  34.50, '2024-01-11T00:00:00'),
('T060', '1FTFW1ET2DFC23456', 'DRV-001', 'RT-001', '2024-01-17', '2024-01', 335.00, 280.00, 71.80,  98.00, 52.00,  2, 1, 28.00, 95.80,  35.20, '2024-01-18T00:00:00'),
-- Additional trips for statistical depth
('T061', '1HGBH41JXMN109186', 'DRV-001', 'RT-001', '2024-03-15', '2024-03', 340.50, 252.00, 81.10, 104.00, 95.80,  0, 1, 16.00, 91.00, 105.00, '2024-03-16T00:00:00'),
('T062', '3VWDX7AJ1DM012345', 'DRV-002', 'RT-002', '2024-03-08', '2024-03', 150.00, 182.00, 49.50,  74.00, 19.00,  1, 0, 40.00, 88.50,  42.50, '2024-03-09T00:00:00'),
('T063', '4T1B11HK5JU567890', 'DRV-004', 'RT-004', '2024-03-22', '2024-03', 382.00, 268.00, 85.50, 113.00, 109.00, 0, 1, 18.00, 96.80, 107.50, '2024-03-23T00:00:00'),
('T064', '6FPPX4EP2N2345678', 'DRV-006', 'RT-003', '2024-03-25', '2024-03', 291.00, 248.00, 70.40,  88.00, 40.00,  1, 1, 34.00, 86.20,  36.50, '2024-03-26T00:00:00'),
('T065', '7WDBRF61XNT654321', 'DRV-007', 'RT-005', '2024-03-21', '2024-03', 613.00, 421.00, 87.40, 107.00, 172.50, 0, 0, 24.00, 92.00, 110.00, '2024-03-22T00:00:00'),
('T066', '8GCGG25K371876543', 'DRV-008', 'RT-006', '2024-03-16', '2024-03', 277.00, 226.00, 73.50,  88.00, 35.50,  1, 1, 29.00, 82.80,  35.50, '2024-03-17T00:00:00'),
('T067', '9BWSE61J984765432', 'DRV-008', 'RT-006', '2024-03-17', '2024-03', 280.00, 214.00, 78.50,  90.00, 18.80,  0, 1, 14.00, 75.50,  33.70, '2024-03-18T00:00:00'),
('T068', '2T1BURHE5JC123456', 'DRV-003', 'RT-002', '2024-03-11', '2024-03', 151.00, 162.00, 55.90,  86.00, 8.00,   2, 1, 26.00, 78.00,  33.20, '2024-03-12T00:00:00'),
('T069', '5YJSA1E26MF098765', 'DRV-005', 'RT-003', '2024-03-19', '2024-03', 289.00, 198.00, 87.60,  99.00, 0.00,   0, 0, 5.00,  41.50,  35.00, '2024-03-20T00:00:00'),
('T070', '1HGBH41JXMN109186', 'DRV-002', 'RT-001', '2024-03-22', '2024-03', 339.00, 258.00, 78.80, 100.00, 97.00,  1, 1, 20.00, 92.00, 105.00, '2024-03-23T00:00:00');

ASSERT ROW_COUNT = 70
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_trips;

