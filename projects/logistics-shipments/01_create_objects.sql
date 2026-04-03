-- =============================================================================
-- Logistics Shipments Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw shipment tracking events and reference data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Deduplicated shipment records with derived metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for delivery performance analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_carriers (
    carrier_id     STRING      NOT NULL,
    carrier_name   STRING,
    carrier_type   STRING,
    fleet_size     INT,
    on_time_rating DECIMAL(3,2),
    cost_per_kg    DECIMAL(6,2),
    ingested_at    TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_carriers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_locations (
    location_id    STRING      NOT NULL,
    city           STRING,
    state          STRING,
    country        STRING,
    region         STRING,
    warehouse_flag BOOLEAN,
    latitude       DECIMAL(9,6),
    longitude      DECIMAL(9,6),
    ingested_at    TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_locations';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_customers (
    customer_id    STRING      NOT NULL,
    customer_name  STRING,
    tier           STRING,
    city           STRING,
    ingested_at    TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_customers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_events (
    event_id         STRING      NOT NULL,
    shipment_id      STRING      NOT NULL,
    customer_id      STRING,
    carrier_id       STRING,
    origin_id        STRING,
    destination_id   STRING,
    event_type       STRING,
    event_timestamp  TIMESTAMP   NOT NULL,
    ship_date        DATE,
    promised_date    DATE,
    delivery_date    DATE,
    weight_kg        DECIMAL(8,2),
    volume_m3        DECIMAL(8,4),
    cost             DECIMAL(10,2),
    revenue          DECIMAL(10,2),
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_events';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.shipments_deduped (
    shipment_id      STRING      NOT NULL,
    customer_id      STRING,
    carrier_id       STRING,
    origin_id        STRING,
    destination_id   STRING,
    ship_date        DATE,
    delivery_date    DATE,
    promised_date    DATE,
    latest_status    STRING,
    weight_kg        DECIMAL(8,2),
    volume_m3        DECIMAL(8,4),
    cost             DECIMAL(10,2),
    revenue          DECIMAL(10,2),
    transit_days     INT,
    on_time_flag     BOOLEAN,
    event_count      INT,
    last_event_time  TIMESTAMP,
    deduped_at       TIMESTAMP
) LOCATION '{{data_path}}/logistics/silver/shipments_deduped';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_carrier (
    carrier_key    STRING      NOT NULL,
    carrier_name   STRING,
    carrier_type   STRING,
    fleet_size     INT,
    on_time_rating DECIMAL(3,2),
    cost_per_kg    DECIMAL(6,2)
) LOCATION '{{data_path}}/logistics/gold/dim_carrier';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_location (
    location_key   STRING      NOT NULL,
    city           STRING,
    state          STRING,
    country        STRING,
    region         STRING,
    warehouse_flag BOOLEAN,
    latitude       DECIMAL(9,6),
    longitude      DECIMAL(9,6)
) LOCATION '{{data_path}}/logistics/gold/dim_location';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_route (
    route_key         STRING      NOT NULL,
    origin_city       STRING,
    destination_city  STRING,
    distance_km       INT,
    avg_transit_days  DECIMAL(5,1),
    mode              STRING
) LOCATION '{{data_path}}/logistics/gold/dim_route';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_shipments (
    shipment_key    STRING      NOT NULL,
    carrier_key     STRING,
    origin_key      STRING,
    destination_key STRING,
    customer_key    STRING,
    ship_date       DATE,
    delivery_date   DATE,
    promised_date   DATE,
    weight_kg       DECIMAL(8,2),
    volume_m3       DECIMAL(8,4),
    cost            DECIMAL(10,2),
    on_time_flag    BOOLEAN,
    transit_days    INT
) LOCATION '{{data_path}}/logistics/gold/fact_shipments';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_delivery_performance (
    carrier          STRING,
    route            STRING,
    month            STRING,
    total_shipments  INT,
    on_time_count    INT,
    on_time_pct      DECIMAL(5,2),
    avg_transit_days DECIMAL(5,1),
    avg_cost_per_kg  DECIMAL(8,2),
    total_weight     DECIMAL(12,2),
    total_revenue    DECIMAL(12,2)
) LOCATION '{{data_path}}/logistics/gold/kpi_delivery_performance';

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_carriers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_locations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_customers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_events TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.shipments_deduped TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_carrier TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_location TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_route TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_shipments TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_delivery_performance TO USER {{current_user}};

-- ===================== SEED DATA: CARRIERS (6 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_carriers VALUES
('CAR-001', 'SwiftFreight',    'LTL',         120, 0.92, 1.85, '2024-06-01T00:00:00'),
('CAR-002', 'GlobalExpress',   'Express',      80, 0.95, 3.20, '2024-06-01T00:00:00'),
('CAR-003', 'OceanLink',       'Ocean',        40, 0.88, 0.45, '2024-06-01T00:00:00'),
('CAR-004', 'RailConnect',     'Rail',          60, 0.90, 0.75, '2024-06-01T00:00:00'),
('CAR-005', 'AeroFreight',     'Air',           30, 0.94, 5.50, '2024-06-01T00:00:00'),
('CAR-006', 'RegionalHaul',    'Regional LTL', 200, 0.87, 1.20, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_carriers;


-- ===================== SEED DATA: LOCATIONS (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_locations VALUES
('LOC-NYC', 'New York',      'NY', 'US', 'Northeast', true,  40.712800, -74.006000, '2024-06-01T00:00:00'),
('LOC-CHI', 'Chicago',       'IL', 'US', 'Midwest',   true,  41.878100, -87.629800, '2024-06-01T00:00:00'),
('LOC-LAX', 'Los Angeles',   'CA', 'US', 'West',      true,  34.052200, -118.243700,'2024-06-01T00:00:00'),
('LOC-MIA', 'Miami',         'FL', 'US', 'Southeast',  true,  25.761700, -80.191800, '2024-06-01T00:00:00'),
('LOC-DAL', 'Dallas',        'TX', 'US', 'South',     true,  32.776700, -96.797000, '2024-06-01T00:00:00'),
('LOC-SEA', 'Seattle',       'WA', 'US', 'Northwest', true,  47.606200, -122.332100,'2024-06-01T00:00:00'),
('LOC-ATL', 'Atlanta',       'GA', 'US', 'Southeast',  false, 33.749000, -84.388000, '2024-06-01T00:00:00'),
('LOC-DEN', 'Denver',        'CO', 'US', 'Mountain',  false, 39.739200, -104.990300,'2024-06-01T00:00:00'),
('LOC-BOS', 'Boston',        'MA', 'US', 'Northeast', false, 42.360100, -71.058900, '2024-06-01T00:00:00'),
('LOC-PHX', 'Phoenix',       'AZ', 'US', 'Southwest', false, 33.448400, -112.074000,'2024-06-01T00:00:00'),
('LOC-PDX', 'Portland',      'OR', 'US', 'Northwest', false, 45.515200, -122.678400,'2024-06-01T00:00:00'),
('LOC-MSP', 'Minneapolis',   'MN', 'US', 'Midwest',   false, 44.977800, -93.265000, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_locations;


-- ===================== SEED DATA: CUSTOMERS (20 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_customers VALUES
('CUST-001', 'Acme Corp',         'Enterprise', 'New York',     '2024-06-01T00:00:00'),
('CUST-002', 'TechStart Inc',     'SMB',        'San Francisco','2024-06-01T00:00:00'),
('CUST-003', 'Global Retail',     'Enterprise', 'Chicago',      '2024-06-01T00:00:00'),
('CUST-004', 'FreshFoods LLC',    'Mid-Market', 'Miami',        '2024-06-01T00:00:00'),
('CUST-005', 'AutoParts Direct',  'Enterprise', 'Dallas',       '2024-06-01T00:00:00'),
('CUST-006', 'HomeGoods Plus',    'SMB',        'Seattle',      '2024-06-01T00:00:00'),
('CUST-007', 'MedSupply Co',      'Enterprise', 'Atlanta',      '2024-06-01T00:00:00'),
('CUST-008', 'BuildRight Ltd',    'Mid-Market', 'Denver',       '2024-06-01T00:00:00'),
('CUST-009', 'ElectroWare',       'SMB',        'Boston',       '2024-06-01T00:00:00'),
('CUST-010', 'SunBelt Dist',      'Mid-Market', 'Phoenix',      '2024-06-01T00:00:00'),
('CUST-011', 'NorthWest Supply',  'SMB',        'Portland',     '2024-06-01T00:00:00'),
('CUST-012', 'Central Logistics', 'Enterprise', 'Minneapolis',  '2024-06-01T00:00:00'),
('CUST-013', 'Pacific Trading',   'Mid-Market', 'Los Angeles',  '2024-06-01T00:00:00'),
('CUST-014', 'Eastern Imports',   'SMB',        'New York',     '2024-06-01T00:00:00'),
('CUST-015', 'Mountain Gear',     'SMB',        'Denver',       '2024-06-01T00:00:00'),
('CUST-016', 'Southern Comfort',  'Mid-Market', 'Dallas',       '2024-06-01T00:00:00'),
('CUST-017', 'Lake Shore Inc',    'Enterprise', 'Chicago',      '2024-06-01T00:00:00'),
('CUST-018', 'Coastal Freight',   'Mid-Market', 'Miami',        '2024-06-01T00:00:00'),
('CUST-019', 'Summit Solutions',  'SMB',        'Seattle',      '2024-06-01T00:00:00'),
('CUST-020', 'Prairie Goods',     'Mid-Market', 'Minneapolis',  '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_customers;


-- ===================== SEED DATA: SHIPMENT EVENTS (68 rows) =====================
-- Multiple events per shipment (picked_up, in_transit, at_hub, out_for_delivery, delivered/exception)
-- 6 carriers, various origin/destination pairs. Some late, some exceptions.

INSERT INTO {{zone_prefix}}.bronze.raw_events VALUES
-- Shipment S001: NYC->CHI, SwiftFreight, on time
('EVT-001', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'picked_up',        '2024-04-01T08:00:00', '2024-04-01', '2024-04-04', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-002', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'in_transit',        '2024-04-01T18:00:00', '2024-04-01', '2024-04-04', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-003', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'at_hub',            '2024-04-02T14:00:00', '2024-04-01', '2024-04-04', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-004', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'out_for_delivery',  '2024-04-03T06:00:00', '2024-04-01', '2024-04-04', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-005', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'delivered',          '2024-04-03T14:30:00', '2024-04-01', '2024-04-04', '2024-04-03', 250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),

-- Shipment S002: LAX->DAL, GlobalExpress, on time
('EVT-006', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'picked_up',        '2024-04-02T10:00:00', '2024-04-02', '2024-04-04', NULL,          85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),
('EVT-007', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'in_transit',        '2024-04-02T22:00:00', '2024-04-02', '2024-04-04', NULL,          85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),
('EVT-008', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'delivered',          '2024-04-04T09:00:00', '2024-04-02', '2024-04-04', '2024-04-04', 85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),

-- Shipment S003: MIA->ATL, RegionalHaul, on time
('EVT-009', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'picked_up',        '2024-04-03T07:00:00', '2024-04-03', '2024-04-05', NULL,          180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),
('EVT-010', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'in_transit',        '2024-04-03T15:00:00', '2024-04-03', '2024-04-05', NULL,          180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),
('EVT-011', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'delivered',          '2024-04-04T16:00:00', '2024-04-03', '2024-04-05', '2024-04-04', 180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),

-- Shipment S004: SEA->LAX, AeroFreight, on time (air)
('EVT-012', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'picked_up',        '2024-04-05T06:00:00', '2024-04-05', '2024-04-06', NULL,          40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),
('EVT-013', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'in_transit',        '2024-04-05T12:00:00', '2024-04-05', '2024-04-06', NULL,          40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),
('EVT-014', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'delivered',          '2024-04-06T08:00:00', '2024-04-05', '2024-04-06', '2024-04-06', 40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),

-- Shipment S005: CHI->MSP, RailConnect, LATE (promised Apr 9, delivered Apr 11)
('EVT-015', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'picked_up',        '2024-04-07T09:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-016', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'in_transit',        '2024-04-07T20:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-017', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'at_hub',            '2024-04-09T08:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-018', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'delivered',          '2024-04-11T10:00:00', '2024-04-07', '2024-04-09', '2024-04-11', 520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),

-- Shipment S006: DAL->PHX, SwiftFreight, on time
('EVT-019', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'picked_up',        '2024-04-08T11:00:00', '2024-04-08', '2024-04-11', NULL,          310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),
('EVT-020', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'in_transit',        '2024-04-08T22:00:00', '2024-04-08', '2024-04-11', NULL,          310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),
('EVT-021', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'delivered',          '2024-04-10T15:00:00', '2024-04-08', '2024-04-11', '2024-04-10', 310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),

-- Shipment S007: BOS->MIA, GlobalExpress, LATE
('EVT-022', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'picked_up',        '2024-04-10T07:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-023', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'in_transit',        '2024-04-10T18:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-024', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'exception',          '2024-04-12T09:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-025', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'delivered',          '2024-04-13T11:00:00', '2024-04-10', '2024-04-12', '2024-04-13', 65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),

-- Shipment S008: DEN->SEA, SwiftFreight, on time
('EVT-026', 'S008', 'CUST-008', 'CAR-001', 'LOC-DEN', 'LOC-SEA', 'picked_up',        '2024-04-12T08:00:00', '2024-04-12', '2024-04-15', NULL,          195.00, 0.9200, 360.75, 490.00, '2024-06-01T00:00:00'),
('EVT-027', 'S008', 'CUST-008', 'CAR-001', 'LOC-DEN', 'LOC-SEA', 'in_transit',        '2024-04-12T20:00:00', '2024-04-12', '2024-04-15', NULL,          195.00, 0.9200, 360.75, 490.00, '2024-06-01T00:00:00'),
('EVT-028', 'S008', 'CUST-008', 'CAR-001', 'LOC-DEN', 'LOC-SEA', 'delivered',          '2024-04-14T12:00:00', '2024-04-12', '2024-04-15', '2024-04-14', 195.00, 0.9200, 360.75, 490.00, '2024-06-01T00:00:00'),

-- Shipment S009: ATL->NYC, OceanLink, on time (truck mode but slow)
('EVT-029', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'picked_up',        '2024-04-14T10:00:00', '2024-04-14', '2024-04-20', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-030', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'in_transit',        '2024-04-15T06:00:00', '2024-04-14', '2024-04-20', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-031', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'at_hub',            '2024-04-17T14:00:00', '2024-04-14', '2024-04-20', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-032', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'delivered',          '2024-04-19T09:00:00', '2024-04-14', '2024-04-20', '2024-04-19', 800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),

-- Shipment S010: PHX->DEN, RegionalHaul, on time
('EVT-033', 'S010', 'CUST-010', 'CAR-006', 'LOC-PHX', 'LOC-DEN', 'picked_up',        '2024-04-15T09:00:00', '2024-04-15', '2024-04-18', NULL,          145.00, 0.6800, 174.00, 260.00, '2024-06-01T00:00:00'),
('EVT-034', 'S010', 'CUST-010', 'CAR-006', 'LOC-PHX', 'LOC-DEN', 'delivered',          '2024-04-17T16:00:00', '2024-04-15', '2024-04-18', '2024-04-17', 145.00, 0.6800, 174.00, 260.00, '2024-06-01T00:00:00'),

-- Shipment S011: PDX->CHI, RailConnect, LATE
('EVT-035', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'picked_up',        '2024-04-16T07:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-036', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'in_transit',        '2024-04-16T18:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-037', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'at_hub',            '2024-04-19T10:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-038', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'delivered',          '2024-04-22T14:00:00', '2024-04-16', '2024-04-20', '2024-04-22', 420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),

-- Shipment S012: NYC->LAX, AeroFreight, on time (air)
('EVT-039', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'picked_up',        '2024-04-18T06:00:00', '2024-04-18', '2024-04-19', NULL,          25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),
('EVT-040', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'delivered',          '2024-04-19T10:00:00', '2024-04-18', '2024-04-19', '2024-04-19', 25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),

-- Shipment S013: MSP->DAL, SwiftFreight, on time
('EVT-041', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'picked_up',        '2024-04-19T11:00:00', '2024-04-19', '2024-04-22', NULL,          380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),
('EVT-042', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'in_transit',        '2024-04-19T22:00:00', '2024-04-19', '2024-04-22', NULL,          380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),
('EVT-043', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'delivered',          '2024-04-21T15:00:00', '2024-04-19', '2024-04-22', '2024-04-21', 380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),

-- Shipment S014: LAX->SEA, RegionalHaul, exception then delivered late
('EVT-044', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'picked_up',        '2024-04-20T08:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-045', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'in_transit',        '2024-04-20T20:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-046', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'exception',          '2024-04-22T10:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-047', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'delivered',          '2024-04-24T14:00:00', '2024-04-20', '2024-04-23', '2024-04-24', 220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),

-- Shipment S015: CHI->BOS, GlobalExpress, on time
('EVT-048', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'picked_up',        '2024-04-22T09:00:00', '2024-04-22', '2024-04-24', NULL,          55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),
('EVT-049', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'in_transit',        '2024-04-22T18:00:00', '2024-04-22', '2024-04-24', NULL,          55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),
('EVT-050', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'delivered',          '2024-04-23T16:00:00', '2024-04-22', '2024-04-24', '2024-04-23', 55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),

-- Shipment S016: MIA->DAL, OceanLink, on time
('EVT-051', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'picked_up',        '2024-04-23T10:00:00', '2024-04-23', '2024-04-28', NULL,          600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),
('EVT-052', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'in_transit',        '2024-04-24T06:00:00', '2024-04-23', '2024-04-28', NULL,          600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),
('EVT-053', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'delivered',          '2024-04-27T12:00:00', '2024-04-23', '2024-04-28', '2024-04-27', 600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),

-- Shipment S017: DEN->PDX, SwiftFreight, on time
('EVT-054', 'S017', 'CUST-015', 'CAR-001', 'LOC-DEN', 'LOC-PDX', 'picked_up',        '2024-04-25T07:00:00', '2024-04-25', '2024-04-28', NULL,          170.00, 0.7500, 314.50, 420.00, '2024-06-01T00:00:00'),
('EVT-055', 'S017', 'CUST-015', 'CAR-001', 'LOC-DEN', 'LOC-PDX', 'in_transit',        '2024-04-25T18:00:00', '2024-04-25', '2024-04-28', NULL,          170.00, 0.7500, 314.50, 420.00, '2024-06-01T00:00:00'),
('EVT-056', 'S017', 'CUST-015', 'CAR-001', 'LOC-DEN', 'LOC-PDX', 'delivered',          '2024-04-27T10:00:00', '2024-04-25', '2024-04-28', '2024-04-27', 170.00, 0.7500, 314.50, 420.00, '2024-06-01T00:00:00'),

-- Shipment S018: SEA->MSP, RailConnect, on time
('EVT-057', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'picked_up',        '2024-04-26T08:00:00', '2024-04-26', '2024-04-30', NULL,          450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),
('EVT-058', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'in_transit',        '2024-04-26T20:00:00', '2024-04-26', '2024-04-30', NULL,          450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),
('EVT-059', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'delivered',          '2024-04-29T14:00:00', '2024-04-26', '2024-04-30', '2024-04-29', 450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),

-- Shipment S019: ATL->MIA, RegionalHaul, exception (not yet delivered)
('EVT-060', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'picked_up',        '2024-04-28T09:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),
('EVT-061', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'in_transit',        '2024-04-28T18:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),
('EVT-062', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'exception',          '2024-04-30T10:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),

-- Shipment S020: NYC->DEN, AeroFreight, on time
('EVT-063', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'picked_up',        '2024-04-29T06:00:00', '2024-04-29', '2024-04-30', NULL,          30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),
('EVT-064', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'in_transit',        '2024-04-29T14:00:00', '2024-04-29', '2024-04-30', NULL,          30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),
('EVT-065', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'delivered',          '2024-04-30T08:00:00', '2024-04-29', '2024-04-30', '2024-04-30', 30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),

-- Shipment S021: DAL->ATL, GlobalExpress, on time
('EVT-066', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'picked_up',        '2024-04-30T07:00:00', '2024-04-30', '2024-05-02', NULL,          90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00'),
('EVT-067', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'in_transit',        '2024-04-30T16:00:00', '2024-04-30', '2024-05-02', NULL,          90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00'),
('EVT-068', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'delivered',          '2024-05-01T14:00:00', '2024-04-30', '2024-05-02', '2024-05-01', 90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 68
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_events;

