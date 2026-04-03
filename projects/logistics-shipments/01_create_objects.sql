-- =============================================================================
-- Logistics Shipments Pipeline: Object Creation & Seed Data
-- =============================================================================
-- Event sourcing pipeline for a global logistics company. Tracking events
-- arrive from 8 carriers with duplicates and out-of-order timestamps.
-- Implements idempotent composite-key MERGE, timeline reconstruction,
-- SLA violation detection, and Z-ordered route optimization.
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Global logistics shipment tracking zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw tracking events, carrier reference data, SLA contracts';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Deduplicated events, reconstructed timelines, SLA violations';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for delivery performance and SLA compliance analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_carriers (
    carrier_id       STRING      NOT NULL,
    carrier_name     STRING      NOT NULL,
    carrier_type     STRING,
    fleet_size       INT,
    headquarters     STRING,
    on_time_rating   DECIMAL(3,2),
    cost_per_kg      DECIMAL(6,2),
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_carriers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_locations (
    location_id      STRING      NOT NULL,
    hub_name         STRING      NOT NULL,
    city             STRING,
    state            STRING,
    country          STRING,
    region           STRING,
    hub_type         STRING,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_locations';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_customers (
    customer_id      STRING      NOT NULL,
    customer_name    STRING      NOT NULL,
    tier             STRING,
    industry         STRING,
    city             STRING,
    country          STRING,
    account_manager  STRING,
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_customers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_sla_contracts (
    sla_id           STRING      NOT NULL,
    carrier_id       STRING      NOT NULL,
    service_level    STRING      NOT NULL,
    max_transit_days INT         NOT NULL,
    penalty_per_day  DECIMAL(8,2),
    contract_start   DATE,
    contract_end     DATE,
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_sla_contracts';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_tracking_events (
    event_id         STRING      NOT NULL,
    shipment_id      STRING      NOT NULL,
    customer_id      STRING,
    carrier_id       STRING,
    origin_id        STRING,
    destination_id   STRING,
    service_level    STRING,
    event_type       STRING      NOT NULL,
    event_timestamp  TIMESTAMP   NOT NULL,
    ship_date        DATE,
    promised_date    DATE,
    delivery_date    DATE,
    weight_kg        DECIMAL(8,2),
    volume_m3        DECIMAL(8,4),
    cost             DECIMAL(10,2),
    revenue          DECIMAL(10,2),
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/logistics/bronze/raw_tracking_events';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.events_deduped (
    event_id         STRING      NOT NULL,
    shipment_id      STRING      NOT NULL,
    customer_id      STRING,
    carrier_id       STRING,
    origin_id        STRING,
    destination_id   STRING,
    service_level    STRING,
    event_type       STRING      NOT NULL,
    event_timestamp  TIMESTAMP   NOT NULL,
    ship_date        DATE,
    promised_date    DATE,
    delivery_date    DATE,
    weight_kg        DECIMAL(8,2),
    volume_m3        DECIMAL(8,4),
    cost             DECIMAL(10,2),
    revenue          DECIMAL(10,2),
    deduped_at       TIMESTAMP
) LOCATION '{{data_path}}/logistics/silver/events_deduped'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.shipment_status (
    shipment_id        STRING      NOT NULL,
    customer_id        STRING,
    carrier_id         STRING,
    origin_id          STRING,
    destination_id     STRING,
    service_level      STRING,
    ship_date          DATE,
    promised_date      DATE,
    delivery_date      DATE,
    latest_status      STRING,
    previous_status    STRING,
    event_count        INT,
    hours_in_last_stage DECIMAL(8,2),
    total_transit_hours DECIMAL(8,2),
    transit_days       INT,
    on_time_flag       BOOLEAN,
    weight_kg          DECIMAL(8,2),
    volume_m3          DECIMAL(8,4),
    cost               DECIMAL(10,2),
    revenue            DECIMAL(10,2),
    first_event_time   TIMESTAMP,
    last_event_time    TIMESTAMP,
    reconstructed_at   TIMESTAMP
) LOCATION '{{data_path}}/logistics/silver/shipment_status';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.sla_violations (
    violation_id       STRING      NOT NULL,
    shipment_id        STRING      NOT NULL,
    carrier_id         STRING      NOT NULL,
    service_level      STRING,
    sla_max_days       INT,
    actual_transit_days INT,
    days_over_sla      INT,
    penalty_amount     DECIMAL(10,2),
    customer_id        STRING,
    origin_id          STRING,
    destination_id     STRING,
    sla_violated       BOOLEAN     NOT NULL,
    detected_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/silver/sla_violations';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_carrier (
    carrier_key      STRING      NOT NULL,
    carrier_name     STRING,
    carrier_type     STRING,
    fleet_size       INT,
    headquarters     STRING,
    on_time_rating   DECIMAL(3,2),
    cost_per_kg      DECIMAL(6,2),
    loaded_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/dim_carrier';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_location (
    location_key     STRING      NOT NULL,
    hub_name         STRING,
    city             STRING,
    state            STRING,
    country          STRING,
    region           STRING,
    hub_type         STRING,
    latitude         DECIMAL(9,6),
    longitude        DECIMAL(9,6),
    loaded_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/dim_location';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_customer (
    customer_key     STRING      NOT NULL,
    customer_name    STRING,
    tier             STRING,
    industry         STRING,
    city             STRING,
    country          STRING,
    account_manager  STRING,
    loaded_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/dim_customer';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_route (
    route_key         STRING      NOT NULL,
    origin_hub        STRING,
    origin_city       STRING,
    destination_hub   STRING,
    destination_city  STRING,
    distance_km       INT,
    avg_transit_days  DECIMAL(5,1),
    shipment_count    INT,
    primary_mode      STRING,
    loaded_at         TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/dim_route';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_shipments (
    shipment_key      STRING      NOT NULL,
    carrier_key       STRING,
    origin_key        STRING,
    destination_key   STRING,
    customer_key      STRING,
    route_key         STRING,
    service_level     STRING,
    ship_date         DATE,
    delivery_date     DATE,
    promised_date     DATE,
    weight_kg         DECIMAL(8,2),
    volume_m3         DECIMAL(8,4),
    cost              DECIMAL(10,2),
    revenue           DECIMAL(10,2),
    margin            DECIMAL(10,2),
    on_time_flag      BOOLEAN,
    transit_days      INT,
    event_count       INT,
    sla_violated      BOOLEAN,
    penalty_amount    DECIMAL(10,2),
    loaded_at         TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/fact_shipments';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_delivery_performance (
    carrier_name     STRING,
    route            STRING,
    month            STRING,
    total_shipments  INT,
    on_time_count    INT,
    on_time_pct      DECIMAL(5,2),
    avg_transit_days DECIMAL(5,1),
    avg_cost_per_kg  DECIMAL(8,2),
    total_weight     DECIMAL(12,2),
    total_revenue    DECIMAL(12,2),
    total_margin     DECIMAL(12,2),
    loaded_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/kpi_delivery_performance';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_sla_compliance (
    carrier_name     STRING,
    service_level    STRING,
    month            STRING,
    total_shipments  INT,
    violated_count   INT,
    violation_rate   DECIMAL(5,2),
    total_penalty    DECIMAL(12,2),
    avg_days_over    DECIMAL(5,1),
    worst_violation  INT,
    loaded_at        TIMESTAMP
) LOCATION '{{data_path}}/logistics/gold/kpi_sla_compliance';

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_carriers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_locations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_customers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_sla_contracts TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_tracking_events TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.events_deduped TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.shipment_status TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.sla_violations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_carrier TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_location TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_customer TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_route TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_shipments TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_delivery_performance TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_sla_compliance TO USER {{current_user}};

-- ===================== SEED DATA: CARRIERS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_carriers VALUES
('CAR-001', 'SwiftFreight',    'LTL',         120, 'Dallas',       0.92, 1.85, '2024-06-01T00:00:00'),
('CAR-002', 'GlobalExpress',   'Express',      80, 'Memphis',      0.95, 3.20, '2024-06-01T00:00:00'),
('CAR-003', 'OceanLink',       'Ocean',        40, 'Long Beach',   0.88, 0.45, '2024-06-01T00:00:00'),
('CAR-004', 'RailConnect',     'Rail',          60, 'Chicago',      0.90, 0.75, '2024-06-01T00:00:00'),
('CAR-005', 'AeroFreight',     'Air',           30, 'Miami',        0.94, 5.50, '2024-06-01T00:00:00'),
('CAR-006', 'RegionalHaul',    'Regional LTL', 200, 'Atlanta',      0.87, 1.20, '2024-06-01T00:00:00'),
('CAR-007', 'NorthStar Cargo', 'LTL',          90, 'Minneapolis',  0.91, 1.55, '2024-06-01T00:00:00'),
('CAR-008', 'PacificWave',     'Ocean',        55, 'Seattle',      0.86, 0.52, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_carriers;

-- ===================== SEED DATA: LOCATIONS (15 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_locations VALUES
('LOC-NYC', 'NYC Metro Hub',     'New York',      'NY', 'US', 'Northeast', 'distribution', 40.712800, -74.006000, '2024-06-01T00:00:00'),
('LOC-CHI', 'Chicago Central',   'Chicago',       'IL', 'US', 'Midwest',   'distribution', 41.878100, -87.629800, '2024-06-01T00:00:00'),
('LOC-LAX', 'LA Port Gateway',   'Los Angeles',   'CA', 'US', 'West',      'port',         34.052200, -118.243700,'2024-06-01T00:00:00'),
('LOC-MIA', 'Miami Intl Hub',    'Miami',         'FL', 'US', 'Southeast', 'port',         25.761700, -80.191800, '2024-06-01T00:00:00'),
('LOC-DAL', 'DFW Logistics',     'Dallas',        'TX', 'US', 'South',     'distribution', 32.776700, -96.797000, '2024-06-01T00:00:00'),
('LOC-SEA', 'Seattle Pacific',   'Seattle',       'WA', 'US', 'Northwest', 'port',         47.606200, -122.332100,'2024-06-01T00:00:00'),
('LOC-ATL', 'Atlanta Cross-Dock','Atlanta',       'GA', 'US', 'Southeast', 'cross-dock',   33.749000, -84.388000, '2024-06-01T00:00:00'),
('LOC-DEN', 'Denver Mountain',   'Denver',        'CO', 'US', 'Mountain',  'warehouse',    39.739200, -104.990300,'2024-06-01T00:00:00'),
('LOC-BOS', 'Boston NE Hub',     'Boston',        'MA', 'US', 'Northeast', 'warehouse',    42.360100, -71.058900, '2024-06-01T00:00:00'),
('LOC-PHX', 'Phoenix Southwest', 'Phoenix',       'AZ', 'US', 'Southwest', 'warehouse',    33.448400, -112.074000,'2024-06-01T00:00:00'),
('LOC-PDX', 'Portland NW Depot', 'Portland',      'OR', 'US', 'Northwest', 'warehouse',    45.515200, -122.678400,'2024-06-01T00:00:00'),
('LOC-MSP', 'Twin Cities Hub',   'Minneapolis',   'MN', 'US', 'Midwest',   'distribution', 44.977800, -93.265000, '2024-06-01T00:00:00'),
('LOC-HOU', 'Houston Gulf',      'Houston',       'TX', 'US', 'South',     'port',         29.760400, -95.369800, '2024-06-01T00:00:00'),
('LOC-SLC', 'Salt Lake Depot',   'Salt Lake City','UT', 'US', 'Mountain',  'warehouse',    40.760800, -111.891000,'2024-06-01T00:00:00'),
('LOC-DET', 'Detroit Auto Hub',  'Detroit',       'MI', 'US', 'Midwest',   'cross-dock',   42.331400, -83.045800, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_locations;

-- ===================== SEED DATA: CUSTOMERS (20 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_customers VALUES
('CUST-001', 'Acme Corp',          'Enterprise', 'Manufacturing', 'New York',      'US', 'Alice Chen',     '2024-06-01T00:00:00'),
('CUST-002', 'TechStart Inc',      'SMB',        'Technology',    'San Francisco', 'US', 'Bob Rivera',     '2024-06-01T00:00:00'),
('CUST-003', 'Global Retail',      'Enterprise', 'Retail',        'Chicago',       'US', 'Alice Chen',     '2024-06-01T00:00:00'),
('CUST-004', 'FreshFoods LLC',     'Mid-Market', 'Food & Bev',    'Miami',         'US', 'Carol Park',     '2024-06-01T00:00:00'),
('CUST-005', 'AutoParts Direct',   'Enterprise', 'Automotive',    'Dallas',        'US', 'Dan Lee',        '2024-06-01T00:00:00'),
('CUST-006', 'HomeGoods Plus',     'SMB',        'Retail',        'Seattle',       'US', 'Eve Johnson',    '2024-06-01T00:00:00'),
('CUST-007', 'MedSupply Co',       'Enterprise', 'Healthcare',    'Atlanta',       'US', 'Carol Park',     '2024-06-01T00:00:00'),
('CUST-008', 'BuildRight Ltd',     'Mid-Market', 'Construction',  'Denver',        'US', 'Frank Gomez',    '2024-06-01T00:00:00'),
('CUST-009', 'ElectroWare',        'SMB',        'Electronics',   'Boston',        'US', 'Bob Rivera',     '2024-06-01T00:00:00'),
('CUST-010', 'SunBelt Dist',       'Mid-Market', 'Distribution',  'Phoenix',       'US', 'Dan Lee',        '2024-06-01T00:00:00'),
('CUST-011', 'NorthWest Supply',   'SMB',        'Industrial',    'Portland',      'US', 'Eve Johnson',    '2024-06-01T00:00:00'),
('CUST-012', 'Central Logistics',  'Enterprise', 'Logistics',     'Minneapolis',   'US', 'Alice Chen',     '2024-06-01T00:00:00'),
('CUST-013', 'Pacific Trading',    'Mid-Market', 'Import/Export', 'Los Angeles',   'US', 'Frank Gomez',    '2024-06-01T00:00:00'),
('CUST-014', 'Eastern Imports',    'SMB',        'Import/Export', 'New York',      'US', 'Bob Rivera',     '2024-06-01T00:00:00'),
('CUST-015', 'Mountain Gear',      'SMB',        'Outdoor',       'Denver',        'US', 'Frank Gomez',    '2024-06-01T00:00:00'),
('CUST-016', 'Southern Comfort',   'Mid-Market', 'HVAC',          'Dallas',        'US', 'Dan Lee',        '2024-06-01T00:00:00'),
('CUST-017', 'Lake Shore Inc',     'Enterprise', 'Manufacturing', 'Chicago',       'US', 'Alice Chen',     '2024-06-01T00:00:00'),
('CUST-018', 'Coastal Freight',    'Mid-Market', 'Logistics',     'Miami',         'US', 'Carol Park',     '2024-06-01T00:00:00'),
('CUST-019', 'Summit Solutions',   'SMB',        'Consulting',    'Seattle',       'US', 'Eve Johnson',    '2024-06-01T00:00:00'),
('CUST-020', 'Prairie Goods',      'Mid-Market', 'Agriculture',   'Minneapolis',   'US', 'Dan Lee',        '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_customers;

-- ===================== SEED DATA: SLA CONTRACTS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_sla_contracts VALUES
('SLA-001', 'CAR-001', 'standard',  5, 150.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-002', 'CAR-002', 'express',   2, 500.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-003', 'CAR-003', 'economy',  14, 75.00,  '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-004', 'CAR-004', 'standard',  7, 120.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-005', 'CAR-005', 'express',   1, 800.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-006', 'CAR-006', 'standard',  4, 200.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-007', 'CAR-007', 'standard',  5, 150.00, '2024-01-01', '2024-12-31', '2024-06-01T00:00:00'),
('SLA-008', 'CAR-008', 'economy',  12, 80.00,  '2024-01-01', '2024-12-31', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_sla_contracts;

-- ===================== SEED DATA: TRACKING EVENTS (80 rows) =====================
-- 25 shipments across 8 carriers. Includes:
--   15 exact duplicates (same shipment_id + event_type + event_timestamp)
--   5 out-of-order events (delivered timestamp before in_transit)
--   3 SLA violations (S005, S011, S014)
--   2 exceptions (S019 lost, S024 damaged)
-- Event types: created, picked_up, in_transit, at_hub, customs_hold,
--              out_for_delivery, delivered, exception

INSERT INTO {{zone_prefix}}.bronze.raw_tracking_events VALUES
-- === S001: NYC->CHI, SwiftFreight standard, on time (4 events) ===
('EVT-001', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'standard', 'created',          '2024-04-01T06:00:00', '2024-04-01', '2024-04-05', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-002', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'standard', 'picked_up',        '2024-04-01T08:00:00', '2024-04-01', '2024-04-05', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-003', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'standard', 'in_transit',        '2024-04-01T18:00:00', '2024-04-01', '2024-04-05', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-004', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'standard', 'delivered',         '2024-04-03T14:30:00', '2024-04-01', '2024-04-05', '2024-04-03', 250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),

-- === S002: LAX->DAL, GlobalExpress express, on time (3 events) ===
('EVT-005', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'express',  'created',          '2024-04-02T09:00:00', '2024-04-02', '2024-04-04', NULL,          85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),
('EVT-006', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'express',  'picked_up',        '2024-04-02T10:00:00', '2024-04-02', '2024-04-04', NULL,          85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),
('EVT-007', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'express',  'delivered',         '2024-04-03T16:00:00', '2024-04-02', '2024-04-04', '2024-04-03', 85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),

-- === S003: MIA->ATL, RegionalHaul standard, on time (3 events) ===
('EVT-008', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'standard', 'created',          '2024-04-03T07:00:00', '2024-04-03', '2024-04-06', NULL,          180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),
('EVT-009', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'standard', 'in_transit',        '2024-04-03T15:00:00', '2024-04-03', '2024-04-06', NULL,          180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),
('EVT-010', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'standard', 'delivered',         '2024-04-04T16:00:00', '2024-04-03', '2024-04-06', '2024-04-04', 180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),

-- === S004: SEA->LAX, AeroFreight express, on time (3 events) ===
('EVT-011', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'express',  'created',          '2024-04-05T06:00:00', '2024-04-05', '2024-04-06', NULL,          40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),
('EVT-012', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'express',  'picked_up',        '2024-04-05T08:00:00', '2024-04-05', '2024-04-06', NULL,          40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),
('EVT-013', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'express',  'delivered',         '2024-04-05T22:00:00', '2024-04-05', '2024-04-06', '2024-04-05', 40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),

-- === S005: CHI->MSP, RailConnect standard, SLA VIOLATION (transit 6 days, SLA 7 but promised Apr 9) ===
('EVT-014', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'standard', 'created',          '2024-04-07T08:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-015', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'standard', 'picked_up',        '2024-04-07T09:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-016', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'standard', 'at_hub',            '2024-04-09T08:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),
('EVT-017', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'standard', 'delivered',         '2024-04-13T10:00:00', '2024-04-07', '2024-04-09', '2024-04-13', 520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),

-- === DUPLICATE BLOCK 1: exact dupes of S001 created and S002 created (2 dupes) ===
('EVT-001', 'S001', 'CUST-001', 'CAR-001', 'LOC-NYC', 'LOC-CHI', 'standard', 'created',          '2024-04-01T06:00:00', '2024-04-01', '2024-04-05', NULL,          250.00, 1.2000, 462.50, 625.00, '2024-06-01T00:00:00'),
('EVT-005', 'S002', 'CUST-013', 'CAR-002', 'LOC-LAX', 'LOC-DAL', 'express',  'created',          '2024-04-02T09:00:00', '2024-04-02', '2024-04-04', NULL,          85.00,  0.4500, 272.00, 380.00, '2024-06-01T00:00:00'),

-- === S006: DAL->PHX, SwiftFreight standard, on time (3 events) ===
('EVT-018', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'standard', 'created',          '2024-04-08T10:00:00', '2024-04-08', '2024-04-12', NULL,          310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),
('EVT-019', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'standard', 'in_transit',        '2024-04-08T22:00:00', '2024-04-08', '2024-04-12', NULL,          310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),
('EVT-020', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'standard', 'delivered',         '2024-04-10T15:00:00', '2024-04-08', '2024-04-12', '2024-04-10', 310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),

-- === S007: BOS->MIA, GlobalExpress express, late with exception (4 events) ===
('EVT-021', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'express',  'created',          '2024-04-10T07:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-022', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'express',  'in_transit',        '2024-04-10T18:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-023', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'express',  'exception',         '2024-04-12T09:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-024', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'express',  'delivered',         '2024-04-13T11:00:00', '2024-04-10', '2024-04-12', '2024-04-13', 65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),

-- === DUPLICATE BLOCK 2: exact dupes of S003 delivered, S004 delivered, S005 picked_up (3 dupes) ===
('EVT-010', 'S003', 'CUST-004', 'CAR-006', 'LOC-MIA', 'LOC-ATL', 'standard', 'delivered',         '2024-04-04T16:00:00', '2024-04-03', '2024-04-06', '2024-04-04', 180.00, 0.8500, 216.00, 340.00, '2024-06-01T00:00:00'),
('EVT-013', 'S004', 'CUST-006', 'CAR-005', 'LOC-SEA', 'LOC-LAX', 'express',  'delivered',         '2024-04-05T22:00:00', '2024-04-05', '2024-04-06', '2024-04-05', 40.00,  0.1800, 220.00, 320.00, '2024-06-01T00:00:00'),
('EVT-015', 'S005', 'CUST-003', 'CAR-004', 'LOC-CHI', 'LOC-MSP', 'standard', 'picked_up',        '2024-04-07T09:00:00', '2024-04-07', '2024-04-09', NULL,          520.00, 2.5000, 390.00, 580.00, '2024-06-01T00:00:00'),

-- === S008: DEN->SEA, NorthStar standard, on time (3 events) ===
('EVT-025', 'S008', 'CUST-008', 'CAR-007', 'LOC-DEN', 'LOC-SEA', 'standard', 'created',          '2024-04-12T08:00:00', '2024-04-12', '2024-04-16', NULL,          195.00, 0.9200, 302.25, 490.00, '2024-06-01T00:00:00'),
('EVT-026', 'S008', 'CUST-008', 'CAR-007', 'LOC-DEN', 'LOC-SEA', 'standard', 'in_transit',        '2024-04-12T20:00:00', '2024-04-12', '2024-04-16', NULL,          195.00, 0.9200, 302.25, 490.00, '2024-06-01T00:00:00'),
('EVT-027', 'S008', 'CUST-008', 'CAR-007', 'LOC-DEN', 'LOC-SEA', 'standard', 'delivered',         '2024-04-14T12:00:00', '2024-04-12', '2024-04-16', '2024-04-14', 195.00, 0.9200, 302.25, 490.00, '2024-06-01T00:00:00'),

-- === S009: ATL->NYC, OceanLink economy, on time (4 events) ===
('EVT-028', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'economy',  'created',          '2024-04-14T09:00:00', '2024-04-14', '2024-04-26', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-029', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'economy',  'in_transit',        '2024-04-15T06:00:00', '2024-04-14', '2024-04-26', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-030', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'economy',  'at_hub',            '2024-04-17T14:00:00', '2024-04-14', '2024-04-26', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-031', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'economy',  'delivered',         '2024-04-19T09:00:00', '2024-04-14', '2024-04-26', '2024-04-19', 800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),

-- === DUPLICATE BLOCK 3: exact dupes of S006 in_transit, S007 in_transit, S008 delivered (3 dupes) ===
('EVT-019', 'S006', 'CUST-005', 'CAR-001', 'LOC-DAL', 'LOC-PHX', 'standard', 'in_transit',        '2024-04-08T22:00:00', '2024-04-08', '2024-04-12', NULL,          310.00, 1.5000, 573.50, 780.00, '2024-06-01T00:00:00'),
('EVT-022', 'S007', 'CUST-009', 'CAR-002', 'LOC-BOS', 'LOC-MIA', 'express',  'in_transit',        '2024-04-10T18:00:00', '2024-04-10', '2024-04-12', NULL,          65.00,  0.3200, 208.00, 295.00, '2024-06-01T00:00:00'),
('EVT-027', 'S008', 'CUST-008', 'CAR-007', 'LOC-DEN', 'LOC-SEA', 'standard', 'delivered',         '2024-04-14T12:00:00', '2024-04-12', '2024-04-16', '2024-04-14', 195.00, 0.9200, 302.25, 490.00, '2024-06-01T00:00:00'),

-- === S010: PHX->DEN, RegionalHaul standard, on time (2 events) ===
('EVT-032', 'S010', 'CUST-010', 'CAR-006', 'LOC-PHX', 'LOC-DEN', 'standard', 'created',          '2024-04-15T09:00:00', '2024-04-15', '2024-04-18', NULL,          145.00, 0.6800, 174.00, 260.00, '2024-06-01T00:00:00'),
('EVT-033', 'S010', 'CUST-010', 'CAR-006', 'LOC-PHX', 'LOC-DEN', 'standard', 'delivered',         '2024-04-17T16:00:00', '2024-04-15', '2024-04-18', '2024-04-17', 145.00, 0.6800, 174.00, 260.00, '2024-06-01T00:00:00'),

-- === S011: PDX->CHI, RailConnect standard, SLA VIOLATION (6 days transit, SLA is 7 but promised Apr 20) ===
('EVT-034', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'standard', 'created',          '2024-04-16T06:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-035', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'standard', 'in_transit',        '2024-04-16T18:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-036', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'standard', 'at_hub',            '2024-04-19T10:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),
('EVT-037', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'standard', 'delivered',         '2024-04-22T14:00:00', '2024-04-16', '2024-04-20', '2024-04-22', 420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),

-- === OUT-OF-ORDER BLOCK 1: S012 delivered event arrives before in_transit ===
('EVT-039', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'express',  'delivered',         '2024-04-19T10:00:00', '2024-04-18', '2024-04-19', '2024-04-19', 25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),
('EVT-038', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'express',  'created',          '2024-04-18T06:00:00', '2024-04-18', '2024-04-19', NULL,          25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),
('EVT-040', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'express',  'in_transit',        '2024-04-18T14:00:00', '2024-04-18', '2024-04-19', NULL,          25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),

-- === DUPLICATE BLOCK 4: exact dupes of S009 at_hub, S010 delivered, S011 in_transit (3 dupes) ===
('EVT-030', 'S009', 'CUST-007', 'CAR-003', 'LOC-ATL', 'LOC-NYC', 'economy',  'at_hub',            '2024-04-17T14:00:00', '2024-04-14', '2024-04-26', NULL,          800.00, 4.0000, 360.00, 520.00, '2024-06-01T00:00:00'),
('EVT-033', 'S010', 'CUST-010', 'CAR-006', 'LOC-PHX', 'LOC-DEN', 'standard', 'delivered',         '2024-04-17T16:00:00', '2024-04-15', '2024-04-18', '2024-04-17', 145.00, 0.6800, 174.00, 260.00, '2024-06-01T00:00:00'),
('EVT-035', 'S011', 'CUST-011', 'CAR-004', 'LOC-PDX', 'LOC-CHI', 'standard', 'in_transit',        '2024-04-16T18:00:00', '2024-04-16', '2024-04-20', NULL,          420.00, 2.1000, 315.00, 470.00, '2024-06-01T00:00:00'),

-- === S013: MSP->DAL, SwiftFreight standard, on time (3 events) ===
('EVT-041', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'standard', 'created',          '2024-04-19T10:00:00', '2024-04-19', '2024-04-23', NULL,          380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),
('EVT-042', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'standard', 'in_transit',        '2024-04-19T22:00:00', '2024-04-19', '2024-04-23', NULL,          380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),
('EVT-043', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'standard', 'delivered',         '2024-04-21T15:00:00', '2024-04-19', '2024-04-23', '2024-04-21', 380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),

-- === S014: LAX->SEA, RegionalHaul standard, SLA VIOLATION + exception then late delivery ===
('EVT-044', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'standard', 'created',          '2024-04-20T07:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-045', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'standard', 'in_transit',        '2024-04-20T20:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-046', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'standard', 'exception',         '2024-04-22T10:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-047', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'standard', 'delivered',         '2024-04-25T14:00:00', '2024-04-20', '2024-04-23', '2024-04-25', 220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),

-- === OUT-OF-ORDER BLOCK 2: S015 at_hub arrives before in_transit ===
('EVT-049', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'express',  'at_hub',            '2024-04-23T06:00:00', '2024-04-22', '2024-04-24', NULL,          55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),
('EVT-048', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'express',  'created',          '2024-04-22T09:00:00', '2024-04-22', '2024-04-24', NULL,          55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),
('EVT-050', 'S015', 'CUST-017', 'CAR-002', 'LOC-CHI', 'LOC-BOS', 'express',  'delivered',         '2024-04-23T16:00:00', '2024-04-22', '2024-04-24', '2024-04-23', 55.00,  0.2500, 176.00, 250.00, '2024-06-01T00:00:00'),

-- === S016: MIA->DAL, OceanLink economy, on time (3 events) ===
('EVT-051', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'economy',  'created',          '2024-04-23T10:00:00', '2024-04-23', '2024-05-05', NULL,          600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),
('EVT-052', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'economy',  'in_transit',        '2024-04-24T06:00:00', '2024-04-23', '2024-05-05', NULL,          600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),
('EVT-053', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'economy',  'delivered',         '2024-04-27T12:00:00', '2024-04-23', '2024-05-05', '2024-04-27', 600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),

-- === DUPLICATE BLOCK 5: exact dupes of S012 delivered, S013 delivered, S014 exception, S016 in_transit (4 dupes) ===
('EVT-039', 'S012', 'CUST-014', 'CAR-005', 'LOC-NYC', 'LOC-LAX', 'express',  'delivered',         '2024-04-19T10:00:00', '2024-04-18', '2024-04-19', '2024-04-19', 25.00,  0.1000, 137.50, 200.00, '2024-06-01T00:00:00'),
('EVT-043', 'S013', 'CUST-012', 'CAR-001', 'LOC-MSP', 'LOC-DAL', 'standard', 'delivered',         '2024-04-21T15:00:00', '2024-04-19', '2024-04-23', '2024-04-21', 380.00, 1.8000, 703.00, 950.00, '2024-06-01T00:00:00'),
('EVT-046', 'S014', 'CUST-013', 'CAR-006', 'LOC-LAX', 'LOC-SEA', 'standard', 'exception',         '2024-04-22T10:00:00', '2024-04-20', '2024-04-23', NULL,          220.00, 1.0500, 264.00, 385.00, '2024-06-01T00:00:00'),
('EVT-052', 'S016', 'CUST-018', 'CAR-003', 'LOC-MIA', 'LOC-DAL', 'economy',  'in_transit',        '2024-04-24T06:00:00', '2024-04-23', '2024-05-05', NULL,          600.00, 3.0000, 270.00, 410.00, '2024-06-01T00:00:00'),

-- === S017: DEN->PDX, NorthStar standard, on time (3 events) ===
('EVT-054', 'S017', 'CUST-015', 'CAR-007', 'LOC-DEN', 'LOC-PDX', 'standard', 'created',          '2024-04-25T07:00:00', '2024-04-25', '2024-04-29', NULL,          170.00, 0.7500, 263.50, 420.00, '2024-06-01T00:00:00'),
('EVT-055', 'S017', 'CUST-015', 'CAR-007', 'LOC-DEN', 'LOC-PDX', 'standard', 'in_transit',        '2024-04-25T18:00:00', '2024-04-25', '2024-04-29', NULL,          170.00, 0.7500, 263.50, 420.00, '2024-06-01T00:00:00'),
('EVT-056', 'S017', 'CUST-015', 'CAR-007', 'LOC-DEN', 'LOC-PDX', 'standard', 'delivered',         '2024-04-27T10:00:00', '2024-04-25', '2024-04-29', '2024-04-27', 170.00, 0.7500, 263.50, 420.00, '2024-06-01T00:00:00'),

-- === S018: SEA->MSP, RailConnect standard, on time (3 events) ===
('EVT-057', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'standard', 'created',          '2024-04-26T07:00:00', '2024-04-26', '2024-04-30', NULL,          450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),
('EVT-058', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'standard', 'in_transit',        '2024-04-26T20:00:00', '2024-04-26', '2024-04-30', NULL,          450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),
('EVT-059', 'S018', 'CUST-019', 'CAR-004', 'LOC-SEA', 'LOC-MSP', 'standard', 'delivered',         '2024-04-29T14:00:00', '2024-04-26', '2024-04-30', '2024-04-29', 450.00, 2.2000, 337.50, 500.00, '2024-06-01T00:00:00'),

-- === S019: ATL->MIA, RegionalHaul standard, exception LOST (3 events, no delivery) ===
('EVT-060', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'standard', 'created',          '2024-04-28T08:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),
('EVT-061', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'standard', 'in_transit',        '2024-04-28T18:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),
('EVT-062', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'standard', 'exception',         '2024-04-30T10:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-06-01T00:00:00'),

-- === OUT-OF-ORDER BLOCK 3: S020 out_for_delivery arrives before at_hub ===
('EVT-065', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'express',  'out_for_delivery',  '2024-04-30T06:00:00', '2024-04-29', '2024-04-30', NULL,          30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),
('EVT-063', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'express',  'created',          '2024-04-29T06:00:00', '2024-04-29', '2024-04-30', NULL,          30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),
('EVT-064', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'express',  'in_transit',        '2024-04-29T14:00:00', '2024-04-29', '2024-04-30', NULL,          30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),
('EVT-066', 'S020', 'CUST-001', 'CAR-005', 'LOC-NYC', 'LOC-DEN', 'express',  'delivered',         '2024-04-30T08:00:00', '2024-04-29', '2024-04-30', '2024-04-30', 30.00,  0.1200, 165.00, 240.00, '2024-06-01T00:00:00'),

-- === S021: DAL->ATL, GlobalExpress express, on time (3 events) ===
('EVT-067', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'express',  'created',          '2024-04-30T07:00:00', '2024-04-30', '2024-05-02', NULL,          90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00'),
('EVT-068', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'express',  'in_transit',        '2024-04-30T16:00:00', '2024-04-30', '2024-05-02', NULL,          90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00'),
('EVT-069', 'S021', 'CUST-016', 'CAR-002', 'LOC-DAL', 'LOC-ATL', 'express',  'delivered',         '2024-05-01T14:00:00', '2024-04-30', '2024-05-02', '2024-05-01', 90.00,  0.4000, 288.00, 400.00, '2024-06-01T00:00:00'),

-- === OUT-OF-ORDER BLOCK 4: S022 delivered before in_transit ===
('EVT-072', 'S022', 'CUST-020', 'CAR-008', 'LOC-HOU', 'LOC-SLC', 'economy',  'delivered',         '2024-05-05T11:00:00', '2024-05-01', '2024-05-10', '2024-05-05', 340.00, 1.6000, 176.80, 310.00, '2024-06-01T00:00:00'),
('EVT-070', 'S022', 'CUST-020', 'CAR-008', 'LOC-HOU', 'LOC-SLC', 'economy',  'created',          '2024-05-01T10:00:00', '2024-05-01', '2024-05-10', NULL,          340.00, 1.6000, 176.80, 310.00, '2024-06-01T00:00:00'),
('EVT-071', 'S022', 'CUST-020', 'CAR-008', 'LOC-HOU', 'LOC-SLC', 'economy',  'in_transit',        '2024-05-02T08:00:00', '2024-05-01', '2024-05-10', NULL,          340.00, 1.6000, 176.80, 310.00, '2024-06-01T00:00:00'),

-- === S023: DET->HOU, NorthStar standard, customs hold then delivered (4 events) ===
('EVT-073', 'S023', 'CUST-005', 'CAR-007', 'LOC-DET', 'LOC-HOU', 'standard', 'created',          '2024-05-02T07:00:00', '2024-05-02', '2024-05-06', NULL,          275.00, 1.3000, 426.25, 600.00, '2024-06-01T00:00:00'),
('EVT-074', 'S023', 'CUST-005', 'CAR-007', 'LOC-DET', 'LOC-HOU', 'standard', 'in_transit',        '2024-05-02T16:00:00', '2024-05-02', '2024-05-06', NULL,          275.00, 1.3000, 426.25, 600.00, '2024-06-01T00:00:00'),
('EVT-075', 'S023', 'CUST-005', 'CAR-007', 'LOC-DET', 'LOC-HOU', 'standard', 'customs_hold',      '2024-05-03T14:00:00', '2024-05-02', '2024-05-06', NULL,          275.00, 1.3000, 426.25, 600.00, '2024-06-01T00:00:00'),
('EVT-076', 'S023', 'CUST-005', 'CAR-007', 'LOC-DET', 'LOC-HOU', 'standard', 'delivered',         '2024-05-05T10:00:00', '2024-05-02', '2024-05-06', '2024-05-05', 275.00, 1.3000, 426.25, 600.00, '2024-06-01T00:00:00'),

-- === S024: SLC->DET, PacificWave economy, exception DAMAGED (2 events, no delivery) ===
('EVT-077', 'S024', 'CUST-003', 'CAR-008', 'LOC-SLC', 'LOC-DET', 'economy',  'created',          '2024-05-03T09:00:00', '2024-05-03', '2024-05-14', NULL,          510.00, 2.4000, 265.20, 440.00, '2024-06-01T00:00:00'),
('EVT-078', 'S024', 'CUST-003', 'CAR-008', 'LOC-SLC', 'LOC-DET', 'economy',  'in_transit',        '2024-05-03T18:00:00', '2024-05-03', '2024-05-14', NULL,          510.00, 2.4000, 265.20, 440.00, '2024-06-01T00:00:00'),
('EVT-079', 'S024', 'CUST-003', 'CAR-008', 'LOC-SLC', 'LOC-DET', 'economy',  'exception',         '2024-05-05T12:00:00', '2024-05-03', '2024-05-14', NULL,          510.00, 2.4000, 265.20, 440.00, '2024-06-01T00:00:00'),

-- === S025: MSP->BOS, NorthStar standard, on time with customs (4 events) ===
-- === OUT-OF-ORDER BLOCK 5: customs_hold arrives before in_transit ===
('EVT-082', 'S025', 'CUST-012', 'CAR-007', 'LOC-MSP', 'LOC-BOS', 'standard', 'customs_hold',      '2024-05-06T08:00:00', '2024-05-04', '2024-05-08', NULL,          160.00, 0.7000, 248.00, 380.00, '2024-06-01T00:00:00'),
('EVT-080', 'S025', 'CUST-012', 'CAR-007', 'LOC-MSP', 'LOC-BOS', 'standard', 'created',          '2024-05-04T08:00:00', '2024-05-04', '2024-05-08', NULL,          160.00, 0.7000, 248.00, 380.00, '2024-06-01T00:00:00'),
('EVT-081', 'S025', 'CUST-012', 'CAR-007', 'LOC-MSP', 'LOC-BOS', 'standard', 'in_transit',        '2024-05-04T20:00:00', '2024-05-04', '2024-05-08', NULL,          160.00, 0.7000, 248.00, 380.00, '2024-06-01T00:00:00'),
('EVT-083', 'S025', 'CUST-012', 'CAR-007', 'LOC-MSP', 'LOC-BOS', 'standard', 'delivered',         '2024-05-07T15:00:00', '2024-05-04', '2024-05-08', '2024-05-07', 160.00, 0.7000, 248.00, 380.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 80
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_tracking_events;
