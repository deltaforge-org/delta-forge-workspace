-- =============================================================================
-- Aviation Flight Operations Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw flight operations feeds';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched flight data with derived metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for OTP and route analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_routes (
    route_id           STRING      NOT NULL,
    origin_iata        STRING      NOT NULL,
    origin_city        STRING,
    dest_iata          STRING      NOT NULL,
    dest_city          STRING,
    distance_nm        INT,
    block_time_min     INT,
    domestic_flag      BOOLEAN,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/aviation/bronze/raw_routes';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_aircraft (
    registration       STRING      NOT NULL,
    aircraft_type      STRING,
    seat_capacity      INT,
    range_nm           INT,
    fuel_efficiency    DECIMAL(5,2),
    age_years          INT,
    maintenance_status STRING,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/aviation/bronze/raw_aircraft';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_crew (
    crew_id            STRING      NOT NULL,
    captain_name       STRING,
    first_officer_name STRING,
    crew_base          STRING,
    duty_hours_month   INT,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/aviation/bronze/raw_crew';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_flight_ops (
    flight_number      STRING      NOT NULL,
    departure_date     DATE        NOT NULL,
    route_id           STRING      NOT NULL,
    registration       STRING      NOT NULL,
    crew_id            STRING      NOT NULL,
    scheduled_departure TIMESTAMP,
    actual_departure    TIMESTAMP,
    scheduled_arrival   TIMESTAMP,
    actual_arrival      TIMESTAMP,
    delay_minutes       INT,
    delay_reason        STRING,
    pax_count           INT,
    fuel_consumed_kg    DECIMAL(10,2),
    revenue             DECIMAL(12,2),
    flight_status       STRING,
    ingested_at         TIMESTAMP
) LOCATION '{{data_path}}/aviation/bronze/raw_flight_ops';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.flights_enriched (
    flight_number      STRING      NOT NULL,
    departure_date     DATE        NOT NULL,
    route_id           STRING,
    registration       STRING,
    crew_id            STRING,
    scheduled_departure TIMESTAMP,
    actual_departure    TIMESTAMP,
    scheduled_arrival   TIMESTAMP,
    actual_arrival      TIMESTAMP,
    delay_minutes       INT,
    delay_reason        STRING,
    delay_category      STRING,
    pax_count           INT,
    seat_capacity       INT,
    load_factor         DECIMAL(5,2),
    fuel_consumed_kg    DECIMAL(10,2),
    fuel_per_pax_nm     DECIMAL(8,4),
    revenue             DECIMAL(12,2),
    flight_status       STRING,
    is_on_time          BOOLEAN,
    is_anomaly          BOOLEAN,
    enriched_at         TIMESTAMP
) LOCATION '{{data_path}}/aviation/silver/flights_enriched';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_route (
    route_key          STRING      NOT NULL,
    origin_iata        STRING,
    origin_city        STRING,
    dest_iata          STRING,
    dest_city          STRING,
    distance_nm        INT,
    block_time_min     INT,
    domestic_flag      BOOLEAN
) LOCATION '{{data_path}}/aviation/gold/dim_route';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_aircraft (
    aircraft_key       STRING      NOT NULL,
    registration       STRING,
    aircraft_type      STRING,
    seat_capacity      INT,
    range_nm           INT,
    fuel_efficiency    DECIMAL(5,2),
    age_years          INT,
    maintenance_status STRING
) LOCATION '{{data_path}}/aviation/gold/dim_aircraft';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_crew (
    crew_key           STRING      NOT NULL,
    crew_id            STRING,
    captain_name       STRING,
    first_officer_name STRING,
    crew_base          STRING,
    duty_hours_month   INT
) LOCATION '{{data_path}}/aviation/gold/dim_crew';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_date (
    date_key           STRING      NOT NULL,
    full_date          DATE        NOT NULL,
    day_of_week        STRING,
    month              INT,
    quarter            INT,
    year               INT,
    is_peak_season     BOOLEAN
) LOCATION '{{data_path}}/aviation/gold/dim_date';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_flights (
    flight_key         STRING      NOT NULL,
    route_key          STRING      NOT NULL,
    aircraft_key       STRING      NOT NULL,
    crew_key           STRING      NOT NULL,
    date_key           STRING      NOT NULL,
    flight_number      STRING,
    scheduled_departure TIMESTAMP,
    actual_departure    TIMESTAMP,
    scheduled_arrival   TIMESTAMP,
    actual_arrival      TIMESTAMP,
    delay_minutes       INT,
    delay_reason        STRING,
    pax_count           INT,
    fuel_consumed_kg    DECIMAL(10,2),
    revenue             DECIMAL(12,2)
) LOCATION '{{data_path}}/aviation/gold/fact_flights';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_otp (
    route              STRING      NOT NULL,
    month              STRING      NOT NULL,
    total_flights      INT,
    on_time_count      INT,
    delayed_count      INT,
    cancelled_count    INT,
    otp_pct            DECIMAL(5,2),
    avg_delay_min      DECIMAL(7,2),
    fuel_efficiency_per_pax_nm DECIMAL(8,4),
    revenue_per_asm    DECIMAL(8,4),
    load_factor_pct    DECIMAL(5,2)
) LOCATION '{{data_path}}/aviation/gold/kpi_otp';

-- ===================== GRANTS =====================
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_flight_ops TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_routes TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_aircraft TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_crew TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.flights_enriched TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_flights TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_route TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_aircraft TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_crew TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_date TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_otp TO USER {{current_user}};

-- ===================== PSEUDONYMISATION =====================
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_crew (captain_name) TRANSFORM mask PARAMS (show = 2);
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_crew (first_officer_name) TRANSFORM mask PARAMS (show = 2);

-- ===================== SEED DATA: ROUTES (10 routes) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_routes VALUES
('RT-001', 'JFK', 'New York',     'LAX', 'Los Angeles',  2145, 330, true,  '2024-01-01T00:00:00'),
('RT-002', 'ORD', 'Chicago',      'MIA', 'Miami',        1197, 195, true,  '2024-01-01T00:00:00'),
('RT-003', 'ATL', 'Atlanta',      'DFW', 'Dallas',        721, 135, true,  '2024-01-01T00:00:00'),
('RT-004', 'SFO', 'San Francisco','SEA', 'Seattle',        679, 120, true,  '2024-01-01T00:00:00'),
('RT-005', 'BOS', 'Boston',       'DCA', 'Washington DC',  399,  90, true,  '2024-01-01T00:00:00'),
('RT-006', 'JFK', 'New York',     'LHR', 'London',       3451, 450, false, '2024-01-01T00:00:00'),
('RT-007', 'LAX', 'Los Angeles',  'NRT', 'Tokyo',        4723, 660, false, '2024-01-01T00:00:00'),
('RT-008', 'ORD', 'Chicago',      'CDG', 'Paris',        4141, 540, false, '2024-01-01T00:00:00'),
('RT-009', 'DEN', 'Denver',       'PHX', 'Phoenix',       602, 120, true,  '2024-01-01T00:00:00'),
('RT-010', 'ATL', 'Atlanta',      'MCO', 'Orlando',       403,  90, true,  '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_routes;


-- ===================== SEED DATA: AIRCRAFT (6 aircraft) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_aircraft VALUES
('N101AA', 'Boeing 737-800',    189, 2935, 2.85, 8,  'active',   '2024-01-01T00:00:00'),
('N202BB', 'Boeing 787-9',      290, 7635, 2.40, 4,  'active',   '2024-01-01T00:00:00'),
('N303CC', 'Airbus A320neo',    180, 3400, 2.50, 3,  'active',   '2024-01-01T00:00:00'),
('N404DD', 'Boeing 777-300ER',  396, 7370, 2.95, 12, 'active',   '2024-01-01T00:00:00'),
('N505EE', 'Airbus A321XLR',    220, 4700, 2.35, 1,  'active',   '2024-01-01T00:00:00'),
('N606FF', 'Embraer E175',       76, 2000, 3.10, 6,  'maintenance', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_aircraft;


-- ===================== SEED DATA: CREW (5 crew teams) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_crew VALUES
('CRW-001', 'Captain James Mitchell',    'FO Sarah Clarke',     'JFK', 72,  '2024-01-01T00:00:00'),
('CRW-002', 'Captain Maria Santos',      'FO David Kim',        'ORD', 68,  '2024-01-01T00:00:00'),
('CRW-003', 'Captain Robert Chen',       'FO Emily Watson',     'ATL', 80,  '2024-01-01T00:00:00'),
('CRW-004', 'Captain Lisa Andersson',    'FO Michael O''Brien', 'LAX', 65,  '2024-01-01T00:00:00'),
('CRW-005', 'Captain Thomas Nguyen',     'FO Jennifer Park',    'SFO', 75,  '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_crew;


-- ===================== SEED DATA: FLIGHT OPERATIONS (62 rows, 2 months) =====================
-- Includes delays (weather, mechanical, ATC, crew), cancellations, diversions
INSERT INTO {{zone_prefix}}.bronze.raw_flight_ops VALUES
-- January 2024 flights
('AA100', '2024-01-02', 'RT-001', 'N101AA', 'CRW-001', '2024-01-02T08:00:00', '2024-01-02T08:15:00', '2024-01-02T13:30:00', '2024-01-02T13:50:00', 15, 'ATC',        172, 8450.00,  185000.00, 'arrived',   '2024-01-02T14:00:00'),
('AA101', '2024-01-02', 'RT-002', 'N303CC', 'CRW-002', '2024-01-02T09:00:00', '2024-01-02T09:05:00', '2024-01-02T12:15:00', '2024-01-02T12:20:00',  5, NULL,         165, 4200.00,   92000.00, 'arrived',   '2024-01-02T12:30:00'),
('AA102', '2024-01-02', 'RT-005', 'N606FF', 'CRW-003', '2024-01-02T07:00:00', '2024-01-02T07:00:00', '2024-01-02T08:30:00', '2024-01-02T08:28:00',  0, NULL,          68, 1650.00,   38000.00, 'arrived',   '2024-01-02T08:35:00'),
('AA103', '2024-01-03', 'RT-003', 'N505EE', 'CRW-003', '2024-01-03T11:00:00', '2024-01-03T11:10:00', '2024-01-03T13:15:00', '2024-01-03T13:30:00', 10, 'weather',    198, 3100.00,   75000.00, 'arrived',   '2024-01-03T13:35:00'),
('AA104', '2024-01-03', 'RT-006', 'N202BB', 'CRW-001', '2024-01-03T18:00:00', '2024-01-03T18:20:00', '2024-01-04T06:30:00', '2024-01-04T07:05:00', 35, 'weather',    268, 28500.00,  420000.00, 'arrived',   '2024-01-04T07:10:00'),
('AA105', '2024-01-04', 'RT-004', 'N303CC', 'CRW-005', '2024-01-04T14:00:00', '2024-01-04T14:00:00', '2024-01-04T16:00:00', '2024-01-04T15:55:00',  0, NULL,         162, 2400.00,   56000.00, 'arrived',   '2024-01-04T16:00:00'),
('AA106', '2024-01-05', 'RT-001', 'N101AA', 'CRW-001', '2024-01-05T08:00:00', '2024-01-05T08:05:00', '2024-01-05T13:30:00', '2024-01-05T13:35:00',  5, NULL,         185, 8300.00,  190000.00, 'arrived',   '2024-01-05T13:40:00'),
('AA107', '2024-01-05', 'RT-009', 'N505EE', 'CRW-003', '2024-01-05T10:00:00', '2024-01-05T10:00:00', '2024-01-05T12:00:00', '2024-01-05T11:58:00',  0, NULL,         195, 2100.00,   48000.00, 'arrived',   '2024-01-05T12:05:00'),
('AA108', '2024-01-07', 'RT-010', 'N606FF', 'CRW-003', '2024-01-07T06:00:00', '2024-01-07T06:00:00', '2024-01-07T07:30:00', '2024-01-07T07:28:00',  0, NULL,          72, 1400.00,   35000.00, 'arrived',   '2024-01-07T07:35:00'),
('AA109', '2024-01-07', 'RT-007', 'N404DD', 'CRW-004', '2024-01-07T11:00:00', '2024-01-07T11:45:00', '2024-01-07T22:00:00', '2024-01-07T22:50:00', 45, 'mechanical', 380, 58000.00,  650000.00, 'arrived',   '2024-01-07T23:00:00'),
('AA110', '2024-01-08', 'RT-002', 'N303CC', 'CRW-002', '2024-01-08T09:00:00', '2024-01-08T09:00:00', '2024-01-08T12:15:00', '2024-01-08T12:10:00',  0, NULL,         170, 4100.00,   95000.00, 'arrived',   '2024-01-08T12:15:00'),
('AA111', '2024-01-09', 'RT-008', 'N202BB', 'CRW-002', '2024-01-09T17:00:00', '2024-01-09T17:30:00', '2024-01-10T05:00:00', '2024-01-10T05:35:00', 30, 'ATC',        275, 32000.00,  380000.00, 'arrived',   '2024-01-10T05:40:00'),
('AA112', '2024-01-10', 'RT-003', 'N505EE', 'CRW-003', '2024-01-10T11:00:00', '2024-01-10T11:00:00', '2024-01-10T13:15:00', '2024-01-10T13:12:00',  0, NULL,         205, 3000.00,   78000.00, 'arrived',   '2024-01-10T13:15:00'),
('AA113', '2024-01-10', 'RT-001', 'N101AA', 'CRW-001', '2024-01-10T08:00:00', NULL,                  '2024-01-10T13:30:00', NULL,                   0, 'weather',      0,    0.00,        0.00, 'cancelled', '2024-01-10T07:30:00'),
('AA114', '2024-01-12', 'RT-005', 'N606FF', 'CRW-005', '2024-01-12T07:00:00', '2024-01-12T07:10:00', '2024-01-12T08:30:00', '2024-01-12T08:45:00', 10, 'crew',        70, 1700.00,   36000.00, 'arrived',   '2024-01-12T08:50:00'),
('AA115', '2024-01-13', 'RT-004', 'N303CC', 'CRW-005', '2024-01-13T14:00:00', '2024-01-13T14:00:00', '2024-01-13T16:00:00', '2024-01-13T15:58:00',  0, NULL,         175, 2350.00,   60000.00, 'arrived',   '2024-01-13T16:05:00'),
('AA116', '2024-01-14', 'RT-006', 'N404DD', 'CRW-001', '2024-01-14T18:00:00', '2024-01-14T18:10:00', '2024-01-15T06:30:00', '2024-01-15T06:45:00', 10, 'ATC',        385, 42000.00,  580000.00, 'arrived',   '2024-01-15T06:50:00'),
('AA117', '2024-01-15', 'RT-009', 'N505EE', 'CRW-003', '2024-01-15T10:00:00', '2024-01-15T12:30:00', '2024-01-15T12:00:00', '2024-01-15T14:35:00',150, 'mechanical', 180, 2200.00,   42000.00, 'arrived',   '2024-01-15T14:40:00'),
('AA118', '2024-01-16', 'RT-010', 'N606FF', 'CRW-003', '2024-01-16T06:00:00', '2024-01-16T06:05:00', '2024-01-16T07:30:00', '2024-01-16T07:38:00',  5, NULL,          74, 1450.00,   36000.00, 'arrived',   '2024-01-16T07:40:00'),
('AA119', '2024-01-18', 'RT-002', 'N303CC', 'CRW-002', '2024-01-18T09:00:00', '2024-01-18T09:20:00', '2024-01-18T12:15:00', '2024-01-18T12:40:00', 20, 'weather',    158, 4300.00,   88000.00, 'arrived',   '2024-01-18T12:45:00'),
('AA120', '2024-01-19', 'RT-001', 'N101AA', 'CRW-001', '2024-01-19T08:00:00', '2024-01-19T08:00:00', '2024-01-19T13:30:00', '2024-01-19T13:25:00',  0, NULL,         188, 8200.00,  195000.00, 'arrived',   '2024-01-19T13:30:00'),
('AA121', '2024-01-20', 'RT-007', 'N404DD', 'CRW-004', '2024-01-20T11:00:00', '2024-01-20T11:00:00', '2024-01-20T22:00:00', '2024-01-20T21:55:00',  0, NULL,         392, 56500.00,  670000.00, 'arrived',   '2024-01-20T22:00:00'),
('AA122', '2024-01-21', 'RT-003', 'N505EE', 'CRW-003', '2024-01-21T11:00:00', '2024-01-21T11:05:00', '2024-01-21T13:15:00', '2024-01-21T13:20:00',  5, NULL,         210, 3050.00,   80000.00, 'arrived',   '2024-01-21T13:25:00'),
('AA123', '2024-01-22', 'RT-008', 'N202BB', 'CRW-002', '2024-01-22T17:00:00', '2024-01-22T17:00:00', '2024-01-23T05:00:00', '2024-01-23T04:55:00',  0, NULL,         282, 31500.00,  395000.00, 'arrived',   '2024-01-23T05:00:00'),
('AA124', '2024-01-23', 'RT-005', 'N606FF', 'CRW-005', '2024-01-23T07:00:00', '2024-01-23T07:00:00', '2024-01-23T08:30:00', '2024-01-23T08:25:00',  0, NULL,          75, 1600.00,   40000.00, 'arrived',   '2024-01-23T08:30:00'),
('AA125', '2024-01-25', 'RT-004', 'N303CC', 'CRW-005', '2024-01-25T14:00:00', '2024-01-25T14:45:00', '2024-01-25T16:00:00', '2024-01-25T16:50:00', 45, 'ATC',        168, 2500.00,   55000.00, 'arrived',   '2024-01-25T16:55:00'),
('AA126', '2024-01-26', 'RT-006', 'N202BB', 'CRW-001', '2024-01-26T18:00:00', '2024-01-26T18:00:00', '2024-01-27T06:30:00', '2024-01-27T06:25:00',  0, NULL,         288, 27800.00,  445000.00, 'arrived',   '2024-01-27T06:30:00'),
('AA127', '2024-01-28', 'RT-010', 'N606FF', 'CRW-003', '2024-01-28T06:00:00', '2024-01-28T06:00:00', '2024-01-28T07:30:00', '2024-01-28T07:30:00',  0, NULL,          70, 1380.00,   34000.00, 'arrived',   '2024-01-28T07:35:00'),
('AA128', '2024-01-29', 'RT-009', 'N505EE', 'CRW-003', '2024-01-29T10:00:00', '2024-01-29T10:05:00', '2024-01-29T12:00:00', '2024-01-29T12:08:00',  5, NULL,         200, 2150.00,   50000.00, 'arrived',   '2024-01-29T12:10:00'),
('AA129', '2024-01-30', 'RT-001', 'N101AA', 'CRW-001', '2024-01-30T08:00:00', '2024-01-30T08:00:00', '2024-01-30T13:30:00', '2024-01-30T13:28:00',  0, NULL,         183, 8100.00,  188000.00, 'arrived',   '2024-01-30T13:30:00'),
('AA130', '2024-01-31', 'RT-003', 'N505EE', 'CRW-003', '2024-01-31T11:00:00', NULL,                  '2024-01-31T13:15:00', NULL,                   0, 'mechanical',   0,    0.00,        0.00, 'cancelled', '2024-01-31T10:30:00'),
-- February 2024 flights
('AA200', '2024-02-01', 'RT-001', 'N101AA', 'CRW-001', '2024-02-01T08:00:00', '2024-02-01T08:10:00', '2024-02-01T13:30:00', '2024-02-01T13:45:00', 10, 'ATC',        180, 8350.00,  192000.00, 'arrived',   '2024-02-01T13:50:00'),
('AA201', '2024-02-01', 'RT-002', 'N303CC', 'CRW-002', '2024-02-01T09:00:00', '2024-02-01T09:00:00', '2024-02-01T12:15:00', '2024-02-01T12:12:00',  0, NULL,         172, 4150.00,   96000.00, 'arrived',   '2024-02-01T12:15:00'),
('AA202', '2024-02-02', 'RT-006', 'N404DD', 'CRW-001', '2024-02-02T18:00:00', '2024-02-02T19:00:00', '2024-02-03T06:30:00', '2024-02-03T07:40:00', 60, 'weather',    370, 43000.00,  560000.00, 'arrived',   '2024-02-03T07:45:00'),
('AA203', '2024-02-03', 'RT-004', 'N303CC', 'CRW-005', '2024-02-03T14:00:00', '2024-02-03T14:00:00', '2024-02-03T16:00:00', '2024-02-03T15:55:00',  0, NULL,         178, 2380.00,   62000.00, 'arrived',   '2024-02-03T16:00:00'),
('AA204', '2024-02-04', 'RT-005', 'N606FF', 'CRW-005', '2024-02-04T07:00:00', '2024-02-04T07:05:00', '2024-02-04T08:30:00', '2024-02-04T08:35:00',  5, NULL,          73, 1620.00,   38000.00, 'arrived',   '2024-02-04T08:40:00'),
('AA205', '2024-02-05', 'RT-009', 'N505EE', 'CRW-003', '2024-02-05T10:00:00', '2024-02-05T10:00:00', '2024-02-05T12:00:00', '2024-02-05T11:55:00',  0, NULL,         210, 2080.00,   52000.00, 'arrived',   '2024-02-05T12:00:00'),
('AA206', '2024-02-06', 'RT-003', 'N505EE', 'CRW-003', '2024-02-06T11:00:00', '2024-02-06T11:00:00', '2024-02-06T13:15:00', '2024-02-06T13:10:00',  0, NULL,         215, 2950.00,   82000.00, 'arrived',   '2024-02-06T13:15:00'),
('AA207', '2024-02-07', 'RT-007', 'N404DD', 'CRW-004', '2024-02-07T11:00:00', '2024-02-07T11:20:00', '2024-02-07T22:00:00', '2024-02-07T22:25:00', 20, 'ATC',        388, 57500.00,  660000.00, 'arrived',   '2024-02-07T22:30:00'),
('AA208', '2024-02-08', 'RT-010', 'N606FF', 'CRW-003', '2024-02-08T06:00:00', '2024-02-08T06:00:00', '2024-02-08T07:30:00', '2024-02-08T07:25:00',  0, NULL,          76, 1350.00,   37000.00, 'arrived',   '2024-02-08T07:30:00'),
('AA209', '2024-02-09', 'RT-001', 'N101AA', 'CRW-001', '2024-02-09T08:00:00', '2024-02-09T08:00:00', '2024-02-09T13:30:00', '2024-02-09T13:30:00',  0, NULL,         186, 8250.00,  194000.00, 'arrived',   '2024-02-09T13:35:00'),
('AA210', '2024-02-10', 'RT-002', 'N303CC', 'CRW-002', '2024-02-10T09:00:00', '2024-02-10T09:35:00', '2024-02-10T12:15:00', '2024-02-10T12:55:00', 35, 'weather',    155, 4400.00,   86000.00, 'arrived',   '2024-02-10T13:00:00'),
('AA211', '2024-02-11', 'RT-008', 'N202BB', 'CRW-002', '2024-02-11T17:00:00', '2024-02-11T17:00:00', '2024-02-12T05:00:00', '2024-02-12T04:50:00',  0, NULL,         280, 31200.00,  400000.00, 'arrived',   '2024-02-12T05:00:00'),
('AA212', '2024-02-12', 'RT-003', 'N505EE', 'CRW-003', '2024-02-12T11:00:00', '2024-02-12T11:05:00', '2024-02-12T13:15:00', '2024-02-12T13:22:00',  5, NULL,         202, 3080.00,   76000.00, 'arrived',   '2024-02-12T13:25:00'),
('AA213', '2024-02-13', 'RT-004', 'N303CC', 'CRW-005', '2024-02-13T14:00:00', '2024-02-13T14:30:00', '2024-02-13T16:00:00', '2024-02-13T16:35:00', 30, 'crew',       160, 2550.00,   54000.00, 'arrived',   '2024-02-13T16:40:00'),
('AA214', '2024-02-14', 'RT-006', 'N202BB', 'CRW-001', '2024-02-14T18:00:00', '2024-02-14T18:00:00', '2024-02-15T06:30:00', '2024-02-15T06:20:00',  0, NULL,         290, 27500.00,  450000.00, 'arrived',   '2024-02-15T06:25:00'),
('AA215', '2024-02-15', 'RT-005', 'N606FF', 'CRW-005', '2024-02-15T07:00:00', '2024-02-15T07:00:00', '2024-02-15T08:30:00', '2024-02-15T08:28:00',  0, NULL,          74, 1580.00,   39000.00, 'arrived',   '2024-02-15T08:30:00'),
('AA216', '2024-02-16', 'RT-009', 'N505EE', 'CRW-003', '2024-02-16T10:00:00', '2024-02-16T10:10:00', '2024-02-16T12:00:00', '2024-02-16T12:15:00', 10, 'ATC',        195, 2180.00,   49000.00, 'arrived',   '2024-02-16T12:20:00'),
('AA217', '2024-02-17', 'RT-010', 'N606FF', 'CRW-003', '2024-02-17T06:00:00', NULL,                  '2024-02-17T07:30:00', NULL,                   0, 'weather',      0,    0.00,        0.00, 'cancelled', '2024-02-17T05:30:00'),
('AA218', '2024-02-18', 'RT-001', 'N101AA', 'CRW-001', '2024-02-18T08:00:00', '2024-02-18T08:25:00', '2024-02-18T13:30:00', '2024-02-18T14:00:00', 25, 'weather',    178, 8500.00,  182000.00, 'arrived',   '2024-02-18T14:05:00'),
('AA219', '2024-02-19', 'RT-007', 'N404DD', 'CRW-004', '2024-02-19T11:00:00', '2024-02-19T11:00:00', '2024-02-19T22:00:00', '2024-02-19T21:50:00',  0, NULL,         395, 55800.00,  680000.00, 'arrived',   '2024-02-19T22:00:00'),
('AA220', '2024-02-20', 'RT-002', 'N303CC', 'CRW-002', '2024-02-20T09:00:00', '2024-02-20T09:00:00', '2024-02-20T12:15:00', '2024-02-20T12:10:00',  0, NULL,         175, 4050.00,   98000.00, 'arrived',   '2024-02-20T12:15:00'),
('AA221', '2024-02-21', 'RT-003', 'N505EE', 'CRW-003', '2024-02-21T11:00:00', '2024-02-21T11:00:00', '2024-02-21T13:15:00', '2024-02-21T13:10:00',  0, NULL,         218, 2920.00,   84000.00, 'arrived',   '2024-02-21T13:15:00'),
('AA222', '2024-02-22', 'RT-004', 'N303CC', 'CRW-005', '2024-02-22T14:00:00', '2024-02-22T14:00:00', '2024-02-22T16:00:00', '2024-02-22T15:52:00',  0, NULL,         180, 2300.00,   63000.00, 'arrived',   '2024-02-22T15:55:00'),
('AA223', '2024-02-23', 'RT-008', 'N202BB', 'CRW-002', '2024-02-23T17:00:00', '2024-02-23T17:15:00', '2024-02-24T05:00:00', '2024-02-24T05:20:00', 15, 'ATC',        278, 32200.00,  385000.00, 'arrived',   '2024-02-24T05:25:00'),
('AA224', '2024-02-24', 'RT-005', 'N606FF', 'CRW-005', '2024-02-24T07:00:00', '2024-02-24T07:00:00', '2024-02-24T08:30:00', '2024-02-24T08:30:00',  0, NULL,          71, 1550.00,   37000.00, 'arrived',   '2024-02-24T08:35:00'),
('AA225', '2024-02-25', 'RT-009', 'N505EE', 'CRW-003', '2024-02-25T10:00:00', '2024-02-25T10:00:00', '2024-02-25T12:00:00', '2024-02-25T11:58:00',  0, NULL,         208, 2050.00,   53000.00, 'arrived',   '2024-02-25T12:00:00'),
('AA226', '2024-02-26', 'RT-010', 'N606FF', 'CRW-003', '2024-02-26T06:00:00', '2024-02-26T06:00:00', '2024-02-26T07:30:00', '2024-02-26T07:28:00',  0, NULL,          75, 1400.00,   36500.00, 'arrived',   '2024-02-26T07:30:00'),
('AA227', '2024-02-27', 'RT-006', 'N404DD', 'CRW-001', '2024-02-27T18:00:00', '2024-02-27T18:00:00', '2024-02-28T06:30:00', '2024-02-28T06:28:00',  0, NULL,         394, 41500.00,  590000.00, 'arrived',   '2024-02-28T06:30:00'),
('AA228', '2024-02-28', 'RT-001', 'N101AA', 'CRW-001', '2024-02-28T08:00:00', '2024-02-28T08:00:00', '2024-02-28T13:30:00', '2024-02-28T13:22:00',  0, NULL,         190, 8100.00,  198000.00, 'arrived',   '2024-02-28T13:25:00');

ASSERT ROW_COUNT = 62
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_flight_ops;

