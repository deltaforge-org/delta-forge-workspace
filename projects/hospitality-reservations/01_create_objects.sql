-- =============================================================================
-- Hospitality Reservations Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw booking feeds from multiple channels';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Deduplicated bookings with soft deletes';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for revenue management analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_properties (
    property_id     STRING      NOT NULL,
    property_name   STRING      NOT NULL,
    brand           STRING,
    city            STRING,
    state           STRING,
    country         STRING,
    star_rating     INT,
    total_rooms     INT,
    avg_rack_rate   DECIMAL(10,2),
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/bronze/raw_properties';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_room_types (
    room_type_id    STRING      NOT NULL,
    room_type       STRING      NOT NULL,
    max_occupancy   INT,
    base_rate       DECIMAL(10,2),
    amenities       STRING,
    view_type       STRING,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/bronze/raw_room_types';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_guests (
    guest_id        STRING      NOT NULL,
    name            STRING,
    loyalty_tier    STRING,
    lifetime_stays  INT,
    lifetime_revenue DECIMAL(14,2),
    home_city       STRING,
    home_country    STRING,
    credit_card_last4 STRING,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/bronze/raw_guests';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_channels (
    channel_id      STRING      NOT NULL,
    channel_name    STRING      NOT NULL,
    channel_type    STRING,
    commission_pct  DECIMAL(5,2),
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/bronze/raw_channels';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_bookings (
    booking_id      STRING      NOT NULL,
    property_id     STRING      NOT NULL,
    room_type_id    STRING      NOT NULL,
    guest_id        STRING      NOT NULL,
    channel_id      STRING      NOT NULL,
    booking_date    DATE,
    check_in_date   DATE,
    check_out_date  DATE,
    room_rate       DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    status          STRING,
    is_duplicate    BOOLEAN     DEFAULT false,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/bronze/raw_bookings';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.bookings_deduped (
    booking_id      STRING      NOT NULL,
    property_id     STRING      NOT NULL,
    room_type_id    STRING      NOT NULL,
    guest_id        STRING      NOT NULL,
    channel_id      STRING      NOT NULL,
    booking_date    DATE,
    check_in_date   DATE,
    check_out_date  DATE,
    nights          INT,
    room_rate       DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    status          STRING,
    is_cancelled    BOOLEAN,
    booking_lead_days INT,
    loyalty_tier    STRING,
    enriched_at     TIMESTAMP
) LOCATION '{{data_path}}/hotel/silver/bookings_deduped';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_property (
    property_key    STRING      NOT NULL,
    property_id     STRING,
    property_name   STRING,
    brand           STRING,
    city            STRING,
    state           STRING,
    country         STRING,
    star_rating     INT,
    total_rooms     INT,
    avg_rack_rate   DECIMAL(10,2)
) LOCATION '{{data_path}}/hotel/gold/dim_property';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_room_type (
    room_type_key   STRING      NOT NULL,
    room_type       STRING,
    max_occupancy   INT,
    base_rate       DECIMAL(10,2),
    amenities       STRING,
    view_type       STRING
) LOCATION '{{data_path}}/hotel/gold/dim_room_type';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_guest (
    guest_key       STRING      NOT NULL,
    guest_id        STRING,
    name            STRING,
    loyalty_tier    STRING,
    lifetime_stays  INT,
    lifetime_revenue DECIMAL(14,2),
    home_city       STRING,
    home_country    STRING
) LOCATION '{{data_path}}/hotel/gold/dim_guest';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_channel (
    channel_key     STRING      NOT NULL,
    channel_name    STRING,
    channel_type    STRING,
    commission_pct  DECIMAL(5,2)
) LOCATION '{{data_path}}/hotel/gold/dim_channel';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_reservations (
    reservation_key STRING      NOT NULL,
    property_key    STRING      NOT NULL,
    room_type_key   STRING      NOT NULL,
    guest_key       STRING      NOT NULL,
    channel_key     STRING      NOT NULL,
    check_in_date   DATE,
    check_out_date  DATE,
    nights          INT,
    room_rate       DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    status          STRING,
    booking_lead_days INT,
    is_cancelled    BOOLEAN
) LOCATION '{{data_path}}/hotel/gold/fact_reservations';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_revenue_management (
    property_id         STRING      NOT NULL,
    month               STRING      NOT NULL,
    occupancy_rate      DECIMAL(5,2),
    adr                 DECIMAL(10,2),
    revpar              DECIMAL(10,2),
    total_revenue       DECIMAL(14,2),
    cancellation_rate   DECIMAL(5,2),
    avg_lead_time       DECIMAL(7,2),
    direct_booking_pct  DECIMAL(5,2),
    loyalty_revenue_pct DECIMAL(5,2),
    weekday_vs_weekend_ratio DECIMAL(5,2)
) LOCATION '{{data_path}}/hotel/gold/kpi_revenue_management';

-- ===================== GRANTS =====================
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_bookings TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_properties TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_room_types TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_guests TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_channels TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.bookings_deduped TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_reservations TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_property TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_room_type TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_guest TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_channel TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_revenue_management TO USER {{current_user}};

-- ===================== PSEUDONYMISATION =====================
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_guests (name) TRANSFORM mask PARAMS (show = 3);
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_guests (credit_card_last4) TRANSFORM redact;

-- ===================== SEED DATA: PROPERTIES (4 properties) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_properties VALUES
('PROP-001', 'Grand Harbor Resort',    'Luxe Collection', 'Miami',         'FL', 'US', 5, 320, 425.00, '2024-01-01T00:00:00'),
('PROP-002', 'Mountain View Lodge',    'Comfort Stays',   'Aspen',         'CO', 'US', 4, 150, 285.00, '2024-01-01T00:00:00'),
('PROP-003', 'City Center Hotel',      'Urban Select',    'Chicago',       'IL', 'US', 4, 250, 199.00, '2024-01-01T00:00:00'),
('PROP-004', 'Coastal Breeze Inn',     'Comfort Stays',   'San Diego',     'CA', 'US', 3, 100, 165.00, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_properties;


-- ===================== SEED DATA: ROOM TYPES (5 types) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_room_types VALUES
('RT-STD', 'Standard King',     2, 179.00, 'WiFi, TV, Mini-fridge',              'city',     '2024-01-01T00:00:00'),
('RT-DLX', 'Deluxe Double',     4, 249.00, 'WiFi, TV, Mini-fridge, Balcony',     'garden',   '2024-01-01T00:00:00'),
('RT-STE', 'Executive Suite',   2, 399.00, 'WiFi, TV, Kitchenette, Living Room', 'ocean',    '2024-01-01T00:00:00'),
('RT-PNT', 'Penthouse Suite',   4, 799.00, 'WiFi, TV, Full Kitchen, Butler',     'panoramic','2024-01-01T00:00:00'),
('RT-FAM', 'Family Room',       6, 299.00, 'WiFi, TV, Bunk Beds, Play Area',     'pool',     '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_room_types;


-- ===================== SEED DATA: GUESTS (18 guests) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_guests VALUES
('G-001', 'Elizabeth Warren',  'platinum', 45, 52000.00,  'New York',      'US', '4532', '2024-01-01T00:00:00'),
('G-002', 'James Patterson',  'gold',     22, 18500.00,  'Chicago',       'US', '8821', '2024-01-01T00:00:00'),
('G-003', 'Maria Gonzalez',   'silver',   10,  6200.00,  'Miami',         'US', '3345', '2024-01-01T00:00:00'),
('G-004', 'Robert Kim',       'platinum', 52, 67000.00,  'San Francisco', 'US', '9012', '2024-01-01T00:00:00'),
('G-005', 'Sarah Mitchell',   'gold',     18, 14200.00,  'Boston',        'US', '5567', '2024-01-01T00:00:00'),
('G-006', 'David Chen',       'bronze',    3,   980.00,  'Seattle',       'US', '2234', '2024-01-01T00:00:00'),
('G-007', 'Jennifer Adams',   'silver',    8,  4800.00,  'Denver',        'US', '7789', '2024-01-01T00:00:00'),
('G-008', 'Michael O''Brien', 'gold',     25, 22000.00,  'London',        'UK', '1156', '2024-01-01T00:00:00'),
('G-009', 'Lisa Tanaka',      'bronze',    2,   450.00,  'Tokyo',         'JP', '6643', '2024-01-01T00:00:00'),
('G-010', 'Thomas Mueller',   'silver',   12,  8900.00,  'Berlin',        'DE', '4478', '2024-01-01T00:00:00'),
('G-011', 'Anna Petrova',     'platinum', 38, 41000.00,  'Moscow',        'RU', '3321', '2024-01-01T00:00:00'),
('G-012', 'Carlos Rivera',    'bronze',    1,   250.00,  'Mexico City',   'MX', '8890', '2024-01-01T00:00:00'),
('G-013', 'Emma Wilson',      'gold',     20, 16800.00,  'Toronto',       'CA', '5534', '2024-01-01T00:00:00'),
('G-014', 'Pierre Dubois',    'silver',   14, 10500.00,  'Paris',         'FR', '2267', '2024-01-01T00:00:00'),
('G-015', 'Sophia Lee',       'bronze',    4,  1200.00,  'Seoul',         'KR', '9945', '2024-01-01T00:00:00'),
('G-016', 'William Harris',   'gold',     16, 13500.00,  'Atlanta',       'US', '7712', '2024-01-01T00:00:00'),
('G-017', 'Olivia Brown',     'silver',    6,  3600.00,  'Dallas',        'US', '1189', '2024-01-01T00:00:00'),
('G-018', 'Daniel Singh',     'bronze',    2,   580.00,  'Mumbai',        'IN', '4456', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_guests;


-- ===================== SEED DATA: CHANNELS (4 channels) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_channels VALUES
('CH-DIR',  'Direct Website',   'direct',    0.00, '2024-01-01T00:00:00'),
('CH-OTA',  'BookNow.com',      'ota',      18.00, '2024-01-01T00:00:00'),
('CH-CORP', 'Corporate Program','corporate',  5.00, '2024-01-01T00:00:00'),
('CH-TA',   'Travel Agents',    'travel_agent', 12.00, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_channels;


-- ===================== SEED DATA: BOOKINGS (67 rows) =====================
-- Includes cancellations (soft delete), no-shows, extended stays, loyalty members,
-- and duplicate booking attempts
INSERT INTO {{zone_prefix}}.bronze.raw_bookings VALUES
-- January 2024 bookings
('BK-001', 'PROP-001', 'RT-STE', 'G-001', 'CH-DIR',  '2023-12-01', '2024-01-05', '2024-01-08', 399.00, 1197.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-002', 'PROP-001', 'RT-DLX', 'G-003', 'CH-OTA',  '2023-12-15', '2024-01-06', '2024-01-09', 269.00,  807.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-003', 'PROP-002', 'RT-STD', 'G-002', 'CH-CORP', '2023-12-20', '2024-01-07', '2024-01-10', 195.00,  585.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-004', 'PROP-003', 'RT-STD', 'G-006', 'CH-OTA',  '2024-01-02', '2024-01-08', '2024-01-10', 189.00,  378.00, 'completed',  false, '2024-01-03T00:00:00'),
('BK-005', 'PROP-001', 'RT-PNT', 'G-004', 'CH-DIR',  '2023-11-15', '2024-01-10', '2024-01-15', 850.00, 4250.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-006', 'PROP-004', 'RT-FAM', 'G-007', 'CH-TA',   '2023-12-28', '2024-01-12', '2024-01-15', 279.00,  837.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-007', 'PROP-002', 'RT-DLX', 'G-005', 'CH-DIR',  '2024-01-05', '2024-01-14', '2024-01-17', 265.00,  795.00, 'completed',  false, '2024-01-06T00:00:00'),
('BK-008', 'PROP-003', 'RT-STE', 'G-008', 'CH-CORP', '2023-12-10', '2024-01-15', '2024-01-18', 379.00, 1137.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-009', 'PROP-001', 'RT-STD', 'G-009', 'CH-OTA',  '2024-01-10', '2024-01-18', '2024-01-20', 199.00,  398.00, 'cancelled',  false, '2024-01-11T00:00:00'),
('BK-010', 'PROP-004', 'RT-STD', 'G-010', 'CH-DIR',  '2024-01-08', '2024-01-19', '2024-01-22', 159.00,  477.00, 'completed',  false, '2024-01-09T00:00:00'),
('BK-011', 'PROP-002', 'RT-STE', 'G-011', 'CH-DIR',  '2023-11-20', '2024-01-20', '2024-01-25', 389.00, 1945.00, 'completed',  false, '2024-01-01T00:00:00'),
('BK-012', 'PROP-003', 'RT-FAM', 'G-013', 'CH-OTA',  '2024-01-12', '2024-01-22', '2024-01-25', 289.00,  867.00, 'completed',  false, '2024-01-13T00:00:00'),
('BK-013', 'PROP-001', 'RT-DLX', 'G-014', 'CH-TA',   '2024-01-15', '2024-01-25', '2024-01-28', 259.00,  777.00, 'completed',  false, '2024-01-16T00:00:00'),
('BK-014', 'PROP-004', 'RT-DLX', 'G-015', 'CH-OTA',  '2024-01-18', '2024-01-26', '2024-01-28', 239.00,  478.00, 'no_show',    false, '2024-01-19T00:00:00'),
('BK-015', 'PROP-002', 'RT-STD', 'G-016', 'CH-CORP', '2024-01-20', '2024-01-28', '2024-01-31', 195.00,  585.00, 'completed',  false, '2024-01-21T00:00:00'),
-- Duplicate booking attempt (BK-009 rebooked)
('BK-009-DUP', 'PROP-001', 'RT-STD', 'G-009', 'CH-OTA', '2024-01-10', '2024-01-18', '2024-01-20', 199.00, 398.00, 'cancelled', true, '2024-01-11T00:00:00'),
-- February 2024 bookings
('BK-016', 'PROP-001', 'RT-STE', 'G-001', 'CH-DIR',  '2024-01-20', '2024-02-01', '2024-02-04', 419.00, 1257.00, 'completed',  false, '2024-01-21T00:00:00'),
('BK-017', 'PROP-003', 'RT-STD', 'G-002', 'CH-CORP', '2024-01-25', '2024-02-03', '2024-02-06', 189.00,  567.00, 'completed',  false, '2024-01-26T00:00:00'),
('BK-018', 'PROP-002', 'RT-DLX', 'G-012', 'CH-OTA',  '2024-02-01', '2024-02-05', '2024-02-08', 275.00,  825.00, 'completed',  false, '2024-02-02T00:00:00'),
('BK-019', 'PROP-001', 'RT-PNT', 'G-004', 'CH-DIR',  '2024-01-10', '2024-02-07', '2024-02-11', 899.00, 3596.00, 'completed',  false, '2024-01-11T00:00:00'),
('BK-020', 'PROP-004', 'RT-STD', 'G-006', 'CH-OTA',  '2024-02-02', '2024-02-08', '2024-02-10', 149.00,  298.00, 'cancelled',  false, '2024-02-03T00:00:00'),
('BK-021', 'PROP-003', 'RT-STE', 'G-008', 'CH-CORP', '2024-01-28', '2024-02-10', '2024-02-13', 379.00, 1137.00, 'completed',  false, '2024-01-29T00:00:00'),
('BK-022', 'PROP-002', 'RT-FAM', 'G-007', 'CH-TA',   '2024-02-05', '2024-02-12', '2024-02-16', 289.00, 1156.00, 'completed',  false, '2024-02-06T00:00:00'),
('BK-023', 'PROP-001', 'RT-DLX', 'G-005', 'CH-DIR',  '2024-02-01', '2024-02-14', '2024-02-17', 279.00,  837.00, 'completed',  false, '2024-02-02T00:00:00'),
('BK-024', 'PROP-004', 'RT-DLX', 'G-017', 'CH-DIR',  '2024-02-08', '2024-02-15', '2024-02-18', 245.00,  735.00, 'completed',  false, '2024-02-09T00:00:00'),
('BK-025', 'PROP-003', 'RT-STD', 'G-018', 'CH-OTA',  '2024-02-10', '2024-02-17', '2024-02-19', 185.00,  370.00, 'completed',  false, '2024-02-11T00:00:00'),
('BK-026', 'PROP-002', 'RT-STE', 'G-011', 'CH-DIR',  '2024-01-15', '2024-02-18', '2024-02-22', 399.00, 1596.00, 'completed',  false, '2024-01-16T00:00:00'),
('BK-027', 'PROP-001', 'RT-STD', 'G-003', 'CH-OTA',  '2024-02-12', '2024-02-20', '2024-02-22', 209.00,  418.00, 'completed',  false, '2024-02-13T00:00:00'),
('BK-028', 'PROP-004', 'RT-FAM', 'G-013', 'CH-TA',   '2024-02-15', '2024-02-22', '2024-02-25', 269.00,  807.00, 'completed',  false, '2024-02-16T00:00:00'),
('BK-029', 'PROP-003', 'RT-DLX', 'G-016', 'CH-CORP', '2024-02-18', '2024-02-24', '2024-02-27', 245.00,  735.00, 'cancelled',  false, '2024-02-19T00:00:00'),
('BK-030', 'PROP-002', 'RT-STD', 'G-010', 'CH-DIR',  '2024-02-20', '2024-02-26', '2024-02-28', 195.00,  390.00, 'completed',  false, '2024-02-21T00:00:00'),
-- March 2024 bookings
('BK-031', 'PROP-001', 'RT-STE', 'G-004', 'CH-DIR',  '2024-02-15', '2024-03-01', '2024-03-04', 429.00, 1287.00, 'completed',  false, '2024-02-16T00:00:00'),
('BK-032', 'PROP-003', 'RT-STD', 'G-014', 'CH-OTA',  '2024-02-25', '2024-03-03', '2024-03-06', 195.00,  585.00, 'completed',  false, '2024-02-26T00:00:00'),
('BK-033', 'PROP-002', 'RT-DLX', 'G-005', 'CH-DIR',  '2024-03-01', '2024-03-05', '2024-03-08', 275.00,  825.00, 'completed',  false, '2024-03-02T00:00:00'),
('BK-034', 'PROP-004', 'RT-STD', 'G-009', 'CH-OTA',  '2024-03-02', '2024-03-07', '2024-03-09', 155.00,  310.00, 'completed',  false, '2024-03-03T00:00:00'),
('BK-035', 'PROP-001', 'RT-PNT', 'G-011', 'CH-DIR',  '2024-02-01', '2024-03-08', '2024-03-13', 899.00, 4495.00, 'completed',  false, '2024-02-02T00:00:00'),
('BK-036', 'PROP-003', 'RT-FAM', 'G-007', 'CH-TA',   '2024-03-05', '2024-03-10', '2024-03-13', 285.00,  855.00, 'completed',  false, '2024-03-06T00:00:00'),
('BK-037', 'PROP-002', 'RT-STD', 'G-002', 'CH-CORP', '2024-03-01', '2024-03-11', '2024-03-14', 195.00,  585.00, 'completed',  false, '2024-03-02T00:00:00'),
('BK-038', 'PROP-004', 'RT-DLX', 'G-015', 'CH-OTA',  '2024-03-08', '2024-03-14', '2024-03-16', 245.00,  490.00, 'cancelled',  false, '2024-03-09T00:00:00'),
('BK-039', 'PROP-001', 'RT-DLX', 'G-008', 'CH-CORP', '2024-02-20', '2024-03-15', '2024-03-18', 269.00,  807.00, 'completed',  false, '2024-02-21T00:00:00'),
('BK-040', 'PROP-003', 'RT-STE', 'G-001', 'CH-DIR',  '2024-03-01', '2024-03-17', '2024-03-20', 395.00, 1185.00, 'completed',  false, '2024-03-02T00:00:00'),
('BK-041', 'PROP-002', 'RT-DLX', 'G-010', 'CH-DIR',  '2024-03-10', '2024-03-18', '2024-03-21', 270.00,  810.00, 'completed',  false, '2024-03-11T00:00:00'),
('BK-042', 'PROP-004', 'RT-STD', 'G-012', 'CH-OTA',  '2024-03-12', '2024-03-20', '2024-03-22', 149.00,  298.00, 'completed',  false, '2024-03-13T00:00:00'),
('BK-043', 'PROP-001', 'RT-STD', 'G-006', 'CH-OTA',  '2024-03-15', '2024-03-22', '2024-03-24', 209.00,  418.00, 'no_show',    false, '2024-03-16T00:00:00'),
('BK-044', 'PROP-003', 'RT-DLX', 'G-013', 'CH-DIR',  '2024-03-18', '2024-03-24', '2024-03-27', 249.00,  747.00, 'completed',  false, '2024-03-19T00:00:00'),
('BK-045', 'PROP-002', 'RT-STE', 'G-004', 'CH-DIR',  '2024-02-28', '2024-03-25', '2024-03-29', 399.00, 1596.00, 'completed',  false, '2024-03-01T00:00:00'),
-- April 2024 bookings (early)
('BK-046', 'PROP-001', 'RT-DLX', 'G-003', 'CH-OTA',  '2024-03-20', '2024-04-01', '2024-04-04', 275.00,  825.00, 'completed',  false, '2024-03-21T00:00:00'),
('BK-047', 'PROP-004', 'RT-FAM', 'G-017', 'CH-TA',   '2024-03-25', '2024-04-03', '2024-04-06', 259.00,  777.00, 'completed',  false, '2024-03-26T00:00:00'),
('BK-048', 'PROP-003', 'RT-STD', 'G-018', 'CH-OTA',  '2024-04-01', '2024-04-05', '2024-04-07', 185.00,  370.00, 'cancelled',  false, '2024-04-02T00:00:00'),
('BK-049', 'PROP-002', 'RT-STD', 'G-016', 'CH-CORP', '2024-03-28', '2024-04-07', '2024-04-10', 195.00,  585.00, 'completed',  false, '2024-03-29T00:00:00'),
('BK-050', 'PROP-001', 'RT-STE', 'G-001', 'CH-DIR',  '2024-03-15', '2024-04-08', '2024-04-12', 439.00, 1756.00, 'completed',  false, '2024-03-16T00:00:00'),
-- Duplicate attempt for BK-029
('BK-029-DUP', 'PROP-003', 'RT-DLX', 'G-016', 'CH-CORP', '2024-02-18', '2024-02-24', '2024-02-27', 245.00, 735.00, 'cancelled', true, '2024-02-19T00:00:00'),
-- Extended stays
('BK-051', 'PROP-001', 'RT-STD', 'G-008', 'CH-CORP', '2024-01-05', '2024-01-15', '2024-01-29', 179.00, 2506.00, 'completed', false, '2024-01-06T00:00:00'),
('BK-052', 'PROP-002', 'RT-STD', 'G-004', 'CH-DIR',  '2024-02-01', '2024-02-10', '2024-02-24', 185.00, 2590.00, 'completed', false, '2024-02-02T00:00:00'),
-- More variety
('BK-053', 'PROP-003', 'RT-DLX', 'G-005', 'CH-DIR',  '2024-01-08', '2024-01-12', '2024-01-14', 245.00,  490.00, 'completed', false, '2024-01-09T00:00:00'),
('BK-054', 'PROP-004', 'RT-STD', 'G-003', 'CH-OTA',  '2024-01-15', '2024-01-20', '2024-01-22', 155.00,  310.00, 'completed', false, '2024-01-16T00:00:00'),
('BK-055', 'PROP-001', 'RT-DLX', 'G-010', 'CH-TA',   '2024-02-10', '2024-02-15', '2024-02-17', 265.00,  530.00, 'completed', false, '2024-02-11T00:00:00'),
('BK-056', 'PROP-002', 'RT-FAM', 'G-013', 'CH-DIR',  '2024-02-14', '2024-02-20', '2024-02-23', 295.00,  885.00, 'completed', false, '2024-02-15T00:00:00'),
('BK-057', 'PROP-003', 'RT-STD', 'G-015', 'CH-OTA',  '2024-03-01', '2024-03-08', '2024-03-10', 185.00,  370.00, 'completed', false, '2024-03-02T00:00:00'),
('BK-058', 'PROP-004', 'RT-DLX', 'G-011', 'CH-DIR',  '2024-03-05', '2024-03-12', '2024-03-15', 255.00,  765.00, 'completed', false, '2024-03-06T00:00:00'),
('BK-059', 'PROP-001', 'RT-STD', 'G-016', 'CH-CORP', '2024-03-10', '2024-03-18', '2024-03-20', 189.00,  378.00, 'completed', false, '2024-03-11T00:00:00'),
('BK-060', 'PROP-002', 'RT-STD', 'G-018', 'CH-OTA',  '2024-03-15', '2024-03-22', '2024-03-24', 195.00,  390.00, 'no_show',   false, '2024-03-16T00:00:00'),
('BK-061', 'PROP-003', 'RT-STE', 'G-004', 'CH-DIR',  '2024-03-20', '2024-03-28', '2024-03-31', 389.00, 1167.00, 'completed', false, '2024-03-21T00:00:00'),
('BK-062', 'PROP-004', 'RT-STD', 'G-014', 'CH-TA',   '2024-03-22', '2024-03-29', '2024-03-31', 159.00,  318.00, 'completed', false, '2024-03-23T00:00:00'),
('BK-063', 'PROP-001', 'RT-DLX', 'G-012', 'CH-OTA',  '2024-04-01', '2024-04-10', '2024-04-13', 269.00,  807.00, 'completed', false, '2024-04-02T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_bookings;

