-- =============================================================================
-- Telecom CDR Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw call detail records and reference data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched CDR with drop detection and subscriber profiles';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for network quality and revenue analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_subscribers (
    subscriber_id   STRING      NOT NULL,
    phone_number    STRING      NOT NULL,
    plan_type       STRING,
    plan_tier       STRING,
    activation_date DATE,
    status          STRING,
    monthly_spend   DECIMAL(8,2),
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/telecom/bronze/raw_subscribers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_cell_towers (
    tower_id       STRING      NOT NULL,
    location       STRING,
    city           STRING,
    region         STRING,
    technology     STRING,
    capacity_mhz   INT,
    ingested_at    TIMESTAMP
) LOCATION '{{data_path}}/telecom/bronze/raw_cell_towers';

-- Initial CDR table (WITHOUT roaming_flag - added later via schema evolution)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_cdr (
    cdr_id          STRING      NOT NULL,
    caller_number   STRING      NOT NULL,
    callee_number   STRING      NOT NULL,
    tower_id        STRING,
    start_time      TIMESTAMP   NOT NULL,
    end_time        TIMESTAMP,
    call_type       STRING,
    data_usage_mb   DECIMAL(10,2),
    revenue         DECIMAL(8,2),
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/telecom/bronze/raw_cdr';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.cdr_enriched (
    cdr_id          STRING      NOT NULL,
    caller_id       STRING,
    callee_id       STRING,
    caller_number   STRING,
    callee_number   STRING,
    tower_id        STRING,
    tower_city      STRING,
    tower_region    STRING,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    duration_sec    INT,
    call_type       STRING,
    data_usage_mb   DECIMAL(10,2),
    roaming_flag    BOOLEAN     DEFAULT false,
    drop_flag       BOOLEAN     DEFAULT false,
    revenue         DECIMAL(8,2),
    enriched_at     TIMESTAMP
) LOCATION '{{data_path}}/telecom/silver/cdr_enriched';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.subscriber_profiles (
    subscriber_id    STRING      NOT NULL,
    phone_number     STRING,
    plan_type        STRING,
    plan_tier        STRING,
    activation_date  DATE,
    status           STRING,
    monthly_spend    DECIMAL(8,2),
    total_calls      INT,
    total_data_mb    DECIMAL(12,2),
    total_revenue    DECIMAL(10,2),
    avg_call_duration INT,
    drop_rate        DECIMAL(5,4),
    last_activity    TIMESTAMP,
    updated_at       TIMESTAMP
) LOCATION '{{data_path}}/telecom/silver/subscriber_profiles';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_subscriber (
    subscriber_key  STRING      NOT NULL,
    phone_number    STRING,
    plan_type       STRING,
    plan_tier       STRING,
    activation_date DATE,
    status          STRING,
    monthly_spend   DECIMAL(8,2)
) LOCATION '{{data_path}}/telecom/gold/dim_subscriber';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_cell_tower (
    tower_key      STRING      NOT NULL,
    tower_id       STRING,
    location       STRING,
    city           STRING,
    region         STRING,
    technology     STRING,
    capacity_mhz   INT
) LOCATION '{{data_path}}/telecom/gold/dim_cell_tower';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_calls (
    call_key        STRING      NOT NULL,
    caller_key      STRING,
    callee_key      STRING,
    cell_tower_key  STRING,
    start_time      TIMESTAMP,
    duration_sec    INT,
    call_type       STRING,
    data_usage_mb   DECIMAL(10,2),
    roaming_flag    BOOLEAN,
    drop_flag       BOOLEAN,
    revenue         DECIMAL(8,2)
) LOCATION '{{data_path}}/telecom/gold/fact_calls';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_network_quality (
    region          STRING,
    hour_bucket     INT,
    total_calls     INT,
    dropped_calls   INT,
    drop_rate       DECIMAL(5,4),
    avg_duration    DECIMAL(8,1),
    total_data_mb   DECIMAL(12,2),
    peak_concurrent INT,
    revenue         DECIMAL(10,2)
) LOCATION '{{data_path}}/telecom/gold/kpi_network_quality';

-- ===================== PSEUDONYMISATION =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_subscribers (phone_number) TRANSFORM generalize PARAMS ('mask_last', '4');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.cdr_enriched (caller_number) TRANSFORM generalize PARAMS ('mask_last', '4');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.cdr_enriched (callee_number) TRANSFORM generalize PARAMS ('mask_last', '4');

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_subscribers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_cell_towers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_cdr TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.cdr_enriched TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.subscriber_profiles TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_subscriber TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_cell_tower TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_calls TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_network_quality TO USER {{current_user}};

-- ===================== SEED DATA: SUBSCRIBERS (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_subscribers VALUES
('SUB-001', '+1-212-555-1001', 'postpaid', 'platinum', '2022-03-15', 'active',   89.99, '2024-06-01T00:00:00'),
('SUB-002', '+1-415-555-2002', 'postpaid', 'gold',     '2022-07-20', 'active',   59.99, '2024-06-01T00:00:00'),
('SUB-003', '+1-312-555-3003', 'prepaid',  'silver',   '2023-01-10', 'active',   29.99, '2024-06-01T00:00:00'),
('SUB-004', '+1-206-555-4004', 'postpaid', 'platinum', '2021-11-05', 'active',   89.99, '2024-06-01T00:00:00'),
('SUB-005', '+1-617-555-5005', 'prepaid',  'bronze',   '2023-06-22', 'active',   14.99, '2024-06-01T00:00:00'),
('SUB-006', '+1-512-555-6006', 'postpaid', 'gold',     '2022-09-30', 'active',   59.99, '2024-06-01T00:00:00'),
('SUB-007', '+1-303-555-7007', 'prepaid',  'silver',   '2023-04-18', 'active',   29.99, '2024-06-01T00:00:00'),
('SUB-008', '+1-503-555-8008', 'postpaid', 'gold',     '2022-02-14', 'suspended',59.99, '2024-06-01T00:00:00'),
('SUB-009', '+1-305-555-9009', 'postpaid', 'platinum', '2021-08-01', 'active',   89.99, '2024-06-01T00:00:00'),
('SUB-010', '+1-602-555-0010', 'prepaid',  'bronze',   '2023-10-12', 'active',   14.99, '2024-06-01T00:00:00'),
('SUB-011', '+1-404-555-1011', 'postpaid', 'gold',     '2022-12-25', 'active',   59.99, '2024-06-01T00:00:00'),
('SUB-012', '+1-214-555-2012', 'prepaid',  'silver',   '2023-08-08', 'churned',  29.99, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_subscribers;


-- ===================== SEED DATA: CELL TOWERS (8 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_cell_towers VALUES
('TWR-NE-01', '40.7128,-74.0060',  'New York',      'Northeast', '5G',  100, '2024-06-01T00:00:00'),
('TWR-NE-02', '42.3601,-71.0589',  'Boston',        'Northeast', '4G',   60, '2024-06-01T00:00:00'),
('TWR-W-01',  '37.7749,-122.4194', 'San Francisco', 'West',      '5G',  100, '2024-06-01T00:00:00'),
('TWR-W-02',  '47.6062,-122.3321', 'Seattle',       'West',      '4G',   60, '2024-06-01T00:00:00'),
('TWR-C-01',  '41.8781,-87.6298',  'Chicago',       'Central',   '5G',   80, '2024-06-01T00:00:00'),
('TWR-C-02',  '30.2672,-97.7431',  'Austin',        'Central',   '4G',   60, '2024-06-01T00:00:00'),
('TWR-S-01',  '25.7617,-80.1918',  'Miami',         'South',     '5G',   80, '2024-06-01T00:00:00'),
('TWR-S-02',  '33.7490,-84.3880',  'Atlanta',       'South',     '4G',   60, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_cell_towers;


-- ===================== SEED DATA: CDR RECORDS (65 rows) =====================
-- Mix of voice, sms, data calls. Short voice calls (<10s) flagged as dropped.

INSERT INTO {{zone_prefix}}.bronze.raw_cdr VALUES
-- Morning peak (8-11am) - Northeast
('CDR-00001', '+1-212-555-1001', '+1-415-555-2002', 'TWR-NE-01', '2024-06-01T08:15:00', '2024-06-01T08:19:32', 'voice',   0.00, 0.45, '2024-06-01T12:00:00'),
('CDR-00002', '+1-212-555-1001', '+1-312-555-3003', 'TWR-NE-01', '2024-06-01T08:45:00', '2024-06-01T08:45:07', 'voice',   0.00, 0.02, '2024-06-01T12:00:00'),
('CDR-00003', '+1-617-555-5005', '+1-212-555-1001', 'TWR-NE-02', '2024-06-01T09:10:00', '2024-06-01T09:22:15', 'voice',   0.00, 1.22, '2024-06-01T12:00:00'),
('CDR-00004', '+1-212-555-1001', '+1-617-555-5005', 'TWR-NE-01', '2024-06-01T10:00:00', NULL,                  'sms',     0.00, 0.05, '2024-06-01T12:00:00'),
('CDR-00005', '+1-617-555-5005', '+1-404-555-1011', 'TWR-NE-02', '2024-06-01T10:30:00', '2024-06-01T10:30:05', 'voice',   0.00, 0.01, '2024-06-01T12:00:00'),

-- Morning peak - West
('CDR-00006', '+1-415-555-2002', '+1-206-555-4004', 'TWR-W-01',  '2024-06-01T08:20:00', '2024-06-01T08:35:42', 'voice',   0.00, 1.55, '2024-06-01T12:00:00'),
('CDR-00007', '+1-206-555-4004', '+1-503-555-8008', 'TWR-W-02',  '2024-06-01T09:00:00', '2024-06-01T09:12:18', 'voice',   0.00, 1.22, '2024-06-01T12:00:00'),
('CDR-00008', '+1-415-555-2002', '+1-206-555-4004', 'TWR-W-01',  '2024-06-01T09:30:00', NULL,                  'data',  250.50, 2.50, '2024-06-01T12:00:00'),
('CDR-00009', '+1-503-555-8008', '+1-415-555-2002', 'TWR-W-02',  '2024-06-01T10:15:00', '2024-06-01T10:15:04', 'voice',   0.00, 0.01, '2024-06-01T12:00:00'),
('CDR-00010', '+1-206-555-4004', '+1-312-555-3003', 'TWR-W-02',  '2024-06-01T11:00:00', '2024-06-01T11:08:45', 'voice',   0.00, 0.87, '2024-06-01T12:00:00'),

-- Morning peak - Central
('CDR-00011', '+1-312-555-3003', '+1-512-555-6006', 'TWR-C-01',  '2024-06-01T08:05:00', '2024-06-01T08:18:30', 'voice',   0.00, 1.35, '2024-06-01T12:00:00'),
('CDR-00012', '+1-512-555-6006', '+1-312-555-3003', 'TWR-C-02',  '2024-06-01T09:20:00', NULL,                  'data',  180.00, 1.80, '2024-06-01T12:00:00'),
('CDR-00013', '+1-312-555-3003', '+1-214-555-2012', 'TWR-C-01',  '2024-06-01T10:45:00', '2024-06-01T10:52:10', 'voice',   0.00, 0.71, '2024-06-01T12:00:00'),
('CDR-00014', '+1-512-555-6006', '+1-602-555-0010', 'TWR-C-02',  '2024-06-01T11:30:00', '2024-06-01T11:30:08', 'voice',   0.00, 0.02, '2024-06-01T12:00:00'),

-- Morning peak - South
('CDR-00015', '+1-305-555-9009', '+1-404-555-1011', 'TWR-S-01',  '2024-06-01T08:30:00', '2024-06-01T08:48:22', 'voice',   0.00, 1.82, '2024-06-01T12:00:00'),
('CDR-00016', '+1-404-555-1011', '+1-305-555-9009', 'TWR-S-02',  '2024-06-01T09:15:00', NULL,                  'sms',     0.00, 0.05, '2024-06-01T12:00:00'),
('CDR-00017', '+1-305-555-9009', '+1-214-555-2012', 'TWR-S-01',  '2024-06-01T10:00:00', '2024-06-01T10:25:40', 'voice',   0.00, 2.56, '2024-06-01T12:00:00'),
('CDR-00018', '+1-404-555-1011', '+1-602-555-0010', 'TWR-S-02',  '2024-06-01T11:20:00', '2024-06-01T11:20:03', 'voice',   0.00, 0.01, '2024-06-01T12:00:00'),

-- Afternoon (12-5pm) - mixed regions
('CDR-00019', '+1-212-555-1001', '+1-303-555-7007', 'TWR-NE-01', '2024-06-01T12:30:00', '2024-06-01T12:42:15', 'voice',   0.00, 1.22, '2024-06-01T18:00:00'),
('CDR-00020', '+1-303-555-7007', '+1-512-555-6006', 'TWR-C-01',  '2024-06-01T13:00:00', NULL,                  'data',  320.00, 3.20, '2024-06-01T18:00:00'),
('CDR-00021', '+1-415-555-2002', '+1-305-555-9009', 'TWR-W-01',  '2024-06-01T13:45:00', '2024-06-01T14:02:30', 'voice',   0.00, 1.75, '2024-06-01T18:00:00'),
('CDR-00022', '+1-602-555-0010', '+1-212-555-1001', 'TWR-W-02',  '2024-06-01T14:15:00', '2024-06-01T14:15:06', 'voice',   0.00, 0.01, '2024-06-01T18:00:00'),
('CDR-00023', '+1-214-555-2012', '+1-404-555-1011', 'TWR-C-02',  '2024-06-01T14:30:00', '2024-06-01T14:38:20', 'voice',   0.00, 0.83, '2024-06-01T18:00:00'),
('CDR-00024', '+1-305-555-9009', '+1-503-555-8008', 'TWR-S-01',  '2024-06-01T15:00:00', NULL,                  'data',  450.75, 4.51, '2024-06-01T18:00:00'),
('CDR-00025', '+1-404-555-1011', '+1-212-555-1001', 'TWR-S-02',  '2024-06-01T15:30:00', '2024-06-01T15:45:10', 'voice',   0.00, 1.51, '2024-06-01T18:00:00'),
('CDR-00026', '+1-312-555-3003', '+1-617-555-5005', 'TWR-C-01',  '2024-06-01T16:00:00', '2024-06-01T16:08:45', 'voice',   0.00, 0.87, '2024-06-01T18:00:00'),
('CDR-00027', '+1-206-555-4004', '+1-602-555-0010', 'TWR-W-02',  '2024-06-01T16:30:00', NULL,                  'sms',     0.00, 0.05, '2024-06-01T18:00:00'),
('CDR-00028', '+1-512-555-6006', '+1-303-555-7007', 'TWR-C-02',  '2024-06-01T17:00:00', '2024-06-01T17:22:55', 'voice',   0.00, 2.29, '2024-06-01T18:00:00'),

-- Evening (6-10pm) - mixed regions
('CDR-00029', '+1-212-555-1001', '+1-415-555-2002', 'TWR-NE-01', '2024-06-01T18:15:00', '2024-06-01T18:28:30', 'voice',   0.00, 1.35, '2024-06-02T00:00:00'),
('CDR-00030', '+1-617-555-5005', '+1-303-555-7007', 'TWR-NE-02', '2024-06-01T18:45:00', NULL,                  'data',  125.30, 1.25, '2024-06-02T00:00:00'),
('CDR-00031', '+1-415-555-2002', '+1-206-555-4004', 'TWR-W-01',  '2024-06-01T19:00:00', '2024-06-01T19:05:20', 'voice',   0.00, 0.53, '2024-06-02T00:00:00'),
('CDR-00032', '+1-305-555-9009', '+1-404-555-1011', 'TWR-S-01',  '2024-06-01T19:30:00', '2024-06-01T19:52:45', 'voice',   0.00, 2.27, '2024-06-02T00:00:00'),
('CDR-00033', '+1-312-555-3003', '+1-512-555-6006', 'TWR-C-01',  '2024-06-01T20:00:00', '2024-06-01T20:00:09', 'voice',   0.00, 0.02, '2024-06-02T00:00:00'),
('CDR-00034', '+1-404-555-1011', '+1-214-555-2012', 'TWR-S-02',  '2024-06-01T20:30:00', '2024-06-01T20:42:18', 'voice',   0.00, 1.22, '2024-06-02T00:00:00'),
('CDR-00035', '+1-503-555-8008', '+1-206-555-4004', 'TWR-W-02',  '2024-06-01T21:00:00', NULL,                  'data',  580.00, 5.80, '2024-06-02T00:00:00'),

-- Day 2 - June 2
('CDR-00036', '+1-212-555-1001', '+1-617-555-5005', 'TWR-NE-01', '2024-06-02T07:30:00', '2024-06-02T07:45:10', 'voice',   0.00, 1.51, '2024-06-02T12:00:00'),
('CDR-00037', '+1-415-555-2002', '+1-503-555-8008', 'TWR-W-01',  '2024-06-02T08:00:00', '2024-06-02T08:00:06', 'voice',   0.00, 0.01, '2024-06-02T12:00:00'),
('CDR-00038', '+1-312-555-3003', '+1-214-555-2012', 'TWR-C-01',  '2024-06-02T08:30:00', '2024-06-02T08:41:20', 'voice',   0.00, 1.13, '2024-06-02T12:00:00'),
('CDR-00039', '+1-305-555-9009', '+1-602-555-0010', 'TWR-S-01',  '2024-06-02T09:00:00', NULL,                  'data',  200.00, 2.00, '2024-06-02T12:00:00'),
('CDR-00040', '+1-206-555-4004', '+1-415-555-2002', 'TWR-W-02',  '2024-06-02T09:30:00', '2024-06-02T09:48:55', 'voice',   0.00, 1.88, '2024-06-02T12:00:00'),
('CDR-00041', '+1-404-555-1011', '+1-305-555-9009', 'TWR-S-02',  '2024-06-02T10:00:00', '2024-06-02T10:18:30', 'voice',   0.00, 1.85, '2024-06-02T12:00:00'),
('CDR-00042', '+1-512-555-6006', '+1-312-555-3003', 'TWR-C-02',  '2024-06-02T10:30:00', '2024-06-02T10:30:04', 'voice',   0.00, 0.01, '2024-06-02T12:00:00'),
('CDR-00043', '+1-303-555-7007', '+1-617-555-5005', 'TWR-C-01',  '2024-06-02T11:00:00', '2024-06-02T11:15:42', 'voice',   0.00, 1.57, '2024-06-02T12:00:00'),
('CDR-00044', '+1-602-555-0010', '+1-503-555-8008', 'TWR-W-02',  '2024-06-02T11:30:00', NULL,                  'sms',     0.00, 0.05, '2024-06-02T12:00:00'),
('CDR-00045', '+1-214-555-2012', '+1-212-555-1001', 'TWR-C-02',  '2024-06-02T12:00:00', '2024-06-02T12:06:30', 'voice',   0.00, 0.65, '2024-06-02T18:00:00'),

-- Day 2 afternoon/evening
('CDR-00046', '+1-212-555-1001', '+1-305-555-9009', 'TWR-NE-01', '2024-06-02T13:00:00', '2024-06-02T13:22:10', 'voice',   0.00, 2.21, '2024-06-02T18:00:00'),
('CDR-00047', '+1-415-555-2002', '+1-312-555-3003', 'TWR-W-01',  '2024-06-02T14:00:00', NULL,                  'data',  410.25, 4.10, '2024-06-02T18:00:00'),
('CDR-00048', '+1-617-555-5005', '+1-206-555-4004', 'TWR-NE-02', '2024-06-02T14:30:00', '2024-06-02T14:30:08', 'voice',   0.00, 0.02, '2024-06-02T18:00:00'),
('CDR-00049', '+1-305-555-9009', '+1-512-555-6006', 'TWR-S-01',  '2024-06-02T15:00:00', '2024-06-02T15:35:15', 'voice',   0.00, 3.52, '2024-06-02T18:00:00'),
('CDR-00050', '+1-303-555-7007', '+1-404-555-1011', 'TWR-C-01',  '2024-06-02T16:00:00', '2024-06-02T16:12:40', 'voice',   0.00, 1.27, '2024-06-02T18:00:00'),
('CDR-00051', '+1-206-555-4004', '+1-602-555-0010', 'TWR-W-02',  '2024-06-02T17:00:00', NULL,                  'data',  310.50, 3.11, '2024-06-02T18:00:00'),
('CDR-00052', '+1-404-555-1011', '+1-212-555-1001', 'TWR-S-02',  '2024-06-02T18:00:00', '2024-06-02T18:14:25', 'voice',   0.00, 1.44, '2024-06-03T00:00:00'),
('CDR-00053', '+1-512-555-6006', '+1-415-555-2002', 'TWR-C-02',  '2024-06-02T19:00:00', '2024-06-02T19:09:50', 'voice',   0.00, 0.98, '2024-06-03T00:00:00'),
('CDR-00054', '+1-212-555-1001', '+1-303-555-7007', 'TWR-NE-01', '2024-06-02T20:00:00', '2024-06-02T20:20:05', 'voice',   0.00, 2.01, '2024-06-03T00:00:00'),
('CDR-00055', '+1-305-555-9009', '+1-617-555-5005', 'TWR-S-01',  '2024-06-02T20:30:00', '2024-06-02T20:30:03', 'voice',   0.00, 0.01, '2024-06-03T00:00:00'),

-- Day 3 - June 3 (smaller sample)
('CDR-00056', '+1-415-555-2002', '+1-206-555-4004', 'TWR-W-01',  '2024-06-03T08:00:00', '2024-06-03T08:25:10', 'voice',   0.00, 2.51, '2024-06-03T12:00:00'),
('CDR-00057', '+1-312-555-3003', '+1-512-555-6006', 'TWR-C-01',  '2024-06-03T09:00:00', NULL,                  'data',  275.00, 2.75, '2024-06-03T12:00:00'),
('CDR-00058', '+1-305-555-9009', '+1-404-555-1011', 'TWR-S-01',  '2024-06-03T10:00:00', '2024-06-03T10:18:45', 'voice',   0.00, 1.87, '2024-06-03T12:00:00'),
('CDR-00059', '+1-617-555-5005', '+1-212-555-1001', 'TWR-NE-02', '2024-06-03T11:00:00', '2024-06-03T11:11:30', 'voice',   0.00, 1.15, '2024-06-03T12:00:00'),
('CDR-00060', '+1-206-555-4004', '+1-303-555-7007', 'TWR-W-02',  '2024-06-03T12:00:00', '2024-06-03T12:00:05', 'voice',   0.00, 0.01, '2024-06-03T18:00:00'),
('CDR-00061', '+1-404-555-1011', '+1-512-555-6006', 'TWR-S-02',  '2024-06-03T13:00:00', '2024-06-03T13:28:20', 'voice',   0.00, 2.83, '2024-06-03T18:00:00'),
('CDR-00062', '+1-503-555-8008', '+1-212-555-1001', 'TWR-W-02',  '2024-06-03T14:00:00', NULL,                  'data',  490.00, 4.90, '2024-06-03T18:00:00'),
('CDR-00063', '+1-214-555-2012', '+1-305-555-9009', 'TWR-C-02',  '2024-06-03T15:00:00', '2024-06-03T15:05:15', 'voice',   0.00, 0.52, '2024-06-03T18:00:00'),
('CDR-00064', '+1-602-555-0010', '+1-415-555-2002', 'TWR-W-02',  '2024-06-03T16:00:00', '2024-06-03T16:00:07', 'voice',   0.00, 0.01, '2024-06-03T18:00:00'),
('CDR-00065', '+1-212-555-1001', '+1-404-555-1011', 'TWR-NE-01', '2024-06-03T17:00:00', '2024-06-03T17:32:00', 'voice',   0.00, 3.20, '2024-06-03T18:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_cdr;

