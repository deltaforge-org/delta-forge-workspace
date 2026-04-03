-- =============================================================================
-- Energy Smart Meters Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw smart meter readings and reference data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Costed readings with peak/off-peak classification';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for consumption and billing analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_regions (
    region_id       STRING      NOT NULL,
    region_name     STRING,
    grid_zone       STRING,
    utility_company STRING,
    regulatory_body STRING,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/energy/bronze/raw_regions';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_tariffs (
    tariff_id            STRING      NOT NULL,
    tariff_name          STRING,
    rate_per_kwh         DECIMAL(6,4),
    peak_rate_per_kwh    DECIMAL(6,4),
    off_peak_rate_per_kwh DECIMAL(6,4),
    standing_charge      DECIMAL(6,2),
    effective_from       DATE,
    ingested_at          TIMESTAMP
) LOCATION '{{data_path}}/energy/bronze/raw_tariffs';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_meters (
    meter_id         STRING      NOT NULL,
    meter_type       STRING,
    customer_name    STRING,
    address          STRING,
    city             STRING,
    state            STRING,
    region_id        STRING,
    tariff_id        STRING,
    install_date     DATE,
    solar_panel_flag BOOLEAN,
    capacity_kw      DECIMAL(6,2),
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/energy/bronze/raw_meters';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_readings (
    reading_id      STRING      NOT NULL,
    meter_id        STRING      NOT NULL,
    reading_date    DATE        NOT NULL,
    reading_hour    INT         NOT NULL,
    kwh_consumed    DECIMAL(8,2),
    kwh_generated   DECIMAL(8,2) DEFAULT 0,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/energy/bronze/raw_readings';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.readings_costed (
    reading_id      STRING      NOT NULL,
    meter_id        STRING,
    region_id       STRING,
    tariff_id       STRING,
    reading_date    DATE,
    reading_hour    INT,
    kwh_consumed    DECIMAL(8,2),
    kwh_generated   DECIMAL(8,2),
    net_kwh         DECIMAL(8,2),
    peak_flag       BOOLEAN,
    rate_applied    DECIMAL(6,4),
    cost            DECIMAL(8,2),
    capacity_kw     DECIMAL(6,2),
    capacity_valid  BOOLEAN,
    costed_at       TIMESTAMP
) LOCATION '{{data_path}}/energy/silver/readings_costed';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_meter (
    meter_key        STRING      NOT NULL,
    meter_id         STRING,
    meter_type       STRING,
    customer_name    STRING,
    address          STRING,
    install_date     DATE,
    solar_panel_flag BOOLEAN,
    capacity_kw      DECIMAL(6,2)
) LOCATION '{{data_path}}/energy/gold/dim_meter';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_tariff (
    tariff_key            STRING      NOT NULL,
    tariff_name           STRING,
    rate_per_kwh          DECIMAL(6,4),
    peak_rate_per_kwh     DECIMAL(6,4),
    off_peak_rate_per_kwh DECIMAL(6,4),
    standing_charge       DECIMAL(6,2),
    effective_from        DATE
) LOCATION '{{data_path}}/energy/gold/dim_tariff';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_region (
    region_key      STRING      NOT NULL,
    region_name     STRING,
    grid_zone       STRING,
    utility_company STRING,
    regulatory_body STRING
) LOCATION '{{data_path}}/energy/gold/dim_region';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_meter_readings (
    reading_key     STRING      NOT NULL,
    meter_key       STRING,
    tariff_key      STRING,
    region_key      STRING,
    reading_date    DATE,
    reading_hour    INT,
    kwh_consumed    DECIMAL(8,2),
    kwh_generated   DECIMAL(8,2),
    net_kwh         DECIMAL(8,2),
    cost            DECIMAL(8,2),
    peak_flag       BOOLEAN
) LOCATION '{{data_path}}/energy/gold/fact_meter_readings';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_consumption_billing (
    region           STRING,
    billing_month    STRING,
    total_meters     INT,
    total_kwh        DECIMAL(12,2),
    total_generated  DECIMAL(12,2),
    net_consumption  DECIMAL(12,2),
    total_revenue    DECIMAL(12,2),
    avg_bill         DECIMAL(8,2),
    peak_pct         DECIMAL(5,2),
    solar_offset_pct DECIMAL(5,2)
) LOCATION '{{data_path}}/energy/gold/kpi_consumption_billing';

-- ===================== PSEUDONYMISATION =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_meters (address) TRANSFORM generalize PARAMS ('show_fields', 'city,state');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_meter (address) TRANSFORM generalize PARAMS ('show_fields', 'city,state');

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_regions TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_tariffs TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_meters TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_readings TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.readings_costed TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_meter TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_tariff TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_region TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_meter_readings TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_consumption_billing TO USER {{current_user}};

-- ===================== SEED DATA: REGIONS (4 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_regions VALUES
('REG-NE', 'Northeast',  'GRID-NE-1', 'NorthEast Power Co',   'NERC',  '2024-06-01T00:00:00'),
('REG-SE', 'Southeast',  'GRID-SE-1', 'Southern Electric',     'SERC',  '2024-06-01T00:00:00'),
('REG-MW', 'Midwest',    'GRID-MW-1', 'MidWest Energy Corp',   'MRO',   '2024-06-01T00:00:00'),
('REG-W',  'West',       'GRID-W-1',  'Pacific Gas & Electric','WECC',  '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_regions;


-- ===================== SEED DATA: TARIFFS (3 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_tariffs VALUES
('TAR-RES',  'Residential Standard', 0.1200, 0.1800, 0.0800, 12.50, '2024-01-01', '2024-06-01T00:00:00'),
('TAR-COM',  'Commercial',          0.0950, 0.1400, 0.0650, 25.00, '2024-01-01', '2024-06-01T00:00:00'),
('TAR-TOUD', 'Time-of-Use Dynamic', 0.1100, 0.2200, 0.0600, 15.00, '2024-01-01', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_tariffs;


-- ===================== SEED DATA: METERS (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_meters VALUES
('MTR-001', 'smart_v2', 'Alice Morgan',    '123 Oak St, Hartford, CT 06103',        'Hartford',     'CT', 'REG-NE', 'TAR-RES',  '2022-03-15', false, 10.00, '2024-06-01T00:00:00'),
('MTR-002', 'smart_v2', 'Bob Chen',        '456 Pine Ave, New Haven, CT 06510',     'New Haven',    'CT', 'REG-NE', 'TAR-RES',  '2022-05-20', true,  12.00, '2024-06-01T00:00:00'),
('MTR-003', 'smart_v3', 'TechCorp LLC',    '789 Industrial Blvd, Stamford, CT 06901','Stamford',    'CT', 'REG-NE', 'TAR-COM',  '2023-01-10', false, 50.00, '2024-06-01T00:00:00'),
('MTR-004', 'smart_v2', 'Carol Davis',     '321 Maple Dr, Atlanta, GA 30301',       'Atlanta',      'GA', 'REG-SE', 'TAR-RES',  '2022-07-05', true,  15.00, '2024-06-01T00:00:00'),
('MTR-005', 'smart_v2', 'David Kim',       '654 Elm St, Charlotte, NC 28201',       'Charlotte',    'NC', 'REG-SE', 'TAR-TOUD', '2023-02-14', false, 10.00, '2024-06-01T00:00:00'),
('MTR-006', 'smart_v3', 'SouthRetail Inc', '987 Commerce Way, Miami, FL 33101',     'Miami',        'FL', 'REG-SE', 'TAR-COM',  '2023-04-20', true,  60.00, '2024-06-01T00:00:00'),
('MTR-007', 'smart_v2', 'Elena Russo',     '147 Lake Rd, Chicago, IL 60601',        'Chicago',      'IL', 'REG-MW', 'TAR-RES',  '2022-09-30', false, 10.00, '2024-06-01T00:00:00'),
('MTR-008', 'smart_v2', 'Frank OConnor',   '258 Prairie St, Milwaukee, WI 53201',   'Milwaukee',    'WI', 'REG-MW', 'TAR-TOUD', '2023-06-15', true,  14.00, '2024-06-01T00:00:00'),
('MTR-009', 'smart_v3', 'MidWest Mfg Co',  '369 Factory Ln, Detroit, MI 48201',     'Detroit',      'MI', 'REG-MW', 'TAR-COM',  '2023-01-25', false, 75.00, '2024-06-01T00:00:00'),
('MTR-010', 'smart_v2', 'Grace Patel',     '741 Sunset Blvd, Los Angeles, CA 90001','Los Angeles',  'CA', 'REG-W',  'TAR-RES',  '2022-11-10', true,  18.00, '2024-06-01T00:00:00'),
('MTR-011', 'smart_v2', 'Henry Watson',    '852 Bay St, San Francisco, CA 94102',   'San Francisco','CA', 'REG-W',  'TAR-TOUD', '2023-03-05', true,  16.00, '2024-06-01T00:00:00'),
('MTR-012', 'smart_v3', 'PacificTech Ltd', '963 Tech Park, San Jose, CA 95101',     'San Jose',     'CA', 'REG-W',  'TAR-COM',  '2023-05-18', false, 80.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_meters;


-- ===================== SEED DATA: METER READINGS (72 rows) =====================
-- 12 meters x ~6 hourly readings across 2 months (May-June 2024)
-- Peak hours: 7am-11pm (hour 7-22). Off-peak: 11pm-7am (hour 23-6).
-- Solar meters generate during daylight (hours 8-18).

INSERT INTO {{zone_prefix}}.bronze.raw_readings VALUES
-- MTR-001 (Residential, NE, no solar) - May readings
('RD-001', 'MTR-001', '2024-05-01',  2,  0.45, 0.00, '2024-06-01T00:00:00'),
('RD-002', 'MTR-001', '2024-05-01',  8,  1.20, 0.00, '2024-06-01T00:00:00'),
('RD-003', 'MTR-001', '2024-05-01', 14,  0.85, 0.00, '2024-06-01T00:00:00'),
('RD-004', 'MTR-001', '2024-05-01', 19,  2.10, 0.00, '2024-06-01T00:00:00'),
('RD-005', 'MTR-001', '2024-05-15',  3,  0.40, 0.00, '2024-06-01T00:00:00'),
('RD-006', 'MTR-001', '2024-05-15', 10,  1.35, 0.00, '2024-06-01T00:00:00'),

-- MTR-002 (Residential, NE, SOLAR) - May readings
('RD-007', 'MTR-002', '2024-05-01',  2,  0.50, 0.00, '2024-06-01T00:00:00'),
('RD-008', 'MTR-002', '2024-05-01', 10,  1.10, 1.80, '2024-06-01T00:00:00'),
('RD-009', 'MTR-002', '2024-05-01', 13,  0.90, 2.20, '2024-06-01T00:00:00'),
('RD-010', 'MTR-002', '2024-05-01', 17,  1.80, 0.90, '2024-06-01T00:00:00'),
('RD-011', 'MTR-002', '2024-05-15', 11,  1.00, 2.00, '2024-06-01T00:00:00'),
('RD-012', 'MTR-002', '2024-05-15', 15,  0.75, 1.50, '2024-06-01T00:00:00'),

-- MTR-003 (Commercial, NE, no solar)
('RD-013', 'MTR-003', '2024-05-01',  4,  5.20, 0.00, '2024-06-01T00:00:00'),
('RD-014', 'MTR-003', '2024-05-01',  9, 12.50, 0.00, '2024-06-01T00:00:00'),
('RD-015', 'MTR-003', '2024-05-01', 15, 14.80, 0.00, '2024-06-01T00:00:00'),
('RD-016', 'MTR-003', '2024-05-01', 21,  8.30, 0.00, '2024-06-01T00:00:00'),
('RD-017', 'MTR-003', '2024-05-15', 10, 13.20, 0.00, '2024-06-01T00:00:00'),
('RD-018', 'MTR-003', '2024-05-15', 16, 15.10, 0.00, '2024-06-01T00:00:00'),

-- MTR-004 (Residential, SE, SOLAR)
('RD-019', 'MTR-004', '2024-05-01',  1,  0.35, 0.00, '2024-06-01T00:00:00'),
('RD-020', 'MTR-004', '2024-05-01',  9,  0.95, 2.50, '2024-06-01T00:00:00'),
('RD-021', 'MTR-004', '2024-05-01', 12,  0.80, 3.10, '2024-06-01T00:00:00'),
('RD-022', 'MTR-004', '2024-05-01', 18,  1.90, 0.40, '2024-06-01T00:00:00'),
('RD-023', 'MTR-004', '2024-05-15', 10,  1.05, 2.80, '2024-06-01T00:00:00'),
('RD-024', 'MTR-004', '2024-05-15', 14,  0.70, 2.90, '2024-06-01T00:00:00'),

-- MTR-005 (TOU Dynamic, SE, no solar)
('RD-025', 'MTR-005', '2024-05-01',  5,  0.55, 0.00, '2024-06-01T00:00:00'),
('RD-026', 'MTR-005', '2024-05-01', 11,  1.40, 0.00, '2024-06-01T00:00:00'),
('RD-027', 'MTR-005', '2024-05-01', 16,  1.15, 0.00, '2024-06-01T00:00:00'),
('RD-028', 'MTR-005', '2024-05-01', 20,  1.85, 0.00, '2024-06-01T00:00:00'),
('RD-029', 'MTR-005', '2024-05-15',  8,  1.25, 0.00, '2024-06-01T00:00:00'),
('RD-030', 'MTR-005', '2024-05-15', 18,  1.70, 0.00, '2024-06-01T00:00:00'),

-- MTR-006 (Commercial, SE, SOLAR)
('RD-031', 'MTR-006', '2024-05-01',  3,  4.80, 0.00, '2024-06-01T00:00:00'),
('RD-032', 'MTR-006', '2024-05-01', 10, 11.20, 5.50, '2024-06-01T00:00:00'),
('RD-033', 'MTR-006', '2024-05-01', 14, 13.50, 7.20, '2024-06-01T00:00:00'),
('RD-034', 'MTR-006', '2024-05-01', 20,  7.60, 0.00, '2024-06-01T00:00:00'),
('RD-035', 'MTR-006', '2024-05-15', 11, 12.00, 6.00, '2024-06-01T00:00:00'),
('RD-036', 'MTR-006', '2024-05-15', 15, 14.20, 7.80, '2024-06-01T00:00:00'),

-- MTR-007 (Residential, MW, no solar)
('RD-037', 'MTR-007', '2024-05-01',  0,  0.30, 0.00, '2024-06-01T00:00:00'),
('RD-038', 'MTR-007', '2024-05-01',  7,  1.10, 0.00, '2024-06-01T00:00:00'),
('RD-039', 'MTR-007', '2024-05-01', 13,  0.90, 0.00, '2024-06-01T00:00:00'),
('RD-040', 'MTR-007', '2024-05-01', 19,  1.95, 0.00, '2024-06-01T00:00:00'),
('RD-041', 'MTR-007', '2024-05-15',  6,  0.85, 0.00, '2024-06-01T00:00:00'),
('RD-042', 'MTR-007', '2024-05-15', 12,  1.00, 0.00, '2024-06-01T00:00:00'),

-- MTR-008 (TOU Dynamic, MW, SOLAR)
('RD-043', 'MTR-008', '2024-05-01',  4,  0.48, 0.00, '2024-06-01T00:00:00'),
('RD-044', 'MTR-008', '2024-05-01', 10,  1.30, 1.60, '2024-06-01T00:00:00'),
('RD-045', 'MTR-008', '2024-05-01', 14,  0.95, 2.10, '2024-06-01T00:00:00'),
('RD-046', 'MTR-008', '2024-05-01', 20,  1.75, 0.00, '2024-06-01T00:00:00'),
('RD-047', 'MTR-008', '2024-05-15',  9,  1.15, 1.80, '2024-06-01T00:00:00'),
('RD-048', 'MTR-008', '2024-05-15', 16,  1.05, 1.40, '2024-06-01T00:00:00'),

-- MTR-009 (Commercial, MW, no solar)
('RD-049', 'MTR-009', '2024-05-01',  2,  8.50, 0.00, '2024-06-01T00:00:00'),
('RD-050', 'MTR-009', '2024-05-01',  9, 18.20, 0.00, '2024-06-01T00:00:00'),
('RD-051', 'MTR-009', '2024-05-01', 15, 20.50, 0.00, '2024-06-01T00:00:00'),
('RD-052', 'MTR-009', '2024-05-01', 22,  9.80, 0.00, '2024-06-01T00:00:00'),
('RD-053', 'MTR-009', '2024-05-15', 10, 19.00, 0.00, '2024-06-01T00:00:00'),
('RD-054', 'MTR-009', '2024-05-15', 17, 21.30, 0.00, '2024-06-01T00:00:00'),

-- MTR-010 (Residential, W, SOLAR)
('RD-055', 'MTR-010', '2024-05-01',  1,  0.42, 0.00, '2024-06-01T00:00:00'),
('RD-056', 'MTR-010', '2024-05-01',  9,  0.88, 2.80, '2024-06-01T00:00:00'),
('RD-057', 'MTR-010', '2024-05-01', 13,  0.75, 3.50, '2024-06-01T00:00:00'),
('RD-058', 'MTR-010', '2024-05-01', 17,  1.60, 1.20, '2024-06-01T00:00:00'),
('RD-059', 'MTR-010', '2024-05-15', 11,  0.92, 3.00, '2024-06-01T00:00:00'),
('RD-060', 'MTR-010', '2024-05-15', 15,  0.80, 2.50, '2024-06-01T00:00:00'),

-- MTR-011 (TOU Dynamic, W, SOLAR)
('RD-061', 'MTR-011', '2024-05-01',  3,  0.55, 0.00, '2024-06-01T00:00:00'),
('RD-062', 'MTR-011', '2024-05-01', 10,  1.25, 2.40, '2024-06-01T00:00:00'),
('RD-063', 'MTR-011', '2024-05-01', 14,  1.00, 3.00, '2024-06-01T00:00:00'),
('RD-064', 'MTR-011', '2024-05-01', 18,  1.65, 0.60, '2024-06-01T00:00:00'),
('RD-065', 'MTR-011', '2024-05-15', 12,  1.10, 2.80, '2024-06-01T00:00:00'),
('RD-066', 'MTR-011', '2024-05-15', 16,  0.95, 2.10, '2024-06-01T00:00:00'),

-- MTR-012 (Commercial, W, no solar)
('RD-067', 'MTR-012', '2024-05-01',  5,  7.80, 0.00, '2024-06-01T00:00:00'),
('RD-068', 'MTR-012', '2024-05-01', 11, 16.50, 0.00, '2024-06-01T00:00:00'),
('RD-069', 'MTR-012', '2024-05-01', 15, 19.20, 0.00, '2024-06-01T00:00:00'),
('RD-070', 'MTR-012', '2024-05-01', 21, 10.40, 0.00, '2024-06-01T00:00:00'),
('RD-071', 'MTR-012', '2024-05-15',  9, 17.00, 0.00, '2024-06-01T00:00:00'),
('RD-072', 'MTR-012', '2024-05-15', 14, 18.80, 0.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 72
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_readings;

