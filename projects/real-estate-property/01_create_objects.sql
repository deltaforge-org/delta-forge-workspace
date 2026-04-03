-- =============================================================================
-- Real Estate Property Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: SCD2 (property listing price history), bloom filters (property_id),
-- RESTORE (rollback bad import), pseudonymisation (HASH buyer/seller names)

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw property listings and transaction data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'SCD2 property dimension and enriched transactions';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Real estate star schema and market KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_properties (
    property_id         STRING      NOT NULL,
    address             STRING,
    city                STRING,
    state               STRING,
    zip                 STRING,
    neighborhood_id     STRING,
    property_type       STRING,
    bedrooms            INT,
    bathrooms           DECIMAL(3,1),
    sqft                INT,
    lot_acres           DECIMAL(6,3),
    year_built          INT,
    list_price          DECIMAL(12,2),
    listing_status      STRING,
    list_date           STRING,
    seller_name         STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/property/raw_properties';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_properties TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_transactions (
    transaction_id      STRING      NOT NULL,
    property_id         STRING      NOT NULL,
    buyer_id            STRING      NOT NULL,
    agent_id            STRING      NOT NULL,
    transaction_date    STRING,
    sale_price          DECIMAL(12,2),
    financing_type      STRING,
    closing_costs       DECIMAL(10,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/property/raw_transactions';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_transactions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_buyers (
    buyer_id            STRING      NOT NULL,
    buyer_name          STRING,
    buyer_type          STRING,
    pre_approved_flag   BOOLEAN,
    budget_range        STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/property/raw_buyers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_buyers TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_agents (
    agent_id            STRING      NOT NULL,
    agent_name          STRING      NOT NULL,
    brokerage           STRING,
    license_number      STRING,
    years_experience    INT,
    specialization      STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/property/raw_agents';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_agents TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_neighborhoods (
    neighborhood_id     STRING      NOT NULL,
    neighborhood_name   STRING      NOT NULL,
    city                STRING,
    state               STRING,
    median_income       DECIMAL(10,2),
    school_rating       INT,
    crime_index         DECIMAL(4,2),
    walkability_score   INT,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/property/raw_neighborhoods';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_neighborhoods TO USER {{current_user}};

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.property_scd2 (
    surrogate_key       INT         NOT NULL,
    property_id         STRING      NOT NULL,
    address             STRING,
    city                STRING,
    state               STRING,
    zip                 STRING,
    neighborhood_id     STRING,
    property_type       STRING,
    bedrooms            INT,
    bathrooms           DECIMAL(3,1),
    sqft                INT,
    lot_acres           DECIMAL(6,3),
    year_built          INT,
    list_price          DECIMAL(12,2),
    listing_status      STRING,
    seller_name         STRING,
    valid_from          DATE        NOT NULL,
    valid_to            DATE,
    is_current          BOOLEAN     NOT NULL
) LOCATION '{{data_path}}/silver/property/property_scd2'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.property_scd2 TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.transaction_enriched (
    transaction_id      STRING      NOT NULL,
    property_id         STRING      NOT NULL,
    buyer_id            STRING      NOT NULL,
    agent_id            STRING      NOT NULL,
    transaction_date    DATE,
    list_price          DECIMAL(12,2),
    sale_price          DECIMAL(12,2),
    price_per_sqft      DECIMAL(8,2),
    days_on_market      INT,
    over_asking_pct     DECIMAL(5,2),
    financing_type      STRING
) LOCATION '{{data_path}}/silver/property/transaction_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.transaction_enriched TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_transactions (
    transaction_key     INT         NOT NULL,
    property_key        INT         NOT NULL,
    buyer_key           INT         NOT NULL,
    seller_key          INT,
    agent_key           INT         NOT NULL,
    transaction_date    DATE,
    list_price          DECIMAL(12,2),
    sale_price          DECIMAL(12,2),
    price_per_sqft      DECIMAL(8,2),
    days_on_market      INT,
    over_asking_pct     DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/property/fact_transactions';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_transactions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_property (
    surrogate_key       INT         NOT NULL,
    property_id         STRING      NOT NULL,
    address             STRING,
    city                STRING,
    state               STRING,
    zip                 STRING,
    property_type       STRING,
    bedrooms            INT,
    bathrooms           DECIMAL(3,1),
    sqft                INT,
    lot_acres           DECIMAL(6,3),
    year_built          INT,
    list_price          DECIMAL(12,2),
    valid_from          DATE,
    valid_to            DATE,
    is_current          BOOLEAN
) LOCATION '{{data_path}}/gold/property/dim_property';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_property TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_buyer (
    buyer_key           INT         NOT NULL,
    name                STRING,
    buyer_type          STRING,
    pre_approved_flag   BOOLEAN,
    budget_range        STRING
) LOCATION '{{data_path}}/gold/property/dim_buyer';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_buyer TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_agent (
    agent_key           INT         NOT NULL,
    name                STRING      NOT NULL,
    brokerage           STRING,
    license_number      STRING,
    years_experience    INT,
    specialization      STRING
) LOCATION '{{data_path}}/gold/property/dim_agent';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_agent TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_neighborhood (
    neighborhood_key    INT         NOT NULL,
    name                STRING      NOT NULL,
    city                STRING,
    state               STRING,
    median_income       DECIMAL(10,2),
    school_rating       INT,
    crime_index         DECIMAL(4,2),
    walkability_score   INT
) LOCATION '{{data_path}}/gold/property/dim_neighborhood';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_neighborhood TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_market_trends (
    city                STRING      NOT NULL,
    property_type       STRING      NOT NULL,
    quarter             STRING      NOT NULL,
    median_sale_price   DECIMAL(12,2),
    avg_price_per_sqft  DECIMAL(8,2),
    avg_days_on_market  DECIMAL(5,1),
    total_transactions  INT,
    over_asking_pct     DECIMAL(5,2),
    inventory_months    DECIMAL(5,1),
    price_change_yoy_pct DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/property/kpi_market_trends';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_market_trends TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: NEIGHBORHOODS (5 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_neighborhoods VALUES
    ('NBH-OHP', 'Oak Hill Park',     'Austin',      'TX', 125000.00, 9, 1.20, 82, '2025-01-01T00:00:00'),
    ('NBH-LSQ', 'Lakeside Square',   'Austin',      'TX', 98000.00,  7, 2.10, 71, '2025-01-01T00:00:00'),
    ('NBH-MVW', 'Mountain View',     'Denver',      'CO', 110000.00, 8, 1.50, 75, '2025-01-01T00:00:00'),
    ('NBH-RVB', 'Riverbank Estates', 'Denver',      'CO', 145000.00, 9, 0.80, 65, '2025-01-01T00:00:00'),
    ('NBH-SUN', 'Sunset Ridge',      'Phoenix',     'AZ', 85000.00,  6, 2.50, 58, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_neighborhoods;


-- ===================== BRONZE SEED DATA: AGENTS (6 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_agents VALUES
    ('AGT-01', 'Jennifer Walsh',    'Premier Realty',       'TX-RE-44521', 15, 'Luxury',       '2025-01-01T00:00:00'),
    ('AGT-02', 'Michael Chen',      'Premier Realty',       'TX-RE-33892', 8,  'Residential',  '2025-01-01T00:00:00'),
    ('AGT-03', 'Sarah Martinez',    'Horizon Properties',   'CO-RE-55673', 12, 'Residential',  '2025-01-01T00:00:00'),
    ('AGT-04', 'David Park',        'Horizon Properties',   'CO-RE-22104', 6,  'Condos',       '2025-01-01T00:00:00'),
    ('AGT-05', 'Lisa Thompson',     'Desert Sun Realty',    'AZ-RE-88745', 10, 'Investment',   '2025-01-01T00:00:00'),
    ('AGT-06', 'Robert Kim',        'Desert Sun Realty',    'AZ-RE-99123', 4,  'Residential',  '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_agents;


-- ===================== BRONZE SEED DATA: BUYERS (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_buyers VALUES
    ('BUY-01', 'Amanda Foster',     'First-time',  true,  '$300K-$450K',   '2025-01-01T00:00:00'),
    ('BUY-02', 'Thomas Wright',     'Move-up',     true,  '$450K-$650K',   '2025-01-01T00:00:00'),
    ('BUY-03', 'Jessica Nguyen',    'First-time',  true,  '$250K-$400K',   '2025-01-01T00:00:00'),
    ('BUY-04', 'Ryan Patel',        'Investor',    true,  '$500K-$800K',   '2025-01-01T00:00:00'),
    ('BUY-05', 'Megan Clark',       'Move-up',     true,  '$600K-$900K',   '2025-01-01T00:00:00'),
    ('BUY-06', 'Daniel Lee',        'First-time',  false, '$200K-$350K',   '2025-01-01T00:00:00'),
    ('BUY-07', 'Olivia Robinson',   'Investor',    true,  '$400K-$700K',   '2025-01-01T00:00:00'),
    ('BUY-08', 'James Wilson',      'Move-up',     true,  '$550K-$800K',   '2025-01-01T00:00:00'),
    ('BUY-09', 'Sophia Anderson',   'First-time',  true,  '$300K-$500K',   '2025-01-01T00:00:00'),
    ('BUY-10', 'William Taylor',    'Investor',    true,  '$700K-$1.2M',   '2025-01-01T00:00:00'),
    ('BUY-11', 'Emily Garcia',      'First-time',  false, '$250K-$400K',   '2025-01-01T00:00:00'),
    ('BUY-12', 'Christopher Brown', 'Move-up',     true,  '$500K-$750K',   '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_buyers;


-- ===================== BRONZE SEED DATA: PROPERTIES (15 rows) =====================
-- Across 5 neighborhoods. list_date and list_price will change over time for SCD2.

INSERT INTO {{zone_prefix}}.bronze.raw_properties VALUES
    ('PROP-001', '142 Oak Hill Dr',      'Austin',  'TX', '78745', 'NBH-OHP', 'Single Family', 4, 3.0, 2850, 0.280, 2018, 525000.00,  'Active',  '2023-03-10', 'Margaret Henderson', '2025-01-01T00:00:00'),
    ('PROP-002', '88 Lakeside Blvd',     'Austin',  'TX', '78748', 'NBH-LSQ', 'Single Family', 3, 2.0, 1950, 0.180, 2005, 389000.00,  'Active',  '2023-04-15', 'William Chang',      '2025-01-01T00:00:00'),
    ('PROP-003', '520 Mountain View Ct', 'Denver',  'CO', '80220', 'NBH-MVW', 'Single Family', 4, 2.5, 2400, 0.220, 2015, 620000.00,  'Active',  '2023-02-01', 'Patricia Kowalski',  '2025-01-01T00:00:00'),
    ('PROP-004', '15 Riverbank Ln',      'Denver',  'CO', '80210', 'NBH-RVB', 'Single Family', 5, 3.5, 3500, 0.450, 2020, 875000.00,  'Active',  '2023-05-20', 'James Rivera',       '2025-01-01T00:00:00'),
    ('PROP-005', '301 Sunset Ridge Way', 'Phoenix', 'AZ', '85044', 'NBH-SUN', 'Single Family', 3, 2.0, 1800, 0.150, 2010, 345000.00,  'Active',  '2023-06-01', 'Barbara Nguyen',     '2025-01-01T00:00:00'),
    ('PROP-006', '78 Oak Hill Ct',       'Austin',  'TX', '78745', 'NBH-OHP', 'Townhouse',     3, 2.5, 2100, 0.100, 2019, 425000.00,  'Active',  '2023-07-10', 'Robert Thompson',    '2025-01-01T00:00:00'),
    ('PROP-007', '225 Lakeside Ave',     'Austin',  'TX', '78748', 'NBH-LSQ', 'Condo',         2, 2.0, 1200, 0.000, 2021, 285000.00,  'Active',  '2023-03-25', 'Linda Petrov',       '2025-01-01T00:00:00'),
    ('PROP-008', '900 Mountain View Dr', 'Denver',  'CO', '80220', 'NBH-MVW', 'Single Family', 3, 2.0, 2000, 0.200, 2008, 485000.00,  'Active',  '2023-08-15', 'Michael Garcia',     '2025-01-01T00:00:00'),
    ('PROP-009', '42 Riverbank Ct',      'Denver',  'CO', '80210', 'NBH-RVB', 'Townhouse',     3, 2.5, 2200, 0.120, 2022, 595000.00,  'Active',  '2023-04-01', 'Jennifer Adams',     '2025-01-01T00:00:00'),
    ('PROP-010', '567 Sunset Blvd',      'Phoenix', 'AZ', '85044', 'NBH-SUN', 'Single Family', 4, 3.0, 2600, 0.250, 2016, 420000.00,  'Active',  '2023-09-01', 'Richard Kim',        '2025-01-01T00:00:00'),
    ('PROP-011', '33 Oak Hill Ln',       'Austin',  'TX', '78745', 'NBH-OHP', 'Single Family', 5, 4.0, 3800, 0.500, 2021, 785000.00,  'Active',  '2024-01-15', 'Karen Singh',        '2025-01-01T00:00:00'),
    ('PROP-012', '110 Mountain View Pl', 'Denver',  'CO', '80220', 'NBH-MVW', 'Condo',         2, 1.5, 1100, 0.000, 2023, 365000.00,  'Active',  '2024-02-01', 'Leo Yamamoto',       '2025-01-01T00:00:00'),
    ('PROP-013', '88 Sunset Ct',         'Phoenix', 'AZ', '85044', 'NBH-SUN', 'Condo',         2, 2.0, 1050, 0.000, 2020, 245000.00,  'Active',  '2024-03-10', 'Nathan Brooks',      '2025-01-01T00:00:00'),
    ('PROP-014', '450 Lakeside Dr',      'Austin',  'TX', '78748', 'NBH-LSQ', 'Single Family', 3, 2.5, 2200, 0.200, 2012, 410000.00,  'Active',  '2024-04-01', 'Olivia Schmidt',     '2025-01-01T00:00:00'),
    ('PROP-015', '72 Riverbank Way',     'Denver',  'CO', '80210', 'NBH-RVB', 'Single Family', 4, 3.0, 2900, 0.350, 2017, 725000.00,  'Active',  '2024-05-15', 'Paul Rivera',        '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_properties;


-- ===================== BRONZE SEED DATA: PRICE CHANGES (for SCD2 tracking) =====================
-- Properties with reduced listing prices after time on market

INSERT INTO {{zone_prefix}}.bronze.raw_properties VALUES
    ('PROP-002', '88 Lakeside Blvd',     'Austin',  'TX', '78748', 'NBH-LSQ', 'Single Family', 3, 2.0, 1950, 0.180, 2005, 369000.00,  'Active',  '2023-06-01', 'William Chang',      '2025-01-01T00:00:00'),
    ('PROP-005', '301 Sunset Ridge Way', 'Phoenix', 'AZ', '85044', 'NBH-SUN', 'Single Family', 3, 2.0, 1800, 0.150, 2010, 329000.00,  'Active',  '2023-08-01', 'Barbara Nguyen',     '2025-01-01T00:00:00'),
    ('PROP-008', '900 Mountain View Dr', 'Denver',  'CO', '80220', 'NBH-MVW', 'Single Family', 3, 2.0, 2000, 0.200, 2008, 465000.00,  'Active',  '2023-10-01', 'Michael Garcia',     '2025-01-01T00:00:00'),
    ('PROP-010', '567 Sunset Blvd',      'Phoenix', 'AZ', '85044', 'NBH-SUN', 'Single Family', 4, 3.0, 2600, 0.250, 2016, 399000.00,  'Active',  '2023-11-01', 'Richard Kim',        '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_properties;


-- ===================== BRONZE SEED DATA: TRANSACTIONS (22 rows) =====================
-- Sales across 2023-2024. Some over asking (bidding wars), some under.

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
    ('TXN-001', 'PROP-001', 'BUY-02', 'AGT-01', '2023-05-15', 540000.00,  'Conventional', 12500.00, '2025-01-01T00:00:00'),
    ('TXN-002', 'PROP-002', 'BUY-01', 'AGT-02', '2023-07-20', 365000.00,  'FHA',          8800.00,  '2025-01-01T00:00:00'),
    ('TXN-003', 'PROP-003', 'BUY-05', 'AGT-03', '2023-04-10', 645000.00,  'Conventional', 15200.00, '2025-01-01T00:00:00'),
    ('TXN-004', 'PROP-004', 'BUY-10', 'AGT-03', '2023-07-30', 860000.00,  'Jumbo',        22000.00, '2025-01-01T00:00:00'),
    ('TXN-005', 'PROP-005', 'BUY-03', 'AGT-05', '2023-09-10', 322000.00,  'VA',           7500.00,  '2025-01-01T00:00:00'),
    ('TXN-006', 'PROP-006', 'BUY-09', 'AGT-02', '2023-09-25', 435000.00,  'Conventional', 10500.00, '2025-01-01T00:00:00'),
    ('TXN-007', 'PROP-007', 'BUY-06', 'AGT-01', '2023-05-30', 290000.00,  'Conventional', 7000.00,  '2025-01-01T00:00:00'),
    ('TXN-008', 'PROP-008', 'BUY-08', 'AGT-04', '2023-11-15', 458000.00,  'Conventional', 11200.00, '2025-01-01T00:00:00'),
    ('TXN-009', 'PROP-009', 'BUY-04', 'AGT-03', '2023-06-15', 610000.00,  'Conventional', 14800.00, '2025-01-01T00:00:00'),
    ('TXN-010', 'PROP-010', 'BUY-07', 'AGT-05', '2023-12-20', 395000.00,  'Conventional', 9500.00,  '2025-01-01T00:00:00'),
    -- 2024 transactions
    ('TXN-011', 'PROP-011', 'BUY-10', 'AGT-01', '2024-03-20', 810000.00,  'Jumbo',        20500.00, '2025-01-01T00:00:00'),
    ('TXN-012', 'PROP-012', 'BUY-11', 'AGT-04', '2024-04-15', 372000.00,  'FHA',          9000.00,  '2025-01-01T00:00:00'),
    ('TXN-013', 'PROP-013', 'BUY-06', 'AGT-06', '2024-05-10', 252000.00,  'Conventional', 6100.00,  '2025-01-01T00:00:00'),
    ('TXN-014', 'PROP-014', 'BUY-12', 'AGT-02', '2024-06-20', 425000.00,  'Conventional', 10200.00, '2025-01-01T00:00:00'),
    ('TXN-015', 'PROP-015', 'BUY-05', 'AGT-03', '2024-07-30', 748000.00,  'Conventional', 18200.00, '2025-01-01T00:00:00'),
    -- Repeat buyer: BUY-04 buys another investment property
    ('TXN-016', 'PROP-001', 'BUY-04', 'AGT-01', '2024-08-15', 565000.00,  'Conventional', 13500.00, '2025-01-01T00:00:00'),
    ('TXN-017', 'PROP-003', 'BUY-08', 'AGT-03', '2024-09-01', 680000.00,  'Conventional', 16500.00, '2025-01-01T00:00:00'),
    ('TXN-018', 'PROP-006', 'BUY-01', 'AGT-02', '2024-10-10', 455000.00,  'Conventional', 11000.00, '2025-01-01T00:00:00'),
    ('TXN-019', 'PROP-007', 'BUY-09', 'AGT-01', '2024-05-20', 298000.00,  'FHA',          7200.00,  '2025-01-01T00:00:00'),
    ('TXN-020', 'PROP-009', 'BUY-12', 'AGT-04', '2024-11-15', 635000.00,  'Conventional', 15500.00, '2025-01-01T00:00:00'),
    ('TXN-021', 'PROP-005', 'BUY-11', 'AGT-06', '2024-07-01', 340000.00,  'VA',           8200.00,  '2025-01-01T00:00:00'),
    ('TXN-022', 'PROP-010', 'BUY-03', 'AGT-05', '2024-12-01', 418000.00,  'Conventional', 10100.00, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 22
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_transactions;


-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.property_scd2 (seller_name) TRANSFORM keyed_hash PARAMS (salt = 'delta_forge_salt_2024');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_buyer (name) TRANSFORM keyed_hash PARAMS (salt = 'delta_forge_salt_2024');
