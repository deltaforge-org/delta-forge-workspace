-- =============================================================================
-- Omnichannel E-Commerce Orders Pipeline: Object Creation & Seed Data
-- =============================================================================
-- Narrative: You are a data engineer at an omnichannel retailer. Orders arrive
-- from 3 systems (web, mobile app, POS terminals). This pipeline merges them
-- into a unified order stream, handles cancellations as soft deletes, builds
-- RFM customer segmentation, detects funnel drop-offs via session window
-- analysis, and feeds inventory adjustments via CDF.
-- =============================================================================

-- ===================== ZONE =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Omnichannel e-commerce project zone';

-- ===================== SCHEMAS =====================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw order feeds from web, mobile, POS, browsing events';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Unified orders, RFM scoring, inventory CDF, sessions';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for sales, funnel, and channel analytics';

-- ===================== BRONZE TABLES =====================

-- Customers dimension feed (18 rows)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_customers (
    customer_id       STRING      NOT NULL,
    email             STRING      NOT NULL,
    first_name        STRING,
    last_name         STRING,
    segment           STRING,
    city              STRING,
    state             STRING,
    country           STRING,
    address           STRING,
    loyalty_tier      STRING,
    registration_date DATE,
    source_system     STRING,
    ingested_at       TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_customers';

-- Products dimension feed (20 rows)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_products (
    product_id   STRING      NOT NULL,
    sku          STRING      NOT NULL,
    product_name STRING,
    category     STRING,
    subcategory  STRING,
    brand        STRING,
    unit_cost    DECIMAL(10,2),
    list_price   DECIMAL(10,2),
    weight_kg    DECIMAL(6,2),
    ingested_at  TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_products';

-- Web channel orders (30 rows)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_web_orders (
    order_id      STRING      NOT NULL,
    customer_id   STRING      NOT NULL,
    product_id    STRING      NOT NULL,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    discount_pct  DECIMAL(5,2),
    shipping_cost DECIMAL(10,2),
    order_date    DATE,
    status        STRING,
    session_id    STRING,
    browser       STRING,
    ingested_at   TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_web_orders';

-- Mobile app orders (20 rows)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_mobile_orders (
    order_id      STRING      NOT NULL,
    customer_id   STRING      NOT NULL,
    product_id    STRING      NOT NULL,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    discount_pct  DECIMAL(5,2),
    shipping_cost DECIMAL(10,2),
    order_date    DATE,
    status        STRING,
    session_id    STRING,
    app_version   STRING,
    ingested_at   TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_mobile_orders';

-- POS terminal orders (25 rows)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_pos_orders (
    order_id      STRING      NOT NULL,
    customer_id   STRING      NOT NULL,
    product_id    STRING      NOT NULL,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    discount_pct  DECIMAL(5,2),
    shipping_cost DECIMAL(10,2),
    order_date    DATE,
    status        STRING,
    store_id      STRING,
    terminal_id   STRING,
    ingested_at   TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_pos_orders';

-- Browsing events for funnel sessionization
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_browsing_events (
    event_id      STRING      NOT NULL,
    customer_id   STRING      NOT NULL,
    session_id    STRING,
    event_type    STRING      NOT NULL,
    event_ts      TIMESTAMP   NOT NULL,
    page_url      STRING,
    product_id    STRING,
    ingested_at   TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_browsing_events';

-- ===================== SILVER TABLES =====================

-- Unified orders from 3 channels, soft-delete for cancellations, CDF-enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.orders_unified (
    order_id        STRING      NOT NULL,
    customer_id     STRING      NOT NULL,
    product_id      STRING      NOT NULL,
    channel         STRING      NOT NULL,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount_pct    DECIMAL(5,2),
    line_total      DECIMAL(12,2),
    shipping_cost   DECIMAL(10,2),
    order_date      DATE,
    status          STRING,
    is_deleted      BOOLEAN     DEFAULT false,
    cancelled_at    TIMESTAMP,
    store_id        STRING,
    session_id      STRING,
    updated_at      TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/orders_unified'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Customer RFM segmentation with NTILE quartile scoring
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.customer_rfm (
    customer_id      STRING      NOT NULL,
    email            STRING,
    first_name       STRING,
    last_name        STRING,
    segment          STRING,
    city             STRING,
    state            STRING,
    country          STRING,
    registration_date DATE,
    total_orders     INT,
    total_revenue    DECIMAL(14,2),
    last_order_date  DATE,
    recency_days     INT,
    r_score          INT,
    f_score          INT,
    m_score          INT,
    rfm_total        INT,
    rfm_segment      STRING,
    updated_at       TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/customer_rfm';

-- Inventory adjustments driven by CDF on orders_unified
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.inventory_adjustments (
    adjustment_id   STRING      NOT NULL,
    product_id      STRING      NOT NULL,
    order_id        STRING      NOT NULL,
    change_type     STRING      NOT NULL,
    quantity_delta  INT         NOT NULL,
    reason          STRING,
    captured_at     TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/inventory_adjustments';

-- Sessionized browsing events for funnel analysis
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.sessions (
    session_key     STRING      NOT NULL,
    customer_id     STRING      NOT NULL,
    session_id      STRING,
    session_start   TIMESTAMP,
    session_end     TIMESTAMP,
    event_count     INT,
    browse_count    INT,
    cart_count      INT,
    checkout_count  INT,
    purchase_count  INT,
    funnel_stage    STRING,
    processed_at    TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/sessions';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_product (
    product_key   STRING      NOT NULL,
    sku           STRING,
    product_name  STRING,
    category      STRING,
    subcategory   STRING,
    brand         STRING,
    unit_cost     DECIMAL(10,2),
    list_price    DECIMAL(10,2)
) LOCATION '{{data_path}}/ecommerce/gold/dim_product';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_customer (
    customer_key      STRING      NOT NULL,
    email             STRING,
    full_name         STRING,
    segment           STRING,
    city              STRING,
    state             STRING,
    country           STRING,
    loyalty_tier      STRING,
    registration_date DATE,
    lifetime_orders   INT,
    lifetime_revenue  DECIMAL(14,2),
    rfm_segment       STRING
) LOCATION '{{data_path}}/ecommerce/gold/dim_customer';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_channel (
    channel_key   STRING      NOT NULL,
    channel_name  STRING,
    channel_detail STRING
) LOCATION '{{data_path}}/ecommerce/gold/dim_channel';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_date (
    date_key      INT         NOT NULL,
    full_date     DATE        NOT NULL,
    year          INT,
    quarter       INT,
    month         INT,
    month_name    STRING,
    day_of_week   INT,
    day_name      STRING,
    is_weekend    BOOLEAN
) LOCATION '{{data_path}}/ecommerce/gold/dim_date';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_order_lines (
    order_line_key STRING      NOT NULL,
    order_key      STRING      NOT NULL,
    product_key    STRING      NOT NULL,
    customer_key   STRING      NOT NULL,
    channel_key    STRING      NOT NULL,
    date_key       INT,
    order_date     DATE,
    quantity       INT,
    unit_price     DECIMAL(10,2),
    discount_pct   DECIMAL(5,2),
    line_total     DECIMAL(12,2),
    shipping_cost  DECIMAL(10,2),
    status         STRING
) LOCATION '{{data_path}}/ecommerce/gold/fact_order_lines';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_sales_dashboard (
    report_month        DATE,
    channel             STRING,
    total_orders        INT,
    total_revenue       DECIMAL(14,2),
    avg_order_value     DECIMAL(10,2),
    cancelled_orders    INT,
    cancellation_rate   DECIMAL(5,4),
    unique_customers    INT,
    repeat_customers    INT,
    repeat_rate_pct     DECIMAL(5,2)
) LOCATION '{{data_path}}/ecommerce/gold/kpi_sales_dashboard';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_funnel_analysis (
    report_month        DATE,
    total_sessions      INT,
    browse_sessions     INT,
    cart_sessions       INT,
    checkout_sessions   INT,
    purchase_sessions   INT,
    browse_to_cart_pct  DECIMAL(5,2),
    cart_to_checkout_pct DECIMAL(5,2),
    checkout_to_purchase_pct DECIMAL(5,2),
    overall_conversion_pct   DECIMAL(5,2)
) LOCATION '{{data_path}}/ecommerce/gold/kpi_funnel_analysis';

-- ===================== PSEUDONYMISATION =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_customers (email) TRANSFORM redact PARAMS (mask = '***@***.***');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.customer_rfm (email) TRANSFORM redact PARAMS (mask = '***@***.***');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_customers (address) TRANSFORM mask PARAMS (show = 5);

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_customers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_products TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_web_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_mobile_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_pos_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_browsing_events TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.orders_unified TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.customer_rfm TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.inventory_adjustments TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.sessions TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_product TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_customer TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_channel TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_date TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_order_lines TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_sales_dashboard TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_funnel_analysis TO USER {{current_user}};

-- =============================================================================
-- SEED DATA: CUSTOMERS (18 rows)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_customers VALUES
('C001', 'alice.morgan@example.com',   'Alice',   'Morgan',   'Premium',    'New York',      'NY', 'US', '123 5th Ave, New York, NY 10001',       'Gold',    '2023-01-15', 'CRM', '2024-06-01T00:00:00'),
('C002', 'bob.chen@example.com',       'Bob',     'Chen',     'Standard',   'San Francisco', 'CA', 'US', '456 Market St, San Francisco, CA 94105', 'Silver',  '2023-02-20', 'CRM', '2024-06-01T00:00:00'),
('C003', 'carol.davis@example.com',    'Carol',   'Davis',    'Premium',    'Chicago',       'IL', 'US', '789 Michigan Ave, Chicago, IL 60601',   'Gold',    '2023-03-10', 'CRM', '2024-06-01T00:00:00'),
('C004', 'david.kim@example.com',      'David',   'Kim',      'Standard',   'Seattle',       'WA', 'US', '321 Pike St, Seattle, WA 98101',        'Bronze',  '2023-01-05', 'CRM', '2024-06-01T00:00:00'),
('C005', 'elena.russo@example.com',    'Elena',   'Russo',    'Enterprise', 'Boston',        'MA', 'US', '654 Beacon St, Boston, MA 02108',       'Gold',    '2022-11-20', 'CRM', '2024-06-01T00:00:00'),
('C006', 'frank.oconnor@example.com',  'Frank',   'OConnor',  'Standard',   'Austin',        'TX', 'US', '987 Congress Ave, Austin, TX 78701',    'Silver',  '2023-04-01', 'CRM', '2024-06-01T00:00:00'),
('C007', 'grace.patel@example.com',    'Grace',   'Patel',    'Premium',    'Denver',        'CO', 'US', '147 Blake St, Denver, CO 80202',        'Gold',    '2023-02-14', 'CRM', '2024-06-01T00:00:00'),
('C008', 'henry.watson@example.com',   'Henry',   'Watson',   'Standard',   'Portland',      'OR', 'US', '258 Burnside St, Portland, OR 97204',   'Bronze',  '2023-05-10', 'CRM', '2024-06-01T00:00:00'),
('C009', 'irene.lopez@example.com',    'Irene',   'Lopez',    'Enterprise', 'Miami',         'FL', 'US', '369 Ocean Dr, Miami, FL 33139',         'Gold',    '2022-12-01', 'CRM', '2024-06-01T00:00:00'),
('C010', 'james.nguyen@example.com',   'James',   'Nguyen',   'Standard',   'Phoenix',       'AZ', 'US', '741 Camelback Rd, Phoenix, AZ 85014',   'Silver',  '2023-03-25', 'CRM', '2024-06-01T00:00:00'),
('C011', 'karen.smith@example.com',    'Karen',   'Smith',    'Premium',    'Atlanta',       'GA', 'US', '852 Peachtree St, Atlanta, GA 30308',   'Gold',    '2023-01-30', 'CRM', '2024-06-01T00:00:00'),
('C012', 'leo.martinez@example.com',   'Leo',     'Martinez', 'Standard',   'Dallas',        'TX', 'US', '963 Elm St, Dallas, TX 75201',          'Bronze',  '2023-04-15', 'CRM', '2024-06-01T00:00:00'),
('C013', 'maya.johnson@example.com',   'Maya',    'Johnson',  'Enterprise', 'Minneapolis',   'MN', 'US', '159 Hennepin Ave, Minneapolis, MN 55401','Gold',   '2022-10-10', 'CRM', '2024-06-01T00:00:00'),
('C014', 'nathan.brown@example.com',   'Nathan',  'Brown',    'Standard',   'Nashville',     'TN', 'US', '357 Broadway, Nashville, TN 37201',     'Silver',  '2023-05-20', 'CRM', '2024-06-01T00:00:00'),
('C015', 'olivia.taylor@example.com',  'Olivia',  'Taylor',   'Premium',    'San Diego',     'CA', 'US', '468 Harbor Dr, San Diego, CA 92101',    'Gold',    '2023-02-28', 'CRM', '2024-06-01T00:00:00'),
('C016', 'peter.garcia@example.com',   'Peter',   'Garcia',   'Standard',   'Las Vegas',     'NV', 'US', '123 Strip Blvd, Las Vegas, NV 89101',   'Bronze',  '2023-06-01', 'CRM', '2024-06-01T00:00:00'),
('C017', 'quinn.adams@example.com',    'Quinn',   'Adams',    'Premium',    'Charlotte',     'NC', 'US', '456 Trade St, Charlotte, NC 28202',     'Silver',  '2023-03-15', 'CRM', '2024-06-01T00:00:00'),
('C018', 'rachel.wilson@example.com',  'Rachel',  'Wilson',   'Standard',   'Detroit',       'MI', 'US', '789 Woodward Ave, Detroit, MI 48226',   'Bronze',  '2023-07-01', 'CRM', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_customers;

-- =============================================================================
-- SEED DATA: PRODUCTS (20 rows) -- electronics, apparel, groceries
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_products VALUES
('P001', 'SKU-ELEC-001', 'Wireless Headphones',       'Electronics',  'Audio',       'SoundMax',     29.99,   79.99,  0.25, '2024-06-01T00:00:00'),
('P002', 'SKU-ELEC-002', 'Bluetooth Speaker',          'Electronics',  'Audio',       'SoundMax',     45.00,  129.99,  1.20, '2024-06-01T00:00:00'),
('P003', 'SKU-ELEC-003', 'USB-C Hub 7-Port',           'Electronics',  'Accessories', 'TechLink',     12.50,   39.99,  0.15, '2024-06-01T00:00:00'),
('P004', 'SKU-ELEC-004', 'Mechanical Keyboard',        'Electronics',  'Peripherals', 'KeyCraft',     35.00,   89.99,  0.90, '2024-06-01T00:00:00'),
('P005', 'SKU-ELEC-005', '4K Webcam',                  'Electronics',  'Peripherals', 'VisionPro',    40.00,  119.99,  0.30, '2024-06-01T00:00:00'),
('P006', 'SKU-ELEC-006', 'Smart Thermostat',           'Electronics',  'Smart Home',  'EcoSmart',     55.00,  149.99,  0.45, '2024-06-01T00:00:00'),
('P007', 'SKU-ELEC-007', 'Robot Vacuum',               'Electronics',  'Cleaning',    'CleanBot',    120.00,  349.99,  4.50, '2024-06-01T00:00:00'),
('P008', 'SKU-APRL-001', 'Running Shoes Mens',         'Apparel',      'Footwear',    'StridePro',    38.00,   99.99,  0.80, '2024-06-01T00:00:00'),
('P009', 'SKU-APRL-002', 'Yoga Pants Womens',          'Apparel',      'Activewear',  'FlexFit',      15.00,   49.99,  0.30, '2024-06-01T00:00:00'),
('P010', 'SKU-APRL-003', 'Winter Jacket Unisex',       'Apparel',      'Outerwear',   'NorthTrail',   45.00,  149.99,  1.50, '2024-06-01T00:00:00'),
('P011', 'SKU-APRL-004', 'Cotton T-Shirt Pack',        'Apparel',      'Basics',      'ComfortCo',     8.00,   29.99,  0.40, '2024-06-01T00:00:00'),
('P012', 'SKU-APRL-005', 'Denim Jeans Slim',           'Apparel',      'Bottoms',     'UrbanDenim',   20.00,   69.99,  0.70, '2024-06-01T00:00:00'),
('P013', 'SKU-GROC-001', 'Organic Coffee Beans 1kg',   'Groceries',    'Beverages',   'BeanCraft',     6.00,   18.99,  1.00, '2024-06-01T00:00:00'),
('P014', 'SKU-GROC-002', 'Extra Virgin Olive Oil 1L',  'Groceries',    'Cooking',     'MedHarvest',    5.00,   14.99,  1.00, '2024-06-01T00:00:00'),
('P015', 'SKU-GROC-003', 'Protein Bar Box (12pk)',     'Groceries',    'Snacks',      'FitFuel',       4.00,   24.99,  0.80, '2024-06-01T00:00:00'),
('P016', 'SKU-GROC-004', 'Almond Milk 6-Pack',         'Groceries',    'Dairy Alt',   'NutriFlow',     5.50,   19.99,  3.60, '2024-06-01T00:00:00'),
('P017', 'SKU-GROC-005', 'Whole Grain Pasta 5-Pack',   'Groceries',    'Pantry',      'PastaRoma',     3.00,   12.99,  2.50, '2024-06-01T00:00:00'),
('P018', 'SKU-HOME-001', 'LED Desk Lamp',              'Home',         'Lighting',    'LumiTech',      8.00,   34.99,  0.60, '2024-06-01T00:00:00'),
('P019', 'SKU-HOME-002', 'Ergonomic Office Chair',     'Home',         'Furniture',   'ComfortPro',  150.00,  399.99, 15.00, '2024-06-01T00:00:00'),
('P020', 'SKU-HOME-003', 'Standing Desk Converter',    'Home',         'Furniture',   'ComfortPro',   85.00,  229.99,  8.00, '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_products;

-- =============================================================================
-- SEED DATA: WEB ORDERS (30 rows) -- Jan-Jun 2024
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_web_orders VALUES
('WEB-001', 'C001', 'P001', 2,  79.99,  0.00,  5.99, '2024-01-05', 'delivered',  'WS-A001-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-002', 'C001', 'P003', 1,  39.99,  0.10,  0.00, '2024-01-05', 'delivered',  'WS-A001-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-003', 'C003', 'P019', 1, 399.99,  0.05, 29.99, '2024-01-12', 'delivered',  'WS-A003-01', 'Firefox', '2024-06-01T00:00:00'),
('WEB-004', 'C005', 'P007', 1, 349.99,  0.00, 12.99, '2024-01-18', 'delivered',  'WS-A005-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-005', 'C009', 'P020', 1, 229.99,  0.10, 15.99, '2024-01-25', 'cancelled',  'WS-A009-01', 'Safari',  '2024-06-01T00:00:00'),
('WEB-006', 'C011', 'P005', 1, 119.99,  0.00,  7.99, '2024-02-02', 'delivered',  'WS-A011-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-007', 'C013', 'P004', 1,  89.99,  0.00,  5.99, '2024-02-10', 'delivered',  'WS-A013-01', 'Edge',    '2024-06-01T00:00:00'),
('WEB-008', 'C002', 'P010', 1, 149.99,  0.15,  8.99, '2024-02-15', 'delivered',  'WS-A002-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-009', 'C007', 'P006', 1, 149.99,  0.10,  8.99, '2024-02-22', 'delivered',  'WS-A007-01', 'Firefox', '2024-06-01T00:00:00'),
('WEB-010', 'C015', 'P002', 2, 129.99,  0.00,  6.99, '2024-03-01', 'delivered',  'WS-A015-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-011', 'C001', 'P013', 3,  18.99,  0.00,  3.99, '2024-03-08', 'delivered',  'WS-A001-02', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-012', 'C004', 'P018', 1,  34.99,  0.00,  3.99, '2024-03-12', 'delivered',  'WS-A004-01', 'Safari',  '2024-06-01T00:00:00'),
('WEB-013', 'C006', 'P012', 2,  69.99,  0.05,  5.99, '2024-03-20', 'cancelled',  'WS-A006-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-014', 'C010', 'P008', 1,  99.99,  0.00,  7.99, '2024-03-28', 'delivered',  'WS-A010-01', 'Edge',    '2024-06-01T00:00:00'),
('WEB-015', 'C003', 'P015', 2,  24.99,  0.00,  3.99, '2024-04-02', 'delivered',  'WS-A003-02', 'Firefox', '2024-06-01T00:00:00'),
('WEB-016', 'C005', 'P014', 1,  14.99,  0.00,  2.99, '2024-04-08', 'delivered',  'WS-A005-02', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-017', 'C009', 'P002', 1, 129.99,  0.00,  6.99, '2024-04-15', 'delivered',  'WS-A009-02', 'Safari',  '2024-06-01T00:00:00'),
('WEB-018', 'C011', 'P017', 4,  12.99,  0.10,  4.99, '2024-04-22', 'delivered',  'WS-A011-02', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-019', 'C014', 'P001', 1,  79.99,  0.00,  5.99, '2024-04-28', 'delivered',  'WS-A014-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-020', 'C016', 'P009', 2,  49.99,  0.00,  4.99, '2024-05-03', 'delivered',  'WS-A016-01', 'Firefox', '2024-06-01T00:00:00'),
('WEB-021', 'C001', 'P006', 1, 149.99,  0.05,  8.99, '2024-05-10', 'delivered',  'WS-A001-03', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-022', 'C017', 'P004', 1,  89.99,  0.00,  5.99, '2024-05-15', 'delivered',  'WS-A017-01', 'Edge',    '2024-06-01T00:00:00'),
('WEB-023', 'C007', 'P011', 3,  29.99,  0.00,  3.99, '2024-05-20', 'cancelled',  'WS-A007-02', 'Firefox', '2024-06-01T00:00:00'),
('WEB-024', 'C012', 'P016', 2,  19.99,  0.00,  3.99, '2024-05-25', 'delivered',  'WS-A012-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-025', 'C003', 'P002', 1, 129.99,  0.00,  6.99, '2024-06-01', 'delivered',  'WS-A003-03', 'Firefox', '2024-06-01T00:00:00'),
('WEB-026', 'C005', 'P001', 2,  79.99,  0.00,  5.99, '2024-06-06', 'delivered',  'WS-A005-03', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-027', 'C013', 'P019', 1, 399.99,  0.10, 29.99, '2024-06-12', 'delivered',  'WS-A013-02', 'Edge',    '2024-06-01T00:00:00'),
('WEB-028', 'C008', 'P003', 1,  39.99,  0.00,  3.99, '2024-06-18', 'delivered',  'WS-A008-01', 'Chrome',  '2024-06-01T00:00:00'),
('WEB-029', 'C018', 'P010', 1, 149.99,  0.20,  8.99, '2024-06-24', 'delivered',  'WS-A018-01', 'Firefox', '2024-06-01T00:00:00'),
('WEB-030', 'C015', 'P007', 1, 349.99,  0.00, 12.99, '2024-06-28', 'delivered',  'WS-A015-02', 'Chrome',  '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 30
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_web_orders;

-- =============================================================================
-- SEED DATA: MOBILE ORDERS (20 rows)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_mobile_orders VALUES
('MOB-001', 'C002', 'P006', 1, 149.99,  0.00,  8.99, '2024-01-08',  'delivered',  'MS-B002-01', 'v3.2.1', '2024-06-01T00:00:00'),
('MOB-002', 'C004', 'P011', 3,  29.99,  0.00,  3.99, '2024-01-15',  'delivered',  'MS-B004-01', 'v3.2.1', '2024-06-01T00:00:00'),
('MOB-003', 'C007', 'P002', 1, 129.99,  0.00,  6.99, '2024-01-22',  'delivered',  'MS-B007-01', 'v3.2.1', '2024-06-01T00:00:00'),
('MOB-004', 'C003', 'P015', 2,  24.99,  0.10,  3.99, '2024-02-05',  'delivered',  'MS-B003-01', 'v3.2.2', '2024-06-01T00:00:00'),
('MOB-005', 'C014', 'P020', 1, 229.99,  0.00, 15.99, '2024-02-12',  'delivered',  'MS-B014-01', 'v3.2.2', '2024-06-01T00:00:00'),
('MOB-006', 'C010', 'P001', 1,  79.99,  0.00,  5.99, '2024-02-20',  'cancelled',  'MS-B010-01', 'v3.2.2', '2024-06-01T00:00:00'),
('MOB-007', 'C006', 'P013', 2,  18.99,  0.00,  3.99, '2024-03-02',  'delivered',  'MS-B006-01', 'v3.3.0', '2024-06-01T00:00:00'),
('MOB-008', 'C001', 'P009', 1,  49.99,  0.00,  4.99, '2024-03-15',  'delivered',  'MS-B001-01', 'v3.3.0', '2024-06-01T00:00:00'),
('MOB-009', 'C009', 'P004', 1,  89.99,  0.05,  5.99, '2024-03-25',  'delivered',  'MS-B009-01', 'v3.3.0', '2024-06-01T00:00:00'),
('MOB-010', 'C015', 'P014', 3,  14.99,  0.00,  2.99, '2024-04-05',  'delivered',  'MS-B015-01', 'v3.3.1', '2024-06-01T00:00:00'),
('MOB-011', 'C012', 'P005', 1, 119.99,  0.10,  7.99, '2024-04-12',  'delivered',  'MS-B012-01', 'v3.3.1', '2024-06-01T00:00:00'),
('MOB-012', 'C008', 'P017', 2,  12.99,  0.00,  4.99, '2024-04-20',  'delivered',  'MS-B008-01', 'v3.3.1', '2024-06-01T00:00:00'),
('MOB-013', 'C004', 'P008', 1,  99.99,  0.00,  7.99, '2024-05-01',  'cancelled',  'MS-B004-02', 'v3.4.0', '2024-06-01T00:00:00'),
('MOB-014', 'C017', 'P006', 1, 149.99,  0.05,  8.99, '2024-05-08',  'delivered',  'MS-B017-01', 'v3.4.0', '2024-06-01T00:00:00'),
('MOB-015', 'C002', 'P012', 1,  69.99,  0.00,  5.99, '2024-05-18',  'delivered',  'MS-B002-02', 'v3.4.0', '2024-06-01T00:00:00'),
('MOB-016', 'C011', 'P013', 4,  18.99,  0.00,  3.99, '2024-05-25',  'delivered',  'MS-B011-01', 'v3.4.0', '2024-06-01T00:00:00'),
('MOB-017', 'C013', 'P016', 2,  19.99,  0.00,  3.99, '2024-06-03',  'delivered',  'MS-B013-01', 'v3.4.1', '2024-06-01T00:00:00'),
('MOB-018', 'C005', 'P003', 3,  39.99,  0.00,  3.99, '2024-06-10',  'delivered',  'MS-B005-01', 'v3.4.1', '2024-06-01T00:00:00'),
('MOB-019', 'C016', 'P018', 1,  34.99,  0.00,  3.99, '2024-06-20',  'delivered',  'MS-B016-01', 'v3.4.1', '2024-06-01T00:00:00'),
('MOB-020', 'C007', 'P010', 1, 149.99,  0.00,  8.99, '2024-06-28',  'delivered',  'MS-B007-02', 'v3.4.1', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_mobile_orders;

-- =============================================================================
-- SEED DATA: POS ORDERS (25 rows) -- store_id + terminal_id
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_pos_orders VALUES
('POS-001', 'C004', 'P013', 5,  18.99,  0.00,  0.00, '2024-01-06',  'delivered',  'STR-NYC-01',  'T01', '2024-06-01T00:00:00'),
('POS-002', 'C002', 'P008', 1,  99.99,  0.00,  0.00, '2024-01-10',  'delivered',  'STR-SF-01',   'T02', '2024-06-01T00:00:00'),
('POS-003', 'C006', 'P015', 3,  24.99,  0.05,  0.00, '2024-01-20',  'delivered',  'STR-ATX-01',  'T01', '2024-06-01T00:00:00'),
('POS-004', 'C010', 'P014', 2,  14.99,  0.00,  0.00, '2024-01-28',  'delivered',  'STR-PHX-01',  'T01', '2024-06-01T00:00:00'),
('POS-005', 'C003', 'P001', 1,  79.99,  0.00,  0.00, '2024-02-03',  'delivered',  'STR-CHI-01',  'T02', '2024-06-01T00:00:00'),
('POS-006', 'C011', 'P009', 2,  49.99,  0.10,  0.00, '2024-02-10',  'cancelled',  'STR-ATL-01',  'T01', '2024-06-01T00:00:00'),
('POS-007', 'C008', 'P016', 1,  19.99,  0.00,  0.00, '2024-02-18',  'delivered',  'STR-POR-01',  'T01', '2024-06-01T00:00:00'),
('POS-008', 'C001', 'P017', 3,  12.99,  0.00,  0.00, '2024-02-25',  'delivered',  'STR-NYC-01',  'T02', '2024-06-01T00:00:00'),
('POS-009', 'C014', 'P004', 1,  89.99,  0.00,  0.00, '2024-03-05',  'delivered',  'STR-NSH-01',  'T01', '2024-06-01T00:00:00'),
('POS-010', 'C005', 'P011', 4,  29.99,  0.00,  0.00, '2024-03-12',  'delivered',  'STR-BOS-01',  'T01', '2024-06-01T00:00:00'),
('POS-011', 'C007', 'P013', 2,  18.99,  0.00,  0.00, '2024-03-20',  'delivered',  'STR-DEN-01',  'T02', '2024-06-01T00:00:00'),
('POS-012', 'C012', 'P002', 1, 129.99,  0.10,  0.00, '2024-03-28',  'cancelled',  'STR-DAL-01',  'T01', '2024-06-01T00:00:00'),
('POS-013', 'C009', 'P018', 1,  34.99,  0.00,  0.00, '2024-04-04',  'delivered',  'STR-MIA-01',  'T02', '2024-06-01T00:00:00'),
('POS-014', 'C016', 'P003', 2,  39.99,  0.00,  0.00, '2024-04-10',  'delivered',  'STR-LV-01',   'T01', '2024-06-01T00:00:00'),
('POS-015', 'C003', 'P014', 1,  14.99,  0.00,  0.00, '2024-04-18',  'delivered',  'STR-CHI-01',  'T01', '2024-06-01T00:00:00'),
('POS-016', 'C018', 'P007', 1, 349.99,  0.05,  0.00, '2024-04-25',  'delivered',  'STR-DET-01',  'T01', '2024-06-01T00:00:00'),
('POS-017', 'C006', 'P001', 1,  79.99,  0.00,  0.00, '2024-05-02',  'delivered',  'STR-ATX-01',  'T02', '2024-06-01T00:00:00'),
('POS-018', 'C015', 'P012', 2,  69.99,  0.00,  0.00, '2024-05-10',  'delivered',  'STR-SD-01',   'T01', '2024-06-01T00:00:00'),
('POS-019', 'C010', 'P005', 1, 119.99,  0.00,  0.00, '2024-05-18',  'delivered',  'STR-PHX-01',  'T02', '2024-06-01T00:00:00'),
('POS-020', 'C017', 'P010', 1, 149.99,  0.10,  0.00, '2024-05-25',  'cancelled',  'STR-CLT-01',  'T01', '2024-06-01T00:00:00'),
('POS-021', 'C004', 'P006', 1, 149.99,  0.00,  0.00, '2024-06-02',  'delivered',  'STR-SEA-01',  'T01', '2024-06-01T00:00:00'),
('POS-022', 'C013', 'P009', 3,  49.99,  0.00,  0.00, '2024-06-08',  'delivered',  'STR-MIN-01',  'T02', '2024-06-01T00:00:00'),
('POS-023', 'C001', 'P012', 1,  69.99,  0.00,  0.00, '2024-06-15',  'delivered',  'STR-NYC-01',  'T01', '2024-06-01T00:00:00'),
('POS-024', 'C008', 'P015', 5,  24.99,  0.00,  0.00, '2024-06-22',  'delivered',  'STR-POR-01',  'T02', '2024-06-01T00:00:00'),
('POS-025', 'C011', 'P020', 1, 229.99,  0.05,  0.00, '2024-06-29',  'delivered',  'STR-ATL-01',  'T01', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 25
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_pos_orders;

-- =============================================================================
-- SEED DATA: BROWSING EVENTS (40 rows for 5 customers) -- funnel stages
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_browsing_events VALUES
-- Customer C001: full funnel browse -> cart -> checkout -> purchase
('EVT-001', 'C001', 'WS-A001-01', 'page_view',      '2024-01-05T09:00:00', '/products',           NULL,   '2024-06-01T00:00:00'),
('EVT-002', 'C001', 'WS-A001-01', 'product_view',    '2024-01-05T09:05:00', '/products/P001',      'P001', '2024-06-01T00:00:00'),
('EVT-003', 'C001', 'WS-A001-01', 'product_view',    '2024-01-05T09:10:00', '/products/P003',      'P003', '2024-06-01T00:00:00'),
('EVT-004', 'C001', 'WS-A001-01', 'add_to_cart',     '2024-01-05T09:12:00', '/cart',               'P001', '2024-06-01T00:00:00'),
('EVT-005', 'C001', 'WS-A001-01', 'add_to_cart',     '2024-01-05T09:13:00', '/cart',               'P003', '2024-06-01T00:00:00'),
('EVT-006', 'C001', 'WS-A001-01', 'checkout_start',  '2024-01-05T09:15:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
('EVT-007', 'C001', 'WS-A001-01', 'purchase',        '2024-01-05T09:18:00', '/checkout/confirm',   NULL,   '2024-06-01T00:00:00'),
-- Customer C002: browse -> cart -> abandon (no checkout)
('EVT-008', 'C002', 'WS-A002-01', 'page_view',      '2024-02-15T14:00:00', '/products',           NULL,   '2024-06-01T00:00:00'),
('EVT-009', 'C002', 'WS-A002-01', 'product_view',    '2024-02-15T14:08:00', '/products/P010',      'P010', '2024-06-01T00:00:00'),
('EVT-010', 'C002', 'WS-A002-01', 'product_view',    '2024-02-15T14:15:00', '/products/P012',      'P012', '2024-06-01T00:00:00'),
('EVT-011', 'C002', 'WS-A002-01', 'add_to_cart',     '2024-02-15T14:20:00', '/cart',               'P010', '2024-06-01T00:00:00'),
('EVT-012', 'C002', 'WS-A002-01', 'page_view',       '2024-02-15T14:25:00', '/products',           NULL,   '2024-06-01T00:00:00'),
-- Customer C003: full funnel with long browsing
('EVT-013', 'C003', 'WS-A003-01', 'page_view',      '2024-01-12T10:00:00', '/home',               NULL,   '2024-06-01T00:00:00'),
('EVT-014', 'C003', 'WS-A003-01', 'product_view',    '2024-01-12T10:05:00', '/products/P019',      'P019', '2024-06-01T00:00:00'),
('EVT-015', 'C003', 'WS-A003-01', 'product_view',    '2024-01-12T10:12:00', '/products/P020',      'P020', '2024-06-01T00:00:00'),
('EVT-016', 'C003', 'WS-A003-01', 'product_view',    '2024-01-12T10:20:00', '/products/P007',      'P007', '2024-06-01T00:00:00'),
('EVT-017', 'C003', 'WS-A003-01', 'add_to_cart',     '2024-01-12T10:22:00', '/cart',               'P019', '2024-06-01T00:00:00'),
('EVT-018', 'C003', 'WS-A003-01', 'checkout_start',  '2024-01-12T10:30:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
('EVT-019', 'C003', 'WS-A003-01', 'purchase',        '2024-01-12T10:35:00', '/checkout/confirm',   NULL,   '2024-06-01T00:00:00'),
-- Customer C005: browse only (no cart)
('EVT-020', 'C005', 'WS-A005-01', 'page_view',      '2024-01-18T16:00:00', '/home',               NULL,   '2024-06-01T00:00:00'),
('EVT-021', 'C005', 'WS-A005-01', 'product_view',    '2024-01-18T16:05:00', '/products/P007',      'P007', '2024-06-01T00:00:00'),
('EVT-022', 'C005', 'WS-A005-01', 'product_view',    '2024-01-18T16:10:00', '/products/P019',      'P019', '2024-06-01T00:00:00'),
('EVT-023', 'C005', 'WS-A005-01', 'add_to_cart',     '2024-01-18T16:15:00', '/cart',               'P007', '2024-06-01T00:00:00'),
('EVT-024', 'C005', 'WS-A005-01', 'checkout_start',  '2024-01-18T16:20:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
('EVT-025', 'C005', 'WS-A005-01', 'purchase',        '2024-01-18T16:25:00', '/checkout/confirm',   NULL,   '2024-06-01T00:00:00'),
-- Customer C009: checkout abandoned
('EVT-026', 'C009', 'WS-A009-01', 'page_view',      '2024-01-25T11:00:00', '/products',           NULL,   '2024-06-01T00:00:00'),
('EVT-027', 'C009', 'WS-A009-01', 'product_view',    '2024-01-25T11:08:00', '/products/P020',      'P020', '2024-06-01T00:00:00'),
('EVT-028', 'C009', 'WS-A009-01', 'add_to_cart',     '2024-01-25T11:12:00', '/cart',               'P020', '2024-06-01T00:00:00'),
('EVT-029', 'C009', 'WS-A009-01', 'checkout_start',  '2024-01-25T11:15:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
-- Customer C007 (2nd session): browse -> purchase fast
('EVT-030', 'C007', 'WS-A007-01', 'page_view',      '2024-02-22T08:00:00', '/products',           NULL,   '2024-06-01T00:00:00'),
('EVT-031', 'C007', 'WS-A007-01', 'product_view',    '2024-02-22T08:03:00', '/products/P006',      'P006', '2024-06-01T00:00:00'),
('EVT-032', 'C007', 'WS-A007-01', 'add_to_cart',     '2024-02-22T08:05:00', '/cart',               'P006', '2024-06-01T00:00:00'),
('EVT-033', 'C007', 'WS-A007-01', 'checkout_start',  '2024-02-22T08:07:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
('EVT-034', 'C007', 'WS-A007-01', 'purchase',        '2024-02-22T08:10:00', '/checkout/confirm',   NULL,   '2024-06-01T00:00:00'),
-- Customer C008: browse only no cart
('EVT-035', 'C008', 'WS-A008-01', 'page_view',      '2024-06-18T19:00:00', '/home',               NULL,   '2024-06-01T00:00:00'),
('EVT-036', 'C008', 'WS-A008-01', 'product_view',    '2024-06-18T19:10:00', '/products/P003',      'P003', '2024-06-01T00:00:00'),
('EVT-037', 'C008', 'WS-A008-01', 'product_view',    '2024-06-18T19:15:00', '/products/P018',      'P018', '2024-06-01T00:00:00'),
('EVT-038', 'C008', 'WS-A008-01', 'add_to_cart',     '2024-06-18T19:20:00', '/cart',               'P003', '2024-06-01T00:00:00'),
('EVT-039', 'C008', 'WS-A008-01', 'checkout_start',  '2024-06-18T19:25:00', '/checkout',           NULL,   '2024-06-01T00:00:00'),
('EVT-040', 'C008', 'WS-A008-01', 'purchase',        '2024-06-18T19:28:00', '/checkout/confirm',   NULL,   '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 40
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_browsing_events;
