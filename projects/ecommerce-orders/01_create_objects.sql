-- =============================================================================
-- E-Commerce Orders Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw order feeds from web, mobile, marketplace';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Merged and deduplicated orders';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for sales analytics';

-- ===================== BRONZE TABLES =====================

-- Raw customers dimension feed
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
    registration_date DATE,
    source_system     STRING,
    ingested_at       TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_customers';

-- Raw products dimension feed
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_products (
    product_id   STRING      NOT NULL,
    sku          STRING      NOT NULL,
    product_name STRING,
    category     STRING,
    subcategory  STRING,
    brand        STRING,
    unit_cost    DECIMAL(10,2),
    list_price   DECIMAL(10,2),
    ingested_at  TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_products';

-- Raw orders from all channels
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_orders (
    order_id        STRING      NOT NULL,
    order_line_id   STRING      NOT NULL,
    customer_id     STRING      NOT NULL,
    product_id      STRING      NOT NULL,
    channel         STRING      NOT NULL,
    platform        STRING,
    region          STRING,
    order_date      DATE,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    shipping_cost    DECIMAL(10,2),
    order_status     STRING,
    is_deleted       BOOLEAN     DEFAULT false,
    source_system    STRING,
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/bronze/raw_orders';

-- ===================== SILVER TABLES =====================

-- Merged orders with soft-delete handling (CDF enabled for downstream sync)
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.orders_merged (
    order_id        STRING      NOT NULL,
    order_line_id   STRING      NOT NULL,
    customer_id     STRING      NOT NULL,
    product_id      STRING      NOT NULL,
    channel         STRING      NOT NULL,
    platform        STRING,
    region          STRING,
    order_date      DATE,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    line_total       DECIMAL(12,2),
    shipping_cost    DECIMAL(10,2),
    order_status     STRING,
    is_deleted       BOOLEAN     DEFAULT false,
    merged_at        TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/orders_merged'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Customer RFM segmentation
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.customer_rfm (
    customer_id      STRING      NOT NULL,
    email            STRING,
    segment          STRING,
    city             STRING,
    state            STRING,
    country          STRING,
    registration_date DATE,
    total_orders     INT,
    total_revenue    DECIMAL(14,2),
    last_order_date  DATE,
    recency_score    INT,
    frequency_score  INT,
    monetary_score   INT,
    rfm_segment      STRING,
    updated_at       TIMESTAMP
) LOCATION '{{data_path}}/ecommerce/silver/customer_rfm';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_customer (
    customer_key      STRING      NOT NULL,
    email             STRING,
    segment           STRING,
    city              STRING,
    state             STRING,
    country           STRING,
    registration_date DATE,
    lifetime_orders   INT,
    rfm_segment       STRING
) LOCATION '{{data_path}}/ecommerce/gold/dim_customer';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_product (
    product_key   STRING      NOT NULL,
    sku           STRING,
    product_name  STRING,
    category      STRING,
    subcategory   STRING,
    brand         STRING,
    unit_cost     DECIMAL(10,2)
) LOCATION '{{data_path}}/ecommerce/gold/dim_product';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_channel (
    channel_key   STRING      NOT NULL,
    channel_name  STRING,
    platform      STRING,
    region        STRING
) LOCATION '{{data_path}}/ecommerce/gold/dim_channel';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_order_lines (
    order_line_key STRING      NOT NULL,
    order_key      STRING      NOT NULL,
    product_key    STRING      NOT NULL,
    customer_key   STRING      NOT NULL,
    channel_key    STRING      NOT NULL,
    order_date     DATE,
    quantity       INT,
    unit_price     DECIMAL(10,2),
    discount_pct   DECIMAL(5,2),
    line_total     DECIMAL(12,2),
    shipping_cost  DECIMAL(10,2)
) LOCATION '{{data_path}}/ecommerce/gold/fact_order_lines';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_sales_dashboard (
    order_date          DATE,
    channel             STRING,
    total_orders        INT,
    total_revenue       DECIMAL(14,2),
    avg_order_value     DECIMAL(10,2),
    cancellation_rate   DECIMAL(5,4),
    unique_customers    INT,
    repeat_customer_pct DECIMAL(5,2)
) LOCATION '{{data_path}}/ecommerce/gold/kpi_sales_dashboard';

-- ===================== PSEUDONYMISATION =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.customer_rfm (email) TRANSFORM redact PARAMS ('replacement', '***@***.***');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_customers (email) TRANSFORM redact PARAMS ('replacement', '***@***.***');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_customers (address) TRANSFORM redact PARAMS ('replacement', '[REDACTED]');

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_customers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_products TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.orders_merged TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.customer_rfm TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_customer TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_product TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_channel TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_order_lines TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_sales_dashboard TO USER {{current_user}};

-- ===================== SEED DATA: CUSTOMERS (15 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_customers VALUES
('C001', 'alice.morgan@example.com',   'Alice',   'Morgan',   'Premium',    'New York',      'NY', 'US', '123 5th Ave, New York, NY 10001',       '2024-01-15', 'CRM', '2024-06-01T00:00:00'),
('C002', 'bob.chen@example.com',       'Bob',     'Chen',     'Standard',   'San Francisco', 'CA', 'US', '456 Market St, San Francisco, CA 94105', '2024-02-20', 'CRM', '2024-06-01T00:00:00'),
('C003', 'carol.davis@example.com',    'Carol',   'Davis',    'Premium',    'Chicago',       'IL', 'US', '789 Michigan Ave, Chicago, IL 60601',   '2024-03-10', 'CRM', '2024-06-01T00:00:00'),
('C004', 'david.kim@example.com',      'David',   'Kim',      'Standard',   'Seattle',       'WA', 'US', '321 Pike St, Seattle, WA 98101',        '2024-01-05', 'CRM', '2024-06-01T00:00:00'),
('C005', 'elena.russo@example.com',    'Elena',   'Russo',    'Enterprise', 'Boston',        'MA', 'US', '654 Beacon St, Boston, MA 02108',       '2023-11-20', 'CRM', '2024-06-01T00:00:00'),
('C006', 'frank.oconnor@example.com',  'Frank',   'OConnor',  'Standard',   'Austin',        'TX', 'US', '987 Congress Ave, Austin, TX 78701',    '2024-04-01', 'CRM', '2024-06-01T00:00:00'),
('C007', 'grace.patel@example.com',    'Grace',   'Patel',    'Premium',    'Denver',        'CO', 'US', '147 Blake St, Denver, CO 80202',        '2024-02-14', 'CRM', '2024-06-01T00:00:00'),
('C008', 'henry.watson@example.com',   'Henry',   'Watson',   'Standard',   'Portland',      'OR', 'US', '258 Burnside St, Portland, OR 97204',   '2024-05-10', 'CRM', '2024-06-01T00:00:00'),
('C009', 'irene.lopez@example.com',    'Irene',   'Lopez',    'Enterprise', 'Miami',         'FL', 'US', '369 Ocean Dr, Miami, FL 33139',         '2023-12-01', 'CRM', '2024-06-01T00:00:00'),
('C010', 'james.nguyen@example.com',   'James',   'Nguyen',   'Standard',   'Phoenix',       'AZ', 'US', '741 Camelback Rd, Phoenix, AZ 85014',   '2024-03-25', 'CRM', '2024-06-01T00:00:00'),
('C011', 'karen.smith@example.com',    'Karen',   'Smith',    'Premium',    'Atlanta',       'GA', 'US', '852 Peachtree St, Atlanta, GA 30308',   '2024-01-30', 'CRM', '2024-06-01T00:00:00'),
('C012', 'leo.martinez@example.com',   'Leo',     'Martinez', 'Standard',   'Dallas',        'TX', 'US', '963 Elm St, Dallas, TX 75201',          '2024-04-15', 'CRM', '2024-06-01T00:00:00'),
('C013', 'maya.johnson@example.com',   'Maya',    'Johnson',  'Enterprise', 'Minneapolis',   'MN', 'US', '159 Hennepin Ave, Minneapolis, MN 55401','2023-10-10', 'CRM', '2024-06-01T00:00:00'),
('C014', 'nathan.brown@example.com',   'Nathan',  'Brown',    'Standard',   'Nashville',     'TN', 'US', '357 Broadway, Nashville, TN 37201',     '2024-05-20', 'CRM', '2024-06-01T00:00:00'),
('C015', 'olivia.taylor@example.com',  'Olivia',  'Taylor',   'Premium',    'San Diego',     'CA', 'US', '468 Harbor Dr, San Diego, CA 92101',    '2024-02-28', 'CRM', '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_customers;


-- ===================== SEED DATA: PRODUCTS (20 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_products VALUES
('P001', 'SKU-ELEC-001', 'Wireless Headphones',       'Electronics',  'Audio',       'SoundMax',   29.99,  79.99,  '2024-06-01T00:00:00'),
('P002', 'SKU-ELEC-002', 'Bluetooth Speaker',          'Electronics',  'Audio',       'SoundMax',   45.00, 129.99,  '2024-06-01T00:00:00'),
('P003', 'SKU-ELEC-003', 'USB-C Hub 7-Port',           'Electronics',  'Accessories', 'TechLink',   12.50,  39.99,  '2024-06-01T00:00:00'),
('P004', 'SKU-ELEC-004', 'Mechanical Keyboard',        'Electronics',  'Peripherals', 'KeyCraft',   35.00,  89.99,  '2024-06-01T00:00:00'),
('P005', 'SKU-ELEC-005', '4K Webcam',                  'Electronics',  'Peripherals', 'VisionPro',  40.00, 119.99,  '2024-06-01T00:00:00'),
('P006', 'SKU-HOME-001', 'Smart Thermostat',           'Home',         'Smart Home',  'EcoSmart',   55.00, 149.99,  '2024-06-01T00:00:00'),
('P007', 'SKU-HOME-002', 'Robot Vacuum',               'Home',         'Cleaning',    'CleanBot',   120.00, 349.99, '2024-06-01T00:00:00'),
('P008', 'SKU-HOME-003', 'Air Purifier',               'Home',         'Air Quality', 'PureAir',    65.00, 199.99,  '2024-06-01T00:00:00'),
('P009', 'SKU-HOME-004', 'LED Desk Lamp',              'Home',         'Lighting',    'LumiTech',   8.00,   34.99,  '2024-06-01T00:00:00'),
('P010', 'SKU-HOME-005', 'Smart Plug 4-Pack',          'Home',         'Smart Home',  'EcoSmart',   10.00,  29.99,  '2024-06-01T00:00:00'),
('P011', 'SKU-SPRT-001', 'Yoga Mat Premium',           'Sports',       'Fitness',     'FlexFit',    7.00,   29.99,  '2024-06-01T00:00:00'),
('P012', 'SKU-SPRT-002', 'Resistance Bands Set',       'Sports',       'Fitness',     'FlexFit',    5.00,   19.99,  '2024-06-01T00:00:00'),
('P013', 'SKU-SPRT-003', 'Insulated Water Bottle',     'Sports',       'Hydration',   'HydroMax',   6.00,   24.99,  '2024-06-01T00:00:00'),
('P014', 'SKU-SPRT-004', 'Running Shoes Mens',         'Sports',       'Footwear',    'StridePro',  38.00,  99.99,  '2024-06-01T00:00:00'),
('P015', 'SKU-SPRT-005', 'Fitness Tracker Watch',      'Sports',       'Wearables',   'PulseTech',  50.00, 149.99,  '2024-06-01T00:00:00'),
('P016', 'SKU-OFFC-001', 'Ergonomic Office Chair',     'Office',       'Furniture',   'ComfortPro', 150.00, 399.99, '2024-06-01T00:00:00'),
('P017', 'SKU-OFFC-002', 'Standing Desk Converter',    'Office',       'Furniture',   'ComfortPro', 85.00, 229.99,  '2024-06-01T00:00:00'),
('P018', 'SKU-OFFC-003', 'Monitor Arm Dual',           'Office',       'Accessories', 'MountIt',    22.00,  64.99,  '2024-06-01T00:00:00'),
('P019', 'SKU-OFFC-004', 'Noise Cancelling Earbuds',   'Office',       'Audio',       'SoundMax',   32.00,  89.99,  '2024-06-01T00:00:00'),
('P020', 'SKU-OFFC-005', 'Laptop Stand Aluminum',      'Office',       'Accessories', 'TechLink',   15.00,  49.99,  '2024-06-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_products;


-- ===================== SEED DATA: ORDERS (72 rows) =====================
-- Spans 2024-01 through 2024-06, 3 channels, includes cancellations, partial shipments, returns

INSERT INTO {{zone_prefix}}.bronze.raw_orders VALUES
-- January 2024 orders
('ORD-0001', 'OL-0001', 'C001', 'P001', 'web',         'Desktop',  'US-East',  '2024-01-10', 2,  79.99,  0.00,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0001', 'OL-0002', 'C001', 'P003', 'web',         'Desktop',  'US-East',  '2024-01-10', 1,  39.99,  0.10,  0.00, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0002', 'OL-0003', 'C002', 'P006', 'mobile',      'iOS',      'US-West',  '2024-01-12', 1, 149.99,  0.00,  8.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0003', 'OL-0004', 'C005', 'P016', 'web',         'Desktop',  'US-East',  '2024-01-15', 1, 399.99,  0.05, 29.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0004', 'OL-0005', 'C003', 'P007', 'marketplace', 'Amazon',   'US-Central','2024-01-18', 1, 349.99,  0.00, 12.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0005', 'OL-0006', 'C004', 'P012', 'mobile',      'Android',  'US-West',  '2024-01-20', 3,  19.99,  0.00,  3.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0006', 'OL-0007', 'C009', 'P017', 'web',         'Desktop',  'US-East',  '2024-01-22', 1, 229.99,  0.10, 15.99, 'cancelled',  true,  'web-feed',   '2024-06-01T00:00:00'),
('ORD-0007', 'OL-0008', 'C007', 'P002', 'mobile',      'iOS',      'US-West',  '2024-01-25', 1, 129.99,  0.00,  6.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0008', 'OL-0009', 'C010', 'P011', 'marketplace', 'eBay',     'US-West',  '2024-01-28', 2,  29.99,  0.05,  4.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0009', 'OL-0010', 'C013', 'P004', 'web',         'Desktop',  'US-Central','2024-01-30', 1,  89.99,  0.00,  5.99, 'returned',   false, 'web-feed',   '2024-06-01T00:00:00'),

-- February 2024 orders
('ORD-0010', 'OL-0011', 'C001', 'P005', 'web',         'Desktop',  'US-East',  '2024-02-02', 1, 119.99,  0.00,  7.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0011', 'OL-0012', 'C006', 'P009', 'mobile',      'Android',  'US-Central','2024-02-05', 2,  34.99,  0.00,  3.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0012', 'OL-0013', 'C008', 'P013', 'marketplace', 'Amazon',   'US-West',  '2024-02-08', 4,  24.99,  0.15,  5.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0013', 'OL-0014', 'C011', 'P001', 'web',         'Desktop',  'US-East',  '2024-02-10', 1,  79.99,  0.00,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0014', 'OL-0015', 'C003', 'P015', 'mobile',      'iOS',      'US-Central','2024-02-12', 1, 149.99,  0.10,  6.99, 'shipped',    false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0015', 'OL-0016', 'C012', 'P008', 'web',         'Desktop',  'US-Central','2024-02-14', 1, 199.99,  0.00,  9.99, 'cancelled',  true,  'web-feed',   '2024-06-01T00:00:00'),
('ORD-0016', 'OL-0017', 'C002', 'P010', 'marketplace', 'Amazon',   'US-West',  '2024-02-16', 2,  29.99,  0.00,  4.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0017', 'OL-0018', 'C014', 'P020', 'mobile',      'Android',  'US-East',  '2024-02-18', 1,  49.99,  0.00,  5.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0018', 'OL-0019', 'C007', 'P004', 'web',         'Desktop',  'US-West',  '2024-02-20', 1,  89.99,  0.05,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0019', 'OL-0020', 'C005', 'P018', 'marketplace', 'eBay',     'US-East',  '2024-02-22', 2,  64.99,  0.00,  7.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),

-- March 2024 orders
('ORD-0020', 'OL-0021', 'C009', 'P002', 'web',         'Desktop',  'US-East',  '2024-03-01', 1, 129.99,  0.00,  6.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0021', 'OL-0022', 'C004', 'P014', 'mobile',      'iOS',      'US-West',  '2024-03-03', 1,  99.99,  0.00,  7.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0022', 'OL-0023', 'C015', 'P006', 'web',         'Desktop',  'US-West',  '2024-03-05', 1, 149.99,  0.10,  8.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0023', 'OL-0024', 'C001', 'P019', 'marketplace', 'Amazon',   'US-East',  '2024-03-07', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0024', 'OL-0025', 'C006', 'P003', 'mobile',      'Android',  'US-Central','2024-03-09', 2,  39.99,  0.00,  3.99, 'cancelled',  true,  'mobile-feed','2024-06-01T00:00:00'),
('ORD-0025', 'OL-0026', 'C011', 'P007', 'web',         'Desktop',  'US-East',  '2024-03-11', 1, 349.99,  0.05, 12.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0026', 'OL-0027', 'C002', 'P015', 'mobile',      'iOS',      'US-West',  '2024-03-13', 1, 149.99,  0.00,  6.99, 'shipped',    false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0027', 'OL-0028', 'C013', 'P009', 'marketplace', 'Amazon',   'US-Central','2024-03-15', 3,  34.99,  0.10,  4.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0028', 'OL-0029', 'C008', 'P016', 'web',         'Desktop',  'US-West',  '2024-03-17', 1, 399.99,  0.00, 29.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0029', 'OL-0030', 'C010', 'P001', 'mobile',      'Android',  'US-West',  '2024-03-19', 1,  79.99,  0.00,  5.99, 'returned',   false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0030', 'OL-0031', 'C003', 'P020', 'web',         'Desktop',  'US-Central','2024-03-21', 2,  49.99,  0.00,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0031', 'OL-0032', 'C014', 'P005', 'marketplace', 'eBay',     'US-East',  '2024-03-23', 1, 119.99,  0.15,  7.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),

-- April 2024 orders
('ORD-0032', 'OL-0033', 'C005', 'P002', 'web',         'Desktop',  'US-East',  '2024-04-01', 2, 129.99,  0.00,  6.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0033', 'OL-0034', 'C007', 'P011', 'mobile',      'iOS',      'US-West',  '2024-04-03', 1,  29.99,  0.00,  3.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0034', 'OL-0035', 'C012', 'P006', 'marketplace', 'Amazon',   'US-Central','2024-04-05', 1, 149.99,  0.10,  8.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0035', 'OL-0036', 'C001', 'P004', 'web',         'Desktop',  'US-East',  '2024-04-07', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0036', 'OL-0037', 'C009', 'P013', 'mobile',      'Android',  'US-East',  '2024-04-09', 2,  24.99,  0.00,  3.99, 'cancelled',  true,  'mobile-feed','2024-06-01T00:00:00'),
('ORD-0037', 'OL-0038', 'C004', 'P017', 'web',         'Desktop',  'US-West',  '2024-04-11', 1, 229.99,  0.05, 15.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0038', 'OL-0039', 'C015', 'P008', 'marketplace', 'Amazon',   'US-West',  '2024-04-13', 1, 199.99,  0.00,  9.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0039', 'OL-0040', 'C006', 'P001', 'mobile',      'iOS',      'US-Central','2024-04-15', 3,  79.99,  0.10,  5.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0040', 'OL-0041', 'C011', 'P019', 'web',         'Desktop',  'US-East',  '2024-04-17', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0041', 'OL-0042', 'C002', 'P014', 'marketplace', 'eBay',     'US-West',  '2024-04-19', 1,  99.99,  0.00,  7.99, 'returned',   false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0042', 'OL-0043', 'C013', 'P010', 'mobile',      'Android',  'US-Central','2024-04-21', 1,  29.99,  0.00,  4.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0043', 'OL-0044', 'C008', 'P003', 'web',         'Desktop',  'US-West',  '2024-04-23', 1,  39.99,  0.00,  3.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),

-- May 2024 orders
('ORD-0044', 'OL-0045', 'C003', 'P002', 'mobile',      'iOS',      'US-Central','2024-05-01', 1, 129.99,  0.00,  6.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0045', 'OL-0046', 'C010', 'P016', 'web',         'Desktop',  'US-West',  '2024-05-03', 1, 399.99,  0.05, 29.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0046', 'OL-0047', 'C005', 'P012', 'marketplace', 'Amazon',   'US-East',  '2024-05-05', 5,  19.99,  0.00,  3.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0047', 'OL-0048', 'C014', 'P006', 'mobile',      'Android',  'US-East',  '2024-05-07', 1, 149.99,  0.10,  8.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0048', 'OL-0049', 'C007', 'P018', 'web',         'Desktop',  'US-West',  '2024-05-09', 1,  64.99,  0.00,  5.99, 'cancelled',  true,  'web-feed',   '2024-06-01T00:00:00'),
('ORD-0049', 'OL-0050', 'C001', 'P015', 'marketplace', 'Amazon',   'US-East',  '2024-05-11', 1, 149.99,  0.00,  6.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0050', 'OL-0051', 'C012', 'P004', 'mobile',      'iOS',      'US-Central','2024-05-13', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0051', 'OL-0052', 'C009', 'P020', 'web',         'Desktop',  'US-East',  '2024-05-15', 2,  49.99,  0.05,  5.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0052', 'OL-0053', 'C004', 'P008', 'marketplace', 'eBay',     'US-West',  '2024-05-17', 1, 199.99,  0.00,  9.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0053', 'OL-0054', 'C015', 'P001', 'mobile',      'Android',  'US-West',  '2024-05-19', 2,  79.99,  0.00,  5.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0054', 'OL-0055', 'C006', 'P007', 'web',         'Desktop',  'US-Central','2024-05-21', 1, 349.99,  0.10, 12.99, 'shipped',    false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0055', 'OL-0056', 'C011', 'P013', 'marketplace', 'Amazon',   'US-East',  '2024-05-23', 3,  24.99,  0.00,  4.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),

-- June 2024 orders
('ORD-0056', 'OL-0057', 'C002', 'P005', 'web',         'Desktop',  'US-West',  '2024-06-01', 1, 119.99,  0.00,  7.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0057', 'OL-0058', 'C013', 'P017', 'mobile',      'iOS',      'US-Central','2024-06-03', 1, 229.99,  0.05, 15.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0058', 'OL-0059', 'C008', 'P011', 'marketplace', 'Amazon',   'US-West',  '2024-06-05', 2,  29.99,  0.00,  3.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0059', 'OL-0060', 'C001', 'P009', 'web',         'Desktop',  'US-East',  '2024-06-07', 1,  34.99,  0.00,  3.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0060', 'OL-0061', 'C010', 'P002', 'mobile',      'Android',  'US-West',  '2024-06-09', 1, 129.99,  0.15,  6.99, 'cancelled',  true,  'mobile-feed','2024-06-01T00:00:00'),
('ORD-0061', 'OL-0062', 'C003', 'P016', 'web',         'Desktop',  'US-Central','2024-06-11', 1, 399.99,  0.00, 29.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0062', 'OL-0063', 'C005', 'P010', 'marketplace', 'Amazon',   'US-East',  '2024-06-13', 3,  29.99,  0.00,  4.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0063', 'OL-0064', 'C014', 'P004', 'mobile',      'iOS',      'US-East',  '2024-06-15', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0064', 'OL-0065', 'C007', 'P006', 'web',         'Desktop',  'US-West',  '2024-06-17', 1, 149.99,  0.10,  8.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0065', 'OL-0066', 'C012', 'P019', 'marketplace', 'eBay',     'US-Central','2024-06-19', 1,  89.99,  0.00,  5.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0066', 'OL-0067', 'C009', 'P014', 'mobile',      'Android',  'US-East',  '2024-06-21', 1,  99.99,  0.00,  7.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0067', 'OL-0068', 'C004', 'P003', 'web',         'Desktop',  'US-West',  '2024-06-23', 3,  39.99,  0.00,  3.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0068', 'OL-0069', 'C015', 'P012', 'mobile',      'iOS',      'US-West',  '2024-06-25', 2,  19.99,  0.00,  3.99, 'delivered',  false, 'mobile-feed','2024-06-01T00:00:00'),
('ORD-0069', 'OL-0070', 'C011', 'P008', 'web',         'Desktop',  'US-East',  '2024-06-27', 1, 199.99,  0.05,  9.99, 'delivered',  false, 'web-feed',   '2024-06-01T00:00:00'),
('ORD-0070', 'OL-0071', 'C006', 'P005', 'marketplace', 'Amazon',   'US-Central','2024-06-28', 1, 119.99,  0.00,  7.99, 'delivered',  false, 'mkt-feed',  '2024-06-01T00:00:00'),
('ORD-0071', 'OL-0072', 'C013', 'P007', 'mobile',      'Android',  'US-Central','2024-06-29', 1, 349.99,  0.10, 12.99, 'returned',   false, 'mobile-feed','2024-06-01T00:00:00');

ASSERT ROW_COUNT = 72
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_orders;

