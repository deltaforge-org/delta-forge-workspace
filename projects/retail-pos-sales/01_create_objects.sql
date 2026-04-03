-- =============================================================================
-- Retail POS Sales Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw POS transaction feeds from 6 stores';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched transactions with basket metrics';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for store performance analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_stores (
    store_id        STRING      NOT NULL,
    store_name      STRING      NOT NULL,
    format          STRING,
    city            STRING,
    state           STRING,
    region          STRING,
    sqft            INT,
    open_date       DATE,
    manager         STRING,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/retail/bronze/raw_stores';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_products (
    sku             STRING      NOT NULL,
    product_name    STRING      NOT NULL,
    category        STRING,
    subcategory     STRING,
    brand           STRING,
    unit_cost       DECIMAL(10,2),
    supplier        STRING,
    shelf_life_days INT,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/retail/bronze/raw_products';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_cashiers (
    employee_id     STRING      NOT NULL,
    name            STRING      NOT NULL,
    hire_date       DATE,
    shift_preference STRING,
    certification_level STRING,
    store_id        STRING,
    ingested_at     TIMESTAMP
) LOCATION '{{data_path}}/retail/bronze/raw_cashiers';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_transactions (
    txn_id          BIGINT      NOT NULL,
    receipt_id      STRING      NOT NULL,
    store_id        STRING      NOT NULL,
    sku             STRING      NOT NULL,
    employee_id     STRING      NOT NULL,
    txn_date        DATE        NOT NULL,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    payment_method   STRING,
    basket_id        STRING,
    region           STRING,
    ingested_at      TIMESTAMP
) LOCATION '{{data_path}}/retail/bronze/raw_transactions';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.transactions_enriched (
    txn_id          BIGINT      NOT NULL,
    receipt_id      STRING      NOT NULL,
    store_id        STRING      NOT NULL,
    sku             STRING      NOT NULL,
    employee_id     STRING      NOT NULL,
    txn_date        DATE        NOT NULL,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    line_total       DECIMAL(12,2),
    payment_method   STRING,
    basket_id        STRING,
    region           STRING,
    product_name     STRING,
    category         STRING,
    unit_cost        DECIMAL(10,2),
    is_return        BOOLEAN,
    anomaly_flag     BOOLEAN,
    enriched_at      TIMESTAMP
) LOCATION '{{data_path}}/retail/silver/transactions_enriched';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.basket_metrics (
    basket_id       STRING      NOT NULL,
    store_id        STRING      NOT NULL,
    txn_date        DATE,
    items_in_basket  INT,
    basket_value     DECIMAL(12,2),
    has_return       BOOLEAN,
    enriched_at      TIMESTAMP
) LOCATION '{{data_path}}/retail/silver/basket_metrics';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_store (
    store_key       STRING      NOT NULL,
    store_id        STRING      NOT NULL,
    store_name      STRING,
    format          STRING,
    city            STRING,
    state           STRING,
    region          STRING,
    sqft            INT,
    open_date       DATE,
    manager         STRING
) LOCATION '{{data_path}}/retail/gold/dim_store';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_product (
    product_key     STRING      NOT NULL,
    sku             STRING      NOT NULL,
    product_name    STRING,
    category        STRING,
    subcategory     STRING,
    brand           STRING,
    unit_cost       DECIMAL(10,2),
    supplier        STRING,
    shelf_life_days INT
) LOCATION '{{data_path}}/retail/gold/dim_product';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_date (
    date_key        STRING      NOT NULL,
    full_date       DATE        NOT NULL,
    day_of_week     STRING,
    week_number     INT,
    month           INT,
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
) LOCATION '{{data_path}}/retail/gold/dim_date';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_cashier (
    cashier_key     STRING      NOT NULL,
    employee_id     STRING      NOT NULL,
    name            STRING,
    hire_date       DATE,
    shift_preference STRING,
    certification_level STRING
) LOCATION '{{data_path}}/retail/gold/dim_cashier';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_sales (
    sale_key        BIGINT      NOT NULL,
    store_key       STRING      NOT NULL,
    product_key     STRING      NOT NULL,
    cashier_key     STRING      NOT NULL,
    date_key        STRING      NOT NULL,
    receipt_id      STRING,
    quantity         INT,
    unit_price       DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    line_total       DECIMAL(12,2),
    payment_method   STRING,
    basket_id        STRING
) LOCATION '{{data_path}}/retail/gold/fact_sales';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_store_performance (
    store_id        STRING      NOT NULL,
    region          STRING,
    month           STRING,
    total_revenue   DECIMAL(14,2),
    total_transactions INT,
    avg_basket_value DECIMAL(10,2),
    items_per_transaction DECIMAL(5,2),
    gross_margin_pct DECIMAL(5,2),
    yoy_growth_pct   DECIMAL(7,2),
    rank_in_region   INT
) LOCATION '{{data_path}}/retail/gold/kpi_store_performance';

-- ===================== GRANTS =====================
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_transactions TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_stores TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_products TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_cashiers TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.transactions_enriched TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.basket_metrics TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_sales TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_store TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_product TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_date TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_cashier TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_store_performance TO USER {{current_user}};

-- ===================== SEED DATA: STORES (6 stores, 3 regions) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_stores VALUES
('STR-001', 'Downtown Flagship',   'flagship',    'Chicago',       'IL', 'Midwest',   45000, '2018-03-15', 'Maria Garcia',    '2024-01-01T00:00:00'),
('STR-002', 'Suburban Plaza',      'standard',    'Naperville',    'IL', 'Midwest',   28000, '2019-07-22', 'James Wilson',    '2024-01-01T00:00:00'),
('STR-003', 'Eastside Market',     'express',     'New York',      'NY', 'Northeast', 15000, '2020-01-10', 'Sarah Chen',      '2024-01-01T00:00:00'),
('STR-004', 'Harbor Square',       'standard',    'Boston',        'MA', 'Northeast', 32000, '2017-11-05', 'David Park',      '2024-01-01T00:00:00'),
('STR-005', 'Sunset Boulevard',    'flagship',    'Los Angeles',   'CA', 'West',      50000, '2016-05-20', 'Emily Rodriguez', '2024-01-01T00:00:00'),
('STR-006', 'Bay Area Express',    'express',     'San Francisco', 'CA', 'West',      18000, '2021-09-01', 'Michael Chang',   '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_stores;


-- ===================== SEED DATA: PRODUCTS (20 products) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_products VALUES
('SKU-1001', 'Organic Whole Milk 1gal',     'Groceries',    'Dairy',         'FreshFarms',    2.89, 'Midwest Dairy Co',    14, '2024-01-01T00:00:00'),
('SKU-1002', 'Sourdough Bread Loaf',        'Groceries',    'Bakery',        'ArtisanBake',   1.45, 'Local Bakery Inc',     5, '2024-01-01T00:00:00'),
('SKU-1003', 'Free Range Eggs 12pk',        'Groceries',    'Dairy',         'FreshFarms',    2.10, 'Valley Poultry',      21, '2024-01-01T00:00:00'),
('SKU-1004', 'Atlantic Salmon Fillet 1lb',  'Groceries',    'Seafood',       'OceanCatch',    6.50, 'Pacific Seafood',      3, '2024-01-01T00:00:00'),
('SKU-1005', 'Avocado Hass 4pk',            'Groceries',    'Produce',       'FreshFarms',    1.80, 'CalGrow Farms',        7, '2024-01-01T00:00:00'),
('SKU-2001', 'Wireless Bluetooth Earbuds',  'Electronics',  'Audio',         'TechPulse',    18.50, 'Shenzhen Audio Ltd',  NULL, '2024-01-01T00:00:00'),
('SKU-2002', 'USB-C Fast Charger 65W',      'Electronics',  'Accessories',   'TechPulse',     8.75, 'PowerTech Corp',     NULL, '2024-01-01T00:00:00'),
('SKU-2003', 'Smart LED Bulb 4pk',          'Electronics',  'Smart Home',    'BrightLife',   12.00, 'LumiTech Inc',       NULL, '2024-01-01T00:00:00'),
('SKU-2004', 'Portable Bluetooth Speaker',  'Electronics',  'Audio',         'TechPulse',    22.00, 'Shenzhen Audio Ltd', NULL, '2024-01-01T00:00:00'),
('SKU-2005', '10000mAh Power Bank',         'Electronics',  'Accessories',   'TechPulse',    11.50, 'PowerTech Corp',     NULL, '2024-01-01T00:00:00'),
('SKU-3001', 'Mens Cotton Crew T-Shirt',    'Apparel',      'Mens Tops',     'UrbanThread',   4.50, 'TextileCo Vietnam',  NULL, '2024-01-01T00:00:00'),
('SKU-3002', 'Womens Running Shorts',       'Apparel',      'Womens Active', 'FlexFit',       6.25, 'ActiveWear Ltd',     NULL, '2024-01-01T00:00:00'),
('SKU-3003', 'Kids Winter Jacket',          'Apparel',      'Kids Outerwear','UrbanThread',  15.00, 'TextileCo Vietnam',  NULL, '2024-01-01T00:00:00'),
('SKU-3004', 'Wool Blend Scarf',            'Apparel',      'Accessories',   'UrbanThread',   5.50, 'Highland Wool Co',   NULL, '2024-01-01T00:00:00'),
('SKU-3005', 'Athletic Crew Socks 6pk',     'Apparel',      'Mens Active',   'FlexFit',       3.00, 'ActiveWear Ltd',     NULL, '2024-01-01T00:00:00'),
('SKU-4001', 'Stainless Steel Water Bottle','Groceries',    'Beverages',     'EcoLife',       4.25, 'MetalWorks Inc',     NULL, '2024-01-01T00:00:00'),
('SKU-4002', 'Organic Ground Coffee 12oz',  'Groceries',    'Beverages',     'BeanOrigin',    5.50, 'Colombia Direct',    180, '2024-01-01T00:00:00'),
('SKU-4003', 'Greek Yogurt Variety 8pk',    'Groceries',    'Dairy',         'FreshFarms',    3.20, 'Midwest Dairy Co',    10, '2024-01-01T00:00:00'),
('SKU-4004', 'Protein Bar Box 12ct',        'Groceries',    'Snacks',        'FuelUp',        8.00, 'NutriFoods Corp',    365, '2024-01-01T00:00:00'),
('SKU-4005', 'Sparkling Water 12pk',        'Groceries',    'Beverages',     'EcoLife',       2.50, 'SpringWater Inc',    365, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_products;


-- ===================== SEED DATA: CASHIERS (8 cashiers) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_cashiers VALUES
('EMP-101', 'Anna Thompson',   '2020-03-15', 'morning',   'senior',       'STR-001', '2024-01-01T00:00:00'),
('EMP-102', 'Brian Martinez',  '2021-06-01', 'afternoon', 'standard',     'STR-001', '2024-01-01T00:00:00'),
('EMP-103', 'Claire Dubois',   '2019-11-20', 'morning',   'senior',       'STR-002', '2024-01-01T00:00:00'),
('EMP-104', 'Derek Johnson',   '2022-01-10', 'evening',   'junior',       'STR-003', '2024-01-01T00:00:00'),
('EMP-105', 'Elena Kowalski',  '2018-08-05', 'morning',   'lead',         'STR-004', '2024-01-01T00:00:00'),
('EMP-106', 'Frank Nguyen',    '2023-02-14', 'afternoon', 'junior',       'STR-004', '2024-01-01T00:00:00'),
('EMP-107', 'Grace Kim',       '2020-10-30', 'morning',   'senior',       'STR-005', '2024-01-01T00:00:00'),
('EMP-108', 'Henry Patel',     '2021-04-18', 'evening',   'standard',     'STR-006', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_cashiers;


-- ===================== SEED DATA: TRANSACTIONS (75 rows, 3 months) =====================
-- Includes returns (negative qty), discounts, multiple payment methods, 6 stores, 3 regions
INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
-- January 2024 - Midwest
(1001, 'RCP-20240105-001', 'STR-001', 'SKU-1001', 'EMP-101', '2024-01-05',  2,   4.99, 0.00, 'credit_card', 'BSK-001', 'Midwest', '2024-01-05T08:15:00'),
(1002, 'RCP-20240105-001', 'STR-001', 'SKU-1002', 'EMP-101', '2024-01-05',  1,   5.49, 0.00, 'credit_card', 'BSK-001', 'Midwest', '2024-01-05T08:15:00'),
(1003, 'RCP-20240105-001', 'STR-001', 'SKU-1003', 'EMP-101', '2024-01-05',  1,   5.99, 0.00, 'credit_card', 'BSK-001', 'Midwest', '2024-01-05T08:15:00'),
(1004, 'RCP-20240108-002', 'STR-001', 'SKU-2001', 'EMP-102', '2024-01-08',  1,  49.99, 0.10, 'debit_card',  'BSK-002', 'Midwest', '2024-01-08T14:30:00'),
(1005, 'RCP-20240108-002', 'STR-001', 'SKU-2002', 'EMP-102', '2024-01-08',  2,  24.99, 0.10, 'debit_card',  'BSK-002', 'Midwest', '2024-01-08T14:30:00'),
(1006, 'RCP-20240110-003', 'STR-002', 'SKU-3001', 'EMP-103', '2024-01-10',  3,  14.99, 0.00, 'cash',        'BSK-003', 'Midwest', '2024-01-10T09:45:00'),
(1007, 'RCP-20240110-003', 'STR-002', 'SKU-3005', 'EMP-103', '2024-01-10',  2,   9.99, 0.00, 'cash',        'BSK-003', 'Midwest', '2024-01-10T09:45:00'),
(1008, 'RCP-20240112-004', 'STR-002', 'SKU-4002', 'EMP-103', '2024-01-12',  1,  14.99, 0.05, 'credit_card', 'BSK-004', 'Midwest', '2024-01-12T10:00:00'),
(1009, 'RCP-20240115-005', 'STR-001', 'SKU-1004', 'EMP-101', '2024-01-15',  2,  16.99, 0.00, 'credit_card', 'BSK-005', 'Midwest', '2024-01-15T08:20:00'),
(1010, 'RCP-20240115-005', 'STR-001', 'SKU-1005', 'EMP-101', '2024-01-15',  4,   3.99, 0.00, 'credit_card', 'BSK-005', 'Midwest', '2024-01-15T08:20:00'),
(1011, 'RCP-20240118-006', 'STR-001', 'SKU-2001', 'EMP-102', '2024-01-18', -1,  49.99, 0.10, 'credit_card', 'BSK-006', 'Midwest', '2024-01-18T15:00:00'),
-- January 2024 - Northeast
(1012, 'RCP-20240106-007', 'STR-003', 'SKU-2003', 'EMP-104', '2024-01-06',  2,  29.99, 0.00, 'credit_card', 'BSK-007', 'Northeast', '2024-01-06T11:00:00'),
(1013, 'RCP-20240106-007', 'STR-003', 'SKU-2005', 'EMP-104', '2024-01-06',  1,  29.99, 0.00, 'credit_card', 'BSK-007', 'Northeast', '2024-01-06T11:00:00'),
(1014, 'RCP-20240109-008', 'STR-004', 'SKU-3002', 'EMP-105', '2024-01-09',  2,  24.99, 0.15, 'debit_card',  'BSK-008', 'Northeast', '2024-01-09T09:10:00'),
(1015, 'RCP-20240109-008', 'STR-004', 'SKU-3004', 'EMP-105', '2024-01-09',  1,  19.99, 0.15, 'debit_card',  'BSK-008', 'Northeast', '2024-01-09T09:10:00'),
(1016, 'RCP-20240113-009', 'STR-003', 'SKU-4004', 'EMP-104', '2024-01-13',  3,  24.99, 0.00, 'mobile_pay',  'BSK-009', 'Northeast', '2024-01-13T12:30:00'),
(1017, 'RCP-20240116-010', 'STR-004', 'SKU-1001', 'EMP-106', '2024-01-16',  1,   4.99, 0.00, 'cash',        'BSK-010', 'Northeast', '2024-01-16T16:45:00'),
(1018, 'RCP-20240116-010', 'STR-004', 'SKU-4003', 'EMP-106', '2024-01-16',  2,   8.99, 0.00, 'cash',        'BSK-010', 'Northeast', '2024-01-16T16:45:00'),
(1019, 'RCP-20240120-011', 'STR-004', 'SKU-2004', 'EMP-105', '2024-01-20',  1,  59.99, 0.05, 'credit_card', 'BSK-011', 'Northeast', '2024-01-20T10:15:00'),
-- January 2024 - West
(1020, 'RCP-20240107-012', 'STR-005', 'SKU-3003', 'EMP-107', '2024-01-07',  1,  49.99, 0.00, 'credit_card', 'BSK-012', 'West', '2024-01-07T10:30:00'),
(1021, 'RCP-20240107-012', 'STR-005', 'SKU-3001', 'EMP-107', '2024-01-07',  2,  14.99, 0.00, 'credit_card', 'BSK-012', 'West', '2024-01-07T10:30:00'),
(1022, 'RCP-20240111-013', 'STR-006', 'SKU-4005', 'EMP-108', '2024-01-11',  2,   6.99, 0.00, 'debit_card',  'BSK-013', 'West', '2024-01-11T18:00:00'),
(1023, 'RCP-20240111-013', 'STR-006', 'SKU-4001', 'EMP-108', '2024-01-11',  1,  12.99, 0.00, 'debit_card',  'BSK-013', 'West', '2024-01-11T18:00:00'),
(1024, 'RCP-20240114-014', 'STR-005', 'SKU-2001', 'EMP-107', '2024-01-14',  3,  49.99, 0.05, 'mobile_pay',  'BSK-014', 'West', '2024-01-14T11:00:00'),
(1025, 'RCP-20240119-015', 'STR-006', 'SKU-1002', 'EMP-108', '2024-01-19',  1,   5.49, 0.00, 'cash',        'BSK-015', 'West', '2024-01-19T19:30:00'),
-- February 2024 - Midwest
(1026, 'RCP-20240202-016', 'STR-001', 'SKU-4002', 'EMP-101', '2024-02-02',  2,  14.99, 0.00, 'credit_card', 'BSK-016', 'Midwest', '2024-02-02T08:00:00'),
(1027, 'RCP-20240202-016', 'STR-001', 'SKU-1001', 'EMP-101', '2024-02-02',  3,   4.99, 0.00, 'credit_card', 'BSK-016', 'Midwest', '2024-02-02T08:00:00'),
(1028, 'RCP-20240205-017', 'STR-002', 'SKU-2002', 'EMP-103', '2024-02-05',  1,  24.99, 0.00, 'debit_card',  'BSK-017', 'Midwest', '2024-02-05T09:30:00'),
(1029, 'RCP-20240205-017', 'STR-002', 'SKU-2005', 'EMP-103', '2024-02-05',  1,  29.99, 0.00, 'debit_card',  'BSK-017', 'Midwest', '2024-02-05T09:30:00'),
(1030, 'RCP-20240208-018', 'STR-001', 'SKU-3002', 'EMP-102', '2024-02-08',  1,  24.99, 0.20, 'credit_card', 'BSK-018', 'Midwest', '2024-02-08T14:00:00'),
(1031, 'RCP-20240210-019', 'STR-002', 'SKU-1003', 'EMP-103', '2024-02-10',  2,   5.99, 0.00, 'cash',        'BSK-019', 'Midwest', '2024-02-10T10:00:00'),
(1032, 'RCP-20240210-019', 'STR-002', 'SKU-1005', 'EMP-103', '2024-02-10',  6,   3.99, 0.00, 'cash',        'BSK-019', 'Midwest', '2024-02-10T10:00:00'),
(1033, 'RCP-20240214-020', 'STR-001', 'SKU-3003', 'EMP-101', '2024-02-14',  1,  49.99, 0.00, 'mobile_pay',  'BSK-020', 'Midwest', '2024-02-14T08:45:00'),
(1034, 'RCP-20240214-020', 'STR-001', 'SKU-3004', 'EMP-101', '2024-02-14',  2,  19.99, 0.00, 'mobile_pay',  'BSK-020', 'Midwest', '2024-02-14T08:45:00'),
(1035, 'RCP-20240218-021', 'STR-001', 'SKU-2003', 'EMP-102', '2024-02-18', -1,  29.99, 0.00, 'credit_card', 'BSK-021', 'Midwest', '2024-02-18T15:30:00'),
-- February 2024 - Northeast
(1036, 'RCP-20240203-022', 'STR-003', 'SKU-4001', 'EMP-104', '2024-02-03',  2,  12.99, 0.00, 'credit_card', 'BSK-022', 'Northeast', '2024-02-03T11:30:00'),
(1037, 'RCP-20240203-022', 'STR-003', 'SKU-4005', 'EMP-104', '2024-02-03',  3,   6.99, 0.00, 'credit_card', 'BSK-022', 'Northeast', '2024-02-03T11:30:00'),
(1038, 'RCP-20240207-023', 'STR-004', 'SKU-1004', 'EMP-105', '2024-02-07',  1,  16.99, 0.00, 'debit_card',  'BSK-023', 'Northeast', '2024-02-07T09:00:00'),
(1039, 'RCP-20240207-023', 'STR-004', 'SKU-1001', 'EMP-105', '2024-02-07',  2,   4.99, 0.00, 'debit_card',  'BSK-023', 'Northeast', '2024-02-07T09:00:00'),
(1040, 'RCP-20240211-024', 'STR-003', 'SKU-2004', 'EMP-104', '2024-02-11',  1,  59.99, 0.10, 'mobile_pay',  'BSK-024', 'Northeast', '2024-02-11T12:00:00'),
(1041, 'RCP-20240215-025', 'STR-004', 'SKU-3001', 'EMP-106', '2024-02-15',  4,  14.99, 0.00, 'cash',        'BSK-025', 'Northeast', '2024-02-15T16:00:00'),
(1042, 'RCP-20240215-025', 'STR-004', 'SKU-3005', 'EMP-106', '2024-02-15',  3,   9.99, 0.00, 'cash',        'BSK-025', 'Northeast', '2024-02-15T16:00:00'),
(1043, 'RCP-20240219-026', 'STR-004', 'SKU-4004', 'EMP-105', '2024-02-19', -2,  24.99, 0.00, 'credit_card', 'BSK-026', 'Northeast', '2024-02-19T10:30:00'),
(1044, 'RCP-20240222-027', 'STR-003', 'SKU-2002', 'EMP-104', '2024-02-22',  2,  24.99, 0.05, 'credit_card', 'BSK-027', 'Northeast', '2024-02-22T13:15:00'),
-- February 2024 - West
(1045, 'RCP-20240204-028', 'STR-005', 'SKU-1001', 'EMP-107', '2024-02-04',  4,   4.99, 0.00, 'credit_card', 'BSK-028', 'West', '2024-02-04T10:00:00'),
(1046, 'RCP-20240204-028', 'STR-005', 'SKU-4003', 'EMP-107', '2024-02-04',  2,   8.99, 0.00, 'credit_card', 'BSK-028', 'West', '2024-02-04T10:00:00'),
(1047, 'RCP-20240209-029', 'STR-006', 'SKU-2001', 'EMP-108', '2024-02-09',  1,  49.99, 0.00, 'debit_card',  'BSK-029', 'West', '2024-02-09T18:30:00'),
(1048, 'RCP-20240212-030', 'STR-005', 'SKU-3002', 'EMP-107', '2024-02-12',  2,  24.99, 0.10, 'mobile_pay',  'BSK-030', 'West', '2024-02-12T11:45:00'),
(1049, 'RCP-20240216-031', 'STR-006', 'SKU-4002', 'EMP-108', '2024-02-16',  1,  14.99, 0.00, 'cash',        'BSK-031', 'West', '2024-02-16T19:00:00'),
(1050, 'RCP-20240220-032', 'STR-005', 'SKU-2004', 'EMP-107', '2024-02-20', -1,  59.99, 0.00, 'credit_card', 'BSK-032', 'West', '2024-02-20T10:30:00'),
-- March 2024 - Midwest
(1051, 'RCP-20240301-033', 'STR-001', 'SKU-1001', 'EMP-101', '2024-03-01',  2,   4.99, 0.00, 'credit_card', 'BSK-033', 'Midwest', '2024-03-01T08:00:00'),
(1052, 'RCP-20240301-033', 'STR-001', 'SKU-1002', 'EMP-101', '2024-03-01',  1,   5.49, 0.00, 'credit_card', 'BSK-033', 'Midwest', '2024-03-01T08:00:00'),
(1053, 'RCP-20240303-034', 'STR-002', 'SKU-2001', 'EMP-103', '2024-03-03',  2,  49.99, 0.05, 'debit_card',  'BSK-034', 'Midwest', '2024-03-03T09:00:00'),
(1054, 'RCP-20240306-035', 'STR-001', 'SKU-4004', 'EMP-102', '2024-03-06',  1,  24.99, 0.00, 'mobile_pay',  'BSK-035', 'Midwest', '2024-03-06T14:20:00'),
(1055, 'RCP-20240306-035', 'STR-001', 'SKU-4005', 'EMP-102', '2024-03-06',  2,   6.99, 0.00, 'mobile_pay',  'BSK-035', 'Midwest', '2024-03-06T14:20:00'),
(1056, 'RCP-20240310-036', 'STR-002', 'SKU-3001', 'EMP-103', '2024-03-10',  5,  14.99, 0.10, 'cash',        'BSK-036', 'Midwest', '2024-03-10T10:15:00'),
(1057, 'RCP-20240310-036', 'STR-002', 'SKU-3004', 'EMP-103', '2024-03-10',  1,  19.99, 0.10, 'cash',        'BSK-036', 'Midwest', '2024-03-10T10:15:00'),
(1058, 'RCP-20240315-037', 'STR-001', 'SKU-2003', 'EMP-101', '2024-03-15',  1,  29.99, 0.00, 'credit_card', 'BSK-037', 'Midwest', '2024-03-15T08:30:00'),
(1059, 'RCP-20240315-037', 'STR-001', 'SKU-1005', 'EMP-101', '2024-03-15',  3,   3.99, 0.00, 'credit_card', 'BSK-037', 'Midwest', '2024-03-15T08:30:00'),
-- March 2024 - Northeast
(1060, 'RCP-20240302-038', 'STR-003', 'SKU-1003', 'EMP-104', '2024-03-02',  2,   5.99, 0.00, 'credit_card', 'BSK-038', 'Northeast', '2024-03-02T11:00:00'),
(1061, 'RCP-20240302-038', 'STR-003', 'SKU-4002', 'EMP-104', '2024-03-02',  1,  14.99, 0.00, 'credit_card', 'BSK-038', 'Northeast', '2024-03-02T11:00:00'),
(1062, 'RCP-20240305-039', 'STR-004', 'SKU-2005', 'EMP-105', '2024-03-05',  2,  29.99, 0.00, 'debit_card',  'BSK-039', 'Northeast', '2024-03-05T09:30:00'),
(1063, 'RCP-20240308-040', 'STR-003', 'SKU-3003', 'EMP-104', '2024-03-08',  1,  49.99, 0.00, 'mobile_pay',  'BSK-040', 'Northeast', '2024-03-08T12:45:00'),
(1064, 'RCP-20240312-041', 'STR-004', 'SKU-1004', 'EMP-106', '2024-03-12',  1,  16.99, 0.00, 'cash',        'BSK-041', 'Northeast', '2024-03-12T16:30:00'),
(1065, 'RCP-20240312-041', 'STR-004', 'SKU-4003', 'EMP-106', '2024-03-12',  3,   8.99, 0.00, 'cash',        'BSK-041', 'Northeast', '2024-03-12T16:30:00'),
(1066, 'RCP-20240316-042', 'STR-004', 'SKU-2001', 'EMP-105', '2024-03-16',  1,  49.99, 0.00, 'credit_card', 'BSK-042', 'Northeast', '2024-03-16T10:00:00'),
(1067, 'RCP-20240316-042', 'STR-004', 'SKU-2002', 'EMP-105', '2024-03-16',  1,  24.99, 0.00, 'credit_card', 'BSK-042', 'Northeast', '2024-03-16T10:00:00'),
-- March 2024 - West
(1068, 'RCP-20240304-043', 'STR-005', 'SKU-4001', 'EMP-107', '2024-03-04',  3,  12.99, 0.00, 'credit_card', 'BSK-043', 'West', '2024-03-04T10:00:00'),
(1069, 'RCP-20240304-043', 'STR-005', 'SKU-1002', 'EMP-107', '2024-03-04',  2,   5.49, 0.00, 'credit_card', 'BSK-043', 'West', '2024-03-04T10:00:00'),
(1070, 'RCP-20240307-044', 'STR-006', 'SKU-2002', 'EMP-108', '2024-03-07',  1,  24.99, 0.00, 'debit_card',  'BSK-044', 'West', '2024-03-07T18:15:00'),
(1071, 'RCP-20240311-045', 'STR-005', 'SKU-3001', 'EMP-107', '2024-03-11',  2,  14.99, 0.00, 'mobile_pay',  'BSK-045', 'West', '2024-03-11T11:30:00'),
(1072, 'RCP-20240311-045', 'STR-005', 'SKU-3002', 'EMP-107', '2024-03-11',  1,  24.99, 0.00, 'mobile_pay',  'BSK-045', 'West', '2024-03-11T11:30:00'),
(1073, 'RCP-20240314-046', 'STR-006', 'SKU-4005', 'EMP-108', '2024-03-14',  4,   6.99, 0.00, 'cash',        'BSK-046', 'West', '2024-03-14T19:00:00'),
(1074, 'RCP-20240318-047', 'STR-005', 'SKU-2004', 'EMP-107', '2024-03-18',  1,  59.99, 0.00, 'credit_card', 'BSK-047', 'West', '2024-03-18T10:45:00'),
(1075, 'RCP-20240318-047', 'STR-005', 'SKU-4004', 'EMP-107', '2024-03-18',  2,  24.99, 0.05, 'credit_card', 'BSK-047', 'West', '2024-03-18T10:45:00');

ASSERT ROW_COUNT = 75
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_transactions;

