-- =============================================================================
-- Bronze: Reference Data
-- =============================================================================
-- Master data tables: 6 suppliers, 4 warehouses, 15 products (3 categories),
-- and 8 stores (3 regions). These are slowly changing dimensions seeded here
-- as bronze and promoted to gold dimensions with enrichment.
-- =============================================================================

-- -------------------------------------------------------------------------
-- Suppliers (6)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{zone_prefix}}.bronze.suppliers (
  supplier_id       STRING,
  supplier_name     STRING,
  country           STRING,
  lead_time_days    INT,
  reliability_rating DECIMAL(3,2),
  payment_terms     STRING,
  active            BOOLEAN,
  ingested_at       TIMESTAMP
);

INSERT INTO {{zone_prefix}}.bronze.suppliers VALUES
  ('S001', 'Acme Packaging',           'US',      7,  0.94, 'NET30', true,  '2026-03-01 00:00:00'),
  ('S002', 'Global Ingredients',       'US',      10, 0.88, 'NET45', true,  '2026-03-01 00:00:00'),
  ('S003', 'Fresh Farm Co-op',         'US',      5,  0.96, 'NET15', true,  '2026-03-01 00:00:00'),
  ('S004', 'Pacific Plastics',         'CN',      14, 0.82, 'NET60', true,  '2026-03-01 00:00:00'),
  ('S005', 'EuroChemicals AG',         'DE',      12, 0.90, 'NET45', true,  '2026-03-01 00:00:00'),
  ('S006', 'Atlas Logistics Materials', 'MX',      8,  0.91, 'NET30', true,  '2026-03-01 00:00:00');

-- -------------------------------------------------------------------------
-- Warehouses (4)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{zone_prefix}}.bronze.warehouses (
  warehouse_id    STRING,
  warehouse_name  STRING,
  city            STRING,
  state           STRING,
  region          STRING,
  capacity_units  INT,
  active          BOOLEAN,
  ingested_at     TIMESTAMP
);

INSERT INTO {{zone_prefix}}.bronze.warehouses VALUES
  ('W001', 'East Coast DC',        'Newark',       'NJ', 'east',    50000, true, '2026-03-01 00:00:00'),
  ('W002', 'West Coast DC',        'Los Angeles',  'CA', 'west',    45000, true, '2026-03-01 00:00:00'),
  ('W003', 'Central Hub',          'Chicago',      'IL', 'central', 60000, true, '2026-03-01 00:00:00'),
  ('W004', 'Southern Fulfillment', 'Houston',      'TX', 'south',   40000, true, '2026-03-01 00:00:00');

-- -------------------------------------------------------------------------
-- Products (15 across 3 categories)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{zone_prefix}}.bronze.products (
  sku             STRING,
  product_name    STRING,
  category        STRING,
  subcategory     STRING,
  unit_weight_kg  DECIMAL(6,2),
  unit_price      DECIMAL(10,2),
  active          BOOLEAN,
  ingested_at     TIMESTAMP
);

INSERT INTO {{zone_prefix}}.bronze.products VALUES
  -- Beverages (5 SKUs)
  ('SKU-001', 'Sparkling Water 500ml',    'Beverages', 'Water',         0.55, 4.99,  true, '2026-03-01 00:00:00'),
  ('SKU-002', 'Orange Juice 1L',          'Beverages', 'Juice',         1.10, 2.49,  true, '2026-03-01 00:00:00'),
  ('SKU-003', 'Green Tea 250ml',          'Beverages', 'Tea',           0.30, 9.50,  true, '2026-03-01 00:00:00'),
  ('SKU-004', 'Cola 330ml 12-pack',       'Beverages', 'Soda',          4.20, 6.80,  true, '2026-03-01 00:00:00'),
  ('SKU-005', 'Craft IPA 330ml 6-pack',   'Beverages', 'Beer',          2.40, 16.40, true, '2026-03-01 00:00:00'),

  -- Snacks (5 SKUs)
  ('SKU-006', 'Potato Chips 200g',        'Snacks',    'Chips',         0.22, 13.60, true, '2026-03-01 00:00:00'),
  ('SKU-007', 'Organic Granola Bar 5pk',  'Snacks',    'Bars',          0.35, 24.95, true, '2026-03-01 00:00:00'),
  ('SKU-008', 'Mixed Nuts 300g',          'Snacks',    'Nuts',          0.32, 6.20,  true, '2026-03-01 00:00:00'),
  ('SKU-009', 'Rice Crackers 150g',       'Snacks',    'Crackers',      0.16, 5.70,  true, '2026-03-01 00:00:00'),
  ('SKU-010', 'Trail Mix 250g',           'Snacks',    'Mix',           0.27, 4.00,  true, '2026-03-01 00:00:00'),

  -- Personal Care (5 SKUs)
  ('SKU-011', 'Shampoo 400ml',            'Personal Care', 'Hair',      0.45, 8.30,  true, '2026-03-01 00:00:00'),
  ('SKU-012', 'Body Wash 500ml',          'Personal Care', 'Body',      0.55, 11.20, true, '2026-03-01 00:00:00'),
  ('SKU-013', 'Face Moisturizer 50ml',    'Personal Care', 'Skin',      0.08, 19.60, true, '2026-03-01 00:00:00'),
  ('SKU-014', 'Sunscreen SPF50 100ml',    'Personal Care', 'Sun',       0.12, 15.10, true, '2026-03-01 00:00:00'),
  ('SKU-015', 'Hand Sanitizer 250ml',     'Personal Care', 'Hygiene',   0.28, 10.50, true, '2026-03-01 00:00:00');

-- -------------------------------------------------------------------------
-- Stores (8 across 3 regions)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{zone_prefix}}.bronze.stores (
  store_id        STRING,
  store_name      STRING,
  city            STRING,
  state           STRING,
  region          STRING,
  format          STRING,
  active          BOOLEAN,
  ingested_at     TIMESTAMP
);

INSERT INTO {{zone_prefix}}.bronze.stores VALUES
  ('ST01', 'DC Metro Supermart',      'Washington',   'DC', 'east',    'supermarket', true, '2026-03-01 00:00:00'),
  ('ST02', 'Boston Fresh Market',     'Boston',       'MA', 'east',    'supermarket', true, '2026-03-01 00:00:00'),
  ('ST03', 'Phoenix Value Store',     'Phoenix',      'AZ', 'west',    'discount',    true, '2026-03-01 00:00:00'),
  ('ST04', 'Norfolk Harbor Shop',     'Norfolk',      'VA', 'east',    'convenience', true, '2026-03-01 00:00:00'),
  ('ST05', 'SF Bay Organics',         'San Francisco','CA', 'west',    'specialty',   true, '2026-03-01 00:00:00'),
  ('ST06', 'Indy Central Mart',       'Indianapolis', 'IN', 'central', 'supermarket', true, '2026-03-01 00:00:00'),
  ('ST07', 'Minneapolis Fresh',       'Minneapolis',  'MN', 'central', 'supermarket', true, '2026-03-01 00:00:00'),
  ('ST08', 'Dallas Big Box',          'Dallas',       'TX', 'south',   'warehouse',   true, '2026-03-01 00:00:00');
