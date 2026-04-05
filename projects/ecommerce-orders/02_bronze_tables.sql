-- =============================================================================
-- E-commerce Orders Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for E-commerce Orders'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'setup', 'ecommerce-orders'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

-- Customers dimension feed (18 rows)
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_customers (
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
) LOCATION 'ecom/ecommerce/bronze/raw_customers';

-- Products dimension feed (20 rows)
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_products (
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
) LOCATION 'ecom/ecommerce/bronze/raw_products';

-- Web channel orders (30 rows)
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_web_orders (
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
) LOCATION 'ecom/ecommerce/bronze/raw_web_orders';

-- Mobile app orders (20 rows)
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_mobile_orders (
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
) LOCATION 'ecom/ecommerce/bronze/raw_mobile_orders';

-- POS terminal orders (25 rows)
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_pos_orders (
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
) LOCATION 'ecom/ecommerce/bronze/raw_pos_orders';

-- Browsing events for funnel sessionization
CREATE DELTA TABLE IF NOT EXISTS ecom.bronze.raw_browsing_events (
  event_id      STRING      NOT NULL,
  customer_id   STRING      NOT NULL,
  session_id    STRING,
  event_type    STRING      NOT NULL,
  event_ts      TIMESTAMP   NOT NULL,
  page_url      STRING,
  product_id    STRING,
  ingested_at   TIMESTAMP
) LOCATION 'ecom/ecommerce/bronze/raw_browsing_events';
