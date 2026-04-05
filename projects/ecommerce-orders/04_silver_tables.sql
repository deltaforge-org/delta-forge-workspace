-- =============================================================================
-- E-commerce Orders Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for E-commerce Orders'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'setup', 'ecommerce-orders'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- Unified orders from 3 channels, soft-delete for cancellations, CDF-enabled
CREATE DELTA TABLE IF NOT EXISTS ecom.silver.orders_unified (
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
) LOCATION 'ecom/ecommerce/silver/orders_unified'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Customer RFM segmentation with NTILE quartile scoring
CREATE DELTA TABLE IF NOT EXISTS ecom.silver.customer_rfm (
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
) LOCATION 'ecom/ecommerce/silver/customer_rfm';

-- Inventory adjustments driven by CDF on orders_unified
CREATE DELTA TABLE IF NOT EXISTS ecom.silver.inventory_adjustments (
  adjustment_id   STRING      NOT NULL,
  product_id      STRING      NOT NULL,
  order_id        STRING      NOT NULL,
  change_type     STRING      NOT NULL,
  quantity_delta  INT         NOT NULL,
  reason          STRING,
  captured_at     TIMESTAMP
) LOCATION 'ecom/ecommerce/silver/inventory_adjustments';

-- Sessionized browsing events for funnel analysis
CREATE DELTA TABLE IF NOT EXISTS ecom.silver.sessions (
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
) LOCATION 'ecom/ecommerce/silver/sessions';
