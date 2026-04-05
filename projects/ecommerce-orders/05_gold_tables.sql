-- =============================================================================
-- E-commerce Orders Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE ecommerce_orders_05_gold_tables
  DESCRIPTION 'Creates gold layer tables for E-commerce Orders'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'setup', 'ecommerce-orders'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.dim_product (
  product_key   STRING      NOT NULL,
  sku           STRING,
  product_name  STRING,
  category      STRING,
  subcategory   STRING,
  brand         STRING,
  unit_cost     DECIMAL(10,2),
  list_price    DECIMAL(10,2)
) LOCATION 'ecom/ecommerce/gold/dim_product';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.dim_customer (
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
) LOCATION 'ecom/ecommerce/gold/dim_customer';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.dim_channel (
  channel_key   STRING      NOT NULL,
  channel_name  STRING,
  channel_detail STRING
) LOCATION 'ecom/ecommerce/gold/dim_channel';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.dim_date (
  date_key      INT         NOT NULL,
  full_date     DATE        NOT NULL,
  year          INT,
  quarter       INT,
  month         INT,
  month_name    STRING,
  day_of_week   INT,
  day_name      STRING,
  is_weekend    BOOLEAN
) LOCATION 'ecom/ecommerce/gold/dim_date';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.fact_order_lines (
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
) LOCATION 'ecom/ecommerce/gold/fact_order_lines';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.kpi_sales_dashboard (
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
) LOCATION 'ecom/ecommerce/gold/kpi_sales_dashboard';

CREATE DELTA TABLE IF NOT EXISTS ecom.gold.kpi_funnel_analysis (
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
) LOCATION 'ecom/ecommerce/gold/kpi_funnel_analysis';
