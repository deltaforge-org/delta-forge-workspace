-- =============================================================================
-- E-commerce Orders Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE ecommerce_orders_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for E-commerce Orders'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'setup', 'ecommerce-orders'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON ecom.bronze.raw_customers (email);
CREATE PSEUDONYMISATION RULE ON ecom.bronze.raw_customers (email) TRANSFORM redact PARAMS (mask = '***@***.***');
DROP PSEUDONYMISATION RULE IF EXISTS ON ecom.silver.customer_rfm (email);
CREATE PSEUDONYMISATION RULE ON ecom.silver.customer_rfm (email) TRANSFORM redact PARAMS (mask = '***@***.***');
DROP PSEUDONYMISATION RULE IF EXISTS ON ecom.bronze.raw_customers (address);
CREATE PSEUDONYMISATION RULE ON ecom.bronze.raw_customers (address) TRANSFORM mask PARAMS (show = 5);

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE ecom.bronze.raw_customers TO USER admin;
GRANT ADMIN ON TABLE ecom.bronze.raw_products TO USER admin;
GRANT ADMIN ON TABLE ecom.bronze.raw_web_orders TO USER admin;
GRANT ADMIN ON TABLE ecom.bronze.raw_mobile_orders TO USER admin;
GRANT ADMIN ON TABLE ecom.bronze.raw_pos_orders TO USER admin;
GRANT ADMIN ON TABLE ecom.bronze.raw_browsing_events TO USER admin;
GRANT ADMIN ON TABLE ecom.silver.orders_unified TO USER admin;
GRANT ADMIN ON TABLE ecom.silver.customer_rfm TO USER admin;
GRANT ADMIN ON TABLE ecom.silver.inventory_adjustments TO USER admin;
GRANT ADMIN ON TABLE ecom.silver.sessions TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.dim_product TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.dim_customer TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.dim_channel TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.dim_date TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.fact_order_lines TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.kpi_sales_dashboard TO USER admin;
GRANT ADMIN ON TABLE ecom.gold.kpi_funnel_analysis TO USER admin;
