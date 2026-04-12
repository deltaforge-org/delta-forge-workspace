-- ============================================================================
-- WWI Data Lake - Teardown (complete cleanup)
-- ============================================================================
-- Drops all objects in reverse dependency order:
--   1. Gold facts (depend on dimensions via FK convention)
--   2. Gold dimensions
--   3. Silver views (depend on bronze)
--   4. Bronze tables
--   5. Schemas
--   6. Zone
-- ============================================================================

-- Gold facts
DROP DELTA TABLE IF EXISTS wwi_lake.gold.fact_supplier_transaction WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.fact_customer_transaction WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.fact_purchase WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.fact_sale WITH FILES;

-- Gold dimensions
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_transaction_type WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_payment_method WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_delivery_method WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_employee WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_city WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_supplier WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_customer WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.gold.dim_date WITH FILES;

-- Silver views
DROP VIEW IF EXISTS wwi_lake.silver.v_supplier_transaction;
DROP VIEW IF EXISTS wwi_lake.silver.v_customer_transaction;
DROP VIEW IF EXISTS wwi_lake.silver.v_purchase;
DROP VIEW IF EXISTS wwi_lake.silver.v_sale;
DROP VIEW IF EXISTS wwi_lake.silver.v_supplier;
DROP VIEW IF EXISTS wwi_lake.silver.v_customer;
DROP VIEW IF EXISTS wwi_lake.silver.v_employee;
DROP VIEW IF EXISTS wwi_lake.silver.v_city;

-- Bronze incremental tables
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.supplier_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.purchase_order_lines WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.purchase_orders WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.customer_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.invoice_lines WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.invoices WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.order_lines WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.orders WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.suppliers WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.customers WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.people WITH FILES;

-- Bronze reference tables
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.special_deals WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.system_parameters WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.customer_categories WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.buying_groups WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.supplier_categories WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.transaction_types WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.payment_methods WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.delivery_methods WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.cities WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.state_provinces WITH FILES;
DROP DELTA TABLE IF EXISTS wwi_lake.bronze.countries WITH FILES;

-- Schemas
DROP SCHEMA IF EXISTS wwi_lake.gold;
DROP SCHEMA IF EXISTS wwi_lake.silver;
DROP SCHEMA IF EXISTS wwi_lake.bronze;

-- Zone
DROP ZONE IF EXISTS wwi_lake;
