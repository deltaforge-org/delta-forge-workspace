-- ============================================================================
-- WWI Data Lake - Master Pipeline
-- ============================================================================
--
-- PREREQUISITE:
--   A SQL Server connection named "mssql" must exist in the Delta Forge
--   connection manager pointing to a WideWorldImporters database instance.
--   The connection prefix determines the catalog namespace — all source
--   tables are accessed as:
--
--       mssql_WideWorldImporters.<Schema>.<Table>
--
--   where "mssql" is the connection name and "WideWorldImporters" is the
--   database name. Create the connection via the GUI Connections page or
--   the control plane API before running this setup.
--
-- Execution order (fully idempotent, safe to run repeatedly):
--   1. Preflight  - verify MSSQL connection is reachable
--   2. DDL        - create zone, schemas, tables (IF NOT EXISTS)
--   3. Bronze     - ingest from MSSQL (full load + incremental with 7-day overlap)
--   4. Silver     - create/refresh transformation views (CREATE OR REPLACE)
--   5. Gold       - materialize star schema dimensions and facts (MERGE)
-- ============================================================================

PIPELINE wwi_lake.pipeline
    DESCRIPTION 'WWI datalake - bronze/silver/gold medallion from MSSQL source'
    TAGS 'wwi', 'medallion', 'mssql'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- ---------------------------------------------------------------------------
-- PREFLIGHT: verify the MSSQL connection exists and is reachable
-- ---------------------------------------------------------------------------

ASSERT ERROR ROW_COUNT > 0
SELECT cityid FROM mssql_WideWorldImporters.Application.Cities LIMIT 1;
-- If this fails the "mssql" connection to WideWorldImporters is missing.
-- Create it in the Connections page (provider: SQL Server, name: mssql,
-- database: WideWorldImporters) then re-run this pipeline.

-- ---------------------------------------------------------------------------
-- ZONE & SCHEMAS
-- ---------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS wwi_lake
    TYPE MANAGED
    COMMENT 'WideWorldImporters Delta Lake - medallion architecture';

CREATE SCHEMA IF NOT EXISTS wwi_lake.bronze
    COMMENT 'Raw ingestion layer - 1:1 with MSSQL source, snake_case columns';

CREATE SCHEMA IF NOT EXISTS wwi_lake.silver
    COMMENT 'Views: cleaned, enriched, business-logic applied over bronze';

CREATE SCHEMA IF NOT EXISTS wwi_lake.gold
    COMMENT 'Star schema for end users - materialized dimensions and facts';

-- ===========================================================================
-- BRONZE TABLES - Full Load (small reference/dimension tables)
-- ===========================================================================

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.countries (
    country_id                INT NOT NULL,
    country_name              VARCHAR NOT NULL,
    formal_name               VARCHAR,
    iso_alpha3_code           VARCHAR,
    iso_numeric_code          INT,
    country_type              VARCHAR,
    latest_recorded_population BIGINT,
    continent                 VARCHAR,
    region                    VARCHAR,
    subregion                 VARCHAR,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/countries'
COMMENT 'Raw countries from Application.Countries';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.state_provinces (
    state_province_id         INT NOT NULL,
    state_province_code       VARCHAR,
    state_province_name       VARCHAR NOT NULL,
    country_id                INT NOT NULL,
    sales_territory           VARCHAR,
    latest_recorded_population BIGINT,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/state_provinces'
COMMENT 'Raw state/provinces from Application.StateProvinces';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.cities (
    city_id                   INT NOT NULL,
    city_name                 VARCHAR NOT NULL,
    state_province_id         INT NOT NULL,
    latest_recorded_population BIGINT,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/cities'
COMMENT 'Raw cities from Application.Cities';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.delivery_methods (
    delivery_method_id        INT NOT NULL,
    delivery_method_name      VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/delivery_methods'
COMMENT 'Raw delivery methods from Application.DeliveryMethods';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.payment_methods (
    payment_method_id         INT NOT NULL,
    payment_method_name       VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/payment_methods'
COMMENT 'Raw payment methods from Application.PaymentMethods';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.transaction_types (
    transaction_type_id       INT NOT NULL,
    transaction_type_name     VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/transaction_types'
COMMENT 'Raw transaction types from Application.TransactionTypes';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.supplier_categories (
    supplier_category_id      INT NOT NULL,
    supplier_category_name    VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/supplier_categories'
COMMENT 'Raw supplier categories from Purchasing.SupplierCategories';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.buying_groups (
    buying_group_id           INT NOT NULL,
    buying_group_name         VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/buying_groups'
COMMENT 'Raw buying groups from Sales.BuyingGroups';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customer_categories (
    customer_category_id      INT NOT NULL,
    customer_category_name    VARCHAR NOT NULL,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/customer_categories'
COMMENT 'Raw customer categories from Sales.CustomerCategories';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.system_parameters (
    system_parameter_id       INT NOT NULL,
    delivery_address_line1    VARCHAR,
    delivery_address_line2    VARCHAR,
    delivery_city_id          INT,
    delivery_postal_code      VARCHAR,
    postal_address_line1      VARCHAR,
    postal_address_line2      VARCHAR,
    postal_city_id            INT,
    postal_postal_code        VARCHAR,
    application_settings      VARCHAR,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP
)
LOCATION '$data_path/bronze/system_parameters'
COMMENT 'Raw system parameters from Application.SystemParameters';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.special_deals (
    special_deal_id           INT NOT NULL,
    stock_item_id             INT,
    customer_id               INT,
    buying_group_id           INT,
    customer_category_id      INT,
    stock_group_id            INT,
    deal_description          VARCHAR,
    start_date                DATE,
    end_date                  DATE,
    discount_amount           DECIMAL(18,2),
    discount_percentage       DECIMAL(18,3),
    unit_price                DECIMAL(18,2),
    last_edited_by            INT,
    last_edited_when          TIMESTAMP
)
LOCATION '$data_path/bronze/special_deals'
COMMENT 'Raw special deals from Sales.SpecialDeals';

-- ===========================================================================
-- BRONZE TABLES - Incremental Load (large transactional tables)
-- ===========================================================================

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.people (
    person_id                 INT NOT NULL,
    full_name                 VARCHAR NOT NULL,
    preferred_name            VARCHAR,
    is_permitted_to_logon     BOOLEAN,
    logon_name                VARCHAR,
    is_external_logon_provider BOOLEAN,
    is_system_user            BOOLEAN,
    is_employee               BOOLEAN,
    is_salesperson            BOOLEAN,
    user_preferences          VARCHAR,
    phone_number              VARCHAR,
    fax_number                VARCHAR,
    email_address             VARCHAR,
    custom_fields             VARCHAR,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/people'
COMMENT 'Raw people from Application.People';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customers (
    customer_id               INT NOT NULL,
    customer_name             VARCHAR NOT NULL,
    bill_to_customer_id       INT,
    customer_category_id      INT,
    buying_group_id           INT,
    primary_contact_person_id INT,
    alternate_contact_person_id INT,
    delivery_method_id        INT,
    delivery_city_id          INT,
    postal_city_id            INT,
    credit_limit              DECIMAL(18,2),
    account_opened_date       DATE,
    standard_discount_percentage DECIMAL(18,3),
    is_statement_sent         BOOLEAN,
    is_on_credit_hold         BOOLEAN,
    payment_days              INT,
    phone_number              VARCHAR,
    fax_number                VARCHAR,
    delivery_run              VARCHAR,
    run_position              VARCHAR,
    website_url               VARCHAR,
    delivery_address_line1    VARCHAR,
    delivery_address_line2    VARCHAR,
    delivery_postal_code      VARCHAR,
    postal_address_line1      VARCHAR,
    postal_address_line2      VARCHAR,
    postal_postal_code        VARCHAR,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/customers'
COMMENT 'Raw customers from Sales.Customers';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.suppliers (
    supplier_id               INT NOT NULL,
    supplier_name             VARCHAR NOT NULL,
    supplier_category_id      INT,
    primary_contact_person_id INT,
    alternate_contact_person_id INT,
    delivery_method_id        INT,
    delivery_city_id          INT,
    postal_city_id            INT,
    supplier_reference        VARCHAR,
    bank_account_name         VARCHAR,
    bank_account_branch       VARCHAR,
    bank_account_code         VARCHAR,
    bank_account_number       VARCHAR,
    bank_international_code   VARCHAR,
    payment_days              INT,
    internal_comments         VARCHAR,
    phone_number              VARCHAR,
    fax_number                VARCHAR,
    website_url               VARCHAR,
    delivery_address_line1    VARCHAR,
    delivery_address_line2    VARCHAR,
    delivery_postal_code      VARCHAR,
    postal_address_line1      VARCHAR,
    postal_address_line2      VARCHAR,
    postal_postal_code        VARCHAR,
    last_edited_by            INT,
    valid_from                TIMESTAMP,
    valid_to                  TIMESTAMP
)
LOCATION '$data_path/bronze/suppliers'
COMMENT 'Raw suppliers from Purchasing.Suppliers';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.orders (
    order_id                  INT NOT NULL,
    customer_id               INT NOT NULL,
    salesperson_person_id     INT,
    picked_by_person_id       INT,
    contact_person_id         INT,
    backorder_order_id        INT,
    order_date                DATE NOT NULL,
    expected_delivery_date    DATE,
    customer_purchase_order_number VARCHAR,
    is_undersupply_backordered BOOLEAN,
    comments                  VARCHAR,
    delivery_instructions     VARCHAR,
    internal_comments         VARCHAR,
    picking_completed_when    TIMESTAMP,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/orders'
COMMENT 'Raw orders from Sales.Orders';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.order_lines (
    order_line_id             INT NOT NULL,
    order_id                  INT NOT NULL,
    stock_item_id             INT NOT NULL,
    description               VARCHAR,
    package_type_id           INT,
    quantity                  INT,
    unit_price                DECIMAL(18,2),
    tax_rate                  DECIMAL(18,3),
    picked_quantity           INT,
    picking_completed_when    TIMESTAMP,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/order_lines'
COMMENT 'Raw order lines from Sales.OrderLines';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.invoices (
    invoice_id                INT NOT NULL,
    customer_id               INT NOT NULL,
    bill_to_customer_id       INT,
    order_id                  INT,
    delivery_method_id        INT,
    contact_person_id         INT,
    accounts_person_id        INT,
    salesperson_person_id     INT,
    packed_by_person_id       INT,
    invoice_date              DATE NOT NULL,
    customer_purchase_order_number VARCHAR,
    is_credit_note            BOOLEAN,
    credit_note_reason        VARCHAR,
    comments                  VARCHAR,
    delivery_instructions     VARCHAR,
    internal_comments         VARCHAR,
    total_dry_items           INT,
    total_chiller_items       INT,
    delivery_run              VARCHAR,
    run_position              VARCHAR,
    returned_delivery_data    VARCHAR,
    confirmed_delivery_time   TIMESTAMP,
    confirmed_received_by     VARCHAR,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/invoices'
COMMENT 'Raw invoices from Sales.Invoices';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.invoice_lines (
    invoice_line_id           INT NOT NULL,
    invoice_id                INT NOT NULL,
    stock_item_id             INT NOT NULL,
    description               VARCHAR,
    package_type_id           INT,
    quantity                  INT NOT NULL,
    unit_price                DECIMAL(18,2) NOT NULL,
    tax_rate                  DECIMAL(18,3) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    line_profit               DECIMAL(18,2) NOT NULL,
    extended_price            DECIMAL(18,2) NOT NULL,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/invoice_lines'
COMMENT 'Raw invoice lines from Sales.InvoiceLines';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customer_transactions (
    customer_transaction_id   INT NOT NULL,
    customer_id               INT NOT NULL,
    transaction_type_id       INT NOT NULL,
    invoice_id                INT,
    payment_method_id         INT,
    transaction_date          DATE NOT NULL,
    amount_excluding_tax      DECIMAL(18,2) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    transaction_amount        DECIMAL(18,2) NOT NULL,
    outstanding_balance       DECIMAL(18,2) NOT NULL,
    finalization_date         DATE,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/customer_transactions'
COMMENT 'Raw customer transactions from Sales.CustomerTransactions';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.purchase_orders (
    purchase_order_id         INT NOT NULL,
    supplier_id               INT NOT NULL,
    order_date                DATE NOT NULL,
    delivery_method_id        INT,
    contact_person_id         INT,
    expected_delivery_date    DATE,
    supplier_reference        VARCHAR,
    is_order_finalized        BOOLEAN,
    comments                  VARCHAR,
    internal_comments         VARCHAR,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/purchase_orders'
COMMENT 'Raw purchase orders from Purchasing.PurchaseOrders';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.purchase_order_lines (
    purchase_order_line_id    INT NOT NULL,
    purchase_order_id         INT NOT NULL,
    stock_item_id             INT NOT NULL,
    ordered_outers            INT,
    description               VARCHAR,
    received_outers           INT,
    package_type_id           INT,
    expected_unit_price_per_outer DECIMAL(18,2),
    last_receipt_date         DATE,
    is_order_line_finalized   BOOLEAN,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/purchase_order_lines'
COMMENT 'Raw purchase order lines from Purchasing.PurchaseOrderLines';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.supplier_transactions (
    supplier_transaction_id   INT NOT NULL,
    supplier_id               INT NOT NULL,
    transaction_type_id       INT NOT NULL,
    purchase_order_id         INT,
    payment_method_id         INT,
    transaction_date          DATE NOT NULL,
    amount_excluding_tax      DECIMAL(18,2) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    transaction_amount        DECIMAL(18,2) NOT NULL,
    outstanding_balance       DECIMAL(18,2) NOT NULL,
    finalization_date         DATE,
    last_edited_by            INT,
    last_edited_when          TIMESTAMP NOT NULL
)
LOCATION '$data_path/bronze/supplier_transactions'
COMMENT 'Raw supplier transactions from Purchasing.SupplierTransactions';

-- ===========================================================================
-- INCREMENTAL CONFIGURATION - 7 day overlap on all incremental bronze tables
-- ===========================================================================

SET INCREMENTAL CONFIG ON wwi_lake.bronze.people
    COLUMNS (person_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.customers
    COLUMNS (customer_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.suppliers
    COLUMNS (supplier_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.orders
    COLUMNS (order_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.order_lines
    COLUMNS (order_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.invoices
    COLUMNS (invoice_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.invoice_lines
    COLUMNS (invoice_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.customer_transactions
    COLUMNS (customer_transaction_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.purchase_orders
    COLUMNS (purchase_order_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.purchase_order_lines
    COLUMNS (purchase_order_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

SET INCREMENTAL CONFIG ON wwi_lake.bronze.supplier_transactions
    COLUMNS (supplier_transaction_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

-- ===========================================================================
-- GOLD TABLES - Star Schema (materialized from silver views)
-- ===========================================================================

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_date (
    date_key                  INT NOT NULL,
    full_date                 DATE NOT NULL,
    day_of_week               INT,
    day_name                  VARCHAR,
    day_of_month              INT,
    day_of_year               INT,
    week_of_year              INT,
    month_number              INT,
    month_name                VARCHAR,
    quarter                   INT,
    year                      INT,
    is_weekend                BOOLEAN,
    fiscal_year               INT,
    fiscal_quarter            INT
)
LOCATION '$data_path/gold/dim_date'
COMMENT 'Calendar dimension (YYYYMMDD key), fiscal year starts July';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_customer (
    customer_key              INT NOT NULL,
    customer_name             VARCHAR NOT NULL,
    bill_to_customer_name     VARCHAR,
    category_name             VARCHAR,
    buying_group_name         VARCHAR,
    primary_contact           VARCHAR,
    delivery_method           VARCHAR,
    delivery_city             VARCHAR,
    delivery_state_province   VARCHAR,
    delivery_country          VARCHAR,
    postal_city               VARCHAR,
    credit_limit              DECIMAL(18,2),
    account_opened_date       DATE,
    standard_discount_pct     DECIMAL(18,3),
    is_on_credit_hold         BOOLEAN,
    payment_days              INT,
    phone_number              VARCHAR,
    website_url               VARCHAR
)
LOCATION '$data_path/gold/dim_customer'
COMMENT 'Customer dimension with denormalized geography and category';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_supplier (
    supplier_key              INT NOT NULL,
    supplier_name             VARCHAR NOT NULL,
    category_name             VARCHAR,
    primary_contact           VARCHAR,
    delivery_method           VARCHAR,
    delivery_city             VARCHAR,
    delivery_state_province   VARCHAR,
    delivery_country          VARCHAR,
    supplier_reference        VARCHAR,
    payment_days              INT,
    phone_number              VARCHAR,
    website_url               VARCHAR
)
LOCATION '$data_path/gold/dim_supplier'
COMMENT 'Supplier dimension with denormalized geography and category';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_city (
    city_key                  INT NOT NULL,
    city_name                 VARCHAR NOT NULL,
    state_province_name       VARCHAR,
    state_province_code       VARCHAR,
    sales_territory           VARCHAR,
    country_name              VARCHAR,
    continent                 VARCHAR,
    region                    VARCHAR,
    subregion                 VARCHAR,
    city_population           BIGINT,
    state_population          BIGINT,
    country_population        BIGINT
)
LOCATION '$data_path/gold/dim_city'
COMMENT 'City/geography dimension with full hierarchy';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_employee (
    employee_key              INT NOT NULL,
    full_name                 VARCHAR NOT NULL,
    preferred_name            VARCHAR,
    is_salesperson            BOOLEAN,
    phone_number              VARCHAR,
    email_address             VARCHAR
)
LOCATION '$data_path/gold/dim_employee'
COMMENT 'Employee dimension (people where is_employee = true)';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_delivery_method (
    delivery_method_key       INT NOT NULL,
    delivery_method_name      VARCHAR NOT NULL
)
LOCATION '$data_path/gold/dim_delivery_method'
COMMENT 'Delivery method dimension';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_payment_method (
    payment_method_key        INT NOT NULL,
    payment_method_name       VARCHAR NOT NULL
)
LOCATION '$data_path/gold/dim_payment_method'
COMMENT 'Payment method dimension';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_transaction_type (
    transaction_type_key      INT NOT NULL,
    transaction_type_name     VARCHAR NOT NULL
)
LOCATION '$data_path/gold/dim_transaction_type'
COMMENT 'Transaction type dimension';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_sale (
    invoice_line_id           INT NOT NULL,
    invoice_id                INT NOT NULL,
    order_id                  INT,
    customer_key              INT NOT NULL,
    bill_to_customer_key      INT,
    salesperson_key           INT,
    delivery_method_key       INT,
    delivery_city_key         INT,
    invoice_date_key          INT NOT NULL,
    order_date_key            INT,
    stock_item_id             INT NOT NULL,
    description               VARCHAR,
    quantity                  INT NOT NULL,
    unit_price                DECIMAL(18,2) NOT NULL,
    tax_rate                  DECIMAL(18,3) NOT NULL,
    line_amount               DECIMAL(18,2) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    total_including_tax       DECIMAL(18,2) NOT NULL,
    line_profit               DECIMAL(18,2) NOT NULL,
    profit_margin_pct         DECIMAL(18,4),
    is_credit_note            BOOLEAN,
    is_backorder              BOOLEAN,
    days_to_fulfill           INT,
    total_dry_items           INT,
    total_chiller_items       INT
)
LOCATION '$data_path/gold/fact_sale'
COMMENT 'Sales fact at invoice-line grain with profit margin and fulfillment metrics';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_purchase (
    purchase_order_line_id    INT NOT NULL,
    purchase_order_id         INT NOT NULL,
    supplier_key              INT NOT NULL,
    delivery_method_key       INT,
    order_date_key            INT NOT NULL,
    expected_delivery_date_key INT,
    last_receipt_date_key     INT,
    stock_item_id             INT NOT NULL,
    description               VARCHAR,
    ordered_outers            INT,
    received_outers           INT,
    expected_unit_price_per_outer DECIMAL(18,2),
    is_order_finalized        BOOLEAN,
    is_order_line_finalized   BOOLEAN,
    order_total               DECIMAL(18,2),
    received_total            DECIMAL(18,2),
    fulfillment_pct           DECIMAL(18,4),
    days_to_deliver           INT
)
LOCATION '$data_path/gold/fact_purchase'
COMMENT 'Purchase fact at PO-line grain with fulfillment % and lead times';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_customer_transaction (
    customer_transaction_id   INT NOT NULL,
    customer_key              INT NOT NULL,
    transaction_type_key      INT NOT NULL,
    payment_method_key        INT,
    invoice_id                INT,
    transaction_date_key      INT NOT NULL,
    finalization_date_key     INT,
    amount_excluding_tax      DECIMAL(18,2) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    transaction_amount        DECIMAL(18,2) NOT NULL,
    outstanding_balance       DECIMAL(18,2) NOT NULL,
    is_finalized              BOOLEAN,
    days_to_finalize          INT
)
LOCATION '$data_path/gold/fact_customer_transaction'
COMMENT 'Customer financial transactions with settlement metrics';

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_supplier_transaction (
    supplier_transaction_id   INT NOT NULL,
    supplier_key              INT NOT NULL,
    transaction_type_key      INT NOT NULL,
    payment_method_key        INT,
    purchase_order_id         INT,
    transaction_date_key      INT NOT NULL,
    finalization_date_key     INT,
    amount_excluding_tax      DECIMAL(18,2) NOT NULL,
    tax_amount                DECIMAL(18,2) NOT NULL,
    transaction_amount        DECIMAL(18,2) NOT NULL,
    outstanding_balance       DECIMAL(18,2) NOT NULL,
    is_finalized              BOOLEAN,
    days_to_finalize          INT
)
LOCATION '$data_path/gold/fact_supplier_transaction'
COMMENT 'Supplier financial transactions with settlement metrics';

-- ===========================================================================
-- DATA FLOW: source -> bronze -> silver -> gold
-- ===========================================================================

-- Bronze: ingest from MSSQL source
INCLUDE SCRIPT 'bronze/01_reference.sql';
INCLUDE SCRIPT 'bronze/02_sales.sql';
INCLUDE SCRIPT 'bronze/03_purchasing.sql';

-- Silver: create transformation views over bronze
INCLUDE SCRIPT 'silver/01_geography.sql';
INCLUDE SCRIPT 'silver/02_sales.sql';
INCLUDE SCRIPT 'silver/03_purchasing.sql';

-- Gold: materialize star schema from silver views
INCLUDE SCRIPT 'gold/01_calendar.sql';
INCLUDE SCRIPT 'gold/02_dimensions.sql';
INCLUDE SCRIPT 'gold/03_fact_sale.sql';
INCLUDE SCRIPT 'gold/04_fact_purchase.sql';
INCLUDE SCRIPT 'gold/05_fact_transactions.sql';
