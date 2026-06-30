-- ============================================================================
-- Bronze Sales Tables (Incremental)
-- ============================================================================
-- Incremental ingestion of Sales schema transactional tables.
-- 7-day overlap: re-fetches last 7 days from source, MERGE upserts.
-- First run (empty target): loads everything via epoch fallback.
-- Each table: CREATE IF NOT EXISTS + SET INCREMENTAL CONFIG + MERGE.
-- ============================================================================

PIPELINE wwi_lake.bronze_sales
    DESCRIPTION 'WWI bronze - incremental sales ingestion from MSSQL'
    SCHEDULE 'wwi_lake_daily'
    TAGS 'wwi', 'medallion', 'mssql', 'bronze', 'sales'
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- Customers

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customers (
    customer_id INT NOT NULL, customer_name VARCHAR NOT NULL, bill_to_customer_id INT,
    customer_category_id INT, buying_group_id INT, primary_contact_person_id INT,
    alternate_contact_person_id INT, delivery_method_id INT, delivery_city_id INT,
    postal_city_id INT, credit_limit DECIMAL(18,2), account_opened_date DATE,
    standard_discount_percentage DECIMAL(18,3), is_statement_sent BOOLEAN,
    is_on_credit_hold BOOLEAN, payment_days INT, phone_number VARCHAR, fax_number VARCHAR,
    delivery_run VARCHAR, run_position VARCHAR, website_url VARCHAR,
    delivery_address_line1 VARCHAR, delivery_address_line2 VARCHAR,
    delivery_postal_code VARCHAR, postal_address_line1 VARCHAR,
    postal_address_line2 VARCHAR, postal_postal_code VARCHAR,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/customers';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.customers
    COLUMNS (customer_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.customers AS tgt
USING (
    SELECT customer_id, customer_name,
        bill_to_customer_id, customer_category_id,
        buying_group_id, primary_contact_person_id,
        alternate_contact_person_id,
        delivery_method_id, delivery_city_id,
        postal_city_id, credit_limit,
        account_opened_date,
        standard_discount_percentage,
        is_statement_sent, is_on_credit_hold,
        payment_days, phone_number, fax_number,
        delivery_run, run_position, website_url,
        delivery_address_line1, delivery_address_line2,
        delivery_postal_code, postal_address_line1,
        postal_address_line2, postal_postal_code,
        last_edited_by, valid_from, valid_to
    FROM mssql_WideWorldImporters.sales.customers
    WHERE valid_from >= (
        SELECT COALESCE(MAX(valid_from) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.customers
    )
) AS src ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Orders

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.orders (
    order_id INT NOT NULL, customer_id INT NOT NULL, salesperson_person_id INT,
    picked_by_person_id INT, contact_person_id INT, backorder_order_id INT,
    order_date DATE NOT NULL, expected_delivery_date DATE,
    customer_purchase_order_number VARCHAR, is_undersupply_backordered BOOLEAN,
    comments VARCHAR, delivery_instructions VARCHAR, internal_comments VARCHAR,
    picking_completed_when TIMESTAMP, last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/orders';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.orders
    COLUMNS (order_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.orders AS tgt
USING (
    SELECT order_id, customer_id,
        salesperson_person_id, picked_by_person_id,
        contact_person_id, backorder_order_id,
        order_date, expected_delivery_date,
        customer_purchase_order_number,
        is_undersupply_backordered, comments,
        delivery_instructions, internal_comments,
        picking_completed_when, last_edited_by,
        last_edited_when
    FROM mssql_WideWorldImporters.sales.orders
    WHERE last_edited_when >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.orders
    )
) AS src ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Order Lines

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.order_lines (
    order_line_id INT NOT NULL, order_id INT NOT NULL, stock_item_id INT NOT NULL,
    description VARCHAR, package_type_id INT, quantity INT, unit_price DECIMAL(18,2),
    tax_rate DECIMAL(18,3), picked_quantity INT, picking_completed_when TIMESTAMP,
    last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/order_lines';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.order_lines
    COLUMNS (order_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.order_lines AS tgt
USING (
    SELECT order_line_id, order_id, stock_item_id,
        description, package_type_id, quantity, unit_price,
        tax_rate, picked_quantity,
        picking_completed_when, last_edited_by,
        last_edited_when
    FROM mssql_WideWorldImporters.sales.order_lines
    WHERE last_edited_when >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.order_lines
    )
) AS src ON tgt.order_line_id = src.order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Invoices

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.invoices (
    invoice_id INT NOT NULL, customer_id INT NOT NULL, bill_to_customer_id INT,
    order_id INT, delivery_method_id INT, contact_person_id INT, accounts_person_id INT,
    salesperson_person_id INT, packed_by_person_id INT, invoice_date DATE NOT NULL,
    customer_purchase_order_number VARCHAR, is_credit_note BOOLEAN, credit_note_reason VARCHAR,
    comments VARCHAR, delivery_instructions VARCHAR, internal_comments VARCHAR,
    total_dry_items INT, total_chiller_items INT, delivery_run VARCHAR, run_position VARCHAR,
    returned_delivery_data VARCHAR, confirmed_delivery_time TIMESTAMP,
    confirmed_received_by VARCHAR, last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/invoices';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.invoices
    COLUMNS (invoice_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.invoices AS tgt
USING (
    SELECT invoice_id, customer_id,
        bill_to_customer_id, order_id,
        delivery_method_id, contact_person_id,
        accounts_person_id, salesperson_person_id,
        packed_by_person_id, invoice_date,
        customer_purchase_order_number,
        is_credit_note, credit_note_reason, comments,
        delivery_instructions, internal_comments,
        total_dry_items, total_chiller_items,
        delivery_run, run_position,
        returned_delivery_data,
        confirmed_delivery_time,
        confirmed_received_by, last_edited_by,
        last_edited_when
    FROM mssql_WideWorldImporters.sales.invoices
    WHERE last_edited_when >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.invoices
    )
) AS src ON tgt.invoice_id = src.invoice_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Invoice Lines

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.invoice_lines (
    invoice_line_id INT NOT NULL, invoice_id INT NOT NULL, stock_item_id INT NOT NULL,
    description VARCHAR, package_type_id INT, quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL, tax_rate DECIMAL(18,3) NOT NULL,
    tax_amount DECIMAL(18,2) NOT NULL, line_profit DECIMAL(18,2) NOT NULL,
    extended_price DECIMAL(18,2) NOT NULL, last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/invoice_lines';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.invoice_lines
    COLUMNS (invoice_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.invoice_lines AS tgt
USING (
    SELECT invoice_line_id, invoice_id,
        stock_item_id, description, package_type_id,
        quantity, unit_price, tax_rate, tax_amount,
        line_profit, extended_price,
        last_edited_by, last_edited_when
    FROM mssql_WideWorldImporters.sales.invoice_lines
    WHERE last_edited_when >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.invoice_lines
    )
) AS src ON tgt.invoice_line_id = src.invoice_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Customer Transactions

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customer_transactions (
    customer_transaction_id INT NOT NULL, customer_id INT NOT NULL,
    transaction_type_id INT NOT NULL, invoice_id INT, payment_method_id INT,
    transaction_date DATE NOT NULL, amount_excluding_tax DECIMAL(18,2) NOT NULL,
    tax_amount DECIMAL(18,2) NOT NULL, transaction_amount DECIMAL(18,2) NOT NULL,
    outstanding_balance DECIMAL(18,2) NOT NULL, finalization_date DATE,
    last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/customer_transactions';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.customer_transactions
    COLUMNS (customer_transaction_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.customer_transactions AS tgt
USING (
    SELECT customer_transaction_id, customer_id,
        transaction_type_id, invoice_id,
        payment_method_id, transaction_date,
        amount_excluding_tax, tax_amount,
        transaction_amount, outstanding_balance,
        finalization_date, last_edited_by,
        last_edited_when
    FROM mssql_WideWorldImporters.sales.customer_transactions
    WHERE last_edited_when >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.customer_transactions
    )
) AS src ON tgt.customer_transaction_id = src.customer_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
