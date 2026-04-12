-- ============================================================================
-- Bronze Sales Tables (Incremental)
-- ============================================================================
-- Incremental ingestion of Sales schema transactional tables.
-- 7-day overlap: re-fetches last 7 days from source, MERGE upserts.
-- First run (empty target): loads everything via epoch fallback.
-- Each table: CREATE IF NOT EXISTS + SET INCREMENTAL CONFIG + MERGE.
-- ============================================================================

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
    SELECT customerid AS customer_id, customername AS customer_name,
        billtocustomerid AS bill_to_customer_id, customercategoryid AS customer_category_id,
        buyinggroupid AS buying_group_id, primarycontactpersonid AS primary_contact_person_id,
        alternatecontactpersonid AS alternate_contact_person_id,
        deliverymethodid AS delivery_method_id, deliverycityid AS delivery_city_id,
        postalcityid AS postal_city_id, creditlimit AS credit_limit,
        accountopeneddate AS account_opened_date,
        standarddiscountpercentage AS standard_discount_percentage,
        isstatementsent AS is_statement_sent, isoncredithold AS is_on_credit_hold,
        paymentdays AS payment_days, phonenumber AS phone_number, faxnumber AS fax_number,
        deliveryrun AS delivery_run, runposition AS run_position, websiteurl AS website_url,
        deliveryaddressline1 AS delivery_address_line1, deliveryaddressline2 AS delivery_address_line2,
        deliverypostalcode AS delivery_postal_code, postaladdressline1 AS postal_address_line1,
        postaladdressline2 AS postal_address_line2, postalpostalcode AS postal_postal_code,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Sales.Customers
    WHERE validfrom >= (
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
    SELECT orderid AS order_id, customerid AS customer_id,
        salespersonpersonid AS salesperson_person_id, pickedbypersonid AS picked_by_person_id,
        contactpersonid AS contact_person_id, backorderorderid AS backorder_order_id,
        orderdate AS order_date, expecteddeliverydate AS expected_delivery_date,
        customerpurchaseordernumber AS customer_purchase_order_number,
        isundersupplybackordered AS is_undersupply_backordered, comments,
        deliveryinstructions AS delivery_instructions, internalcomments AS internal_comments,
        pickingcompletedwhen AS picking_completed_when, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.Orders
    WHERE lasteditedwhen >= (
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
    SELECT orderlineid AS order_line_id, orderid AS order_id, stockitemid AS stock_item_id,
        description, packagetypeid AS package_type_id, quantity, unitprice AS unit_price,
        taxrate AS tax_rate, pickedquantity AS picked_quantity,
        pickingcompletedwhen AS picking_completed_when, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.OrderLines
    WHERE lasteditedwhen >= (
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
    SELECT invoiceid AS invoice_id, customerid AS customer_id,
        billtocustomerid AS bill_to_customer_id, orderid AS order_id,
        deliverymethodid AS delivery_method_id, contactpersonid AS contact_person_id,
        accountspersonid AS accounts_person_id, salespersonpersonid AS salesperson_person_id,
        packedbypersonid AS packed_by_person_id, invoicedate AS invoice_date,
        customerpurchaseordernumber AS customer_purchase_order_number,
        iscreditnote AS is_credit_note, creditnotereason AS credit_note_reason, comments,
        deliveryinstructions AS delivery_instructions, internalcomments AS internal_comments,
        totaldryitems AS total_dry_items, totalchilleritems AS total_chiller_items,
        deliveryrun AS delivery_run, runposition AS run_position,
        returneddeliverydata AS returned_delivery_data,
        confirmeddeliverytime AS confirmed_delivery_time,
        confirmedreceivedby AS confirmed_received_by, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.Invoices
    WHERE lasteditedwhen >= (
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
    SELECT invoicelineid AS invoice_line_id, invoiceid AS invoice_id,
        stockitemid AS stock_item_id, description, packagetypeid AS package_type_id,
        quantity, unitprice AS unit_price, taxrate AS tax_rate, taxamount AS tax_amount,
        lineprofit AS line_profit, extendedprice AS extended_price,
        lasteditedby AS last_edited_by, lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.InvoiceLines
    WHERE lasteditedwhen >= (
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
    SELECT customertransactionid AS customer_transaction_id, customerid AS customer_id,
        transactiontypeid AS transaction_type_id, invoiceid AS invoice_id,
        paymentmethodid AS payment_method_id, transactiondate AS transaction_date,
        amountexcludingtax AS amount_excluding_tax, taxamount AS tax_amount,
        transactionamount AS transaction_amount, outstandingbalance AS outstanding_balance,
        finalizationdate AS finalization_date, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.CustomerTransactions
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.customer_transactions
    )
) AS src ON tgt.customer_transaction_id = src.customer_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
