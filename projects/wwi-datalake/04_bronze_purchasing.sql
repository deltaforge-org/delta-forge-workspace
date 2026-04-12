-- ============================================================================
-- Bronze Purchasing & People (Incremental)
-- ============================================================================
-- Incremental ingestion of Purchasing schema + Application.People.
-- 7-day overlap on watermark column, MERGE upsert for deduplication.
-- Each table: CREATE IF NOT EXISTS + SET INCREMENTAL CONFIG + MERGE.
-- ============================================================================

PIPELINE wwi_lake.bronze_purchasing
    DESCRIPTION 'WWI bronze - incremental purchasing & people from MSSQL'
    TAGS 'wwi', 'medallion', 'mssql', 'bronze', 'purchasing'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- People

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.people (
    person_id INT NOT NULL, full_name VARCHAR NOT NULL, preferred_name VARCHAR,
    is_permitted_to_logon BOOLEAN, logon_name VARCHAR, is_external_logon_provider BOOLEAN,
    is_system_user BOOLEAN, is_employee BOOLEAN, is_salesperson BOOLEAN,
    user_preferences VARCHAR, phone_number VARCHAR, fax_number VARCHAR,
    email_address VARCHAR, custom_fields VARCHAR, last_edited_by INT,
    valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/people';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.people
    COLUMNS (person_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.people AS tgt
USING (
    SELECT personid AS person_id, fullname AS full_name, preferredname AS preferred_name,
        ispermittedtologon AS is_permitted_to_logon, logonname AS logon_name,
        isexternallogonprovider AS is_external_logon_provider, issystemuser AS is_system_user,
        isemployee AS is_employee, issalesperson AS is_salesperson,
        userpreferences AS user_preferences, phonenumber AS phone_number,
        faxnumber AS fax_number, emailaddress AS email_address,
        customfields AS custom_fields, lasteditedby AS last_edited_by,
        validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.People
    WHERE validfrom >= (
        SELECT COALESCE(MAX(valid_from) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.people
    )
) AS src ON tgt.person_id = src.person_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Suppliers

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.suppliers (
    supplier_id INT NOT NULL, supplier_name VARCHAR NOT NULL, supplier_category_id INT,
    primary_contact_person_id INT, alternate_contact_person_id INT, delivery_method_id INT,
    delivery_city_id INT, postal_city_id INT, supplier_reference VARCHAR,
    bank_account_name VARCHAR, bank_account_branch VARCHAR, bank_account_code VARCHAR,
    bank_account_number VARCHAR, bank_international_code VARCHAR, payment_days INT,
    internal_comments VARCHAR, phone_number VARCHAR, fax_number VARCHAR, website_url VARCHAR,
    delivery_address_line1 VARCHAR, delivery_address_line2 VARCHAR,
    delivery_postal_code VARCHAR, postal_address_line1 VARCHAR,
    postal_address_line2 VARCHAR, postal_postal_code VARCHAR,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/suppliers';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.suppliers
    COLUMNS (supplier_id) DATECOL valid_from OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.suppliers AS tgt
USING (
    SELECT supplierid AS supplier_id, suppliername AS supplier_name,
        suppliercategoryid AS supplier_category_id,
        primarycontactpersonid AS primary_contact_person_id,
        alternatecontactpersonid AS alternate_contact_person_id,
        deliverymethodid AS delivery_method_id, deliverycityid AS delivery_city_id,
        postalcityid AS postal_city_id, supplierreference AS supplier_reference,
        bankaccountname AS bank_account_name, bankaccountbranch AS bank_account_branch,
        bankaccountcode AS bank_account_code, bankaccountnumber AS bank_account_number,
        bankinternationalcode AS bank_international_code, paymentdays AS payment_days,
        internalcomments AS internal_comments, phonenumber AS phone_number,
        faxnumber AS fax_number, websiteurl AS website_url,
        deliveryaddressline1 AS delivery_address_line1,
        deliveryaddressline2 AS delivery_address_line2,
        deliverypostalcode AS delivery_postal_code,
        postaladdressline1 AS postal_address_line1,
        postaladdressline2 AS postal_address_line2,
        postalpostalcode AS postal_postal_code,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Purchasing.Suppliers
    WHERE validfrom >= (
        SELECT COALESCE(MAX(valid_from) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.suppliers
    )
) AS src ON tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Purchase Orders

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.purchase_orders (
    purchase_order_id INT NOT NULL, supplier_id INT NOT NULL, order_date DATE NOT NULL,
    delivery_method_id INT, contact_person_id INT, expected_delivery_date DATE,
    supplier_reference VARCHAR, is_order_finalized BOOLEAN, comments VARCHAR,
    internal_comments VARCHAR, last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/purchase_orders';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.purchase_orders
    COLUMNS (purchase_order_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.purchase_orders AS tgt
USING (
    SELECT purchaseorderid AS purchase_order_id, supplierid AS supplier_id,
        orderdate AS order_date, deliverymethodid AS delivery_method_id,
        contactpersonid AS contact_person_id, expecteddeliverydate AS expected_delivery_date,
        supplierreference AS supplier_reference, isorderfinalized AS is_order_finalized,
        comments, internalcomments AS internal_comments, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.PurchaseOrders
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.purchase_orders
    )
) AS src ON tgt.purchase_order_id = src.purchase_order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Purchase Order Lines

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.purchase_order_lines (
    purchase_order_line_id INT NOT NULL, purchase_order_id INT NOT NULL,
    stock_item_id INT NOT NULL, ordered_outers INT, description VARCHAR,
    received_outers INT, package_type_id INT, expected_unit_price_per_outer DECIMAL(18,2),
    last_receipt_date DATE, is_order_line_finalized BOOLEAN,
    last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/purchase_order_lines';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.purchase_order_lines
    COLUMNS (purchase_order_line_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.purchase_order_lines AS tgt
USING (
    SELECT purchaseorderlineid AS purchase_order_line_id, purchaseorderid AS purchase_order_id,
        stockitemid AS stock_item_id, orderedouters AS ordered_outers, description,
        receivedouters AS received_outers, packagetypeid AS package_type_id,
        expectedunitpriceperouter AS expected_unit_price_per_outer,
        lastreceiptdate AS last_receipt_date, isorderlinefinalized AS is_order_line_finalized,
        lasteditedby AS last_edited_by, lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.PurchaseOrderLines
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.purchase_order_lines
    )
) AS src ON tgt.purchase_order_line_id = src.purchase_order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Supplier Transactions

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.supplier_transactions (
    supplier_transaction_id INT NOT NULL, supplier_id INT NOT NULL,
    transaction_type_id INT NOT NULL, purchase_order_id INT, payment_method_id INT,
    transaction_date DATE NOT NULL, amount_excluding_tax DECIMAL(18,2) NOT NULL,
    tax_amount DECIMAL(18,2) NOT NULL, transaction_amount DECIMAL(18,2) NOT NULL,
    outstanding_balance DECIMAL(18,2) NOT NULL, finalization_date DATE,
    last_edited_by INT, last_edited_when TIMESTAMP NOT NULL
) LOCATION 'wwi/bronze/supplier_transactions';

SET INCREMENTAL CONFIG ON wwi_lake.bronze.supplier_transactions
    COLUMNS (supplier_transaction_id) DATECOL last_edited_when OVERLAP 7 DAYS DIALECT MSSQL;

MERGE INTO wwi_lake.bronze.supplier_transactions AS tgt
USING (
    SELECT suppliertransactionid AS supplier_transaction_id, supplierid AS supplier_id,
        transactiontypeid AS transaction_type_id, purchaseorderid AS purchase_order_id,
        paymentmethodid AS payment_method_id, transactiondate AS transaction_date,
        amountexcludingtax AS amount_excluding_tax, taxamount AS tax_amount,
        transactionamount AS transaction_amount, outstandingbalance AS outstanding_balance,
        finalizationdate AS finalization_date, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.SupplierTransactions
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.supplier_transactions
    )
) AS src ON tgt.supplier_transaction_id = src.supplier_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
