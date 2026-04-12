-- ============================================================================
-- WWI Data Lake - Bronze Purchasing & People (Incremental)
-- ============================================================================
-- Incremental ingestion of Purchasing schema + Application.People.
-- 7-day overlap on the watermark column, MERGE upsert for deduplication.
-- ============================================================================

PIPELINE wwi_lake.bronze_purchasing
    DESCRIPTION 'Incremental load of Purchasing tables and People from MSSQL'
    SCHEDULE '0 3 * * *'
    TIMEZONE 'UTC'
    TAGS 'wwi', 'bronze', 'incremental', 'purchasing'
    SLA 1.0
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- People (system-versioned, watermark on valid_from)

MERGE INTO wwi_lake.bronze.people AS tgt
USING (
    SELECT
        personid                AS person_id,
        fullname                AS full_name,
        preferredname           AS preferred_name,
        ispermittedtologon      AS is_permitted_to_logon,
        logonname               AS logon_name,
        isexternallogonprovider AS is_external_logon_provider,
        issystemuser            AS is_system_user,
        isemployee              AS is_employee,
        issalesperson           AS is_salesperson,
        userpreferences         AS user_preferences,
        phonenumber             AS phone_number,
        faxnumber               AS fax_number,
        emailaddress            AS email_address,
        customfields            AS custom_fields,
        lasteditedby            AS last_edited_by,
        validfrom               AS valid_from,
        validto                 AS valid_to
    FROM mssql_WideWorldImporters.Application.People
    WHERE validfrom >= (
        SELECT COALESCE(MAX(valid_from) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.people
    )
) AS src ON tgt.person_id = src.person_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Suppliers (system-versioned, watermark on valid_from)

MERGE INTO wwi_lake.bronze.suppliers AS tgt
USING (
    SELECT
        supplierid               AS supplier_id,
        suppliername             AS supplier_name,
        suppliercategoryid       AS supplier_category_id,
        primarycontactpersonid   AS primary_contact_person_id,
        alternatecontactpersonid AS alternate_contact_person_id,
        deliverymethodid         AS delivery_method_id,
        deliverycityid           AS delivery_city_id,
        postalcityid             AS postal_city_id,
        supplierreference        AS supplier_reference,
        bankaccountname          AS bank_account_name,
        bankaccountbranch        AS bank_account_branch,
        bankaccountcode          AS bank_account_code,
        bankaccountnumber        AS bank_account_number,
        bankinternationalcode    AS bank_international_code,
        paymentdays              AS payment_days,
        internalcomments         AS internal_comments,
        phonenumber              AS phone_number,
        faxnumber                AS fax_number,
        websiteurl               AS website_url,
        deliveryaddressline1     AS delivery_address_line1,
        deliveryaddressline2     AS delivery_address_line2,
        deliverypostalcode       AS delivery_postal_code,
        postaladdressline1       AS postal_address_line1,
        postaladdressline2       AS postal_address_line2,
        postalpostalcode         AS postal_postal_code,
        lasteditedby             AS last_edited_by,
        validfrom                AS valid_from,
        validto                  AS valid_to
    FROM mssql_WideWorldImporters.Purchasing.Suppliers
    WHERE validfrom >= (
        SELECT COALESCE(MAX(valid_from) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.suppliers
    )
) AS src ON tgt.supplier_id = src.supplier_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Purchase Orders (watermark on last_edited_when)

MERGE INTO wwi_lake.bronze.purchase_orders AS tgt
USING (
    SELECT
        purchaseorderid    AS purchase_order_id,
        supplierid         AS supplier_id,
        orderdate          AS order_date,
        deliverymethodid   AS delivery_method_id,
        contactpersonid    AS contact_person_id,
        expecteddeliverydate AS expected_delivery_date,
        supplierreference  AS supplier_reference,
        isorderfinalized   AS is_order_finalized,
        comments,
        internalcomments   AS internal_comments,
        lasteditedby       AS last_edited_by,
        lasteditedwhen     AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.PurchaseOrders
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.purchase_orders
    )
) AS src ON tgt.purchase_order_id = src.purchase_order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Purchase Order Lines (watermark on last_edited_when)

MERGE INTO wwi_lake.bronze.purchase_order_lines AS tgt
USING (
    SELECT
        purchaseorderlineid       AS purchase_order_line_id,
        purchaseorderid           AS purchase_order_id,
        stockitemid               AS stock_item_id,
        orderedouters             AS ordered_outers,
        description,
        receivedouters            AS received_outers,
        packagetypeid             AS package_type_id,
        expectedunitpriceperouter AS expected_unit_price_per_outer,
        lastreceiptdate           AS last_receipt_date,
        isorderlinefinalized      AS is_order_line_finalized,
        lasteditedby              AS last_edited_by,
        lasteditedwhen            AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.PurchaseOrderLines
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.purchase_order_lines
    )
) AS src ON tgt.purchase_order_line_id = src.purchase_order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Supplier Transactions (watermark on last_edited_when)

MERGE INTO wwi_lake.bronze.supplier_transactions AS tgt
USING (
    SELECT
        suppliertransactionid AS supplier_transaction_id,
        supplierid            AS supplier_id,
        transactiontypeid     AS transaction_type_id,
        purchaseorderid       AS purchase_order_id,
        paymentmethodid       AS payment_method_id,
        transactiondate       AS transaction_date,
        amountexcludingtax    AS amount_excluding_tax,
        taxamount             AS tax_amount,
        transactionamount     AS transaction_amount,
        outstandingbalance    AS outstanding_balance,
        finalizationdate      AS finalization_date,
        lasteditedby          AS last_edited_by,
        lasteditedwhen        AS last_edited_when
    FROM mssql_WideWorldImporters.Purchasing.SupplierTransactions
    WHERE lasteditedwhen >= (
        SELECT COALESCE(MAX(last_edited_when) - INTERVAL '7' DAY, TIMESTAMP '1900-01-01 00:00:00')
        FROM wwi_lake.bronze.supplier_transactions
    )
) AS src ON tgt.supplier_transaction_id = src.supplier_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
