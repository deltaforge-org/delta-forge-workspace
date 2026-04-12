-- ============================================================================
-- WWI Data Lake - Bronze Reference Tables (Full Load)
-- ============================================================================
-- Small, slowly-changing reference/dimension tables fully synced each run.
-- MERGE with NOT MATCHED BY SOURCE detects source-side deletions.
-- ============================================================================

-- Countries

MERGE INTO wwi_lake.bronze.countries AS tgt
USING (
    SELECT
        countryid                AS country_id,
        countryname              AS country_name,
        formalname               AS formal_name,
        isoalpha3code            AS iso_alpha3_code,
        isonumericcode           AS iso_numeric_code,
        countrytype              AS country_type,
        latestrecordedpopulation AS latest_recorded_population,
        continent, region, subregion,
        lasteditedby             AS last_edited_by,
        validfrom                AS valid_from,
        validto                  AS valid_to
    FROM mssql_WideWorldImporters.Application.Countries
) AS src ON tgt.country_id = src.country_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- State Provinces

MERGE INTO wwi_lake.bronze.state_provinces AS tgt
USING (
    SELECT
        stateprovinceid          AS state_province_id,
        stateprovincecode        AS state_province_code,
        stateprovincename        AS state_province_name,
        countryid                AS country_id,
        salesterritory           AS sales_territory,
        latestrecordedpopulation AS latest_recorded_population,
        lasteditedby             AS last_edited_by,
        validfrom                AS valid_from,
        validto                  AS valid_to
    FROM mssql_WideWorldImporters.Application.StateProvinces
) AS src ON tgt.state_province_id = src.state_province_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Cities

MERGE INTO wwi_lake.bronze.cities AS tgt
USING (
    SELECT
        cityid                   AS city_id,
        cityname                 AS city_name,
        stateprovinceid          AS state_province_id,
        latestrecordedpopulation AS latest_recorded_population,
        lasteditedby             AS last_edited_by,
        validfrom                AS valid_from,
        validto                  AS valid_to
    FROM mssql_WideWorldImporters.Application.Cities
) AS src ON tgt.city_id = src.city_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Delivery Methods

MERGE INTO wwi_lake.bronze.delivery_methods AS tgt
USING (
    SELECT
        deliverymethodid   AS delivery_method_id,
        deliverymethodname AS delivery_method_name,
        lasteditedby       AS last_edited_by,
        validfrom          AS valid_from,
        validto            AS valid_to
    FROM mssql_WideWorldImporters.Application.DeliveryMethods
) AS src ON tgt.delivery_method_id = src.delivery_method_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Payment Methods

MERGE INTO wwi_lake.bronze.payment_methods AS tgt
USING (
    SELECT
        paymentmethodid   AS payment_method_id,
        paymentmethodname AS payment_method_name,
        lasteditedby      AS last_edited_by,
        validfrom         AS valid_from,
        validto           AS valid_to
    FROM mssql_WideWorldImporters.Application.PaymentMethods
) AS src ON tgt.payment_method_id = src.payment_method_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Transaction Types

MERGE INTO wwi_lake.bronze.transaction_types AS tgt
USING (
    SELECT
        transactiontypeid   AS transaction_type_id,
        transactiontypename AS transaction_type_name,
        lasteditedby        AS last_edited_by,
        validfrom           AS valid_from,
        validto             AS valid_to
    FROM mssql_WideWorldImporters.Application.TransactionTypes
) AS src ON tgt.transaction_type_id = src.transaction_type_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Supplier Categories

MERGE INTO wwi_lake.bronze.supplier_categories AS tgt
USING (
    SELECT
        suppliercategoryid   AS supplier_category_id,
        suppliercategoryname AS supplier_category_name,
        lasteditedby         AS last_edited_by,
        validfrom            AS valid_from,
        validto              AS valid_to
    FROM mssql_WideWorldImporters.Purchasing.SupplierCategories
) AS src ON tgt.supplier_category_id = src.supplier_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Buying Groups

MERGE INTO wwi_lake.bronze.buying_groups AS tgt
USING (
    SELECT
        buyinggroupid   AS buying_group_id,
        buyinggroupname AS buying_group_name,
        lasteditedby    AS last_edited_by,
        validfrom       AS valid_from,
        validto         AS valid_to
    FROM mssql_WideWorldImporters.Sales.BuyingGroups
) AS src ON tgt.buying_group_id = src.buying_group_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Customer Categories

MERGE INTO wwi_lake.bronze.customer_categories AS tgt
USING (
    SELECT
        customercategoryid   AS customer_category_id,
        customercategoryname AS customer_category_name,
        lasteditedby         AS last_edited_by,
        validfrom            AS valid_from,
        validto              AS valid_to
    FROM mssql_WideWorldImporters.Sales.CustomerCategories
) AS src ON tgt.customer_category_id = src.customer_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- System Parameters

MERGE INTO wwi_lake.bronze.system_parameters AS tgt
USING (
    SELECT
        systemparameterid    AS system_parameter_id,
        deliveryaddressline1 AS delivery_address_line1,
        deliveryaddressline2 AS delivery_address_line2,
        deliverycityid       AS delivery_city_id,
        deliverypostalcode   AS delivery_postal_code,
        postaladdressline1   AS postal_address_line1,
        postaladdressline2   AS postal_address_line2,
        postalcityid         AS postal_city_id,
        postalpostalcode     AS postal_postal_code,
        applicationsettings  AS application_settings,
        lasteditedby         AS last_edited_by,
        lasteditedwhen       AS last_edited_when
    FROM mssql_WideWorldImporters.Application.SystemParameters
) AS src ON tgt.system_parameter_id = src.system_parameter_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Special Deals

MERGE INTO wwi_lake.bronze.special_deals AS tgt
USING (
    SELECT
        specialdealid      AS special_deal_id,
        stockitemid        AS stock_item_id,
        customerid         AS customer_id,
        buyinggroupid      AS buying_group_id,
        customercategoryid AS customer_category_id,
        stockgroupid       AS stock_group_id,
        dealdescription    AS deal_description,
        startdate          AS start_date,
        enddate            AS end_date,
        discountamount     AS discount_amount,
        discountpercentage AS discount_percentage,
        unitprice          AS unit_price,
        lasteditedby       AS last_edited_by,
        lasteditedwhen     AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.SpecialDeals
) AS src ON tgt.special_deal_id = src.special_deal_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
