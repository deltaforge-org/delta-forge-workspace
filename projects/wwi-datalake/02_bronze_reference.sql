-- ============================================================================
-- Bronze Reference Tables (Full Load)
-- ============================================================================
-- Small, slowly-changing reference/dimension tables fully synced each run.
-- Each table: CREATE IF NOT EXISTS + MERGE with NOT MATCHED BY SOURCE.
-- ============================================================================

PIPELINE wwi_lake.bronze_reference
    DESCRIPTION 'WWI bronze - full-load reference tables from MSSQL'
    TAGS 'wwi', 'medallion', 'mssql', 'bronze', 'reference'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- Countries

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.countries (
    country_id INT NOT NULL, country_name VARCHAR NOT NULL, formal_name VARCHAR,
    iso_alpha3_code VARCHAR, iso_numeric_code INT, country_type VARCHAR,
    latest_recorded_population BIGINT, continent VARCHAR, region VARCHAR,
    subregion VARCHAR, last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/countries';

MERGE INTO wwi_lake.bronze.countries AS tgt
USING (
    SELECT countryid AS country_id, countryname AS country_name, formalname AS formal_name,
        isoalpha3code AS iso_alpha3_code, isonumericcode AS iso_numeric_code,
        countrytype AS country_type, latestrecordedpopulation AS latest_recorded_population,
        continent, region, subregion, lasteditedby AS last_edited_by,
        validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.Countries
) AS src ON tgt.country_id = src.country_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- State Provinces

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.state_provinces (
    state_province_id INT NOT NULL, state_province_code VARCHAR,
    state_province_name VARCHAR NOT NULL, country_id INT NOT NULL,
    sales_territory VARCHAR, latest_recorded_population BIGINT,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/state_provinces';

MERGE INTO wwi_lake.bronze.state_provinces AS tgt
USING (
    SELECT stateprovinceid AS state_province_id, stateprovincecode AS state_province_code,
        stateprovincename AS state_province_name, countryid AS country_id,
        salesterritory AS sales_territory, latestrecordedpopulation AS latest_recorded_population,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.StateProvinces
) AS src ON tgt.state_province_id = src.state_province_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Cities

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.cities (
    city_id INT NOT NULL, city_name VARCHAR NOT NULL, state_province_id INT NOT NULL,
    latest_recorded_population BIGINT, last_edited_by INT,
    valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/cities';

MERGE INTO wwi_lake.bronze.cities AS tgt
USING (
    SELECT cityid AS city_id, cityname AS city_name, stateprovinceid AS state_province_id,
        latestrecordedpopulation AS latest_recorded_population,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.Cities
) AS src ON tgt.city_id = src.city_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Delivery Methods

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.delivery_methods (
    delivery_method_id INT NOT NULL, delivery_method_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/delivery_methods';

MERGE INTO wwi_lake.bronze.delivery_methods AS tgt
USING (
    SELECT deliverymethodid AS delivery_method_id, deliverymethodname AS delivery_method_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.DeliveryMethods
) AS src ON tgt.delivery_method_id = src.delivery_method_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Payment Methods

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.payment_methods (
    payment_method_id INT NOT NULL, payment_method_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/payment_methods';

MERGE INTO wwi_lake.bronze.payment_methods AS tgt
USING (
    SELECT paymentmethodid AS payment_method_id, paymentmethodname AS payment_method_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.PaymentMethods
) AS src ON tgt.payment_method_id = src.payment_method_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Transaction Types

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.transaction_types (
    transaction_type_id INT NOT NULL, transaction_type_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/transaction_types';

MERGE INTO wwi_lake.bronze.transaction_types AS tgt
USING (
    SELECT transactiontypeid AS transaction_type_id, transactiontypename AS transaction_type_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Application.TransactionTypes
) AS src ON tgt.transaction_type_id = src.transaction_type_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Supplier Categories

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.supplier_categories (
    supplier_category_id INT NOT NULL, supplier_category_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/supplier_categories';

MERGE INTO wwi_lake.bronze.supplier_categories AS tgt
USING (
    SELECT suppliercategoryid AS supplier_category_id, suppliercategoryname AS supplier_category_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Purchasing.SupplierCategories
) AS src ON tgt.supplier_category_id = src.supplier_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Buying Groups

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.buying_groups (
    buying_group_id INT NOT NULL, buying_group_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/buying_groups';

MERGE INTO wwi_lake.bronze.buying_groups AS tgt
USING (
    SELECT buyinggroupid AS buying_group_id, buyinggroupname AS buying_group_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Sales.BuyingGroups
) AS src ON tgt.buying_group_id = src.buying_group_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Customer Categories

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.customer_categories (
    customer_category_id INT NOT NULL, customer_category_name VARCHAR NOT NULL,
    last_edited_by INT, valid_from TIMESTAMP, valid_to TIMESTAMP
) LOCATION 'wwi/bronze/customer_categories';

MERGE INTO wwi_lake.bronze.customer_categories AS tgt
USING (
    SELECT customercategoryid AS customer_category_id, customercategoryname AS customer_category_name,
        lasteditedby AS last_edited_by, validfrom AS valid_from, validto AS valid_to
    FROM mssql_WideWorldImporters.Sales.CustomerCategories
) AS src ON tgt.customer_category_id = src.customer_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- System Parameters

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.system_parameters (
    system_parameter_id INT NOT NULL, delivery_address_line1 VARCHAR,
    delivery_address_line2 VARCHAR, delivery_city_id INT, delivery_postal_code VARCHAR,
    postal_address_line1 VARCHAR, postal_address_line2 VARCHAR, postal_city_id INT,
    postal_postal_code VARCHAR, application_settings VARCHAR,
    last_edited_by INT, last_edited_when TIMESTAMP
) LOCATION 'wwi/bronze/system_parameters';

MERGE INTO wwi_lake.bronze.system_parameters AS tgt
USING (
    SELECT systemparameterid AS system_parameter_id, deliveryaddressline1 AS delivery_address_line1,
        deliveryaddressline2 AS delivery_address_line2, deliverycityid AS delivery_city_id,
        deliverypostalcode AS delivery_postal_code, postaladdressline1 AS postal_address_line1,
        postaladdressline2 AS postal_address_line2, postalcityid AS postal_city_id,
        postalpostalcode AS postal_postal_code, applicationsettings AS application_settings,
        lasteditedby AS last_edited_by, lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Application.SystemParameters
) AS src ON tgt.system_parameter_id = src.system_parameter_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Special Deals

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.bronze.special_deals (
    special_deal_id INT NOT NULL, stock_item_id INT, customer_id INT,
    buying_group_id INT, customer_category_id INT, stock_group_id INT,
    deal_description VARCHAR, start_date DATE, end_date DATE,
    discount_amount DECIMAL(18,2), discount_percentage DECIMAL(18,3),
    unit_price DECIMAL(18,2), last_edited_by INT, last_edited_when TIMESTAMP
) LOCATION 'wwi/bronze/special_deals';

MERGE INTO wwi_lake.bronze.special_deals AS tgt
USING (
    SELECT specialdealid AS special_deal_id, stockitemid AS stock_item_id,
        customerid AS customer_id, buyinggroupid AS buying_group_id,
        customercategoryid AS customer_category_id, stockgroupid AS stock_group_id,
        dealdescription AS deal_description, startdate AS start_date, enddate AS end_date,
        discountamount AS discount_amount, discountpercentage AS discount_percentage,
        unitprice AS unit_price, lasteditedby AS last_edited_by,
        lasteditedwhen AS last_edited_when
    FROM mssql_WideWorldImporters.Sales.SpecialDeals
) AS src ON tgt.special_deal_id = src.special_deal_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
