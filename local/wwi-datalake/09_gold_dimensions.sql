-- ============================================================================
-- Gold Dimensions
-- ============================================================================
-- Star schema dimensions from silver views. Natural keys from source.
-- Each: CREATE IF NOT EXISTS + MERGE with NOT MATCHED BY SOURCE.
-- ============================================================================

PIPELINE wwi_lake.gold_dimensions
    DESCRIPTION 'WWI gold - star schema dimensions from silver views'
    SCHEDULE 'wwi_lake_daily'
    TAGS 'wwi', 'medallion', 'gold', 'dimension'
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- dim_customer

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_customer (
    customer_key INT NOT NULL, customer_name VARCHAR NOT NULL, bill_to_customer_name VARCHAR,
    category_name VARCHAR, buying_group_name VARCHAR, primary_contact VARCHAR,
    delivery_method VARCHAR, delivery_city VARCHAR, delivery_state_province VARCHAR,
    delivery_country VARCHAR, postal_city VARCHAR, credit_limit DECIMAL(18,2),
    account_opened_date DATE, standard_discount_pct DECIMAL(18,3),
    is_on_credit_hold BOOLEAN, payment_days INT, phone_number VARCHAR, website_url VARCHAR
) LOCATION 'wwi/gold/dim_customer';

MERGE INTO wwi_lake.gold.dim_customer AS tgt
USING (
    SELECT customer_id AS customer_key, customer_name, bill_to_customer_name,
        category_name, buying_group_name, primary_contact, delivery_method,
        delivery_city, delivery_state_province, delivery_country, postal_city,
        credit_limit, account_opened_date, standard_discount_pct,
        is_on_credit_hold, payment_days, phone_number, website_url
    FROM wwi_lake.silver.v_customer
) AS src ON tgt.customer_key = src.customer_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_supplier

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_supplier (
    supplier_key INT NOT NULL, supplier_name VARCHAR NOT NULL, category_name VARCHAR,
    primary_contact VARCHAR, delivery_method VARCHAR, delivery_city VARCHAR,
    delivery_state_province VARCHAR, delivery_country VARCHAR, supplier_reference VARCHAR,
    payment_days INT, phone_number VARCHAR, website_url VARCHAR
) LOCATION 'wwi/gold/dim_supplier';

MERGE INTO wwi_lake.gold.dim_supplier AS tgt
USING (
    SELECT supplier_id AS supplier_key, supplier_name, category_name, primary_contact,
        delivery_method, delivery_city, delivery_state_province, delivery_country,
        supplier_reference, payment_days, phone_number, website_url
    FROM wwi_lake.silver.v_supplier
) AS src ON tgt.supplier_key = src.supplier_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_city

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_city (
    city_key INT NOT NULL, city_name VARCHAR NOT NULL, state_province_name VARCHAR,
    state_province_code VARCHAR, sales_territory VARCHAR, country_name VARCHAR,
    continent VARCHAR, region VARCHAR, subregion VARCHAR,
    city_population BIGINT, state_population BIGINT, country_population BIGINT
) LOCATION 'wwi/gold/dim_city';

MERGE INTO wwi_lake.gold.dim_city AS tgt
USING (
    SELECT city_id AS city_key, city_name, state_province_name, state_province_code,
        sales_territory, country_name, continent, region, subregion,
        city_population, state_population, country_population
    FROM wwi_lake.silver.v_city
) AS src ON tgt.city_key = src.city_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_employee

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_employee (
    employee_key INT NOT NULL, full_name VARCHAR NOT NULL, preferred_name VARCHAR,
    is_salesperson BOOLEAN, phone_number VARCHAR, email_address VARCHAR
) LOCATION 'wwi/gold/dim_employee';

MERGE INTO wwi_lake.gold.dim_employee AS tgt
USING (
    SELECT employee_id AS employee_key, full_name, preferred_name,
        is_salesperson, phone_number, email_address
    FROM wwi_lake.silver.v_employee
) AS src ON tgt.employee_key = src.employee_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_delivery_method

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_delivery_method (
    delivery_method_key INT NOT NULL, delivery_method_name VARCHAR NOT NULL
) LOCATION 'wwi/gold/dim_delivery_method';

MERGE INTO wwi_lake.gold.dim_delivery_method AS tgt
USING (
    SELECT delivery_method_id AS delivery_method_key, delivery_method_name
    FROM wwi_lake.bronze.delivery_methods
) AS src ON tgt.delivery_method_key = src.delivery_method_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_payment_method

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_payment_method (
    payment_method_key INT NOT NULL, payment_method_name VARCHAR NOT NULL
) LOCATION 'wwi/gold/dim_payment_method';

MERGE INTO wwi_lake.gold.dim_payment_method AS tgt
USING (
    SELECT payment_method_id AS payment_method_key, payment_method_name
    FROM wwi_lake.bronze.payment_methods
) AS src ON tgt.payment_method_key = src.payment_method_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_transaction_type

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_transaction_type (
    transaction_type_key INT NOT NULL, transaction_type_name VARCHAR NOT NULL
) LOCATION 'wwi/gold/dim_transaction_type';

MERGE INTO wwi_lake.gold.dim_transaction_type AS tgt
USING (
    SELECT transaction_type_id AS transaction_type_key, transaction_type_name
    FROM wwi_lake.bronze.transaction_types
) AS src ON tgt.transaction_type_key = src.transaction_type_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
