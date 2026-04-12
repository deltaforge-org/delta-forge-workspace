-- ============================================================================
-- WWI Data Lake - Gold Dimensions
-- ============================================================================
-- Materializes star schema dimensions from silver views.
-- All dimensions use natural keys from the source system.
-- MERGE with NOT MATCHED BY SOURCE catches dimension member deletions.
-- ============================================================================


-- dim_customer

MERGE INTO wwi_lake.gold.dim_customer AS tgt
USING (
    SELECT
        customer_id              AS customer_key,
        customer_name,
        bill_to_customer_name,
        category_name,
        buying_group_name,
        primary_contact,
        delivery_method,
        delivery_city,
        delivery_state_province,
        delivery_country,
        postal_city,
        credit_limit,
        account_opened_date,
        standard_discount_pct,
        is_on_credit_hold,
        payment_days,
        phone_number,
        website_url
    FROM wwi_lake.silver.v_customer
) AS src ON tgt.customer_key = src.customer_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_supplier

MERGE INTO wwi_lake.gold.dim_supplier AS tgt
USING (
    SELECT
        supplier_id              AS supplier_key,
        supplier_name,
        category_name,
        primary_contact,
        delivery_method,
        delivery_city,
        delivery_state_province,
        delivery_country,
        supplier_reference,
        payment_days,
        phone_number,
        website_url
    FROM wwi_lake.silver.v_supplier
) AS src ON tgt.supplier_key = src.supplier_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_city

MERGE INTO wwi_lake.gold.dim_city AS tgt
USING (
    SELECT
        city_id                  AS city_key,
        city_name,
        state_province_name,
        state_province_code,
        sales_territory,
        country_name,
        continent,
        region,
        subregion,
        city_population,
        state_population,
        country_population
    FROM wwi_lake.silver.v_city
) AS src ON tgt.city_key = src.city_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_employee

MERGE INTO wwi_lake.gold.dim_employee AS tgt
USING (
    SELECT
        employee_id              AS employee_key,
        full_name,
        preferred_name,
        is_salesperson,
        phone_number,
        email_address
    FROM wwi_lake.silver.v_employee
) AS src ON tgt.employee_key = src.employee_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_delivery_method

MERGE INTO wwi_lake.gold.dim_delivery_method AS tgt
USING (
    SELECT delivery_method_id AS delivery_method_key, delivery_method_name
    FROM wwi_lake.bronze.delivery_methods
) AS src ON tgt.delivery_method_key = src.delivery_method_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_payment_method

MERGE INTO wwi_lake.gold.dim_payment_method AS tgt
USING (
    SELECT payment_method_id AS payment_method_key, payment_method_name
    FROM wwi_lake.bronze.payment_methods
) AS src ON tgt.payment_method_key = src.payment_method_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- dim_transaction_type

MERGE INTO wwi_lake.gold.dim_transaction_type AS tgt
USING (
    SELECT transaction_type_id AS transaction_type_key, transaction_type_name
    FROM wwi_lake.bronze.transaction_types
) AS src ON tgt.transaction_type_key = src.transaction_type_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
