-- =============================================================================
-- Retail POS Sales Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE retail_daily_630am_schedule
    CRON '30 6 * * *'
    TIMEZONE 'America/Chicago'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE retail_pos_sales_pipeline
    DESCRIPTION 'Retail POS pipeline with Bloom filters, Z-ordering, window functions for store performance analytics'
    SCHEDULE 'retail_daily_630am_schedule'
    TAGS 'retail,pos,medallion,star-schema'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich transactions with product data, compute line totals
-- =============================================================================
-- Join transactions to products, compute line_total, flag returns and anomalies.

MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS tgt
USING (
    SELECT
        t.txn_id,
        t.receipt_id,
        t.store_id,
        t.sku,
        t.employee_id,
        t.txn_date,
        t.quantity,
        t.unit_price,
        t.discount_pct,
        CAST(t.quantity * t.unit_price * (1.0 - t.discount_pct) AS DECIMAL(12,2)) AS line_total,
        t.payment_method,
        t.basket_id,
        t.region,
        p.product_name,
        p.category,
        p.unit_cost,
        CASE WHEN t.quantity < 0 THEN true ELSE false END AS is_return,
        false AS anomaly_flag,
        t.ingested_at
    FROM {{zone_prefix}}.bronze.raw_transactions t
    LEFT JOIN {{zone_prefix}}.bronze.raw_products p ON t.sku = p.sku
) AS src
ON tgt.txn_id = src.txn_id
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.is_return     = src.is_return,
    tgt.enriched_at   = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    txn_id, receipt_id, store_id, sku, employee_id, txn_date, quantity, unit_price,
    discount_pct, line_total, payment_method, basket_id, region, product_name,
    category, unit_cost, is_return, anomaly_flag, enriched_at
) VALUES (
    src.txn_id, src.receipt_id, src.store_id, src.sku, src.employee_id, src.txn_date,
    src.quantity, src.unit_price, src.discount_pct, src.line_total, src.payment_method,
    src.basket_id, src.region, src.product_name, src.category, src.unit_cost,
    src.is_return, src.anomaly_flag, src.ingested_at
);

-- =============================================================================
-- STEP 2: SILVER - Flag anomalies (stores where returns > 20% of sales)
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS tgt
USING (
    WITH store_return_pct AS (
        SELECT
            store_id,
            CAST(ABS(SUM(CASE WHEN is_return THEN line_total ELSE 0 END)) * 100.0
                / NULLIF(SUM(CASE WHEN NOT is_return THEN line_total ELSE 0 END), 0) AS DECIMAL(5,2)) AS return_pct
        FROM {{zone_prefix}}.silver.transactions_enriched
        GROUP BY store_id
    )
    SELECT t.txn_id, true AS anomaly_flag
    FROM {{zone_prefix}}.silver.transactions_enriched t
    JOIN store_return_pct r ON t.store_id = r.store_id
    WHERE r.return_pct > 20.0
      AND t.is_return = true
) AS src
ON tgt.txn_id = src.txn_id
WHEN MATCHED THEN UPDATE SET
    tgt.anomaly_flag = src.anomaly_flag;

-- =============================================================================
-- STEP 3: SILVER - Build basket metrics
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.basket_metrics AS tgt
USING (
    SELECT
        basket_id,
        store_id,
        MIN(txn_date) AS txn_date,
        COUNT(*) AS items_in_basket,
        CAST(SUM(line_total) AS DECIMAL(12,2)) AS basket_value,
        MAX(CASE WHEN is_return THEN true ELSE false END) AS has_return,
        MAX(enriched_at) AS enriched_at
    FROM {{zone_prefix}}.silver.transactions_enriched
    GROUP BY basket_id, store_id
) AS src
ON tgt.basket_id = src.basket_id
WHEN MATCHED THEN UPDATE SET
    tgt.items_in_basket = src.items_in_basket,
    tgt.basket_value    = src.basket_value,
    tgt.has_return      = src.has_return,
    tgt.enriched_at     = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    basket_id, store_id, txn_date, items_in_basket, basket_value, has_return, enriched_at
) VALUES (
    src.basket_id, src.store_id, src.txn_date, src.items_in_basket,
    src.basket_value, src.has_return, src.enriched_at
);

-- =============================================================================
-- STEP 4: GOLD - Populate dim_store
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_store AS tgt
USING (
    SELECT
        store_id AS store_key,
        store_id,
        store_name,
        format,
        city,
        state,
        region,
        sqft,
        open_date,
        manager
    FROM {{zone_prefix}}.bronze.raw_stores
) AS src
ON tgt.store_key = src.store_key
WHEN MATCHED THEN UPDATE SET
    tgt.store_name = src.store_name,
    tgt.format     = src.format,
    tgt.city       = src.city,
    tgt.state      = src.state,
    tgt.region     = src.region,
    tgt.sqft       = src.sqft,
    tgt.open_date  = src.open_date,
    tgt.manager    = src.manager
WHEN NOT MATCHED THEN INSERT (
    store_key, store_id, store_name, format, city, state, region, sqft, open_date, manager
) VALUES (
    src.store_key, src.store_id, src.store_name, src.format, src.city, src.state,
    src.region, src.sqft, src.open_date, src.manager
);

-- =============================================================================
-- STEP 5: GOLD - Populate dim_product
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_product AS tgt
USING (
    SELECT
        sku AS product_key,
        sku,
        product_name,
        category,
        subcategory,
        brand,
        unit_cost,
        supplier,
        shelf_life_days
    FROM {{zone_prefix}}.bronze.raw_products
) AS src
ON tgt.product_key = src.product_key
WHEN MATCHED THEN UPDATE SET
    tgt.product_name    = src.product_name,
    tgt.category        = src.category,
    tgt.subcategory     = src.subcategory,
    tgt.brand           = src.brand,
    tgt.unit_cost       = src.unit_cost,
    tgt.supplier        = src.supplier,
    tgt.shelf_life_days = src.shelf_life_days
WHEN NOT MATCHED THEN INSERT (
    product_key, sku, product_name, category, subcategory, brand, unit_cost, supplier, shelf_life_days
) VALUES (
    src.product_key, src.sku, src.product_name, src.category, src.subcategory,
    src.brand, src.unit_cost, src.supplier, src.shelf_life_days
);

-- =============================================================================
-- STEP 6: GOLD - Populate dim_date (derived from transaction dates)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_date AS tgt
USING (
    SELECT DISTINCT
        CAST(txn_date AS STRING) AS date_key,
        txn_date AS full_date,
        CASE EXTRACT(DOW FROM txn_date)
            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(WEEK FROM txn_date) AS week_number,
        EXTRACT(MONTH FROM txn_date) AS month,
        EXTRACT(QUARTER FROM txn_date) AS quarter,
        EXTRACT(YEAR FROM txn_date) AS year,
        CASE WHEN EXTRACT(DOW FROM txn_date) IN (0, 6) THEN true ELSE false END AS is_weekend,
        false AS is_holiday
    FROM {{zone_prefix}}.silver.transactions_enriched
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day_of_week, week_number, month, quarter, year, is_weekend, is_holiday
) VALUES (
    src.date_key, src.full_date, src.day_of_week, src.week_number, src.month,
    src.quarter, src.year, src.is_weekend, src.is_holiday
);

-- =============================================================================
-- STEP 7: GOLD - Populate dim_cashier
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_cashier AS tgt
USING (
    SELECT
        employee_id AS cashier_key,
        employee_id,
        name,
        hire_date,
        shift_preference,
        certification_level
    FROM {{zone_prefix}}.bronze.raw_cashiers
) AS src
ON tgt.cashier_key = src.cashier_key
WHEN MATCHED THEN UPDATE SET
    tgt.name                = src.name,
    tgt.hire_date           = src.hire_date,
    tgt.shift_preference    = src.shift_preference,
    tgt.certification_level = src.certification_level
WHEN NOT MATCHED THEN INSERT (
    cashier_key, employee_id, name, hire_date, shift_preference, certification_level
) VALUES (
    src.cashier_key, src.employee_id, src.name, src.hire_date,
    src.shift_preference, src.certification_level
);

-- =============================================================================
-- STEP 8: GOLD - Populate fact_sales (non-return transactions only for core fact)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_sales AS tgt
USING (
    SELECT
        t.txn_id        AS sale_key,
        t.store_id      AS store_key,
        t.sku           AS product_key,
        t.employee_id   AS cashier_key,
        CAST(t.txn_date AS STRING) AS date_key,
        t.receipt_id,
        t.quantity,
        t.unit_price,
        t.discount_pct,
        t.line_total,
        t.payment_method,
        t.basket_id
    FROM {{zone_prefix}}.silver.transactions_enriched t
) AS src
ON tgt.sale_key = src.sale_key
WHEN MATCHED THEN UPDATE SET
    tgt.quantity       = src.quantity,
    tgt.unit_price     = src.unit_price,
    tgt.discount_pct   = src.discount_pct,
    tgt.line_total     = src.line_total
WHEN NOT MATCHED THEN INSERT (
    sale_key, store_key, product_key, cashier_key, date_key, receipt_id,
    quantity, unit_price, discount_pct, line_total, payment_method, basket_id
) VALUES (
    src.sale_key, src.store_key, src.product_key, src.cashier_key, src.date_key,
    src.receipt_id, src.quantity, src.unit_price, src.discount_pct, src.line_total,
    src.payment_method, src.basket_id
);

-- =============================================================================
-- STEP 9: GOLD - KPI Store Performance with YoY growth and regional ranking
-- =============================================================================
-- Uses LAG for YoY growth and DENSE_RANK for regional ranking.

MERGE INTO {{zone_prefix}}.gold.kpi_store_performance AS tgt
USING (
    WITH monthly_store AS (
        SELECT
            f.store_key                                AS store_id,
            ds.region,
            CAST(EXTRACT(YEAR FROM dd.full_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM dd.full_date) AS STRING), 2, '0') AS month,
            CAST(SUM(f.line_total) AS DECIMAL(14,2))   AS total_revenue,
            COUNT(DISTINCT f.receipt_id)               AS total_transactions,
            CAST(SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.basket_id), 0) AS DECIMAL(10,2)) AS avg_basket_value,
            CAST(CAST(COUNT(*) AS DECIMAL(10,2)) / NULLIF(COUNT(DISTINCT f.receipt_id), 0) AS DECIMAL(5,2)) AS items_per_transaction,
            CAST(
                (SUM(f.line_total) - SUM(f.quantity * dp.unit_cost)) * 100.0
                / NULLIF(SUM(f.line_total), 0)
            AS DECIMAL(5,2)) AS gross_margin_pct
        FROM {{zone_prefix}}.gold.fact_sales f
        JOIN {{zone_prefix}}.gold.dim_store ds ON f.store_key = ds.store_key
        JOIN {{zone_prefix}}.gold.dim_product dp ON f.product_key = dp.product_key
        JOIN {{zone_prefix}}.gold.dim_date dd ON f.date_key = dd.date_key
        GROUP BY f.store_key, ds.region,
            CAST(EXTRACT(YEAR FROM dd.full_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM dd.full_date) AS STRING), 2, '0')
    ),
    with_yoy AS (
        SELECT
            ms.*,
            LAG(ms.total_revenue, 12) OVER (PARTITION BY ms.store_id ORDER BY ms.month) AS prev_year_revenue,
            CAST(
                CASE
                    WHEN LAG(ms.total_revenue, 12) OVER (PARTITION BY ms.store_id ORDER BY ms.month) IS NOT NULL
                    THEN (ms.total_revenue - LAG(ms.total_revenue, 12) OVER (PARTITION BY ms.store_id ORDER BY ms.month))
                         * 100.0 / LAG(ms.total_revenue, 12) OVER (PARTITION BY ms.store_id ORDER BY ms.month)
                    ELSE NULL
                END
            AS DECIMAL(7,2)) AS yoy_growth_pct,
            DENSE_RANK() OVER (PARTITION BY ms.region, ms.month ORDER BY ms.total_revenue DESC) AS rank_in_region
        FROM monthly_store ms
    )
    SELECT
        store_id, region, month, total_revenue, total_transactions,
        avg_basket_value, items_per_transaction, gross_margin_pct,
        yoy_growth_pct, rank_in_region
    FROM with_yoy
) AS src
ON tgt.store_id = src.store_id AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.total_revenue          = src.total_revenue,
    tgt.total_transactions     = src.total_transactions,
    tgt.avg_basket_value       = src.avg_basket_value,
    tgt.items_per_transaction  = src.items_per_transaction,
    tgt.gross_margin_pct       = src.gross_margin_pct,
    tgt.yoy_growth_pct         = src.yoy_growth_pct,
    tgt.rank_in_region         = src.rank_in_region
WHEN NOT MATCHED THEN INSERT (
    store_id, region, month, total_revenue, total_transactions, avg_basket_value,
    items_per_transaction, gross_margin_pct, yoy_growth_pct, rank_in_region
) VALUES (
    src.store_id, src.region, src.month, src.total_revenue, src.total_transactions,
    src.avg_basket_value, src.items_per_transaction, src.gross_margin_pct,
    src.yoy_growth_pct, src.rank_in_region
);

-- =============================================================================
-- STEP 10: Z-ORDER fact_sales for fast range scans on store+date
-- =============================================================================

OPTIMIZE {{zone_prefix}}.gold.fact_sales;

-- =============================================================================
-- STEP 11: VACUUM to clean up old files
-- =============================================================================

VACUUM {{zone_prefix}}.gold.fact_sales RETAIN 168 HOURS;
VACUUM {{zone_prefix}}.silver.transactions_enriched RETAIN 168 HOURS;
