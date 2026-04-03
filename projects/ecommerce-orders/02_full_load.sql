-- =============================================================================
-- E-Commerce Orders Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE ecommerce_30min_schedule
    CRON '*/30 * * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 1800
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE ecommerce_orders_pipeline
    DESCRIPTION 'Multi-source e-commerce order pipeline with soft-delete, CDF, and RFM segmentation'
    SCHEDULE 'ecommerce_30min_schedule'
    TAGS 'ecommerce,orders,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Merge multi-source orders with soft-delete handling
-- =============================================================================
-- Multi-source MERGE: web, mobile, marketplace feeds into unified silver table.
-- Soft delete: cancelled orders get is_deleted=true instead of physical DELETE.
-- Derive line_total = quantity * unit_price * (1 - discount_pct).

MERGE INTO {{zone_prefix}}.silver.orders_merged AS tgt
USING (
    SELECT
        order_id,
        order_line_id,
        customer_id,
        product_id,
        channel,
        platform,
        region,
        order_date,
        quantity,
        unit_price,
        discount_pct,
        CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
        shipping_cost,
        order_status,
        CASE WHEN order_status = 'cancelled' THEN true ELSE is_deleted END AS is_deleted,
        ingested_at
    FROM {{zone_prefix}}.bronze.raw_orders
) AS src
ON tgt.order_line_id = src.order_line_id
WHEN MATCHED AND src.ingested_at > tgt.merged_at THEN UPDATE SET
    tgt.order_status = src.order_status,
    tgt.is_deleted   = src.is_deleted,
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.shipping_cost = src.shipping_cost,
    tgt.merged_at     = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    order_id, order_line_id, customer_id, product_id, channel, platform, region,
    order_date, quantity, unit_price, discount_pct, line_total, shipping_cost,
    order_status, is_deleted, merged_at
) VALUES (
    src.order_id, src.order_line_id, src.customer_id, src.product_id, src.channel,
    src.platform, src.region, src.order_date, src.quantity, src.unit_price,
    src.discount_pct, src.line_total, src.shipping_cost, src.order_status,
    src.is_deleted, src.ingested_at
);

-- =============================================================================
-- STEP 2: SILVER - Customer RFM segmentation
-- =============================================================================
-- RFM (Recency, Frequency, Monetary) using NTILE(4) for quartile scoring.
-- Only non-deleted, non-cancelled orders count toward customer metrics.

MERGE INTO {{zone_prefix}}.silver.customer_rfm AS tgt
USING (
    WITH customer_metrics AS (
        SELECT
            c.customer_id,
            c.email,
            c.segment,
            c.city,
            c.state,
            c.country,
            c.registration_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COALESCE(SUM(o.line_total), 0) AS total_revenue,
            MAX(o.order_date) AS last_order_date
        FROM {{zone_prefix}}.bronze.raw_customers c
        LEFT JOIN {{zone_prefix}}.silver.orders_merged o
            ON c.customer_id = o.customer_id
            AND o.is_deleted = false
            AND o.order_status NOT IN ('cancelled', 'returned')
        GROUP BY c.customer_id, c.email, c.segment, c.city, c.state, c.country, c.registration_date
    ),
    rfm_scored AS (
        SELECT
            *,
            NTILE(4) OVER (ORDER BY last_order_date ASC)  AS recency_score,
            NTILE(4) OVER (ORDER BY total_orders ASC)      AS frequency_score,
            NTILE(4) OVER (ORDER BY total_revenue ASC)     AS monetary_score
        FROM customer_metrics
    )
    SELECT
        *,
        CASE
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Champion'
            WHEN recency_score >= 3 AND frequency_score >= 2 THEN 'Loyal'
            WHEN recency_score >= 3 AND monetary_score >= 3 THEN 'Big Spender'
            WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 1 AND frequency_score <= 1 THEN 'Lost'
            ELSE 'Developing'
        END AS rfm_segment
    FROM rfm_scored
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    tgt.total_orders    = src.total_orders,
    tgt.total_revenue   = src.total_revenue,
    tgt.last_order_date = src.last_order_date,
    tgt.recency_score   = src.recency_score,
    tgt.frequency_score = src.frequency_score,
    tgt.monetary_score  = src.monetary_score,
    tgt.rfm_segment     = src.rfm_segment,
    tgt.updated_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    customer_id, email, segment, city, state, country, registration_date,
    total_orders, total_revenue, last_order_date, recency_score, frequency_score,
    monetary_score, rfm_segment, updated_at
) VALUES (
    src.customer_id, src.email, src.segment, src.city, src.state, src.country,
    src.registration_date, src.total_orders, src.total_revenue, src.last_order_date,
    src.recency_score, src.frequency_score, src.monetary_score, src.rfm_segment,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_channel
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_channel AS tgt
USING (
    SELECT DISTINCT
        channel || '-' || platform || '-' || region AS channel_key,
        channel   AS channel_name,
        platform,
        region
    FROM {{zone_prefix}}.silver.orders_merged
) AS src
ON tgt.channel_key = src.channel_key
WHEN NOT MATCHED THEN INSERT (channel_key, channel_name, platform, region)
VALUES (src.channel_key, src.channel_name, src.platform, src.region);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_product
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_product AS tgt
USING (
    SELECT
        product_id  AS product_key,
        sku,
        product_name,
        category,
        subcategory,
        brand,
        unit_cost
    FROM {{zone_prefix}}.bronze.raw_products
) AS src
ON tgt.product_key = src.product_key
WHEN MATCHED THEN UPDATE SET
    tgt.sku          = src.sku,
    tgt.product_name = src.product_name,
    tgt.category     = src.category,
    tgt.subcategory  = src.subcategory,
    tgt.brand        = src.brand,
    tgt.unit_cost    = src.unit_cost
WHEN NOT MATCHED THEN INSERT (product_key, sku, product_name, category, subcategory, brand, unit_cost)
VALUES (src.product_key, src.sku, src.product_name, src.category, src.subcategory, src.brand, src.unit_cost);

-- =============================================================================
-- STEP 5: GOLD - Dimension: dim_customer
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_customer AS tgt
USING (
    SELECT
        customer_id      AS customer_key,
        email,
        segment,
        city,
        state,
        country,
        registration_date,
        total_orders     AS lifetime_orders,
        rfm_segment
    FROM {{zone_prefix}}.silver.customer_rfm
) AS src
ON tgt.customer_key = src.customer_key
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_orders = src.lifetime_orders,
    tgt.rfm_segment     = src.rfm_segment
WHEN NOT MATCHED THEN INSERT (customer_key, email, segment, city, state, country, registration_date, lifetime_orders, rfm_segment)
VALUES (src.customer_key, src.email, src.segment, src.city, src.state, src.country, src.registration_date, src.lifetime_orders, src.rfm_segment);

-- =============================================================================
-- STEP 6: GOLD - Fact: fact_order_lines
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_order_lines AS tgt
USING (
    SELECT
        o.order_line_id                                        AS order_line_key,
        o.order_id                                             AS order_key,
        o.product_id                                           AS product_key,
        o.customer_id                                          AS customer_key,
        o.channel || '-' || o.platform || '-' || o.region      AS channel_key,
        o.order_date,
        o.quantity,
        o.unit_price,
        o.discount_pct,
        o.line_total,
        o.shipping_cost
    FROM {{zone_prefix}}.silver.orders_merged o
    WHERE o.is_deleted = false
) AS src
ON tgt.order_line_key = src.order_line_key
WHEN MATCHED THEN UPDATE SET
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.shipping_cost = src.shipping_cost
WHEN NOT MATCHED THEN INSERT (
    order_line_key, order_key, product_key, customer_key, channel_key,
    order_date, quantity, unit_price, discount_pct, line_total, shipping_cost
) VALUES (
    src.order_line_key, src.order_key, src.product_key, src.customer_key,
    src.channel_key, src.order_date, src.quantity, src.unit_price,
    src.discount_pct, src.line_total, src.shipping_cost
);

-- =============================================================================
-- STEP 7: GOLD - KPI: kpi_sales_dashboard
-- =============================================================================
-- Aggregates by order_date and channel with cancellation analysis.
-- Repeat customer = customer with > 1 distinct order across all time.

MERGE INTO {{zone_prefix}}.gold.kpi_sales_dashboard AS tgt
USING (
    WITH all_orders AS (
        SELECT
            order_date,
            channel,
            order_id,
            customer_id,
            line_total,
            is_deleted,
            order_status
        FROM {{zone_prefix}}.silver.orders_merged
    ),
    repeat_customers AS (
        SELECT customer_id
        FROM all_orders
        WHERE is_deleted = false AND order_status NOT IN ('cancelled')
        GROUP BY customer_id
        HAVING COUNT(DISTINCT order_id) > 1
    ),
    daily_channel AS (
        SELECT
            a.order_date,
            a.channel,
            COUNT(DISTINCT a.order_id) AS total_orders,
            COALESCE(SUM(CASE WHEN a.is_deleted = false THEN a.line_total ELSE 0 END), 0) AS total_revenue,
            CASE
                WHEN COUNT(DISTINCT a.order_id) > 0
                THEN CAST(SUM(CASE WHEN a.is_deleted = false THEN a.line_total ELSE 0 END) / COUNT(DISTINCT a.order_id) AS DECIMAL(10,2))
                ELSE 0
            END AS avg_order_value,
            CASE
                WHEN COUNT(DISTINCT a.order_id) > 0
                THEN CAST(
                    COUNT(DISTINCT CASE WHEN a.order_status = 'cancelled' THEN a.order_id END) * 1.0
                    / COUNT(DISTINCT a.order_id)
                AS DECIMAL(5,4))
                ELSE 0
            END AS cancellation_rate,
            COUNT(DISTINCT CASE WHEN a.is_deleted = false THEN a.customer_id END) AS unique_customers,
            CASE
                WHEN COUNT(DISTINCT CASE WHEN a.is_deleted = false THEN a.customer_id END) > 0
                THEN CAST(
                    COUNT(DISTINCT CASE WHEN r.customer_id IS NOT NULL AND a.is_deleted = false THEN a.customer_id END) * 100.0
                    / COUNT(DISTINCT CASE WHEN a.is_deleted = false THEN a.customer_id END)
                AS DECIMAL(5,2))
                ELSE 0
            END AS repeat_customer_pct
        FROM all_orders a
        LEFT JOIN repeat_customers r ON a.customer_id = r.customer_id
        GROUP BY a.order_date, a.channel
    )
    SELECT * FROM daily_channel
) AS src
ON tgt.order_date = src.order_date AND tgt.channel = src.channel
WHEN MATCHED THEN UPDATE SET
    tgt.total_orders        = src.total_orders,
    tgt.total_revenue       = src.total_revenue,
    tgt.avg_order_value     = src.avg_order_value,
    tgt.cancellation_rate   = src.cancellation_rate,
    tgt.unique_customers    = src.unique_customers,
    tgt.repeat_customer_pct = src.repeat_customer_pct
WHEN NOT MATCHED THEN INSERT (
    order_date, channel, total_orders, total_revenue, avg_order_value,
    cancellation_rate, unique_customers, repeat_customer_pct
) VALUES (
    src.order_date, src.channel, src.total_orders, src.total_revenue,
    src.avg_order_value, src.cancellation_rate, src.unique_customers,
    src.repeat_customer_pct
);

-- =============================================================================
-- OPTIMIZE
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.orders_merged;
OPTIMIZE {{zone_prefix}}.gold.fact_order_lines;
OPTIMIZE {{zone_prefix}}.gold.kpi_sales_dashboard;
