-- =============================================================================
-- Omnichannel E-Commerce Orders Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- Pipeline DAG:
--   validate_bronze
--        |
--   merge_multi_source_orders  (3-way UNION ALL MERGE)
--        |
--   +----+--------------+
--   |                    |
--   segment_customers   build_sessions          (parallel)
--   |                    |
--   +----+--------+     |
--   |    |        |     |
--   dim_product  dim_channel  dim_customer  dim_date   (parallel)
--   |    |        |           |
--   +----+--------+-----------+
--        |
--   build_fact_order_lines
--        |
--   +----+-----------+
--   |                 |
--   compute_sales_kpi  compute_funnel_kpi       (parallel)
--   |                 |
--   materialize_inventory_cdf
--   |                 |
--   +--------+--------+
--            |
--   optimize_zorder (CONTINUE ON FAILURE)
-- =============================================================================

PIPELINE ecommerce_orders_07_full_load
  DESCRIPTION 'Omnichannel order pipeline: 3-way MERGE, soft-delete, CDF inventory, RFM segmentation, funnel sessionization'
  SCHEDULE 'ecommerce_30min_schedule'
  TAGS 'ecommerce,orders,medallion,multi-source'
  SLA 30
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze =====================

SELECT COUNT(*) AS web_count FROM ecom.bronze.raw_web_orders;
ASSERT VALUE web_count >= 30

SELECT COUNT(*) AS mobile_count FROM ecom.bronze.raw_mobile_orders;
ASSERT VALUE mobile_count >= 20

SELECT COUNT(*) AS pos_count FROM ecom.bronze.raw_pos_orders;
ASSERT VALUE pos_count >= 25

SELECT COUNT(*) AS product_count FROM ecom.bronze.raw_products;
ASSERT VALUE product_count = 20

SELECT COUNT(*) AS customer_count FROM ecom.bronze.raw_customers;
ASSERT VALUE customer_count = 18

SELECT COUNT(*) AS event_count FROM ecom.bronze.raw_browsing_events;
ASSERT VALUE event_count >= 40
SELECT 'Bronze validation passed' AS status;

-- ===================== merge_multi_source_orders =====================
-- 3-way UNION ALL from web, mobile, POS into unified silver table.
-- Soft-delete: cancelled orders get is_deleted=true + cancelled_at timestamp.

MERGE INTO ecom.silver.orders_unified AS tgt
USING (
    SELECT
        order_id, customer_id, product_id,
        'web' AS channel,
        quantity, unit_price, discount_pct,
        CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
        shipping_cost, order_date, status,
        CASE WHEN status = 'cancelled' THEN true ELSE false END AS is_deleted,
        CASE WHEN status = 'cancelled' THEN ingested_at ELSE NULL END AS cancelled_at,
        NULL AS store_id,
        session_id,
        ingested_at AS updated_at
    FROM ecom.bronze.raw_web_orders
    UNION ALL
    SELECT
        order_id, customer_id, product_id,
        'mobile' AS channel,
        quantity, unit_price, discount_pct,
        CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
        shipping_cost, order_date, status,
        CASE WHEN status = 'cancelled' THEN true ELSE false END AS is_deleted,
        CASE WHEN status = 'cancelled' THEN ingested_at ELSE NULL END AS cancelled_at,
        NULL AS store_id,
        session_id,
        ingested_at AS updated_at
    FROM ecom.bronze.raw_mobile_orders
    UNION ALL
    SELECT
        order_id, customer_id, product_id,
        'pos' AS channel,
        quantity, unit_price, discount_pct,
        CAST(quantity * unit_price * (1.0 - discount_pct) AS DECIMAL(12,2)) AS line_total,
        shipping_cost, order_date, status,
        CASE WHEN status = 'cancelled' THEN true ELSE false END AS is_deleted,
        CASE WHEN status = 'cancelled' THEN ingested_at ELSE NULL END AS cancelled_at,
        store_id,
        NULL AS session_id,
        ingested_at AS updated_at
    FROM ecom.bronze.raw_pos_orders
) AS src
ON tgt.order_id = src.order_id AND tgt.product_id = src.product_id
WHEN MATCHED AND src.status = 'cancelled' THEN UPDATE SET
    tgt.is_deleted   = true,
    tgt.cancelled_at = CURRENT_TIMESTAMP,
    tgt.status       = src.status,
    tgt.updated_at   = src.updated_at
WHEN MATCHED THEN UPDATE SET
    tgt.status       = src.status,
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.shipping_cost = src.shipping_cost,
    tgt.updated_at    = src.updated_at
WHEN NOT MATCHED THEN INSERT (
    order_id, customer_id, product_id, channel, quantity, unit_price, discount_pct,
    line_total, shipping_cost, order_date, status, is_deleted, cancelled_at,
    store_id, session_id, updated_at
) VALUES (
    src.order_id, src.customer_id, src.product_id, src.channel, src.quantity,
    src.unit_price, src.discount_pct, src.line_total, src.shipping_cost,
    src.order_date, src.status, src.is_deleted, src.cancelled_at,
    src.store_id, src.session_id, src.updated_at
);

-- ===================== segment_customers (RFM with NTILE) =====================

MERGE INTO ecom.silver.customer_rfm AS tgt
USING (
    WITH customer_metrics AS (
        SELECT
            c.customer_id,
            c.email,
            c.first_name,
            c.last_name,
            c.segment,
            c.city,
            c.state,
            c.country,
            c.registration_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COALESCE(SUM(o.line_total), 0) AS total_revenue,
            MAX(o.order_date) AS last_order_date,
            DATEDIFF(CAST('2024-07-01' AS DATE), MAX(o.order_date)) AS recency_days
        FROM ecom.bronze.raw_customers c
        LEFT JOIN ecom.silver.orders_unified o
            ON c.customer_id = o.customer_id
            AND o.is_deleted = false
        GROUP BY c.customer_id, c.email, c.first_name, c.last_name, c.segment,
                 c.city, c.state, c.country, c.registration_date
    ),
    rfm_scored AS (
        SELECT
            *,
            NTILE(4) OVER (ORDER BY recency_days ASC)  AS r_score,
            NTILE(4) OVER (ORDER BY total_orders DESC)  AS f_score,
            NTILE(4) OVER (ORDER BY total_revenue DESC) AS m_score
        FROM customer_metrics
    )
    SELECT
        *,
        r_score + f_score + m_score AS rfm_total,
        CASE
            WHEN r_score + f_score + m_score >= 10 THEN 'Champion'
            WHEN r_score + f_score + m_score >= 8  THEN 'Loyal'
            WHEN r_score + f_score + m_score >= 7  THEN 'Potential Loyalist'
            WHEN r_score >= 3 AND m_score >= 3     THEN 'Big Spender'
            WHEN r_score <= 2 AND f_score >= 3     THEN 'At Risk'
            WHEN r_score <= 1 AND f_score <= 1     THEN 'Lost'
            ELSE 'Developing'
        END AS rfm_segment
    FROM rfm_scored
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
    tgt.total_orders    = src.total_orders,
    tgt.total_revenue   = src.total_revenue,
    tgt.last_order_date = src.last_order_date,
    tgt.recency_days    = src.recency_days,
    tgt.r_score         = src.r_score,
    tgt.f_score         = src.f_score,
    tgt.m_score         = src.m_score,
    tgt.rfm_total       = src.rfm_total,
    tgt.rfm_segment     = src.rfm_segment,
    tgt.updated_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    customer_id, email, first_name, last_name, segment, city, state, country,
    registration_date, total_orders, total_revenue, last_order_date, recency_days,
    r_score, f_score, m_score, rfm_total, rfm_segment, updated_at
) VALUES (
    src.customer_id, src.email, src.first_name, src.last_name, src.segment,
    src.city, src.state, src.country, src.registration_date, src.total_orders,
    src.total_revenue, src.last_order_date, src.recency_days, src.r_score,
    src.f_score, src.m_score, src.rfm_total, src.rfm_segment, CURRENT_TIMESTAMP
);

-- ===================== build_sessions (funnel sessionization) =====================
-- Use LAG to detect session breaks (gap > 30 min), assign session boundaries,
-- then count events per funnel stage.

MERGE INTO ecom.silver.sessions AS tgt
USING (
    WITH event_gaps AS (
        SELECT
            event_id,
            customer_id,
            session_id,
            event_type,
            event_ts,
            LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts) AS prev_event_ts,
            CASE
                WHEN LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts) IS NULL THEN 1
                WHEN DATEDIFF(event_ts, LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts)) > 0 THEN 1
                ELSE 0
            END AS new_session_flag
        FROM ecom.bronze.raw_browsing_events
    ),
    session_numbered AS (
        SELECT
            *,
            SUM(new_session_flag) OVER (PARTITION BY customer_id ORDER BY event_ts) AS session_num
        FROM event_gaps
    )
    SELECT
        customer_id || '-' || CAST(session_num AS STRING) AS session_key,
        customer_id,
        MAX(session_id) AS session_id,
        MIN(event_ts) AS session_start,
        MAX(event_ts) AS session_end,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN event_type IN ('page_view', 'product_view') THEN 1 END) AS browse_count,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_count,
        COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END) AS checkout_count,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
        CASE
            WHEN COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0 THEN 'purchased'
            WHEN COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END) > 0 THEN 'checkout_abandoned'
            WHEN COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) > 0 THEN 'cart_abandoned'
            ELSE 'browse_only'
        END AS funnel_stage
    FROM session_numbered
    GROUP BY customer_id, session_num
) AS src
ON tgt.session_key = src.session_key
WHEN MATCHED THEN UPDATE SET
    tgt.session_start  = src.session_start,
    tgt.session_end    = src.session_end,
    tgt.event_count    = src.event_count,
    tgt.browse_count   = src.browse_count,
    tgt.cart_count     = src.cart_count,
    tgt.checkout_count = src.checkout_count,
    tgt.purchase_count = src.purchase_count,
    tgt.funnel_stage   = src.funnel_stage,
    tgt.processed_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    session_key, customer_id, session_id, session_start, session_end, event_count,
    browse_count, cart_count, checkout_count, purchase_count, funnel_stage, processed_at
) VALUES (
    src.session_key, src.customer_id, src.session_id, src.session_start, src.session_end,
    src.event_count, src.browse_count, src.cart_count, src.checkout_count, src.purchase_count,
    src.funnel_stage, CURRENT_TIMESTAMP
);

-- ===================== build_dim_product =====================

MERGE INTO ecom.gold.dim_product AS tgt
USING (
    SELECT
        product_id AS product_key, sku, product_name, category,
        subcategory, brand, unit_cost, list_price
    FROM ecom.bronze.raw_products
) AS src
ON tgt.product_key = src.product_key
WHEN MATCHED THEN UPDATE SET
    tgt.sku          = src.sku,
    tgt.product_name = src.product_name,
    tgt.category     = src.category,
    tgt.subcategory  = src.subcategory,
    tgt.brand        = src.brand,
    tgt.unit_cost    = src.unit_cost,
    tgt.list_price   = src.list_price
WHEN NOT MATCHED THEN INSERT (product_key, sku, product_name, category, subcategory, brand, unit_cost, list_price)
VALUES (src.product_key, src.sku, src.product_name, src.category, src.subcategory, src.brand, src.unit_cost, src.list_price);

-- ===================== build_dim_channel =====================

MERGE INTO ecom.gold.dim_channel AS tgt
USING (
    SELECT DISTINCT
        channel AS channel_key,
        channel AS channel_name,
        CASE
            WHEN channel = 'web' THEN 'Browser-based storefront'
            WHEN channel = 'mobile' THEN 'Mobile application'
            WHEN channel = 'pos' THEN 'In-store POS terminal'
        END AS channel_detail
    FROM ecom.silver.orders_unified
) AS src
ON tgt.channel_key = src.channel_key
WHEN NOT MATCHED THEN INSERT (channel_key, channel_name, channel_detail)
VALUES (src.channel_key, src.channel_name, src.channel_detail);

-- ===================== build_dim_customer =====================

MERGE INTO ecom.gold.dim_customer AS tgt
USING (
    SELECT
        r.customer_id AS customer_key,
        r.email,
        r.first_name || ' ' || r.last_name AS full_name,
        r.segment,
        r.city,
        r.state,
        r.country,
        c.loyalty_tier,
        r.registration_date,
        r.total_orders AS lifetime_orders,
        r.total_revenue AS lifetime_revenue,
        r.rfm_segment
    FROM ecom.silver.customer_rfm r
    JOIN ecom.bronze.raw_customers c ON r.customer_id = c.customer_id
) AS src
ON tgt.customer_key = src.customer_key
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_orders  = src.lifetime_orders,
    tgt.lifetime_revenue = src.lifetime_revenue,
    tgt.rfm_segment      = src.rfm_segment,
    tgt.loyalty_tier     = src.loyalty_tier
WHEN NOT MATCHED THEN INSERT (
    customer_key, email, full_name, segment, city, state, country,
    loyalty_tier, registration_date, lifetime_orders, lifetime_revenue, rfm_segment
) VALUES (
    src.customer_key, src.email, src.full_name, src.segment, src.city, src.state,
    src.country, src.loyalty_tier, src.registration_date, src.lifetime_orders,
    src.lifetime_revenue, src.rfm_segment
);

-- ===================== build_dim_date =====================

MERGE INTO ecom.gold.dim_date AS tgt
USING (
    WITH date_spine AS (
        SELECT DISTINCT order_date AS full_date
        FROM ecom.silver.orders_unified
    )
    SELECT
        CAST(EXTRACT(YEAR FROM full_date) * 10000
           + EXTRACT(MONTH FROM full_date) * 100
           + EXTRACT(DAY FROM full_date) AS INT) AS date_key,
        full_date,
        CAST(EXTRACT(YEAR FROM full_date) AS INT) AS year,
        CAST(EXTRACT(QUARTER FROM full_date) AS INT) AS quarter,
        CAST(EXTRACT(MONTH FROM full_date) AS INT) AS month,
        CASE CAST(EXTRACT(MONTH FROM full_date) AS INT)
            WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'   WHEN 5 THEN 'May'      WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'    WHEN 8 THEN 'August'   WHEN 9 THEN 'September'
            WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
        END AS month_name,
        CAST(EXTRACT(DOW FROM full_date) AS INT) AS day_of_week,
        CASE CAST(EXTRACT(DOW FROM full_date) AS INT)
            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN true ELSE false END AS is_weekend
    FROM date_spine
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend)
VALUES (src.date_key, src.full_date, src.year, src.quarter, src.month, src.month_name, src.day_of_week, src.day_name, src.is_weekend);

-- ===================== build_fact_order_lines =====================

MERGE INTO ecom.gold.fact_order_lines AS tgt
USING (
    SELECT
        o.order_id || '-' || o.product_id AS order_line_key,
        o.order_id AS order_key,
        o.product_id AS product_key,
        o.customer_id AS customer_key,
        o.channel AS channel_key,
        CAST(EXTRACT(YEAR FROM o.order_date) * 10000
           + EXTRACT(MONTH FROM o.order_date) * 100
           + EXTRACT(DAY FROM o.order_date) AS INT) AS date_key,
        o.order_date,
        o.quantity,
        o.unit_price,
        o.discount_pct,
        o.line_total,
        o.shipping_cost,
        o.status
    FROM ecom.silver.orders_unified o
    WHERE o.is_deleted = false
) AS src
ON tgt.order_line_key = src.order_line_key
WHEN MATCHED THEN UPDATE SET
    tgt.quantity      = src.quantity,
    tgt.unit_price    = src.unit_price,
    tgt.discount_pct  = src.discount_pct,
    tgt.line_total    = src.line_total,
    tgt.shipping_cost = src.shipping_cost,
    tgt.status        = src.status
WHEN NOT MATCHED THEN INSERT (
    order_line_key, order_key, product_key, customer_key, channel_key, date_key,
    order_date, quantity, unit_price, discount_pct, line_total, shipping_cost, status
) VALUES (
    src.order_line_key, src.order_key, src.product_key, src.customer_key,
    src.channel_key, src.date_key, src.order_date, src.quantity, src.unit_price,
    src.discount_pct, src.line_total, src.shipping_cost, src.status
);

-- ===================== compute_sales_kpi =====================

MERGE INTO ecom.gold.kpi_sales_dashboard AS tgt
USING (
    WITH monthly_orders AS (
        SELECT
            DATE_TRUNC('month', o.order_date) AS report_month,
            o.channel,
            o.order_id,
            o.customer_id,
            o.line_total,
            o.is_deleted,
            o.status
        FROM ecom.silver.orders_unified o
    ),
    repeat_cust AS (
        SELECT customer_id
        FROM monthly_orders
        WHERE is_deleted = false
        GROUP BY customer_id
        HAVING COUNT(DISTINCT order_id) > 1
    ),
    agg AS (
        SELECT
            m.report_month,
            m.channel,
            COUNT(DISTINCT m.order_id) AS total_orders,
            COALESCE(SUM(CASE WHEN m.is_deleted = false THEN m.line_total ELSE 0 END), 0) AS total_revenue,
            CASE
                WHEN COUNT(DISTINCT m.order_id) > 0
                THEN CAST(SUM(CASE WHEN m.is_deleted = false THEN m.line_total ELSE 0 END) / COUNT(DISTINCT m.order_id) AS DECIMAL(10,2))
                ELSE 0
            END AS avg_order_value,
            COUNT(DISTINCT CASE WHEN m.status = 'cancelled' THEN m.order_id END) AS cancelled_orders,
            CASE
                WHEN COUNT(DISTINCT m.order_id) > 0
                THEN CAST(
                    COUNT(DISTINCT CASE WHEN m.status = 'cancelled' THEN m.order_id END) * 1.0
                    / COUNT(DISTINCT m.order_id)
                AS DECIMAL(5,4))
                ELSE 0
            END AS cancellation_rate,
            COUNT(DISTINCT CASE WHEN m.is_deleted = false THEN m.customer_id END) AS unique_customers,
            COUNT(DISTINCT CASE WHEN r.customer_id IS NOT NULL AND m.is_deleted = false THEN m.customer_id END) AS repeat_customers,
            CASE
                WHEN COUNT(DISTINCT CASE WHEN m.is_deleted = false THEN m.customer_id END) > 0
                THEN CAST(
                    COUNT(DISTINCT CASE WHEN r.customer_id IS NOT NULL AND m.is_deleted = false THEN m.customer_id END) * 100.0
                    / COUNT(DISTINCT CASE WHEN m.is_deleted = false THEN m.customer_id END)
                AS DECIMAL(5,2))
                ELSE 0
            END AS repeat_rate_pct
        FROM monthly_orders m
        LEFT JOIN repeat_cust r ON m.customer_id = r.customer_id
        GROUP BY m.report_month, m.channel
    )
    SELECT * FROM agg
) AS src
ON tgt.report_month = src.report_month AND tgt.channel = src.channel
WHEN MATCHED THEN UPDATE SET
    tgt.total_orders      = src.total_orders,
    tgt.total_revenue     = src.total_revenue,
    tgt.avg_order_value   = src.avg_order_value,
    tgt.cancelled_orders  = src.cancelled_orders,
    tgt.cancellation_rate = src.cancellation_rate,
    tgt.unique_customers  = src.unique_customers,
    tgt.repeat_customers  = src.repeat_customers,
    tgt.repeat_rate_pct   = src.repeat_rate_pct
WHEN NOT MATCHED THEN INSERT (
    report_month, channel, total_orders, total_revenue, avg_order_value,
    cancelled_orders, cancellation_rate, unique_customers, repeat_customers, repeat_rate_pct
) VALUES (
    src.report_month, src.channel, src.total_orders, src.total_revenue,
    src.avg_order_value, src.cancelled_orders, src.cancellation_rate,
    src.unique_customers, src.repeat_customers, src.repeat_rate_pct
);

-- ===================== compute_funnel_kpi =====================

MERGE INTO ecom.gold.kpi_funnel_analysis AS tgt
USING (
    SELECT
        DATE_TRUNC('month', session_start) AS report_month,
        COUNT(*) AS total_sessions,
        COUNT(CASE WHEN browse_count > 0 THEN 1 END) AS browse_sessions,
        COUNT(CASE WHEN cart_count > 0 THEN 1 END) AS cart_sessions,
        COUNT(CASE WHEN checkout_count > 0 THEN 1 END) AS checkout_sessions,
        COUNT(CASE WHEN purchase_count > 0 THEN 1 END) AS purchase_sessions,
        CASE WHEN COUNT(CASE WHEN browse_count > 0 THEN 1 END) > 0
            THEN CAST(COUNT(CASE WHEN cart_count > 0 THEN 1 END) * 100.0
                 / COUNT(CASE WHEN browse_count > 0 THEN 1 END) AS DECIMAL(5,2))
            ELSE 0 END AS browse_to_cart_pct,
        CASE WHEN COUNT(CASE WHEN cart_count > 0 THEN 1 END) > 0
            THEN CAST(COUNT(CASE WHEN checkout_count > 0 THEN 1 END) * 100.0
                 / COUNT(CASE WHEN cart_count > 0 THEN 1 END) AS DECIMAL(5,2))
            ELSE 0 END AS cart_to_checkout_pct,
        CASE WHEN COUNT(CASE WHEN checkout_count > 0 THEN 1 END) > 0
            THEN CAST(COUNT(CASE WHEN purchase_count > 0 THEN 1 END) * 100.0
                 / COUNT(CASE WHEN checkout_count > 0 THEN 1 END) AS DECIMAL(5,2))
            ELSE 0 END AS checkout_to_purchase_pct,
        CASE WHEN COUNT(*) > 0
            THEN CAST(COUNT(CASE WHEN purchase_count > 0 THEN 1 END) * 100.0
                 / COUNT(*) AS DECIMAL(5,2))
            ELSE 0 END AS overall_conversion_pct
    FROM ecom.silver.sessions
    GROUP BY DATE_TRUNC('month', session_start)
) AS src
ON tgt.report_month = src.report_month
WHEN MATCHED THEN UPDATE SET
    tgt.total_sessions          = src.total_sessions,
    tgt.browse_sessions         = src.browse_sessions,
    tgt.cart_sessions           = src.cart_sessions,
    tgt.checkout_sessions       = src.checkout_sessions,
    tgt.purchase_sessions       = src.purchase_sessions,
    tgt.browse_to_cart_pct      = src.browse_to_cart_pct,
    tgt.cart_to_checkout_pct    = src.cart_to_checkout_pct,
    tgt.checkout_to_purchase_pct = src.checkout_to_purchase_pct,
    tgt.overall_conversion_pct  = src.overall_conversion_pct
WHEN NOT MATCHED THEN INSERT (
    report_month, total_sessions, browse_sessions, cart_sessions, checkout_sessions,
    purchase_sessions, browse_to_cart_pct, cart_to_checkout_pct,
    checkout_to_purchase_pct, overall_conversion_pct
) VALUES (
    src.report_month, src.total_sessions, src.browse_sessions, src.cart_sessions,
    src.checkout_sessions, src.purchase_sessions, src.browse_to_cart_pct,
    src.cart_to_checkout_pct, src.checkout_to_purchase_pct, src.overall_conversion_pct
);

-- ===================== materialize_inventory_cdf =====================
-- Capture order status changes from CDF on orders_unified for stock reconciliation.

MERGE INTO ecom.silver.inventory_adjustments AS tgt
USING (
    SELECT
        order_id || '-' || product_id || '-INV' AS adjustment_id,
        product_id,
        order_id,
        CASE
            WHEN status = 'cancelled' THEN 'restock'
            WHEN status = 'delivered' THEN 'sold'
            WHEN status = 'returned'  THEN 'return_restock'
            ELSE 'reserved'
        END AS change_type,
        CASE
            WHEN status IN ('cancelled', 'returned') THEN quantity
            ELSE -1 * quantity
        END AS quantity_delta,
        'CDF from orders_unified: status=' || status AS reason
    FROM ecom.silver.orders_unified
) AS src
ON tgt.adjustment_id = src.adjustment_id
WHEN MATCHED THEN UPDATE SET
    tgt.change_type    = src.change_type,
    tgt.quantity_delta  = src.quantity_delta,
    tgt.reason         = src.reason,
    tgt.captured_at    = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    adjustment_id, product_id, order_id, change_type, quantity_delta, reason, captured_at
) VALUES (
    src.adjustment_id, src.product_id, src.order_id, src.change_type,
    src.quantity_delta, src.reason, CURRENT_TIMESTAMP
);

-- ===================== optimize_zorder =====================

OPTIMIZE ecom.silver.orders_unified ZORDER BY (customer_id, order_date);
OPTIMIZE ecom.gold.fact_order_lines ZORDER BY (customer_key, order_date);
OPTIMIZE ecom.gold.kpi_sales_dashboard;
