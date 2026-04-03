-- =============================================================================
-- Logistics Shipments Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE logistics_6hr_schedule
    CRON '0 */6 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE logistics_shipments_pipeline
    DESCRIPTION 'Shipment tracking pipeline with idempotent dedup, route analysis, and carrier scorecards'
    SCHEDULE 'logistics_6hr_schedule'
    TAGS 'logistics,shipments,delivery-performance'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Idempotent dedup and shipment-level aggregation
-- =============================================================================
-- Collapse multiple events per shipment into single row with latest status.
-- Derive transit_days and on_time_flag. Idempotent: same shipment_id = update only.

MERGE INTO {{zone_prefix}}.silver.shipments_deduped AS tgt
USING (
    WITH latest_events AS (
        SELECT
            shipment_id,
            customer_id,
            carrier_id,
            origin_id,
            destination_id,
            ship_date,
            promised_date,
            MAX(delivery_date) AS delivery_date,
            -- Latest status based on event_timestamp
            FIRST_VALUE(event_type) OVER (
                PARTITION BY shipment_id ORDER BY event_timestamp DESC
            ) AS latest_status,
            weight_kg,
            volume_m3,
            cost,
            revenue,
            COUNT(*) OVER (PARTITION BY shipment_id) AS event_count,
            MAX(event_timestamp) OVER (PARTITION BY shipment_id) AS last_event_time,
            ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_timestamp DESC) AS rn
        FROM {{zone_prefix}}.bronze.raw_events
        GROUP BY shipment_id, customer_id, carrier_id, origin_id, destination_id,
                 ship_date, promised_date, event_type, event_timestamp, weight_kg,
                 volume_m3, cost, revenue
    ),
    shipment_summary AS (
        SELECT
            shipment_id,
            customer_id,
            carrier_id,
            origin_id,
            destination_id,
            ship_date,
            delivery_date,
            promised_date,
            latest_status,
            weight_kg,
            volume_m3,
            cost,
            revenue,
            CASE
                WHEN delivery_date IS NOT NULL
                THEN CAST(DATEDIFF(delivery_date, ship_date) AS INT)
                ELSE NULL
            END AS transit_days,
            CASE
                WHEN delivery_date IS NOT NULL AND delivery_date <= promised_date THEN true
                WHEN delivery_date IS NOT NULL AND delivery_date > promised_date THEN false
                ELSE NULL
            END AS on_time_flag,
            event_count,
            last_event_time
        FROM latest_events
        WHERE rn = 1
    )
    SELECT * FROM shipment_summary
) AS src
ON tgt.shipment_id = src.shipment_id
WHEN MATCHED AND src.last_event_time > tgt.last_event_time THEN UPDATE SET
    tgt.latest_status   = src.latest_status,
    tgt.delivery_date   = src.delivery_date,
    tgt.transit_days    = src.transit_days,
    tgt.on_time_flag    = src.on_time_flag,
    tgt.event_count     = src.event_count,
    tgt.last_event_time = src.last_event_time,
    tgt.deduped_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    shipment_id, customer_id, carrier_id, origin_id, destination_id,
    ship_date, delivery_date, promised_date, latest_status,
    weight_kg, volume_m3, cost, revenue, transit_days, on_time_flag,
    event_count, last_event_time, deduped_at
) VALUES (
    src.shipment_id, src.customer_id, src.carrier_id, src.origin_id,
    src.destination_id, src.ship_date, src.delivery_date, src.promised_date,
    src.latest_status, src.weight_kg, src.volume_m3, src.cost, src.revenue,
    src.transit_days, src.on_time_flag, src.event_count, src.last_event_time,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_carrier
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_carrier AS tgt
USING (
    SELECT
        carrier_id     AS carrier_key,
        carrier_name,
        carrier_type,
        fleet_size,
        on_time_rating,
        cost_per_kg
    FROM {{zone_prefix}}.bronze.raw_carriers
) AS src
ON tgt.carrier_key = src.carrier_key
WHEN MATCHED THEN UPDATE SET
    tgt.on_time_rating = src.on_time_rating,
    tgt.cost_per_kg    = src.cost_per_kg
WHEN NOT MATCHED THEN INSERT (carrier_key, carrier_name, carrier_type, fleet_size, on_time_rating, cost_per_kg)
VALUES (src.carrier_key, src.carrier_name, src.carrier_type, src.fleet_size, src.on_time_rating, src.cost_per_kg);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_location
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_location AS tgt
USING (
    SELECT
        location_id    AS location_key,
        city,
        state,
        country,
        region,
        warehouse_flag,
        latitude,
        longitude
    FROM {{zone_prefix}}.bronze.raw_locations
) AS src
ON tgt.location_key = src.location_key
WHEN NOT MATCHED THEN INSERT (location_key, city, state, country, region, warehouse_flag, latitude, longitude)
VALUES (src.location_key, src.city, src.state, src.country, src.region, src.warehouse_flag, src.latitude, src.longitude);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_route (derived from actual shipments)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_route AS tgt
USING (
    SELECT
        s.origin_id || '->' || s.destination_id AS route_key,
        ol.city AS origin_city,
        dl.city AS destination_city,
        -- Approximate distance using lat/lon (Haversine simplified as linear km)
        CAST(
            111.0 * SQRT(
                POWER(ol.latitude - dl.latitude, 2) +
                POWER((ol.longitude - dl.longitude) * COS(RADIANS((ol.latitude + dl.latitude) / 2)), 2)
            )
        AS INT) AS distance_km,
        CAST(AVG(s.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
        CASE
            WHEN c.carrier_type = 'Air' THEN 'Air'
            WHEN c.carrier_type = 'Ocean' THEN 'Ocean'
            WHEN c.carrier_type = 'Rail' THEN 'Rail'
            ELSE 'Road'
        END AS mode
    FROM {{zone_prefix}}.silver.shipments_deduped s
    JOIN {{zone_prefix}}.bronze.raw_locations ol ON s.origin_id = ol.location_id
    JOIN {{zone_prefix}}.bronze.raw_locations dl ON s.destination_id = dl.location_id
    JOIN {{zone_prefix}}.bronze.raw_carriers c ON s.carrier_id = c.carrier_id
    WHERE s.transit_days IS NOT NULL
    GROUP BY s.origin_id || '->' || s.destination_id, ol.city, dl.city,
             ol.latitude, ol.longitude, dl.latitude, dl.longitude, c.carrier_type
) AS src
ON tgt.route_key = src.route_key
WHEN MATCHED THEN UPDATE SET
    tgt.avg_transit_days = src.avg_transit_days,
    tgt.distance_km      = src.distance_km
WHEN NOT MATCHED THEN INSERT (route_key, origin_city, destination_city, distance_km, avg_transit_days, mode)
VALUES (src.route_key, src.origin_city, src.destination_city, src.distance_km, src.avg_transit_days, src.mode);

-- =============================================================================
-- STEP 5: GOLD - Fact: fact_shipments
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_shipments AS tgt
USING (
    SELECT
        shipment_id    AS shipment_key,
        carrier_id     AS carrier_key,
        origin_id      AS origin_key,
        destination_id AS destination_key,
        customer_id    AS customer_key,
        ship_date,
        delivery_date,
        promised_date,
        weight_kg,
        volume_m3,
        cost,
        on_time_flag,
        transit_days
    FROM {{zone_prefix}}.silver.shipments_deduped
    WHERE latest_status = 'delivered'
) AS src
ON tgt.shipment_key = src.shipment_key
WHEN MATCHED THEN UPDATE SET
    tgt.delivery_date = src.delivery_date,
    tgt.on_time_flag  = src.on_time_flag,
    tgt.transit_days  = src.transit_days
WHEN NOT MATCHED THEN INSERT (
    shipment_key, carrier_key, origin_key, destination_key, customer_key,
    ship_date, delivery_date, promised_date, weight_kg, volume_m3, cost,
    on_time_flag, transit_days
) VALUES (
    src.shipment_key, src.carrier_key, src.origin_key, src.destination_key,
    src.customer_key, src.ship_date, src.delivery_date, src.promised_date,
    src.weight_kg, src.volume_m3, src.cost, src.on_time_flag, src.transit_days
);

-- =============================================================================
-- STEP 6: GOLD - KPI: kpi_delivery_performance
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_delivery_performance AS tgt
USING (
    SELECT
        dc.carrier_name                         AS carrier,
        ol.city || ' -> ' || dl.city            AS route,
        DATE_FORMAT(f.ship_date, 'yyyy-MM')     AS month,
        COUNT(*)                                AS total_shipments,
        COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) AS on_time_count,
        CAST(
            COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) * 100.0 / COUNT(*)
        AS DECIMAL(5,2)) AS on_time_pct,
        CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
        CAST(AVG(f.cost / NULLIF(f.weight_kg, 0)) AS DECIMAL(8,2)) AS avg_cost_per_kg,
        SUM(f.weight_kg)                        AS total_weight,
        COALESCE(SUM(sd.revenue), 0)            AS total_revenue
    FROM {{zone_prefix}}.gold.fact_shipments f
    JOIN {{zone_prefix}}.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
    JOIN {{zone_prefix}}.gold.dim_location ol ON f.origin_key = ol.location_key
    JOIN {{zone_prefix}}.gold.dim_location dl ON f.destination_key = dl.location_key
    LEFT JOIN {{zone_prefix}}.silver.shipments_deduped sd ON f.shipment_key = sd.shipment_id
    GROUP BY dc.carrier_name, ol.city || ' -> ' || dl.city, DATE_FORMAT(f.ship_date, 'yyyy-MM')
) AS src
ON tgt.carrier = src.carrier AND tgt.route = src.route AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.total_shipments  = src.total_shipments,
    tgt.on_time_count    = src.on_time_count,
    tgt.on_time_pct      = src.on_time_pct,
    tgt.avg_transit_days = src.avg_transit_days,
    tgt.avg_cost_per_kg  = src.avg_cost_per_kg,
    tgt.total_weight     = src.total_weight,
    tgt.total_revenue    = src.total_revenue
WHEN NOT MATCHED THEN INSERT (
    carrier, route, month, total_shipments, on_time_count, on_time_pct,
    avg_transit_days, avg_cost_per_kg, total_weight, total_revenue
) VALUES (
    src.carrier, src.route, src.month, src.total_shipments, src.on_time_count,
    src.on_time_pct, src.avg_transit_days, src.avg_cost_per_kg,
    src.total_weight, src.total_revenue
);

-- =============================================================================
-- OPTIMIZE with Z-ORDER for route queries
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.shipments_deduped;
OPTIMIZE {{zone_prefix}}.gold.fact_shipments;
