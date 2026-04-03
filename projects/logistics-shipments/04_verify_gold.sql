-- =============================================================================
-- Logistics Shipments Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify delivered shipments in fact table
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_shipment_count
FROM {{zone_prefix}}.gold.fact_shipments;

-- -----------------------------------------------------------------------------
-- 2. Verify all 6 carriers in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_shipment_count >= 18
SELECT COUNT(*) AS carrier_count
FROM {{zone_prefix}}.gold.dim_carrier;

-- -----------------------------------------------------------------------------
-- 3. Verify all 12 locations in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS location_count
FROM {{zone_prefix}}.gold.dim_location;

-- -----------------------------------------------------------------------------
-- 4. Carrier performance scorecard
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 12
SELECT
    dc.carrier_name,
    dc.carrier_type,
    COUNT(*) AS total_shipments,
    COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) AS on_time,
    CAST(
        COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) * 100.0 / COUNT(*)
    AS DECIMAL(5,2)) AS actual_on_time_pct,
    dc.on_time_rating * 100 AS contracted_on_time_pct,
    CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
    CAST(AVG(f.cost / NULLIF(f.weight_kg, 0)) AS DECIMAL(8,2)) AS avg_cost_per_kg
FROM {{zone_prefix}}.gold.fact_shipments f
JOIN {{zone_prefix}}.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
GROUP BY dc.carrier_name, dc.carrier_type, dc.on_time_rating
ORDER BY actual_on_time_pct DESC;

-- -----------------------------------------------------------------------------
-- 5. Route analysis (Z-ordered query pattern)
-- -----------------------------------------------------------------------------
ASSERT VALUE total_shipments > 0
SELECT
    dr.origin_city,
    dr.destination_city,
    dr.distance_km,
    dr.mode,
    dr.avg_transit_days,
    COUNT(f.shipment_key)      AS shipment_count,
    SUM(f.weight_kg)           AS total_weight_kg,
    SUM(f.cost)                AS total_cost
FROM {{zone_prefix}}.gold.dim_route dr
LEFT JOIN {{zone_prefix}}.gold.fact_shipments f
    ON f.origin_key || '->' || f.destination_key = dr.route_key
GROUP BY dr.origin_city, dr.destination_city, dr.distance_km, dr.mode, dr.avg_transit_days
ORDER BY total_cost DESC;

-- -----------------------------------------------------------------------------
-- 6. On-time delivery trend by month (window function)
-- -----------------------------------------------------------------------------
ASSERT VALUE origin_city IS NOT NULL
SELECT
    kp.month,
    SUM(kp.total_shipments)  AS total_shipments,
    SUM(kp.on_time_count)    AS on_time_total,
    CAST(SUM(kp.on_time_count) * 100.0 / NULLIF(SUM(kp.total_shipments), 0) AS DECIMAL(5,2)) AS on_time_pct,
    SUM(SUM(kp.total_shipments)) OVER (ORDER BY kp.month) AS cumulative_shipments
FROM {{zone_prefix}}.gold.kpi_delivery_performance kp
GROUP BY kp.month
ORDER BY kp.month;

-- -----------------------------------------------------------------------------
-- 7. SLA compliance by carrier
-- -----------------------------------------------------------------------------
ASSERT VALUE total_shipments > 0
SELECT
    carrier,
    SUM(total_shipments) AS total,
    SUM(on_time_count)   AS on_time,
    CAST(SUM(on_time_count) * 100.0 / NULLIF(SUM(total_shipments), 0) AS DECIMAL(5,2)) AS sla_compliance_pct,
    CAST(AVG(avg_transit_days) AS DECIMAL(5,1)) AS avg_transit
FROM {{zone_prefix}}.gold.kpi_delivery_performance
GROUP BY carrier
ORDER BY sla_compliance_pct DESC;

-- -----------------------------------------------------------------------------
-- 8. Late delivery analysis (fact-dim join)
-- -----------------------------------------------------------------------------
ASSERT VALUE total > 0
SELECT
    f.shipment_key,
    dc.carrier_name,
    ol.city AS origin,
    dl.city AS destination,
    f.ship_date,
    f.promised_date,
    f.delivery_date,
    f.transit_days,
    DATEDIFF(f.delivery_date, f.promised_date) AS days_late
FROM {{zone_prefix}}.gold.fact_shipments f
JOIN {{zone_prefix}}.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
JOIN {{zone_prefix}}.gold.dim_location ol ON f.origin_key = ol.location_key
JOIN {{zone_prefix}}.gold.dim_location dl ON f.destination_key = dl.location_key
WHERE f.on_time_flag = false
ORDER BY days_late DESC;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity
-- -----------------------------------------------------------------------------
ASSERT VALUE days_late > 0
SELECT COUNT(*) AS orphaned_carriers
FROM {{zone_prefix}}.gold.fact_shipments f
LEFT JOIN {{zone_prefix}}.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
WHERE dc.carrier_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. Cost efficiency: cost per km by carrier type
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_carriers = 0
SELECT
    dc.carrier_type,
    COUNT(*) AS shipments,
    CAST(AVG(f.cost) AS DECIMAL(10,2)) AS avg_cost,
    CAST(AVG(f.weight_kg) AS DECIMAL(8,2)) AS avg_weight,
    CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit
FROM {{zone_prefix}}.gold.fact_shipments f
JOIN {{zone_prefix}}.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
GROUP BY dc.carrier_type
ORDER BY avg_cost ASC;

ASSERT VALUE shipments > 0
SELECT 'shipments check passed' AS shipments_status;

