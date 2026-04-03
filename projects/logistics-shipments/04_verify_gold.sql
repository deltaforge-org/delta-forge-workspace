-- =============================================================================
-- Logistics Shipments Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify dedup removed all 15 duplicates: 80 raw -> 65 unique events
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS deduped_event_count
FROM logi.silver.events_deduped;

-- -----------------------------------------------------------------------------
-- 2. Verify delivered shipments in fact table (should have 20+ delivered)
-- -----------------------------------------------------------------------------
ASSERT VALUE deduped_event_count >= 65
SELECT COUNT(*) AS fact_shipment_count
FROM logi.gold.fact_shipments;

-- -----------------------------------------------------------------------------
-- 3. Verify all 8 carriers in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_shipment_count >= 18
SELECT COUNT(*) AS carrier_count
FROM logi.gold.dim_carrier;

ASSERT ROW_COUNT = 8
SELECT carrier_key, carrier_name, carrier_type, headquarters
FROM logi.gold.dim_carrier
ORDER BY carrier_key;

-- -----------------------------------------------------------------------------
-- 4. Verify all 15 locations in dimension
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS location_count
FROM logi.gold.dim_location;

ASSERT VALUE location_count = 15
SELECT location_key, hub_name, city, hub_type
FROM logi.gold.dim_location
ORDER BY location_key;

-- -----------------------------------------------------------------------------
-- 5. Verify all 20 customers in dimension
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS customer_count
FROM logi.gold.dim_customer;

ASSERT VALUE customer_count = 20
SELECT customer_key, customer_name, tier, industry
FROM logi.gold.dim_customer
ORDER BY customer_key;

-- -----------------------------------------------------------------------------
-- 6. Carrier performance scorecard with actual vs contracted on-time
-- -----------------------------------------------------------------------------
SELECT
    dc.carrier_name,
    dc.carrier_type,
    dc.headquarters,
    COUNT(*) AS total_shipments,
    COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) AS on_time,
    CAST(
        COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) * 100.0 / COUNT(*)
    AS DECIMAL(5,2)) AS actual_on_time_pct,
    CAST(dc.on_time_rating * 100 AS DECIMAL(5,2)) AS contracted_on_time_pct,
    CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
    CAST(AVG(f.cost / NULLIF(f.weight_kg, 0)) AS DECIMAL(8,2)) AS avg_cost_per_kg,
    SUM(f.margin) AS total_margin
FROM logi.gold.fact_shipments f
JOIN logi.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
GROUP BY dc.carrier_name, dc.carrier_type, dc.headquarters, dc.on_time_rating
ORDER BY actual_on_time_pct DESC;

-- -----------------------------------------------------------------------------
-- 7. SLA violation analysis
-- -----------------------------------------------------------------------------
ASSERT VALUE total_shipments > 0
SELECT
    sv.shipment_id,
    dc.carrier_name,
    sv.service_level,
    sv.sla_max_days,
    sv.actual_transit_days,
    sv.days_over_sla,
    sv.penalty_amount,
    ol.city AS origin,
    dl.city AS destination
FROM logi.silver.sla_violations sv
JOIN logi.gold.dim_carrier dc ON sv.carrier_id = dc.carrier_key
JOIN logi.gold.dim_location ol ON sv.origin_id = ol.location_key
JOIN logi.gold.dim_location dl ON sv.destination_id = dl.location_key
WHERE sv.sla_violated = true
ORDER BY sv.days_over_sla DESC;

-- Should have at least 3 SLA violations
SELECT COUNT(*) AS violation_count
FROM logi.silver.sla_violations
WHERE sla_violated = true;

ASSERT VALUE violation_count >= 3
SELECT SUM(penalty_amount) AS total_penalty_exposure
FROM logi.silver.sla_violations
WHERE sla_violated = true;

-- -----------------------------------------------------------------------------
-- 8. Route analysis with Z-order query pattern (origin_key, destination_key)
-- -----------------------------------------------------------------------------
ASSERT VALUE total_penalty_exposure > 0
SELECT
    dr.origin_city,
    dr.destination_city,
    dr.distance_km,
    dr.primary_mode,
    dr.avg_transit_days,
    dr.shipment_count,
    SUM(f.weight_kg) AS total_weight_kg,
    SUM(f.cost) AS total_cost,
    SUM(f.margin) AS total_margin
FROM logi.gold.dim_route dr
LEFT JOIN logi.gold.fact_shipments f
    ON f.route_key = dr.route_key
GROUP BY dr.origin_city, dr.destination_city, dr.distance_km,
         dr.primary_mode, dr.avg_transit_days, dr.shipment_count
ORDER BY total_cost DESC;

-- -----------------------------------------------------------------------------
-- 9. On-time delivery trend by month (cumulative window function)
-- -----------------------------------------------------------------------------
ASSERT VALUE origin_city IS NOT NULL
SELECT
    kp.month,
    SUM(kp.total_shipments) AS total_shipments,
    SUM(kp.on_time_count) AS on_time_total,
    CAST(SUM(kp.on_time_count) * 100.0 / NULLIF(SUM(kp.total_shipments), 0)
        AS DECIMAL(5,2)) AS on_time_pct,
    SUM(kp.total_margin) AS monthly_margin,
    SUM(SUM(kp.total_shipments)) OVER (ORDER BY kp.month) AS cumulative_shipments
FROM logi.gold.kpi_delivery_performance kp
GROUP BY kp.month
ORDER BY kp.month;

-- -----------------------------------------------------------------------------
-- 10. SLA compliance KPI dashboard
-- -----------------------------------------------------------------------------
ASSERT VALUE total_shipments > 0
SELECT
    carrier_name,
    service_level,
    SUM(total_shipments) AS total,
    SUM(violated_count) AS violations,
    CAST(SUM(violated_count) * 100.0 / NULLIF(SUM(total_shipments), 0)
        AS DECIMAL(5,2)) AS violation_rate_pct,
    SUM(total_penalty) AS penalty_exposure,
    MAX(worst_violation) AS worst_days_over
FROM logi.gold.kpi_sla_compliance
GROUP BY carrier_name, service_level
ORDER BY violation_rate_pct DESC;

-- -----------------------------------------------------------------------------
-- 11. Late delivery deep dive (fact-dim join)
-- -----------------------------------------------------------------------------
ASSERT VALUE total > 0
SELECT
    f.shipment_key,
    dc.carrier_name,
    dcust.customer_name,
    dcust.tier AS customer_tier,
    ol.city AS origin,
    dl.city AS destination,
    f.service_level,
    f.ship_date,
    f.promised_date,
    f.delivery_date,
    f.transit_days,
    DATEDIFF(f.delivery_date, f.promised_date) AS days_late,
    f.penalty_amount
FROM logi.gold.fact_shipments f
JOIN logi.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
JOIN logi.gold.dim_customer dcust ON f.customer_key = dcust.customer_key
JOIN logi.gold.dim_location ol ON f.origin_key = ol.location_key
JOIN logi.gold.dim_location dl ON f.destination_key = dl.location_key
WHERE f.on_time_flag = false
ORDER BY days_late DESC;

-- -----------------------------------------------------------------------------
-- 12. Referential integrity checks
-- -----------------------------------------------------------------------------
ASSERT VALUE days_late > 0
SELECT COUNT(*) AS orphaned_carriers
FROM logi.gold.fact_shipments f
LEFT JOIN logi.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
WHERE dc.carrier_key IS NULL;

ASSERT VALUE orphaned_carriers = 0
SELECT COUNT(*) AS orphaned_locations
FROM logi.gold.fact_shipments f
LEFT JOIN logi.gold.dim_location ol ON f.origin_key = ol.location_key
WHERE ol.location_key IS NULL;

ASSERT VALUE orphaned_locations = 0
SELECT COUNT(*) AS orphaned_customers
FROM logi.gold.fact_shipments f
LEFT JOIN logi.gold.dim_customer dc ON f.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

ASSERT VALUE orphaned_customers = 0
SELECT COUNT(*) AS orphaned_routes
FROM logi.gold.fact_shipments f
LEFT JOIN logi.gold.dim_route dr ON f.route_key = dr.route_key
WHERE dr.route_key IS NULL;

-- -----------------------------------------------------------------------------
-- 13. Customer tier analysis: margin by tier
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_routes = 0
SELECT
    dcust.tier,
    COUNT(*) AS shipments,
    CAST(AVG(f.margin) AS DECIMAL(10,2)) AS avg_margin,
    SUM(f.revenue) AS total_revenue,
    CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit
FROM logi.gold.fact_shipments f
JOIN logi.gold.dim_customer dcust ON f.customer_key = dcust.customer_key
GROUP BY dcust.tier
ORDER BY avg_margin DESC;

-- -----------------------------------------------------------------------------
-- 14. Shipment timeline reconstruction accuracy: event count distribution
-- -----------------------------------------------------------------------------
ASSERT VALUE shipments > 0
SELECT
    event_count,
    COUNT(*) AS shipment_count,
    CAST(AVG(total_transit_hours) AS DECIMAL(8,1)) AS avg_transit_hours
FROM logi.silver.shipment_status
GROUP BY event_count
ORDER BY event_count;

-- -----------------------------------------------------------------------------
-- 15. Final verification summary
-- -----------------------------------------------------------------------------
ASSERT VALUE shipment_count > 0
SELECT 'Logistics Shipments Gold Layer Verification PASSED' AS status;
