-- =============================================================================
-- Hospitality Reservations Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_reservations row count (includes soft-deleted cancellations)
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_reservations_count
FROM {{zone_prefix}}.gold.fact_reservations;

-- -----------------------------------------------------------------------------
-- 2. Verify all 4 properties in dim_property
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_reservations_count >= 60
SELECT COUNT(*) AS property_count
FROM {{zone_prefix}}.gold.dim_property;

-- -----------------------------------------------------------------------------
-- 3. Verify all 18 guests in dim_guest
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS guest_count
FROM {{zone_prefix}}.gold.dim_guest;

-- -----------------------------------------------------------------------------
-- 4. Revenue by property with star schema join
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 18
SELECT
    dp.property_name,
    dp.city,
    dp.star_rating,
    COUNT(*) AS total_reservations,
    COUNT(*) FILTER (WHERE f.is_cancelled = false) AS active_reservations,
    CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END) AS DECIMAL(14,2)) AS total_revenue,
    CAST(AVG(CASE WHEN f.is_cancelled = false THEN f.room_rate END) AS DECIMAL(10,2)) AS avg_room_rate
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_property dp ON f.property_key = dp.property_key
GROUP BY dp.property_name, dp.city, dp.star_rating
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 5. Channel mix analysis with commission impact
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    dc.channel_name,
    dc.channel_type,
    dc.commission_pct,
    COUNT(*) AS bookings,
    CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END) AS DECIMAL(14,2)) AS gross_revenue,
    CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount * dc.commission_pct / 100.0 ELSE 0 END) AS DECIMAL(14,2)) AS commission_cost,
    CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount * (1.0 - dc.commission_pct / 100.0) ELSE 0 END) AS DECIMAL(14,2)) AS net_revenue
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
GROUP BY dc.channel_name, dc.channel_type, dc.commission_pct
ORDER BY gross_revenue DESC;

-- -----------------------------------------------------------------------------
-- 6. Loyalty tier impact on revenue
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    dg.loyalty_tier,
    COUNT(DISTINCT dg.guest_key) AS guests,
    COUNT(*) AS reservations,
    CAST(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END) AS DECIMAL(14,2)) AS total_revenue,
    CAST(AVG(CASE WHEN f.is_cancelled = false THEN f.total_amount END) AS DECIMAL(10,2)) AS avg_booking_value,
    CAST(AVG(f.booking_lead_days) AS DECIMAL(7,2)) AS avg_lead_days
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_guest dg ON f.guest_key = dg.guest_key
GROUP BY dg.loyalty_tier
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 7. Room type performance with occupancy running average
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 4
SELECT
    rt.room_type,
    rt.base_rate,
    COUNT(*) AS reservations,
    CAST(AVG(CASE WHEN f.is_cancelled = false THEN f.room_rate END) AS DECIMAL(10,2)) AS actual_avg_rate,
    CAST(AVG(CASE WHEN f.is_cancelled = false THEN f.nights END) AS DECIMAL(5,2)) AS avg_stay_length,
    SUM(CASE WHEN f.is_cancelled = false THEN f.nights ELSE 0 END) AS total_room_nights,
    CAST(SUM(SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END))
        OVER (ORDER BY SUM(CASE WHEN f.is_cancelled = false THEN f.total_amount ELSE 0 END) DESC)
    AS DECIMAL(14,2)) AS cumulative_revenue
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_room_type rt ON f.room_type_key = rt.room_type_key
GROUP BY rt.room_type, rt.base_rate
ORDER BY total_room_nights DESC;

-- -----------------------------------------------------------------------------
-- 8. Cancellation pattern analysis (by lead time and channel)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    dc.channel_name,
    CASE
        WHEN f.booking_lead_days <= 7 THEN '0-7 days'
        WHEN f.booking_lead_days <= 30 THEN '8-30 days'
        ELSE '30+ days'
    END AS lead_time_bucket,
    COUNT(*) AS total_bookings,
    COUNT(*) FILTER (WHERE f.is_cancelled = true) AS cancellations,
    CAST(COUNT(*) FILTER (WHERE f.is_cancelled = true) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS cancel_rate
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
GROUP BY dc.channel_name,
    CASE
        WHEN f.booking_lead_days <= 7 THEN '0-7 days'
        WHEN f.booking_lead_days <= 30 THEN '8-30 days'
        ELSE '30+ days'
    END
ORDER BY dc.channel_name, lead_time_bucket;

-- -----------------------------------------------------------------------------
-- 9. Soft delete verification - cancelled bookings preserved
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 4
SELECT
    f.reservation_key,
    f.status,
    f.is_cancelled,
    f.total_amount,
    dp.property_name,
    dc.channel_name
FROM {{zone_prefix}}.gold.fact_reservations f
JOIN {{zone_prefix}}.gold.dim_property dp ON f.property_key = dp.property_key
JOIN {{zone_prefix}}.gold.dim_channel dc ON f.channel_key = dc.channel_key
WHERE f.is_cancelled = true;

-- -----------------------------------------------------------------------------
-- 10. KPI Revenue Management verification
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 4
SELECT
    k.property_id,
    dp.property_name,
    k.month,
    k.occupancy_rate,
    k.adr,
    k.revpar,
    k.total_revenue,
    k.cancellation_rate,
    k.direct_booking_pct,
    k.loyalty_revenue_pct
FROM {{zone_prefix}}.gold.kpi_revenue_management k
JOIN {{zone_prefix}}.gold.dim_property dp ON k.property_id = dp.property_key
ORDER BY k.property_id, k.month;

-- -----------------------------------------------------------------------------
-- Verification Summary
-- -----------------------------------------------------------------------------
ASSERT VALUE revpar > 0
SELECT
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_reservations) AS fact_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_property) AS dim_property_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_room_type) AS dim_room_type_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_guest) AS dim_guest_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_channel) AS dim_channel_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.kpi_revenue_management) AS kpi_rows;
