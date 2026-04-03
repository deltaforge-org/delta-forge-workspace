-- =============================================================================
-- Energy Smart Meters Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact table row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_readings_count
FROM {{zone_prefix}}.gold.fact_meter_readings;

-- -----------------------------------------------------------------------------
-- 2. Verify all 12 meters in dimension
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_readings_count >= 72
SELECT COUNT(*) AS meter_count
FROM {{zone_prefix}}.gold.dim_meter;

-- -----------------------------------------------------------------------------
-- 3. Verify all 3 tariffs in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS tariff_count
FROM {{zone_prefix}}.gold.dim_tariff;

-- -----------------------------------------------------------------------------
-- 4. Verify all 4 regions in dimension
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS region_count
FROM {{zone_prefix}}.gold.dim_region;

-- -----------------------------------------------------------------------------
-- 5. Peak vs off-peak distribution
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    CASE WHEN f.peak_flag = true THEN 'Peak' ELSE 'Off-Peak' END AS period,
    COUNT(*)               AS reading_count,
    SUM(f.kwh_consumed)    AS total_consumed,
    SUM(f.kwh_generated)   AS total_generated,
    SUM(f.cost)            AS total_cost,
    CAST(AVG(f.cost) AS DECIMAL(8,4)) AS avg_cost_per_reading
FROM {{zone_prefix}}.gold.fact_meter_readings f
GROUP BY CASE WHEN f.peak_flag = true THEN 'Peak' ELSE 'Off-Peak' END;

-- -----------------------------------------------------------------------------
-- 6. Solar offset analysis (meters with solar panels)
-- -----------------------------------------------------------------------------
ASSERT VALUE reading_count > 0
SELECT
    dm.meter_id,
    dm.customer_name,
    dm.capacity_kw,
    SUM(f.kwh_consumed)   AS total_consumed,
    SUM(f.kwh_generated)  AS total_generated,
    SUM(f.net_kwh)        AS net_consumption,
    CAST(SUM(f.kwh_generated) * 100.0 / NULLIF(SUM(f.kwh_consumed), 0) AS DECIMAL(5,2)) AS solar_offset_pct,
    SUM(f.cost)           AS total_bill
FROM {{zone_prefix}}.gold.fact_meter_readings f
JOIN {{zone_prefix}}.gold.dim_meter dm ON f.meter_key = dm.meter_key
WHERE dm.solar_panel_flag = true
GROUP BY dm.meter_id, dm.customer_name, dm.capacity_kw
ORDER BY solar_offset_pct DESC;

-- -----------------------------------------------------------------------------
-- 7. Regional consumption dashboard from KPI table
-- -----------------------------------------------------------------------------
ASSERT VALUE total_generated > 0
SELECT
    region,
    billing_month,
    total_meters,
    total_kwh,
    total_generated,
    net_consumption,
    total_revenue,
    avg_bill,
    peak_pct,
    solar_offset_pct
FROM {{zone_prefix}}.gold.kpi_consumption_billing
ORDER BY region, billing_month;

-- -----------------------------------------------------------------------------
-- 8. Revenue by tariff type (fact-dim join with window)
-- -----------------------------------------------------------------------------
ASSERT VALUE total_meters > 0
SELECT
    dt.tariff_name,
    dt.peak_rate_per_kwh,
    dt.off_peak_rate_per_kwh,
    COUNT(*)           AS readings,
    SUM(f.cost)        AS total_revenue,
    SUM(SUM(f.cost)) OVER (ORDER BY dt.tariff_name) AS cumulative_revenue
FROM {{zone_prefix}}.gold.fact_meter_readings f
JOIN {{zone_prefix}}.gold.dim_tariff dt ON f.tariff_key = dt.tariff_key
GROUP BY dt.tariff_name, dt.peak_rate_per_kwh, dt.off_peak_rate_per_kwh
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 9. Hourly consumption pattern (avg by hour across all meters)
-- -----------------------------------------------------------------------------
ASSERT VALUE readings > 0
SELECT
    f.reading_hour,
    f.peak_flag,
    CAST(AVG(f.kwh_consumed) AS DECIMAL(8,2)) AS avg_consumed,
    CAST(AVG(f.kwh_generated) AS DECIMAL(8,2)) AS avg_generated,
    CAST(AVG(f.cost) AS DECIMAL(8,4)) AS avg_cost
FROM {{zone_prefix}}.gold.fact_meter_readings f
GROUP BY f.reading_hour, f.peak_flag
ORDER BY f.reading_hour;

-- -----------------------------------------------------------------------------
-- 10. Referential integrity
-- -----------------------------------------------------------------------------
ASSERT VALUE avg_consumed > 0
SELECT COUNT(*) AS orphaned_meters
FROM {{zone_prefix}}.gold.fact_meter_readings f
LEFT JOIN {{zone_prefix}}.gold.dim_meter dm ON f.meter_key = dm.meter_key
WHERE dm.meter_key IS NULL;

ASSERT VALUE orphaned_meters = 0

SELECT COUNT(*) AS orphaned_tariffs
FROM {{zone_prefix}}.gold.fact_meter_readings f
LEFT JOIN {{zone_prefix}}.gold.dim_tariff dt ON f.tariff_key = dt.tariff_key
WHERE dt.tariff_key IS NULL;

ASSERT VALUE orphaned_tariffs = 0
SELECT 'orphaned_tariffs check passed' AS orphaned_tariffs_status;

