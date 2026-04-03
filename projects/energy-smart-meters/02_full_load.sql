-- =============================================================================
-- Energy Smart Meters Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE energy_daily_schedule
    CRON '0 3 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE energy_smart_meters_pipeline
    DESCRIPTION 'Smart meter pipeline with peak/off-peak costing, solar offset, and billing analytics'
    SCHEDULE 'energy_daily_schedule'
    TAGS 'energy,smart-meters,billing,solar'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Cost readings based on peak/off-peak tariff rates
-- =============================================================================
-- Peak hours: 7am-10pm (hour 7-22). Off-peak: 11pm-6am (hour 23, 0-6).
-- Net kWh = consumed - generated. Cost = net_kwh * applicable rate.
-- Validate against meter capacity.

MERGE INTO {{zone_prefix}}.silver.readings_costed AS tgt
USING (
    SELECT
        r.reading_id,
        r.meter_id,
        m.region_id,
        m.tariff_id,
        r.reading_date,
        r.reading_hour,
        r.kwh_consumed,
        COALESCE(r.kwh_generated, 0) AS kwh_generated,
        r.kwh_consumed - COALESCE(r.kwh_generated, 0) AS net_kwh,
        CASE WHEN r.reading_hour BETWEEN 7 AND 22 THEN true ELSE false END AS peak_flag,
        CASE
            WHEN r.reading_hour BETWEEN 7 AND 22 THEN t.peak_rate_per_kwh
            ELSE t.off_peak_rate_per_kwh
        END AS rate_applied,
        CAST(
            GREATEST(r.kwh_consumed - COALESCE(r.kwh_generated, 0), 0)
            * CASE
                WHEN r.reading_hour BETWEEN 7 AND 22 THEN t.peak_rate_per_kwh
                ELSE t.off_peak_rate_per_kwh
              END
        AS DECIMAL(8,2)) AS cost,
        m.capacity_kw,
        CASE
            WHEN r.kwh_consumed <= m.capacity_kw THEN true
            ELSE false
        END AS capacity_valid,
        r.ingested_at
    FROM {{zone_prefix}}.bronze.raw_readings r
    JOIN {{zone_prefix}}.bronze.raw_meters m ON r.meter_id = m.meter_id
    JOIN {{zone_prefix}}.bronze.raw_tariffs t ON m.tariff_id = t.tariff_id
) AS src
ON tgt.reading_id = src.reading_id
WHEN MATCHED THEN UPDATE SET
    tgt.net_kwh       = src.net_kwh,
    tgt.cost          = src.cost,
    tgt.rate_applied  = src.rate_applied,
    tgt.capacity_valid= src.capacity_valid,
    tgt.costed_at     = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    reading_id, meter_id, region_id, tariff_id, reading_date, reading_hour,
    kwh_consumed, kwh_generated, net_kwh, peak_flag, rate_applied, cost,
    capacity_kw, capacity_valid, costed_at
) VALUES (
    src.reading_id, src.meter_id, src.region_id, src.tariff_id, src.reading_date,
    src.reading_hour, src.kwh_consumed, src.kwh_generated, src.net_kwh,
    src.peak_flag, src.rate_applied, src.cost, src.capacity_kw,
    src.capacity_valid, src.ingested_at
);

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_region
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_region AS tgt
USING (
    SELECT
        region_id       AS region_key,
        region_name,
        grid_zone,
        utility_company,
        regulatory_body
    FROM {{zone_prefix}}.bronze.raw_regions
) AS src
ON tgt.region_key = src.region_key
WHEN NOT MATCHED THEN INSERT (region_key, region_name, grid_zone, utility_company, regulatory_body)
VALUES (src.region_key, src.region_name, src.grid_zone, src.utility_company, src.regulatory_body);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_tariff
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_tariff AS tgt
USING (
    SELECT
        tariff_id             AS tariff_key,
        tariff_name,
        rate_per_kwh,
        peak_rate_per_kwh,
        off_peak_rate_per_kwh,
        standing_charge,
        effective_from
    FROM {{zone_prefix}}.bronze.raw_tariffs
) AS src
ON tgt.tariff_key = src.tariff_key
WHEN MATCHED THEN UPDATE SET
    tgt.rate_per_kwh          = src.rate_per_kwh,
    tgt.peak_rate_per_kwh     = src.peak_rate_per_kwh,
    tgt.off_peak_rate_per_kwh = src.off_peak_rate_per_kwh,
    tgt.standing_charge       = src.standing_charge
WHEN NOT MATCHED THEN INSERT (tariff_key, tariff_name, rate_per_kwh, peak_rate_per_kwh, off_peak_rate_per_kwh, standing_charge, effective_from)
VALUES (src.tariff_key, src.tariff_name, src.rate_per_kwh, src.peak_rate_per_kwh, src.off_peak_rate_per_kwh, src.standing_charge, src.effective_from);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_meter
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_meter AS tgt
USING (
    SELECT
        meter_id         AS meter_key,
        meter_id,
        meter_type,
        customer_name,
        address,
        install_date,
        solar_panel_flag,
        capacity_kw
    FROM {{zone_prefix}}.bronze.raw_meters
) AS src
ON tgt.meter_key = src.meter_key
WHEN MATCHED THEN UPDATE SET
    tgt.capacity_kw      = src.capacity_kw,
    tgt.solar_panel_flag = src.solar_panel_flag
WHEN NOT MATCHED THEN INSERT (meter_key, meter_id, meter_type, customer_name, address, install_date, solar_panel_flag, capacity_kw)
VALUES (src.meter_key, src.meter_id, src.meter_type, src.customer_name, src.address, src.install_date, src.solar_panel_flag, src.capacity_kw);

-- =============================================================================
-- STEP 5: GOLD - Fact: fact_meter_readings
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_meter_readings AS tgt
USING (
    SELECT
        reading_id  AS reading_key,
        meter_id    AS meter_key,
        tariff_id   AS tariff_key,
        region_id   AS region_key,
        reading_date,
        reading_hour,
        kwh_consumed,
        kwh_generated,
        net_kwh,
        cost,
        peak_flag
    FROM {{zone_prefix}}.silver.readings_costed
    WHERE capacity_valid = true
) AS src
ON tgt.reading_key = src.reading_key
WHEN MATCHED THEN UPDATE SET
    tgt.net_kwh = src.net_kwh,
    tgt.cost    = src.cost
WHEN NOT MATCHED THEN INSERT (
    reading_key, meter_key, tariff_key, region_key, reading_date, reading_hour,
    kwh_consumed, kwh_generated, net_kwh, cost, peak_flag
) VALUES (
    src.reading_key, src.meter_key, src.tariff_key, src.region_key,
    src.reading_date, src.reading_hour, src.kwh_consumed, src.kwh_generated,
    src.net_kwh, src.cost, src.peak_flag
);

-- =============================================================================
-- STEP 6: GOLD - KPI: kpi_consumption_billing
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_consumption_billing AS tgt
USING (
    SELECT
        dr.region_name                        AS region,
        DATE_FORMAT(f.reading_date, 'yyyy-MM') AS billing_month,
        COUNT(DISTINCT f.meter_key)           AS total_meters,
        SUM(f.kwh_consumed)                   AS total_kwh,
        SUM(f.kwh_generated)                  AS total_generated,
        SUM(f.net_kwh)                        AS net_consumption,
        SUM(f.cost)                           AS total_revenue,
        CAST(SUM(f.cost) / NULLIF(COUNT(DISTINCT f.meter_key), 0) AS DECIMAL(8,2)) AS avg_bill,
        CAST(
            COUNT(CASE WHEN f.peak_flag = true THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)) AS peak_pct,
        CASE
            WHEN SUM(f.kwh_consumed) > 0
            THEN CAST(SUM(f.kwh_generated) * 100.0 / SUM(f.kwh_consumed) AS DECIMAL(5,2))
            ELSE 0
        END AS solar_offset_pct
    FROM {{zone_prefix}}.gold.fact_meter_readings f
    JOIN {{zone_prefix}}.gold.dim_region dr ON f.region_key = dr.region_key
    GROUP BY dr.region_name, DATE_FORMAT(f.reading_date, 'yyyy-MM')
) AS src
ON tgt.region = src.region AND tgt.billing_month = src.billing_month
WHEN MATCHED THEN UPDATE SET
    tgt.total_meters     = src.total_meters,
    tgt.total_kwh        = src.total_kwh,
    tgt.total_generated  = src.total_generated,
    tgt.net_consumption  = src.net_consumption,
    tgt.total_revenue    = src.total_revenue,
    tgt.avg_bill         = src.avg_bill,
    tgt.peak_pct         = src.peak_pct,
    tgt.solar_offset_pct = src.solar_offset_pct
WHEN NOT MATCHED THEN INSERT (
    region, billing_month, total_meters, total_kwh, total_generated,
    net_consumption, total_revenue, avg_bill, peak_pct, solar_offset_pct
) VALUES (
    src.region, src.billing_month, src.total_meters, src.total_kwh,
    src.total_generated, src.net_consumption, src.total_revenue, src.avg_bill,
    src.peak_pct, src.solar_offset_pct
);

-- =============================================================================
-- OPTIMIZE & VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.readings_costed;
OPTIMIZE {{zone_prefix}}.gold.fact_meter_readings;
VACUUM {{zone_prefix}}.bronze.raw_readings RETAIN 168 HOURS;
VACUUM {{zone_prefix}}.silver.readings_costed RETAIN 168 HOURS;
