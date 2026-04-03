-- =============================================================================
-- Mining Operations Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE mining_daily_schedule
    CRON '0 5 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE mining_operations_pipeline
    DESCRIPTION 'Mining extraction analytics with strip ratio, cumulative YTD, equipment utilization, and reserve depletion'
    SCHEDULE 'mining_daily_schedule'
    TAGS 'mining,extraction,production,medallion'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich extractions with derived metrics
-- =============================================================================
-- Calculate strip ratio (waste/ore), cost per tonne, equipment utilization rate.
-- Equipment utilization = (equipment_hours / 8) * 100 (8-hour shift).

MERGE INTO {{zone_prefix}}.silver.extractions_enriched AS tgt
USING (
    SELECT
        e.extraction_id,
        e.site_id,
        e.pit_id,
        e.equipment_id,
        e.shift_id,
        e.extraction_date,
        e.ore_tonnes,
        e.waste_tonnes,
        CAST(e.waste_tonnes / NULLIF(e.ore_tonnes, 0) AS DECIMAL(8,3)) AS strip_ratio,
        e.grade_pct,
        e.recovery_pct,
        e.haul_distance_km,
        e.fuel_litres,
        e.cycle_time_min,
        e.equipment_hours,
        CAST(e.equipment_hours / 8.0 * 100.0 AS DECIMAL(5,2)) AS equipment_utilization_pct,
        CAST(
            (e.fuel_litres * 1.50 + e.equipment_hours * 250.00)
            / NULLIF(e.ore_tonnes, 0)
        AS DECIMAL(10,2)) AS cost_per_tonne,
        e.safety_incidents,
        CURRENT_TIMESTAMP AS enriched_at
    FROM {{zone_prefix}}.bronze.raw_extractions e
) AS src
ON tgt.extraction_id = src.extraction_id
WHEN MATCHED THEN UPDATE SET
    tgt.strip_ratio                = src.strip_ratio,
    tgt.equipment_utilization_pct  = src.equipment_utilization_pct,
    tgt.cost_per_tonne             = src.cost_per_tonne,
    tgt.enriched_at                = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    extraction_id, site_id, pit_id, equipment_id, shift_id, extraction_date,
    ore_tonnes, waste_tonnes, strip_ratio, grade_pct, recovery_pct,
    haul_distance_km, fuel_litres, cycle_time_min, equipment_hours,
    equipment_utilization_pct, cost_per_tonne, safety_incidents, enriched_at
) VALUES (
    src.extraction_id, src.site_id, src.pit_id, src.equipment_id, src.shift_id,
    src.extraction_date, src.ore_tonnes, src.waste_tonnes, src.strip_ratio,
    src.grade_pct, src.recovery_pct, src.haul_distance_km, src.fuel_litres,
    src.cycle_time_min, src.equipment_hours, src.equipment_utilization_pct,
    src.cost_per_tonne, src.safety_incidents, src.enriched_at
);

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_site
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_site AS tgt
USING (
    SELECT
        site_id AS site_key,
        site_id,
        site_name,
        mineral_type,
        country,
        region,
        reserve_estimate_mt,
        mine_type,
        environmental_rating
    FROM {{zone_prefix}}.bronze.raw_sites
) AS src
ON tgt.site_key = src.site_key
WHEN MATCHED THEN UPDATE SET
    tgt.reserve_estimate_mt  = src.reserve_estimate_mt,
    tgt.environmental_rating = src.environmental_rating
WHEN NOT MATCHED THEN INSERT (
    site_key, site_id, site_name, mineral_type, country, region,
    reserve_estimate_mt, mine_type, environmental_rating
) VALUES (
    src.site_key, src.site_id, src.site_name, src.mineral_type, src.country,
    src.region, src.reserve_estimate_mt, src.mine_type, src.environmental_rating
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_pit
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_pit AS tgt
USING (
    SELECT
        pit_id AS pit_key,
        pit_id,
        pit_name,
        site_id,
        current_depth_m,
        target_depth_m,
        bench_height_m,
        status
    FROM {{zone_prefix}}.bronze.raw_pits
) AS src
ON tgt.pit_key = src.pit_key
WHEN MATCHED THEN UPDATE SET
    tgt.current_depth_m = src.current_depth_m,
    tgt.status          = src.status
WHEN NOT MATCHED THEN INSERT (
    pit_key, pit_id, pit_name, site_id, current_depth_m, target_depth_m, bench_height_m, status
) VALUES (
    src.pit_key, src.pit_id, src.pit_name, src.site_id, src.current_depth_m,
    src.target_depth_m, src.bench_height_m, src.status
);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_equipment
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_equipment AS tgt
USING (
    SELECT
        equipment_id AS equipment_key,
        equipment_id,
        equipment_type,
        manufacturer,
        capacity_tonnes,
        operating_hours,
        maintenance_status,
        fuel_rate_l_per_hr
    FROM {{zone_prefix}}.bronze.raw_equipment
) AS src
ON tgt.equipment_key = src.equipment_key
WHEN MATCHED THEN UPDATE SET
    tgt.operating_hours    = src.operating_hours,
    tgt.maintenance_status = src.maintenance_status
WHEN NOT MATCHED THEN INSERT (
    equipment_key, equipment_id, equipment_type, manufacturer, capacity_tonnes,
    operating_hours, maintenance_status, fuel_rate_l_per_hr
) VALUES (
    src.equipment_key, src.equipment_id, src.equipment_type, src.manufacturer,
    src.capacity_tonnes, src.operating_hours, src.maintenance_status, src.fuel_rate_l_per_hr
);

-- =============================================================================
-- STEP 5: GOLD - Dimension: dim_shift
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_shift AS tgt
USING (
    SELECT
        shift_id AS shift_key,
        shift_type,
        start_hour,
        end_hour,
        crew_size,
        supervisor
    FROM {{zone_prefix}}.bronze.raw_shifts
) AS src
ON tgt.shift_key = src.shift_key
WHEN MATCHED THEN UPDATE SET
    tgt.crew_size  = src.crew_size,
    tgt.supervisor = src.supervisor
WHEN NOT MATCHED THEN INSERT (
    shift_key, shift_type, start_hour, end_hour, crew_size, supervisor
) VALUES (
    src.shift_key, src.shift_type, src.start_hour, src.end_hour,
    src.crew_size, src.supervisor
);

-- =============================================================================
-- STEP 6: GOLD - Fact: fact_extraction
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_extraction AS tgt
USING (
    SELECT
        extraction_id           AS extraction_key,
        site_id                 AS site_key,
        pit_id                  AS pit_key,
        equipment_id            AS equipment_key,
        shift_id                AS shift_key,
        extraction_date,
        ore_tonnes,
        waste_tonnes,
        strip_ratio,
        grade_pct,
        recovery_pct,
        haul_distance_km,
        fuel_litres,
        cycle_time_min,
        equipment_utilization_pct
    FROM {{zone_prefix}}.silver.extractions_enriched
) AS src
ON tgt.extraction_key = src.extraction_key
WHEN MATCHED THEN UPDATE SET
    tgt.ore_tonnes                 = src.ore_tonnes,
    tgt.strip_ratio                = src.strip_ratio,
    tgt.equipment_utilization_pct  = src.equipment_utilization_pct
WHEN NOT MATCHED THEN INSERT (
    extraction_key, site_key, pit_key, equipment_key, shift_key, extraction_date,
    ore_tonnes, waste_tonnes, strip_ratio, grade_pct, recovery_pct,
    haul_distance_km, fuel_litres, cycle_time_min, equipment_utilization_pct
) VALUES (
    src.extraction_key, src.site_key, src.pit_key, src.equipment_key, src.shift_key,
    src.extraction_date, src.ore_tonnes, src.waste_tonnes, src.strip_ratio,
    src.grade_pct, src.recovery_pct, src.haul_distance_km, src.fuel_litres,
    src.cycle_time_min, src.equipment_utilization_pct
);

-- =============================================================================
-- STEP 7: GOLD - KPI: kpi_production with cumulative YTD
-- =============================================================================
-- Cumulative ore extraction YTD using running SUM window function.
-- Reserve depletion = cumulative_ore_ytd / reserve_estimate_mt * 100.

MERGE INTO {{zone_prefix}}.gold.kpi_production AS tgt
USING (
    WITH monthly_agg AS (
        SELECT
            fe.site_key AS site_id,
            fe.pit_key AS pit_id,
            DATE_TRUNC('month', fe.extraction_date) AS month_date,
            SUM(fe.ore_tonnes) AS total_ore_tonnes,
            SUM(fe.waste_tonnes) AS total_waste_tonnes,
            CAST(AVG(fe.strip_ratio) AS DECIMAL(8,3)) AS avg_strip_ratio,
            CAST(AVG(fe.grade_pct) AS DECIMAL(6,3)) AS avg_grade,
            CAST(AVG(fe.recovery_pct) AS DECIMAL(6,2)) AS avg_recovery,
            CAST(AVG(fe.equipment_utilization_pct) AS DECIMAL(5,2)) AS equipment_utilization_avg,
            SUM(se.safety_incidents) AS safety_incidents,
            CAST(AVG(se.cost_per_tonne) AS DECIMAL(10,2)) AS cost_per_tonne
        FROM {{zone_prefix}}.gold.fact_extraction fe
        JOIN {{zone_prefix}}.silver.extractions_enriched se ON fe.extraction_key = se.extraction_id
        GROUP BY fe.site_key, fe.pit_key, DATE_TRUNC('month', fe.extraction_date)
    ),
    with_cumulative AS (
        SELECT
            ma.*,
            SUM(ma.total_ore_tonnes) OVER (
                PARTITION BY ma.site_id, ma.pit_id
                ORDER BY ma.month_date
            ) AS cumulative_ore_ytd
        FROM monthly_agg ma
    )
    SELECT
        wc.site_id,
        wc.pit_id,
        CAST(wc.month_date AS STRING) AS month,
        wc.total_ore_tonnes,
        wc.total_waste_tonnes,
        wc.avg_strip_ratio,
        wc.avg_grade,
        wc.avg_recovery,
        wc.cumulative_ore_ytd,
        CAST(
            wc.cumulative_ore_ytd / NULLIF(ds.reserve_estimate_mt * 1000000.0, 0) * 100.0
        AS DECIMAL(8,4)) AS reserve_depletion_pct,
        wc.cost_per_tonne,
        wc.equipment_utilization_avg,
        wc.safety_incidents
    FROM with_cumulative wc
    JOIN {{zone_prefix}}.gold.dim_site ds ON wc.site_id = ds.site_key
) AS src
ON tgt.site_id = src.site_id AND tgt.pit_id = src.pit_id AND tgt.month = src.month
WHEN MATCHED THEN UPDATE SET
    tgt.total_ore_tonnes          = src.total_ore_tonnes,
    tgt.total_waste_tonnes        = src.total_waste_tonnes,
    tgt.avg_strip_ratio           = src.avg_strip_ratio,
    tgt.cumulative_ore_ytd        = src.cumulative_ore_ytd,
    tgt.reserve_depletion_pct     = src.reserve_depletion_pct,
    tgt.cost_per_tonne            = src.cost_per_tonne,
    tgt.equipment_utilization_avg = src.equipment_utilization_avg,
    tgt.safety_incidents          = src.safety_incidents
WHEN NOT MATCHED THEN INSERT (
    site_id, pit_id, month, total_ore_tonnes, total_waste_tonnes, avg_strip_ratio,
    avg_grade, avg_recovery, cumulative_ore_ytd, reserve_depletion_pct,
    cost_per_tonne, equipment_utilization_avg, safety_incidents
) VALUES (
    src.site_id, src.pit_id, src.month, src.total_ore_tonnes, src.total_waste_tonnes,
    src.avg_strip_ratio, src.avg_grade, src.avg_recovery, src.cumulative_ore_ytd,
    src.reserve_depletion_pct, src.cost_per_tonne, src.equipment_utilization_avg,
    src.safety_incidents
);

-- =============================================================================
-- OPTIMIZE & VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.extractions_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_extraction;
OPTIMIZE {{zone_prefix}}.gold.kpi_production;

VACUUM {{zone_prefix}}.silver.extractions_enriched RETAIN 168 HOURS;
VACUUM {{zone_prefix}}.gold.fact_extraction RETAIN 168 HOURS;
