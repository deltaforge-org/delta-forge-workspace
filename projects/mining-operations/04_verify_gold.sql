-- =============================================================================
-- Mining Operations Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_extraction row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_extraction_count
FROM {{zone_prefix}}.gold.fact_extraction;

-- -----------------------------------------------------------------------------
-- 2. Verify all 3 sites loaded into dim_site
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_extraction_count >= 70
SELECT COUNT(*) AS site_count
FROM {{zone_prefix}}.gold.dim_site;

-- -----------------------------------------------------------------------------
-- 3. Verify all 6 pits loaded into dim_pit
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS pit_count
FROM {{zone_prefix}}.gold.dim_pit;

-- -----------------------------------------------------------------------------
-- 4. Production by site and mineral type - cumulative extraction
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT
    ds.site_name,
    ds.mineral_type,
    ds.mine_type,
    COUNT(*) AS total_extractions,
    SUM(fe.ore_tonnes) AS total_ore_tonnes,
    SUM(fe.waste_tonnes) AS total_waste_tonnes,
    CAST(AVG(fe.strip_ratio) AS DECIMAL(8,3)) AS avg_strip_ratio,
    CAST(AVG(fe.grade_pct) AS DECIMAL(6,3)) AS avg_grade_pct,
    SUM(fe.ore_tonnes) OVER (PARTITION BY ds.site_name ORDER BY ds.site_name) AS site_cumulative_ore
FROM {{zone_prefix}}.gold.fact_extraction fe
JOIN {{zone_prefix}}.gold.dim_site ds ON fe.site_key = ds.site_key
GROUP BY ds.site_name, ds.mineral_type, ds.mine_type
ORDER BY total_ore_tonnes DESC;

-- -----------------------------------------------------------------------------
-- 5. Equipment productivity ranking
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    de.equipment_type,
    de.manufacturer,
    de.capacity_tonnes,
    de.maintenance_status,
    COUNT(*) AS extraction_count,
    SUM(fe.ore_tonnes) AS total_ore,
    CAST(AVG(fe.equipment_utilization_pct) AS DECIMAL(5,2)) AS avg_utilization_pct,
    CAST(AVG(fe.cycle_time_min) AS DECIMAL(6,2)) AS avg_cycle_time,
    RANK() OVER (ORDER BY SUM(fe.ore_tonnes) DESC) AS productivity_rank
FROM {{zone_prefix}}.gold.fact_extraction fe
JOIN {{zone_prefix}}.gold.dim_equipment de ON fe.equipment_key = de.equipment_key
GROUP BY de.equipment_type, de.manufacturer, de.capacity_tonnes, de.maintenance_status
ORDER BY productivity_rank;

-- -----------------------------------------------------------------------------
-- 6. Shift performance comparison
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT
    dsh.shift_type,
    dsh.supervisor,
    dsh.crew_size,
    COUNT(*) AS extractions,
    SUM(fe.ore_tonnes) AS total_ore,
    CAST(AVG(fe.equipment_utilization_pct) AS DECIMAL(5,2)) AS avg_utilization,
    CAST(AVG(fe.strip_ratio) AS DECIMAL(8,3)) AS avg_strip_ratio,
    CAST(AVG(fe.grade_pct) AS DECIMAL(6,3)) AS avg_grade
FROM {{zone_prefix}}.gold.fact_extraction fe
JOIN {{zone_prefix}}.gold.dim_shift dsh ON fe.shift_key = dsh.shift_key
GROUP BY dsh.shift_type, dsh.supervisor, dsh.crew_size
ORDER BY total_ore DESC;

-- -----------------------------------------------------------------------------
-- 7. Pit depth progress and extraction trends with LAG
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 3
SELECT
    dp.pit_name,
    dp.site_id,
    dp.current_depth_m,
    dp.target_depth_m,
    CAST(dp.current_depth_m / dp.target_depth_m * 100.0 AS DECIMAL(5,2)) AS depth_progress_pct,
    SUM(fe.ore_tonnes) AS total_ore_extracted,
    CAST(AVG(fe.grade_pct) AS DECIMAL(6,3)) AS avg_grade,
    dp.status
FROM {{zone_prefix}}.gold.fact_extraction fe
JOIN {{zone_prefix}}.gold.dim_pit dp ON fe.pit_key = dp.pit_key
GROUP BY dp.pit_name, dp.site_id, dp.current_depth_m, dp.target_depth_m, dp.status
ORDER BY total_ore_extracted DESC;

-- -----------------------------------------------------------------------------
-- 8. Cost optimization signals - high cost per tonne extractions
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 6
SELECT
    se.extraction_id,
    ds.site_name,
    dp.pit_name,
    de.equipment_type,
    se.ore_tonnes,
    se.strip_ratio,
    se.cost_per_tonne,
    se.equipment_utilization_pct,
    CASE
        WHEN se.cost_per_tonne > 5.00 THEN 'High Cost'
        WHEN se.cost_per_tonne > 3.00 THEN 'Medium Cost'
        ELSE 'Low Cost'
    END AS cost_category
FROM {{zone_prefix}}.silver.extractions_enriched se
JOIN {{zone_prefix}}.gold.dim_site ds ON se.site_id = ds.site_key
JOIN {{zone_prefix}}.gold.dim_pit dp ON se.pit_id = dp.pit_key
JOIN {{zone_prefix}}.gold.dim_equipment de ON se.equipment_id = de.equipment_key
WHERE se.cost_per_tonne > 3.00
ORDER BY se.cost_per_tonne DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT VALUE cost_per_tonne > 0
SELECT COUNT(*) AS orphaned_sites
FROM {{zone_prefix}}.gold.fact_extraction fe
LEFT JOIN {{zone_prefix}}.gold.dim_site ds ON fe.site_key = ds.site_key
WHERE ds.site_key IS NULL;

ASSERT VALUE orphaned_sites = 0

SELECT COUNT(*) AS orphaned_equipment
FROM {{zone_prefix}}.gold.fact_extraction fe
LEFT JOIN {{zone_prefix}}.gold.dim_equipment de ON fe.equipment_key = de.equipment_key
WHERE de.equipment_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. KPI production - verify cumulative YTD and reserve depletion
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_equipment = 0
SELECT
    kp.site_id,
    ds.site_name,
    kp.pit_id,
    kp.month,
    kp.total_ore_tonnes,
    kp.cumulative_ore_ytd,
    kp.reserve_depletion_pct,
    kp.avg_strip_ratio,
    kp.cost_per_tonne,
    kp.equipment_utilization_avg,
    kp.safety_incidents
FROM {{zone_prefix}}.gold.kpi_production kp
JOIN {{zone_prefix}}.gold.dim_site ds ON kp.site_id = ds.site_key
ORDER BY kp.site_id, kp.pit_id, kp.month;

ASSERT VALUE total_ore_tonnes > 0
SELECT 'total_ore_tonnes check passed' AS total_ore_tonnes_status;

