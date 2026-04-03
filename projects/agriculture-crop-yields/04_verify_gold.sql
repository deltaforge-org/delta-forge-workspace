-- =============================================================================
-- Agriculture Crop Yields Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_harvest row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_harvest_count
FROM {{zone_prefix}}.gold.fact_harvest;

-- -----------------------------------------------------------------------------
-- 2. Verify all 8 fields loaded into dim_field
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_harvest_count >= 65
SELECT COUNT(*) AS field_count
FROM {{zone_prefix}}.gold.dim_field;

-- -----------------------------------------------------------------------------
-- 3. Verify crop dimension completeness (6 crop types with varieties)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT COUNT(DISTINCT crop_type) AS distinct_crop_types
FROM {{zone_prefix}}.gold.dim_crop;

-- -----------------------------------------------------------------------------
-- 4. Regional yield benchmarking - average yield per hectare by region and crop
-- -----------------------------------------------------------------------------
ASSERT VALUE distinct_crop_types >= 6
SELECT
    df.region,
    dc.crop_type,
    COUNT(*) AS harvests,
    CAST(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) AS DECIMAL(10,3)) AS avg_yield_per_ha,
    SUM(fh.revenue) AS total_revenue,
    SUM(fh.revenue) - SUM(fh.input_cost) AS total_profit
FROM {{zone_prefix}}.gold.fact_harvest fh
JOIN {{zone_prefix}}.gold.dim_field df ON fh.field_key = df.field_key
JOIN {{zone_prefix}}.gold.dim_crop dc ON fh.crop_key = dc.crop_key
GROUP BY df.region, dc.crop_type
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- 5. Crop profitability ranking with ROW_NUMBER
-- -----------------------------------------------------------------------------
ASSERT VALUE harvests > 0
SELECT
    dc.crop_type,
    dc.variety,
    SUM(fh.revenue) AS total_revenue,
    SUM(fh.revenue) - SUM(fh.input_cost) AS gross_profit,
    CAST(AVG(fh.profit_per_hectare) AS DECIMAL(12,2)) AS avg_profit_per_ha,
    ROW_NUMBER() OVER (ORDER BY SUM(fh.revenue) - SUM(fh.input_cost) DESC) AS profitability_rank
FROM {{zone_prefix}}.gold.fact_harvest fh
JOIN {{zone_prefix}}.gold.dim_crop dc ON fh.crop_key = dc.crop_key
GROUP BY dc.crop_type, dc.variety
ORDER BY profitability_rank
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 6. Weather impact analysis - yield vs drought index
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 5
SELECT
    dw.station_id,
    ds.season_name,
    ds.year,
    dw.drought_index,
    dw.total_rainfall_mm,
    CAST(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) AS DECIMAL(10,3)) AS avg_yield_per_ha,
    COUNT(*) AS harvest_count,
    CASE
        WHEN dw.drought_index > 0.5 THEN 'Severe Drought'
        WHEN dw.drought_index > 0.2 THEN 'Moderate Drought'
        ELSE 'Normal'
    END AS drought_severity
FROM {{zone_prefix}}.gold.fact_harvest fh
JOIN {{zone_prefix}}.gold.dim_weather dw ON fh.weather_key = dw.weather_key
JOIN {{zone_prefix}}.gold.dim_season ds ON fh.season_key = ds.season_key
GROUP BY dw.station_id, ds.season_name, ds.year, dw.drought_index, dw.total_rainfall_mm
ORDER BY dw.drought_index DESC;

-- -----------------------------------------------------------------------------
-- 7. Seasonal trend comparison with LAG window function
-- -----------------------------------------------------------------------------
ASSERT VALUE harvest_count > 0
SELECT
    dc.crop_type,
    ds.season_name || ' ' || CAST(ds.year AS STRING) AS period,
    CAST(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) AS DECIMAL(10,3)) AS avg_yield,
    LAG(CAST(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) AS DECIMAL(10,3)))
        OVER (PARTITION BY dc.crop_type ORDER BY ds.year, ds.season_name) AS prev_avg_yield,
    CAST(
        (AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) -
         LAG(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)))
            OVER (PARTITION BY dc.crop_type ORDER BY ds.year, ds.season_name))
        / NULLIF(LAG(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)))
            OVER (PARTITION BY dc.crop_type ORDER BY ds.year, ds.season_name), 0) * 100.0
    AS DECIMAL(8,2)) AS yield_change_pct
FROM {{zone_prefix}}.gold.fact_harvest fh
JOIN {{zone_prefix}}.gold.dim_crop dc ON fh.crop_key = dc.crop_key
JOIN {{zone_prefix}}.gold.dim_season ds ON fh.season_key = ds.season_key
GROUP BY dc.crop_type, ds.season_name, ds.year
ORDER BY dc.crop_type, ds.year, ds.season_name;

-- -----------------------------------------------------------------------------
-- 8. Field-level performance scorecard with ranking
-- -----------------------------------------------------------------------------
ASSERT VALUE avg_yield > 0
SELECT
    df.field_name,
    df.farm_name,
    df.region,
    df.soil_type,
    df.irrigation_type,
    COUNT(*) AS total_harvests,
    CAST(AVG(fh.yield_tonnes / NULLIF(fh.area_hectares, 0)) AS DECIMAL(10,3)) AS avg_yield_per_ha,
    CAST(AVG(fh.profit_per_hectare) AS DECIMAL(12,2)) AS avg_profit_per_ha,
    SUM(CASE WHEN fh.quality_grade = 'A' THEN 1 ELSE 0 END) AS grade_a_count,
    CAST(SUM(CASE WHEN fh.quality_grade = 'A' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS grade_a_pct,
    RANK() OVER (ORDER BY AVG(fh.profit_per_hectare) DESC) AS profit_rank
FROM {{zone_prefix}}.gold.fact_harvest fh
JOIN {{zone_prefix}}.gold.dim_field df ON fh.field_key = df.field_key
GROUP BY df.field_name, df.farm_name, df.region, df.soil_type, df.irrigation_type
ORDER BY profit_rank;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS orphaned_fields
FROM {{zone_prefix}}.gold.fact_harvest fh
LEFT JOIN {{zone_prefix}}.gold.dim_field df ON fh.field_key = df.field_key
WHERE df.field_key IS NULL;

ASSERT VALUE orphaned_fields = 0

SELECT COUNT(*) AS orphaned_crops
FROM {{zone_prefix}}.gold.fact_harvest fh
LEFT JOIN {{zone_prefix}}.gold.dim_crop dc ON fh.crop_key = dc.crop_key
WHERE dc.crop_key IS NULL;

ASSERT VALUE orphaned_crops = 0

SELECT COUNT(*) AS orphaned_weather
FROM {{zone_prefix}}.gold.fact_harvest fh
LEFT JOIN {{zone_prefix}}.gold.dim_weather dw ON fh.weather_key = dw.weather_key
WHERE dw.weather_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. KPI yield analysis - verify underperformer detection and weather correlation
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_weather = 0
SELECT
    region,
    crop_type,
    season,
    avg_yield_per_hectare,
    yield_vs_expected_pct,
    avg_profit_per_hectare,
    best_field,
    worst_field,
    weather_correlation
FROM {{zone_prefix}}.gold.kpi_yield_analysis
ORDER BY yield_vs_expected_pct ASC
LIMIT 10;

ASSERT VALUE avg_yield_per_hectare > 0
SELECT 'avg_yield_per_hectare check passed' AS avg_yield_per_hectare_status;

