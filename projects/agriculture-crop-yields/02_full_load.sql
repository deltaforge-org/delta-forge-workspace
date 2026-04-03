-- =============================================================================
-- Agriculture Crop Yields Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE agriculture_daily_schedule
    CRON '0 12 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE agriculture_crop_yields_pipeline
    DESCRIPTION 'Agriculture crop yield analytics with weather correlation, field scorecards, and regional benchmarking'
    SCHEDULE 'agriculture_daily_schedule'
    TAGS 'agriculture,crops,yields,medallion'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Enrich harvests with yield calculations and sensor data
-- =============================================================================
-- Calculate yield per hectare, profit per hectare, compare vs expected yield,
-- flag underperformers (< 70% of expected), join with average sensor readings.

MERGE INTO {{zone_prefix}}.silver.harvests_enriched AS tgt
USING (
    WITH crop_expectations AS (
        SELECT 'Corn'     AS crop_type, 10.000 AS expected_yield UNION ALL
        SELECT 'Soybeans',               3.200 UNION ALL
        SELECT 'Wheat',                  4.000 UNION ALL
        SELECT 'Rice',                   7.000 UNION ALL
        SELECT 'Cotton',                 1.500 UNION ALL
        SELECT 'Almonds',                2.000 UNION ALL
        SELECT 'Grapes',                 7.000
    ),
    sensor_avg AS (
        SELECT
            field_id,
            AVG(soil_moisture) AS avg_soil_moisture,
            AVG(soil_ph)       AS avg_soil_ph
        FROM {{zone_prefix}}.bronze.raw_sensors
        GROUP BY field_id
    ),
    field_region AS (
        SELECT field_id, region
        FROM {{zone_prefix}}.bronze.raw_fields
    )
    SELECT
        h.harvest_id,
        h.field_id,
        h.crop_type,
        h.variety,
        h.season,
        h.year,
        h.station_id,
        fr.region,
        h.harvest_date,
        h.area_hectares,
        h.yield_tonnes,
        CAST(h.yield_tonnes / NULLIF(h.area_hectares, 0) AS DECIMAL(10,3)) AS yield_per_hectare,
        ce.expected_yield AS expected_yield_per_hectare,
        CAST(
            (h.yield_tonnes / NULLIF(h.area_hectares, 0)) / NULLIF(ce.expected_yield, 0) * 100.0
        AS DECIMAL(8,2)) AS yield_vs_expected_pct,
        h.moisture_pct,
        h.quality_grade,
        h.input_cost,
        h.revenue,
        CAST(h.revenue - h.input_cost AS DECIMAL(12,2)) AS profit,
        CAST((h.revenue - h.input_cost) / NULLIF(h.area_hectares, 0) AS DECIMAL(12,2)) AS profit_per_hectare,
        CASE
            WHEN (h.yield_tonnes / NULLIF(h.area_hectares, 0)) / NULLIF(ce.expected_yield, 0) < 0.70
            THEN true ELSE false
        END AS is_underperformer,
        sa.avg_soil_moisture,
        sa.avg_soil_ph,
        CURRENT_TIMESTAMP AS enriched_at
    FROM {{zone_prefix}}.bronze.raw_harvests h
    JOIN field_region fr ON h.field_id = fr.field_id
    LEFT JOIN crop_expectations ce ON h.crop_type = ce.crop_type
    LEFT JOIN sensor_avg sa ON h.field_id = sa.field_id
) AS src
ON tgt.harvest_id = src.harvest_id
WHEN MATCHED THEN UPDATE SET
    tgt.yield_per_hectare          = src.yield_per_hectare,
    tgt.expected_yield_per_hectare = src.expected_yield_per_hectare,
    tgt.yield_vs_expected_pct      = src.yield_vs_expected_pct,
    tgt.profit                     = src.profit,
    tgt.profit_per_hectare         = src.profit_per_hectare,
    tgt.is_underperformer          = src.is_underperformer,
    tgt.avg_soil_moisture          = src.avg_soil_moisture,
    tgt.avg_soil_ph                = src.avg_soil_ph,
    tgt.enriched_at                = src.enriched_at
WHEN NOT MATCHED THEN INSERT (
    harvest_id, field_id, crop_type, variety, season, year, station_id, region,
    harvest_date, area_hectares, yield_tonnes, yield_per_hectare,
    expected_yield_per_hectare, yield_vs_expected_pct, moisture_pct, quality_grade,
    input_cost, revenue, profit, profit_per_hectare, is_underperformer,
    avg_soil_moisture, avg_soil_ph, enriched_at
) VALUES (
    src.harvest_id, src.field_id, src.crop_type, src.variety, src.season, src.year,
    src.station_id, src.region, src.harvest_date, src.area_hectares, src.yield_tonnes,
    src.yield_per_hectare, src.expected_yield_per_hectare, src.yield_vs_expected_pct,
    src.moisture_pct, src.quality_grade, src.input_cost, src.revenue, src.profit,
    src.profit_per_hectare, src.is_underperformer, src.avg_soil_moisture, src.avg_soil_ph,
    src.enriched_at
);

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_field
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_field AS tgt
USING (
    SELECT
        field_id AS field_key,
        field_id,
        field_name,
        farm_name,
        region,
        soil_type,
        irrigation_type,
        total_hectares,
        gps_lat,
        gps_lon
    FROM {{zone_prefix}}.bronze.raw_fields
) AS src
ON tgt.field_key = src.field_key
WHEN MATCHED THEN UPDATE SET
    tgt.field_name      = src.field_name,
    tgt.farm_name       = src.farm_name,
    tgt.soil_type       = src.soil_type,
    tgt.irrigation_type = src.irrigation_type,
    tgt.total_hectares  = src.total_hectares
WHEN NOT MATCHED THEN INSERT (
    field_key, field_id, field_name, farm_name, region, soil_type,
    irrigation_type, total_hectares, gps_lat, gps_lon
) VALUES (
    src.field_key, src.field_id, src.field_name, src.farm_name, src.region,
    src.soil_type, src.irrigation_type, src.total_hectares, src.gps_lat, src.gps_lon
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_crop
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_crop AS tgt
USING (
    SELECT DISTINCT
        crop_type || '-' || variety AS crop_key,
        crop_type,
        variety,
        CASE crop_type
            WHEN 'Corn'     THEN 120
            WHEN 'Soybeans' THEN 100
            WHEN 'Wheat'    THEN 110
            WHEN 'Rice'     THEN 130
            WHEN 'Cotton'   THEN 160
            WHEN 'Almonds'  THEN 210
            WHEN 'Grapes'   THEN 180
            ELSE 120
        END AS growth_days,
        CASE crop_type
            WHEN 'Corn'     THEN 600.00
            WHEN 'Soybeans' THEN 400.00
            WHEN 'Wheat'    THEN 420.00
            WHEN 'Rice'     THEN 500.00
            WHEN 'Cotton'   THEN 630.00
            WHEN 'Almonds'  THEN 700.00
            WHEN 'Grapes'   THEN 800.00
            ELSE 500.00
        END AS seed_cost_per_hectare,
        CASE crop_type
            WHEN 'Corn'     THEN 10.000
            WHEN 'Soybeans' THEN 3.200
            WHEN 'Wheat'    THEN 4.000
            WHEN 'Rice'     THEN 7.000
            WHEN 'Cotton'   THEN 1.500
            WHEN 'Almonds'  THEN 2.000
            WHEN 'Grapes'   THEN 7.000
            ELSE 5.000
        END AS expected_yield_per_hectare
    FROM {{zone_prefix}}.bronze.raw_harvests
) AS src
ON tgt.crop_key = src.crop_key
WHEN MATCHED THEN UPDATE SET
    tgt.growth_days                = src.growth_days,
    tgt.seed_cost_per_hectare      = src.seed_cost_per_hectare,
    tgt.expected_yield_per_hectare = src.expected_yield_per_hectare
WHEN NOT MATCHED THEN INSERT (
    crop_key, crop_type, variety, growth_days, seed_cost_per_hectare, expected_yield_per_hectare
) VALUES (
    src.crop_key, src.crop_type, src.variety, src.growth_days,
    src.seed_cost_per_hectare, src.expected_yield_per_hectare
);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_season
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_season AS tgt
USING (
    SELECT DISTINCT
        season || '-' || CAST(year AS STRING) AS season_key,
        season AS season_name,
        year,
        CASE season
            WHEN 'Spring' THEN CAST(CAST(year AS STRING) || '-03-15' AS DATE)
            WHEN 'Summer' THEN CAST(CAST(year AS STRING) || '-06-01' AS DATE)
            ELSE CAST(CAST(year AS STRING) || '-03-01' AS DATE)
        END AS planting_start,
        CASE season
            WHEN 'Spring' THEN CAST(CAST(year AS STRING) || '-08-15' AS DATE)
            WHEN 'Summer' THEN CAST(CAST(year AS STRING) || '-07-15' AS DATE)
            ELSE CAST(CAST(year AS STRING) || '-09-01' AS DATE)
        END AS harvest_start,
        CASE
            WHEN season = 'Spring' AND year = 2024 THEN 'Low'
            WHEN season = 'Spring' THEN 'Medium'
            WHEN season = 'Summer' THEN 'None'
            ELSE 'High'
        END AS frost_risk_level
    FROM {{zone_prefix}}.bronze.raw_harvests
) AS src
ON tgt.season_key = src.season_key
WHEN MATCHED THEN UPDATE SET
    tgt.frost_risk_level = src.frost_risk_level
WHEN NOT MATCHED THEN INSERT (
    season_key, season_name, year, planting_start, harvest_start, frost_risk_level
) VALUES (
    src.season_key, src.season_name, src.year, src.planting_start,
    src.harvest_start, src.frost_risk_level
);

-- =============================================================================
-- STEP 5: GOLD - Dimension: dim_weather
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_weather AS tgt
USING (
    SELECT
        station_id || '-' || season || '-' || CAST(year AS STRING) AS weather_key,
        station_id,
        avg_temp_c,
        total_rainfall_mm,
        sunshine_hours,
        drought_index
    FROM {{zone_prefix}}.bronze.raw_weather
) AS src
ON tgt.weather_key = src.weather_key
WHEN MATCHED THEN UPDATE SET
    tgt.avg_temp_c        = src.avg_temp_c,
    tgt.total_rainfall_mm = src.total_rainfall_mm,
    tgt.sunshine_hours    = src.sunshine_hours,
    tgt.drought_index     = src.drought_index
WHEN NOT MATCHED THEN INSERT (
    weather_key, station_id, avg_temp_c, total_rainfall_mm, sunshine_hours, drought_index
) VALUES (
    src.weather_key, src.station_id, src.avg_temp_c, src.total_rainfall_mm,
    src.sunshine_hours, src.drought_index
);

-- =============================================================================
-- STEP 6: GOLD - Fact: fact_harvest
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_harvest AS tgt
USING (
    SELECT
        h.harvest_id AS harvest_key,
        h.field_id AS field_key,
        h.crop_type || '-' || h.variety AS crop_key,
        h.season || '-' || CAST(h.year AS STRING) AS season_key,
        h.station_id || '-' || h.season || '-' || CAST(h.year AS STRING) AS weather_key,
        h.harvest_date,
        h.area_hectares,
        h.yield_tonnes,
        h.moisture_pct,
        h.quality_grade,
        h.input_cost,
        h.revenue,
        h.profit_per_hectare
    FROM {{zone_prefix}}.silver.harvests_enriched h
) AS src
ON tgt.harvest_key = src.harvest_key
WHEN MATCHED THEN UPDATE SET
    tgt.yield_tonnes       = src.yield_tonnes,
    tgt.revenue            = src.revenue,
    tgt.profit_per_hectare = src.profit_per_hectare,
    tgt.quality_grade      = src.quality_grade
WHEN NOT MATCHED THEN INSERT (
    harvest_key, field_key, crop_key, season_key, weather_key, harvest_date,
    area_hectares, yield_tonnes, moisture_pct, quality_grade, input_cost,
    revenue, profit_per_hectare
) VALUES (
    src.harvest_key, src.field_key, src.crop_key, src.season_key, src.weather_key,
    src.harvest_date, src.area_hectares, src.yield_tonnes, src.moisture_pct,
    src.quality_grade, src.input_cost, src.revenue, src.profit_per_hectare
);

-- =============================================================================
-- STEP 7: GOLD - KPI: kpi_yield_analysis
-- =============================================================================
-- Regional yield benchmarking with weather correlation, crop profitability
-- ranking, and field-level performance scorecards.

MERGE INTO {{zone_prefix}}.gold.kpi_yield_analysis AS tgt
USING (
    WITH harvest_metrics AS (
        SELECT
            h.region,
            h.crop_type,
            h.season || '-' || CAST(h.year AS STRING) AS season,
            AVG(h.yield_per_hectare) AS avg_yield_per_hectare,
            AVG(h.yield_vs_expected_pct) AS yield_vs_expected_pct,
            SUM(h.revenue) AS total_revenue,
            SUM(h.input_cost) AS total_cost,
            AVG(h.profit_per_hectare) AS avg_profit_per_hectare,
            h.station_id
        FROM {{zone_prefix}}.silver.harvests_enriched h
        GROUP BY h.region, h.crop_type, h.season, h.year, h.station_id
    ),
    best_worst AS (
        SELECT
            region,
            crop_type,
            season || '-' || CAST(year AS STRING) AS season,
            FIRST_VALUE(field_id) OVER (
                PARTITION BY region, crop_type, season, year
                ORDER BY yield_per_hectare DESC
            ) AS best_field,
            FIRST_VALUE(field_id) OVER (
                PARTITION BY region, crop_type, season, year
                ORDER BY yield_per_hectare ASC
            ) AS worst_field
        FROM {{zone_prefix}}.silver.harvests_enriched
    ),
    weather_corr AS (
        SELECT
            station_id || '-' || season || '-' || CAST(year AS STRING) AS weather_key,
            station_id,
            season,
            CAST(year AS STRING) AS year_str,
            CASE
                WHEN drought_index > 0.5 THEN 'High Drought Stress'
                WHEN drought_index > 0.2 THEN 'Moderate Drought Stress'
                WHEN total_rainfall_mm > 400 THEN 'High Rainfall - Positive'
                WHEN total_rainfall_mm > 250 THEN 'Adequate Rainfall'
                ELSE 'Low Rainfall'
            END AS weather_correlation
        FROM {{zone_prefix}}.bronze.raw_weather
    )
    SELECT DISTINCT
        hm.region,
        hm.crop_type,
        hm.season,
        CAST(hm.avg_yield_per_hectare AS DECIMAL(10,3)),
        CAST(hm.yield_vs_expected_pct AS DECIMAL(8,2)),
        CAST(hm.total_revenue AS DECIMAL(14,2)),
        CAST(hm.total_cost AS DECIMAL(14,2)),
        CAST(hm.avg_profit_per_hectare AS DECIMAL(12,2)),
        bw.best_field,
        bw.worst_field,
        wc.weather_correlation
    FROM harvest_metrics hm
    LEFT JOIN (
        SELECT DISTINCT region, crop_type, season, best_field, worst_field
        FROM best_worst
    ) bw ON hm.region = bw.region AND hm.crop_type = bw.crop_type AND hm.season = bw.season
    LEFT JOIN weather_corr wc ON hm.station_id = wc.station_id
        AND hm.season = wc.season || '-' || wc.year_str
) AS src
ON tgt.region = src.region AND tgt.crop_type = src.crop_type AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET
    tgt.avg_yield_per_hectare  = src.avg_yield_per_hectare,
    tgt.yield_vs_expected_pct  = src.yield_vs_expected_pct,
    tgt.total_revenue          = src.total_revenue,
    tgt.total_cost             = src.total_cost,
    tgt.avg_profit_per_hectare = src.avg_profit_per_hectare,
    tgt.best_field             = src.best_field,
    tgt.worst_field            = src.worst_field,
    tgt.weather_correlation    = src.weather_correlation
WHEN NOT MATCHED THEN INSERT (
    region, crop_type, season, avg_yield_per_hectare, yield_vs_expected_pct,
    total_revenue, total_cost, avg_profit_per_hectare, best_field, worst_field,
    weather_correlation
) VALUES (
    src.region, src.crop_type, src.season, src.avg_yield_per_hectare,
    src.yield_vs_expected_pct, src.total_revenue, src.total_cost,
    src.avg_profit_per_hectare, src.best_field, src.worst_field,
    src.weather_correlation
);

-- =============================================================================
-- OPTIMIZE
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.harvests_enriched;
OPTIMIZE {{zone_prefix}}.gold.fact_harvest;
OPTIMIZE {{zone_prefix}}.gold.kpi_yield_analysis;
