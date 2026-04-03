-- =============================================================================
-- Agriculture Crop Yields Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- New harvest records from a late-season 2024 batch are added and processed.

-- Show current state before incremental load
SELECT 'silver.harvests_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.harvests_enriched
UNION ALL
SELECT 'gold.fact_harvest', COUNT(*)
FROM {{zone_prefix}}.gold.fact_harvest;

-- =============================================================================
-- Insert 8 new bronze harvest records (late-season 2024 batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_harvests VALUES
('H066', 'F001', 'Corn',     'Pioneer P1197',   'Spring', 2024, 'WS-MW-01', '2024-09-28', 40.00,  420.000, 13.80, 'A', 25000.00,  52080.00, '2024-10-01T00:00:00'),
('H067', 'F007', 'Wheat',    'SY Wolverine',    'Spring', 2024, 'WS-MW-01', '2024-07-28', 80.00,  296.000, 12.00, 'B', 33600.00,  50320.00, '2024-10-01T00:00:00'),
('H068', 'F003', 'Rice',     'CL153',           'Spring', 2024, 'WS-SE-01', '2024-09-10', 30.00,  216.000, 18.00, 'A', 15600.00,  32400.00, '2024-10-01T00:00:00'),
('H069', 'F004', 'Cotton',   'DP 1646 B2XF',    'Spring', 2024, 'WS-SE-01', '2024-10-28', 30.00,   43.500, 8.50,  'B', 18900.00,  36975.00, '2024-10-30T00:00:00'),
('H070', 'F006', 'Almonds',  'Nonpareil',       'Spring', 2024, 'WS-WC-01', '2024-08-20', 50.00,  110.000, 5.00,  'A', 36250.00,  99000.00, '2024-10-30T00:00:00'),
('H071', 'F005', 'Grapes',   'Cabernet Sauv.',  'Spring', 2024, 'WS-WC-01', '2024-10-02', 15.00,  108.000, 22.00, 'A', 12375.00,  43200.00, '2024-10-30T00:00:00'),
('H072', 'F008', 'Soybeans', 'Credenz CZ 4105', 'Spring', 2024, 'WS-SE-01', '2024-11-12', 25.00,   40.000, 15.20, 'D', 10500.00,  14400.00, '2024-11-15T00:00:00'),
('H073', 'F002', 'Soybeans', 'Asgrow AG36X6',   'Spring', 2024, 'WS-MW-01', '2024-10-12', 30.00,   96.000, 12.40, 'A', 12600.00,  34560.00, '2024-11-15T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only process new/changed harvests using INCREMENTAL_FILTER
-- =============================================================================

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
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.harvests_enriched, harvest_id, harvest_date, 7)}}
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

-- Refresh gold fact for incremental records
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
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_harvest, harvest_key, harvest_date, 7)}}
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
-- Verify incremental processing
-- =============================================================================

-- Silver should now have 65 + 8 = 73 rows
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.harvests_enriched;
-- Verify new records appeared in gold
ASSERT ROW_COUNT = 73
SELECT COUNT(*) AS gold_total FROM {{zone_prefix}}.gold.fact_harvest;
-- Verify CDF captured the incremental changes
ASSERT VALUE gold_total >= 73
SELECT COUNT(*) AS cdf_changes
FROM table_changes('{{zone_prefix}}.silver.harvests_enriched', 1);
