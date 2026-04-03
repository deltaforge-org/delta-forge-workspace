-- =============================================================================
-- Mining Operations Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- New March 2024 extraction records are added and processed.

-- Show current state before incremental load
SELECT 'silver.extractions_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.extractions_enriched
UNION ALL
SELECT 'gold.fact_extraction', COUNT(*)
FROM {{zone_prefix}}.gold.fact_extraction;

-- =============================================================================
-- Insert 8 new bronze extraction records (March 2024 batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_extractions VALUES
('EX-071', 'SITE-001', 'PIT-001', 'EQ-001', 'SH-DAY',   '2024-03-01', 950.000,  2380.000, 1.160, 90.50, 4.20, 740.00, 29.50, 8.00, 0, '2024-03-02T00:00:00'),
('EX-072', 'SITE-001', 'PIT-001', 'EQ-002', 'SH-SWING', '2024-03-01', 910.000,  2250.000, 1.190, 89.80, 4.30, 905.00, 28.50, 7.80, 0, '2024-03-02T00:00:00'),
('EX-073', 'SITE-002', 'PIT-003', 'EQ-004', 'SH-DAY',   '2024-03-05', 1950.000, 3450.000, 0.825, 66.80, 6.40, 1200.00, 23.50, 8.00, 0, '2024-03-06T00:00:00'),
('EX-074', 'SITE-002', 'PIT-004', 'EQ-005', 'SH-DAY',   '2024-03-05', 1720.000, 3100.000, 0.860, 65.00, 7.20, 1420.00, 24.00, 3.50, 0, '2024-03-06T00:00:00'),
('EX-075', 'SITE-003', 'PIT-005', 'EQ-007', 'SH-DAY',   '2024-03-03', 142.000,   86.000, 8.450, 92.80, 2.50, 232.00, 52.00, 7.50, 0, '2024-03-04T00:00:00'),
('EX-076', 'SITE-003', 'PIT-005', 'EQ-008', 'SH-SWING', '2024-03-03', 158.000,   94.000, 8.050, 91.20, 2.80, 298.00, 47.50, 8.00, 0, '2024-03-04T00:00:00'),
('EX-077', 'SITE-001', 'PIT-002', 'EQ-003', 'SH-DAY',   '2024-03-08', 540.000,  1850.000, 1.630, 83.20, 5.10, 530.00, 40.00, 6.50, 0, '2024-03-09T00:00:00'),
('EX-078', 'SITE-002', 'PIT-003', 'EQ-006', 'SH-NIGHT', '2024-03-08', 880.000,  1650.000, 0.925, 63.50, 5.80, 395.00, 33.00, 6.50, 1, '2024-03-09T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only process new extractions using INCREMENTAL_FILTER
-- =============================================================================

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
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.extractions_enriched, extraction_id, extraction_date, 3)}}
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

-- Refresh gold fact incrementally
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
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_extraction, extraction_key, extraction_date, 3)}}
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
-- Verify incremental processing
-- =============================================================================

-- Silver should now have 70 + 8 = 78 rows
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.extractions_enriched;
-- Verify gold fact updated
ASSERT ROW_COUNT = 78
SELECT COUNT(*) AS gold_total FROM {{zone_prefix}}.gold.fact_extraction;
-- Verify March records appeared
ASSERT VALUE gold_total >= 78
SELECT COUNT(*) AS march_extractions
FROM {{zone_prefix}}.silver.extractions_enriched
WHERE extraction_date >= '2024-03-01';

ASSERT VALUE march_extractions = 8
SELECT 'march_extractions check passed' AS march_extractions_status;

