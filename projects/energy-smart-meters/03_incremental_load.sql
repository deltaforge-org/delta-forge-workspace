-- =============================================================================
-- Energy Smart Meters Pipeline: Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "reading_id > 'RD-072' AND reading_date > '2024-05-28'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.readings_costed, reading_id, reading_date, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.readings_costed
-- SELECT * FROM {{zone_prefix}}.bronze.raw_readings
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.readings_costed, reading_id, reading_date, 3)}};

-- Show current watermark
SELECT MAX(costed_at) AS current_watermark
FROM {{zone_prefix}}.silver.readings_costed;

SELECT 'silver.readings_costed' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.readings_costed
UNION ALL
SELECT 'gold.fact_meter_readings', COUNT(*)
FROM {{zone_prefix}}.gold.fact_meter_readings;

-- =============================================================================
-- Insert 10 new readings (June 2024 data - second billing month)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_readings VALUES
('RD-073', 'MTR-001', '2024-06-01',  8,  1.30, 0.00, '2024-07-01T00:00:00'),
('RD-074', 'MTR-001', '2024-06-01', 19,  2.25, 0.00, '2024-07-01T00:00:00'),
('RD-075', 'MTR-002', '2024-06-01', 10,  1.15, 2.00, '2024-07-01T00:00:00'),
('RD-076', 'MTR-002', '2024-06-01', 14,  0.85, 2.40, '2024-07-01T00:00:00'),
('RD-077', 'MTR-004', '2024-06-01',  9,  1.00, 2.70, '2024-07-01T00:00:00'),
('RD-078', 'MTR-006', '2024-06-01', 11, 12.50, 6.20, '2024-07-01T00:00:00'),
('RD-079', 'MTR-007', '2024-06-01', 19,  2.05, 0.00, '2024-07-01T00:00:00'),
('RD-080', 'MTR-010', '2024-06-01', 12,  0.80, 3.20, '2024-07-01T00:00:00'),
('RD-081', 'MTR-011', '2024-06-01', 10,  1.20, 2.60, '2024-07-01T00:00:00'),
('RD-082', 'MTR-012', '2024-06-01', 15, 20.10, 0.00, '2024-07-01T00:00:00');

ASSERT ROW_COUNT = 10
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only new readings
-- =============================================================================

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
        CASE WHEN r.kwh_consumed <= m.capacity_kw THEN true ELSE false END AS capacity_valid,
        r.ingested_at
    FROM {{zone_prefix}}.bronze.raw_readings r
    JOIN {{zone_prefix}}.bronze.raw_meters m ON r.meter_id = m.meter_id
    JOIN {{zone_prefix}}.bronze.raw_tariffs t ON m.tariff_id = t.tariff_id
    WHERE r.ingested_at > (SELECT COALESCE(MAX(costed_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.readings_costed)
) AS src
ON tgt.reading_id = src.reading_id
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
-- Verify incremental results
-- =============================================================================

-- Silver should now have 72 + 10 = 82 costed readings
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.readings_costed;
-- Verify June data present
ASSERT ROW_COUNT = 82
SELECT COUNT(*) AS june_readings
FROM {{zone_prefix}}.silver.readings_costed
WHERE reading_date >= '2024-06-01';

-- Verify solar offset for MTR-010 (solar meter, generated > consumed at midday)
ASSERT VALUE june_readings = 10
SELECT reading_id, kwh_consumed, kwh_generated, net_kwh, cost
FROM {{zone_prefix}}.silver.readings_costed
WHERE reading_id = 'RD-080';

-- Verify watermark advanced
ASSERT VALUE net_kwh < 0
SELECT MAX(costed_at) AS new_watermark
FROM {{zone_prefix}}.silver.readings_costed;
