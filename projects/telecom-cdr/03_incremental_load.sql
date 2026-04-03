-- =============================================================================
-- Telecom CDR Pipeline: Incremental Load with Schema Evolution
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "cdr_id > 'CDR-00065' AND start_time > '2024-06-02T23:00:00'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.cdr_enriched, cdr_id, start_time, 1)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.cdr_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_cdr
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.cdr_enriched, cdr_id, start_time, 1)}};

-- Show current watermark
SELECT MAX(enriched_at) AS current_watermark
FROM {{zone_prefix}}.silver.cdr_enriched;

SELECT 'silver.cdr_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.cdr_enriched
UNION ALL
SELECT 'gold.fact_calls', COUNT(*)
FROM {{zone_prefix}}.gold.fact_calls;

-- =============================================================================
-- Schema Evolution: Add roaming_flag to bronze raw_cdr table
-- =============================================================================
-- In production, new CDR feeds include a roaming indicator. The bronze schema
-- evolves to accept this new column. Existing rows default to NULL/false.

ALTER TABLE {{zone_prefix}}.bronze.raw_cdr ADD COLUMN roaming_flag BOOLEAN DEFAULT false;

-- =============================================================================
-- Insert 8 new CDR records (Day 4 - June 4, includes roaming calls)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_cdr (cdr_id, caller_number, callee_number, tower_id, start_time, end_time, call_type, data_usage_mb, revenue, ingested_at, roaming_flag) VALUES
('CDR-00066', '+1-212-555-1001', '+1-415-555-2002', 'TWR-NE-01', '2024-06-04T08:00:00', '2024-06-04T08:18:30', 'voice',    0.00, 1.85, '2024-06-04T12:00:00', false),
('CDR-00067', '+1-415-555-2002', '+1-305-555-9009', 'TWR-W-01',  '2024-06-04T09:00:00', NULL,                  'data',   520.00, 5.20, '2024-06-04T12:00:00', true),
('CDR-00068', '+1-305-555-9009', '+1-212-555-1001', 'TWR-S-01',  '2024-06-04T09:30:00', '2024-06-04T09:30:06', 'voice',    0.00, 0.01, '2024-06-04T12:00:00', false),
('CDR-00069', '+1-312-555-3003', '+1-404-555-1011', 'TWR-C-01',  '2024-06-04T10:00:00', '2024-06-04T10:22:15', 'voice',    0.00, 2.22, '2024-06-04T12:00:00', false),
('CDR-00070', '+1-206-555-4004', '+1-617-555-5005', 'TWR-W-02',  '2024-06-04T10:30:00', '2024-06-04T10:45:50', 'voice',    0.00, 1.58, '2024-06-04T12:00:00', true),
('CDR-00071', '+1-404-555-1011', '+1-503-555-8008', 'TWR-S-02',  '2024-06-04T11:00:00', NULL,                  'data',   340.00, 3.40, '2024-06-04T12:00:00', false),
('CDR-00072', '+1-512-555-6006', '+1-303-555-7007', 'TWR-C-02',  '2024-06-04T12:00:00', '2024-06-04T12:12:40', 'voice',    0.00, 1.27, '2024-06-04T12:00:00', false),
('CDR-00073', '+1-602-555-0010', '+1-214-555-2012', 'TWR-W-02',  '2024-06-04T13:00:00', '2024-06-04T13:00:04', 'voice',    0.00, 0.01, '2024-06-04T18:00:00', true);

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only new CDRs
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.cdr_enriched AS tgt
USING (
    WITH subscriber_lookup AS (
        SELECT subscriber_id, phone_number
        FROM {{zone_prefix}}.bronze.raw_subscribers
    )
    SELECT
        c.cdr_id,
        caller_sub.subscriber_id AS caller_id,
        callee_sub.subscriber_id AS callee_id,
        c.caller_number,
        c.callee_number,
        c.tower_id,
        t.city         AS tower_city,
        t.region       AS tower_region,
        c.start_time,
        c.end_time,
        CASE
            WHEN c.call_type = 'voice' AND c.end_time IS NOT NULL
            THEN CAST(EXTRACT(EPOCH FROM (c.end_time - c.start_time)) AS INT)
            WHEN c.call_type = 'sms' THEN 0
            ELSE NULL
        END AS duration_sec,
        c.call_type,
        c.data_usage_mb,
        COALESCE(c.roaming_flag, false) AS roaming_flag,
        CASE
            WHEN c.call_type = 'voice' AND c.end_time IS NOT NULL
                AND CAST(EXTRACT(EPOCH FROM (c.end_time - c.start_time)) AS INT) < 10
            THEN true ELSE false
        END AS drop_flag,
        c.revenue,
        c.ingested_at
    FROM {{zone_prefix}}.bronze.raw_cdr c
    LEFT JOIN subscriber_lookup caller_sub ON c.caller_number = caller_sub.phone_number
    LEFT JOIN subscriber_lookup callee_sub ON c.callee_number = callee_sub.phone_number
    LEFT JOIN {{zone_prefix}}.bronze.raw_cell_towers t ON c.tower_id = t.tower_id
    WHERE c.ingested_at > (SELECT COALESCE(MAX(enriched_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.cdr_enriched)
) AS src
ON tgt.cdr_id = src.cdr_id
WHEN NOT MATCHED THEN INSERT (
    cdr_id, caller_id, callee_id, caller_number, callee_number, tower_id,
    tower_city, tower_region, start_time, end_time, duration_sec, call_type,
    data_usage_mb, roaming_flag, drop_flag, revenue, enriched_at
) VALUES (
    src.cdr_id, src.caller_id, src.callee_id, src.caller_number, src.callee_number,
    src.tower_id, src.tower_city, src.tower_region, src.start_time, src.end_time,
    src.duration_sec, src.call_type, src.data_usage_mb, src.roaming_flag,
    src.drop_flag, src.revenue, src.ingested_at
);

-- =============================================================================
-- Verify incremental results
-- =============================================================================

-- Silver should now have 65 + 8 = 73 enriched CDRs
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.cdr_enriched;
-- Verify roaming records captured via schema evolution
ASSERT ROW_COUNT = 73
SELECT COUNT(*) AS roaming_count
FROM {{zone_prefix}}.silver.cdr_enriched
WHERE roaming_flag = true;

-- Verify new watermark advanced
ASSERT VALUE roaming_count = 3
SELECT MAX(enriched_at) AS new_watermark
FROM {{zone_prefix}}.silver.cdr_enriched;
