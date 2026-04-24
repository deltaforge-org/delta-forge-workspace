-- =============================================================================
-- Telecom CDR Pipeline: Incremental Load with Schema Evolution
-- =============================================================================
-- Demonstrates incremental processing of new v3 CDR records (5G era) into
-- the unified silver layer. Uses INCREMENTAL_FILTER macro for watermark-based
-- filtering and RESTORE for point-in-time recovery.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (DeltaForge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "call_id > 'V3-020' AND start_time > '2024-09-19'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(telco.silver.cdr_unified, call_id, start_time, 1)}};

-- Show current watermark
SELECT MAX(unified_at) AS current_watermark
FROM telco.silver.cdr_unified;

SELECT
  'silver.cdr_unified' AS table_name, COUNT(*) AS row_count
FROM telco.silver.cdr_unified
UNION ALL
SELECT 'gold.fact_calls', COUNT(*)
FROM telco.gold.fact_calls
UNION ALL
SELECT 'gold.kpi_churn_risk', COUNT(*)
FROM telco.gold.kpi_churn_risk;

-- =============================================================================
-- Insert 8 new v3 CDR records (October 2024: latest 5G batch)
-- =============================================================================
-- Includes 1 roaming event, 1 dropped call, 1 5G handover, long data session.

MERGE INTO telco.bronze.raw_cdr_v3 AS tgt
USING (
  SELECT * FROM (VALUES
    ('V3-021', '+1-212-555-1001', '+1-415-555-2002', '2024-10-01T08:00:00', '2024-10-01T08:22:45', 'TWR-NE-01', 1365, 'voice', 0.00,   0, false, '5G',  0, '2024-10-01T12:00:00'),
    ('V3-022', '+1-415-555-2002', '+1-305-555-9009', '2024-10-01T09:00:00', NULL,                  'TWR-W-01',  NULL, 'data',  680.00, 0, true,  '5G',  0, '2024-10-01T12:00:00'),
    ('V3-023', '+1-312-555-3003', '+1-512-555-6006', '2024-10-05T10:00:00', '2024-10-05T10:18:30', 'TWR-C-01',  1110, 'voice', 0.00,   0, false, '5G',  2, '2024-10-05T12:00:00'),
    ('V3-024', '+1-206-555-4004', '+1-303-555-7007', '2024-10-05T11:30:00', '2024-10-05T11:30:05', 'TWR-W-02',  5,    'voice', 0.00,   0, false, '4G',  0, '2024-10-05T12:00:00'),
    ('V3-025', '+1-720-555-3013', '+1-813-555-4014', '2024-10-10T14:00:00', '2024-10-10T14:08:15', 'TWR-C-01',  495,  'voice', 0.00,   0, false, '5G',  0, '2024-10-10T18:00:00'),
    ('V3-026', '+1-404-555-1011', '+1-602-555-0010', '2024-10-10T15:00:00', NULL,                  'TWR-S-02',  NULL, 'data',  245.00, 0, false, '4G',  0, '2024-10-10T18:00:00'),
    ('V3-027', '+1-813-555-4014', '+1-901-555-5015', '2024-10-15T09:00:00', NULL,                  'TWR-S-03',  0,    'sms',   0.00,   6, false, '5G',  0, '2024-10-15T12:00:00'),
    ('V3-028', '+1-617-555-5005', '+1-214-555-2012', '2024-10-15T10:30:00', '2024-10-15T10:42:10', 'TWR-NE-02', 730,  'voice', 0.00,   0, false, '4G',  0, '2024-10-15T12:00:00')
  ) AS v(call_id, caller, callee, start_time, end_time, tower_id, duration_sec,
         call_type, data_usage_mb, sms_count, roaming_flag, network_type, handover_count, ingested_at)
) AS src
ON tgt.call_id = src.call_id
WHEN MATCHED THEN UPDATE SET
    tgt.caller         = src.caller,
    tgt.callee         = src.callee,
    tgt.start_time     = src.start_time,
    tgt.end_time       = src.end_time,
    tgt.tower_id       = src.tower_id,
    tgt.duration_sec   = src.duration_sec,
    tgt.call_type      = src.call_type,
    tgt.data_usage_mb  = src.data_usage_mb,
    tgt.sms_count      = src.sms_count,
    tgt.roaming_flag   = src.roaming_flag,
    tgt.network_type   = src.network_type,
    tgt.handover_count = src.handover_count,
    tgt.ingested_at    = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    call_id, caller, callee, start_time, end_time, tower_id, duration_sec,
    call_type, data_usage_mb, sms_count, roaming_flag, network_type, handover_count, ingested_at
) VALUES (
    src.call_id, src.caller, src.callee, src.start_time, src.end_time, src.tower_id, src.duration_sec,
    src.call_type, src.data_usage_mb, src.sms_count, src.roaming_flag, src.network_type, src.handover_count, src.ingested_at
);

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS new_v3_count
FROM telco.bronze.raw_cdr_v3
WHERE ingested_at >= '2024-10-01T00:00:00';


-- =============================================================================
-- Incremental MERGE: only new v3 CDRs into unified silver
-- Uses INCREMENTAL_FILTER for watermark-based selection
-- =============================================================================

MERGE INTO telco.silver.cdr_unified AS tgt
USING (
  WITH subscriber_lookup AS (
      SELECT subscriber_id, phone_number
      FROM telco.bronze.raw_subscribers
  )
  SELECT
      v.call_id,
      v.caller,
      v.callee,
      caller_sub.subscriber_id AS caller_id,
      callee_sub.subscriber_id AS callee_id,
      v.tower_id,
      t.city          AS tower_city,
      t.region        AS tower_region,
      v.start_time,
      v.end_time,
      v.duration_sec,
      v.call_type,
      COALESCE(v.data_usage_mb, 0.00) AS data_usage_mb,
      COALESCE(v.sms_count, 0)        AS sms_count,
      COALESCE(v.roaming_flag, false)  AS roaming_flag,
      v.network_type,
      COALESCE(v.handover_count, 0)    AS handover_count,
      CASE
          WHEN v.call_type = 'voice' AND v.duration_sec IS NOT NULL AND v.duration_sec < 10
          THEN true ELSE false
      END AS drop_flag,
      CASE
          WHEN v.call_type = 'voice' THEN CAST(COALESCE(v.duration_sec, 0) * 0.01 AS DECIMAL(8,2))
          WHEN v.call_type = 'data'  THEN CAST(COALESCE(v.data_usage_mb, 0) * 0.01 AS DECIMAL(8,2))
          WHEN v.call_type = 'sms'   THEN CAST(COALESCE(v.sms_count, 0) * 0.05 AS DECIMAL(8,2))
          ELSE 0.00
      END AS revenue,
      3               AS schema_version,
      v.ingested_at   AS unified_at
  FROM telco.bronze.raw_cdr_v3 v
  LEFT JOIN subscriber_lookup caller_sub ON v.caller = caller_sub.phone_number
  LEFT JOIN subscriber_lookup callee_sub ON v.callee = callee_sub.phone_number
  LEFT JOIN telco.bronze.raw_cell_towers t ON v.tower_id = t.tower_id
  WHERE {{INCREMENTAL_FILTER(telco.silver.cdr_unified, call_id, start_time, 1)}}
) AS src
ON tgt.call_id = src.call_id
WHEN NOT MATCHED THEN INSERT (
  call_id, caller, callee, caller_id, callee_id, tower_id, tower_city, tower_region,
  start_time, end_time, duration_sec, call_type, data_usage_mb, sms_count,
  roaming_flag, network_type, handover_count, drop_flag, revenue, schema_version, unified_at
) VALUES (
  src.call_id, src.caller, src.callee, src.caller_id, src.callee_id, src.tower_id,
  src.tower_city, src.tower_region, src.start_time, src.end_time, src.duration_sec,
  src.call_type, src.data_usage_mb, src.sms_count, src.roaming_flag, src.network_type,
  src.handover_count, src.drop_flag, src.revenue, src.schema_version, src.unified_at
);

-- =============================================================================
-- Verify incremental results
-- =============================================================================

-- Unified should now have 70 + 8 = 78 records
SELECT COUNT(*) AS unified_total FROM telco.silver.cdr_unified;

ASSERT VALUE unified_total = 78
SELECT COUNT(*) AS v3_total
FROM telco.silver.cdr_unified
WHERE schema_version = 3;

-- Verify roaming records captured (V3-002, V3-005, V3-008, V3-022 = 4 total)
ASSERT VALUE v3_total = 28
SELECT COUNT(*) AS roaming_count
FROM telco.silver.cdr_unified
WHERE roaming_flag = true;

ASSERT VALUE roaming_count = 4
SELECT COUNT(*) AS dropped_count
FROM telco.silver.cdr_unified
WHERE drop_flag = true;

-- Verify the new dropped call from incremental batch (V3-024, duration=5)
ASSERT VALUE dropped_count >= 5
SELECT call_id, duration_sec, drop_flag
FROM telco.silver.cdr_unified
WHERE call_id = 'V3-024';

ASSERT VALUE drop_flag = true
SELECT MAX(unified_at) AS new_watermark
FROM telco.silver.cdr_unified;

-- =============================================================================
-- RESTORE demonstration: point-in-time recovery
-- =============================================================================
-- Show version history before restore
SELECT COUNT(*) AS pre_restore_count
FROM telco.silver.cdr_unified;

RESTORE telco.silver.cdr_unified TO VERSION 0;

-- After restore, re-run the full load to get back to current state
-- (in production, you would restore to a specific version after investigation)
