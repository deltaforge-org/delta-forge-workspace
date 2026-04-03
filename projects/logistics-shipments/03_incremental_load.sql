-- =============================================================================
-- Logistics Shipments Pipeline: Incremental Load with Schema Evolution
-- =============================================================================
-- Demonstrates:
--   1. INCREMENTAL_FILTER macro for watermark-based filtering
--   2. Schema evolution: ADD COLUMN customs_cleared BOOLEAN
--   3. New events with customs data for existing and new shipments
--   4. Idempotent composite-key MERGE (re-running produces same result)
--   5. Time travel for dispute resolution
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target table and
-- generates a WHERE clause dynamically. This eliminates manual watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: e.g. "shipment_id > 'S025' AND deduped_at > '2024-05-07'"
--         or "1=1" if the target is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(logi.silver.events_deduped, shipment_id, deduped_at, 3)}};

-- Show current watermark state:
SELECT MAX(deduped_at) AS dedup_watermark FROM logi.silver.events_deduped;
SELECT MAX(reconstructed_at) AS timeline_watermark FROM logi.silver.shipment_status;

-- Current counts before incremental:
SELECT 'events_deduped' AS table_name, COUNT(*) AS row_count
FROM logi.silver.events_deduped
UNION ALL
SELECT 'shipment_status', COUNT(*)
FROM logi.silver.shipment_status
UNION ALL
SELECT 'sla_violations', COUNT(*)
FROM logi.silver.sla_violations
UNION ALL
SELECT 'fact_shipments', COUNT(*)
FROM logi.gold.fact_shipments;

-- =============================================================================
-- Schema Evolution: Add customs_cleared to raw_tracking_events
-- =============================================================================
-- In real operations, customs clearance fields are added mid-lifecycle
-- as the company expands to cross-border shipments.

ALTER TABLE logi.bronze.raw_tracking_events ADD COLUMN customs_cleared BOOLEAN;

-- =============================================================================
-- Insert 12 new events: updates to existing shipments + new shipments
-- Includes customs_cleared field (schema evolution in action)
-- =============================================================================

INSERT INTO logi.bronze.raw_tracking_events (
    event_id, shipment_id, customer_id, carrier_id, origin_id, destination_id,
    service_level, event_type, event_timestamp, ship_date, promised_date,
    delivery_date, weight_kg, volume_m3, cost, revenue, ingested_at, customs_cleared
) VALUES
-- S019 finally found and delivered (was lost exception) — dispute resolution via time travel
('EVT-084', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'standard', 'out_for_delivery', '2024-05-08T08:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-07-01T00:00:00', NULL),
('EVT-085', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'standard', 'delivered',        '2024-05-08T15:00:00', '2024-04-28', '2024-04-30', '2024-05-08', 290.00, 1.3500, 348.00, 480.00, '2024-07-01T00:00:00', NULL),

-- S024 damaged shipment resolved — replacement delivered
('EVT-086', 'S024', 'CUST-003', 'CAR-008', 'LOC-SLC', 'LOC-DET', 'economy',  'out_for_delivery', '2024-05-10T09:00:00', '2024-05-03', '2024-05-14', NULL,          510.00, 2.4000, 265.20, 440.00, '2024-07-01T00:00:00', NULL),
('EVT-087', 'S024', 'CUST-003', 'CAR-008', 'LOC-SLC', 'LOC-DET', 'economy',  'delivered',        '2024-05-10T16:00:00', '2024-05-03', '2024-05-14', '2024-05-10', 510.00, 2.4000, 265.20, 440.00, '2024-07-01T00:00:00', NULL),

-- New S026: HOU->LAX, SwiftFreight, with customs clearance
('EVT-088', 'S026', 'CUST-016', 'CAR-001', 'LOC-HOU', 'LOC-LAX', 'standard', 'created',          '2024-05-10T07:00:00', '2024-05-10', '2024-05-14', NULL,          420.00, 2.0000, 777.00, 1050.00, '2024-07-01T00:00:00', NULL),
('EVT-089', 'S026', 'CUST-016', 'CAR-001', 'LOC-HOU', 'LOC-LAX', 'standard', 'in_transit',        '2024-05-10T18:00:00', '2024-05-10', '2024-05-14', NULL,          420.00, 2.0000, 777.00, 1050.00, '2024-07-01T00:00:00', NULL),
('EVT-090', 'S026', 'CUST-016', 'CAR-001', 'LOC-HOU', 'LOC-LAX', 'standard', 'customs_hold',      '2024-05-11T14:00:00', '2024-05-10', '2024-05-14', NULL,          420.00, 2.0000, 777.00, 1050.00, '2024-07-01T00:00:00', false),
('EVT-091', 'S026', 'CUST-016', 'CAR-001', 'LOC-HOU', 'LOC-LAX', 'standard', 'out_for_delivery',  '2024-05-12T08:00:00', '2024-05-10', '2024-05-14', NULL,          420.00, 2.0000, 777.00, 1050.00, '2024-07-01T00:00:00', true),
('EVT-092', 'S026', 'CUST-016', 'CAR-001', 'LOC-HOU', 'LOC-LAX', 'standard', 'delivered',         '2024-05-12T16:00:00', '2024-05-10', '2024-05-14', '2024-05-12', 420.00, 2.0000, 777.00, 1050.00, '2024-07-01T00:00:00', true),

-- New S027: BOS->DET, AeroFreight express, with customs
('EVT-093', 'S027', 'CUST-009', 'CAR-005', 'LOC-BOS', 'LOC-DET', 'express',  'created',          '2024-05-13T06:00:00', '2024-05-13', '2024-05-14', NULL,          18.00,  0.0800, 99.00,  155.00, '2024-07-01T00:00:00', NULL),
('EVT-094', 'S027', 'CUST-009', 'CAR-005', 'LOC-BOS', 'LOC-DET', 'express',  'delivered',         '2024-05-13T22:00:00', '2024-05-13', '2024-05-14', '2024-05-13', 18.00,  0.0800, 99.00,  155.00, '2024-07-01T00:00:00', true);

-- =============================================================================
-- Incremental idempotent composite-key MERGE
-- =============================================================================
-- Uses INCREMENTAL_FILTER to only process new events since last watermark.
-- The composite key (shipment_id + event_type + event_timestamp) ensures
-- duplicates are silently skipped.

MERGE INTO logi.silver.events_deduped AS target
USING (
    SELECT
        event_id, shipment_id, customer_id, carrier_id, origin_id,
        destination_id, service_level, event_type, event_timestamp,
        ship_date, promised_date, delivery_date, weight_kg, volume_m3,
        cost, revenue
    FROM logi.bronze.raw_tracking_events
    WHERE {{INCREMENTAL_FILTER(logi.silver.events_deduped, shipment_id, deduped_at, 3)}}
) AS source
ON  target.shipment_id     = source.shipment_id
AND target.event_type      = source.event_type
AND target.event_timestamp = source.event_timestamp
WHEN NOT MATCHED THEN INSERT (
    event_id, shipment_id, customer_id, carrier_id, origin_id,
    destination_id, service_level, event_type, event_timestamp,
    ship_date, promised_date, delivery_date, weight_kg, volume_m3,
    cost, revenue, deduped_at
) VALUES (
    source.event_id, source.shipment_id, source.customer_id,
    source.carrier_id, source.origin_id, source.destination_id,
    source.service_level, source.event_type, source.event_timestamp,
    source.ship_date, source.promised_date, source.delivery_date,
    source.weight_kg, source.volume_m3, source.cost, source.revenue,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- Incremental timeline reconstruction
-- =============================================================================

MERGE INTO logi.silver.shipment_status AS tgt
USING (
    WITH ordered_events AS (
        SELECT
            shipment_id, customer_id, carrier_id, origin_id, destination_id,
            service_level, event_type, event_timestamp, ship_date, promised_date,
            delivery_date, weight_kg, volume_m3, cost, revenue,
            ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_timestamp DESC) AS rn_desc,
            LAG(event_type) OVER (PARTITION BY shipment_id ORDER BY event_timestamp DESC) AS next_newer_status,
            LAG(event_timestamp) OVER (PARTITION BY shipment_id ORDER BY event_timestamp) AS prev_event_time,
            COUNT(*) OVER (PARTITION BY shipment_id) AS event_count,
            MIN(event_timestamp) OVER (PARTITION BY shipment_id) AS first_event_time,
            MAX(event_timestamp) OVER (PARTITION BY shipment_id) AS last_event_time,
            MAX(delivery_date) OVER (PARTITION BY shipment_id) AS final_delivery_date
        FROM logi.silver.events_deduped
        WHERE shipment_id IN (
            SELECT DISTINCT shipment_id FROM logi.silver.events_deduped
            WHERE deduped_at > (SELECT COALESCE(MAX(reconstructed_at), '1970-01-01T00:00:00') FROM logi.silver.shipment_status)
        )
    )
    SELECT
        shipment_id, customer_id, carrier_id, origin_id, destination_id,
        service_level, ship_date, promised_date,
        final_delivery_date AS delivery_date,
        event_type AS latest_status,
        next_newer_status AS previous_status,
        event_count,
        CAST(DATEDIFF(event_timestamp, prev_event_time) * 24 AS DECIMAL(8,2)) AS hours_in_last_stage,
        CAST(DATEDIFF(last_event_time, first_event_time) * 24 AS DECIMAL(8,2)) AS total_transit_hours,
        CASE WHEN final_delivery_date IS NOT NULL
             THEN CAST(DATEDIFF(final_delivery_date, ship_date) AS INT) ELSE NULL END AS transit_days,
        CASE WHEN final_delivery_date IS NOT NULL AND final_delivery_date <= promised_date THEN true
             WHEN final_delivery_date IS NOT NULL AND final_delivery_date > promised_date THEN false
             ELSE NULL END AS on_time_flag,
        weight_kg, volume_m3, cost, revenue, first_event_time, last_event_time
    FROM ordered_events WHERE rn_desc = 1
) AS src
ON tgt.shipment_id = src.shipment_id
WHEN MATCHED AND src.last_event_time > tgt.last_event_time THEN UPDATE SET
    tgt.latest_status       = src.latest_status,
    tgt.previous_status     = src.previous_status,
    tgt.delivery_date       = src.delivery_date,
    tgt.transit_days        = src.transit_days,
    tgt.on_time_flag        = src.on_time_flag,
    tgt.event_count         = src.event_count,
    tgt.hours_in_last_stage = src.hours_in_last_stage,
    tgt.total_transit_hours = src.total_transit_hours,
    tgt.last_event_time     = src.last_event_time,
    tgt.reconstructed_at    = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    shipment_id, customer_id, carrier_id, origin_id, destination_id,
    service_level, ship_date, promised_date, delivery_date, latest_status,
    previous_status, event_count, hours_in_last_stage, total_transit_hours,
    transit_days, on_time_flag, weight_kg, volume_m3, cost, revenue,
    first_event_time, last_event_time, reconstructed_at
) VALUES (
    src.shipment_id, src.customer_id, src.carrier_id, src.origin_id,
    src.destination_id, src.service_level, src.ship_date, src.promised_date,
    src.delivery_date, src.latest_status, src.previous_status, src.event_count,
    src.hours_in_last_stage, src.total_transit_hours, src.transit_days,
    src.on_time_flag, src.weight_kg, src.volume_m3, src.cost, src.revenue,
    src.first_event_time, src.last_event_time, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Verify incremental results
-- =============================================================================

-- S019 should now be delivered (was lost exception)
ASSERT VALUE latest_status = 'delivered'
SELECT shipment_id, latest_status, delivery_date, on_time_flag, transit_days
FROM logi.silver.shipment_status
WHERE shipment_id = 'S019';

-- S024 should now be delivered (was damaged exception)
ASSERT VALUE latest_status = 'delivered'
SELECT shipment_id, latest_status, delivery_date, on_time_flag
FROM logi.silver.shipment_status
WHERE shipment_id = 'S024';

-- New shipments S026, S027 should exist
ASSERT VALUE total_shipments = 27
SELECT COUNT(*) AS total_shipments
FROM logi.silver.shipment_status;

-- Verify watermark advanced
SELECT MAX(reconstructed_at) AS new_watermark
FROM logi.silver.shipment_status;

-- =============================================================================
-- Time travel: show S019 dispute resolution history via CDF
-- =============================================================================

SELECT shipment_id, event_type, event_timestamp, deduped_at
FROM logi.silver.events_deduped
WHERE shipment_id = 'S019'
ORDER BY event_timestamp;
