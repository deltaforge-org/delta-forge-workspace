-- =============================================================================
-- Logistics Shipments Pipeline: Incremental Load with Schema Evolution
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "shipment_id > 'S021' AND last_event_time > '2024-04-28'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.shipments_deduped, shipment_id, last_event_time, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.shipments_deduped
-- SELECT * FROM {{zone_prefix}}.bronze.raw_events
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.shipments_deduped, shipment_id, last_event_time, 3)}};

-- Show current watermark
SELECT MAX(deduped_at) AS current_watermark
FROM {{zone_prefix}}.silver.shipments_deduped;

SELECT 'silver.shipments_deduped' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.shipments_deduped
UNION ALL
SELECT 'gold.fact_shipments', COUNT(*)
FROM {{zone_prefix}}.gold.fact_shipments;

-- =============================================================================
-- Schema Evolution: Add tracking_url to raw_events
-- =============================================================================

ALTER TABLE {{zone_prefix}}.bronze.raw_events ADD COLUMN tracking_url STRING;

-- =============================================================================
-- Insert 8 new events (new shipments + updates to existing S019)
-- Includes tracking_url for new events (schema evolution)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_events (event_id, shipment_id, customer_id, carrier_id, origin_id, destination_id, event_type, event_timestamp, ship_date, promised_date, delivery_date, weight_kg, volume_m3, cost, revenue, ingested_at, tracking_url) VALUES
-- S019 finally delivered (was exception)
('EVT-069', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'out_for_delivery', '2024-05-01T08:00:00', '2024-04-28', '2024-04-30', NULL,          290.00, 1.3500, 348.00, 480.00, '2024-07-01T00:00:00', 'https://track.example.com/S019'),
('EVT-070', 'S019', 'CUST-007', 'CAR-006', 'LOC-ATL', 'LOC-MIA', 'delivered',         '2024-05-01T15:00:00', '2024-04-28', '2024-04-30', '2024-05-01', 290.00, 1.3500, 348.00, 480.00, '2024-07-01T00:00:00', 'https://track.example.com/S019'),

-- New shipment S022: CHI->LAX, SwiftFreight
('EVT-071', 'S022', 'CUST-003', 'CAR-001', 'LOC-CHI', 'LOC-LAX', 'picked_up',        '2024-05-02T09:00:00', '2024-05-02', '2024-05-05', NULL,          350.00, 1.6000, 647.50, 870.00, '2024-07-01T00:00:00', 'https://track.example.com/S022'),
('EVT-072', 'S022', 'CUST-003', 'CAR-001', 'LOC-CHI', 'LOC-LAX', 'in_transit',        '2024-05-02T20:00:00', '2024-05-02', '2024-05-05', NULL,          350.00, 1.6000, 647.50, 870.00, '2024-07-01T00:00:00', 'https://track.example.com/S022'),
('EVT-073', 'S022', 'CUST-003', 'CAR-001', 'LOC-CHI', 'LOC-LAX', 'delivered',          '2024-05-04T14:00:00', '2024-05-02', '2024-05-05', '2024-05-04', 350.00, 1.6000, 647.50, 870.00, '2024-07-01T00:00:00', 'https://track.example.com/S022'),

-- New shipment S023: MIA->BOS, AeroFreight
('EVT-074', 'S023', 'CUST-004', 'CAR-005', 'LOC-MIA', 'LOC-BOS', 'picked_up',        '2024-05-03T06:00:00', '2024-05-03', '2024-05-04', NULL,          18.00,  0.0800, 99.00,  155.00, '2024-07-01T00:00:00', 'https://track.example.com/S023'),
('EVT-075', 'S023', 'CUST-004', 'CAR-005', 'LOC-MIA', 'LOC-BOS', 'in_transit',        '2024-05-03T12:00:00', '2024-05-03', '2024-05-04', NULL,          18.00,  0.0800, 99.00,  155.00, '2024-07-01T00:00:00', 'https://track.example.com/S023'),
('EVT-076', 'S023', 'CUST-004', 'CAR-005', 'LOC-MIA', 'LOC-BOS', 'delivered',          '2024-05-04T08:00:00', '2024-05-03', '2024-05-04', '2024-05-04', 18.00,  0.0800, 99.00,  155.00, '2024-07-01T00:00:00', 'https://track.example.com/S023');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: idempotent dedup
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.shipments_deduped AS tgt
USING (
    WITH latest_events AS (
        SELECT
            shipment_id, customer_id, carrier_id, origin_id, destination_id,
            ship_date, promised_date,
            MAX(delivery_date) AS delivery_date,
            FIRST_VALUE(event_type) OVER (
                PARTITION BY shipment_id ORDER BY event_timestamp DESC
            ) AS latest_status,
            weight_kg, volume_m3, cost, revenue,
            COUNT(*) OVER (PARTITION BY shipment_id) AS event_count,
            MAX(event_timestamp) OVER (PARTITION BY shipment_id) AS last_event_time,
            ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_timestamp DESC) AS rn
        FROM {{zone_prefix}}.bronze.raw_events
        WHERE ingested_at > (SELECT COALESCE(MAX(deduped_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.shipments_deduped)
        GROUP BY shipment_id, customer_id, carrier_id, origin_id, destination_id,
                 ship_date, promised_date, event_type, event_timestamp, weight_kg,
                 volume_m3, cost, revenue
    )
    SELECT
        shipment_id, customer_id, carrier_id, origin_id, destination_id,
        ship_date, delivery_date, promised_date, latest_status,
        weight_kg, volume_m3, cost, revenue,
        CASE WHEN delivery_date IS NOT NULL THEN CAST(DATEDIFF(delivery_date, ship_date) AS INT) ELSE NULL END AS transit_days,
        CASE WHEN delivery_date IS NOT NULL AND delivery_date <= promised_date THEN true
             WHEN delivery_date IS NOT NULL AND delivery_date > promised_date THEN false
             ELSE NULL END AS on_time_flag,
        event_count, last_event_time
    FROM latest_events WHERE rn = 1
) AS src
ON tgt.shipment_id = src.shipment_id
WHEN MATCHED AND src.last_event_time > tgt.last_event_time THEN UPDATE SET
    tgt.latest_status   = src.latest_status,
    tgt.delivery_date   = src.delivery_date,
    tgt.transit_days    = src.transit_days,
    tgt.on_time_flag    = src.on_time_flag,
    tgt.event_count     = src.event_count,
    tgt.last_event_time = src.last_event_time,
    tgt.deduped_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    shipment_id, customer_id, carrier_id, origin_id, destination_id,
    ship_date, delivery_date, promised_date, latest_status,
    weight_kg, volume_m3, cost, revenue, transit_days, on_time_flag,
    event_count, last_event_time, deduped_at
) VALUES (
    src.shipment_id, src.customer_id, src.carrier_id, src.origin_id,
    src.destination_id, src.ship_date, src.delivery_date, src.promised_date,
    src.latest_status, src.weight_kg, src.volume_m3, src.cost, src.revenue,
    src.transit_days, src.on_time_flag, src.event_count, src.last_event_time,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- Verify incremental results
-- =============================================================================

-- S019 should now be delivered (was exception)
SELECT shipment_id, latest_status, delivery_date, on_time_flag
FROM {{zone_prefix}}.silver.shipments_deduped
WHERE shipment_id = 'S019';

-- Two new shipments added
ASSERT VALUE latest_status = 'delivered'
SELECT COUNT(*) AS total_shipments
FROM {{zone_prefix}}.silver.shipments_deduped;

-- Verify watermark advanced
ASSERT VALUE total_shipments = 23
SELECT MAX(deduped_at) AS new_watermark
FROM {{zone_prefix}}.silver.shipments_deduped;
