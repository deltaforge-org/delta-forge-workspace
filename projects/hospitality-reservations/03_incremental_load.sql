-- =============================================================================
-- Hospitality Reservations Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using INCREMENTAL_FILTER macro.

-- Show current watermark
SELECT MAX(booking_id) AS max_booking_id, MAX(enriched_at) AS latest_enriched
FROM {{zone_prefix}}.silver.bookings_deduped;

-- Current row counts
SELECT 'silver.bookings_deduped' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.bookings_deduped
UNION ALL
SELECT 'gold.fact_reservations', COUNT(*)
FROM {{zone_prefix}}.gold.fact_reservations;

-- =============================================================================
-- Insert 8 new bookings (May 2024 incremental batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_bookings VALUES
('BK-064', 'PROP-001', 'RT-DLX', 'G-001', 'CH-DIR',  '2024-04-15', '2024-05-01', '2024-05-04', 285.00,  855.00, 'completed',  false, '2024-04-16T00:00:00'),
('BK-065', 'PROP-002', 'RT-STD', 'G-005', 'CH-CORP', '2024-04-20', '2024-05-03', '2024-05-06', 195.00,  585.00, 'completed',  false, '2024-04-21T00:00:00'),
('BK-066', 'PROP-003', 'RT-STE', 'G-008', 'CH-DIR',  '2024-04-25', '2024-05-05', '2024-05-08', 385.00, 1155.00, 'completed',  false, '2024-04-26T00:00:00'),
('BK-067', 'PROP-004', 'RT-FAM', 'G-013', 'CH-OTA',  '2024-05-01', '2024-05-08', '2024-05-11', 265.00,  795.00, 'completed',  false, '2024-05-02T00:00:00'),
('BK-068', 'PROP-001', 'RT-PNT', 'G-011', 'CH-DIR',  '2024-04-01', '2024-05-10', '2024-05-14', 899.00, 3596.00, 'completed',  false, '2024-04-02T00:00:00'),
('BK-069', 'PROP-002', 'RT-DLX', 'G-010', 'CH-DIR',  '2024-05-02', '2024-05-12', '2024-05-15', 275.00,  825.00, 'cancelled',  false, '2024-05-03T00:00:00'),
('BK-070', 'PROP-003', 'RT-STD', 'G-014', 'CH-TA',   '2024-05-05', '2024-05-15', '2024-05-17', 189.00,  378.00, 'completed',  false, '2024-05-06T00:00:00'),
('BK-071', 'PROP-004', 'RT-STD', 'G-006', 'CH-OTA',  '2024-05-08', '2024-05-18', '2024-05-20', 155.00,  310.00, 'completed',  false, '2024-05-09T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE to silver using INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.bookings_deduped AS tgt
USING (
    SELECT
        b.booking_id,
        b.property_id,
        b.room_type_id,
        b.guest_id,
        b.channel_id,
        b.booking_date,
        b.check_in_date,
        b.check_out_date,
        CAST(EXTRACT(EPOCH FROM (b.check_out_date - b.check_in_date)) / 86400 AS INT) AS nights,
        b.room_rate,
        b.total_amount,
        b.status,
        CASE WHEN b.status = 'cancelled' THEN true ELSE false END AS is_cancelled,
        CAST(EXTRACT(EPOCH FROM (b.check_in_date - b.booking_date)) / 86400 AS INT) AS booking_lead_days,
        g.loyalty_tier,
        b.ingested_at
    FROM {{zone_prefix}}.bronze.raw_bookings b
    LEFT JOIN {{zone_prefix}}.bronze.raw_guests g ON b.guest_id = g.guest_id
    WHERE b.is_duplicate = false
      AND {{INCREMENTAL_FILTER({{zone_prefix}}.silver.bookings_deduped, booking_id, check_in_date, 7)}}
) AS src
ON tgt.booking_id = src.booking_id
WHEN MATCHED AND src.ingested_at > tgt.enriched_at THEN UPDATE SET
    tgt.status       = src.status,
    tgt.is_cancelled = src.is_cancelled,
    tgt.total_amount = src.total_amount,
    tgt.enriched_at  = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    booking_id, property_id, room_type_id, guest_id, channel_id,
    booking_date, check_in_date, check_out_date, nights, room_rate,
    total_amount, status, is_cancelled, booking_lead_days, loyalty_tier, enriched_at
) VALUES (
    src.booking_id, src.property_id, src.room_type_id, src.guest_id, src.channel_id,
    src.booking_date, src.check_in_date, src.check_out_date, src.nights, src.room_rate,
    src.total_amount, src.status, src.is_cancelled, src.booking_lead_days,
    src.loyalty_tier, src.ingested_at
);

-- =============================================================================
-- Incremental gold fact refresh
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_reservations AS tgt
USING (
    SELECT
        b.booking_id AS reservation_key, b.property_id AS property_key,
        b.room_type_id AS room_type_key, b.guest_id AS guest_key,
        b.channel_id AS channel_key, b.check_in_date, b.check_out_date,
        b.nights, b.room_rate, b.total_amount, b.status,
        b.booking_lead_days, b.is_cancelled
    FROM {{zone_prefix}}.silver.bookings_deduped b
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_reservations, reservation_key, check_in_date, 7)}}
) AS src
ON tgt.reservation_key = src.reservation_key
WHEN MATCHED THEN UPDATE SET
    tgt.status       = src.status,
    tgt.is_cancelled = src.is_cancelled,
    tgt.total_amount = src.total_amount
WHEN NOT MATCHED THEN INSERT (
    reservation_key, property_key, room_type_key, guest_key, channel_key,
    check_in_date, check_out_date, nights, room_rate, total_amount,
    status, booking_lead_days, is_cancelled
) VALUES (
    src.reservation_key, src.property_key, src.room_type_key, src.guest_key,
    src.channel_key, src.check_in_date, src.check_out_date, src.nights,
    src.room_rate, src.total_amount, src.status, src.booking_lead_days, src.is_cancelled
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Silver should now have original deduped count + 8 new
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.bookings_deduped;
-- Verify May bookings exist
ASSERT VALUE silver_total >= 70
SELECT COUNT(*) AS may_bookings
FROM {{zone_prefix}}.gold.fact_reservations
WHERE check_in_date >= '2024-05-01';

-- Verify cancelled booking soft-deleted (BK-069)
ASSERT VALUE may_bookings >= 6
SELECT is_cancelled
FROM {{zone_prefix}}.gold.fact_reservations
WHERE reservation_key = 'BK-069';

ASSERT VALUE is_cancelled = true
SELECT 'is_cancelled check passed' AS is_cancelled_status;

