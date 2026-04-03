-- =============================================================================
-- Bronze: Transport Shipments
-- =============================================================================
-- Raw TMS tracking events for 25 shipments. Each shipment progresses through
-- statuses: dispatched -> in_transit -> (customs_hold?) -> delivered | exception.
-- 35 total tracking event rows.
-- =============================================================================

CREATE TABLE IF NOT EXISTS {{zone_prefix}}_bronze.raw.transport_shipments (
  event_id        STRING,
  shipment_id     STRING,
  carrier_id      STRING,
  origin_wh       STRING,
  destination     STRING,
  event_type      STRING,
  event_ts        TIMESTAMP,
  latitude        DECIMAL(9,6),
  longitude       DECIMAL(9,6),
  notes           STRING,
  ingested_at     TIMESTAMP
);

INSERT INTO {{zone_prefix}}_bronze.raw.transport_shipments VALUES
  -- SH-001: W001 -> Store ST01 (delivered on time)
  ('TE-001', 'SH-001', 'CAR-A', 'W001', 'ST01', 'dispatched',  '2026-03-08 14:30:00', 40.7128, -74.0060, 'Loaded 120 units SKU-001',   '2026-03-08 15:00:00'),
  ('TE-002', 'SH-001', 'CAR-A', 'W001', 'ST01', 'in_transit',  '2026-03-09 06:00:00', 39.9526, -75.1652, 'En route Philadelphia',      '2026-03-09 06:30:00'),
  ('TE-003', 'SH-001', 'CAR-A', 'W001', 'ST01', 'delivered',   '2026-03-09 14:00:00', 38.9072, -77.0369, 'Delivered to store dock',     '2026-03-09 14:30:00'),

  -- SH-003: W002 -> Store ST03 (delivered)
  ('TE-004', 'SH-003', 'CAR-B', 'W002', 'ST03', 'dispatched',  '2026-03-14 14:30:00', 34.0522, -118.2437, 'Loaded 150 units SKU-004',  '2026-03-14 15:00:00'),
  ('TE-005', 'SH-003', 'CAR-B', 'W002', 'ST03', 'delivered',   '2026-03-15 10:00:00', 33.4484, -112.0740, 'Delivered Phoenix store',    '2026-03-15 10:30:00'),

  -- SH-005: W001 -> Store ST02 (delivered)
  ('TE-006', 'SH-005', 'CAR-A', 'W001', 'ST02', 'dispatched',  '2026-03-11 13:30:00', 40.7128, -74.0060, 'Loaded 80 units SKU-003',    '2026-03-11 14:00:00'),
  ('TE-007', 'SH-005', 'CAR-A', 'W001', 'ST02', 'delivered',   '2026-03-12 11:00:00', 42.3601, -71.0589, 'Delivered Boston store',      '2026-03-12 11:30:00'),

  -- SH-008: W001 -> Store ST04 (delivered)
  ('TE-008', 'SH-008', 'CAR-C', 'W001', 'ST04', 'dispatched',  '2026-03-13 15:30:00', 40.7128, -74.0060, 'Loaded 200 units SKU-002',   '2026-03-13 16:00:00'),
  ('TE-009', 'SH-008', 'CAR-C', 'W001', 'ST04', 'in_transit',  '2026-03-14 08:00:00', 39.2904, -76.6122, 'En route Baltimore',          '2026-03-14 08:30:00'),
  ('TE-010', 'SH-008', 'CAR-C', 'W001', 'ST04', 'delivered',   '2026-03-14 16:00:00', 36.8529, -75.9780, 'Delivered Norfolk store',     '2026-03-14 16:30:00'),

  -- SH-009: W002 -> Store ST05 (delivered)
  ('TE-011', 'SH-009', 'CAR-B', 'W002', 'ST05', 'dispatched',  '2026-03-15 10:30:00', 34.0522, -118.2437, 'Loaded 100 units SKU-005',  '2026-03-15 11:00:00'),
  ('TE-012', 'SH-009', 'CAR-B', 'W002', 'ST05', 'delivered',   '2026-03-16 09:00:00', 37.7749, -122.4194, 'Delivered SF store',          '2026-03-16 09:30:00'),

  -- SH-010: W003 -> Store ST06 (delivered)
  ('TE-013', 'SH-010', 'CAR-D', 'W003', 'ST06', 'dispatched',  '2026-03-22 14:30:00', 41.8781, -87.6298, 'Loaded 75 units SKU-009',    '2026-03-22 15:00:00'),
  ('TE-014', 'SH-010', 'CAR-D', 'W003', 'ST06', 'delivered',   '2026-03-23 12:00:00', 39.7684, -86.1581, 'Delivered Indianapolis',      '2026-03-23 12:30:00'),

  -- SH-012: W001 -> Store ST01 (delivered)
  ('TE-015', 'SH-012', 'CAR-A', 'W001', 'ST01', 'dispatched',  '2026-03-09 09:00:00', 40.7128, -74.0060, 'Loaded 50 units SKU-007',    '2026-03-09 09:30:00'),
  ('TE-016', 'SH-012', 'CAR-A', 'W001', 'ST01', 'delivered',   '2026-03-09 18:00:00', 38.9072, -77.0369, 'Delivered DC store',           '2026-03-09 18:30:00'),

  -- SH-013: W002 -> Store ST03 (in transit — customs hold)
  ('TE-017', 'SH-013', 'CAR-B', 'W002', 'ST03', 'dispatched',  '2026-03-17 13:30:00', 34.0522, -118.2437, 'Loaded 180 units SKU-008',  '2026-03-17 14:00:00'),
  ('TE-018', 'SH-013', 'CAR-B', 'W002', 'ST03', 'customs_hold','2026-03-18 10:00:00', 32.7157, -117.1611, 'Held at San Diego customs',  '2026-03-18 10:30:00'),

  -- SH-014: W003 -> Store ST07 (delivered)
  ('TE-019', 'SH-014', 'CAR-D', 'W003', 'ST07', 'dispatched',  '2026-03-12 11:00:00', 41.8781, -87.6298, 'Loaded 40 units SKU-013',    '2026-03-12 11:30:00'),
  ('TE-020', 'SH-014', 'CAR-D', 'W003', 'ST07', 'delivered',   '2026-03-13 08:00:00', 44.9778, -93.2650, 'Delivered Minneapolis',       '2026-03-13 08:30:00'),

  -- SH-016: W002 -> Store ST05 (delivered)
  ('TE-021', 'SH-016', 'CAR-B', 'W002', 'ST05', 'dispatched',  '2026-03-19 12:00:00', 34.0522, -118.2437, 'Loaded 90 units SKU-006',   '2026-03-19 12:30:00'),
  ('TE-022', 'SH-016', 'CAR-B', 'W002', 'ST05', 'delivered',   '2026-03-20 10:00:00', 37.7749, -122.4194, 'Delivered SF store',          '2026-03-20 10:30:00'),

  -- SH-017: W003 -> Store ST06 (delivered)
  ('TE-023', 'SH-017', 'CAR-D', 'W003', 'ST06', 'dispatched',  '2026-03-16 13:30:00', 41.8781, -87.6298, 'Loaded 60 units SKU-007',    '2026-03-16 14:00:00'),
  ('TE-024', 'SH-017', 'CAR-D', 'W003', 'ST06', 'delivered',   '2026-03-17 11:00:00', 39.7684, -86.1581, 'Delivered Indianapolis',      '2026-03-17 11:30:00'),

  -- SH-018: W004 -> Store ST08 (in transit)
  ('TE-025', 'SH-018', 'CAR-E', 'W004', 'ST08', 'dispatched',  '2026-03-16 10:00:00', 29.7604, -95.3698, 'Loaded 65 units SKU-006',    '2026-03-16 10:30:00'),
  ('TE-026', 'SH-018', 'CAR-E', 'W004', 'ST08', 'in_transit',  '2026-03-17 06:00:00', 30.2672, -97.7431, 'En route Austin',              '2026-03-17 06:30:00'),
  ('TE-027', 'SH-018', 'CAR-E', 'W004', 'ST08', 'delivered',   '2026-03-17 14:00:00', 32.7767, -96.7970, 'Delivered Dallas store',       '2026-03-17 14:30:00'),

  -- SH-019: W003 -> Store ST07 (delivered)
  ('TE-028', 'SH-019', 'CAR-D', 'W003', 'ST07', 'dispatched',  '2026-03-19 10:00:00', 41.8781, -87.6298, 'Loaded 45 units SKU-014',    '2026-03-19 10:30:00'),
  ('TE-029', 'SH-019', 'CAR-D', 'W003', 'ST07', 'delivered',   '2026-03-20 08:00:00', 44.9778, -93.2650, 'Delivered Minneapolis',       '2026-03-20 08:30:00'),

  -- SH-021: W003 -> Store ST06 (in transit)
  ('TE-030', 'SH-021', 'CAR-D', 'W003', 'ST06', 'dispatched',  '2026-03-23 11:00:00', 41.8781, -87.6298, 'Loaded 100 units SKU-010',   '2026-03-23 11:30:00'),
  ('TE-031', 'SH-021', 'CAR-D', 'W003', 'ST06', 'in_transit',  '2026-03-24 07:00:00', 40.4406, -86.1336, 'En route Lafayette',           '2026-03-24 07:30:00'),

  -- SH-022: W004 -> Store ST08 (delivered)
  ('TE-032', 'SH-022', 'CAR-E', 'W004', 'ST08', 'dispatched',  '2026-03-23 15:30:00', 29.7604, -95.3698, 'Loaded 110 units SKU-011',   '2026-03-23 16:00:00'),
  ('TE-033', 'SH-022', 'CAR-E', 'W004', 'ST08', 'delivered',   '2026-03-24 12:00:00', 32.7767, -96.7970, 'Delivered Dallas store',       '2026-03-24 12:30:00'),

  -- SH-024: W004 -> Store ST08 (exception — damaged)
  ('TE-034', 'SH-024', 'CAR-E', 'W004', 'ST08', 'dispatched',  '2026-03-25 08:30:00', 29.7604, -95.3698, 'Loaded 80 units SKU-015',    '2026-03-25 09:00:00'),
  ('TE-035', 'SH-024', 'CAR-E', 'W004', 'ST08', 'exception',   '2026-03-25 16:00:00', 31.5493, -97.1467, 'Partial damage in transit',    '2026-03-25 16:30:00');
