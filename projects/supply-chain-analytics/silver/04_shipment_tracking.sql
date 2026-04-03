-- =============================================================================
-- Silver: Shipment Tracking
-- =============================================================================
-- Idempotent deduplication of transport events. Derives the latest status for
-- each shipment and calculates transit duration. Uses ROW_NUMBER to pick the
-- most recent event per shipment, then MERGEs into the cleansed table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS sc.silver.shipment_tracking (
  shipment_id       STRING,
  carrier_id        STRING,
  origin_wh         STRING,
  destination       STRING,
  current_status    STRING,
  dispatched_at     TIMESTAMP,
  delivered_at      TIMESTAMP,
  transit_hours     DECIMAL(8,2),
  total_events      INT,
  has_exception     BOOLEAN,
  has_customs_hold  BOOLEAN,
  last_latitude     DECIMAL(9,6),
  last_longitude    DECIMAL(9,6),
  updated_at        TIMESTAMP
);

MERGE INTO sc.silver.shipment_tracking AS tgt
USING (
  WITH ranked_events AS (
    SELECT
      shipment_id,
      carrier_id,
      origin_wh,
      destination,
      event_type,
      event_ts,
      latitude,
      longitude,
      ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_ts DESC) AS rn
    FROM sc.bronze.transport_shipments
  ),
  shipment_agg AS (
    SELECT
      shipment_id,
      COUNT(*) AS total_events,
      MAX(CASE WHEN event_type = 'exception' THEN true ELSE false END) AS has_exception,
      MAX(CASE WHEN event_type = 'customs_hold' THEN true ELSE false END) AS has_customs_hold,
      MIN(CASE WHEN event_type = 'dispatched' THEN event_ts END) AS dispatched_at,
      MAX(CASE WHEN event_type = 'delivered' THEN event_ts END) AS delivered_at
    FROM sc.bronze.transport_shipments
    GROUP BY shipment_id
  )
  SELECT
    re.shipment_id,
    re.carrier_id,
    re.origin_wh,
    re.destination,
    re.event_type AS current_status,
    sa.dispatched_at,
    sa.delivered_at,
    CASE
      WHEN sa.delivered_at IS NOT NULL AND sa.dispatched_at IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(sa.delivered_at) - UNIX_TIMESTAMP(sa.dispatched_at)) / 3600.0 AS DECIMAL(8,2))
      ELSE NULL
    END AS transit_hours,
    sa.total_events,
    sa.has_exception,
    sa.has_customs_hold,
    re.latitude AS last_latitude,
    re.longitude AS last_longitude
  FROM ranked_events re
  JOIN shipment_agg sa ON sa.shipment_id = re.shipment_id
  WHERE re.rn = 1
) AS src
ON tgt.shipment_id = src.shipment_id
WHEN MATCHED THEN UPDATE SET
  current_status   = src.current_status,
  dispatched_at    = src.dispatched_at,
  delivered_at     = src.delivered_at,
  transit_hours    = src.transit_hours,
  total_events     = src.total_events,
  has_exception    = src.has_exception,
  has_customs_hold = src.has_customs_hold,
  last_latitude    = src.last_latitude,
  last_longitude   = src.last_longitude,
  updated_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  shipment_id, carrier_id, origin_wh, destination, current_status,
  dispatched_at, delivered_at, transit_hours, total_events,
  has_exception, has_customs_hold, last_latitude, last_longitude, updated_at
) VALUES (
  src.shipment_id, src.carrier_id, src.origin_wh, src.destination, src.current_status,
  src.dispatched_at, src.delivered_at, src.transit_hours, src.total_events,
  src.has_exception, src.has_customs_hold, src.last_latitude, src.last_longitude, CURRENT_TIMESTAMP
);
