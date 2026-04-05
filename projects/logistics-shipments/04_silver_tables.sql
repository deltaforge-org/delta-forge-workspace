-- =============================================================================
-- Logistics Shipments Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE logistics_shipments_04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Logistics Shipments'
  SCHEDULE 'logistics_6hr_schedule'
  TAGS 'setup', 'logistics-shipments'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS logi.silver.events_deduped (
  event_id         STRING      NOT NULL,
  shipment_id      STRING      NOT NULL,
  customer_id      STRING,
  carrier_id       STRING,
  origin_id        STRING,
  destination_id   STRING,
  service_level    STRING,
  event_type       STRING      NOT NULL,
  event_timestamp  TIMESTAMP   NOT NULL,
  ship_date        DATE,
  promised_date    DATE,
  delivery_date    DATE,
  weight_kg        DECIMAL(8,2),
  volume_m3        DECIMAL(8,4),
  cost             DECIMAL(10,2),
  revenue          DECIMAL(10,2),
  deduped_at       TIMESTAMP
) LOCATION 'logi/logistics/silver/events_deduped'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE DELTA TABLE IF NOT EXISTS logi.silver.shipment_status (
  shipment_id        STRING      NOT NULL,
  customer_id        STRING,
  carrier_id         STRING,
  origin_id          STRING,
  destination_id     STRING,
  service_level      STRING,
  ship_date          DATE,
  promised_date      DATE,
  delivery_date      DATE,
  latest_status      STRING,
  previous_status    STRING,
  event_count        INT,
  hours_in_last_stage DECIMAL(8,2),
  total_transit_hours DECIMAL(8,2),
  transit_days       INT,
  on_time_flag       BOOLEAN,
  weight_kg          DECIMAL(8,2),
  volume_m3          DECIMAL(8,4),
  cost               DECIMAL(10,2),
  revenue            DECIMAL(10,2),
  first_event_time   TIMESTAMP,
  last_event_time    TIMESTAMP,
  reconstructed_at   TIMESTAMP
) LOCATION 'logi/logistics/silver/shipment_status';

CREATE DELTA TABLE IF NOT EXISTS logi.silver.sla_violations (
  violation_id       STRING      NOT NULL,
  shipment_id        STRING      NOT NULL,
  carrier_id         STRING      NOT NULL,
  service_level      STRING,
  sla_max_days       INT,
  actual_transit_days INT,
  days_over_sla      INT,
  penalty_amount     DECIMAL(10,2),
  customer_id        STRING,
  origin_id          STRING,
  destination_id     STRING,
  sla_violated       BOOLEAN     NOT NULL,
  detected_at        TIMESTAMP
) LOCATION 'logi/logistics/silver/sla_violations';
