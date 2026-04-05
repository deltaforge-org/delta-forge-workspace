-- =============================================================================
-- Logistics Shipments Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Logistics Shipments'
  SCHEDULE 'logistics_6hr_schedule'
  TAGS 'setup', 'logistics-shipments'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS logi.bronze.raw_carriers (
  carrier_id       STRING      NOT NULL,
  carrier_name     STRING      NOT NULL,
  carrier_type     STRING,
  fleet_size       INT,
  headquarters     STRING,
  on_time_rating   DECIMAL(3,2),
  cost_per_kg      DECIMAL(6,2),
  ingested_at      TIMESTAMP
) LOCATION 'logi/logistics/bronze/raw_carriers';

CREATE DELTA TABLE IF NOT EXISTS logi.bronze.raw_locations (
  location_id      STRING      NOT NULL,
  hub_name         STRING      NOT NULL,
  city             STRING,
  state            STRING,
  country          STRING,
  region           STRING,
  hub_type         STRING,
  latitude         DECIMAL(9,6),
  longitude        DECIMAL(9,6),
  ingested_at      TIMESTAMP
) LOCATION 'logi/logistics/bronze/raw_locations';

CREATE DELTA TABLE IF NOT EXISTS logi.bronze.raw_customers (
  customer_id      STRING      NOT NULL,
  customer_name    STRING      NOT NULL,
  tier             STRING,
  industry         STRING,
  city             STRING,
  country          STRING,
  account_manager  STRING,
  ingested_at      TIMESTAMP
) LOCATION 'logi/logistics/bronze/raw_customers';

CREATE DELTA TABLE IF NOT EXISTS logi.bronze.raw_sla_contracts (
  sla_id           STRING      NOT NULL,
  carrier_id       STRING      NOT NULL,
  service_level    STRING      NOT NULL,
  max_transit_days INT         NOT NULL,
  penalty_per_day  DECIMAL(8,2),
  contract_start   DATE,
  contract_end     DATE,
  ingested_at      TIMESTAMP
) LOCATION 'logi/logistics/bronze/raw_sla_contracts';

CREATE DELTA TABLE IF NOT EXISTS logi.bronze.raw_tracking_events (
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
  ingested_at      TIMESTAMP
) LOCATION 'logi/logistics/bronze/raw_tracking_events';
