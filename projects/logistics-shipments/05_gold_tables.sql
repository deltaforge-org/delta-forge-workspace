-- =============================================================================
-- Logistics Shipments Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE logistics_shipments_05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Logistics Shipments'
  SCHEDULE 'logistics_6hr_schedule'
  TAGS 'setup', 'logistics-shipments'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS logi.gold.dim_carrier (
  carrier_key      STRING      NOT NULL,
  carrier_name     STRING,
  carrier_type     STRING,
  fleet_size       INT,
  headquarters     STRING,
  on_time_rating   DECIMAL(3,2),
  cost_per_kg      DECIMAL(6,2),
  loaded_at        TIMESTAMP
) LOCATION 'logi/logistics/gold/dim_carrier';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.dim_location (
  location_key     STRING      NOT NULL,
  hub_name         STRING,
  city             STRING,
  state            STRING,
  country          STRING,
  region           STRING,
  hub_type         STRING,
  latitude         DECIMAL(9,6),
  longitude        DECIMAL(9,6),
  loaded_at        TIMESTAMP
) LOCATION 'logi/logistics/gold/dim_location';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.dim_customer (
  customer_key     STRING      NOT NULL,
  customer_name    STRING,
  tier             STRING,
  industry         STRING,
  city             STRING,
  country          STRING,
  account_manager  STRING,
  loaded_at        TIMESTAMP
) LOCATION 'logi/logistics/gold/dim_customer';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.dim_route (
  route_key         STRING      NOT NULL,
  origin_hub        STRING,
  origin_city       STRING,
  destination_hub   STRING,
  destination_city  STRING,
  distance_km       INT,
  avg_transit_days  DECIMAL(5,1),
  shipment_count    INT,
  primary_mode      STRING,
  loaded_at         TIMESTAMP
) LOCATION 'logi/logistics/gold/dim_route';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.fact_shipments (
  shipment_key      STRING      NOT NULL,
  carrier_key       STRING,
  origin_key        STRING,
  destination_key   STRING,
  customer_key      STRING,
  route_key         STRING,
  service_level     STRING,
  ship_date         DATE,
  delivery_date     DATE,
  promised_date     DATE,
  weight_kg         DECIMAL(8,2),
  volume_m3         DECIMAL(8,4),
  cost              DECIMAL(10,2),
  revenue           DECIMAL(10,2),
  margin            DECIMAL(10,2),
  on_time_flag      BOOLEAN,
  transit_days      INT,
  event_count       INT,
  sla_violated      BOOLEAN,
  penalty_amount    DECIMAL(10,2),
  loaded_at         TIMESTAMP
) LOCATION 'logi/logistics/gold/fact_shipments';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.kpi_delivery_performance (
  carrier_name     STRING,
  route            STRING,
  month            STRING,
  total_shipments  INT,
  on_time_count    INT,
  on_time_pct      DECIMAL(5,2),
  avg_transit_days DECIMAL(5,1),
  avg_cost_per_kg  DECIMAL(8,2),
  total_weight     DECIMAL(12,2),
  total_revenue    DECIMAL(12,2),
  total_margin     DECIMAL(12,2),
  loaded_at        TIMESTAMP
) LOCATION 'logi/logistics/gold/kpi_delivery_performance';

CREATE DELTA TABLE IF NOT EXISTS logi.gold.kpi_sla_compliance (
  carrier_name     STRING,
  service_level    STRING,
  month            STRING,
  total_shipments  INT,
  violated_count   INT,
  violation_rate   DECIMAL(5,2),
  total_penalty    DECIMAL(12,2),
  avg_days_over    DECIMAL(5,1),
  worst_violation  INT,
  loaded_at        TIMESTAMP
) LOCATION 'logi/logistics/gold/kpi_sla_compliance';

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE logi.bronze.raw_carriers TO USER admin;
GRANT ADMIN ON TABLE logi.bronze.raw_locations TO USER admin;
GRANT ADMIN ON TABLE logi.bronze.raw_customers TO USER admin;
GRANT ADMIN ON TABLE logi.bronze.raw_sla_contracts TO USER admin;
GRANT ADMIN ON TABLE logi.bronze.raw_tracking_events TO USER admin;
GRANT ADMIN ON TABLE logi.silver.events_deduped TO USER admin;
GRANT ADMIN ON TABLE logi.silver.shipment_status TO USER admin;
GRANT ADMIN ON TABLE logi.silver.sla_violations TO USER admin;
GRANT ADMIN ON TABLE logi.gold.dim_carrier TO USER admin;
GRANT ADMIN ON TABLE logi.gold.dim_location TO USER admin;
GRANT ADMIN ON TABLE logi.gold.dim_customer TO USER admin;
GRANT ADMIN ON TABLE logi.gold.dim_route TO USER admin;
GRANT ADMIN ON TABLE logi.gold.fact_shipments TO USER admin;
GRANT ADMIN ON TABLE logi.gold.kpi_delivery_performance TO USER admin;
GRANT ADMIN ON TABLE logi.gold.kpi_sla_compliance TO USER admin;
