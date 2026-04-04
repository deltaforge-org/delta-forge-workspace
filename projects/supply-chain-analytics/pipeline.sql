-- =============================================================================
-- Supply Chain Analytics — Pipeline Orchestrator
-- =============================================================================
-- Split-file pattern: each STEP references an individual SQL file via
-- INCLUDE SCRIPT so that transformations can be versioned, tested, and
-- reviewed independently.
-- =============================================================================

SCHEDULE supply_chain_daily
  CRON '0 4 * * *'
  TIMEZONE 'UTC'
  RETRIES 2
  TIMEOUT 7200
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE supply_chain_analytics
  DESCRIPTION 'End-to-end supply chain pipeline: procurement -> warehouse -> transport -> demand'
  SCHEDULE 'supply_chain_daily'
  TAGS 'supply-chain', 'inventory', 'demand', 'medallion'
  SLA 2.0
  FAIL_FAST true
  LIFECYCLE production;

-- =========================================================================
-- Bronze: seed reference and transactional data
-- =========================================================================
STEP create_zones
  TIMEOUT '1m'
AS
  INCLUDE SCRIPT 'bronze/01_create_zones.sql';

STEP load_reference_data
  DEPENDS ON (create_zones)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'bronze/06_reference_data.sql';

STEP load_purchase_orders
  DEPENDS ON (create_zones)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'bronze/02_purchase_orders.sql';

STEP load_warehouse_movements
  DEPENDS ON (create_zones)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'bronze/03_warehouse_movements.sql';

STEP load_transport_shipments
  DEPENDS ON (create_zones)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'bronze/04_transport_shipments.sql';

STEP load_pos_demand
  DEPENDS ON (create_zones)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'bronze/05_pos_demand.sql';

-- =========================================================================
-- Silver: validate, cleanse, enrich
-- =========================================================================
STEP validate_bronze
  DEPENDS ON (load_purchase_orders, load_warehouse_movements, load_transport_shipments, load_pos_demand, load_reference_data)
  TIMEOUT '2m'
AS
  INCLUDE SCRIPT 'silver/01_validate_bronze.sql';

STEP build_inventory
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'silver/02_inventory_positions.sql';

STEP build_fulfillment
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'silver/03_order_fulfillment.sql';

STEP track_shipments
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'silver/04_shipment_tracking.sql';

STEP score_demand
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'silver/05_demand_signals.sql';

-- =========================================================================
-- Gold: dimensions (parallel builds)
-- =========================================================================
STEP dim_supplier
  DEPENDS ON (build_fulfillment)
AS
  INCLUDE SCRIPT 'gold/01_dim_supplier.sql';

STEP dim_product
  DEPENDS ON (build_inventory)
AS
  INCLUDE SCRIPT 'gold/02_dim_product.sql';

STEP dim_warehouse
  DEPENDS ON (build_inventory)
AS
  INCLUDE SCRIPT 'gold/03_dim_warehouse.sql';

STEP dim_store
  DEPENDS ON (score_demand)
AS
  INCLUDE SCRIPT 'gold/04_dim_store.sql';

-- =========================================================================
-- Gold: fact tables (depend on dims + silver)
-- =========================================================================
STEP fact_inventory
  DEPENDS ON (build_inventory, dim_product, dim_warehouse)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'gold/05_fact_inventory.sql';

STEP fact_orders
  DEPENDS ON (build_fulfillment, track_shipments, dim_supplier, dim_product)
  TIMEOUT '5m'
AS
  INCLUDE SCRIPT 'gold/06_fact_orders.sql';

-- =========================================================================
-- Gold: KPIs (depend on facts)
-- =========================================================================
STEP kpi_supplier
  DEPENDS ON (fact_orders)
AS
  INCLUDE SCRIPT 'gold/07_kpi_supplier_scorecard.sql';

STEP kpi_inventory
  DEPENDS ON (fact_inventory, score_demand)
AS
  INCLUDE SCRIPT 'gold/08_kpi_inventory_health.sql';

STEP kpi_demand
  DEPENDS ON (score_demand, fact_inventory)
AS
  INCLUDE SCRIPT 'gold/09_kpi_demand_forecast.sql';

-- =========================================================================
-- Maintenance (runs after all KPIs)
-- =========================================================================
STEP optimize
  DEPENDS ON (kpi_supplier, kpi_inventory, kpi_demand)
  CONTINUE ON FAILURE
AS
  INCLUDE SCRIPT 'maintenance/01_optimize.sql';
