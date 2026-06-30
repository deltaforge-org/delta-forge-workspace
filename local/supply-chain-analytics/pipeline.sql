-- =============================================================================
-- Supply Chain Analytics: Pipeline Orchestrator
-- =============================================================================
-- Split-file pattern: each STEP references an individual SQL file via
-- INCLUDE SCRIPT so that transformations can be versioned, tested, and
-- reviewed independently.
-- =============================================================================

SCHEDULE supply_chain_daily
  CRON '0 4 * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 7200
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE supply_chain_analytics
  DESCRIPTION 'End-to-end supply chain pipeline: procurement -> warehouse -> transport -> demand'
  SCHEDULE 'supply_chain_daily'
  TAGS 'supply-chain', 'inventory', 'demand', 'medallion'
  SLA 2.0
  FAIL_FAST true
  LIFECYCLE production
;

-- =========================================================================
-- Bronze: seed reference and transactional data
-- =========================================================================
INCLUDE SCRIPT 'bronze/01_create_zones.sql';

INCLUDE SCRIPT 'bronze/06_reference_data.sql';

INCLUDE SCRIPT 'bronze/02_purchase_orders.sql';

INCLUDE SCRIPT 'bronze/03_warehouse_movements.sql';

INCLUDE SCRIPT 'bronze/04_transport_shipments.sql';

INCLUDE SCRIPT 'bronze/05_pos_demand.sql';

-- =========================================================================
-- Silver: validate, cleanse, enrich
-- =========================================================================
INCLUDE SCRIPT 'silver/01_validate_bronze.sql';

INCLUDE SCRIPT 'silver/02_inventory_positions.sql';

INCLUDE SCRIPT 'silver/03_order_fulfillment.sql';

INCLUDE SCRIPT 'silver/04_shipment_tracking.sql';

INCLUDE SCRIPT 'silver/05_demand_signals.sql';

-- =========================================================================
-- Gold: dimensions (parallel builds)
-- =========================================================================
INCLUDE SCRIPT 'gold/01_dim_supplier.sql';

INCLUDE SCRIPT 'gold/02_dim_product.sql';

INCLUDE SCRIPT 'gold/03_dim_warehouse.sql';

INCLUDE SCRIPT 'gold/04_dim_store.sql';

-- =========================================================================
-- Gold: fact tables (depend on dims + silver)
-- =========================================================================
INCLUDE SCRIPT 'gold/05_fact_inventory.sql';

INCLUDE SCRIPT 'gold/06_fact_orders.sql';

-- =========================================================================
-- Gold: KPIs (depend on facts)
-- =========================================================================
INCLUDE SCRIPT 'gold/07_kpi_supplier_scorecard.sql';

INCLUDE SCRIPT 'gold/08_kpi_inventory_health.sql';

INCLUDE SCRIPT 'gold/09_kpi_demand_forecast.sql';

-- =========================================================================
-- Maintenance (runs after all KPIs)
-- =========================================================================
INCLUDE SCRIPT 'maintenance/01_optimize.sql';
