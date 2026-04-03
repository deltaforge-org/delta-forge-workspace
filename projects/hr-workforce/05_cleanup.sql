-- =============================================================================
-- HR Workforce Analytics Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE hr_workforce_cleanup
  DESCRIPTION 'Cleanup pipeline for Hr Workforce — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'hr-workforce'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.gold.kpi_workforce_analytics;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.fact_compensation_events;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_position;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_department;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_employee;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.silver.comp_events_enriched;
DROP TABLE IF EXISTS {{zone_prefix}}.silver.dim_employee_scd2;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_comp_events;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_positions;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_departments;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_employees;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS {{zone_prefix}};
