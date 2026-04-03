-- =============================================================================
-- Legal Case Management Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE legal_case_management_cleanup
  DESCRIPTION 'Cleanup pipeline for Legal Case Management — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'legal-case-management'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_client (client_name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_case (case_number);

-- ===================== DROP GRAPH =====================

DROP GRAPH IF EXISTS {{zone_prefix}}.gold.legal_network;

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_billings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_client WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_attorney WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_case WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.billings_enriched WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_case_clients WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_case_attorneys WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_billings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_clients WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_cases WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_attorneys WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS {{zone_prefix}};
