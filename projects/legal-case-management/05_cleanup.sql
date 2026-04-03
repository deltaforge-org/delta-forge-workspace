-- =============================================================================
-- Legal Case Management Pipeline - Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================

PIPELINE legal_case_management_cleanup
  DESCRIPTION 'Cleanup pipeline for Legal Case Management - drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'legal-case-management'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_parties (ssn);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_parties (party_name);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_parties (contact_email);
DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_parties (contact_phone);

-- ===================== DROP GRAPH =====================

DROP GRAPH IF EXISTS {{zone_prefix}}.gold.legal_network;

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_firm_performance WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_billings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_party WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_attorney WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_case WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.party_profiles WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.billings_validated WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.cases_enriched WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_relationships WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_billings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_attorneys WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_parties WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_cases WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS {{zone_prefix}};
