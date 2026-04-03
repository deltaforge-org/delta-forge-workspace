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

DROP PSEUDONYMISATION RULE ON legal.bronze.raw_parties (ssn);
DROP PSEUDONYMISATION RULE ON legal.bronze.raw_parties (party_name);
DROP PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_email);
DROP PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_phone);

-- ===================== DROP GRAPH =====================

DROP GRAPH IF EXISTS legal.gold.legal_network;

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS legal.gold.kpi_firm_performance WITH FILES;
DROP DELTA TABLE IF EXISTS legal.gold.fact_billings WITH FILES;
DROP DELTA TABLE IF EXISTS legal.gold.dim_party WITH FILES;
DROP DELTA TABLE IF EXISTS legal.gold.dim_attorney WITH FILES;
DROP DELTA TABLE IF EXISTS legal.gold.dim_case WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS legal.silver.party_profiles WITH FILES;
DROP DELTA TABLE IF EXISTS legal.silver.billings_validated WITH FILES;
DROP DELTA TABLE IF EXISTS legal.silver.cases_enriched WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS legal.bronze.raw_relationships WITH FILES;
DROP DELTA TABLE IF EXISTS legal.bronze.raw_billings WITH FILES;
DROP DELTA TABLE IF EXISTS legal.bronze.raw_attorneys WITH FILES;
DROP DELTA TABLE IF EXISTS legal.bronze.raw_parties WITH FILES;
DROP DELTA TABLE IF EXISTS legal.bronze.raw_cases WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS legal.gold;
DROP SCHEMA IF EXISTS legal.silver;
DROP SCHEMA IF EXISTS legal.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS legal;
