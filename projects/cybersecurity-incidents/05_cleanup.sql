-- =============================================================================
-- Cybersecurity Incidents Pipeline: Cleanup
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE cybersecurity_incidents_cleanup
  DESCRIPTION 'Cleanup pipeline for Cybersecurity Incidents — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'cybersecurity-incidents'
  STATUS disabled
  LIFECYCLE production
;

-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_source_ip (ip_address);

-- ===================== DROP BLOOM FILTER INDEXES =====================

DROP BLOOMFILTER INDEX ON {{zone_prefix}}.silver.alerts_deduped FOR COLUMNS (source_ip);
DROP BLOOMFILTER INDEX ON {{zone_prefix}}.gold.fact_incidents FOR COLUMNS (source_ip_key);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_response_metrics WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.kpi_threat_dashboard WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.fact_incidents WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_mitre WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_rule WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_target WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.gold.dim_source_ip WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.threat_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.incidents_correlated WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.silver.alerts_deduped WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_endpoint_alerts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_ids_alerts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_firewall_alerts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_mitre_techniques WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_prefix}}.bronze.raw_threat_intel WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== DROP ZONE =====================

DROP ZONE IF EXISTS {{zone_prefix}};
