-- =============================================================================
-- Pharma Clinical Trials Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE pharma_clinical_trials_cleanup
  DESCRIPTION 'Cleanup pipeline for Pharma Clinical Trials — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'pharma-clinical-trials'
  STATUS disabled
  LIFECYCLE production
;

-- Drop in reverse dependency order: gold -> silver -> bronze -> schemas -> zones

-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.gold.kpi_trial_efficacy;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.fact_observations;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_visit;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_site;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_trial;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_participant;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.silver.observation_enriched;
DROP TABLE IF EXISTS {{zone_prefix}}.silver.participant_clean;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_observations;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_visits;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_sites;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_trials;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_participants;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS {{zone_prefix}};
