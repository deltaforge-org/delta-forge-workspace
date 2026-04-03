-- =============================================================================
-- Bronze: Create Zones and Schemas
-- =============================================================================
-- Sets up the three medallion zones (bronze, silver, gold) and their schemas.
-- Safe to re-run — uses IF NOT EXISTS throughout.
-- =============================================================================

CREATE ZONE IF NOT EXISTS {{zone_prefix}}_bronze
  LOCATION '{{data_path}}/bronze';

CREATE ZONE IF NOT EXISTS {{zone_prefix}}_silver
  LOCATION '{{data_path}}/silver';

CREATE ZONE IF NOT EXISTS {{zone_prefix}}_gold
  LOCATION '{{data_path}}/gold';

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}_bronze.raw;
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}_silver.cleansed;
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}_gold.analytics;
