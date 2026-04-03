-- =============================================================================
-- Create Zone and Schemas
-- =============================================================================
-- Sets up one dedicated zone with three medallion schemas (bronze, silver, gold).
-- Safe to re-run — uses IF NOT EXISTS throughout.
-- =============================================================================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Supply chain analytics workspace project';

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze
    COMMENT 'Raw supply chain feeds — POs, warehouse movements, transport, POS';

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver
    COMMENT 'Validated and enriched supply chain data';

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold
    COMMENT 'Star schema for supply chain analytics and KPIs';
