-- ============================================================================
-- WWI Data Lake - Zone & Schema Setup
-- ============================================================================
--
-- PREREQUISITE:
--   A SQL Server connection named "mssql" must exist in the DeltaForge
--   connection manager pointing to a WideWorldImporters database instance.
--   All source tables are accessed as: mssql_WideWorldImporters.<schema>.<table>
--
-- Creates zone and schemas (IF NOT EXISTS). Safe to run repeatedly.
-- ============================================================================

PIPELINE wwi_lake.setup
    DESCRIPTION 'WWI datalake - zone and schema setup'
    SCHEDULE 'wwi_lake_daily'
    TAGS 'wwi', 'medallion', 'mssql', 'setup'
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- Preflight: verify MSSQL connection

ASSERT ERROR ROW_COUNT > 0
SELECT city_id FROM mssql_WideWorldImporters.application.cities LIMIT 1;
-- If this fails: create an MSSQL connection named "mssql" pointing to
-- a WideWorldImporters database in the Connections page, then re-run.

-- Zone & Schemas

CREATE ZONE IF NOT EXISTS wwi_lake
    TYPE MANAGED
    COMMENT 'WideWorldImporters Delta Lake - medallion architecture';

CREATE SCHEMA IF NOT EXISTS wwi_lake.bronze
    COMMENT 'Raw ingestion layer - 1:1 with MSSQL source, snake_case columns';

CREATE SCHEMA IF NOT EXISTS wwi_lake.silver
    COMMENT 'Views: cleaned, enriched, business-logic applied over bronze';

CREATE SCHEMA IF NOT EXISTS wwi_lake.gold
    COMMENT 'Star schema for end users - materialized dimensions and facts';
