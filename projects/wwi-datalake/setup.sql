-- ============================================================================
-- WWI Data Lake - Master Pipeline
-- ============================================================================
--
-- PREREQUISITE:
--   A SQL Server connection named "mssql" must exist in the Delta Forge
--   connection manager pointing to a WideWorldImporters database instance.
--   All source tables are accessed as: mssql_WideWorldImporters.<Schema>.<Table>
--
-- Execution order (fully idempotent, safe to run repeatedly):
--   1. Preflight  - verify MSSQL connection is reachable
--   2. Zones      - create zone and schemas (IF NOT EXISTS)
--   3. Bronze     - create tables + ingest from MSSQL
--   4. Silver     - create/refresh transformation views
--   5. Gold       - create tables + materialize star schema
-- ============================================================================

PIPELINE wwi_lake.pipeline
    DESCRIPTION 'WWI datalake - bronze/silver/gold medallion from MSSQL source'
    TAGS 'wwi', 'medallion', 'mssql'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- ---------------------------------------------------------------------------
-- Preflight: verify MSSQL connection
-- ---------------------------------------------------------------------------

ASSERT ERROR ROW_COUNT > 0
SELECT cityid FROM mssql_WideWorldImporters.Application.Cities LIMIT 1;
-- If this fails: create an MSSQL connection named "mssql" pointing to
-- a WideWorldImporters database in the Connections page, then re-run.

-- ---------------------------------------------------------------------------
-- Zone & Schemas
-- ---------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS wwi_lake
    TYPE MANAGED
    COMMENT 'WideWorldImporters Delta Lake - medallion architecture';

CREATE SCHEMA IF NOT EXISTS wwi_lake.bronze
    COMMENT 'Raw ingestion layer - 1:1 with MSSQL source, snake_case columns';

CREATE SCHEMA IF NOT EXISTS wwi_lake.silver
    COMMENT 'Views: cleaned, enriched, business-logic applied over bronze';

CREATE SCHEMA IF NOT EXISTS wwi_lake.gold
    COMMENT 'Star schema for end users - materialized dimensions and facts';

