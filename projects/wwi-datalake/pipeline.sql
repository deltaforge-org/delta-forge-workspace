-- ============================================================================
-- WWI Data Lake — Pipeline Orchestrator
-- ============================================================================
-- Defines the shared schedule and master pipeline for the WWI datalake
-- medallion architecture (bronze -> silver -> gold).
--
-- Each numbered SQL file declares its own PIPELINE step attached to this
-- schedule. Teardown (13_teardown.sql) is excluded — it must be activated
-- and run manually.
-- ============================================================================

SCHEDULE wwi_lake_daily
  CRON '0 3 * * *'
  TIMEZONE 'UTC'
  RETRIES 0
  TIMEOUT 7200
  MAX_CONCURRENT 1
  INACTIVE;

PIPELINE wwi_lake
  DESCRIPTION 'WWI datalake end-to-end: MSSQL ingestion -> bronze -> silver -> gold star schema'
  SCHEDULE 'wwi_lake_daily'
  TAGS 'wwi', 'medallion', 'mssql', 'datalake'
  SLA 2.0
  FAIL_FAST true
  LIFECYCLE PRODUCTION
;
