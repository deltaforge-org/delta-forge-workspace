-- =============================================================================
-- Telecom CDR Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE telecom_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Telecom CDR'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'setup', 'telecom-cdr'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

-- CDR v1 (2023): Voice-only format — 6 core columns
CREATE DELTA TABLE IF NOT EXISTS telco.bronze.raw_cdr_v1 (
  call_id         STRING      NOT NULL,
  caller          STRING      NOT NULL,
  callee          STRING      NOT NULL,
  start_time      TIMESTAMP   NOT NULL,
  end_time        TIMESTAMP,
  tower_id        STRING,
  duration_sec    INT,
  ingested_at     TIMESTAMP
) LOCATION 'telco/telecom/bronze/raw_cdr_v1';

-- CDR v2 (2024 H1): Multi-service — adds call_type, data_usage_mb, sms_count
CREATE DELTA TABLE IF NOT EXISTS telco.bronze.raw_cdr_v2 (
  call_id         STRING      NOT NULL,
  caller          STRING      NOT NULL,
  callee          STRING      NOT NULL,
  start_time      TIMESTAMP   NOT NULL,
  end_time        TIMESTAMP,
  tower_id        STRING,
  duration_sec    INT,
  call_type       STRING,
  data_usage_mb   DECIMAL(10,2),
  sms_count       INT,
  ingested_at     TIMESTAMP
) LOCATION 'telco/telecom/bronze/raw_cdr_v2';

-- CDR v3 (2024 H2): 5G era — adds roaming_flag, network_type, handover_count
-- Type widening: duration_sec from INT to BIGINT for long data sessions
CREATE DELTA TABLE IF NOT EXISTS telco.bronze.raw_cdr_v3 (
  call_id         STRING      NOT NULL,
  caller          STRING      NOT NULL,
  callee          STRING      NOT NULL,
  start_time      TIMESTAMP   NOT NULL,
  end_time        TIMESTAMP,
  tower_id        STRING,
  duration_sec    BIGINT,
  call_type       STRING,
  data_usage_mb   DECIMAL(10,2),
  sms_count       INT,
  roaming_flag    BOOLEAN,
  network_type    STRING,
  handover_count  INT,
  ingested_at     TIMESTAMP
) LOCATION 'telco/telecom/bronze/raw_cdr_v3';

-- Subscriber reference
CREATE DELTA TABLE IF NOT EXISTS telco.bronze.raw_subscribers (
  subscriber_id   STRING      NOT NULL,
  phone_number    STRING      NOT NULL,
  plan_type       STRING,
  plan_tier       STRING,
  activation_date DATE,
  status          STRING,
  monthly_spend   DECIMAL(8,2),
  balance         DECIMAL(8,2),
  ingested_at     TIMESTAMP
) LOCATION 'telco/telecom/bronze/raw_subscribers';

-- Cell tower reference
CREATE DELTA TABLE IF NOT EXISTS telco.bronze.raw_cell_towers (
  tower_id       STRING      NOT NULL,
  location       STRING,
  city           STRING,
  region         STRING,
  technology     STRING,
  capacity_mhz   INT,
  ingested_at    TIMESTAMP
) LOCATION 'telco/telecom/bronze/raw_cell_towers';
