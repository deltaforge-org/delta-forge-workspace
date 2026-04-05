-- =============================================================================
-- Telecom CDR Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Telecom CDR'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'setup', 'telecom-cdr'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- Unified CDR: all 3 versions merged, NULL-filled for missing columns
CREATE DELTA TABLE IF NOT EXISTS telco.silver.cdr_unified (
  call_id         STRING      NOT NULL,
  caller          STRING      NOT NULL,
  callee          STRING      NOT NULL,
  caller_id       STRING,
  callee_id       STRING,
  tower_id        STRING,
  tower_city      STRING,
  tower_region    STRING,
  start_time      TIMESTAMP,
  end_time        TIMESTAMP,
  duration_sec    BIGINT,
  call_type       STRING      DEFAULT 'voice',
  data_usage_mb   DECIMAL(10,2) DEFAULT 0.00,
  sms_count       INT         DEFAULT 0,
  roaming_flag    BOOLEAN     DEFAULT false,
  network_type    STRING,
  handover_count  INT         DEFAULT 0,
  drop_flag       BOOLEAN     DEFAULT false,
  revenue         DECIMAL(8,2),
  schema_version  INT,
  unified_at      TIMESTAMP
) LOCATION 'telco/telecom/silver/cdr_unified'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Subscriber profiles: MERGE-upserted from CDR aggregation
CREATE DELTA TABLE IF NOT EXISTS telco.silver.subscriber_profiles (
  subscriber_id    STRING      NOT NULL,
  phone_number     STRING,
  plan_type        STRING,
  plan_tier        STRING,
  activation_date  DATE,
  status           STRING,
  monthly_spend    DECIMAL(8,2),
  balance          DECIMAL(8,2),
  total_calls      INT,
  total_data_mb    DECIMAL(12,2),
  total_sms        INT,
  total_revenue    DECIMAL(10,2),
  avg_call_duration BIGINT,
  drop_rate        DECIMAL(5,4),
  days_since_last_call INT,
  monthly_usage_trend  DECIMAL(8,2),
  last_activity    TIMESTAMP,
  updated_at       TIMESTAMP
) LOCATION 'telco/telecom/silver/subscriber_profiles';

-- Sessions: reconstructed from CDR events using LAG gap detection
CREATE DELTA TABLE IF NOT EXISTS telco.silver.sessions (
  session_id      STRING      NOT NULL,
  subscriber_id   STRING      NOT NULL,
  session_start   TIMESTAMP,
  session_end     TIMESTAMP,
  event_count     INT,
  total_duration  BIGINT,
  total_data_mb   DECIMAL(12,2),
  call_types      STRING,
  roaming_events  INT,
  drop_events     INT,
  created_at      TIMESTAMP
) LOCATION 'telco/telecom/silver/sessions';
