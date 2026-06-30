-- =============================================================================
-- Telecom CDR Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE telecom_gold_tables
  DESCRIPTION 'Creates gold layer tables for Telecom CDR'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'setup', 'telecom-cdr'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS telco.gold.dim_subscriber (
  subscriber_key  STRING      NOT NULL,
  phone_number    STRING,
  plan_type       STRING,
  plan_tier       STRING,
  activation_date DATE,
  status          STRING,
  monthly_spend   DECIMAL(8,2),
  balance         DECIMAL(8,2)
) LOCATION 'telco/telecom/gold/dim_subscriber';

CREATE DELTA TABLE IF NOT EXISTS telco.gold.dim_tower (
  tower_key      STRING      NOT NULL,
  tower_id       STRING,
  location       STRING,
  city           STRING,
  region         STRING,
  technology     STRING,
  capacity_mhz   INT
) LOCATION 'telco/telecom/gold/dim_tower';

CREATE DELTA TABLE IF NOT EXISTS telco.gold.dim_plan (
  plan_key       STRING      NOT NULL,
  plan_type      STRING,
  plan_tier      STRING,
  monthly_spend  DECIMAL(8,2)
) LOCATION 'telco/telecom/gold/dim_plan';

CREATE DELTA TABLE IF NOT EXISTS telco.gold.fact_calls (
  call_key        STRING      NOT NULL,
  caller_key      STRING,
  callee_key      STRING,
  tower_key       STRING,
  plan_key        STRING,
  start_time      TIMESTAMP,
  duration_sec    BIGINT,
  call_type       STRING,
  data_usage_mb   DECIMAL(10,2),
  roaming_flag    BOOLEAN,
  drop_flag       BOOLEAN,
  network_type    STRING,
  schema_version  INT,
  revenue         DECIMAL(8,2)
) LOCATION 'telco/telecom/gold/fact_calls'
PARTITIONED BY (start_time);

CREATE DELTA TABLE IF NOT EXISTS telco.gold.kpi_network_quality (
  region          STRING,
  hour_bucket     INT,
  total_calls     INT,
  dropped_calls   INT,
  drop_rate       DECIMAL(5,4),
  avg_duration    DECIMAL(8,1),
  total_data_mb   DECIMAL(12,2),
  avg_throughput_mb DECIMAL(8,2),
  roaming_calls   INT,
  fiveg_calls     INT,
  revenue         DECIMAL(10,2)
) LOCATION 'telco/telecom/gold/kpi_network_quality';

CREATE DELTA TABLE IF NOT EXISTS telco.gold.kpi_churn_risk (
  subscriber_id    STRING      NOT NULL,
  phone_number     STRING,
  plan_type        STRING,
  plan_tier        STRING,
  status           STRING,
  days_since_last_call INT,
  monthly_usage_trend  DECIMAL(8,2),
  drop_rate        DECIMAL(5,4),
  balance          DECIMAL(8,2),
  churn_score      INT,
  churn_risk_level STRING,
  scored_at        TIMESTAMP
) LOCATION 'telco/telecom/gold/kpi_churn_risk';
