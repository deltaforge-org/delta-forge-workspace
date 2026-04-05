-- =============================================================================
-- Real Estate Property Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE 04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- SCD2: assessment value changes tracked with valid_from/valid_to/is_current.
-- CDF enabled so every SCD2 update is captured.
CREATE DELTA TABLE IF NOT EXISTS realty.silver.property_dim (
  surrogate_key       INT         NOT NULL,
  parcel_id           STRING      NOT NULL,
  address             STRING,
  city                STRING,
  county              STRING,
  state               STRING,
  zip                 STRING,
  neighborhood_id     STRING,
  property_type       STRING,
  bedrooms            INT,
  bathrooms           DECIMAL(3,1),
  sqft                INT,
  lot_acres           DECIMAL(6,3),
  year_built          INT,
  assessed_value      DECIMAL(14,2),
  land_value          DECIMAL(14,2),
  improvement_value   DECIMAL(14,2),
  assessment_year     INT,
  valid_from          DATE        NOT NULL,
  valid_to            DATE,
  is_current          BOOLEAN     NOT NULL
) LOCATION 'realty/silver/property/property_dim'
PARTITIONED BY (county)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE realty.silver.property_dim TO USER admin;

-- Transactions enriched with assessed value at time of sale (point-in-time join)
CREATE DELTA TABLE IF NOT EXISTS realty.silver.transactions_enriched (
  transaction_id      STRING      NOT NULL,
  parcel_id           STRING      NOT NULL,
  buyer_name          STRING,
  seller_name         STRING,
  agent_id            STRING,
  transaction_date    DATE,
  sale_price          DECIMAL(14,2),
  assessed_value_at_sale DECIMAL(14,2),
  price_per_sqft      DECIMAL(8,2),
  days_on_market      INT,
  over_asking_pct     DECIMAL(5,2),
  assessed_vs_sale_ratio DECIMAL(5,2),
  assessment_outlier  BOOLEAN,
  financing_type      STRING,
  closing_costs       DECIMAL(10,2)
) LOCATION 'realty/silver/property/transactions_enriched';

GRANT ADMIN ON TABLE realty.silver.transactions_enriched TO USER admin;

-- Captures RESTORE events
CREATE DELTA TABLE IF NOT EXISTS realty.silver.correction_log (
  correction_id       INT         NOT NULL,
  table_name          STRING      NOT NULL,
  operation           STRING      NOT NULL,
  restored_to_version INT,
  reason              STRING,
  row_count_before    INT,
  row_count_after     INT,
  correction_timestamp TIMESTAMP  NOT NULL
) LOCATION 'realty/silver/property/correction_log';

GRANT ADMIN ON TABLE realty.silver.correction_log TO USER admin;
