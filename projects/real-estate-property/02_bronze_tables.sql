-- =============================================================================
-- Real Estate Property Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE 02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS realty.bronze.raw_properties (
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
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'realty/bronze/property/raw_properties';

GRANT ADMIN ON TABLE realty.bronze.raw_properties TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.bronze.raw_assessments (
  assessment_id       STRING      NOT NULL,
  parcel_id           STRING      NOT NULL,
  assessment_year     INT         NOT NULL,
  assessed_value      DECIMAL(14,2),
  land_value          DECIMAL(14,2),
  improvement_value   DECIMAL(14,2),
  tax_rate            DECIMAL(7,5),
  annual_tax          DECIMAL(12,2),
  assessment_date     STRING      NOT NULL,
  assessor_notes      STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'realty/bronze/property/raw_assessments';

GRANT ADMIN ON TABLE realty.bronze.raw_assessments TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.bronze.raw_transactions (
  transaction_id      STRING      NOT NULL,
  parcel_id           STRING      NOT NULL,
  buyer_name          STRING,
  seller_name         STRING,
  agent_id            STRING,
  transaction_date    STRING      NOT NULL,
  sale_price          DECIMAL(14,2),
  financing_type      STRING,
  closing_costs       DECIMAL(10,2),
  days_on_market      INT,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'realty/bronze/property/raw_transactions';

GRANT ADMIN ON TABLE realty.bronze.raw_transactions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.bronze.raw_neighborhoods (
  neighborhood_id     STRING      NOT NULL,
  neighborhood_name   STRING      NOT NULL,
  city                STRING,
  county              STRING,
  state               STRING,
  median_income       DECIMAL(10,2),
  school_rating       INT,
  crime_index         DECIMAL(4,2),
  walkability_score   INT,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'realty/bronze/property/raw_neighborhoods';

GRANT ADMIN ON TABLE realty.bronze.raw_neighborhoods TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.bronze.raw_agents (
  agent_id            STRING      NOT NULL,
  agent_name          STRING      NOT NULL,
  brokerage           STRING,
  license_number      STRING,
  years_experience    INT,
  specialization      STRING,
  county              STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'realty/bronze/property/raw_agents';

GRANT ADMIN ON TABLE realty.bronze.raw_agents TO USER admin;
