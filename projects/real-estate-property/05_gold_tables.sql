-- =============================================================================
-- Real Estate Property Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE 05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS realty.gold.dim_neighborhood (
  neighborhood_key    INT         NOT NULL,
  neighborhood_id     STRING      NOT NULL,
  neighborhood_name   STRING      NOT NULL,
  city                STRING,
  county              STRING,
  state               STRING,
  median_income       DECIMAL(10,2),
  school_rating       INT,
  crime_index         DECIMAL(4,2),
  walkability_score   INT
) LOCATION 'realty/gold/property/dim_neighborhood';

GRANT ADMIN ON TABLE realty.gold.dim_neighborhood TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.gold.dim_agent (
  agent_key           INT         NOT NULL,
  agent_id            STRING      NOT NULL,
  agent_name          STRING      NOT NULL,
  brokerage           STRING,
  license_number      STRING,
  years_experience    INT,
  specialization      STRING,
  county              STRING
) LOCATION 'realty/gold/property/dim_agent';

GRANT ADMIN ON TABLE realty.gold.dim_agent TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.gold.dim_property_type (
  property_type_key   INT         NOT NULL,
  property_type       STRING      NOT NULL,
  avg_sqft            INT,
  avg_assessed_value  DECIMAL(14,2),
  property_count      INT
) LOCATION 'realty/gold/property/dim_property_type';

GRANT ADMIN ON TABLE realty.gold.dim_property_type TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.gold.fact_transactions (
  transaction_key     INT         NOT NULL,
  parcel_id           STRING      NOT NULL,
  neighborhood_key    INT,
  agent_key           INT,
  property_type_key   INT,
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
) LOCATION 'realty/gold/property/fact_transactions';

GRANT ADMIN ON TABLE realty.gold.fact_transactions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.gold.kpi_market_trends (
  city                STRING      NOT NULL,
  property_type       STRING      NOT NULL,
  sale_quarter        STRING      NOT NULL,
  median_sale_price   DECIMAL(14,2),
  avg_price_per_sqft  DECIMAL(8,2),
  avg_days_on_market  DECIMAL(5,1),
  total_transactions  INT,
  avg_over_asking_pct DECIMAL(5,2),
  inventory_months    DECIMAL(5,1),
  yoy_price_change_pct DECIMAL(5,2)
) LOCATION 'realty/gold/property/kpi_market_trends';

GRANT ADMIN ON TABLE realty.gold.kpi_market_trends TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS realty.gold.kpi_assessment_accuracy (
  county              STRING      NOT NULL,
  property_type       STRING      NOT NULL,
  assessment_year     INT         NOT NULL,
  total_sales         INT,
  avg_assessed_value  DECIMAL(14,2),
  avg_sale_price      DECIMAL(14,2),
  avg_ratio           DECIMAL(5,2),
  median_ratio        DECIMAL(5,2),
  outlier_count       INT,
  outlier_rate_pct    DECIMAL(5,2),
  cod                 DECIMAL(5,2)
) LOCATION 'realty/gold/property/kpi_assessment_accuracy';

GRANT ADMIN ON TABLE realty.gold.kpi_assessment_accuracy TO USER admin;
