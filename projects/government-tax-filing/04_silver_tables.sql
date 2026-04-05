-- =============================================================================
-- Government Tax Filing Pipeline - Silver Table Definitions
-- =============================================================================

PIPELINE government_tax_filing_04_silver_tables
  DESCRIPTION 'Creates silver layer tables for Government Tax Filing'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'setup', 'government-tax-filing'
  LIFECYCLE production
;

-- ===================== SILVER TABLES =====================

-- Append-only: original filings are NEVER updated or deleted.
-- CDF enabled so every INSERT is captured for the audit_trail.
CREATE DELTA TABLE IF NOT EXISTS tax.silver.filings_immutable (
  filing_id           STRING      NOT NULL,
  taxpayer_id         STRING      NOT NULL,
  jurisdiction_id     STRING      NOT NULL,
  preparer_id         STRING,
  fiscal_year         INT         NOT NULL,
  filing_date         DATE        NOT NULL,
  gross_income        DECIMAL(14,2),
  deductions          DECIMAL(14,2),
  taxable_income      DECIMAL(14,2),
  tax_owed            DECIMAL(14,2),
  tax_paid            DECIMAL(14,2),
  refund_amount       DECIMAL(14,2),
  filing_status       STRING,
  filing_type         STRING,
  effective_tax_rate  DECIMAL(7,4),
  deduction_pct       DECIMAL(5,2),
  audit_flag          BOOLEAN,
  income_bracket      STRING,
  loaded_at           TIMESTAMP   NOT NULL
) LOCATION 'tax/silver/filing/filings_immutable'
PARTITIONED BY (fiscal_year)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE tax.silver.filings_immutable TO USER admin;

-- Amendments linked to originals with computed deltas
CREATE DELTA TABLE IF NOT EXISTS tax.silver.amendments_applied (
  amendment_id            STRING      NOT NULL,
  original_filing_id      STRING      NOT NULL,
  taxpayer_id             STRING      NOT NULL,
  amendment_date          DATE        NOT NULL,
  original_taxable_income DECIMAL(14,2),
  amended_taxable_income  DECIMAL(14,2),
  income_delta            DECIMAL(14,2),
  original_tax_owed       DECIMAL(14,2),
  amended_tax_owed        DECIMAL(14,2),
  tax_delta               DECIMAL(14,2),
  amendment_reason        STRING,
  delta_pct               DECIMAL(7,2),
  large_delta_flag        BOOLEAN,
  preparer_id             STRING,
  loaded_at               TIMESTAMP   NOT NULL
) LOCATION 'tax/silver/filing/amendments_applied'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE tax.silver.amendments_applied TO USER admin;

-- Aggregated taxpayer profile from filings
CREATE DELTA TABLE IF NOT EXISTS tax.silver.taxpayer_profiles (
  taxpayer_id         STRING      NOT NULL,
  taxpayer_name       STRING,
  ssn                 STRING,
  filing_type         STRING,
  state               STRING,
  dependent_count     INT,
  total_filings       INT,
  total_amendments    INT,
  lifetime_gross_income   DECIMAL(14,2),
  lifetime_tax_paid       DECIMAL(14,2),
  latest_income_bracket   STRING,
  avg_deduction_pct       DECIMAL(5,2),
  ever_audited            BOOLEAN
) LOCATION 'tax/silver/filing/taxpayer_profiles';

GRANT ADMIN ON TABLE tax.silver.taxpayer_profiles TO USER admin;

-- CDF-driven audit trail — captures every change to filings_immutable and amendments
CREATE DELTA TABLE IF NOT EXISTS tax.silver.audit_trail (
  audit_id            INT         NOT NULL,
  table_name          STRING      NOT NULL,
  operation           STRING      NOT NULL,
  record_key          STRING      NOT NULL,
  taxpayer_id         STRING,
  fiscal_year         INT,
  change_timestamp    TIMESTAMP   NOT NULL,
  details             STRING
) LOCATION 'tax/silver/filing/audit_trail';

GRANT ADMIN ON TABLE tax.silver.audit_trail TO USER admin;
