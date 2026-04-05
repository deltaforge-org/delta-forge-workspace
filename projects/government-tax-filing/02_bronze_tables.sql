-- =============================================================================
-- Government Tax Filing Pipeline - Bronze Table Definitions
-- =============================================================================

PIPELINE government_tax_filing_02_bronze_tables
  DESCRIPTION 'Creates bronze layer tables for Government Tax Filing'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'setup', 'government-tax-filing'
  LIFECYCLE production
;

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS tax.bronze.raw_filings (
  filing_id           STRING      NOT NULL,
  taxpayer_id         STRING      NOT NULL,
  jurisdiction_id     STRING      NOT NULL,
  preparer_id         STRING,
  fiscal_year         INT         NOT NULL,
  filing_date         STRING      NOT NULL,
  gross_income        DECIMAL(14,2),
  deductions          DECIMAL(14,2),
  taxable_income      DECIMAL(14,2),
  tax_owed            DECIMAL(14,2),
  tax_paid            DECIMAL(14,2),
  refund_amount       DECIMAL(14,2),
  filing_status       STRING,
  filing_type         STRING,
  notes               STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'tax/bronze/filing/raw_filings';

GRANT ADMIN ON TABLE tax.bronze.raw_filings TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.bronze.raw_amendments (
  amendment_id        STRING      NOT NULL,
  original_filing_id  STRING      NOT NULL,
  taxpayer_id         STRING      NOT NULL,
  amendment_date      STRING      NOT NULL,
  amended_gross_income    DECIMAL(14,2),
  amended_deductions      DECIMAL(14,2),
  amended_taxable_income  DECIMAL(14,2),
  amended_tax_owed        DECIMAL(14,2),
  amendment_reason    STRING,
  preparer_id         STRING,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'tax/bronze/filing/raw_amendments';

GRANT ADMIN ON TABLE tax.bronze.raw_amendments TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.bronze.raw_taxpayers (
  taxpayer_id         STRING      NOT NULL,
  taxpayer_name       STRING,
  ssn                 STRING,
  filing_type         STRING,
  state               STRING,
  dependent_count     INT,
  annual_income       DECIMAL(14,2),
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'tax/bronze/filing/raw_taxpayers';

GRANT ADMIN ON TABLE tax.bronze.raw_taxpayers TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.bronze.raw_jurisdictions (
  jurisdiction_id     STRING      NOT NULL,
  jurisdiction_name   STRING      NOT NULL,
  jurisdiction_level  STRING,
  state               STRING,
  base_tax_rate       DECIMAL(5,4),
  standard_deduction  DECIMAL(10,2),
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'tax/bronze/filing/raw_jurisdictions';

GRANT ADMIN ON TABLE tax.bronze.raw_jurisdictions TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.bronze.raw_preparers (
  preparer_id         STRING      NOT NULL,
  preparer_name       STRING      NOT NULL,
  firm                STRING,
  certification       STRING,
  years_experience    INT,
  ingested_at         TIMESTAMP   NOT NULL
) LOCATION 'tax/bronze/filing/raw_preparers';

GRANT ADMIN ON TABLE tax.bronze.raw_preparers TO USER admin;
