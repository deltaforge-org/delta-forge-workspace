-- =============================================================================
-- Government Tax Filing Pipeline - Gold Table Definitions
-- =============================================================================

PIPELINE 05_gold_tables
  DESCRIPTION 'Creates gold layer tables for Government Tax Filing'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'setup', 'government-tax-filing'
  LIFECYCLE production
;

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS tax.gold.dim_taxpayer (
  taxpayer_key        INT         NOT NULL,
  taxpayer_id         STRING      NOT NULL,
  filing_type         STRING,
  state               STRING,
  income_bracket      STRING,
  dependent_count     INT,
  total_filings       INT,
  ever_audited        BOOLEAN
) LOCATION 'tax/gold/filing/dim_taxpayer';

GRANT ADMIN ON TABLE tax.gold.dim_taxpayer TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.gold.dim_jurisdiction (
  jurisdiction_key    INT         NOT NULL,
  jurisdiction_id     STRING      NOT NULL,
  jurisdiction_name   STRING      NOT NULL,
  jurisdiction_level  STRING,
  state               STRING,
  base_tax_rate       DECIMAL(5,4),
  standard_deduction  DECIMAL(10,2)
) LOCATION 'tax/gold/filing/dim_jurisdiction';

GRANT ADMIN ON TABLE tax.gold.dim_jurisdiction TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.gold.dim_preparer (
  preparer_key        INT         NOT NULL,
  preparer_id         STRING      NOT NULL,
  preparer_name       STRING      NOT NULL,
  firm                STRING,
  certification       STRING,
  years_experience    INT,
  total_filings_prepared  INT,
  amendment_rate      DECIMAL(5,2)
) LOCATION 'tax/gold/filing/dim_preparer';

GRANT ADMIN ON TABLE tax.gold.dim_preparer TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.gold.dim_fiscal_year (
  fiscal_year_key     INT         NOT NULL,
  fiscal_year         INT         NOT NULL,
  filing_deadline     DATE,
  total_filings       INT,
  total_amendments    INT,
  audit_flag_count    INT
) LOCATION 'tax/gold/filing/dim_fiscal_year';

GRANT ADMIN ON TABLE tax.gold.dim_fiscal_year TO USER admin;

-- Star-schema fact: joins originals + amendments via COALESCE
CREATE DELTA TABLE IF NOT EXISTS tax.gold.fact_filings (
  filing_key              INT         NOT NULL,
  taxpayer_key            INT         NOT NULL,
  jurisdiction_key        INT         NOT NULL,
  preparer_key            INT,
  fiscal_year_key         INT         NOT NULL,
  fiscal_year             INT,
  filing_date             DATE,
  gross_income            DECIMAL(14,2),
  deductions              DECIMAL(14,2),
  taxable_income          DECIMAL(14,2),
  tax_owed                DECIMAL(14,2),
  tax_paid                DECIMAL(14,2),
  refund_amount           DECIMAL(14,2),
  effective_taxable_income DECIMAL(14,2),
  effective_tax_owed       DECIMAL(14,2),
  was_amended             BOOLEAN,
  amendment_reason        STRING,
  amendment_count         INT,
  effective_tax_rate      DECIMAL(7,4),
  audit_flag              BOOLEAN,
  filing_status           STRING
) LOCATION 'tax/gold/filing/fact_filings';

GRANT ADMIN ON TABLE tax.gold.fact_filings TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.gold.kpi_revenue_analysis (
  fiscal_year         INT         NOT NULL,
  jurisdiction_name   STRING      NOT NULL,
  total_filings       INT,
  total_taxable_income DECIMAL(16,2),
  total_tax_collected DECIMAL(16,2),
  total_refunds       DECIMAL(16,2),
  avg_effective_rate  DECIMAL(7,4),
  audit_flag_count    INT,
  compliance_rate     DECIMAL(5,2),
  amendment_count     INT,
  audit_yield_pct     DECIMAL(5,2)
) LOCATION 'tax/gold/filing/kpi_revenue_analysis';

GRANT ADMIN ON TABLE tax.gold.kpi_revenue_analysis TO USER admin;

CREATE DELTA TABLE IF NOT EXISTS tax.gold.kpi_preparer_quality (
  preparer_name       STRING      NOT NULL,
  firm                STRING,
  certification       STRING,
  total_filings       INT,
  amendment_count     INT,
  amendment_rate_pct  DECIMAL(5,2),
  audit_count         INT,
  audit_rate_pct      DECIMAL(5,2),
  avg_client_income   DECIMAL(14,2),
  avg_deduction_pct   DECIMAL(5,2),
  avg_effective_rate  DECIMAL(7,4)
) LOCATION 'tax/gold/filing/kpi_preparer_quality';

GRANT ADMIN ON TABLE tax.gold.kpi_preparer_quality TO USER admin;
