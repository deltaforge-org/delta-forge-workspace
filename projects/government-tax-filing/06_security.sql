-- =============================================================================
-- Government Tax Filing Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE tax_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Government Tax Filing'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'setup', 'government-tax-filing'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON tax.silver.taxpayer_profiles (ssn) TRANSFORM redact PARAMS (mask = '[REDACTED]');
CREATE PSEUDONYMISATION RULE ON tax.silver.taxpayer_profiles (taxpayer_name) TRANSFORM mask PARAMS (show = 1);

-- ===================== BLOOM FILTER INDEX =====================

CREATE BLOOMFILTER INDEX ON tax.silver.filings_immutable FOR COLUMNS (taxpayer_id);
