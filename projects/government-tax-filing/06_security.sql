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

DROP PSEUDONYMISATION RULE IF EXISTS ON tax.silver.taxpayer_profiles (ssn);
CREATE PSEUDONYMISATION RULE ON tax.silver.taxpayer_profiles (ssn) TRANSFORM redact PARAMS (mask = '[REDACTED]');

DROP PSEUDONYMISATION RULE IF EXISTS ON tax.silver.taxpayer_profiles (taxpayer_name);
CREATE PSEUDONYMISATION RULE ON tax.silver.taxpayer_profiles (taxpayer_name) TRANSFORM mask PARAMS (show = 1);

-- ===================== BLOOM FILTER INDEX =====================

DROP BLOOMFILTER INDEX IF EXISTS ON tax.silver.filings_immutable (taxpayer_id);
CREATE BLOOMFILTER INDEX ON tax.silver.filings_immutable FOR COLUMNS (taxpayer_id);
