-- =============================================================================
-- Legal Case Management Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE 06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON legal.bronze.raw_parties (ssn);
CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

DROP PSEUDONYMISATION RULE IF EXISTS ON legal.bronze.raw_parties (party_name);
CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (party_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'legal_pii_salt_2024');

DROP PSEUDONYMISATION RULE IF EXISTS ON legal.bronze.raw_parties (contact_email);
CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_email) TRANSFORM mask PARAMS (show = 3);

DROP PSEUDONYMISATION RULE IF EXISTS ON legal.bronze.raw_parties (contact_phone);
CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_phone) TRANSFORM mask PARAMS (show = 4);
