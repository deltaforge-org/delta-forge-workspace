-- =============================================================================
-- Legal Case Management Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE legal_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Legal Case Management'
  SCHEDULE 'legal_daily_schedule'
  TAGS 'setup', 'legal-case-management'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (party_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'legal_pii_salt_2024');

CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_email) TRANSFORM mask PARAMS (show = 3);

CREATE PSEUDONYMISATION RULE ON legal.bronze.raw_parties (contact_phone) TRANSFORM mask PARAMS (show = 4);
