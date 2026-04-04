-- =============================================================================
-- Healthcare Patient EHR Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE ehr_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Healthcare Patient EHR'
  SCHEDULE 'ehr_daily_schedule'
  TAGS 'setup', 'healthcare-patient-ehr'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES (ALL 4 TRANSFORMS) =====================

CREATE PSEUDONYMISATION RULE ON ehr.silver.patient_dim (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON ehr.silver.patient_dim (email) TRANSFORM mask PARAMS (show = 3);

CREATE PSEUDONYMISATION RULE ON ehr.silver.patient_dim (patient_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'hipaa_salt_2024');

CREATE PSEUDONYMISATION RULE ON ehr.silver.patient_dim (date_of_birth) TRANSFORM generalize PARAMS (range = 10);
