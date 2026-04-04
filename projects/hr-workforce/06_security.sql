-- =============================================================================
-- HR Workforce Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE hr_security
  DESCRIPTION 'Creates pseudonymisation and security rules for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES (all 5 transforms) =====================

CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (email) TRANSFORM mask PARAMS (show = 3);

CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (employee_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'hr_salt_2024');

CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (date_of_birth) TRANSFORM generalize PARAMS (range = 10);

CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (base_salary) TRANSFORM redact PARAMS (mask = '[REDACTED]');
