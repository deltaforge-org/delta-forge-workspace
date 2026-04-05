-- =============================================================================
-- HR Workforce Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE hr_workforce_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for HR Workforce'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'setup', 'hr-workforce'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES (all 5 transforms) =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (ssn);
CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (email);
CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (email) TRANSFORM mask PARAMS (show = 3);

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (employee_name);
CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (employee_name) TRANSFORM keyed_hash SCOPE person PARAMS (salt = 'hr_salt_2024');

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (date_of_birth);
CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (date_of_birth) TRANSFORM generalize PARAMS (range = 10);

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (base_salary);
CREATE PSEUDONYMISATION RULE ON hr.silver.employee_dim (base_salary) TRANSFORM redact PARAMS (mask = '[REDACTED]');
