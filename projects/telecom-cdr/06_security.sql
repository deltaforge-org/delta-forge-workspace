-- =============================================================================
-- Telecom CDR Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE telecom_cdr_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Telecom CDR'
  SCHEDULE 'telecom_daily_schedule'
  TAGS 'setup', 'telecom-cdr'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON telco.bronze.raw_subscribers (phone_number);
CREATE PSEUDONYMISATION RULE ON telco.bronze.raw_subscribers (phone_number) TRANSFORM generalize PARAMS (range = 4);
DROP PSEUDONYMISATION RULE IF EXISTS ON telco.silver.cdr_unified (caller);
CREATE PSEUDONYMISATION RULE ON telco.silver.cdr_unified (caller) TRANSFORM generalize PARAMS (range = 4);
DROP PSEUDONYMISATION RULE IF EXISTS ON telco.silver.cdr_unified (callee);
CREATE PSEUDONYMISATION RULE ON telco.silver.cdr_unified (callee) TRANSFORM generalize PARAMS (range = 4);
DROP PSEUDONYMISATION RULE IF EXISTS ON telco.gold.kpi_churn_risk (phone_number);
CREATE PSEUDONYMISATION RULE ON telco.gold.kpi_churn_risk (phone_number) TRANSFORM generalize PARAMS (range = 4);

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE telco.bronze.raw_cdr_v1 TO USER admin;
GRANT ADMIN ON TABLE telco.bronze.raw_cdr_v2 TO USER admin;
GRANT ADMIN ON TABLE telco.bronze.raw_cdr_v3 TO USER admin;
GRANT ADMIN ON TABLE telco.bronze.raw_subscribers TO USER admin;
GRANT ADMIN ON TABLE telco.bronze.raw_cell_towers TO USER admin;
GRANT ADMIN ON TABLE telco.silver.cdr_unified TO USER admin;
GRANT ADMIN ON TABLE telco.silver.subscriber_profiles TO USER admin;
GRANT ADMIN ON TABLE telco.silver.sessions TO USER admin;
GRANT ADMIN ON TABLE telco.gold.dim_subscriber TO USER admin;
GRANT ADMIN ON TABLE telco.gold.dim_tower TO USER admin;
GRANT ADMIN ON TABLE telco.gold.dim_plan TO USER admin;
GRANT ADMIN ON TABLE telco.gold.fact_calls TO USER admin;
GRANT ADMIN ON TABLE telco.gold.kpi_network_quality TO USER admin;
GRANT ADMIN ON TABLE telco.gold.kpi_churn_risk TO USER admin;
