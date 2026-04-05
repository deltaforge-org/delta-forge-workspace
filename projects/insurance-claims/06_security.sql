-- =============================================================================
-- Insurance Claims Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE insurance_claims_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Insurance Claims'
  SCHEDULE 'ins_weekly_schedule'
  TAGS 'setup', 'insurance-claims'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON ins.gold.dim_claimant (name);
CREATE PSEUDONYMISATION RULE ON ins.gold.dim_claimant (name) TRANSFORM mask PARAMS (show = 1);

DROP PSEUDONYMISATION RULE IF EXISTS ON ins.bronze.raw_claimants (ssn);
CREATE PSEUDONYMISATION RULE ON ins.bronze.raw_claimants (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

DROP PSEUDONYMISATION RULE IF EXISTS ON ins.bronze.raw_policies (ssn);
CREATE PSEUDONYMISATION RULE ON ins.bronze.raw_policies (ssn) TRANSFORM redact PARAMS (mask = '***-**-****');

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE ins.bronze.raw_policies TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_claims TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_claimants TO USER admin;
GRANT ADMIN ON TABLE ins.bronze.raw_adjusters TO USER admin;
GRANT ADMIN ON TABLE ins.silver.policy_dim TO USER admin;
GRANT ADMIN ON TABLE ins.silver.claims_enriched TO USER admin;
GRANT ADMIN ON TABLE ins.silver.actuarial_snapshots TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_claimant TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_adjuster TO USER admin;
GRANT ADMIN ON TABLE ins.gold.dim_coverage_type TO USER admin;
GRANT ADMIN ON TABLE ins.gold.fact_claims TO USER admin;
GRANT ADMIN ON TABLE ins.gold.kpi_loss_ratios TO USER admin;
GRANT ADMIN ON TABLE ins.gold.kpi_adjuster_performance TO USER admin;
