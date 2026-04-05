-- =============================================================================
-- HR Workforce Analytics Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================

PIPELINE 10_cleanup
  DESCRIPTION 'Cleanup pipeline for HR Workforce - drops all objects. DISABLED by default.'
  SCHEDULE 'hr_daily_schedule'
  TAGS 'cleanup', 'maintenance', 'hr-workforce'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== DROP PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (ssn);
DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (email);
DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (employee_name);
DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (date_of_birth);
DROP PSEUDONYMISATION RULE IF EXISTS ON hr.silver.employee_dim (base_salary);

-- ===================== DROP GOLD TABLES =====================

DROP DELTA TABLE IF EXISTS hr.gold.kpi_retention_risk WITH FILES;
DROP DELTA TABLE IF EXISTS hr.gold.kpi_workforce_analytics WITH FILES;
DROP DELTA TABLE IF EXISTS hr.gold.fact_compensation WITH FILES;
DROP DELTA TABLE IF EXISTS hr.gold.dim_position WITH FILES;
DROP DELTA TABLE IF EXISTS hr.gold.dim_department WITH FILES;

-- ===================== DROP SILVER TABLES =====================

DROP DELTA TABLE IF EXISTS hr.silver.org_change_log WITH FILES;
DROP DELTA TABLE IF EXISTS hr.silver.comp_events_enriched WITH FILES;
DROP DELTA TABLE IF EXISTS hr.silver.employee_dim WITH FILES;

-- ===================== DROP BRONZE TABLES =====================

DROP DELTA TABLE IF EXISTS hr.bronze.raw_comp_events WITH FILES;
DROP DELTA TABLE IF EXISTS hr.bronze.raw_positions WITH FILES;
DROP DELTA TABLE IF EXISTS hr.bronze.raw_departments WITH FILES;
DROP DELTA TABLE IF EXISTS hr.bronze.raw_employees WITH FILES;

-- ===================== DROP SCHEMAS =====================

DROP SCHEMA IF EXISTS hr.gold;
DROP SCHEMA IF EXISTS hr.silver;
DROP SCHEMA IF EXISTS hr.bronze;

-- ===================== DROP ZONES =====================

DROP ZONE IF EXISTS hr;
