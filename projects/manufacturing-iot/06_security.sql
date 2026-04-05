-- =============================================================================
-- Manufacturing IoT Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE manufacturing_iot_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Manufacturing IoT'
  SCHEDULE 'manufacturing_2hr_schedule'
  TAGS 'setup', 'manufacturing-iot'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON mfg.bronze.raw_shifts (supervisor);
CREATE PSEUDONYMISATION RULE ON mfg.bronze.raw_shifts (supervisor) TRANSFORM mask PARAMS (chars = 4);

-- ===================== GRANTS =====================

GRANT ADMIN ON TABLE mfg.bronze.raw_sensors TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_production_lines TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_shifts TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_production_targets TO USER admin;
GRANT ADMIN ON TABLE mfg.bronze.raw_readings TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.readings_validated TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.readings_smoothed TO USER admin;
GRANT ADMIN ON TABLE mfg.silver.equipment_status TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_sensor TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_line TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.dim_shift TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.fact_readings TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.kpi_oee TO USER admin;
GRANT ADMIN ON TABLE mfg.gold.kpi_anomaly_trends TO USER admin;
