-- =============================================================================
-- Cybersecurity Incidents Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE cybersecurity_incidents_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON cyber.gold.dim_source_ip (ip_address);
CREATE PSEUDONYMISATION RULE ON cyber.gold.dim_source_ip (ip_address) TRANSFORM generalize PARAMS (range = 24);
