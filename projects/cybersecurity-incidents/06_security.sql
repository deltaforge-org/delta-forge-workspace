-- =============================================================================
-- Cybersecurity Incidents Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE cyber_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Cybersecurity Incidents'
  SCHEDULE 'cyber_15min_schedule'
  TAGS 'setup', 'cybersecurity-incidents'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON cyber.gold.dim_source_ip (ip_address) TRANSFORM generalize PARAMS (range = 24);
