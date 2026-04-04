-- =============================================================================
-- Real Estate Property Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE realty_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON realty.silver.transactions_enriched (buyer_name) TRANSFORM keyed_hash PARAMS (salt = delta_forge_salt_2024);
CREATE PSEUDONYMISATION RULE ON realty.silver.transactions_enriched (seller_name) TRANSFORM keyed_hash PARAMS (salt = delta_forge_salt_2024);

-- ===================== BLOOM FILTER INDEX =====================

CREATE BLOOMFILTER INDEX ON realty.silver.property_dim FOR COLUMNS (parcel_id);
