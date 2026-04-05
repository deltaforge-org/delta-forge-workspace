-- =============================================================================
-- Real Estate Property Pipeline - Security & Pseudonymisation Rules
-- =============================================================================

PIPELINE real_estate_property_06_security
  DESCRIPTION 'Creates pseudonymisation and security rules for Real Estate Property'
  SCHEDULE 'realty_daily_schedule'
  TAGS 'setup', 'real-estate-property'
  LIFECYCLE production
;

-- ===================== PSEUDONYMISATION RULES =====================

DROP PSEUDONYMISATION RULE IF EXISTS ON realty.silver.transactions_enriched (buyer_name);
CREATE PSEUDONYMISATION RULE ON realty.silver.transactions_enriched (buyer_name) TRANSFORM keyed_hash PARAMS (salt = delta_forge_salt_2024);

DROP PSEUDONYMISATION RULE IF EXISTS ON realty.silver.transactions_enriched (seller_name);
CREATE PSEUDONYMISATION RULE ON realty.silver.transactions_enriched (seller_name) TRANSFORM keyed_hash PARAMS (salt = delta_forge_salt_2024);

-- ===================== BLOOM FILTER INDEX =====================

DROP BLOOMFILTER INDEX IF EXISTS ON realty.silver.property_dim (parcel_id);
CREATE BLOOMFILTER INDEX ON realty.silver.property_dim FOR COLUMNS (parcel_id);
