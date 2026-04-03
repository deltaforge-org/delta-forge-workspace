-- =============================================================================
-- Nonprofit Donations Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using INCREMENTAL_FILTER macro.
-- Inserts new donations (July 2024 batch), refreshes tiers, verifies.

-- Show current watermark
SELECT MAX(donation_id) AS max_donation_id, MAX(enriched_at) AS latest_enriched
FROM {{zone_prefix}}.silver.donations_enriched;

-- Current row counts
SELECT 'silver.donations_enriched' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.donations_enriched
UNION ALL
SELECT 'gold.fact_donations', COUNT(*)
FROM {{zone_prefix}}.gold.fact_donations;

-- =============================================================================
-- Insert 8 new donations (July 2024 incremental batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_donations VALUES
('DON-061', 'DNR-001', 'CMP-004', 'FND-001', '2024-07-15', 500.00,   'credit_card', true,  true, true,  '2024-07-15T00:00:00'),
('DON-062', 'DNR-002', 'CMP-004', 'FND-001', '2024-07-01', 250.00,   'credit_card', true,  true, true,  '2024-07-01T00:00:00'),
('DON-063', 'DNR-003', 'CMP-005', 'FND-003', '2024-07-10', 40000.00, 'wire',        false, true, true,  '2024-07-10T00:00:00'),
('DON-064', 'DNR-015', 'CMP-004', 'FND-002', '2024-07-05', 50.00,    'credit_card', false, true, false, '2024-07-05T00:00:00'),
('DON-065', 'DNR-009', 'CMP-004', 'FND-004', '2024-07-20', 200.00,   'check',       false, true, true,  '2024-07-20T00:00:00'),
('DON-066', 'DNR-013', 'CMP-005', 'FND-003', '2024-07-15', 5000.00,  'credit_card', false, true, true,  '2024-07-15T00:00:00'),
('DON-067', 'DNR-010', 'CMP-004', 'FND-002', '2024-07-15', 200.00,   'credit_card', true,  true, true,  '2024-07-15T00:00:00'),
('DON-068', 'DNR-005', 'CMP-005', 'FND-003', '2024-07-25', 30000.00, 'wire',        false, true, true,  '2024-07-25T00:00:00');

ASSERT ROW_COUNT = 8
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental donor tier refresh (CDF will capture any tier changes)
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.donors_tiered AS tgt
USING (
    WITH donor_metrics AS (
        SELECT
            d.donor_id, d.name, d.email, d.donor_type, d.acquisition_source,
            d.first_donation_date,
            CAST(COALESCE(SUM(dn.amount), 0) AS DECIMAL(14,2)) AS lifetime_amount,
            COUNT(dn.donation_id) AS donation_count,
            MAX(dn.donation_date) AS last_donation_date
        FROM {{zone_prefix}}.bronze.raw_donors d
        LEFT JOIN {{zone_prefix}}.bronze.raw_donations dn ON d.donor_id = dn.donor_id
        GROUP BY d.donor_id, d.name, d.email, d.donor_type, d.acquisition_source, d.first_donation_date
    )
    SELECT
        dm.*,
        CASE
            WHEN dm.lifetime_amount >= 10000 THEN 'platinum'
            WHEN dm.lifetime_amount >= 2000 THEN 'gold'
            WHEN dm.lifetime_amount >= 500 THEN 'silver'
            ELSE 'bronze'
        END AS current_tier,
        CASE WHEN EXISTS (
            SELECT 1 FROM {{zone_prefix}}.bronze.raw_donations r
            WHERE r.donor_id = dm.donor_id AND EXTRACT(YEAR FROM r.donation_date) = 2024
        ) AND EXISTS (
            SELECT 1 FROM {{zone_prefix}}.bronze.raw_donations r
            WHERE r.donor_id = dm.donor_id AND EXTRACT(YEAR FROM r.donation_date) = 2023
        ) THEN true ELSE false END AS is_retained,
        CASE WHEN dm.last_donation_date < CAST('2023-07-01' AS DATE) AND dm.donation_count > 0
            THEN true ELSE false END AS is_lapsed
    FROM donor_metrics dm
) AS src
ON tgt.donor_id = src.donor_id
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_amount    = src.lifetime_amount,
    tgt.previous_tier      = tgt.current_tier,
    tgt.current_tier       = src.current_tier,
    tgt.tier_since_date    = CASE WHEN tgt.current_tier != src.current_tier THEN CURRENT_DATE ELSE tgt.tier_since_date END,
    tgt.donation_count     = src.donation_count,
    tgt.last_donation_date = src.last_donation_date,
    tgt.is_retained        = src.is_retained,
    tgt.is_lapsed          = src.is_lapsed,
    tgt.enriched_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    donor_id, name, email, donor_type, acquisition_source, first_donation_date,
    lifetime_amount, current_tier, tier_since_date, previous_tier,
    donation_count, last_donation_date, is_retained, is_lapsed, enriched_at
) VALUES (
    src.donor_id, src.name, src.email, src.donor_type, src.acquisition_source,
    src.first_donation_date, src.lifetime_amount, src.current_tier, CURRENT_DATE,
    NULL, src.donation_count, src.last_donation_date, src.is_retained, src.is_lapsed,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- Incremental donation enrichment using INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.donations_enriched AS tgt
USING (
    SELECT
        d.donation_id, d.donor_id, d.campaign_id, d.fund_id, d.donation_date,
        d.amount, d.payment_method, d.is_recurring, d.tax_deductible_flag,
        d.acknowledgment_sent,
        dt.current_tier AS donor_tier,
        CASE WHEN d.amount >= 10000 THEN true ELSE false END AS is_major_gift,
        d.ingested_at
    FROM {{zone_prefix}}.bronze.raw_donations d
    LEFT JOIN {{zone_prefix}}.silver.donors_tiered dt ON d.donor_id = dt.donor_id
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.donations_enriched, donation_id, donation_date, 7)}}
) AS src
ON tgt.donation_id = src.donation_id
WHEN MATCHED THEN UPDATE SET
    tgt.donor_tier   = src.donor_tier,
    tgt.is_major_gift = src.is_major_gift,
    tgt.enriched_at  = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    donation_id, donor_id, campaign_id, fund_id, donation_date, amount,
    payment_method, is_recurring, tax_deductible_flag, acknowledgment_sent,
    donor_tier, is_major_gift, enriched_at
) VALUES (
    src.donation_id, src.donor_id, src.campaign_id, src.fund_id, src.donation_date,
    src.amount, src.payment_method, src.is_recurring, src.tax_deductible_flag,
    src.acknowledgment_sent, src.donor_tier, src.is_major_gift, src.ingested_at
);

-- =============================================================================
-- Incremental gold fact refresh
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_donations AS tgt
USING (
    SELECT
        donation_id AS donation_key, donor_id AS donor_key,
        campaign_id AS campaign_key, fund_id AS fund_key,
        donation_date, amount, payment_method, is_recurring,
        tax_deductible_flag, acknowledgment_sent
    FROM {{zone_prefix}}.silver.donations_enriched
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_donations, donation_key, donation_date, 7)}}
) AS src
ON tgt.donation_key = src.donation_key
WHEN MATCHED THEN UPDATE SET tgt.amount = src.amount
WHEN NOT MATCHED THEN INSERT (
    donation_key, donor_key, campaign_key, fund_key, donation_date,
    amount, payment_method, is_recurring, tax_deductible_flag, acknowledgment_sent
) VALUES (
    src.donation_key, src.donor_key, src.campaign_key, src.fund_key,
    src.donation_date, src.amount, src.payment_method, src.is_recurring,
    src.tax_deductible_flag, src.acknowledgment_sent
);

-- Refresh dim_donor with updated tiers
MERGE INTO {{zone_prefix}}.gold.dim_donor AS tgt
USING (
    SELECT donor_id AS donor_key, donor_id, name, email, donor_type,
           acquisition_source, first_donation_date, lifetime_amount,
           current_tier, tier_since_date
    FROM {{zone_prefix}}.silver.donors_tiered
) AS src
ON tgt.donor_key = src.donor_key
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_amount = src.lifetime_amount,
    tgt.current_tier    = src.current_tier,
    tgt.tier_since_date = src.tier_since_date
WHEN NOT MATCHED THEN INSERT (
    donor_key, donor_id, name, email, donor_type, acquisition_source,
    first_donation_date, lifetime_amount, current_tier, tier_since_date
) VALUES (
    src.donor_key, src.donor_id, src.name, src.email, src.donor_type,
    src.acquisition_source, src.first_donation_date, src.lifetime_amount,
    src.current_tier, src.tier_since_date
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.donations_enriched;
ASSERT VALUE silver_total = 68

SELECT COUNT(*) AS gold_fact_total FROM {{zone_prefix}}.gold.fact_donations;
-- Verify July donations exist
ASSERT VALUE gold_fact_total = 68
SELECT COUNT(*) AS july_donations
FROM {{zone_prefix}}.gold.fact_donations
WHERE donation_date >= '2024-07-01';

-- Verify CDF captured tier changes
ASSERT VALUE july_donations = 8
SELECT COUNT(*) AS cdf_changes
FROM table_changes('{{zone_prefix}}.silver.donors_tiered', 1);

-- Time travel: view donor tiers at previous version
SELECT donor_id, current_tier, lifetime_amount
FROM {{zone_prefix}}.silver.donors_tiered VERSION AS OF 1
ORDER BY lifetime_amount DESC
LIMIT 5;
