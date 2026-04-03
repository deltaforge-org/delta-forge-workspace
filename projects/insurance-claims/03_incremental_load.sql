-- =============================================================================
-- Insurance Claims Pipeline - Incremental Load (Full Reload Pattern)
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "claim_id > 'C0052' AND incident_date > '2024-01-05'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.claims_enriched, claim_id, incident_date, 7)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.claims_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_claims
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.claims_enriched, claim_id, incident_date, 7)}};

-- ===================== STEP 1: Capture Current State =====================

SELECT COUNT(*) AS pre_claim_count FROM {{zone_prefix}}.silver.claims_enriched;
SELECT COUNT(*) AS pre_fact_count FROM {{zone_prefix}}.gold.fact_claims;
SELECT MAX(processed_at) AS current_watermark FROM {{zone_prefix}}.silver.claims_enriched;

-- ===================== STEP 2: Simulate New Bronze Data (policy change + new claims) =====================

-- A policy premium increase arrives
INSERT INTO {{zone_prefix}}.bronze.raw_policies VALUES
    ('POL003', 'Chris Okafor', 'auto', 1150.00, 'South', 3.1, '2024-01-15', 'premium_increase', '2024-01-22T00:00:00'),
    ('POL010', 'Julia Fernandez', 'auto_plus', 1200.00, 'West', 2.2, '2024-01-15', 'coverage_upgrade', '2024-01-22T00:00:00');

-- New claims arrive
INSERT INTO {{zone_prefix}}.bronze.raw_claims VALUES
    ('C0053', 'POL003', 'CLM03', 'ADJ02', '2024-01-18', '2024-01-19', 4800.00, NULL, 'open', NULL, 'Rear bumper damage parking lot', '2024-01-22T00:00:00'),
    ('C0054', 'POL010', 'CLM10', 'ADJ02', '2024-01-20', '2024-01-21', 8200.00, NULL, 'open', NULL, 'Collision at intersection', '2024-01-22T00:00:00'),
    ('C0055', 'POL004', 'CLM04', 'ADJ04', '2024-01-15', '2024-01-16', 12500.00, NULL, 'under_review', NULL, 'MRI and specialist referral', '2024-01-22T00:00:00'),
    ('C0056', 'POL007', 'CLM07', 'ADJ01', '2024-01-12', '2024-01-14', 16000.00, NULL, 'open', NULL, 'Bathroom flood damage', '2024-01-22T00:00:00'),
    ('C0057', 'POL005', 'CLM05', 'ADJ03', '2024-01-16', '2024-01-18', 55000.00, NULL, 'open', NULL, 'Major liability incident', '2024-01-22T00:00:00');

-- Also update a settled claim
INSERT INTO {{zone_prefix}}.bronze.raw_claims VALUES
    ('C0025', 'POL001', 'CLM01', 'ADJ02', '2023-12-10', '2023-12-12', 11000.00, 9200.00, 'settled', '2024-01-18', 'Multi-vehicle accident - SETTLED', '2024-01-22T00:00:00');

-- ===================== STEP 3: Re-run SCD2 Two-Pass MERGE for Policies =====================

-- PASS 1: Expire old versions
MERGE INTO {{zone_prefix}}.silver.dim_policy_scd2 AS target
USING (
    WITH ranked AS (
        SELECT
            policy_id,
            holder_name,
            coverage_type,
            annual_premium,
            region,
            risk_score,
            effective_date,
            LEAD(effective_date) OVER (PARTITION BY policy_id ORDER BY effective_date) AS next_effective_date,
            ROW_NUMBER() OVER (ORDER BY policy_id, effective_date) AS surrogate_key
        FROM {{zone_prefix}}.bronze.raw_policies
    )
    SELECT
        surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
        region, risk_score,
        effective_date AS valid_from,
        CASE WHEN next_effective_date IS NOT NULL THEN next_effective_date ELSE NULL END AS valid_to,
        CASE WHEN next_effective_date IS NULL THEN 1 ELSE 0 END AS is_current
    FROM ranked
) AS source
ON target.policy_id = source.policy_id AND target.valid_from = source.valid_from
WHEN MATCHED AND source.is_current = 0 THEN UPDATE SET
    valid_to     = source.valid_to,
    is_current   = 0,
    processed_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
    region, risk_score, valid_from, valid_to, is_current, processed_at
) VALUES (
    source.surrogate_key, source.policy_id, source.holder_name, source.coverage_type,
    source.annual_premium, source.region, source.risk_score, source.valid_from,
    source.valid_to, source.is_current, CURRENT_TIMESTAMP
);

-- PASS 2: Ensure current versions are up to date
MERGE INTO {{zone_prefix}}.silver.dim_policy_scd2 AS target
USING (
    WITH latest AS (
        SELECT policy_id, holder_name, coverage_type, annual_premium,
               region, risk_score, effective_date AS valid_from,
               ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY effective_date DESC) AS rn
        FROM {{zone_prefix}}.bronze.raw_policies
    )
    SELECT * FROM latest WHERE rn = 1
) AS source
ON target.policy_id = source.policy_id AND target.valid_from = source.valid_from AND target.is_current = 1
WHEN MATCHED THEN UPDATE SET
    coverage_type  = source.coverage_type,
    annual_premium = source.annual_premium,
    risk_score     = source.risk_score,
    valid_to       = NULL,
    is_current     = 1,
    processed_at   = CURRENT_TIMESTAMP;

-- ===================== STEP 4: Re-process Claims =====================

MERGE INTO {{zone_prefix}}.silver.claims_enriched AS target
USING (
    SELECT
        c.claim_id, c.policy_id, c.claimant_id, c.adjuster_id,
        c.incident_date, c.reported_date, c.claim_amount, c.approved_amount,
        c.status, c.settlement_date,
        CASE WHEN c.settlement_date IS NOT NULL THEN DATEDIFF(c.settlement_date, c.incident_date) ELSE NULL END AS days_to_settle,
        DATEDIFF(c.reported_date, c.incident_date) AS days_to_report,
        p.coverage_type, p.region
    FROM {{zone_prefix}}.bronze.raw_claims c
    JOIN {{zone_prefix}}.silver.dim_policy_scd2 p
        ON c.policy_id = p.policy_id
        AND c.incident_date >= p.valid_from
        AND (p.valid_to IS NULL OR c.incident_date < p.valid_to)
    WHERE c.ingested_at >= '2024-01-22T00:00:00'
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET
    approved_amount = source.approved_amount,
    status          = source.status,
    settlement_date = source.settlement_date,
    days_to_settle  = source.days_to_settle,
    coverage_type   = source.coverage_type,
    region          = source.region,
    processed_at    = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    claim_id, policy_id, claimant_id, adjuster_id, incident_date, reported_date,
    claim_amount, approved_amount, status, settlement_date, days_to_settle,
    days_to_report, coverage_type, region, processed_at
) VALUES (
    source.claim_id, source.policy_id, source.claimant_id, source.adjuster_id,
    source.incident_date, source.reported_date, source.claim_amount, source.approved_amount,
    source.status, source.settlement_date, source.days_to_settle, source.days_to_report,
    source.coverage_type, source.region, CURRENT_TIMESTAMP
);

-- ===================== STEP 5: Verify Incremental =====================

SELECT COUNT(*) AS post_claim_count FROM {{zone_prefix}}.silver.claims_enriched;

-- Should have added 5 new claims (C0025 was updated, not added)
SELECT post.cnt - pre.cnt AS new_claims_added
FROM (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.claims_enriched) post,
     (SELECT 52 AS cnt) pre;

-- Verify C0025 was updated to settled
ASSERT VALUE new_claims_added = 5
SELECT status AS c0025_status FROM {{zone_prefix}}.silver.claims_enriched WHERE claim_id = 'C0025';
-- Verify SCD2 grew
ASSERT VALUE c0025_status = 'settled'
SELECT COUNT(*) AS post_scd2_count FROM {{zone_prefix}}.silver.dim_policy_scd2;
-- Verify no duplicate claims
ASSERT VALUE post_scd2_count = 24
SELECT COUNT(*) AS dup_check
FROM (
    SELECT claim_id, COUNT(*) AS cnt
    FROM {{zone_prefix}}.silver.claims_enriched
    GROUP BY claim_id
    HAVING COUNT(*) > 1
);
ASSERT VALUE dup_check = 0
SELECT 'dup_check check passed' AS dup_check_status;

