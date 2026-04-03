-- =============================================================================
-- Insurance Claims Pipeline - Full Load Transformation
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE ins_weekly_schedule CRON '0 7 * * 1' TIMEZONE 'America/Chicago' RETRIES 2 TIMEOUT 7200 MAX_CONCURRENT 1 ACTIVE;

PIPELINE ins_claims_pipeline DESCRIPTION 'Weekly insurance claims pipeline with SCD2 policy tracking and loss ratio analysis' SCHEDULE 'ins_weekly_schedule' TAGS 'insurance,claims,SCD2,loss-ratio' SLA 7200 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP 1: Validate Bronze =====================

SELECT COUNT(*) AS raw_policy_count FROM {{zone_prefix}}.bronze.raw_policies;
ASSERT VALUE raw_policy_count >= 20

SELECT COUNT(*) AS raw_claim_count FROM {{zone_prefix}}.bronze.raw_claims;
ASSERT VALUE raw_claim_count >= 50

SELECT COUNT(*) AS raw_claimant_count FROM {{zone_prefix}}.bronze.raw_claimants;
ASSERT VALUE raw_claimant_count = 10

SELECT COUNT(*) AS raw_adjuster_count FROM {{zone_prefix}}.bronze.raw_adjusters;
ASSERT VALUE raw_adjuster_count = 5
SELECT 'raw_adjuster_count check passed' AS raw_adjuster_count_status;


-- ===================== STEP 2: Bronze -> Silver SCD2 Policy (Two-Pass MERGE) =====================

-- PASS 1: Expire old versions that have a newer version
-- For each policy_id, if a newer effective_date exists, set valid_to and is_current=0
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
        surrogate_key,
        policy_id,
        holder_name,
        coverage_type,
        annual_premium,
        region,
        risk_score,
        effective_date AS valid_from,
        CASE
            WHEN next_effective_date IS NOT NULL THEN next_effective_date
            ELSE NULL
        END AS valid_to,
        CASE
            WHEN next_effective_date IS NULL THEN 1
            ELSE 0
        END AS is_current
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

-- PASS 2: Insert new current versions (re-merge to ensure current versions are properly inserted)
MERGE INTO {{zone_prefix}}.silver.dim_policy_scd2 AS target
USING (
    WITH latest AS (
        SELECT
            policy_id,
            holder_name,
            coverage_type,
            annual_premium,
            region,
            risk_score,
            effective_date AS valid_from,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY effective_date DESC) AS rn
        FROM {{zone_prefix}}.bronze.raw_policies
    )
    SELECT
        policy_id, holder_name, coverage_type, annual_premium,
        region, risk_score, valid_from
    FROM latest
    WHERE rn = 1
) AS source
ON target.policy_id = source.policy_id AND target.valid_from = source.valid_from AND target.is_current = 1
WHEN MATCHED THEN UPDATE SET
    holder_name    = source.holder_name,
    coverage_type  = source.coverage_type,
    annual_premium = source.annual_premium,
    region         = source.region,
    risk_score     = source.risk_score,
    valid_to       = NULL,
    is_current     = 1,
    processed_at   = CURRENT_TIMESTAMP;

-- Verify SCD2 structure
SELECT COUNT(*) AS scd2_total_rows FROM {{zone_prefix}}.silver.dim_policy_scd2;
ASSERT VALUE scd2_total_rows = 22

SELECT COUNT(*) AS scd2_current_rows FROM {{zone_prefix}}.silver.dim_policy_scd2 WHERE is_current = 1;
-- ===================== STEP 3: Bronze -> Silver Claims Enriched =====================

MERGE INTO {{zone_prefix}}.silver.claims_enriched AS target
USING (
ASSERT VALUE scd2_current_rows = 15
    SELECT
        c.claim_id,
        c.policy_id,
        c.claimant_id,
        c.adjuster_id,
        c.incident_date,
        c.reported_date,
        c.claim_amount,
        c.approved_amount,
        c.status,
        c.settlement_date,
        CASE
            WHEN c.settlement_date IS NOT NULL THEN DATEDIFF(c.settlement_date, c.incident_date)
            ELSE NULL
        END AS days_to_settle,
        DATEDIFF(c.reported_date, c.incident_date) AS days_to_report,
        p.coverage_type,
        p.region
    FROM {{zone_prefix}}.bronze.raw_claims c
    JOIN {{zone_prefix}}.silver.dim_policy_scd2 p
        ON c.policy_id = p.policy_id
        AND c.incident_date >= p.valid_from
        AND (p.valid_to IS NULL OR c.incident_date < p.valid_to)
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET
    approved_amount = source.approved_amount,
    status          = source.status,
    settlement_date = source.settlement_date,
    days_to_settle  = source.days_to_settle,
    days_to_report  = source.days_to_report,
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

SELECT COUNT(*) AS silver_claim_count FROM {{zone_prefix}}.silver.claims_enriched;
-- ===================== STEP 4: Silver -> Gold Dimensions =====================

-- dim_policy (copy from SCD2)
MERGE INTO {{zone_prefix}}.gold.dim_policy AS target
USING (
ASSERT VALUE silver_claim_count = 52
    SELECT surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
           region, risk_score, valid_from, valid_to, is_current
    FROM {{zone_prefix}}.silver.dim_policy_scd2
) AS source
ON target.surrogate_key = source.surrogate_key
WHEN MATCHED THEN UPDATE SET
    holder_name    = source.holder_name,
    coverage_type  = source.coverage_type,
    annual_premium = source.annual_premium,
    region         = source.region,
    risk_score     = source.risk_score,
    valid_to       = source.valid_to,
    is_current     = source.is_current,
    loaded_at      = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
    region, risk_score, valid_from, valid_to, is_current, loaded_at
) VALUES (
    source.surrogate_key, source.policy_id, source.holder_name, source.coverage_type,
    source.annual_premium, source.region, source.risk_score, source.valid_from,
    source.valid_to, source.is_current, CURRENT_TIMESTAMP
);

-- dim_claimant
MERGE INTO {{zone_prefix}}.gold.dim_claimant AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY claimant_id) AS claimant_key,
        claimant_id, name, age_band, state, risk_tier
    FROM {{zone_prefix}}.bronze.raw_claimants
) AS source
ON target.claimant_id = source.claimant_id
WHEN MATCHED THEN UPDATE SET
    name       = source.name,
    age_band   = source.age_band,
    state      = source.state,
    risk_tier  = source.risk_tier,
    loaded_at  = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    claimant_key, claimant_id, name, age_band, state, risk_tier, loaded_at
) VALUES (
    source.claimant_key, source.claimant_id, source.name, source.age_band,
    source.state, source.risk_tier, CURRENT_TIMESTAMP
);

-- dim_adjuster
MERGE INTO {{zone_prefix}}.gold.dim_adjuster AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY adjuster_id) AS adjuster_key,
        adjuster_id, name, specialization, years_experience
    FROM {{zone_prefix}}.bronze.raw_adjusters
) AS source
ON target.adjuster_id = source.adjuster_id
WHEN MATCHED THEN UPDATE SET
    name             = source.name,
    specialization   = source.specialization,
    years_experience = source.years_experience,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    adjuster_key, adjuster_id, name, specialization, years_experience, loaded_at
) VALUES (
    source.adjuster_key, source.adjuster_id, source.name, source.specialization,
    source.years_experience, CURRENT_TIMESTAMP
);

-- ===================== STEP 5: Silver -> Gold Fact Claims =====================

MERGE INTO {{zone_prefix}}.gold.fact_claims AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ce.incident_date, ce.claim_id) AS claim_key,
        dp.surrogate_key AS policy_key,
        dc.claimant_key,
        da.adjuster_key,
        ce.incident_date,
        ce.reported_date,
        ce.claim_amount,
        ce.approved_amount,
        ce.status,
        ce.days_to_settle,
        ce.days_to_report
    FROM {{zone_prefix}}.silver.claims_enriched ce
    JOIN {{zone_prefix}}.silver.dim_policy_scd2 dp
        ON ce.policy_id = dp.policy_id
        AND ce.incident_date >= dp.valid_from
        AND (dp.valid_to IS NULL OR ce.incident_date < dp.valid_to)
    JOIN {{zone_prefix}}.gold.dim_claimant dc ON ce.claimant_id = dc.claimant_id
    JOIN {{zone_prefix}}.gold.dim_adjuster da ON ce.adjuster_id = da.adjuster_id
) AS source
ON target.policy_key = source.policy_key AND target.incident_date = source.incident_date AND target.claim_amount = source.claim_amount
WHEN MATCHED THEN UPDATE SET
    approved_amount = source.approved_amount,
    status          = source.status,
    days_to_settle  = source.days_to_settle,
    days_to_report  = source.days_to_report,
    loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    claim_key, policy_key, claimant_key, adjuster_key, incident_date, reported_date,
    claim_amount, approved_amount, status, days_to_settle, days_to_report, loaded_at
) VALUES (
    source.claim_key, source.policy_key, source.claimant_key, source.adjuster_key,
    source.incident_date, source.reported_date, source.claim_amount, source.approved_amount,
    source.status, source.days_to_settle, source.days_to_report, CURRENT_TIMESTAMP
);

-- ===================== STEP 6: Gold KPI - Loss Ratios =====================

MERGE INTO {{zone_prefix}}.gold.kpi_loss_ratios AS target
USING (
    SELECT
        dp.coverage_type,
        dp.region,
        COUNT(*) AS claim_count,
        ROUND(SUM(fc.claim_amount), 2) AS total_claimed,
        ROUND(SUM(COALESCE(fc.approved_amount, 0)), 2) AS total_approved,
        ROUND(
            CASE WHEN SUM(dp.annual_premium) > 0
                 THEN SUM(COALESCE(fc.approved_amount, 0)) / SUM(dp.annual_premium)
                 ELSE 0
            END, 4
        ) AS loss_ratio,
        ROUND(AVG(CASE WHEN fc.days_to_settle IS NOT NULL THEN fc.days_to_settle ELSE NULL END), 2) AS avg_days_to_settle
    FROM {{zone_prefix}}.gold.fact_claims fc
    JOIN {{zone_prefix}}.gold.dim_policy dp ON fc.policy_key = dp.surrogate_key
    GROUP BY dp.coverage_type, dp.region
) AS source
ON target.coverage_type = source.coverage_type AND target.region = source.region
WHEN MATCHED THEN UPDATE SET
    claim_count        = source.claim_count,
    total_claimed      = source.total_claimed,
    total_approved     = source.total_approved,
    loss_ratio         = source.loss_ratio,
    avg_days_to_settle = source.avg_days_to_settle,
    loaded_at          = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    coverage_type, region, claim_count, total_claimed, total_approved,
    loss_ratio, avg_days_to_settle, loaded_at
) VALUES (
    source.coverage_type, source.region, source.claim_count, source.total_claimed,
    source.total_approved, source.loss_ratio, source.avg_days_to_settle, CURRENT_TIMESTAMP
);

-- ===================== STEP 7: Optimize =====================

OPTIMIZE {{zone_prefix}}.gold.dim_policy;
OPTIMIZE {{zone_prefix}}.gold.dim_claimant;
OPTIMIZE {{zone_prefix}}.gold.dim_adjuster;
OPTIMIZE {{zone_prefix}}.gold.fact_claims;
OPTIMIZE {{zone_prefix}}.gold.kpi_loss_ratios;
