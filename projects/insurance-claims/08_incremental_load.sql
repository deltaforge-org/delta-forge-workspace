-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates: INCREMENTAL_FILTER macro, new SCD2 policy changes, new claims
-- arriving incrementally, point-in-time re-enrichment, and actuarial CDF capture.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (DeltaForge Macro)
-- ============================================================================
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "claim_id > 'C0045' AND incident_date > '2024-01-05'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER(ins.silver.claims_enriched, claim_id, incident_date, 7)}};

-- ===================== Pre-load state capture =====================

SELECT COUNT(*) AS pre_claim_count FROM ins.silver.claims_enriched;
SELECT COUNT(*) AS pre_fact_count FROM ins.gold.fact_claims;
SELECT COUNT(*) AS pre_scd2_count FROM ins.silver.policy_dim;
SELECT MAX(processed_at) AS current_watermark FROM ins.silver.claims_enriched;

-- =============================================================================
-- New SCD2 policy changes arriving (batch 4: 2 more changes)
-- =============================================================================

-- POL003 gets a premium increase, POL010 gets a coverage upgrade
MERGE INTO ins.bronze.raw_policies AS target
USING (
  SELECT * FROM (VALUES
    ('POL003', 'Chris Okafor', '345-67-8901', 'auto', 1150.00, 'South', 'TX', 3.1, '2024-01-15', 'premium_increase', '2024-02-01T00:00:00'),
    ('POL010', 'Julia Fernandez', '012-34-5678', 'auto_plus', 1200.00, 'West', 'CO', 2.2, '2024-01-15', 'coverage_upgrade', '2024-02-01T00:00:00')
  ) AS t(policy_id, holder_name, ssn, coverage_type, annual_premium, region, state, risk_score, effective_date, change_type, _loaded_at)
) AS source
ON target.policy_id = source.policy_id AND target.effective_date = source.effective_date
WHEN MATCHED THEN UPDATE SET
  holder_name    = source.holder_name,
  ssn            = source.ssn,
  coverage_type  = source.coverage_type,
  annual_premium = source.annual_premium,
  region         = source.region,
  state          = source.state,
  risk_score     = source.risk_score,
  change_type    = source.change_type,
  _loaded_at     = source._loaded_at
WHEN NOT MATCHED THEN INSERT VALUES (
  source.policy_id, source.holder_name, source.ssn, source.coverage_type,
  source.annual_premium, source.region, source.state, source.risk_score,
  source.effective_date, source.change_type, source._loaded_at
);

-- =============================================================================
-- New claims arriving (5 new + 1 updated settlement)
-- =============================================================================

MERGE INTO ins.bronze.raw_claims AS target
USING (
  SELECT * FROM (VALUES
    ('C0046', 'POL003', 'CLM03', 'ADJ02', '2024-01-18', '2024-01-19',  4800.00,     CAST(NULL AS DOUBLE), 'filed',  CAST(NULL AS STRING), 'Rear bumper damage parking lot',  '2024-02-01T00:00:00'),
    ('C0047', 'POL010', 'CLM10', 'ADJ02', '2024-01-20', '2024-01-21',  8200.00,     NULL, 'filed',  NULL,         'Collision at intersection',       '2024-02-01T00:00:00'),
    ('C0048', 'POL004', 'CLM04', 'ADJ04', '2024-01-15', '2024-01-16', 12500.00,     NULL, 'under_review', NULL,   'MRI and specialist referral',     '2024-02-01T00:00:00'),
    ('C0049', 'POL011', 'CLM11', 'ADJ06', '2024-01-12', '2024-01-14', 16000.00,     NULL, 'filed',  NULL,         'Bathroom flood damage',           '2024-02-01T00:00:00'),
    ('C0050', 'POL005', 'CLM05', 'ADJ03', '2024-01-16', '2024-01-18', 55000.00,     NULL, 'filed',  NULL,         'Major liability incident',        '2024-02-01T00:00:00'),
    ('C0025', 'POL001', 'CLM01', 'ADJ02', '2023-12-10', '2023-12-12', 11000.00,  9200.00, 'settled', '2024-01-18', 'Multi-vehicle accident - SETTLED', '2024-02-01T00:00:00')
  ) AS t(claim_id, policy_id, claimant_id, adjuster_id, incident_date, reported_date, claim_amount, approved_amount, status, settlement_date, description, _loaded_at)
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET
  policy_id       = source.policy_id,
  claimant_id     = source.claimant_id,
  adjuster_id     = source.adjuster_id,
  incident_date   = source.incident_date,
  reported_date   = source.reported_date,
  claim_amount    = source.claim_amount,
  approved_amount = source.approved_amount,
  status          = source.status,
  settlement_date = source.settlement_date,
  description     = source.description,
  _loaded_at      = source._loaded_at
WHEN NOT MATCHED THEN INSERT VALUES (
  source.claim_id, source.policy_id, source.claimant_id, source.adjuster_id,
  source.incident_date, source.reported_date, source.claim_amount,
  source.approved_amount, source.status, source.settlement_date,
  source.description, source._loaded_at
);

-- =============================================================================
-- Re-run SCD2 two-pass MERGE for policy changes
-- =============================================================================

-- PASS 1: Expire old versions
MERGE INTO ins.silver.policy_dim AS target
USING (
  WITH ranked AS (
      SELECT
          policy_id, holder_name, coverage_type, annual_premium,
          region, state, risk_score, effective_date,
          LEAD(effective_date) OVER (PARTITION BY policy_id ORDER BY effective_date) AS next_effective_date,
          ROW_NUMBER() OVER (ORDER BY policy_id, effective_date) AS surrogate_key
      FROM ins.bronze.raw_policies
  )
  SELECT
      surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
      region, state, risk_score,
      effective_date AS valid_from,
      next_effective_date AS valid_to,
      CASE WHEN next_effective_date IS NULL THEN 1 ELSE 0 END AS is_current
  FROM ranked
) AS source
ON target.policy_id = source.policy_id AND target.valid_from = source.valid_from
WHEN MATCHED THEN UPDATE SET
  valid_to       = source.valid_to,
  is_current     = source.is_current,
  annual_premium = source.annual_premium,
  coverage_type  = source.coverage_type,
  risk_score     = source.risk_score,
  processed_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  surrogate_key, policy_id, holder_name, coverage_type, annual_premium,
  region, state, risk_score, valid_from, valid_to, is_current, processed_at
) VALUES (
  source.surrogate_key, source.policy_id, source.holder_name, source.coverage_type,
  source.annual_premium, source.region, source.state, source.risk_score,
  source.valid_from, source.valid_to, source.is_current, CURRENT_TIMESTAMP
);

-- PASS 2: Ensure current versions are up to date
MERGE INTO ins.silver.policy_dim AS target
USING (
  WITH latest AS (
      SELECT policy_id, holder_name, coverage_type, annual_premium,
             region, state, risk_score, effective_date AS valid_from,
             ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY effective_date DESC) AS rn
      FROM ins.bronze.raw_policies
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

-- =============================================================================
-- Incremental claims enrichment using INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO ins.silver.claims_enriched AS target
USING (
  WITH claim_with_policy AS (
      SELECT
          c.claim_id, c.policy_id, c.claimant_id, c.adjuster_id,
          c.incident_date, c.reported_date, c.claim_amount, c.approved_amount,
          c.status, c.settlement_date,
          CASE WHEN c.settlement_date IS NOT NULL THEN DATEDIFF(c.settlement_date, c.incident_date) ELSE NULL END AS days_to_settle,
          DATEDIFF(c.reported_date, c.incident_date) AS days_to_report,
          p.coverage_type, p.annual_premium AS annual_premium_at_incident,
          p.region, p.risk_score AS risk_score_at_incident
      FROM ins.bronze.raw_claims c
      JOIN ins.silver.policy_dim p
          ON c.policy_id = p.policy_id
          AND c.incident_date >= p.valid_from
          AND (p.valid_to IS NULL OR c.incident_date < p.valid_to)
      WHERE {{INCREMENTAL_FILTER(ins.silver.claims_enriched, claim_id, incident_date, 7)}}
  ),
  coverage_stats AS (
      SELECT coverage_type, AVG(claim_amount) AS avg_claim, STDDEV(claim_amount) AS stddev_claim
      FROM claim_with_policy
      GROUP BY coverage_type
  )
  SELECT
      cp.*,
      CASE
          WHEN cp.claim_amount > cs.avg_claim * 2.5 THEN 'high_risk'
          WHEN cp.claim_amount > cs.avg_claim * 1.5 THEN 'medium_risk'
          ELSE 'low_risk'
      END AS fraud_risk,
      CASE
          WHEN cs.stddev_claim > 0
          THEN CAST((cp.claim_amount - cs.avg_claim) / cs.stddev_claim AS DECIMAL(5,2))
          ELSE 0
      END AS fraud_score
  FROM claim_with_policy cp
  JOIN coverage_stats cs ON cp.coverage_type = cs.coverage_type
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET
  approved_amount            = source.approved_amount,
  status                     = source.status,
  settlement_date            = source.settlement_date,
  days_to_settle             = source.days_to_settle,
  coverage_type              = source.coverage_type,
  annual_premium_at_incident = source.annual_premium_at_incident,
  region                     = source.region,
  risk_score_at_incident     = source.risk_score_at_incident,
  fraud_risk                 = source.fraud_risk,
  fraud_score                = source.fraud_score,
  processed_at               = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  claim_id, policy_id, claimant_id, adjuster_id, incident_date, reported_date,
  claim_amount, approved_amount, status, settlement_date, days_to_settle,
  days_to_report, coverage_type, annual_premium_at_incident, region,
  risk_score_at_incident, fraud_risk, fraud_score, processed_at
) VALUES (
  source.claim_id, source.policy_id, source.claimant_id, source.adjuster_id,
  source.incident_date, source.reported_date, source.claim_amount,
  source.approved_amount, source.status, source.settlement_date,
  source.days_to_settle, source.days_to_report, source.coverage_type,
  source.annual_premium_at_incident, source.region, source.risk_score_at_incident,
  source.fraud_risk, source.fraud_score, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Capture actuarial snapshot for new claims via CDF
-- =============================================================================

MERGE INTO ins.silver.actuarial_snapshots AS tgt
USING (
  SELECT
      claim_id || '-' || status || '-INC' AS snapshot_id,
      claim_id,
      policy_id,
      status,
      claim_amount,
      approved_amount,
      coverage_type,
      'incremental_load' AS change_type
  FROM ins.silver.claims_enriched
  WHERE processed_at >= (SELECT MAX(processed_at) FROM ins.silver.claims_enriched)
) AS src
ON tgt.snapshot_id = src.snapshot_id
WHEN NOT MATCHED THEN INSERT (
  snapshot_id, claim_id, policy_id, status, claim_amount, approved_amount,
  coverage_type, change_type, captured_at
) VALUES (
  src.snapshot_id, src.claim_id, src.policy_id, src.status, src.claim_amount,
  src.approved_amount, src.coverage_type, src.change_type, CURRENT_TIMESTAMP
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Claims should have grown by 5 (C0025 updated, not added)
ASSERT VALUE post_claim_count = 50
SELECT COUNT(*) AS post_claim_count FROM ins.silver.claims_enriched;

SELECT 'Incremental claims: 45 -> 50 (5 new)' AS status;

-- Verify C0025 was updated to settled
ASSERT VALUE c0025_status = 'settled' WHERE claim_id = 'C0025'
SELECT claim_id, status AS c0025_status FROM ins.silver.claims_enriched;

-- Verify SCD2 grew by 2 (POL003 premium_increase + POL010 coverage_upgrade)
ASSERT VALUE post_scd2_count = 25
SELECT COUNT(*) AS post_scd2_count FROM ins.silver.policy_dim;

SELECT 'SCD2 grew: 23 -> 25 (2 new changes)' AS status;

-- Verify no duplicate claims
ASSERT VALUE dup_check = 0
SELECT COUNT(*) AS dup_check
FROM (
  SELECT claim_id, COUNT(*) AS cnt
  FROM ins.silver.claims_enriched
  GROUP BY claim_id
  HAVING COUNT(*) > 1
);

SELECT 'Incremental load complete: no duplicates, SCD2 updated, CDF captured' AS status;
