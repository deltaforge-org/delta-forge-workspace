-- =============================================================================
-- Property & Casualty Insurance Claims Pipeline: Full Load
-- =============================================================================
-- Pipeline DAG:
--   validate_bronze
--        |
--   +----+----------------+
--   |                      |
--   expire_scd2_policies   load_raw_claims    (parallel)
--        |                      |
--   insert_scd2_current         |
--        |                      |
--   enable_cdf_actuarial        |
--        |                      |
--        +----------+-----------+
--                   |
--   enrich_claims_point_in_time  (point-in-time join + fraud scoring)
--                   |
--   +---------------+---------------+
--   |               |               |
--   dim_claimant    dim_adjuster    dim_coverage    (parallel)
--   |               |               |
--   +---------------+---------------+
--                   |
--   build_fact_claims
--                   |
--   +---------------+---------------+
--   |                               |
--   kpi_loss_ratios    kpi_adjuster_perf    (parallel)
--   |                               |
--   +---------------+---------------+
--                   |
--   restore_correction_demo  (RESTORE bad batch)
--                   |
--   optimize  (CONTINUE ON FAILURE)
-- =============================================================================

SCHEDULE ins_weekly_schedule
    CRON '0 7 * * 1'
    TIMEZONE 'America/Chicago'
    RETRIES 2
    TIMEOUT 7200
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE ins_claims_pipeline
    DESCRIPTION 'P&C insurance pipeline: SCD2 policies, point-in-time joins, fraud detection, RESTORE workflow'
    SCHEDULE 'ins_weekly_schedule'
    TAGS 'insurance,claims,SCD2,loss-ratio,fraud,RESTORE'
    SLA 7200
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: validate_bronze =====================

STEP validate_bronze
  TIMEOUT '2m'
AS
  SELECT COUNT(*) AS raw_policy_count FROM {{zone_prefix}}.bronze.raw_policies;
  ASSERT VALUE raw_policy_count = 23

  SELECT COUNT(*) AS raw_claim_count FROM {{zone_prefix}}.bronze.raw_claims;
  ASSERT VALUE raw_claim_count = 45

  SELECT COUNT(*) AS raw_claimant_count FROM {{zone_prefix}}.bronze.raw_claimants;
  ASSERT VALUE raw_claimant_count = 12

  SELECT COUNT(*) AS raw_adjuster_count FROM {{zone_prefix}}.bronze.raw_adjusters;
  ASSERT VALUE raw_adjuster_count = 6
  SELECT 'Bronze validation passed: 23 policies, 45 claims, 12 claimants, 6 adjusters' AS status;

-- ===================== STEP 2: expire_scd2_policies =====================
-- PASS 1: Load all policy versions, compute valid_from/valid_to boundaries,
-- expire non-current versions. Uses LEAD() to derive valid_to from next version.

STEP expire_scd2_policies
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.silver.policy_dim AS target
  USING (
      WITH ranked AS (
          SELECT
              policy_id,
              holder_name,
              coverage_type,
              annual_premium,
              region,
              state,
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
          state,
          risk_score,
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

-- ===================== STEP 3: insert_scd2_current =====================
-- PASS 2: Ensure current versions have is_current=1 and valid_to=NULL

STEP insert_scd2_current
  DEPENDS ON (expire_scd2_policies)
AS
  MERGE INTO {{zone_prefix}}.silver.policy_dim AS target
  USING (
      WITH latest AS (
          SELECT
              policy_id, holder_name, coverage_type, annual_premium,
              region, state, risk_score, effective_date AS valid_from,
              ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY effective_date DESC) AS rn
          FROM {{zone_prefix}}.bronze.raw_policies
      )
      SELECT policy_id, holder_name, coverage_type, annual_premium,
             region, state, risk_score, valid_from
      FROM latest WHERE rn = 1
  ) AS source
  ON target.policy_id = source.policy_id AND target.valid_from = source.valid_from AND target.is_current = 1
  WHEN MATCHED THEN UPDATE SET
      holder_name    = source.holder_name,
      coverage_type  = source.coverage_type,
      annual_premium = source.annual_premium,
      region         = source.region,
      state          = source.state,
      risk_score     = source.risk_score,
      valid_to       = NULL,
      is_current     = 1,
      processed_at   = CURRENT_TIMESTAMP;

  -- Verify SCD2 structure: 23 total rows, 15 current
  SELECT COUNT(*) AS scd2_total FROM {{zone_prefix}}.silver.policy_dim;
  ASSERT VALUE scd2_total = 23

  SELECT COUNT(*) AS scd2_current FROM {{zone_prefix}}.silver.policy_dim WHERE is_current = 1;
  ASSERT VALUE scd2_current = 15
  SELECT 'SCD2 verified: 23 total versions, 15 current' AS status;

-- ===================== STEP 4: enable_cdf_actuarial =====================
-- Materialize initial actuarial snapshot from the policy_dim CDF

STEP enable_cdf_actuarial
  DEPENDS ON (insert_scd2_current)
AS
  MERGE INTO {{zone_prefix}}.silver.actuarial_snapshots AS tgt
  USING (
      SELECT
          policy_id || '-' || CAST(surrogate_key AS STRING) || '-INIT' AS snapshot_id,
          policy_id || '-snapshot' AS claim_id,
          policy_id,
          CASE WHEN is_current = 1 THEN 'active' ELSE 'expired' END AS status,
          annual_premium AS claim_amount,
          NULL AS approved_amount,
          coverage_type,
          'policy_loaded' AS change_type
      FROM {{zone_prefix}}.silver.policy_dim
  ) AS src
  ON tgt.snapshot_id = src.snapshot_id
  WHEN NOT MATCHED THEN INSERT (
      snapshot_id, claim_id, policy_id, status, claim_amount, approved_amount,
      coverage_type, change_type, captured_at
  ) VALUES (
      src.snapshot_id, src.claim_id, src.policy_id, src.status, src.claim_amount,
      src.approved_amount, src.coverage_type, src.change_type, CURRENT_TIMESTAMP
  );

-- ===================== STEP 5: load_raw_claims =====================
-- Parallel with SCD2 steps: validate claims data quality

STEP load_raw_claims
  DEPENDS ON (validate_bronze)
AS
  -- Check for claims with invalid dates (reported before incident)
  SELECT COUNT(*) AS invalid_dates
  FROM {{zone_prefix}}.bronze.raw_claims
  WHERE reported_date < incident_date;

  ASSERT VALUE invalid_dates = 0

  -- Check for negative claim amounts
  SELECT COUNT(*) AS negative_amounts
  FROM {{zone_prefix}}.bronze.raw_claims
  WHERE claim_amount <= 0;

  ASSERT VALUE negative_amounts = 0
  SELECT 'Claims data quality validated' AS status;

-- ===================== STEP 6: enrich_claims_point_in_time =====================
-- Point-in-time join: match each claim to the policy version that was active
-- on the incident_date. Also compute fraud risk via statistical outlier analysis.

STEP enrich_claims_point_in_time
  DEPENDS ON (insert_scd2_current, load_raw_claims)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.silver.claims_enriched AS target
  USING (
      WITH claim_with_policy AS (
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
              p.annual_premium AS annual_premium_at_incident,
              p.region,
              p.risk_score AS risk_score_at_incident
          FROM {{zone_prefix}}.bronze.raw_claims c
          JOIN {{zone_prefix}}.silver.policy_dim p
              ON c.policy_id = p.policy_id
              AND c.incident_date >= p.valid_from
              AND (p.valid_to IS NULL OR c.incident_date < p.valid_to)
      ),
      coverage_stats AS (
          SELECT
              coverage_type,
              AVG(claim_amount) AS avg_claim,
              STDDEV(claim_amount) AS stddev_claim
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
      days_to_report             = source.days_to_report,
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

  SELECT COUNT(*) AS enriched_count FROM {{zone_prefix}}.silver.claims_enriched;
  ASSERT VALUE enriched_count = 45

  -- Verify fraud detection found the 3 outliers
  SELECT COUNT(*) AS high_risk_count
  FROM {{zone_prefix}}.silver.claims_enriched
  WHERE fraud_risk = 'high_risk';

  ASSERT VALUE high_risk_count >= 2
  SELECT 'Claims enriched with point-in-time policy + fraud scoring' AS status;

-- ===================== STEP 7: build_dim_claimant =====================

STEP build_dim_claimant
  DEPENDS ON (enrich_claims_point_in_time)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_claimant AS tgt
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY claimant_id) AS claimant_key,
          claimant_id, name, age_band, state, risk_tier
      FROM {{zone_prefix}}.bronze.raw_claimants
  ) AS src
  ON tgt.claimant_id = src.claimant_id
  WHEN MATCHED THEN UPDATE SET
      name      = src.name,
      age_band  = src.age_band,
      state     = src.state,
      risk_tier = src.risk_tier,
      loaded_at = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (claimant_key, claimant_id, name, age_band, state, risk_tier, loaded_at)
  VALUES (src.claimant_key, src.claimant_id, src.name, src.age_band, src.state, src.risk_tier, CURRENT_TIMESTAMP);

-- ===================== STEP 8: build_dim_adjuster =====================

STEP build_dim_adjuster
  DEPENDS ON (enrich_claims_point_in_time)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_adjuster AS tgt
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY adjuster_id) AS adjuster_key,
          adjuster_id, name, specialization, years_experience, region
      FROM {{zone_prefix}}.bronze.raw_adjusters
  ) AS src
  ON tgt.adjuster_id = src.adjuster_id
  WHEN MATCHED THEN UPDATE SET
      name             = src.name,
      specialization   = src.specialization,
      years_experience = src.years_experience,
      region           = src.region,
      loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (adjuster_key, adjuster_id, name, specialization, years_experience, region, loaded_at)
  VALUES (src.adjuster_key, src.adjuster_id, src.name, src.specialization, src.years_experience, src.region, CURRENT_TIMESTAMP);

-- ===================== STEP 9: build_dim_coverage_type =====================

STEP build_dim_coverage_type
  DEPENDS ON (enrich_claims_point_in_time)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_coverage_type AS tgt
  USING (
      SELECT DISTINCT
          coverage_type AS coverage_key,
          coverage_type,
          CASE
              WHEN coverage_type IN ('auto', 'auto_plus') THEN 'Automobile'
              WHEN coverage_type IN ('home', 'home_plus') THEN 'Property'
              WHEN coverage_type IN ('health', 'health_plus') THEN 'Health'
              WHEN coverage_type = 'liability' THEN 'Liability'
              WHEN coverage_type = 'workers_comp' THEN 'Workers Compensation'
              ELSE 'Other'
          END AS coverage_category
      FROM {{zone_prefix}}.silver.policy_dim
  ) AS src
  ON tgt.coverage_key = src.coverage_key
  WHEN NOT MATCHED THEN INSERT (coverage_key, coverage_type, coverage_category, loaded_at)
  VALUES (src.coverage_key, src.coverage_type, src.coverage_category, CURRENT_TIMESTAMP);

-- ===================== STEP 10: build_fact_claims =====================

STEP build_fact_claims
  DEPENDS ON (build_dim_claimant, build_dim_adjuster, build_dim_coverage_type)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.gold.fact_claims AS tgt
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ce.incident_date, ce.claim_id) AS claim_key,
          pd.surrogate_key AS policy_surrogate_key,
          dc.claimant_key,
          da.adjuster_key,
          ce.coverage_type AS coverage_key,
          ce.incident_date,
          ce.reported_date,
          ce.claim_amount,
          ce.approved_amount,
          ce.status,
          ce.days_to_settle,
          ce.days_to_report,
          ce.fraud_risk,
          ce.fraud_score,
          CASE
              WHEN ce.annual_premium_at_incident > 0
              THEN CAST(COALESCE(ce.approved_amount, 0) / ce.annual_premium_at_incident AS DECIMAL(6,4))
              ELSE 0
          END AS loss_ratio
      FROM {{zone_prefix}}.silver.claims_enriched ce
      JOIN {{zone_prefix}}.silver.policy_dim pd
          ON ce.policy_id = pd.policy_id
          AND ce.incident_date >= pd.valid_from
          AND (pd.valid_to IS NULL OR ce.incident_date < pd.valid_to)
      JOIN {{zone_prefix}}.gold.dim_claimant dc ON ce.claimant_id = dc.claimant_id
      JOIN {{zone_prefix}}.gold.dim_adjuster da ON ce.adjuster_id = da.adjuster_id
  ) AS src
  ON tgt.policy_surrogate_key = src.policy_surrogate_key
      AND tgt.incident_date = src.incident_date
      AND tgt.claim_amount = src.claim_amount
  WHEN MATCHED THEN UPDATE SET
      approved_amount = src.approved_amount,
      status          = src.status,
      days_to_settle  = src.days_to_settle,
      days_to_report  = src.days_to_report,
      fraud_risk      = src.fraud_risk,
      fraud_score     = src.fraud_score,
      loss_ratio      = src.loss_ratio,
      loaded_at       = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      claim_key, policy_surrogate_key, claimant_key, adjuster_key, coverage_key,
      incident_date, reported_date, claim_amount, approved_amount, status,
      days_to_settle, days_to_report, fraud_risk, fraud_score, loss_ratio, loaded_at
  ) VALUES (
      src.claim_key, src.policy_surrogate_key, src.claimant_key, src.adjuster_key,
      src.coverage_key, src.incident_date, src.reported_date, src.claim_amount,
      src.approved_amount, src.status, src.days_to_settle, src.days_to_report,
      src.fraud_risk, src.fraud_score, src.loss_ratio, CURRENT_TIMESTAMP
  );

-- ===================== STEP 11: kpi_loss_ratios =====================
-- Loss ratios by coverage type, region, and quarter

STEP kpi_loss_ratios
  DEPENDS ON (build_fact_claims)
AS
  MERGE INTO {{zone_prefix}}.gold.kpi_loss_ratios AS tgt
  USING (
      SELECT
          fc.coverage_key AS coverage_type,
          pd.region,
          'Q' || CAST(EXTRACT(QUARTER FROM fc.incident_date) AS STRING) || '-' || CAST(EXTRACT(YEAR FROM fc.incident_date) AS STRING) AS quarter,
          COUNT(*) AS claim_count,
          ROUND(SUM(fc.claim_amount), 2) AS total_claimed,
          ROUND(SUM(COALESCE(fc.approved_amount, 0)), 2) AS total_approved,
          ROUND(SUM(pd.annual_premium), 2) AS total_premium,
          ROUND(
              CASE WHEN SUM(pd.annual_premium) > 0
                   THEN SUM(COALESCE(fc.approved_amount, 0)) / SUM(pd.annual_premium)
                   ELSE 0
              END, 4
          ) AS loss_ratio,
          ROUND(AVG(CASE WHEN fc.days_to_settle IS NOT NULL THEN fc.days_to_settle END), 2) AS avg_days_to_settle
      FROM {{zone_prefix}}.gold.fact_claims fc
      JOIN {{zone_prefix}}.silver.policy_dim pd ON fc.policy_surrogate_key = pd.surrogate_key
      GROUP BY fc.coverage_key, pd.region,
               'Q' || CAST(EXTRACT(QUARTER FROM fc.incident_date) AS STRING) || '-' || CAST(EXTRACT(YEAR FROM fc.incident_date) AS STRING)
  ) AS src
  ON tgt.coverage_type = src.coverage_type AND tgt.region = src.region AND tgt.quarter = src.quarter
  WHEN MATCHED THEN UPDATE SET
      claim_count        = src.claim_count,
      total_claimed      = src.total_claimed,
      total_approved     = src.total_approved,
      total_premium      = src.total_premium,
      loss_ratio         = src.loss_ratio,
      avg_days_to_settle = src.avg_days_to_settle,
      loaded_at          = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      coverage_type, region, quarter, claim_count, total_claimed, total_approved,
      total_premium, loss_ratio, avg_days_to_settle, loaded_at
  ) VALUES (
      src.coverage_type, src.region, src.quarter, src.claim_count, src.total_claimed,
      src.total_approved, src.total_premium, src.loss_ratio, src.avg_days_to_settle,
      CURRENT_TIMESTAMP
  );

-- ===================== STEP 12: kpi_adjuster_performance =====================
-- Settlement speed, approval rate, volume by adjuster

STEP kpi_adjuster_performance
  DEPENDS ON (build_fact_claims)
AS
  MERGE INTO {{zone_prefix}}.gold.kpi_adjuster_performance AS tgt
  USING (
      SELECT
          da.adjuster_id,
          da.name AS adjuster_name,
          da.specialization,
          COUNT(*) AS claims_handled,
          COUNT(CASE WHEN fc.status = 'settled' THEN 1 END) AS claims_settled,
          COUNT(CASE WHEN fc.status = 'denied' THEN 1 END) AS claims_denied,
          ROUND(AVG(CASE WHEN fc.days_to_settle IS NOT NULL THEN fc.days_to_settle END), 2) AS avg_days_to_settle,
          ROUND(AVG(fc.claim_amount), 2) AS avg_claim_amount,
          CAST(
              COUNT(CASE WHEN fc.approved_amount IS NOT NULL AND fc.approved_amount > 0 THEN 1 END) * 100.0
              / NULLIF(COUNT(*), 0)
          AS DECIMAL(5,2)) AS approval_rate_pct,
          ROUND(SUM(COALESCE(fc.approved_amount, 0)), 2) AS total_approved
      FROM {{zone_prefix}}.gold.fact_claims fc
      JOIN {{zone_prefix}}.gold.dim_adjuster da ON fc.adjuster_key = da.adjuster_key
      GROUP BY da.adjuster_id, da.name, da.specialization
  ) AS src
  ON tgt.adjuster_id = src.adjuster_id
  WHEN MATCHED THEN UPDATE SET
      adjuster_name      = src.adjuster_name,
      specialization     = src.specialization,
      claims_handled     = src.claims_handled,
      claims_settled     = src.claims_settled,
      claims_denied      = src.claims_denied,
      avg_days_to_settle = src.avg_days_to_settle,
      avg_claim_amount   = src.avg_claim_amount,
      approval_rate_pct  = src.approval_rate_pct,
      total_approved     = src.total_approved,
      loaded_at          = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      adjuster_id, adjuster_name, specialization, claims_handled, claims_settled,
      claims_denied, avg_days_to_settle, avg_claim_amount, approval_rate_pct,
      total_approved, loaded_at
  ) VALUES (
      src.adjuster_id, src.adjuster_name, src.specialization, src.claims_handled,
      src.claims_settled, src.claims_denied, src.avg_days_to_settle,
      src.avg_claim_amount, src.approval_rate_pct, src.total_approved, CURRENT_TIMESTAMP
  );

-- ===================== STEP 13: restore_correction_demo =====================
-- Demonstrate RESTORE: insert bad claims, show damage, then RESTORE to rollback.

STEP restore_correction_demo
  DEPENDS ON (kpi_loss_ratios, kpi_adjuster_performance)
AS
  -- Record version before bad batch
  SELECT COUNT(*) AS pre_restore_count FROM {{zone_prefix}}.silver.claims_enriched;

  -- Insert a batch of claims with obviously wrong amounts (data entry error)
  INSERT INTO {{zone_prefix}}.silver.claims_enriched VALUES
      ('C-BAD-01', 'POL001', 'CLM01', 'ADJ02', '2024-02-01', '2024-02-02', 999999.99, NULL, 'filed', NULL, NULL, 1, 'auto_plus', 1650.00, 'West', 3.5, 'high_risk', 9.99, CURRENT_TIMESTAMP),
      ('C-BAD-02', 'POL002', 'CLM02', 'ADJ01', '2024-02-03', '2024-02-04', 888888.88, NULL, 'filed', NULL, NULL, 1, 'home',      2640.00, 'Northeast', 2.4, 'high_risk', 9.50, CURRENT_TIMESTAMP),
      ('C-BAD-03', 'POL003', 'CLM03', 'ADJ02', '2024-02-05', '2024-02-06', 777777.77, NULL, 'filed', NULL, NULL, 1, 'auto',       980.00, 'South', 2.8, 'high_risk', 9.00, CURRENT_TIMESTAMP);

  -- Show the damage: 3 corrupted rows with absurd amounts
  SELECT COUNT(*) AS post_bad_count FROM {{zone_prefix}}.silver.claims_enriched;

  -- RESTORE to the version before the bad batch
  RESTORE {{zone_prefix}}.silver.claims_enriched TO VERSION 1;

  -- Verify restoration: count should be back to original
  SELECT COUNT(*) AS post_restore_count FROM {{zone_prefix}}.silver.claims_enriched;
  ASSERT VALUE post_restore_count = 45
  SELECT 'RESTORE correction verified: bad batch rolled back successfully' AS status;

-- ===================== STEP 14: optimize =====================

STEP optimize
  DEPENDS ON (restore_correction_demo)
  CONTINUE ON FAILURE
AS
  OPTIMIZE {{zone_prefix}}.silver.policy_dim;
  OPTIMIZE {{zone_prefix}}.silver.claims_enriched;
  OPTIMIZE {{zone_prefix}}.gold.fact_claims;
