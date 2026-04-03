# Insurance Claims Pipeline

## Scenario

An insurance company processes claims data weekly through a medallion architecture with SCD Type 2 policy tracking. Policies change over time (premium adjustments, coverage upgrades), and claims must join to the correct policy version based on incident date. The pipeline calculates loss ratios, claims aging, and adjuster performance metrics.

## Table Schemas

### Bronze Layer
- **raw_policies** (22 rows): policy_id, holder_name, coverage_type, premium, region, risk_score, effective_date, change_type -- includes multiple versions per policy for SCD2
- **raw_claims** (52 rows): claim_id, policy/claimant/adjuster references, dates, amounts, status (open/under_review/approved/denied/settled)
- **raw_claimants** (10 rows): demographics with age bands and risk tiers
- **raw_adjusters** (5 rows): specializations (property/auto/liability/health/workers_comp)

### Silver Layer
- **dim_policy_scd2** - SCD Type 2 policy dimension with valid_from, valid_to, is_current tracking
- **claims_enriched** - Claims with computed days_to_settle, days_to_report, point-in-time policy join

### Gold Layer (Star Schema)
- **fact_claims** - Claim facts with policy/claimant/adjuster keys, amounts, settlement timing
- **dim_policy** - SCD2 policy dimension (all versions with validity windows)
- **dim_claimant** - Claimant dimension (10 claimants, 6 states)
- **dim_adjuster** - Adjuster dimension (5 adjusters, 5 specializations)
- **kpi_loss_ratios** - Loss ratio analysis by coverage type and region

## Medallion Flow

```
Bronze                           Silver                          Gold
+-------------------+    +--------------------------+    +-------------------+
| raw_policies      |--->| dim_policy_scd2          |--->| dim_policy (SCD2) |
| (22 rows, SCD2)   |    | (expire+insert 2-pass)   |    +-------------------+
+-------------------+    +--------------------------+    | fact_claims       |
| raw_claims        |--->| claims_enriched          |--->| (point-in-time)   |
| (52 rows)         |    | (days_to_settle, enrich) |    +-------------------+
+-------------------+    +--------------------------+    | dim_claimant      |
| raw_claimants     |------------------------------------| dim_adjuster      |
| raw_adjusters     |------------------------------------| kpi_loss_ratios   |
+-------------------+                                    +-------------------+
```

## Star Schema

```
+-------------------+    +-------------------+    +-------------------+
| dim_policy (SCD2) |    | fact_claims       |    | dim_claimant      |
|-------------------|    |-------------------|    |-------------------|
| surrogate_key  PK |----| claim_key      PK |----| claimant_key   PK |
| policy_id         |    | policy_key     FK |    | name              |
| coverage_type     |    | claimant_key   FK |    | age_band          |
| annual_premium    |    | adjuster_key   FK |    | state, risk_tier  |
| valid_from/to     |    | incident_date     |    +-------------------+
| is_current        |    | claim_amount      |
+-------------------+    | approved_amount   |    +-------------------+
                         | status            |    | dim_adjuster      |
                         | days_to_settle    |    |-------------------|
                         +-------------------+----| adjuster_key   PK |
                                                  | specialization    |
+-------------------+                             | years_experience  |
| kpi_loss_ratios   |                             +-------------------+
|-------------------|
| coverage_type     |
| region            |
| loss_ratio        |
| avg_days_settle   |
+-------------------+
```

## Key Features

- **SCD Type 2** two-pass MERGE: first expire matching rows (set valid_to, is_current=0), then insert new current versions
- **Point-in-time policy joins**: claims join to the policy version that was active on the incident date
- **Loss ratio analysis**: approved amounts vs. annual premiums by coverage type and region
- **Claims aging report**: status distribution with average settlement times
- **Adjuster performance metrics**: claims handled, approval rates, settlement speed
- **CHECK constraints**: claim_amount > 0, annual_premium > 0, is_current IN (0,1)
- **Pseudonymisation**: MASK on claimant names

## Verification Checklist

- [ ] POL001 has 3 SCD2 versions, only 1 current
- [ ] 15 current policy versions total
- [ ] Highest single claim >= $100,000
- [ ] Liability coverage total claimed >= $100,000
- [ ] At least 3 distinct claim statuses
- [ ] Auto adjuster handles most claims
- [ ] All fact FK references resolve to dimension PKs
- [ ] At least 4 distinct regions represented
- [ ] Settlement days computed correctly for settled claims
