# Pharma Clinical Trials Pipeline

## Scenario

A pharmaceutical company runs three clinical trials (Cardiovascular Phase III, Oncology Phase II, Neurology Phase I) across four international sites. This pipeline implements the full medallion architecture to transform raw EDC (Electronic Data Capture) feeds into an analytical star schema for trial efficacy, safety monitoring, and regulatory reporting.

Key compliance requirements include GDPR right-to-erasure for withdrawn participants (deletion vectors), data recovery for regulatory audits (RESTORE), and comprehensive pseudonymisation of all PII fields using all four available transforms.

## Star Schema

```
                    +------------------+
                    |   dim_trial      |
                    |------------------|
                    | trial_key (PK)   |
                    | trial_id         |
                    | trial_name       |
                    | phase            |
                    | therapeutic_area |
                    | sponsor          |
                    | start_date       |
                    | target_enrollment|
                    +--------+---------+
                             |
+------------------+         |         +------------------+
| dim_participant  |         |         |   dim_site       |
|------------------|         |         |------------------|
| participant_key  |    +----+----+    | site_key (PK)    |
| participant_id   |    |  fact_  |    | site_id          |
| age_band         +----+observa- +----+ site_name        |
| gender           |    |  tions  |    | city             |
| ethnicity        |    +---------+    | country          |
| enrollment_date  |    |obs_key  |    | principal_inv    |
| consent_status   |    |part_key |    | irb_approval_date|
| withdrawal_date  |    |trial_key|    +------------------+
+------------------+    |site_key |
                        |visit_key|    +------------------+
                        |obs_date |    |   dim_visit      |
                        |biomarker|    |------------------|
                        |ae_flag  |    | visit_key (PK)   |
                        |severity |    | visit_number     |
                        |dosage_mg|    | visit_type       |
                        |response |    | scheduled_day    |
                        +---------+    | window_days      |
                                       +------------------+

+---------------------------+
|   kpi_trial_efficacy      |
|---------------------------|
| trial_id                  |
| phase                     |
| response_rate_pct         |
| adverse_event_rate        |
| mean_biomarker_change     |
| enrollment_pct            |
| screen_fail_rate          |
| dropout_rate              |
+---------------------------+
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **Pseudonymisation (REDACT)** | SSN fully redacted with `[REDACTED]` placeholder |
| **Pseudonymisation (MASK)** | Email masked: `al***@email.com` pattern |
| **Pseudonymisation (keyed_hash)** | Participant name replaced with SHA256 hash |
| **Pseudonymisation (generalize)** | DOB generalized to decade (e.g., `1960s`) |
| **Deletion Vectors** | GDPR erasure of withdrawn participant PT-1003 |
| **RESTORE** | Recovery of accidentally purged data for regulatory audit |
| **Star Schema** | 1 fact table + 4 dimension tables + 1 KPI summary |
| **Biomarker Analysis** | Change-from-baseline calculations, response categorization |
| **Adverse Event Tracking** | Severity classification, site-level AE rate analysis |
| **Enrollment Funnel** | Screen fail, dropout, and completion rate tracking |

## Data Profile

- **3 clinical trials**: Phase I (Neurology), Phase II (Oncology), Phase III (Cardiovascular)
- **4 international sites**: Boston, London, Munich, Tokyo
- **20 participants**: Including screen failures (3), withdrawals (3), completed (2), active (12)
- **5 visit types**: Screening, Baseline, Week 4, Week 12, End of Trial
- **71+ observations**: With biomarker values, adverse events, protocol deviations

## Verification Checklist

- [ ] All 3 trials present in `kpi_trial_efficacy` with non-zero metrics
- [ ] Star schema joins produce complete observation records across all dimensions
- [ ] Adverse events correctly categorized by severity (Mild, Moderate, Severe)
- [ ] All 4 sites represented with enrollment and AE rate metrics
- [ ] Enrollment funnel shows screen failures, withdrawals, and completions
- [ ] Visit completion rates decrease appropriately from Screening to End of Trial
- [ ] Biomarker LAG window analysis shows visit-over-visit deltas
- [ ] Age band and gender distribution analysis produces valid response rates
- [ ] Deletion vector demo removes withdrawn participant data
- [ ] RESTORE demo recovers the purged participant record
