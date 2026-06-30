# Healthcare Patient EHR Pipeline

## Scenario

You are a data engineer at a hospital network. Your pipeline handles patient admissions from messy EHR feeds, applies HIPAA-compliant pseudonymisation using all four transforms (redact, mask, keyed_hash, generalize), tracks patient demographic changes via SCD Type 2, detects 30-day readmissions, supports GDPR erasure requests with deletion vectors, and maintains a full audit history via Change Data Feed (CDF). The gold layer provides a star schema for clinical analytics and department-level KPI reporting.

## Table Schemas

### Bronze Layer
- **raw_admissions** (55 rows): record_id, patient_id, department_code, diagnosis_code, admission_date, discharge_date, total_charges, attending_physician, notes, ingested_at -- messy with 8 duplicates and 5 readmissions within 30 days
- **raw_patients** (25 rows): patient_id, patient_name, ssn, date_of_birth, email, address, city, state, insurance_id, insurance_name, ingested_at -- demographics with PII for pseudonymisation and 3 address changes for SCD2
- **raw_departments** (9 rows): department_code, department_name, floor, wing, ingested_at
- **raw_diagnoses** (16 rows): diagnosis_code, description, category, severity, ingested_at

### Silver Layer
- **patient_dim** (SCD2, CDF-enabled): patient_id, patient_name, ssn, date_of_birth, email, address, city, state, insurance_id, insurance_name, valid_from, valid_to, is_current, updated_at -- tracks address/insurance changes with valid_from/valid_to/is_current
- **admissions_cleaned**: record_id, patient_id, department_code, diagnosis_code, admission_date, discharge_date, los_days, total_charges, attending_physician, readmission_flag, prev_discharge_date, days_since_last_discharge, los_percentile, ingested_at, processed_at -- deduplicated, LOS calculated, readmission flagged
- **audit_log**: audit_id, table_name, patient_id, change_type, changed_fields, old_values, new_values, change_timestamp -- CDF-driven audit trail from patient_dim

### Gold Layer (Star Schema)
- **dim_department** (9 rows): department_key, department_code, department_name, floor, wing, loaded_at
- **dim_diagnosis** (16 rows): diagnosis_key, diagnosis_code, description, category, severity, loaded_at
- **fact_admissions**: admission_key, patient_id, patient_name_hash, department_key, diagnosis_key, admission_date, discharge_date, los_days, total_charges, readmission_flag, los_percentile, cost_rank, patient_valid_from, patient_valid_to, loaded_at -- point-in-time join to patient_dim
- **kpi_readmission_rates**: department_name, period (YYYY-QN), total_admissions, readmissions, readmission_pct, avg_los, avg_charges, max_los, loaded_at

## Pipeline DAG (11 steps)

```
validate_bronze
       |
  +----+----+
  |         |
dedup_admissions  build_patient_scd2
  |               |
  |          enable_cdf_audit  <-- CDF captures patient_dim changes
  |               |
  +-------+-------+
          |
  +-------+-------+
  |               |
build_dim_dept  build_dim_diag   <-- parallel
  |               |
  +-------+-------+
          |
  build_fact_admissions   <-- joins all dims with point-in-time
          |
  compute_kpi_readmission
          |
  gdpr_erasure_demo  <-- DELETE patient, VACUUM, then RESTORE
          |
  optimize_and_maintain (CONTINUE ON FAILURE)
```

## Star Schema

```
                    +-------------------+
                    | dim_department    |
                    |-------------------|
                    | department_key PK |
                    | department_code   |
                    | department_name   |
                    | floor, wing       |
                    +--------+----------+
                             |
+-------------------+  +-----+------------------+  +-------------------+
| dim_diagnosis     |  | fact_admissions        |  | kpi_readmission   |
|-------------------|  |------------------------|  | _rates            |
| diagnosis_key  PK |--| admission_key       PK |  |-------------------|
| diagnosis_code    |  | patient_id          FK |  | department_name   |
| description       |  | patient_name_hash      |  | period            |
| category          |  | department_key      FK |  | readmission_pct   |
| severity          |  | diagnosis_key       FK |  | avg_los           |
+-------------------+  | admission_date         |  | avg_charges       |
                        | discharge_date         |  +-------------------+
                        | los_days               |
                        | total_charges          |
                        | readmission_flag       |
                        | los_percentile         |
                        | cost_rank              |
                        | patient_valid_from     |
                        | patient_valid_to       |
                        +------------------------+
```

## Key Features

- **SCD Type 2** on patient_dim: tracks address and insurance changes with valid_from/valid_to/is_current columns, two-pass MERGE (expire old, insert new)
- **All 4 pseudonymisation transforms**: REDACT SSN, MASK email, keyed_hash patient_name, GENERALIZE date_of_birth
- **Change Data Feed (CDF)** on patient_dim to capture every change into audit_log
- **MERGE deduplication** on record_id to eliminate duplicate EHR feeds (8 duplicates removed)
- **Readmission detection** using LAG window function (30-day window, 5 flagged)
- **LOS percentiles** using NTILE(100) for length-of-stay distribution
- **Cost ranking** using DENSE_RANK for top-cost patient identification
- **GDPR erasure**: DELETE a patient, VACUUM to prove physical removal, RESTORE to show recovery
- **Point-in-time join**: fact_admissions joins patient_dim where admission_date BETWEEN valid_from AND valid_to
- **Incremental load** with INCREMENTAL_FILTER macro and 3-day overlap
- **Star schema** with fact + 2 dimensions + quarterly KPI aggregate

## Verification Checklist

- [ ] 9 departments loaded in dim_department
- [ ] 16 diagnosis codes loaded in dim_diagnosis
- [ ] No duplicate record_ids in silver admissions_cleaned
- [ ] SCD2 patient_dim has multiple versions for patients with address changes
- [ ] Readmission flag correctly set for 30-day re-admissions (>= 5 flagged)
- [ ] All 4 pseudonymisation rules active on patient_dim
- [ ] CDF audit_log has entries for patient_dim changes
- [ ] Referential integrity: all fact FKs resolve to dimension PKs
- [ ] Critical severity diagnoses have highest average charges
- [ ] Oncology has highest single-admission cost
- [ ] At least 4 departments have readmissions
- [ ] Data spans 10+ distinct months
- [ ] KPI readmission percentages are mathematically correct
- [ ] GDPR erasure removes patient and RESTORE recovers the table
