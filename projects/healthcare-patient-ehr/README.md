# Healthcare Patient EHR Pipeline

## Scenario

A hospital system processes Electronic Health Record (EHR) data through a medallion architecture pipeline. Raw admission feeds arrive with duplicate records, inconsistent formatting, and messy whitespace. The pipeline deduplicates patients, normalizes data, detects 30-day readmissions using window functions, and builds a gold-layer star schema for clinical analytics and KPI reporting.

## Table Schemas

### Bronze Layer
- **raw_admissions** - Raw EHR feed (55 rows): record_id, patient_id, patient_name, ssn, email, department_code, diagnosis_code, admission/discharge dates, charges
- **raw_departments** - Department reference (9 rows): code, name, floor, wing
- **raw_diagnoses** - Diagnosis codes (16 rows): ICD-10 codes, descriptions, category, severity

### Silver Layer
- **admissions_cleaned** - Deduplicated, cleaned admissions with computed LOS and readmission flags
- **patients_deduped** - Patient master with latest demographics and admission counts

### Gold Layer (Star Schema)
- **fact_admissions** - Admission facts with LOS percentiles (NTILE) and cost rankings (DENSE_RANK)
- **dim_department** - Department dimension (9 departments across 4 wings)
- **dim_diagnosis** - Diagnosis dimension (16 ICD-10 codes, 7 categories)
- **kpi_readmission_rates** - Department readmission KPIs by quarter

## Medallion Flow

```
Bronze                          Silver                          Gold
+-------------------+    +------------------------+    +-------------------+
| raw_admissions    |--->| admissions_cleaned     |--->| fact_admissions   |
| (55 rows, dupes)  |    | (dedup, LOS, readmit)  |    | (percentiles,rank)|
+-------------------+    +------------------------+    +-------------------+
| raw_departments   |--->|                        |--->| dim_department    |
+-------------------+    | patients_deduped       |    +-------------------+
| raw_diagnoses     |--->| (patient master)       |--->| dim_diagnosis     |
+-------------------+    +------------------------+    +-------------------+
                                                       | kpi_readmission   |
                                                       | _rates            |
                                                       +-------------------+
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
+-------------------+  +-----+-------------+  +-------------------+
| dim_diagnosis     |  | fact_admissions   |  | kpi_readmission   |
|-------------------|  |-------------------|  | _rates            |
| diagnosis_key  PK |--| admission_key  PK |  |-------------------|
| diagnosis_code    |  | patient_key    FK |  | department_name   |
| description       |  | department_key FK |  | period            |
| category          |  | diagnosis_key  FK |  | readmission_pct   |
| severity          |  | admission_date    |  | avg_los           |
+-------------------+  | discharge_date    |  | avg_charges       |
                       | los_days          |  +-------------------+
                       | total_charges     |
                       | readmission_flag  |
                       | los_percentile    |
                       | cost_rank         |
                       +-------------------+
```

## Key Features

- **MERGE deduplication** on record_id to eliminate duplicate EHR feeds
- **Readmission detection** using LAG window function (30-day window)
- **LOS percentiles** using NTILE(100) for length-of-stay distribution
- **Cost ranking** using DENSE_RANK for top-cost patient identification
- **Pseudonymisation** (HIPAA compliance): REDACT SSN, MASK email, keyed_hash patient_name
- **Incremental load** with watermark-based processing
- **Star schema** with fact + 2 dimension tables + KPI aggregate

## Verification Checklist

- [ ] 9 departments loaded in dim_department
- [ ] 16 diagnosis codes loaded in dim_diagnosis
- [ ] No duplicate record_ids in silver
- [ ] Readmission flag correctly set for 30-day re-admissions
- [ ] Referential integrity: all fact FKs resolve to dimension PKs
- [ ] Critical severity diagnoses have highest average charges
- [ ] Oncology has highest single-admission cost
- [ ] At least 3 departments have readmissions
- [ ] Data spans 10+ distinct months
- [ ] KPI readmission percentages are mathematically correct
