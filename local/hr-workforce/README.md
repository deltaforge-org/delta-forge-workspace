# HR Workforce Analytics Pipeline

## Scenario

A 500-person company tracks the full employee lifecycle: hire, promotions, transfers, compensation changes, and termination. SCD2 captures every change across 20 employees producing 32+ dimension rows. All sensitive fields are pseudonymised using all five transforms. CDF feeds an org change log for compliance. GDPR erasure demonstrates DELETE + VACUUM + VERSION AS OF + RESTORE proving physical data removal. Gold star schema drives compa-ratio analysis, gender pay gap detection, and retention risk scoring.

## Table Schemas

### Bronze Layer
- **raw_employees** (20 rows): 10M/10F across 6 departments, 10 positions, with SSN/DOB/email/salary
- **raw_comp_events** (55 rows): 20 hires, 12 salary adjustments, 8 promotions, 5 transfers, 3 bonuses, 5 terminations, 2 PIPs
- **raw_departments** (6 rows): with budgets and head count targets
- **raw_positions** (10 rows): with pay grade min/max for compa-ratio

### Silver Layer
- **employee_dim** (SCD2, 32+ rows): CDF-enabled, tracks title/dept/salary changes with valid_from/valid_to/is_current
- **comp_events_enriched** (55 rows): total_comp, salary_change_pct, compa_ratio
- **org_change_log** (CDF-driven): every dimension change captured

### Gold Layer (Star Schema + KPIs)
- **dim_department** (6 departments with budgets)
- **dim_position** (10 positions with pay_grade_midpoint)
- **fact_compensation** (55 events: base_salary, bonus, total_comp, salary_change_pct, performance_rating, compa_ratio)
- **kpi_workforce_analytics** (headcount, avg/median salary, turnover_rate, promotion_rate, gender_pay_gap_pct per department x quarter)
- **kpi_retention_risk** (tenure, salary trend, promotion velocity, compa_ratio -> risk score)

## SCD2 Design (3-Pass)

```
Pass 1: INSERT initial hire records (20 rows, all is_current=true)
Pass 2: MERGE to EXPIRE current rows where promotion/transfer/salary change occurred
Pass 3: INSERT new current versions with latest state
Result: 20 initial + 5 promotions + 3 transfers + 4 salary adj = 32+ rows
        Active current: 15 (5 terminated)
```

## Pipeline DAG (13 Steps)

```
validate_bronze
       |
  +----+----------------------------+
build_employee_scd2_initial          |
       |                             |
expire_scd2_records                  |
       |                             |
insert_scd2_current    enrich_comp_events   <-- parallel
       |                      |
  enable_cdf_org_log          |
       |                      |
       +----------+-----------+
                  |
  +---------------+---------------+
dim_department              dim_position        <-- parallel
  |                              |
  +------------------------------+
                  |
    build_fact_compensation
                  |
    +-------------+-------------+
kpi_workforce    kpi_retention_risk              <-- parallel
    |                  |
    +--------+---------+
             |
  gdpr_erasure_demo (DELETE + VACUUM + VERSION AS OF + RESTORE)
             |
  optimize (CONTINUE ON FAILURE)
```

## Pseudonymisation (All 5 Transforms)

| Field | Transform | Details |
|---|---|---|
| SSN | REDACT | `***-**-****` |
| email | MASK | Show first 3 characters |
| employee_name | keyed_hash | SCOPE person, salted SHA256 |
| date_of_birth | generalize | 10-year range bands |
| base_salary | REDACT | `[REDACTED]` |

## Retention Risk Score Formula

```
risk_score =
  CASE WHEN tenure_years < 1 THEN 25 ELSE 0 END
+ CASE WHEN salary_change_pct < 3 THEN 20 ELSE 0 END
+ CASE WHEN promotions_in_3yr = 0 THEN 25 ELSE 0 END
+ CASE WHEN compa_ratio < 0.85 THEN 30 ELSE 0 END

Categories: high (>= 50), medium (>= 25), low (< 25)
```

## GDPR Erasure Demo

1. Show employee EMP-005 exists (SELECT)
2. DELETE all SCD2 rows for EMP-005
3. DELETE comp events for EMP-005
4. VACUUM to physically remove data
5. ASSERT 0 rows remain
6. VERSION AS OF 1 to prove prior existence
7. RESTORE TO VERSION 1 to demonstrate recovery
8. Re-delete to finalize erasure

## Seed Data Profile

- 20 employees: 10 Male / 10 Female for pay gap analysis
- 6 departments: Engineering, Marketing, Finance, HR, Sales, Operations
- 10 positions: 4 Engineering levels + analyst/generalist/sales/ops roles
- 55 compensation events over 2+ years
- 3 underpaid employees (compa-ratio < 0.80): measurable in fact table
- 1 overpaid employee (compa-ratio > 1.20)
- Measurable gender pay gap in Engineering and Sales departments
- 5 terminated employees for turnover analysis

## Verification Checklist

- [ ] 6 departments, 10 positions in dimensions
- [ ] SCD2 has >= 32 rows with exactly 1 current row per active employee
- [ ] Multi-version employees exist (promotions/transfers)
- [ ] EMP-002 history shows hire -> SE2 promotion -> SSE promotion
- [ ] Fact compensation has 55 rows with full star schema join
- [ ] Compa-ratio detects >= 3 underpaid employees (< 0.80)
- [ ] Gender pay gap analysis shows measurable gap in 2+ departments
- [ ] Workforce KPIs generated for all department/quarter combinations
- [ ] Retention risk identifies at least 1 high-risk employee
- [ ] Turnover analysis correctly identifies terminations
- [ ] Org change log populated from SCD2 dimension changes
- [ ] Salary growth trajectories use LAG for period-over-period comparison
- [ ] Department budget vs actual headcount shows open positions
- [ ] GDPR erasure: EMP-005 deleted, VACUUM proven, VERSION AS OF works
