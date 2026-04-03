# HR Workforce Analytics Pipeline

## Scenario

An enterprise HR department tracks 20 employees across 5 departments and 8 positions over a 2-year period. The pipeline implements SCD Type 2 tracking for the employee dimension (capturing title changes, department transfers, and salary adjustments as versioned records), pseudonymisation for sensitive compensation and identity data, and deletion vectors for terminated employee data erasure requests.

The gold layer provides a comprehensive workforce analytics dashboard with compensation trends, turnover analysis, gender pay gap monitoring, and compa-ratio benchmarking against pay grade ranges.

## Star Schema

```
+--------------------+
|   dim_employee     |  (SCD2)
|--------------------|
| surrogate_key (PK) |
| employee_id        |
| name               |
| hire_date          |
| termination_date   |
| education_level    |
| gender             |
| age_band           |
| valid_from         |        +--------------------+
| valid_to           |        |  dim_department    |
| is_current         |        |--------------------|
+--------+-----------+        | department_key(PK) |
         |                    | department_name    |
    +----+--------+      +---+ division           |
    |   fact_     +------+   | cost_center        |
    | compensation|          | head_count_budget  |
    |   _events   |          | manager_name       |
    +-------------+----+     +--------------------+
    | event_key        |
    | employee_key     |     +--------------------+
    | department_key   |     |   dim_position     |
    | position_key  +--+-----|--------------------|
    | event_date       |     | position_key (PK)  |
    | event_type       |     | title              |
    | base_salary      |     | job_family         |
    | bonus            |     | job_level          |
    | total_comp       |     | pay_grade_min      |
    | salary_change_pct|     | pay_grade_max      |
    | performance_rating     | exempt_flag        |
    +------------------+     +--------------------+

+----------------------------+
|  kpi_workforce_analytics   |
|----------------------------|
| department                 |
| quarter                    |
| headcount                  |
| avg_salary                 |
| median_salary              |
| turnover_rate              |
| avg_tenure_years           |
| promotion_rate             |
| gender_pay_gap_pct         |
| compa_ratio                |
+----------------------------+
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **SCD Type 2** | Two-pass MERGE: expire old records, insert new current versions for title/dept/salary changes |
| **Pseudonymisation (REDACT)** | Salary field redacted with `0.00` placeholder |
| **Pseudonymisation (MASK)** | SSN masked: `***-**-3333` pattern (last 4 visible) |
| **Pseudonymisation (keyed_hash)** | Employee name replaced with SHA256 hash |
| **Deletion Vectors** | Terminated employee EMP-005 data fully erased on request |
| **Compa-Ratio** | Actual salary / pay grade midpoint for market benchmarking |
| **Gender Pay Gap** | (avg_male - avg_female) / avg_male percentage per department |
| **Turnover Analysis** | Terminations / headcount rate by department and year |
| **Promotion Pipeline** | Promotion counts and average raise percentages by job level |

## Data Profile

- **20 employees**: Across Engineering, Marketing, Finance, HR, Sales
- **8 positions**: 4 Engineering levels (SE1 through EM), plus analyst/generalist/sales roles
- **5 departments**: With head count budgets, cost centers, and managers
- **55 compensation events**: Hires, annual raises, promotions, transfers, terminations over 2 years
- **3 terminated employees**: EMP-005, EMP-008, EMP-017

## Verification Checklist

- [ ] Workforce KPIs generated for all department/quarter combinations
- [ ] Star schema joins produce complete compensation event records
- [ ] SCD2 dimension shows multiple versions for promoted/transferred employees
- [ ] Compa-ratio correctly calculated against pay grade midpoints
- [ ] Gender pay gap analysis by department shows valid percentages
- [ ] Turnover analysis correctly identifies terminations by department/year
- [ ] Promotion pipeline shows progression through job levels
- [ ] Salary growth trajectories use LAG for period-over-period comparison
- [ ] Performance rating distribution correlates with raise percentages
- [ ] Department budget vs actual headcount shows open positions
