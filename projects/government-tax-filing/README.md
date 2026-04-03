# Government Tax Filing Pipeline

## Scenario

A government tax authority processes individual and business tax filings across federal and state jurisdictions. Filed returns are append-only (immutable once filed). Amendments arrive as separate records that NEVER overwrite originals. The pipeline tracks every amendment via CDF for audit compliance, partitions by fiscal year, pseudonymises taxpayer PII, and builds analytics for revenue forecasting and preparer quality monitoring.

## Unique Combination

Append-only enforcement + amendment MERGE that preserves originals + CDF audit trail + partitioning by fiscal_year + pseudonymisation (REDACT SSN, MASK name) + bloom filter on taxpayer_id + star schema with amendment-aware fact + preparer quality KPI

## Star Schema

```
+------------------+         +-------------------+
| dim_taxpayer     |         | dim_jurisdiction  |
|------------------|         |-------------------|
| taxpayer_key(PK) |         | jurisdiction_key  |
| taxpayer_id      |         | jurisdiction_id   |
| filing_type      |    +----+ jurisdiction_name |
| state            |    |    | jurisdiction_level|
| income_bracket   |    |    | state             |
| dependent_count  |    |    | base_tax_rate     |
| total_filings    |    |    | standard_deduction|
| ever_audited     |    |    +-------------------+
+--------+---------+    |
         |              |    +-------------------+
    +----+----+----+----+    | dim_preparer      |
    |   fact_filings   |    |-------------------|
    |------------------|    | preparer_key (PK) |
    | filing_key       +----+ preparer_id       |
    | taxpayer_key     |    | preparer_name     |
    | jurisdiction_key |    | firm              |
    | preparer_key     |    | certification     |
    | fiscal_year_key  |    | amendment_rate    |
    | gross_income     |    +-------------------+
    | deductions       |
    | taxable_income   |    +-------------------+
    | tax_owed         |    | dim_fiscal_year   |
    | effective_       +----+-------------------|
    |   taxable_income |    | fiscal_year_key   |
    | effective_       |    | fiscal_year       |
    |   tax_owed       |    | filing_deadline   |
    | was_amended      |    | total_filings     |
    | amendment_reason |    | total_amendments  |
    | amendment_count  |    | audit_flag_count  |
    | effective_tax_   |    +-------------------+
    |   rate           |
    | audit_flag       |
    +------------------+

+-----------------------------+   +----------------------------+
| kpi_revenue_analysis        |   | kpi_preparer_quality       |
|-----------------------------|   |----------------------------|
| fiscal_year                 |   | preparer_name              |
| jurisdiction_name           |   | firm                       |
| total_filings               |   | certification              |
| total_taxable_income        |   | total_filings              |
| total_tax_collected         |   | amendment_count            |
| total_refunds               |   | amendment_rate_pct         |
| avg_effective_rate          |   | audit_count                |
| audit_flag_count            |   | audit_rate_pct             |
| compliance_rate             |   | avg_client_income          |
| amendment_count             |   | avg_deduction_pct          |
| audit_yield_pct             |   | avg_effective_rate         |
+-----------------------------+   +----------------------------+
```

## Pipeline DAG

```
validate_bronze
       |
  +----+------------+
append_filings    apply_amendments       <-- parallel (append-only + amendment MERGE)
       |                  |
  enable_cdf_audit        |
       |                  |
  build_taxpayer_profiles |
       |                  |
       +--------+---------+
                |
  +-------------+-------------+-------------------+
build_dim_    build_dim_    build_dim_    build_dim_
 taxpayer     jurisdiction   preparer    fiscal_year   <-- parallel
  |              |              |              |
  +--------------+--------------+--------------+
                         |
              build_fact_filings              <-- joins originals + amendments
                         |
              +----------+-----------+
  build_kpi_revenue_analysis  build_kpi_preparer_quality   <-- parallel
              |                         |
              +-----------+-------------+
                          |
           bloom_and_optimize (CONTINUE ON FAILURE)
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **Append-only Filings** | filings_immutable receives INSERTs only -- no UPDATE, no DELETE |
| **Separate Amendments** | amendments_applied table with MERGE, computing delta from original |
| **CDF Audit Trail** | Change Data Feed on filings_immutable materialized into audit_trail |
| **Pseudonymisation (REDACT)** | SSN fully redacted with `[REDACTED]` |
| **Pseudonymisation (MASK)** | Taxpayer name masked: `M*****` pattern |
| **Bloom Filter** | Fast taxpayer_id lookup on filings_immutable |
| **Partitioning** | Filings partitioned by fiscal_year |
| **Audit Detection** | Automatic flagging of deductions > 40% of gross income |
| **Amendment Impact** | effective_taxable_income and effective_tax_owed via COALESCE |
| **Preparer Quality** | Amendment rates and audit rates per preparer |
| **NTILE Analysis** | Income quartile distribution in verification queries |
| **YoY Trends** | LAG window function for year-over-year revenue comparison |

## Data Profile

- **20 taxpayers**: 14 individuals + 6 businesses across NY, CA, TX, FL, IL
- **6 jurisdictions**: Federal IRS + 5 state agencies
- **5 tax preparers**: CPA, EA, RTRP with varying amendment rates
- **50 filings**: 20 FY2022, 18 FY2023, 12 FY2024 with audits and reviews
- **12 amendments**: Referencing specific filing_ids with delta computation
- **4 audit-flagged filings**: High deductions (>40% of gross income)
- **2 high-amendment-rate preparers**: PREP-02 and PREP-04

## Verification Checklist

- [ ] Revenue analysis KPIs exist for all fiscal years and jurisdictions
- [ ] Star schema joins produce 50+ filing records with all 4 dimensions
- [ ] Audit candidates correctly flagged (deductions > 40% of income)
- [ ] Amendment impact shows original vs effective taxable_income and tax_owed
- [ ] Preparer quality KPI shows amendment_rate_pct and audit_rate_pct for all 5
- [ ] Fiscal year dimension has filing_deadline, total_filings, amendment counts
- [ ] YoY tax revenue trends calculated with LAG by jurisdiction
- [ ] Income distribution NTILE analysis shows proper quartile assignment
- [ ] Compliance rates reflect accepted vs total filings by jurisdiction
- [ ] Audit trail has 50+ records covering all filings and amendments
- [ ] Refund analysis distinguishes overpayments from underpayments by type
- [ ] PERCENT_RANK correctly ranks taxpayers by total contribution
