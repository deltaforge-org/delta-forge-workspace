# Government Tax Filing Pipeline

## Scenario

A government tax authority processes individual and business tax filings across federal and state jurisdictions. This pipeline implements the full medallion architecture to transform raw filing data into an analytical star schema for revenue analysis, compliance monitoring, audit candidate detection, and preparer performance evaluation.

The system tracks filing status changes via Change Data Feed (CDF), partitions data by fiscal year, and applies pseudonymisation to taxpayer PII (REDACT SSN, MASK taxpayer_name).

## Star Schema

```
+------------------+         +------------------+
| dim_taxpayer     |         | dim_jurisdiction |
|------------------|         |------------------|
| taxpayer_key(PK) |         | jurisdiction_key |
| taxpayer_id      |         | jurisdiction_name|
| filing_type      |         | jurisdiction_lvl |
| state            |    +----+ state            |
| income_bracket   |    |    | tax_rate         |
| dependent_count  |    |    | standard_deduct  |
+--------+---------+    |    +------------------+
         |              |
    +----+----+    +----+
    |  fact_  +----+
    | filings |
    +---------+----+    +------------------+
    |filing_key    |    | dim_preparer     |
    |taxpayer_key  |    |------------------|
    |jurisd_key    +----+ preparer_key(PK) |
    |preparer_key  |    | preparer_name    |
    |fiscal_year   |    | firm             |
    |filing_date   |    | certification    |
    |gross_income  |    | error_rate       |
    |deductions    |    +------------------+
    |taxable_income|
    |tax_owed      |
    |tax_paid      |
    |refund_amount |
    |filing_status |
    |amended_flag  |
    +--------------+

+----------------------------+
|   kpi_revenue_analysis     |
|----------------------------|
| fiscal_year                |
| jurisdiction               |
| total_filings              |
| total_taxable_income       |
| total_tax_collected        |
| total_refunds              |
| avg_effective_rate         |
| audit_flag_count           |
| compliance_rate            |
+----------------------------+
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **Pseudonymisation (REDACT)** | SSN fully redacted with `[REDACTED]` |
| **Pseudonymisation (MASK)** | Taxpayer name masked: `M*****` pattern |
| **Partitioning** | Filings partitioned by fiscal_year for query performance |
| **CDF** | Change Data Feed tracks filing status transitions (Filed -> Accepted -> Audited) |
| **Audit Detection** | Automatic flagging of deductions > 40% of gross income |
| **Effective Tax Rate** | Calculated per filing and aggregated in KPIs |
| **Income Brackets** | Dynamic derivation (Under $50K through Over $1M) |
| **NTILE Analysis** | Income quartile distribution in verification queries |
| **YoY Trends** | LAG window function for year-over-year revenue comparison |

## Data Profile

- **15 taxpayers**: 10 individuals + 5 businesses across NY, CA, TX, FL
- **5 jurisdictions**: Federal IRS + 4 state agencies
- **4 tax preparers**: CPA, EA, RTRP with varying error rates
- **42+ filings**: Spanning FY 2022-2024 with amended returns, audits, and reviews
- **Amended filings**: Post-audit corrections demonstrating CDF tracking

## Verification Checklist

- [ ] Revenue analysis KPIs exist for all fiscal years and jurisdictions
- [ ] Star schema joins produce complete filing records with all dimensions
- [ ] Audit candidates correctly flagged (deductions > 40% of income)
- [ ] Preparer error rates reflect amended filing ratios
- [ ] Income distribution NTILE analysis shows proper quartile assignment
- [ ] Year-over-year tax revenue trends calculated with LAG
- [ ] Compliance rates reflect accepted vs total filings by jurisdiction
- [ ] Refund analysis distinguishes overpayments from underpayments
- [ ] CDF captures filing status transitions during incremental load
- [ ] PERCENT_RANK correctly ranks taxpayers by total contribution
