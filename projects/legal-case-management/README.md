# Legal Case Management Pipeline

## Scenario

A law firm tracks attorneys, cases, clients, and billing through a medallion architecture with graph-based relationship modeling. The pipeline computes attorney utilization rates, case profitability, complexity scores, and uses Cypher queries to discover co-counsel networks, attorney-client relationship depth, and practice group communities.

## Table Schemas

### Bronze Layer
- **raw_attorneys** (8 rows): attorney_id, name, bar_number, practice_area (corporate/litigation/IP/employment/real_estate), partner_flag, hourly_rate
- **raw_cases** (12 rows): case_id, case_number, case_type, court, filing_date, status (active/settled/closed)
- **raw_clients** (10 rows): client_name, client_type (corporate/individual), industry, jurisdiction
- **raw_billings** (65 rows): billing entries with hours, rates, amounts, billable flags
- **raw_case_attorneys** (20 rows): case-attorney relationships with roles (lead/co_counsel)
- **raw_case_clients** (14 rows): case-client relationships with party roles

### Silver Layer
- **billings_enriched** - Billings with case type, practice area, partner flag, complexity score

### Gold Layer (Star Schema + Graph)
- **fact_billings** - Billing facts with case/attorney/client keys, hours, rates, amounts
- **dim_case** - Case dimension with complexity scores
- **dim_attorney** - Attorney dimension (8 attorneys, 5 practice areas)
- **dim_client** - Client dimension (10 clients, 7 industries)
- **legal_network** (graph) - Attorneys, cases, clients connected via REPRESENTS and PARTY_TO edges

## Medallion Flow

```
Bronze                          Silver                          Gold
+-------------------+    +------------------------+    +-------------------+
| raw_billings      |--->| billings_enriched      |--->| fact_billings     |
| (65 entries)      |    | (complexity, enriched) |    +-------------------+
+-------------------+    +------------------------+    | dim_case          |
| raw_cases         |-----------------------------------| dim_attorney      |
| raw_attorneys     |-----------------------------------| dim_client        |
| raw_clients       |-----------------------------------+-------------------+
+-------------------+                                  | legal_network     |
| raw_case_attorneys|----------------------------------| (graph)           |
| raw_case_clients  |----------------------------------+-------------------+
+-------------------+
```

## Star Schema

```
+-------------------+    +-------------------+    +-------------------+
| dim_case          |    | fact_billings     |    | dim_attorney      |
|-------------------|    |-------------------|    |-------------------|
| case_key       PK |----| billing_key    PK |----| attorney_key   PK |
| case_number       |    | case_key       FK |    | name              |
| case_type         |    | attorney_key   FK |    | practice_area     |
| court             |    | client_key     FK |    | partner_flag      |
| complexity_score  |    | billing_date      |    | hourly_rate       |
+-------------------+    | hours             |    +-------------------+
                         | hourly_rate       |
+-------------------+    | amount            |
| dim_client        |    | billable_flag     |
|-------------------|    +-------------------+
| client_key     PK |----+
| client_name       |
| client_type       |    +-------------------+
| industry          |    | legal_network     |
+-------------------+    | (graph)           |
                         |-------------------|
                         | REPRESENTS edges  |
                         | PARTY_TO edges    |
                         +-------------------+
```

## Graph Queries

- **Co-counsel network**: Find pairs of attorneys working on the same case
- **Degree centrality**: Most connected attorneys by case count
- **Attorney-client depth**: Number of shared cases between each attorney-client pair
- **Practice group communities**: Group attorneys by practice area with case counts

## Key Features

- **Graph modeling** with Cypher queries for relationship analysis
- **Complexity scoring**: LN(total_hours) * attorney_count * party_count / 5 (normalized 1-10)
- **Attorney utilization**: billable_hours / total_hours percentage
- **Case profitability**: total billed per case with complexity weighting
- **Partner vs Associate analysis**: revenue split and rate comparison
- **Pseudonymisation**: REDACT client_name, keyed_hash case_number

## Verification Checklist

- [ ] Victoria Sterling is top revenue attorney
- [ ] Securities fraud case total billed >= $20,000
- [ ] Corporate client total billing >= $100,000
- [ ] Partner average hourly rate >= $550
- [ ] 12 cases, 8 attorneys, 10 clients in dimensions
- [ ] No orphan keys in fact table
- [ ] Graph co-counsel pairs discoverable via Cypher
- [ ] Practice group communities identifiable
