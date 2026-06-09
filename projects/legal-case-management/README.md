# Legal Case Management Pipeline

## Scenario

A litigation analytics firm tracks cases, parties, attorneys, and billing through a medallion architecture with a property graph for relationship analysis. Cypher queries reveal hidden connections between parties, conflict-of-interest chains, and attorney influence networks. PageRank identifies the most-influential attorneys, community detection (Louvain) discovers practice groups from co-counsel edges, and multi-hop traversals perform conflict-of-interest checks. PII of clients and witnesses is pseudonymised.

## Table Schemas

### Bronze Layer
- **raw_cases** (15 rows): 5 civil, 3 criminal, 3 corporate, 2 IP, 2 employment
- **raw_parties** (25 rows): plaintiffs, defendants, witnesses, experts with SSN/email/phone
- **raw_attorneys** (10 rows): across 3 practice groups (litigation, corporate, ip)
- **raw_billings** (70 rows): hours, rates, amounts, billable flags, billing types (billable, admin, pro_bono)
- **raw_relationships** (40 rows): typed/weighted edges: represents, opposes, witnesses_for, co_counsel, referral

### Silver Layer
- **cases_enriched** - Duration, attorney count, billing totals, complexity score
- **billings_validated** - CHECK constraints (hours > 0, rate > 0), joined with case/attorney metadata
- **party_profiles** - Aggregated involvement counts per party

### Gold Layer (Star Schema + Graph)
- **dim_case** (15 cases with complexity scores)
- **dim_attorney** (10 attorneys, 3 practice groups)
- **dim_party** (25 parties)
- **fact_billings** (70+ billing facts with case/attorney keys)
- **kpi_firm_performance** (utilization rates, revenue per attorney, case profitability)
- **legal_network** (graph: parties as vertices, relationships as typed/weighted edges)

## Pipeline DAG (13 Steps)

```
validate_bronze
       |
  +----+-------------------+
enrich_cases    validate_billings      <-- parallel
  |                  |
  +------------------+
           |
  build_party_profiles
           |
  +--------+----------+
dim_case  dim_attorney  dim_party      <-- parallel
  |          |            |
  +----------+------------+
             |
  build_fact_billings
             |
  compute_firm_kpi
             |
  build_legal_graph (CREATE GRAPH with typed edges)
             |
  run_graph_analytics (PageRank, Louvain, conflict check, adversarial pairs)
             |
  restore_demo
             |
  optimize (CONTINUE ON FAILURE)
```

## Star Schema

```
+-------------------+    +-------------------+    +-------------------+
| dim_case          |    | fact_billings     |    | dim_attorney      |
|-------------------|    |-------------------|    |-------------------|
| case_key       PK |--->| billing_key    PK |<---| attorney_key   PK |
| case_number       |    | case_key       FK |    | attorney_name     |
| case_type         |    | attorney_key   FK |    | practice_group    |
| court             |    | billing_date      |    | partner_flag      |
| complexity_score  |    | hours             |    | hourly_rate       |
+-------------------+    | hourly_rate       |    +-------------------+
                         | amount            |
+-------------------+    | billable_flag     |
| dim_party         |    | case_complexity   |
|-------------------|    +-------------------+
| party_key      PK |
| party_name        |    +-------------------------+
| party_type        |    | kpi_firm_performance    |
| organization      |    |-------------------------|
+-------------------+    | utilization_pct         |
                         | total_revenue           |
                         | revenue_per_hour        |
                         +-------------------------+
```

## Graph Queries

- **PageRank**: Most influential nodes across the legal network
- **Community Detection (Louvain)**: Practice groups from co_counsel edges
- **Multi-hop (3 hops)**: Conflict-of-interest check from any party
- **Betweenness Centrality**: Attorneys bridging practice groups
- **Adversarial Pairs**: (attorney)-[:represents]->(party)-[:opposes]->(opponent)<-[:represents]-(attorney)

## Pseudonymisation

| Field | Transform | Details |
|---|---|---|
| SSN | REDACT | Masked as `***-**-****` |
| party_name | keyed_hash | SCOPE person, salted SHA256 |
| contact_email | MASK | Show first 3 characters |
| contact_phone | MASK | Show last 4 digits |

## Seed Data Profile

- 15 cases across 5 types with varying priority and status
- 25 parties: plaintiffs, defendants, witnesses, experts
- 10 attorneys across 3 practice groups (3 partners)
- 70 billing entries including non-billable (admin, pro-bono)
- 40 relationship edges with 5 types and weights
- 2 frequent co-counsel pairs (A001-A002, A005-A007)
- 1 conflict-of-interest chain (A009 connected to opposing parties via referral)
- 3 high-complexity cases (C001, C006, C009)

## Verification Checklist

- [ ] 15 cases, 10 attorneys, 25 parties in dimensions
- [ ] Victoria Sterling is top revenue attorney
- [ ] Partner average hourly rate >= $550
- [ ] Criminal case C006 total billed >= $25,000
- [ ] No orphan keys in fact table
- [ ] 3 practice groups in KPI table
- [ ] 10 attorneys with utilization metrics
- [ ] Graph co-counsel pairs via Cypher
- [ ] PageRank identifies influential nodes
- [ ] Community detection finds practice groups
- [ ] Multi-hop conflict check traverses 3 hops
- [ ] Adversarial pairs pattern matching works
