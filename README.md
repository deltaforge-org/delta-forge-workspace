# DeltaForge Workspace

Enterprise pipeline cookbook with **13 advanced projects** demonstrating medallion architecture, scheduled multi-step pipelines, incremental/full loads, PII protection, and DeltaForge features in combination.

Each project has a unique feature combination — no two projects demonstrate the same patterns.

## Quick Start

1. Open DeltaForge GUI
2. Navigate to any project under `projects/`
3. **Standard projects** (12 of 13) — run files in numbered order:
   - `01_create_objects.sql` — Creates zone, schemas, tables, seeds bronze data
   - `02_full_load.sql` — Multi-step PIPELINE with STEP DAG (bronze → silver → gold)
   - `03_incremental_load.sql` — Delta processing with `INCREMENTAL_FILTER` macro
   - `04_verify_gold.sql` — Validates gold-layer star schema with ASSERT statements
   - `05_cleanup.sql` — Deactivated pipeline (STATUS disabled) — must be enabled manually
4. **Split-file project** (`supply-chain-analytics`) — actions organized per file by layer:
   - `bronze/*.sql` → `silver/*.sql` → `gold/*.sql` → `maintenance/*.sql`
   - Orchestrated by `pipeline.sql` using `INCLUDE SCRIPT` references

## Zone Naming

Each project creates a dedicated zone with a fixed name. No template variables — all SQL uses actual values:

| Project | Zone | Example table reference |
|---------|------|----------------------|
| healthcare-patient-ehr | `ehr` | `ehr.bronze.raw_admissions` |
| banking-transactions | `bank` | `bank.silver.transactions_enriched` |
| ecommerce-orders | `ecom` | `ecom.gold.fact_order_lines` |
| insurance-claims | `ins` | `ins.silver.policy_dim` |
| telecom-cdr | `telco` | `telco.bronze.raw_cdr_v1` |
| manufacturing-iot | `mfg` | `mfg.gold.kpi_oee` |
| logistics-shipments | `logi` | `logi.silver.events_deduped` |
| government-tax-filing | `tax` | `tax.gold.fact_filings` |
| real-estate-property | `realty` | `realty.silver.property_dim` |
| legal-case-management | `legal` | `legal.gold.legal_network` |
| hr-workforce | `hr` | `hr.silver.employee_dim` |
| cybersecurity-incidents | `cyber` | `cyber.gold.fact_incidents` |
| supply-chain-analytics | `sc` | `sc.gold.fact_inventory` |

## Incremental Loading

Projects use the `INCREMENTAL_FILTER` macro for dynamic watermark-based loading:

```sql
SELECT * FROM ehr.bronze.raw_admissions
WHERE {{INCREMENTAL_FILTER(ehr.silver.admissions_cleaned, record_id, admission_date, 3)}};
-- Expands to: record_id > 'R-055' AND admission_date > '2024-06-01'
-- Or: 1=1 (if target is empty — first run = full load)
```

## Projects

| # | Project | Industry | Unique Feature Combination | Tables | Steps |
|---|---------|----------|---------------------------|--------|-------|
| 1 | healthcare-patient-ehr | Healthcare | All 4 pseudonymisation transforms + SCD2 + CDF audit + GDPR erasure + RESTORE | 11 | 11 |
| 2 | banking-transactions | Banking | CDF → materialized snapshots + fraud scoring + SCD2 customer + time travel audit | 12 | 11 |
| 3 | ecommerce-orders | E-Commerce | Multi-source MERGE (3 channels) + soft delete + RFM segmentation + funnel sessionization + CDF inventory | 17 | 13 |
| 4 | insurance-claims | Insurance | SCD2 (3 batches) + point-in-time joins + fraud outlier detection + RESTORE correction | 13 | 14 |
| 5 | telecom-cdr | Telecom | Schema evolution lifecycle (v1→v2→v3 + type widening) + session windows + churn scoring | 14 | 14 |
| 6 | manufacturing-iot | Manufacturing | Anomaly detection (moving avg + stddev) + OEE formula + VACUUM retention + CHECK constraints | 14 | 11 |
| 7 | logistics-shipments | Logistics | Event sourcing + idempotent composite-key dedup + SLA violation detection + schema evolution | 16 | 12 |
| 8 | government-tax-filing | Government | Append-only + amendment MERGE preserving originals + CDF audit trail + bloom filters | 13 | 13 |
| 9 | real-estate-property | Real Estate | SCD2 assessment history (3 batches) + RESTORE correction + assessment accuracy analysis | 12 | 14 |
| 10 | legal-case-management | Legal | Graph/Cypher (PageRank, Louvain, conflict check) + pseudonymisation + Delta star schema | 12 | 13 |
| 11 | hr-workforce | HR | All 5 pseudonymisation transforms + SCD2 + CDF org log + GDPR erasure + compa-ratio + pay gap | 12 | 13 |
| 12 | cybersecurity-incidents | Cybersecurity | 3-source SIEM ingestion + 5-min window dedup + MITRE ATT&CK + bloom filters + incident correlation | 16 | 12 |
| 13 | supply-chain-analytics | Supply Chain | **Split-file pattern**: INCLUDE SCRIPT per step + procurement→warehouse→transport→demand pipeline | 20 | 15 |

## Pipeline Patterns

### Standard: Single-file STEP DAG
```sql
PIPELINE my_pipeline SCHEDULE 'daily_schedule' ...;

STEP validate_bronze TIMEOUT '2m' AS ...;
STEP transform DEPENDS ON (validate_bronze) AS ...;
STEP build_dim_a DEPENDS ON (transform) AS ...;   -- parallel
STEP build_dim_b DEPENDS ON (transform) AS ...;   -- parallel
STEP build_fact DEPENDS ON (build_dim_a, build_dim_b) AS ...;
STEP optimize DEPENDS ON (build_fact) CONTINUE ON FAILURE AS ...;
```

### Split-file: INCLUDE SCRIPT per step
```sql
STEP validate_bronze AS INCLUDE SCRIPT 'silver/01_validate_bronze.sql';
STEP build_inventory DEPENDS ON (validate_bronze) AS INCLUDE SCRIPT 'silver/02_inventory_positions.sql';
```

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │         Zone: {{zone_prefix}}           │
                    │                                         │
                    │  ┌───────────┐  ┌───────────┐  ┌─────────────┐
                    │  │  Schema:  │  │  Schema:  │  │   Schema:   │
                    │  │  bronze   │  │  silver   │  │    gold     │
                    │  │           │  │           │  │             │
                    │  │ Raw feeds │  │ Deduped   │  │ Star Schema │
                    │  │ Messy data│─→│ Enriched  │─→│ Fact tables │
                    │  │ Duplicates│  │ Pseudonym │  │ Dimensions  │
                    │  │           │  │           │  │ KPI metrics │
                    │  └───────────┘  └───────────┘  └─────────────┘
                    └─────────────────────────────────────────┘
```

Each project gets **one dedicated zone** with three schemas (`bronze`, `silver`, `gold`), keeping workshop projects cleanly isolated.
