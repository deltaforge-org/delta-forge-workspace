# Delta Forge Workspace

Enterprise pipeline cookbook with **25 self-contained projects** demonstrating medallion architecture, scheduled pipelines, incremental/full loads, PII protection, and advanced Delta Forge features.

## Quick Start

1. Open Delta Forge GUI
2. Navigate to any project under `projects/`
3. Run files in numbered order:
   - `01_create_objects.sql` — Creates zones, schemas, tables, seeds bronze data
   - `02_full_load.sql` — Runs the full pipeline (bronze → silver → gold)
   - `03_incremental_load.sql` — Demonstrates incremental delta processing
   - `04_verify_gold.sql` — Validates gold-layer metrics with ASSERT statements
   - `05_cleanup.sql` — Tears down all created objects (deactivated pipeline — must be enabled manually)

## Template Variables

All SQL files use these template variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `{{data_path}}` | Root path for Delta table storage | Yes |
| `{{zone_prefix}}` | Zone name for this project (each project gets one dedicated zone) | No |
| `{{current_user}}` | Current user for GRANT statements | Yes |

## Incremental Loading

Projects use the `INCREMENTAL_FILTER` macro for dynamic watermark-based loading:

```sql
-- Reads MAX values from the target table and generates a WHERE clause
SELECT * FROM {{zone_prefix}}.bronze.source_table
WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.target_table, id, updated_at, 3)}};

-- Expands to: id > 12345 AND updated_at > '2024-01-15'
-- Or: 1=1 (if target is empty — first run = full load)
```

## Projects

| # | Project | Industry | Key Features | Load Type |
|---|---------|----------|-------------|-----------|
| 1 | healthcare-patient-ehr | Healthcare | MERGE dedup, pseudonymisation, constraints, window fn | Both |
| 2 | banking-transactions | Banking | CDF, time travel, partitioning, deletion vectors | Incremental |
| 3 | retail-pos-sales | Retail | Bloom filters, Z-ordering, partition pruning, window fn | Both |
| 4 | telecom-cdr | Telecom | Schema evolution, MERGE upsert, VACUUM, OPTIMIZE | Incremental |
| 5 | insurance-claims | Insurance | MERGE SCD2, time travel joins, CHECK constraints | Full |
| 6 | ecommerce-orders | E-Commerce | Soft delete, MERGE multi-source, CDF | Incremental |
| 7 | manufacturing-iot | Manufacturing | Partitioning, OPTIMIZE, moving avg, constraints | Incremental |
| 8 | pharma-clinical-trials | Pharma | All pseudonymisation transforms, deletion vectors, RESTORE | Full |
| 9 | logistics-shipments | Logistics | MERGE idempotent, schema evolution, Z-ordering | Incremental |
| 10 | energy-smart-meters | Energy | Partitioning, bloom filters, VACUUM retention | Incremental |
| 11 | education-student-records | Education | Constraints (GPA), MERGE upsert, time travel | Full |
| 12 | government-tax-filing | Government | Pseudonymisation REDACT, partitioning, CDF | Both |
| 13 | media-streaming | Media | Window fn (sessions), deletion vectors, OPTIMIZE | Incremental |
| 14 | agriculture-crop-yields | Agriculture | Schema evolution, constraints, partitioning | Full |
| 15 | aviation-flight-ops | Aviation | MERGE composite keys, Z-ordering, time travel | Both |
| 16 | real-estate-property | Real Estate | SCD2, bloom filters, RESTORE | Full |
| 17 | hospitality-reservations | Hospitality | Soft delete, MERGE dedup, occupancy window fn | Incremental |
| 18 | legal-case-management | Legal | Graph/Cypher, pseudonymisation, constraints | Full |
| 19 | automotive-telematics | Automotive | Partitioning, schema evolution, deletion vectors | Incremental |
| 20 | nonprofit-donations | Nonprofit | CDF, constraints, time travel | Both |
| 21 | mining-operations | Mining | Partitioning, cumulative window fn, VACUUM | Incremental |
| 22 | sports-analytics | Sports | Graph/Cypher, MERGE upsert, Z-ordering | Full |
| 23 | hr-workforce | HR | SCD2, pseudonymisation (all transforms), deletion vectors | Both |
| 24 | cybersecurity-incidents | Cybersecurity | MERGE dedup, bloom filters, OPTIMIZE | Incremental |
| 25 | food-safety-inspections | Food & Bev | Constraints, RESTORE, partitioning | Full |

## Architecture

Each project follows the **medallion pattern**:

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

Each project gets **one dedicated zone** with three schemas (`bronze`, `silver`, `gold`). This keeps all workshop projects cleanly isolated while minimizing zone noise in the catalog.
