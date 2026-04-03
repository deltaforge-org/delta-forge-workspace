# Food Safety Inspections Pipeline

## Scenario

A city health department conducts food safety inspections across 15 establishments (restaurants, food trucks, catering, bakeries) in 4 districts (Downtown, Midtown, Uptown, Harbor). The pipeline scores inspections by deducting violation points from a base score of 100, assigns letter grades (A/B/C/F), identifies repeat offenders with multiple critical violations, and analyzes inspector calibration to detect inconsistent grading patterns.

The system tracks 12 violation types across 5 severity categories, handles re-inspections (follow-ups), complaint-driven inspections, and closures. A RESTORE demonstration rolls back an incorrectly scored batch. Inspector names are pseudonymised via masking.

## Star Schema

```
+--------------------+     +------------------+     +------------------+
| dim_establishment  |     | dim_inspector    |     | dim_district     |
|--------------------|     |------------------|     |------------------|
| establishment_key  |<-+  | inspector_key    |<-+  | district_key     |<-+
| name               |  |  | name (MASKED)    |  |  | district_name    |  |
| cuisine_type       |  |  | cert_level       |  |  | city / state     |  |
| seating_capacity   |  |  | years_experience |  |  | population       |  |
| owner_name         |  |  | avg_score_given  |  |  | estab_count      |  |
| chain_flag         |  |  | inspection_count |  |  | avg_score        |  |
| previous_score     |  |  +------------------+  |  +------------------+  |
| risk_category      |  |                        |                        |
+--------------------+  |  +--------------------+|  +------------------+  |
                        |  | fact_inspections   ||  | dim_violation    |  |
                        +--| inspection_key     ||  |------------------|  |
                           | establishment_key  ||  | violation_key    |<-+
                           | inspector_key   FK |+  | violation_code   |  |
                           | district_key    FK |-->| description      |  |
                           | violation_key   FK |-->| category         |  |
                           | inspection_date    |   | severity         |  |
                           | inspection_type    |   | points_deducted  |  |
                           | score / grade      |   | corrective_action|  |
                           | critical_violations|   +------------------+  |
                           | follow_up_required |                         |
                           | closure_ordered    |                         |
                           +--------------------+                         |
                                                                          |
                           +--------------------+                         |
                           | kpi_compliance     |                         |
                           |--------------------|                         |
                           | district / cuisine |                         |
                           | quarter            |                         |
                           | avg_score          |                         |
                           | pass_rate          |                         |
                           | critical_viol_rate |                         |
                           | repeat_offenders   |                         |
                           | inspector_consist  |                         |
                           | improvement_trend  |                         |
                           +--------------------+                         |
```

## Medallion Flow

```
BRONZE                       SILVER                          GOLD
+------------------+    +------------------------+    +---------------------+
| raw_inspections  |--->| inspections_scored(CDF)|--->| fact_inspections     |
| (65+ records)    |    | - 100 minus violations  |    | dim_establishment   |
+------------------+    | - grade A/B/C/F         |    | dim_inspector       |
| raw_establishments|   | - CHECK score 0-100     |    | dim_district        |
| raw_inspectors   |    +------------------------+    | dim_violation       |
| raw_violations   |                                  | kpi_compliance      |
+------------------+                                  +---------------------+
```

## Features

- **CHECK constraints**: Score 0-100 enforced, valid grades at silver layer
- **RESTORE**: Demonstrates rollback of incorrectly scored batch to previous version
- **Partitioning**: Inspections partitioned by district for efficient queries
- **Inspector calibration**: Compares avg scores given by each inspector vs overall average
- **Repeat offender detection**: Flags establishments with 2+ critical violations
- **Establishment improvement tracking**: LAG window function for score trend analysis
- **Grade assignment**: A (90-100), B (80-89), C (70-79), F (<70)
- **INCREMENTAL_FILTER macro**: Incremental load uses inspection_id + inspection_date with 7-day overlap
- **Pseudonymisation**: Inspector names masked with asterisks

## Seed Data

- **15 establishments** across 4 districts (mix of restaurants, food trucks, catering, bakery)
- **5 inspectors** with varying experience and certification levels
- **12 violation types** across 6 categories (temperature, hygiene, sanitation, facility, documentation)
- **65 inspection records** spanning Q1-Q2 2024
- Includes routine inspections, follow-ups, complaint-driven inspections, and 3 closures
- Repeat offenders: EST-002 (Golden Dragon), EST-010 (Fresh Catch), EST-005 (Taco Fiesta)

## Verification Checklist

- [ ] All 15 establishments in dim_establishment with risk categories
- [ ] All 5 inspectors in dim_inspector with avg_score_given
- [ ] All 4 districts in dim_district with population
- [ ] All 12 violation types in dim_violation
- [ ] 65+ inspections in fact table with correct scores
- [ ] Grades assigned correctly (A/B/C/F based on score ranges)
- [ ] Repeat offenders identified (EST-002, EST-010 with 3+ critical violations)
- [ ] Inspector calibration shows variation in scoring patterns
- [ ] Improvement tracking shows score trends per establishment
- [ ] RESTORE demo rolls back silver to pre-incremental state
