-- =============================================================================
-- Food Safety Inspections Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- New Q3 2024 inspections added, plus RESTORE demo for an incorrectly scored batch.

-- Show current state before incremental load
SELECT 'silver.inspections_scored' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.inspections_scored
UNION ALL
SELECT 'gold.fact_inspections', COUNT(*)
FROM {{zone_prefix}}.gold.fact_inspections;

-- =============================================================================
-- Insert 6 new bronze inspection records (Q3 2024 batch)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_inspections VALUES
('I066', 'EST-001', 'INS-001', 'Downtown', '2024-07-08', 'Routine',     'V009',               false, false, '2024-07-09T00:00:00'),
('I067', 'EST-002', 'INS-005', 'Downtown', '2024-07-10', 'Routine',     'V001,V007,V010',     true,  false, '2024-07-11T00:00:00'),
('I068', 'EST-004', 'INS-002', 'Midtown',  '2024-07-12', 'Routine',     'V002,V006',          true,  false, '2024-07-13T00:00:00'),
('I069', 'EST-007', 'INS-003', 'Uptown',   '2024-07-15', 'Routine',     NULL,                 false, false, '2024-07-16T00:00:00'),
('I070', 'EST-010', 'INS-004', 'Harbor',   '2024-07-11', 'Routine',     'V001,V003,V005,V007',true,  true,  '2024-07-12T00:00:00'),
('I071', 'EST-011', 'INS-004', 'Harbor',   '2024-07-18', 'Routine',     'V006,V010',          false, false, '2024-07-19T00:00:00');

ASSERT ROW_COUNT = 6
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE: only process new inspections using INCREMENTAL_FILTER
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.inspections_scored AS tgt
USING (
    WITH violation_points AS (
        SELECT
            i.inspection_id,
            COALESCE(SUM(v.points_deducted), 0) AS total_points_deducted,
            SUM(CASE WHEN v.severity = 'Critical' THEN 1 ELSE 0 END) AS critical_violations,
            SUM(CASE WHEN v.severity = 'Non-Critical' THEN 1 ELSE 0 END) AS non_critical_violations
        FROM {{zone_prefix}}.bronze.raw_inspections i
        LEFT JOIN {{zone_prefix}}.bronze.raw_violations v
            ON i.violation_codes LIKE '%' || v.violation_code || '%'
        WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.inspections_scored, inspection_id, inspection_date, 7)}}
        GROUP BY i.inspection_id
    )
    SELECT
        i.inspection_id,
        i.establishment_id,
        i.inspector_id,
        i.district,
        i.inspection_date,
        i.inspection_type,
        GREATEST(0, 100 - vp.total_points_deducted) AS score,
        CASE
            WHEN GREATEST(0, 100 - vp.total_points_deducted) >= 90 THEN 'A'
            WHEN GREATEST(0, 100 - vp.total_points_deducted) >= 80 THEN 'B'
            WHEN GREATEST(0, 100 - vp.total_points_deducted) >= 70 THEN 'C'
            ELSE 'F'
        END AS grade,
        COALESCE(vp.critical_violations, 0) AS critical_violations,
        COALESCE(vp.non_critical_violations, 0) AS non_critical_violations,
        COALESCE(vp.total_points_deducted, 0) AS total_points_deducted,
        i.follow_up_required,
        i.closure_ordered,
        CURRENT_TIMESTAMP AS scored_at
    FROM {{zone_prefix}}.bronze.raw_inspections i
    LEFT JOIN violation_points vp ON i.inspection_id = vp.inspection_id
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.inspections_scored, inspection_id, inspection_date, 7)}}
) AS src
ON tgt.inspection_id = src.inspection_id
WHEN MATCHED THEN UPDATE SET
    tgt.score                   = src.score,
    tgt.grade                   = src.grade,
    tgt.critical_violations     = src.critical_violations,
    tgt.non_critical_violations = src.non_critical_violations,
    tgt.total_points_deducted   = src.total_points_deducted,
    tgt.scored_at               = src.scored_at
WHEN NOT MATCHED THEN INSERT (
    inspection_id, establishment_id, inspector_id, district, inspection_date,
    inspection_type, score, grade, critical_violations, non_critical_violations,
    total_points_deducted, follow_up_required, closure_ordered, scored_at
) VALUES (
    src.inspection_id, src.establishment_id, src.inspector_id, src.district,
    src.inspection_date, src.inspection_type, src.score, src.grade,
    src.critical_violations, src.non_critical_violations, src.total_points_deducted,
    src.follow_up_required, src.closure_ordered, src.scored_at
);

-- Refresh gold fact incrementally
MERGE INTO {{zone_prefix}}.gold.fact_inspections AS tgt
USING (
    SELECT
        sc.inspection_id AS inspection_key,
        sc.establishment_id AS establishment_key,
        sc.inspector_id AS inspector_key,
        sc.district AS district_key,
        CASE
            WHEN sc.critical_violations > 0 THEN 'CRITICAL'
            WHEN sc.non_critical_violations > 0 THEN 'NON-CRITICAL'
            ELSE 'NONE'
        END AS violation_key,
        sc.inspection_date,
        sc.inspection_type,
        sc.score,
        sc.grade,
        sc.critical_violations,
        sc.non_critical_violations,
        sc.follow_up_required,
        sc.closure_ordered
    FROM {{zone_prefix}}.silver.inspections_scored sc
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_inspections, inspection_key, inspection_date, 7)}}
) AS src
ON tgt.inspection_key = src.inspection_key
WHEN MATCHED THEN UPDATE SET
    tgt.score               = src.score,
    tgt.grade               = src.grade,
    tgt.critical_violations = src.critical_violations
WHEN NOT MATCHED THEN INSERT (
    inspection_key, establishment_key, inspector_key, district_key, violation_key,
    inspection_date, inspection_type, score, grade, critical_violations,
    non_critical_violations, follow_up_required, closure_ordered
) VALUES (
    src.inspection_key, src.establishment_key, src.inspector_key, src.district_key,
    src.violation_key, src.inspection_date, src.inspection_type, src.score, src.grade,
    src.critical_violations, src.non_critical_violations, src.follow_up_required,
    src.closure_ordered
);

-- =============================================================================
-- Verify incremental processing
-- =============================================================================

-- Silver should now have 65 + 6 = 71 rows
SELECT COUNT(*) AS silver_total FROM {{zone_prefix}}.silver.inspections_scored;
-- Verify gold fact updated
ASSERT ROW_COUNT = 71
SELECT COUNT(*) AS gold_total FROM {{zone_prefix}}.gold.fact_inspections;
ASSERT VALUE gold_total >= 71
SELECT 'gold_total check passed' AS gold_total_status;


-- =============================================================================
-- RESTORE DEMO: Rollback incorrectly scored batch
-- =============================================================================
-- Scenario: Q3 batch I066-I071 was scored incorrectly. Roll back silver to
-- the version before the incremental load.

-- Check current version
SELECT COUNT(*) AS current_count FROM {{zone_prefix}}.silver.inspections_scored;

-- Restore to version 1 (before incremental load)
RESTORE {{zone_prefix}}.silver.inspections_scored TO VERSION AS OF 1;

-- Verify rollback
SELECT COUNT(*) AS restored_count FROM {{zone_prefix}}.silver.inspections_scored;
ASSERT ROW_COUNT = 65
SELECT 'row count check' AS status;

