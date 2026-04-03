-- =============================================================================
-- Education Student Records Pipeline - Incremental Load
-- =============================================================================
-- MERGE upsert: update incomplete grades, add new enrollments

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "enrollment_id > 'ENR-057' AND semester_id > 'SEM-F24'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.enrollment_enriched, enrollment_id, semester_id, 7)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.enrollment_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_enrollments
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.enrollment_enriched, enrollment_id, semester_id, 7)}};

-- ===================== STEP 1: Watermark Check =====================

SELECT MAX(semester_id) AS last_semester
FROM {{zone_prefix}}.silver.enrollment_enriched;

-- ===================== STEP 2: New/Updated Bronze Enrollments =====================
-- Grade updates for incompletes and withdrawals, plus new Spring 2025 registrations

INSERT INTO {{zone_prefix}}.bronze.raw_enrollments VALUES
    -- Incomplete resolved: STU-014 PHYS101 gets final grade
    ('ENR-058-U', 'STU-014', 'PHYS101', 'INS-05', 'SEM-F24', 'B-', 'Completed', '2025-02-01T06:00:00');

-- ===================== STEP 3: MERGE Upsert — Update Existing + Insert New =====================

MERGE INTO {{zone_prefix}}.silver.enrollment_enriched AS tgt
USING (
ASSERT ROW_COUNT = 1
    SELECT
        e.enrollment_id,
        e.student_id,
        e.course_code,
        e.instructor_id,
        e.semester_id,
        e.grade,
        CASE e.grade
            WHEN 'A'  THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3
            WHEN 'B'  THEN 3.0 WHEN 'B-' THEN 2.7 WHEN 'C+' THEN 2.3
            WHEN 'C'  THEN 2.0 WHEN 'C-' THEN 1.7 WHEN 'D'  THEN 1.0
            WHEN 'F'  THEN 0.0 ELSE NULL
        END AS grade_points,
        c.credits,
        CASE
            WHEN e.grade IN ('A','A-','B+','B','B-','C+','C','C-','D') THEN true
            WHEN e.grade = 'F' THEN false ELSE NULL
        END AS passed_flag,
        CASE
            WHEN e.grade NOT IN ('W','I','IP') THEN
                CASE e.grade
                    WHEN 'A'  THEN 4.0 WHEN 'A-' THEN 3.7 WHEN 'B+' THEN 3.3
                    WHEN 'B'  THEN 3.0 WHEN 'B-' THEN 2.7 WHEN 'C+' THEN 2.3
                    WHEN 'C'  THEN 2.0 WHEN 'C-' THEN 1.7 WHEN 'D'  THEN 1.0
                    WHEN 'F'  THEN 0.0 ELSE 0.0
                END * c.credits
            ELSE 0.0
        END AS quality_points,
        e.status
    FROM {{zone_prefix}}.bronze.raw_enrollments e
    JOIN {{zone_prefix}}.bronze.raw_courses c ON e.course_code = c.course_code
    WHERE e.ingested_at > '2025-01-01T12:00:00'
) AS src
ON tgt.enrollment_id = REPLACE(src.enrollment_id, '-U', '')
   AND tgt.student_id = src.student_id
WHEN MATCHED AND tgt.status IN ('Incomplete', 'Withdrawn') THEN UPDATE SET
    grade = src.grade,
    grade_points = src.grade_points,
    passed_flag = src.passed_flag,
    quality_points = src.quality_points,
    status = src.status
WHEN NOT MATCHED THEN INSERT (
    enrollment_id, student_id, course_code, instructor_id, semester_id,
    grade, grade_points, credits, passed_flag, quality_points, status
) VALUES (
    src.enrollment_id, src.student_id, src.course_code, src.instructor_id,
    src.semester_id, src.grade, src.grade_points, src.credits, src.passed_flag,
    src.quality_points, src.status
);

-- ===================== STEP 4: Refresh Student GPA =====================

DELETE FROM {{zone_prefix}}.silver.student_gpa WHERE student_id = 'STU-014';

INSERT INTO {{zone_prefix}}.silver.student_gpa
SELECT
    ee.student_id,
    ee.semester_id,
    ROUND(SUM(ee.quality_points) / NULLIF(SUM(ee.credits), 0), 2) AS cumulative_gpa,
    ROUND(SUM(ee.quality_points) / NULLIF(SUM(ee.credits), 0), 2) AS semester_gpa,
    SUM(ee.credits) AS total_credits,
    SUM(ee.credits) AS semester_credits,
    CASE WHEN ROUND(SUM(ee.quality_points) / NULLIF(SUM(ee.credits), 0), 2) >= 3.50 THEN true ELSE false END AS deans_list,
    CASE WHEN ROUND(SUM(ee.quality_points) / NULLIF(SUM(ee.credits), 0), 2) < 2.00 THEN true ELSE false END AS probation
FROM {{zone_prefix}}.silver.enrollment_enriched ee
WHERE ee.student_id = 'STU-014'
  AND ee.status = 'Completed'
GROUP BY ee.student_id, ee.semester_id;

-- ===================== STEP 5: Verify Constraints =====================
-- GPA must be between 0.0 and 4.0

ASSERT VALUE cumulative_gpa >= 0.00 WHERE student_id = 'STU-014'
-- Credits must be >= 0
ASSERT VALUE total_credits >= 0 WHERE student_id = 'STU-014'
ASSERT VALUE cumulative_gpa <= 4.00 WHERE student_id = 'STU-014'
SELECT 'total_credits check passed' AS total_credits_status;


OPTIMIZE {{zone_prefix}}.silver.enrollment_enriched;
