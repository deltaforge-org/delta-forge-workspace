-- =============================================================================
-- Education Student Records Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE edu_weekly_schedule
    CRON '0 6 * * 1'
    TIMEZONE 'America/New_York'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE edu_student_records_pipeline
    DESCRIPTION 'Full load: transform student records through medallion layers with GPA calculations'
    SCHEDULE 'edu_weekly_schedule'
    TAGS 'education,academic,full-load'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: Bronze -> Silver (Enrollment Enrichment) =====================
-- Convert letter grades to grade points, calculate quality points, determine pass/fail

INSERT INTO {{zone_prefix}}.silver.enrollment_enriched
SELECT
    e.enrollment_id,
    e.student_id,
    e.course_code,
    e.instructor_id,
    e.semester_id,
    e.grade,
    CASE e.grade
        WHEN 'A'  THEN 4.0
        WHEN 'A-' THEN 3.7
        WHEN 'B+' THEN 3.3
        WHEN 'B'  THEN 3.0
        WHEN 'B-' THEN 2.7
        WHEN 'C+' THEN 2.3
        WHEN 'C'  THEN 2.0
        WHEN 'C-' THEN 1.7
        WHEN 'D'  THEN 1.0
        WHEN 'F'  THEN 0.0
        ELSE NULL
    END                                                      AS grade_points,
    c.credits,
    CASE
        WHEN e.grade IN ('A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D') THEN true
        WHEN e.grade = 'F' THEN false
        ELSE NULL
    END                                                      AS passed_flag,
    CASE
        WHEN e.grade NOT IN ('W', 'I', 'IP') THEN
            CASE e.grade
                WHEN 'A'  THEN 4.0
                WHEN 'A-' THEN 3.7
                WHEN 'B+' THEN 3.3
                WHEN 'B'  THEN 3.0
                WHEN 'B-' THEN 2.7
                WHEN 'C+' THEN 2.3
                WHEN 'C'  THEN 2.0
                WHEN 'C-' THEN 1.7
                WHEN 'D'  THEN 1.0
                WHEN 'F'  THEN 0.0
                ELSE 0.0
            END * c.credits
        ELSE 0.0
    END                                                      AS quality_points,
    e.status
FROM {{zone_prefix}}.bronze.raw_enrollments e
JOIN {{zone_prefix}}.bronze.raw_courses c ON e.course_code = c.course_code;

ASSERT ROW_COUNT = 65
SELECT 'row count check' AS status;


-- ===================== STEP 2: Bronze -> Silver (Student GPA Calculation) =====================
-- Calculate cumulative and semester GPA, flag dean's list and probation
-- Constraint: GPA must be 0.0-4.0, credits >= 0

INSERT INTO {{zone_prefix}}.silver.student_gpa
SELECT
    ee.student_id,
    ee.semester_id,
    -- Cumulative GPA (all semesters up to and including this one)
    ROUND(
        SUM(cum.quality_points) / NULLIF(SUM(cum.credits), 0),
        2
    )                                                        AS cumulative_gpa,
    -- Semester GPA (this semester only)
    ROUND(
        SUM(CASE WHEN ee.enrollment_id = sem.enrollment_id THEN sem.quality_points ELSE 0 END)
        / NULLIF(SUM(CASE WHEN ee.enrollment_id = sem.enrollment_id THEN sem.credits ELSE 0 END), 0),
        2
    )                                                        AS semester_gpa,
    SUM(cum.credits)                                         AS total_credits,
    SUM(CASE WHEN ee.enrollment_id = sem.enrollment_id THEN sem.credits ELSE 0 END) AS semester_credits,
    -- Dean's list: GPA >= 3.5
    CASE
        WHEN ROUND(SUM(cum.quality_points) / NULLIF(SUM(cum.credits), 0), 2) >= 3.50 THEN true
        ELSE false
    END                                                      AS deans_list,
    -- Probation: GPA < 2.0
    CASE
        WHEN ROUND(SUM(cum.quality_points) / NULLIF(SUM(cum.credits), 0), 2) < 2.00 THEN true
        ELSE false
    END                                                      AS probation
FROM {{zone_prefix}}.silver.enrollment_enriched ee
JOIN {{zone_prefix}}.silver.enrollment_enriched sem
    ON ee.student_id = sem.student_id AND ee.semester_id = sem.semester_id
JOIN {{zone_prefix}}.silver.enrollment_enriched cum
    ON ee.student_id = cum.student_id AND cum.semester_id <= ee.semester_id
WHERE ee.status = 'Completed'
  AND cum.status = 'Completed'
GROUP BY ee.student_id, ee.semester_id, ee.enrollment_id;

-- ===================== STEP 3: Silver -> Gold (dim_semester) =====================

INSERT INTO {{zone_prefix}}.gold.dim_semester
SELECT
    ROW_NUMBER() OVER (ORDER BY s.semester_id)              AS semester_key,
    s.semester_name,
    s.year,
    s.term,
    CAST(s.start_date AS DATE)                              AS start_date,
    CAST(s.end_date AS DATE)                                AS end_date
FROM {{zone_prefix}}.bronze.raw_semesters s;

-- ===================== STEP 4: Silver -> Gold (dim_instructor) =====================

INSERT INTO {{zone_prefix}}.gold.dim_instructor
ASSERT ROW_COUNT = 3
SELECT
    ROW_NUMBER() OVER (ORDER BY i.instructor_id)            AS instructor_key,
    i.instructor_name                                        AS name,
    i.department,
    i.rank,
    i.tenure_flag,
    i.avg_rating
FROM {{zone_prefix}}.bronze.raw_instructors i;

-- ===================== STEP 5: Silver -> Gold (dim_course) =====================

INSERT INTO {{zone_prefix}}.gold.dim_course
ASSERT ROW_COUNT = 6
SELECT
    ROW_NUMBER() OVER (ORDER BY c.course_code)              AS course_key,
    c.course_code,
    c.course_name,
    c.department,
    c.level,
    c.credits,
    c.prerequisite_code
FROM {{zone_prefix}}.bronze.raw_courses c;

-- ===================== STEP 6: Silver -> Gold (dim_student) =====================
-- Include cumulative GPA and total credits from latest semester

INSERT INTO {{zone_prefix}}.gold.dim_student
ASSERT ROW_COUNT = 12
SELECT
    ROW_NUMBER() OVER (ORDER BY s.student_id)               AS student_key,
    s.student_id,
    TRIM(s.student_name)                                     AS name,
    s.major,
    s.minor,
    s.enrollment_year,
    s.expected_graduation,
    s.status,
    gpa.cumulative_gpa,
    gpa.total_credits
FROM {{zone_prefix}}.bronze.raw_students s
LEFT JOIN (
    SELECT student_id, cumulative_gpa, total_credits,
           ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY semester_id DESC) AS rn
    FROM {{zone_prefix}}.silver.student_gpa
) gpa ON s.student_id = gpa.student_id AND gpa.rn = 1;

-- ===================== STEP 7: Silver -> Gold (fact_enrollments) =====================

INSERT INTO {{zone_prefix}}.gold.fact_enrollments
ASSERT ROW_COUNT = 18
SELECT
    ROW_NUMBER() OVER (ORDER BY ee.enrollment_id)           AS enrollment_key,
    ds.student_key,
    dc.course_key,
    di.instructor_key,
    dsem.semester_key,
    ee.grade,
    ee.grade_points,
    ee.credits,
    ee.passed_flag,
    -- GPA impact: (grade_points - current_gpa) * credits / total_credits
    CASE
        WHEN ee.grade_points IS NOT NULL AND gpa.cumulative_gpa IS NOT NULL AND gpa.total_credits > 0
        THEN ROUND((ee.grade_points - gpa.cumulative_gpa) * ee.credits / gpa.total_credits, 2)
        ELSE 0.00
    END                                                      AS gpa_impact
FROM {{zone_prefix}}.silver.enrollment_enriched ee
JOIN {{zone_prefix}}.gold.dim_student ds       ON ee.student_id = ds.student_id
JOIN {{zone_prefix}}.gold.dim_course dc        ON ee.course_code = dc.course_code
JOIN {{zone_prefix}}.gold.dim_instructor di    ON ee.instructor_id = (
    SELECT i.instructor_id FROM {{zone_prefix}}.bronze.raw_instructors i WHERE i.instructor_name = di.name
)
JOIN {{zone_prefix}}.gold.dim_semester dsem    ON ee.semester_id = (
    SELECT sem.semester_id FROM {{zone_prefix}}.bronze.raw_semesters sem WHERE sem.semester_name = dsem.semester_name
)
LEFT JOIN (
    SELECT student_id, cumulative_gpa, total_credits,
           ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY semester_id DESC) AS rn
    FROM {{zone_prefix}}.silver.student_gpa
) gpa ON ee.student_id = gpa.student_id AND gpa.rn = 1;

-- ===================== STEP 8: Silver -> Gold (kpi_academic_performance) =====================

INSERT INTO {{zone_prefix}}.gold.kpi_academic_performance
ASSERT ROW_COUNT = 65
SELECT
    dc.department,
    dsem.semester_name                                       AS semester,
    ROUND(AVG(f.grade_points), 2)                           AS avg_gpa,
    ROUND(
        100.0 * SUM(CASE WHEN f.passed_flag = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN f.grade IS NOT NULL AND f.grade NOT IN ('W', 'I', 'IP') THEN 1 END), 0),
        2
    )                                                        AS pass_rate,
    COUNT(*)                                                 AS enrollment_count,
    COUNT(DISTINCT CASE WHEN ds.cumulative_gpa >= 3.50 THEN ds.student_id END) AS deans_list_count,
    COUNT(DISTINCT CASE WHEN ds.cumulative_gpa IS NOT NULL AND ds.cumulative_gpa < 2.00 THEN ds.student_id END) AS probation_count,
    -- Retention: students enrolled this semester who were also enrolled in the previous semester
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN dsem.semester_key > 1 THEN ds.student_id END)
        / NULLIF(COUNT(DISTINCT ds.student_id), 0),
        2
    )                                                        AS retention_rate,
    ROUND(
        CAST(COUNT(*) AS DECIMAL(10,1)) / NULLIF(COUNT(DISTINCT dc.course_key), 0),
        1
    )                                                        AS avg_class_size
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_course dc        ON f.course_key = dc.course_key
JOIN {{zone_prefix}}.gold.dim_student ds       ON f.student_key = ds.student_key
JOIN {{zone_prefix}}.gold.dim_semester dsem    ON f.semester_key = dsem.semester_key
WHERE f.grade_points IS NOT NULL
GROUP BY dc.department, dsem.semester_name;

ASSERT ROW_COUNT > 0
SELECT 'row count check' AS status;


-- ===================== OPTIMIZE =====================

OPTIMIZE {{zone_prefix}}.silver.enrollment_enriched;
OPTIMIZE {{zone_prefix}}.silver.student_gpa;
OPTIMIZE {{zone_prefix}}.gold.fact_enrollments;
OPTIMIZE {{zone_prefix}}.gold.dim_student;
OPTIMIZE {{zone_prefix}}.gold.kpi_academic_performance;
