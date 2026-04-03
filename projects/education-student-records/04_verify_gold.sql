-- =============================================================================
-- Education Student Records Pipeline - Gold Layer Verification
-- =============================================================================

-- ===================== QUERY 1: Academic Performance KPI Summary =====================

SELECT
    k.department,
    k.semester,
    k.avg_gpa,
    k.pass_rate,
    k.enrollment_count,
    k.deans_list_count,
    k.probation_count,
    k.retention_rate,
    k.avg_class_size
FROM {{zone_prefix}}.gold.kpi_academic_performance k
ORDER BY k.department, k.semester;

-- ===================== QUERY 2: Star Schema Join — Full Enrollment Detail =====================

ASSERT ROW_COUNT > 0
SELECT
    f.enrollment_key,
    ds.student_id,
    ds.name,
    ds.major,
    dc.course_code,
    dc.course_name,
    dc.department,
    di.name              AS instructor_name,
    dsem.semester_name,
    f.grade,
    f.grade_points,
    f.credits,
    f.passed_flag,
    f.gpa_impact
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_student ds       ON f.student_key = ds.student_key
JOIN {{zone_prefix}}.gold.dim_course dc        ON f.course_key = dc.course_key
JOIN {{zone_prefix}}.gold.dim_instructor di    ON f.instructor_key = di.instructor_key
JOIN {{zone_prefix}}.gold.dim_semester dsem    ON f.semester_key = dsem.semester_key
ORDER BY dsem.semester_name, ds.student_id, dc.course_code;

-- ===================== QUERY 3: GPA Distribution using PERCENT_RANK =====================

ASSERT VALUE enrollment_key > 0
SELECT
    ds.student_id,
    ds.name,
    ds.major,
    ds.cumulative_gpa,
    ds.total_credits,
    ROUND(PERCENT_RANK() OVER (ORDER BY ds.cumulative_gpa), 4) AS gpa_percentile,
    CASE
        WHEN ds.cumulative_gpa >= 3.90 THEN 'Summa Cum Laude'
        WHEN ds.cumulative_gpa >= 3.70 THEN 'Magna Cum Laude'
        WHEN ds.cumulative_gpa >= 3.50 THEN 'Cum Laude'
        WHEN ds.cumulative_gpa >= 2.00 THEN 'Good Standing'
        WHEN ds.cumulative_gpa IS NOT NULL THEN 'Academic Probation'
        ELSE 'No GPA'
    END                                                          AS academic_standing
FROM {{zone_prefix}}.gold.dim_student ds
WHERE ds.cumulative_gpa IS NOT NULL
ORDER BY ds.cumulative_gpa DESC;

ASSERT VALUE cumulative_gpa >= 0.00
-- ===================== QUERY 4: Department Performance Dashboard =====================

ASSERT VALUE cumulative_gpa <= 4.00
SELECT
    dc.department,
    COUNT(*)                                                    AS total_enrollments,
    COUNT(DISTINCT ds.student_id)                              AS unique_students,
    ROUND(AVG(f.grade_points), 2)                             AS dept_avg_gpa,
    ROUND(100.0 * SUM(CASE WHEN f.passed_flag = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN f.grade_points IS NOT NULL THEN 1 END), 0), 2) AS pass_rate_pct,
    ROUND(AVG(f.credits), 1)                                  AS avg_credits_per_course
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_course dc ON f.course_key = dc.course_key
JOIN {{zone_prefix}}.gold.dim_student ds ON f.student_key = ds.student_key
WHERE f.grade_points IS NOT NULL
GROUP BY dc.department
ORDER BY dept_avg_gpa DESC;

-- ===================== QUERY 5: Instructor Effectiveness =====================
-- Compare avg grade given vs instructor rating

ASSERT ROW_COUNT = 4
SELECT
    di.name                                                     AS instructor,
    di.department,
    di.rank,
    di.avg_rating,
    COUNT(*)                                                    AS sections_taught,
    COUNT(DISTINCT dc.course_key)                              AS unique_courses,
    ROUND(AVG(f.grade_points), 2)                             AS avg_grade_given,
    ROUND(AVG(f.grade_points) - di.avg_rating, 2)            AS grade_vs_rating_diff
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_instructor di ON f.instructor_key = di.instructor_key
JOIN {{zone_prefix}}.gold.dim_course dc     ON f.course_key = dc.course_key
WHERE f.grade_points IS NOT NULL
GROUP BY di.name, di.department, di.rank, di.avg_rating
ORDER BY avg_grade_given DESC;

-- ===================== QUERY 6: Semester-over-Semester GPA Trends =====================

ASSERT ROW_COUNT = 6
SELECT
    dsem.semester_name,
    dc.department,
    ROUND(AVG(f.grade_points), 2)                             AS avg_gpa,
    LAG(ROUND(AVG(f.grade_points), 2)) OVER (PARTITION BY dc.department ORDER BY dsem.semester_key) AS prev_sem_gpa,
    ROUND(AVG(f.grade_points), 2) -
        LAG(ROUND(AVG(f.grade_points), 2)) OVER (PARTITION BY dc.department ORDER BY dsem.semester_key) AS gpa_change
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_course dc     ON f.course_key = dc.course_key
JOIN {{zone_prefix}}.gold.dim_semester dsem ON f.semester_key = dsem.semester_key
WHERE f.grade_points IS NOT NULL
GROUP BY dsem.semester_name, dsem.semester_key, dc.department
ORDER BY dc.department, dsem.semester_key;

-- ===================== QUERY 7: Student Retention Analysis =====================
-- Students enrolled in consecutive semesters

ASSERT VALUE avg_gpa > 0
SELECT
    dsem.semester_name,
    COUNT(DISTINCT ds.student_id)                              AS enrolled_students,
    COUNT(DISTINCT CASE WHEN ds.status = 'Active' THEN ds.student_id END) AS active_students,
    COUNT(DISTINCT CASE WHEN ds.status = 'Withdrawn' THEN ds.student_id END) AS withdrawn,
    COUNT(DISTINCT CASE WHEN ds.status = 'Graduated' THEN ds.student_id END) AS graduated
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_student ds       ON f.student_key = ds.student_key
JOIN {{zone_prefix}}.gold.dim_semester dsem    ON f.semester_key = dsem.semester_key
GROUP BY dsem.semester_name
ORDER BY dsem.semester_name;

-- ===================== QUERY 8: Prerequisite Chain Validation =====================
-- Verify students took prerequisites before advanced courses

ASSERT ROW_COUNT = 3
SELECT
    ds.student_id,
    ds.name,
    dc.course_code,
    dc.course_name,
    dc.prerequisite_code,
    CASE
        WHEN dc.prerequisite_code IS NULL THEN 'No Prereq'
        WHEN EXISTS (
            SELECT 1 FROM {{zone_prefix}}.gold.fact_enrollments f2
            JOIN {{zone_prefix}}.gold.dim_course dc2 ON f2.course_key = dc2.course_key
            WHERE f2.student_key = f.student_key
              AND dc2.course_code = dc.prerequisite_code
              AND f2.passed_flag = true
        ) THEN 'Prereq Met'
        ELSE 'Prereq Missing'
    END                                                          AS prereq_status
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_student ds   ON f.student_key = ds.student_key
JOIN {{zone_prefix}}.gold.dim_course dc    ON f.course_key = dc.course_key
WHERE dc.prerequisite_code IS NOT NULL
ORDER BY ds.student_id, dc.course_code;

-- ===================== QUERY 9: Dean's List and Probation Summary =====================

ASSERT VALUE course_code IS NOT NULL
SELECT
    ds.major,
    COUNT(DISTINCT ds.student_id)                              AS total_students,
    COUNT(DISTINCT CASE WHEN ds.cumulative_gpa >= 3.50 THEN ds.student_id END) AS deans_list,
    COUNT(DISTINCT CASE WHEN ds.cumulative_gpa < 2.00 THEN ds.student_id END) AS probation,
    ROUND(AVG(ds.cumulative_gpa), 2)                          AS avg_major_gpa,
    ROUND(AVG(ds.total_credits), 0)                           AS avg_credits_earned
FROM {{zone_prefix}}.gold.dim_student ds
WHERE ds.cumulative_gpa IS NOT NULL
GROUP BY ds.major
ORDER BY avg_major_gpa DESC;

-- ===================== QUERY 10: Course Difficulty Ranking =====================

ASSERT VALUE total_students > 0
SELECT
    dc.course_code,
    dc.course_name,
    dc.department,
    dc.level,
    COUNT(*)                                                    AS total_enrolled,
    ROUND(AVG(f.grade_points), 2)                             AS avg_grade,
    ROUND(100.0 * SUM(CASE WHEN f.grade IN ('D', 'F') THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN f.grade_points IS NOT NULL THEN 1 END), 0), 2) AS dfw_rate_pct,
    RANK() OVER (ORDER BY AVG(f.grade_points))                AS difficulty_rank
FROM {{zone_prefix}}.gold.fact_enrollments f
JOIN {{zone_prefix}}.gold.dim_course dc ON f.course_key = dc.course_key
WHERE f.grade_points IS NOT NULL
GROUP BY dc.course_code, dc.course_name, dc.department, dc.level
ORDER BY avg_grade;

ASSERT VALUE total_enrolled > 0
SELECT 'total_enrolled check passed' AS total_enrolled_status;

