-- =============================================================================
-- Education Student Records Pipeline - Cleanup (Drop All Objects)
-- =============================================================================

-- ===================== DEACTIVATED PIPELINE =====================
-- This cleanup pipeline is DISABLED by default. It must be manually
-- activated before execution to prevent accidental data loss.
-- To run: first SET STATUS on this pipeline to 'active', then trigger.

PIPELINE education_student_records_cleanup
  DESCRIPTION 'Cleanup pipeline for Education Student Records — drops all objects. DISABLED by default.'
  TAGS 'cleanup', 'maintenance', 'education-student-records'
  STATUS disabled
  LIFECYCLE production
;


-- ===================== GOLD TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.gold.kpi_academic_performance;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.fact_enrollments;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_semester;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_instructor;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_course;
DROP TABLE IF EXISTS {{zone_prefix}}.gold.dim_student;

-- ===================== SILVER TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.silver.student_gpa;
DROP TABLE IF EXISTS {{zone_prefix}}.silver.enrollment_enriched;

-- ===================== BRONZE TABLES =====================
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_enrollments;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_semesters;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_instructors;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_courses;
DROP TABLE IF EXISTS {{zone_prefix}}.bronze.raw_students;

-- ===================== SCHEMAS =====================
DROP SCHEMA IF EXISTS {{zone_prefix}}.gold;
DROP SCHEMA IF EXISTS {{zone_prefix}}.silver;
DROP SCHEMA IF EXISTS {{zone_prefix}}.bronze;

-- ===================== ZONES =====================
DROP ZONE IF EXISTS {{zone_prefix}};
