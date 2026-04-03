-- =============================================================================
-- Education Student Records Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================
-- Features: Constraints (GPA 0.0-4.0, credits >= 0), MERGE upsert,
-- time travel (semester snapshots), pseudonymisation (HASH student_name, student_id)

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw enrollment and course data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched enrollment and GPA calculations';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Academic star schema and performance KPIs';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_students (
    student_id          STRING      NOT NULL,
    student_name        STRING,
    major               STRING,
    minor               STRING,
    enrollment_year     INT,
    expected_graduation STRING,
    status              STRING,
    email               STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/academic/raw_students';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_students TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_courses (
    course_code         STRING      NOT NULL,
    course_name         STRING      NOT NULL,
    department          STRING,
    level               INT,
    credits             INT,
    prerequisite_code   STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/academic/raw_courses';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_courses TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_instructors (
    instructor_id       STRING      NOT NULL,
    instructor_name     STRING      NOT NULL,
    department          STRING,
    rank                STRING,
    tenure_flag         BOOLEAN,
    avg_rating          DECIMAL(3,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/academic/raw_instructors';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_instructors TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_semesters (
    semester_id         STRING      NOT NULL,
    semester_name       STRING      NOT NULL,
    year                INT,
    term                STRING,
    start_date          STRING,
    end_date            STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/academic/raw_semesters';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_semesters TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_enrollments (
    enrollment_id       STRING      NOT NULL,
    student_id          STRING      NOT NULL,
    course_code         STRING      NOT NULL,
    instructor_id       STRING      NOT NULL,
    semester_id         STRING      NOT NULL,
    grade               STRING,
    status              STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/academic/raw_enrollments';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_enrollments TO USER {{current_user}};

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.enrollment_enriched (
    enrollment_id       STRING      NOT NULL,
    student_id          STRING      NOT NULL,
    course_code         STRING      NOT NULL,
    instructor_id       STRING      NOT NULL,
    semester_id         STRING      NOT NULL,
    grade               STRING,
    grade_points        DECIMAL(3,1),
    credits             INT,
    passed_flag         BOOLEAN,
    quality_points      DECIMAL(5,1),
    status              STRING
) LOCATION '{{data_path}}/silver/academic/enrollment_enriched'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.enrollment_enriched TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.student_gpa (
    student_id          STRING      NOT NULL,
    semester_id         STRING,
    cumulative_gpa      DECIMAL(3,2),
    semester_gpa        DECIMAL(3,2),
    total_credits       INT,
    semester_credits    INT,
    deans_list          BOOLEAN,
    probation           BOOLEAN
) LOCATION '{{data_path}}/silver/academic/student_gpa';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.student_gpa TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_enrollments (
    enrollment_key      INT         NOT NULL,
    student_key         INT         NOT NULL,
    course_key          INT         NOT NULL,
    instructor_key      INT         NOT NULL,
    semester_key        INT         NOT NULL,
    grade               STRING,
    grade_points        DECIMAL(3,1),
    credits             INT,
    passed_flag         BOOLEAN,
    gpa_impact          DECIMAL(5,2)
) LOCATION '{{data_path}}/gold/academic/fact_enrollments';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_enrollments TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_student (
    student_key         INT         NOT NULL,
    student_id          STRING      NOT NULL,
    name                STRING,
    major               STRING,
    minor               STRING,
    enrollment_year     INT,
    expected_graduation STRING,
    status              STRING,
    cumulative_gpa      DECIMAL(3,2),
    total_credits       INT
) LOCATION '{{data_path}}/gold/academic/dim_student';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_student TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_course (
    course_key          INT         NOT NULL,
    course_code         STRING      NOT NULL,
    course_name         STRING,
    department          STRING,
    level               INT,
    credits             INT,
    prerequisite_code   STRING
) LOCATION '{{data_path}}/gold/academic/dim_course';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_course TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_instructor (
    instructor_key      INT         NOT NULL,
    name                STRING      NOT NULL,
    department          STRING,
    rank                STRING,
    tenure_flag         BOOLEAN,
    avg_rating          DECIMAL(3,2)
) LOCATION '{{data_path}}/gold/academic/dim_instructor';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_instructor TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_semester (
    semester_key        INT         NOT NULL,
    semester_name       STRING      NOT NULL,
    year                INT,
    term                STRING,
    start_date          DATE,
    end_date            DATE
) LOCATION '{{data_path}}/gold/academic/dim_semester';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_semester TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_academic_performance (
    department          STRING      NOT NULL,
    semester            STRING      NOT NULL,
    avg_gpa             DECIMAL(3,2),
    pass_rate           DECIMAL(5,2),
    enrollment_count    INT,
    deans_list_count    INT,
    probation_count     INT,
    retention_rate      DECIMAL(5,2),
    avg_class_size      DECIMAL(5,1)
) LOCATION '{{data_path}}/gold/academic/kpi_academic_performance';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_academic_performance TO USER {{current_user}};

-- ===================== BRONZE SEED DATA: SEMESTERS (3 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_semesters VALUES
    ('SEM-F23', 'Fall 2023',   2023, 'Fall',   '2023-08-28', '2023-12-15', '2025-01-01T00:00:00'),
    ('SEM-S24', 'Spring 2024', 2024, 'Spring', '2024-01-16', '2024-05-10', '2025-01-01T00:00:00'),
    ('SEM-F24', 'Fall 2024',   2024, 'Fall',   '2024-08-26', '2024-12-13', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_semesters;


-- ===================== BRONZE SEED DATA: INSTRUCTORS (6 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_instructors VALUES
    ('INS-01', 'Dr. Sarah Chen',       'Computer Science',    'Professor',           true,  4.52, '2025-01-01T00:00:00'),
    ('INS-02', 'Dr. Michael Torres',   'Computer Science',    'Associate Professor', true,  4.15, '2025-01-01T00:00:00'),
    ('INS-03', 'Dr. Emily Watson',     'Mathematics',         'Professor',           true,  4.38, '2025-01-01T00:00:00'),
    ('INS-04', 'Dr. James Liu',        'Mathematics',         'Assistant Professor', false, 3.87, '2025-01-01T00:00:00'),
    ('INS-05', 'Dr. Anna Kowalski',    'Physics',             'Associate Professor', true,  4.25, '2025-01-01T00:00:00'),
    ('INS-06', 'Dr. Robert Nakamura',  'English',             'Professor',           true,  4.60, '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_instructors;


-- ===================== BRONZE SEED DATA: COURSES (12 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_courses VALUES
    ('CS101',  'Introduction to Programming',  'Computer Science', 100, 3, NULL,    '2025-01-01T00:00:00'),
    ('CS201',  'Data Structures',              'Computer Science', 200, 3, 'CS101', '2025-01-01T00:00:00'),
    ('CS301',  'Algorithms',                   'Computer Science', 300, 3, 'CS201', '2025-01-01T00:00:00'),
    ('CS350',  'Database Systems',             'Computer Science', 300, 3, 'CS201', '2025-01-01T00:00:00'),
    ('MATH101','Calculus I',                   'Mathematics',      100, 4, NULL,    '2025-01-01T00:00:00'),
    ('MATH201','Calculus II',                  'Mathematics',      200, 4, 'MATH101','2025-01-01T00:00:00'),
    ('MATH301','Linear Algebra',              'Mathematics',      300, 3, 'MATH201','2025-01-01T00:00:00'),
    ('MATH250','Statistics',                   'Mathematics',      200, 3, NULL,    '2025-01-01T00:00:00'),
    ('PHYS101','Physics I',                    'Physics',          100, 4, NULL,    '2025-01-01T00:00:00'),
    ('PHYS201','Physics II',                   'Physics',          200, 4, 'PHYS101','2025-01-01T00:00:00'),
    ('ENG101', 'English Composition',          'English',          100, 3, NULL,    '2025-01-01T00:00:00'),
    ('ENG201', 'Technical Writing',            'English',          200, 3, 'ENG101','2025-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_courses;


-- ===================== BRONZE SEED DATA: STUDENTS (18 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_students VALUES
    ('STU-001', 'Alice Johnson',    'Computer Science', 'Mathematics', 2022, '2026-05', 'Active',    'alice.j@university.edu',    '2025-01-01T00:00:00'),
    ('STU-002', 'Brian Williams',   'Computer Science', NULL,          2022, '2026-05', 'Active',    'brian.w@university.edu',    '2025-01-01T00:00:00'),
    ('STU-003', 'Carla Martinez',   'Mathematics',      'Physics',     2022, '2026-05', 'Active',    'carla.m@university.edu',    '2025-01-01T00:00:00'),
    ('STU-004', 'David Kim',        'Computer Science', NULL,          2023, '2027-05', 'Active',    'david.k@university.edu',    '2025-01-01T00:00:00'),
    ('STU-005', 'Elena Petrov',     'Physics',          'Mathematics', 2022, '2026-05', 'Active',    'elena.p@university.edu',    '2025-01-01T00:00:00'),
    ('STU-006', 'Frank Okafor',     'Computer Science', NULL,          2023, '2027-05', 'Active',    'frank.o@university.edu',    '2025-01-01T00:00:00'),
    ('STU-007', 'Grace Lee',        'Mathematics',      NULL,          2023, '2027-05', 'Active',    'grace.l@university.edu',    '2025-01-01T00:00:00'),
    ('STU-008', 'Henry Chen',       'Computer Science', 'English',     2022, '2026-05', 'Active',    'henry.c@university.edu',    '2025-01-01T00:00:00'),
    ('STU-009', 'Irene Novak',      'Physics',          NULL,          2023, '2027-05', 'Probation', 'irene.n@university.edu',    '2025-01-01T00:00:00'),
    ('STU-010', 'James Brown',      'English',          NULL,          2022, '2026-05', 'Active',    'james.b@university.edu',    '2025-01-01T00:00:00'),
    ('STU-011', 'Karen Singh',      'Computer Science', 'Mathematics', 2023, '2027-05', 'Active',    'karen.s@university.edu',    '2025-01-01T00:00:00'),
    ('STU-012', 'Leo Yamamoto',     'Mathematics',      NULL,          2022, '2026-05', 'Active',    'leo.y@university.edu',      '2025-01-01T00:00:00'),
    ('STU-013', 'Maya Patel',       'Computer Science', NULL,          2023, '2027-05', 'Withdrawn', 'maya.p@university.edu',     '2025-01-01T00:00:00'),
    ('STU-014', 'Nathan Brooks',    'Physics',          'English',     2022, '2026-05', 'Active',    'nathan.b@university.edu',   '2025-01-01T00:00:00'),
    ('STU-015', 'Olivia Schmidt',   'Mathematics',      NULL,          2023, '2027-05', 'Active',    'olivia.s@university.edu',   '2025-01-01T00:00:00'),
    ('STU-016', 'Paul Rivera',      'Computer Science', NULL,          2022, '2026-05', 'Graduated', 'paul.r@university.edu',     '2025-01-01T00:00:00'),
    ('STU-017', 'Quinn Murphy',     'English',          'Computer Science', 2023, '2027-05', 'Active', 'quinn.m@university.edu',  '2025-01-01T00:00:00'),
    ('STU-018', 'Rachel Adams',     'Physics',          NULL,          2023, '2027-05', 'Active',    'rachel.a@university.edu',   '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 18
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_students;


-- ===================== BRONZE SEED DATA: ENROLLMENTS (65 rows) =====================
-- Grades: A=4.0, A-=3.7, B+=3.3, B=3.0, B-=2.7, C+=2.3, C=2.0, C-=1.7, D=1.0, F=0.0
-- Includes withdrawals (W), incompletes (I), in-progress (IP)

INSERT INTO {{zone_prefix}}.bronze.raw_enrollments VALUES
    -- Fall 2023 enrollments
    ('ENR-001', 'STU-001', 'CS201',   'INS-01', 'SEM-F23', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-002', 'STU-001', 'MATH201', 'INS-03', 'SEM-F23', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-003', 'STU-001', 'ENG101',  'INS-06', 'SEM-F23', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-004', 'STU-002', 'CS201',   'INS-01', 'SEM-F23', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-005', 'STU-002', 'MATH101', 'INS-03', 'SEM-F23', 'C+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-006', 'STU-003', 'MATH201', 'INS-03', 'SEM-F23', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-007', 'STU-003', 'PHYS101', 'INS-05', 'SEM-F23', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-008', 'STU-003', 'CS101',   'INS-02', 'SEM-F23', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-009', 'STU-004', 'CS101',   'INS-02', 'SEM-F23', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-010', 'STU-004', 'MATH101', 'INS-04', 'SEM-F23', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-011', 'STU-005', 'PHYS201', 'INS-05', 'SEM-F23', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-012', 'STU-005', 'MATH301', 'INS-04', 'SEM-F23', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-013', 'STU-006', 'CS101',   'INS-01', 'SEM-F23', 'B-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-014', 'STU-006', 'ENG101',  'INS-06', 'SEM-F23', 'C',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-015', 'STU-007', 'MATH101', 'INS-03', 'SEM-F23', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-016', 'STU-007', 'MATH250', 'INS-04', 'SEM-F23', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-017', 'STU-008', 'CS301',   'INS-02', 'SEM-F23', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-018', 'STU-008', 'ENG201',  'INS-06', 'SEM-F23', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-019', 'STU-009', 'PHYS101', 'INS-05', 'SEM-F23', 'D',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-020', 'STU-009', 'MATH101', 'INS-04', 'SEM-F23', 'F',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-021', 'STU-010', 'ENG101',  'INS-06', 'SEM-F23', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-022', 'STU-010', 'MATH250', 'INS-03', 'SEM-F23', 'C+', 'Completed', '2025-01-01T00:00:00'),
    -- Spring 2024 enrollments
    ('ENR-023', 'STU-001', 'CS301',   'INS-02', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-024', 'STU-001', 'CS350',   'INS-01', 'SEM-S24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-025', 'STU-002', 'CS301',   'INS-02', 'SEM-S24', 'B-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-026', 'STU-002', 'MATH201', 'INS-04', 'SEM-S24', 'C',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-027', 'STU-003', 'MATH301', 'INS-03', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-028', 'STU-003', 'PHYS201', 'INS-05', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-029', 'STU-004', 'CS201',   'INS-01', 'SEM-S24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-030', 'STU-004', 'MATH250', 'INS-03', 'SEM-S24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-031', 'STU-005', 'MATH250', 'INS-04', 'SEM-S24', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-032', 'STU-005', 'ENG101',  'INS-06', 'SEM-S24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-033', 'STU-006', 'CS201',   'INS-02', 'SEM-S24', 'C+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-034', 'STU-007', 'MATH201', 'INS-03', 'SEM-S24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-035', 'STU-008', 'CS350',   'INS-01', 'SEM-S24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-036', 'STU-009', 'PHYS101', 'INS-05', 'SEM-S24', 'C-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-037', 'STU-009', 'MATH101', 'INS-04', 'SEM-S24', 'D',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-038', 'STU-010', 'ENG201',  'INS-06', 'SEM-S24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-039', 'STU-011', 'CS101',   'INS-01', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-040', 'STU-011', 'MATH101', 'INS-03', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-041', 'STU-012', 'MATH301', 'INS-04', 'SEM-S24', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-042', 'STU-013', 'CS101',   'INS-02', 'SEM-S24', 'W',  'Withdrawn', '2025-01-01T00:00:00'),
    ('ENR-043', 'STU-014', 'PHYS201', 'INS-05', 'SEM-S24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-044', 'STU-014', 'ENG201',  'INS-06', 'SEM-S24', 'A',  'Completed', '2025-01-01T00:00:00'),
    -- Fall 2024 enrollments (mix of completed and in-progress)
    ('ENR-045', 'STU-001', 'MATH301', 'INS-03', 'SEM-F24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-046', 'STU-002', 'CS350',   'INS-01', 'SEM-F24', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-047', 'STU-003', 'CS201',   'INS-02', 'SEM-F24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-048', 'STU-004', 'CS301',   'INS-02', 'SEM-F24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-049', 'STU-004', 'MATH201', 'INS-04', 'SEM-F24', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-050', 'STU-005', 'CS101',   'INS-01', 'SEM-F24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-051', 'STU-006', 'CS301',   'INS-02', 'SEM-F24', 'C',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-052', 'STU-007', 'MATH301', 'INS-03', 'SEM-F24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-053', 'STU-008', 'MATH250', 'INS-04', 'SEM-F24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-054', 'STU-009', 'PHYS201', 'INS-05', 'SEM-F24', 'C',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-055', 'STU-011', 'CS201',   'INS-01', 'SEM-F24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-056', 'STU-011', 'MATH201', 'INS-03', 'SEM-F24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-057', 'STU-012', 'MATH250', 'INS-04', 'SEM-F24', 'B-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-058', 'STU-014', 'PHYS101', 'INS-05', 'SEM-F24', 'I',  'Incomplete','2025-01-01T00:00:00'),
    ('ENR-059', 'STU-015', 'MATH201', 'INS-03', 'SEM-F24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-060', 'STU-015', 'MATH250', 'INS-04', 'SEM-F24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-061', 'STU-016', 'CS350',   'INS-01', 'SEM-F24', 'A-', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-062', 'STU-017', 'ENG201',  'INS-06', 'SEM-F24', 'A',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-063', 'STU-017', 'CS101',   'INS-02', 'SEM-F24', 'B',  'Completed', '2025-01-01T00:00:00'),
    ('ENR-064', 'STU-018', 'PHYS101', 'INS-05', 'SEM-F24', 'B+', 'Completed', '2025-01-01T00:00:00'),
    ('ENR-065', 'STU-018', 'MATH101', 'INS-04', 'SEM-F24', 'B',  'Completed', '2025-01-01T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_enrollments;


-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.student_gpa (student_id) TRANSFORM keyed_hash PARAMS (salt = 'delta_forge_salt_2024');
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_student (name) TRANSFORM keyed_hash PARAMS (salt = 'delta_forge_salt_2024');
