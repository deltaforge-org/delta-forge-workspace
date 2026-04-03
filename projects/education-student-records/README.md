# Education Student Records Pipeline

## Scenario

A university registrar manages student enrollment records across 12 courses, 6 instructors, and 3 semesters. The pipeline calculates cumulative and semester GPAs, identifies dean's list qualifiers and academic probation students, validates prerequisite chains, and produces department-level performance analytics.

Key features include GPA constraints (0.0-4.0 range enforcement), MERGE upserts for resolving incomplete grades, time travel for semester snapshot comparisons, and pseudonymisation of student identity data.

## Star Schema

```
+------------------+         +------------------+
| dim_student      |         | dim_course       |
|------------------|         |------------------|
| student_key (PK) |         | course_key (PK)  |
| student_id       |         | course_code      |
| name             |         | course_name      |
| major            |    +----+ department       |
| minor            |    |    | level            |
| enrollment_year  |    |    | credits          |
| expected_grad    |    |    | prerequisite     |
| status           |    |    +------------------+
| cumulative_gpa   |    |
| total_credits    |    |
+--------+---------+    |
         |              |
    +----+--------+-----+
    |   fact_     |
    | enrollments |
    +---+---------+----+  +------------------+
    | enrollment_key   |  | dim_instructor   |
    | student_key      |  |------------------|
    | course_key       |  | instructor_key   |
    | instructor_key+--+--+ name             |
    | semester_key     |  | department       |
    | grade            |  | rank             |
    | grade_points     |  | tenure_flag      |
    | credits          |  | avg_rating       |
    | passed_flag      |  +------------------+
    | gpa_impact       |
    +--------+---------+  +------------------+
             |            | dim_semester     |
             +------------+------------------|
                          | semester_key(PK) |
                          | semester_name    |
                          | year             |
                          | term             |
                          | start_date       |
                          | end_date         |
                          +------------------+

+----------------------------+
|  kpi_academic_performance  |
|----------------------------|
| department                 |
| semester                   |
| avg_gpa                    |
| pass_rate                  |
| enrollment_count           |
| deans_list_count           |
| probation_count            |
| retention_rate             |
| avg_class_size             |
+----------------------------+
```

## Features Demonstrated

| Feature | Description |
|---|---|
| **Constraints** | GPA CHECK between 0.0 and 4.0, credits CHECK >= 0 |
| **MERGE Upsert** | Update incomplete/withdrawn grades when final grades arrive |
| **Time Travel** | Semester snapshots enable historical GPA comparisons |
| **Pseudonymisation (keyed_hash)** | Student name and ID hashed with SHA256 |
| **GPA Calculation** | Weighted quality points: grade_points * credits / total_credits |
| **Dean's List** | Automatic flagging for cumulative GPA >= 3.5 |
| **Academic Probation** | Flagging for GPA < 2.0 |
| **PERCENT_RANK** | GPA percentile distribution with Latin honors classification |
| **Prerequisite Validation** | Verification of course prerequisite chains |
| **Instructor Effectiveness** | Comparison of average grade given vs instructor rating |

## Data Profile

- **18 students**: Across Computer Science, Mathematics, Physics, English majors
- **12 courses**: 100-300 level with prerequisite chains (CS101 -> CS201 -> CS301)
- **6 instructors**: Professor through Assistant Professor, with tenure status and ratings
- **3 semesters**: Fall 2023, Spring 2024, Fall 2024
- **65 enrollments**: Including grades A through F, withdrawals (W), incompletes (I)

## Verification Checklist

- [ ] Academic performance KPIs generated for all department/semester combinations
- [ ] Star schema joins produce complete enrollment records with all dimensions
- [ ] GPA distribution shows valid percentile rankings with PERCENT_RANK
- [ ] All GPA values within 0.0-4.0 constraint range
- [ ] Department performance comparison with pass rates and average GPAs
- [ ] Instructor effectiveness analysis correlates grades with ratings
- [ ] Semester-over-semester GPA trends use LAG for period comparison
- [ ] Student retention analysis across semesters
- [ ] Prerequisite chain validation identifies met/missing prerequisites
- [ ] Course difficulty ranking by average grade and DFW rate
