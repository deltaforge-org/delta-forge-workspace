-- =============================================================================
-- Food Safety Inspections Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE food_weekly_schedule
    CRON '0 8 * * 1'
    TIMEZONE 'US/Eastern'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE food_safety_inspections_pipeline
    DESCRIPTION 'Food safety compliance analytics with scoring, grading, repeat offenders, and inspector calibration'
    SCHEDULE 'food_weekly_schedule'
    TAGS 'food,safety,inspections,compliance,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Score inspections and assign grades
-- =============================================================================
-- Score = 100 - SUM(violation points_deducted)
-- Grade: A (90-100), B (80-89), C (70-79), F (<70)

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

-- =============================================================================
-- STEP 2: GOLD - Dimension: dim_establishment (with previous_score from LAG)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_establishment AS tgt
USING (
    WITH latest_scores AS (
        SELECT
            establishment_id,
            score,
            LAG(score) OVER (PARTITION BY establishment_id ORDER BY inspection_date) AS previous_score,
            ROW_NUMBER() OVER (PARTITION BY establishment_id ORDER BY inspection_date DESC) AS rn
        FROM {{zone_prefix}}.silver.inspections_scored
    )
    SELECT
        e.establishment_id AS establishment_key,
        e.establishment_id,
        e.name,
        e.cuisine_type,
        e.seating_capacity,
        e.license_date,
        e.owner_name,
        e.chain_flag,
        ls.previous_score,
        e.risk_category
    FROM {{zone_prefix}}.bronze.raw_establishments e
    LEFT JOIN latest_scores ls ON e.establishment_id = ls.establishment_id AND ls.rn = 1
) AS src
ON tgt.establishment_key = src.establishment_key
WHEN MATCHED THEN UPDATE SET
    tgt.previous_score = src.previous_score,
    tgt.risk_category  = src.risk_category
WHEN NOT MATCHED THEN INSERT (
    establishment_key, establishment_id, name, cuisine_type, seating_capacity,
    license_date, owner_name, chain_flag, previous_score, risk_category
) VALUES (
    src.establishment_key, src.establishment_id, src.name, src.cuisine_type,
    src.seating_capacity, src.license_date, src.owner_name, src.chain_flag,
    src.previous_score, src.risk_category
);

-- =============================================================================
-- STEP 3: GOLD - Dimension: dim_inspector (with avg_score_given and count)
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_inspector AS tgt
USING (
    SELECT
        insp.inspector_id AS inspector_key,
        insp.inspector_id,
        insp.name,
        insp.certification_level,
        insp.years_experience,
        CAST(AVG(sc.score) AS DECIMAL(5,2)) AS avg_score_given,
        COUNT(sc.inspection_id) AS inspection_count
    FROM {{zone_prefix}}.bronze.raw_inspectors insp
    LEFT JOIN {{zone_prefix}}.silver.inspections_scored sc ON insp.inspector_id = sc.inspector_id
    GROUP BY insp.inspector_id, insp.name, insp.certification_level, insp.years_experience
) AS src
ON tgt.inspector_key = src.inspector_key
WHEN MATCHED THEN UPDATE SET
    tgt.avg_score_given  = src.avg_score_given,
    tgt.inspection_count = src.inspection_count
WHEN NOT MATCHED THEN INSERT (
    inspector_key, inspector_id, name, certification_level, years_experience,
    avg_score_given, inspection_count
) VALUES (
    src.inspector_key, src.inspector_id, src.name, src.certification_level,
    src.years_experience, src.avg_score_given, src.inspection_count
);

-- =============================================================================
-- STEP 4: GOLD - Dimension: dim_district
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_district AS tgt
USING (
    SELECT
        e.district AS district_key,
        e.district AS district_name,
        e.city,
        e.state,
        CASE e.district
            WHEN 'Downtown' THEN 125000
            WHEN 'Midtown'  THEN 95000
            WHEN 'Uptown'   THEN 80000
            WHEN 'Harbor'   THEN 65000
        END AS population,
        COUNT(DISTINCT e.establishment_id) AS establishment_count,
        CAST(AVG(sc.score) AS DECIMAL(5,2)) AS avg_score
    FROM {{zone_prefix}}.bronze.raw_establishments e
    LEFT JOIN {{zone_prefix}}.silver.inspections_scored sc ON e.establishment_id = sc.establishment_id
    GROUP BY e.district, e.city, e.state
) AS src
ON tgt.district_key = src.district_key
WHEN MATCHED THEN UPDATE SET
    tgt.establishment_count = src.establishment_count,
    tgt.avg_score           = src.avg_score
WHEN NOT MATCHED THEN INSERT (
    district_key, district_name, city, state, population, establishment_count, avg_score
) VALUES (
    src.district_key, src.district_name, src.city, src.state, src.population,
    src.establishment_count, src.avg_score
);

-- =============================================================================
-- STEP 5: GOLD - Dimension: dim_violation
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_violation AS tgt
USING (
    SELECT
        violation_code AS violation_key,
        violation_code,
        description,
        category,
        severity,
        points_deducted,
        corrective_action
    FROM {{zone_prefix}}.bronze.raw_violations
) AS src
ON tgt.violation_key = src.violation_key
WHEN NOT MATCHED THEN INSERT (
    violation_key, violation_code, description, category, severity,
    points_deducted, corrective_action
) VALUES (
    src.violation_key, src.violation_code, src.description, src.category,
    src.severity, src.points_deducted, src.corrective_action
);

-- =============================================================================
-- STEP 6: GOLD - Fact: fact_inspections
-- =============================================================================

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
-- STEP 7: GOLD - KPI: kpi_compliance
-- =============================================================================
-- District compliance dashboard with repeat offender identification,
-- inspector consistency scoring, and improvement trends.

MERGE INTO {{zone_prefix}}.gold.kpi_compliance AS tgt
USING (
    WITH quarterly AS (
        SELECT
            fi.district_key AS district,
            de.cuisine_type,
            CASE
                WHEN MONTH(fi.inspection_date) BETWEEN 1 AND 3 THEN 'Q1'
                WHEN MONTH(fi.inspection_date) BETWEEN 4 AND 6 THEN 'Q2'
                WHEN MONTH(fi.inspection_date) BETWEEN 7 AND 9 THEN 'Q3'
                ELSE 'Q4'
            END || '-' || CAST(YEAR(fi.inspection_date) AS STRING) AS quarter,
            COUNT(*) AS total_inspections,
            CAST(AVG(fi.score) AS DECIMAL(5,2)) AS avg_score,
            CAST(SUM(CASE WHEN fi.grade IN ('A','B') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pass_rate,
            CAST(SUM(fi.critical_violations) * 1.0 / COUNT(*) AS DECIMAL(5,4)) AS critical_violation_rate,
            CAST(SUM(CASE WHEN fi.closure_ordered = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(5,4)) AS closure_rate
        FROM {{zone_prefix}}.gold.fact_inspections fi
        JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
        GROUP BY fi.district_key, de.cuisine_type,
            CASE
                WHEN MONTH(fi.inspection_date) BETWEEN 1 AND 3 THEN 'Q1'
                WHEN MONTH(fi.inspection_date) BETWEEN 4 AND 6 THEN 'Q2'
                WHEN MONTH(fi.inspection_date) BETWEEN 7 AND 9 THEN 'Q3'
                ELSE 'Q4'
            END || '-' || CAST(YEAR(fi.inspection_date) AS STRING)
    ),
    repeat_offenders AS (
        SELECT
            fi.district_key,
            de.cuisine_type,
            COUNT(DISTINCT fi.establishment_key) AS repeat_offender_count
        FROM {{zone_prefix}}.gold.fact_inspections fi
        JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
        WHERE fi.critical_violations >= 2
        GROUP BY fi.district_key, de.cuisine_type
    ),
    inspector_consistency AS (
        SELECT
            fi.district_key,
            CAST(100.0 - STDDEV(fi.score) AS DECIMAL(5,2)) AS inspector_consistency_score
        FROM {{zone_prefix}}.gold.fact_inspections fi
        GROUP BY fi.district_key
    )
    SELECT
        q.district,
        q.cuisine_type,
        q.quarter,
        q.total_inspections,
        q.avg_score,
        q.pass_rate,
        q.critical_violation_rate,
        q.closure_rate,
        COALESCE(ro.repeat_offender_count, 0) AS repeat_offender_count,
        COALESCE(ic.inspector_consistency_score, 0.00) AS inspector_consistency_score,
        CASE
            WHEN q.avg_score >= 90 THEN 'Excellent'
            WHEN q.avg_score >= 80 THEN 'Improving'
            WHEN q.avg_score >= 70 THEN 'Stable'
            ELSE 'Declining'
        END AS improvement_trend
    FROM quarterly q
    LEFT JOIN repeat_offenders ro ON q.district = ro.district_key AND q.cuisine_type = ro.cuisine_type
    LEFT JOIN inspector_consistency ic ON q.district = ic.district_key
) AS src
ON tgt.district = src.district AND tgt.cuisine_type = src.cuisine_type AND tgt.quarter = src.quarter
WHEN MATCHED THEN UPDATE SET
    tgt.total_inspections           = src.total_inspections,
    tgt.avg_score                   = src.avg_score,
    tgt.pass_rate                   = src.pass_rate,
    tgt.critical_violation_rate     = src.critical_violation_rate,
    tgt.closure_rate                = src.closure_rate,
    tgt.repeat_offender_count       = src.repeat_offender_count,
    tgt.inspector_consistency_score = src.inspector_consistency_score,
    tgt.improvement_trend           = src.improvement_trend
WHEN NOT MATCHED THEN INSERT (
    district, cuisine_type, quarter, total_inspections, avg_score, pass_rate,
    critical_violation_rate, closure_rate, repeat_offender_count,
    inspector_consistency_score, improvement_trend
) VALUES (
    src.district, src.cuisine_type, src.quarter, src.total_inspections, src.avg_score,
    src.pass_rate, src.critical_violation_rate, src.closure_rate,
    src.repeat_offender_count, src.inspector_consistency_score, src.improvement_trend
);

-- =============================================================================
-- OPTIMIZE
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.inspections_scored;
OPTIMIZE {{zone_prefix}}.gold.fact_inspections;
OPTIMIZE {{zone_prefix}}.gold.kpi_compliance;
