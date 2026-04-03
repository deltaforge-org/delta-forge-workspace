-- =============================================================================
-- Food Safety Inspections Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_inspections row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_inspections_count
FROM {{zone_prefix}}.gold.fact_inspections;

-- -----------------------------------------------------------------------------
-- 2. Verify all 15 establishments loaded into dim_establishment
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_inspections_count >= 65
SELECT COUNT(*) AS establishment_count
FROM {{zone_prefix}}.gold.dim_establishment;

-- -----------------------------------------------------------------------------
-- 3. Verify all 5 inspectors loaded into dim_inspector
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS inspector_count
FROM {{zone_prefix}}.gold.dim_inspector;

-- -----------------------------------------------------------------------------
-- 4. District compliance overview
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    dd.district_name,
    dd.establishment_count,
    dd.avg_score,
    COUNT(*) AS inspections,
    CAST(AVG(fi.score) AS DECIMAL(5,2)) AS avg_inspection_score,
    SUM(CASE WHEN fi.grade = 'A' THEN 1 ELSE 0 END) AS grade_a_count,
    SUM(CASE WHEN fi.grade = 'F' THEN 1 ELSE 0 END) AS grade_f_count,
    SUM(CASE WHEN fi.closure_ordered = true THEN 1 ELSE 0 END) AS closures
FROM {{zone_prefix}}.gold.fact_inspections fi
JOIN {{zone_prefix}}.gold.dim_district dd ON fi.district_key = dd.district_key
GROUP BY dd.district_name, dd.establishment_count, dd.avg_score
ORDER BY avg_inspection_score DESC;

-- -----------------------------------------------------------------------------
-- 5. Cuisine-type risk analysis
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    de.cuisine_type,
    de.risk_category,
    COUNT(*) AS inspections,
    CAST(AVG(fi.score) AS DECIMAL(5,2)) AS avg_score,
    SUM(fi.critical_violations) AS total_critical,
    CAST(SUM(fi.critical_violations) * 1.0 / COUNT(*) AS DECIMAL(5,4)) AS critical_rate,
    RANK() OVER (ORDER BY AVG(fi.score) ASC) AS risk_rank
FROM {{zone_prefix}}.gold.fact_inspections fi
JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
GROUP BY de.cuisine_type, de.risk_category
ORDER BY avg_score ASC;

-- -----------------------------------------------------------------------------
-- 6. Inspector calibration (consistency scoring - harsh vs lenient)
-- -----------------------------------------------------------------------------
ASSERT VALUE inspections > 0
SELECT
    di.name,
    di.certification_level,
    di.years_experience,
    di.inspection_count,
    di.avg_score_given,
    CAST(AVG(fi.score) AS DECIMAL(5,2)) AS calculated_avg_score,
    CAST(STDDEV(fi.score) AS DECIMAL(5,2)) AS score_stddev,
    CASE
        WHEN AVG(fi.score) < (SELECT AVG(score) FROM {{zone_prefix}}.gold.fact_inspections) - 5
        THEN 'Strict'
        WHEN AVG(fi.score) > (SELECT AVG(score) FROM {{zone_prefix}}.gold.fact_inspections) + 5
        THEN 'Lenient'
        ELSE 'Consistent'
    END AS calibration_category
FROM {{zone_prefix}}.gold.fact_inspections fi
JOIN {{zone_prefix}}.gold.dim_inspector di ON fi.inspector_key = di.inspector_key
GROUP BY di.name, di.certification_level, di.years_experience, di.inspection_count, di.avg_score_given
ORDER BY calculated_avg_score ASC;

-- -----------------------------------------------------------------------------
-- 7. Establishment improvement tracking using LAG
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    de.name AS establishment,
    de.cuisine_type,
    fi.inspection_date,
    fi.score,
    fi.grade,
    LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) AS prev_score,
    fi.score - LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) AS score_change,
    CASE
        WHEN fi.score - LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) > 10 THEN 'Significant Improvement'
        WHEN fi.score - LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) > 0 THEN 'Slight Improvement'
        WHEN fi.score - LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) = 0 THEN 'No Change'
        WHEN fi.score - LAG(fi.score) OVER (PARTITION BY fi.establishment_key ORDER BY fi.inspection_date) IS NULL THEN 'First Inspection'
        ELSE 'Declined'
    END AS trend
FROM {{zone_prefix}}.gold.fact_inspections fi
JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
ORDER BY de.name, fi.inspection_date;

-- -----------------------------------------------------------------------------
-- 8. Repeat offender identification (2+ critical violations in 6 months)
-- -----------------------------------------------------------------------------
ASSERT VALUE score > 0
SELECT
    de.name AS establishment,
    de.cuisine_type,
    de.district_key,
    de.risk_category,
    COUNT(*) AS inspections_with_critical,
    SUM(fi.critical_violations) AS total_critical_violations,
    MIN(fi.inspection_date) AS first_critical,
    MAX(fi.inspection_date) AS last_critical
FROM {{zone_prefix}}.gold.fact_inspections fi
JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
WHERE fi.critical_violations > 0
GROUP BY de.name, de.cuisine_type, de.district_key, de.risk_category
HAVING SUM(fi.critical_violations) >= 3
ORDER BY total_critical_violations DESC;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT VALUE total_critical_violations >= 3
SELECT COUNT(*) AS orphaned_establishments
FROM {{zone_prefix}}.gold.fact_inspections fi
LEFT JOIN {{zone_prefix}}.gold.dim_establishment de ON fi.establishment_key = de.establishment_key
WHERE de.establishment_key IS NULL;

ASSERT VALUE orphaned_establishments = 0

SELECT COUNT(*) AS orphaned_inspectors
FROM {{zone_prefix}}.gold.fact_inspections fi
LEFT JOIN {{zone_prefix}}.gold.dim_inspector di ON fi.inspector_key = di.inspector_key
WHERE di.inspector_key IS NULL;

-- -----------------------------------------------------------------------------
-- 10. KPI compliance dashboard - verify district compliance and trends
-- -----------------------------------------------------------------------------
ASSERT VALUE orphaned_inspectors = 0
SELECT
    district,
    cuisine_type,
    quarter,
    total_inspections,
    avg_score,
    pass_rate,
    critical_violation_rate,
    closure_rate,
    repeat_offender_count,
    inspector_consistency_score,
    improvement_trend
FROM {{zone_prefix}}.gold.kpi_compliance
ORDER BY district, quarter, cuisine_type;

ASSERT VALUE total_inspections > 0
SELECT 'total_inspections check passed' AS total_inspections_status;

