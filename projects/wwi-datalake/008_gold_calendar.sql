-- ============================================================================
-- WWI Data Lake - Gold Calendar Dimension
-- ============================================================================
-- Generates a date spine from 2013-01-01 to 2025-12-31 covering the full
-- WideWorldImporters date range plus headroom.
-- Fiscal year starts July 1 (Microsoft fiscal calendar).
-- Only needs to run once; re-running is safe (MERGE on date_key).
-- ============================================================================

PIPELINE wwi_lake.gold_calendar
    DESCRIPTION 'Generate or refresh the calendar dimension'
    TAGS 'wwi', 'gold', 'dimension', 'calendar'
    LIFECYCLE PRODUCTION;

MERGE INTO wwi_lake.gold.dim_date AS tgt
USING (
    WITH date_spine AS (
        SELECT CAST('2013-01-01' AS DATE) + CAST(seq AS INT) AS full_date
        FROM generate_series(0, 4748) AS t(seq)
    )
    SELECT
        CAST(DATE_FORMAT(full_date, '%Y%m%d') AS INT) AS date_key,
        full_date,
        DAYOFWEEK(full_date)             AS day_of_week,
        DATE_FORMAT(full_date, '%A')     AS day_name,
        DAY(full_date)                   AS day_of_month,
        DAYOFYEAR(full_date)             AS day_of_year,
        WEEKOFYEAR(full_date)            AS week_of_year,
        MONTH(full_date)                 AS month_number,
        DATE_FORMAT(full_date, '%B')     AS month_name,
        QUARTER(full_date)               AS quarter,
        YEAR(full_date)                  AS year,
        DAYOFWEEK(full_date) IN (1, 7)   AS is_weekend,

        -- Microsoft fiscal year starts July 1
        CASE
            WHEN MONTH(full_date) >= 7 THEN YEAR(full_date) + 1
            ELSE YEAR(full_date)
        END                              AS fiscal_year,
        CASE
            WHEN MONTH(full_date) >= 7 THEN QUARTER(full_date) - 2
            ELSE QUARTER(full_date) + 2
        END                              AS fiscal_quarter
    FROM date_spine
) AS src ON tgt.date_key = src.date_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
