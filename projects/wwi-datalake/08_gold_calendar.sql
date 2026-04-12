-- ============================================================================
-- Gold Calendar Dimension
-- ============================================================================
-- Date spine 2013-01-01 to 2025-12-31. Fiscal year starts July 1.
-- Idempotent: CREATE IF NOT EXISTS + MERGE on date_key.
-- ============================================================================

PIPELINE wwi_lake.gold_calendar
    DESCRIPTION 'WWI gold - calendar date dimension'
    TAGS 'wwi', 'medallion', 'gold', 'dimension'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.dim_date (
    date_key INT NOT NULL, full_date DATE NOT NULL, day_of_week INT, day_name VARCHAR,
    day_of_month INT, day_of_year INT, week_of_year INT, month_number INT,
    month_name VARCHAR, quarter INT, year INT, is_weekend BOOLEAN,
    fiscal_year INT, fiscal_quarter INT
) LOCATION 'wwi/gold/dim_date';

MERGE INTO wwi_lake.gold.dim_date AS tgt
USING (
    WITH date_spine AS (
        SELECT CAST('2013-01-01' AS DATE) + CAST(seq AS INT) AS full_date
        FROM generate_series(0, 4748) AS t(seq)
    )
    SELECT
        CAST(DATE_FORMAT(full_date, '%Y%m%d') AS INT) AS date_key,
        full_date,
        DAYOFWEEK(full_date) AS day_of_week,
        DATE_FORMAT(full_date, '%A') AS day_name,
        DAY(full_date) AS day_of_month,
        DAYOFYEAR(full_date) AS day_of_year,
        WEEKOFYEAR(full_date) AS week_of_year,
        MONTH(full_date) AS month_number,
        DATE_FORMAT(full_date, '%B') AS month_name,
        QUARTER(full_date) AS quarter,
        YEAR(full_date) AS year,
        DAYOFWEEK(full_date) IN (1, 7) AS is_weekend,
        CASE WHEN MONTH(full_date) >= 7 THEN YEAR(full_date) + 1 ELSE YEAR(full_date) END AS fiscal_year,
        CASE WHEN MONTH(full_date) >= 7 THEN QUARTER(full_date) - 2 ELSE QUARTER(full_date) + 2 END AS fiscal_quarter
    FROM date_spine
) AS src ON tgt.date_key = src.date_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
