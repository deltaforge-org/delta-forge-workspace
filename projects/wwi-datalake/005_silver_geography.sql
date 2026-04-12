-- ============================================================================
-- WWI Data Lake - Silver Geography & Reference Views
-- ============================================================================
-- Flattens the city -> state -> country hierarchy into a single row.
-- Resolves employee subset from people.
-- These views are consumed by gold dimensions.
-- CREATE OR REPLACE VIEW is idempotent - safe to run on every schedule.
-- ============================================================================

PIPELINE wwi_lake.silver_geography
    DESCRIPTION 'Create/refresh geography and employee silver views'
    SCHEDULE '0 4 * * *'
    TIMEZONE 'UTC'
    TAGS 'wwi', 'silver', 'views', 'geography'
    SLA 0.5
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- Full geographic hierarchy in one row

CREATE OR REPLACE VIEW wwi_lake.silver.v_city AS
SELECT
    c.city_id,
    c.city_name,
    sp.state_province_id,
    sp.state_province_name,
    sp.state_province_code,
    sp.sales_territory,
    co.country_id,
    co.country_name,
    co.continent,
    co.region,
    co.subregion,
    c.latest_recorded_population  AS city_population,
    sp.latest_recorded_population AS state_population,
    co.latest_recorded_population AS country_population
FROM wwi_lake.bronze.cities c
INNER JOIN wwi_lake.bronze.state_provinces sp
    ON c.state_province_id = sp.state_province_id
INNER JOIN wwi_lake.bronze.countries co
    ON sp.country_id = co.country_id;

-- Employees only (filtered from people)

CREATE OR REPLACE VIEW wwi_lake.silver.v_employee AS
SELECT
    person_id   AS employee_id,
    full_name,
    preferred_name,
    is_salesperson,
    phone_number,
    email_address
FROM wwi_lake.bronze.people
WHERE is_employee = true;
