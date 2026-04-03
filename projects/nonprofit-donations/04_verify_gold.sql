-- =============================================================================
-- Nonprofit Donations Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_donations row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_donations_count
FROM {{zone_prefix}}.gold.fact_donations;

-- -----------------------------------------------------------------------------
-- 2. Verify all 15 donors in dim_donor
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_donations_count >= 60
SELECT COUNT(*) AS donor_count
FROM {{zone_prefix}}.gold.dim_donor;

-- -----------------------------------------------------------------------------
-- 3. Verify all 5 campaigns in dim_campaign
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS campaign_count
FROM {{zone_prefix}}.gold.dim_campaign;

-- -----------------------------------------------------------------------------
-- 4. Donor tier distribution
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    dd.current_tier,
    COUNT(*) AS donor_count,
    CAST(SUM(dd.lifetime_amount) AS DECIMAL(14,2)) AS total_lifetime_giving,
    CAST(AVG(dd.lifetime_amount) AS DECIMAL(14,2)) AS avg_lifetime_giving
FROM {{zone_prefix}}.gold.dim_donor dd
GROUP BY dd.current_tier
ORDER BY total_lifetime_giving DESC;

-- -----------------------------------------------------------------------------
-- 5. Campaign effectiveness with goal attainment
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    dc.campaign_name,
    dc.campaign_type,
    dc.goal_amount,
    CAST(SUM(f.amount) AS DECIMAL(14,2)) AS total_raised,
    COUNT(DISTINCT f.donor_key) AS unique_donors,
    CAST(SUM(f.amount) * 100.0 / NULLIF(dc.goal_amount, 0) AS DECIMAL(7,2)) AS goal_pct,
    COUNT(*) FILTER (WHERE f.amount >= 10000) AS major_gifts,
    CAST(AVG(f.amount) AS DECIMAL(10,2)) AS avg_donation
FROM {{zone_prefix}}.gold.fact_donations f
JOIN {{zone_prefix}}.gold.dim_campaign dc ON f.campaign_key = dc.campaign_key
GROUP BY dc.campaign_name, dc.campaign_type, dc.goal_amount
ORDER BY total_raised DESC;

-- -----------------------------------------------------------------------------
-- 6. Donor retention analysis using LAG (year-over-year)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 5
SELECT
    period_year,
    active_donors,
    LAG(active_donors) OVER (ORDER BY period_year) AS prev_year_donors,
    CASE WHEN LAG(active_donors) OVER (ORDER BY period_year) IS NOT NULL
        THEN CAST(retained_donors * 100.0 / LAG(active_donors) OVER (ORDER BY period_year) AS DECIMAL(5,2))
        ELSE NULL
    END AS retention_rate_pct
FROM (
    SELECT
        EXTRACT(YEAR FROM f.donation_date) AS period_year,
        COUNT(DISTINCT f.donor_key) AS active_donors,
        COUNT(DISTINCT f.donor_key) FILTER (WHERE EXISTS (
            SELECT 1 FROM {{zone_prefix}}.gold.fact_donations prev
            WHERE prev.donor_key = f.donor_key
              AND EXTRACT(YEAR FROM prev.donation_date) = EXTRACT(YEAR FROM f.donation_date) - 1
        )) AS retained_donors
    FROM {{zone_prefix}}.gold.fact_donations f
    GROUP BY EXTRACT(YEAR FROM f.donation_date)
) yearly
ORDER BY period_year;

-- -----------------------------------------------------------------------------
-- 7. Fund allocation summary
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 2
SELECT
    df.fund_name,
    df.fund_type,
    df.restricted_flag,
    CAST(SUM(f.amount) AS DECIMAL(14,2)) AS total_received,
    COUNT(DISTINCT f.donor_key) AS unique_donors,
    COUNT(*) AS donation_count,
    CAST(SUM(f.amount) * 100.0 / NULLIF(SUM(SUM(f.amount)) OVER (), 0) AS DECIMAL(5,2)) AS pct_of_total
FROM {{zone_prefix}}.gold.fact_donations f
JOIN {{zone_prefix}}.gold.dim_fund df ON f.fund_key = df.fund_key
GROUP BY df.fund_name, df.fund_type, df.restricted_flag
ORDER BY total_received DESC;

-- -----------------------------------------------------------------------------
-- 8. Recurring vs one-time revenue trend
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    CAST(EXTRACT(YEAR FROM f.donation_date) AS STRING) || '-' ||
        LPAD(CAST(EXTRACT(MONTH FROM f.donation_date) AS STRING), 2, '0') AS period,
    CAST(SUM(CASE WHEN f.is_recurring THEN f.amount ELSE 0 END) AS DECIMAL(14,2)) AS recurring_revenue,
    CAST(SUM(CASE WHEN NOT f.is_recurring THEN f.amount ELSE 0 END) AS DECIMAL(14,2)) AS onetime_revenue,
    CAST(SUM(f.amount) AS DECIMAL(14,2)) AS total_revenue,
    CAST(SUM(CASE WHEN f.is_recurring THEN f.amount ELSE 0 END) * 100.0 / NULLIF(SUM(f.amount), 0) AS DECIMAL(5,2)) AS recurring_pct
FROM {{zone_prefix}}.gold.fact_donations f
GROUP BY CAST(EXTRACT(YEAR FROM f.donation_date) AS STRING) || '-' ||
    LPAD(CAST(EXTRACT(MONTH FROM f.donation_date) AS STRING), 2, '0')
ORDER BY period;

-- -----------------------------------------------------------------------------
-- 9. Major gift analysis (donations >= $10,000)
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT >= 10
SELECT
    dd.name AS donor_name,
    dd.donor_type,
    dd.current_tier,
    f.donation_date,
    f.amount,
    dc.campaign_name,
    df.fund_name
FROM {{zone_prefix}}.gold.fact_donations f
JOIN {{zone_prefix}}.gold.dim_donor dd ON f.donor_key = dd.donor_key
JOIN {{zone_prefix}}.gold.dim_campaign dc ON f.campaign_key = dc.campaign_key
JOIN {{zone_prefix}}.gold.dim_fund df ON f.fund_key = df.fund_key
WHERE f.amount >= 10000
ORDER BY f.amount DESC;

-- -----------------------------------------------------------------------------
-- 10. KPI fundraising dashboard verification
-- -----------------------------------------------------------------------------
ASSERT VALUE amount >= 10000
SELECT
    k.campaign_id,
    dc.campaign_name,
    k.fund_name,
    k.period,
    k.total_donations,
    k.unique_donors,
    k.recurring_pct,
    k.goal_attainment_pct,
    k.cost_per_dollar_raised,
    k.major_gift_count
FROM {{zone_prefix}}.gold.kpi_fundraising k
JOIN {{zone_prefix}}.gold.dim_campaign dc ON k.campaign_id = dc.campaign_key
ORDER BY k.total_donations DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Verification Summary
-- -----------------------------------------------------------------------------
ASSERT VALUE total_donations > 0
SELECT
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_donations) AS fact_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_donor) AS dim_donor_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_campaign) AS dim_campaign_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.dim_fund) AS dim_fund_rows,
    (SELECT COUNT(*) FROM {{zone_prefix}}.gold.kpi_fundraising) AS kpi_rows;
