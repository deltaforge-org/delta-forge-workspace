-- =============================================================================
-- Nonprofit Donations Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE npo_weekly_monday_schedule
    CRON '0 10 * * 1'
    TIMEZONE 'America/New_York'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE nonprofit_donations_pipeline
    DESCRIPTION 'Nonprofit donations pipeline with CDF tier tracking, donor retention analysis, and campaign effectiveness'
    SCHEDULE 'npo_weekly_monday_schedule'
    TAGS 'nonprofit,donations,fundraising,medallion'
    SLA 30
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Calculate donor tiers and track changes via CDF
-- =============================================================================
-- Tier logic: Bronze <$500, Silver $500-$2K, Gold $2K-$10K, Platinum >$10K
-- CDF enabled on donors_tiered to capture tier promotions/demotions.

MERGE INTO {{zone_prefix}}.silver.donors_tiered AS tgt
USING (
    WITH donor_metrics AS (
        SELECT
            d.donor_id,
            d.name,
            d.email,
            d.donor_type,
            d.acquisition_source,
            d.first_donation_date,
            CAST(COALESCE(SUM(dn.amount), 0) AS DECIMAL(14,2)) AS lifetime_amount,
            COUNT(dn.donation_id) AS donation_count,
            MAX(dn.donation_date) AS last_donation_date
        FROM {{zone_prefix}}.bronze.raw_donors d
        LEFT JOIN {{zone_prefix}}.bronze.raw_donations dn ON d.donor_id = dn.donor_id
        GROUP BY d.donor_id, d.name, d.email, d.donor_type, d.acquisition_source, d.first_donation_date
    ),
    tiered AS (
        SELECT
            dm.*,
            CASE
                WHEN dm.lifetime_amount >= 10000 THEN 'platinum'
                WHEN dm.lifetime_amount >= 2000 THEN 'gold'
                WHEN dm.lifetime_amount >= 500 THEN 'silver'
                ELSE 'bronze'
            END AS current_tier,
            -- Retained = donated in both current year and prior year
            CASE WHEN EXISTS (
                SELECT 1 FROM {{zone_prefix}}.bronze.raw_donations r
                WHERE r.donor_id = dm.donor_id AND EXTRACT(YEAR FROM r.donation_date) = 2024
            ) AND EXISTS (
                SELECT 1 FROM {{zone_prefix}}.bronze.raw_donations r
                WHERE r.donor_id = dm.donor_id AND EXTRACT(YEAR FROM r.donation_date) = 2023
            ) THEN true ELSE false END AS is_retained,
            -- Lapsed = donated before but not in last 12 months
            CASE WHEN dm.last_donation_date < CAST('2023-07-01' AS DATE) AND dm.donation_count > 0
                THEN true ELSE false END AS is_lapsed
        FROM donor_metrics dm
    )
    SELECT * FROM tiered
) AS src
ON tgt.donor_id = src.donor_id
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_amount    = src.lifetime_amount,
    tgt.previous_tier      = tgt.current_tier,
    tgt.current_tier       = src.current_tier,
    tgt.tier_since_date    = CASE WHEN tgt.current_tier != src.current_tier THEN CURRENT_DATE ELSE tgt.tier_since_date END,
    tgt.donation_count     = src.donation_count,
    tgt.last_donation_date = src.last_donation_date,
    tgt.is_retained        = src.is_retained,
    tgt.is_lapsed          = src.is_lapsed,
    tgt.enriched_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    donor_id, name, email, donor_type, acquisition_source, first_donation_date,
    lifetime_amount, current_tier, tier_since_date, previous_tier,
    donation_count, last_donation_date, is_retained, is_lapsed, enriched_at
) VALUES (
    src.donor_id, src.name, src.email, src.donor_type, src.acquisition_source,
    src.first_donation_date, src.lifetime_amount, src.current_tier, CURRENT_DATE,
    NULL, src.donation_count, src.last_donation_date, src.is_retained, src.is_lapsed,
    CURRENT_TIMESTAMP
);

-- =============================================================================
-- STEP 2: SILVER - Enrich donations with donor tier and major gift flag
-- =============================================================================

MERGE INTO {{zone_prefix}}.silver.donations_enriched AS tgt
USING (
    SELECT
        d.donation_id,
        d.donor_id,
        d.campaign_id,
        d.fund_id,
        d.donation_date,
        d.amount,
        d.payment_method,
        d.is_recurring,
        d.tax_deductible_flag,
        d.acknowledgment_sent,
        dt.current_tier AS donor_tier,
        CASE WHEN d.amount >= 10000 THEN true ELSE false END AS is_major_gift,
        d.ingested_at
    FROM {{zone_prefix}}.bronze.raw_donations d
    LEFT JOIN {{zone_prefix}}.silver.donors_tiered dt ON d.donor_id = dt.donor_id
) AS src
ON tgt.donation_id = src.donation_id
WHEN MATCHED THEN UPDATE SET
    tgt.donor_tier   = src.donor_tier,
    tgt.is_major_gift = src.is_major_gift,
    tgt.enriched_at  = src.ingested_at
WHEN NOT MATCHED THEN INSERT (
    donation_id, donor_id, campaign_id, fund_id, donation_date, amount,
    payment_method, is_recurring, tax_deductible_flag, acknowledgment_sent,
    donor_tier, is_major_gift, enriched_at
) VALUES (
    src.donation_id, src.donor_id, src.campaign_id, src.fund_id, src.donation_date,
    src.amount, src.payment_method, src.is_recurring, src.tax_deductible_flag,
    src.acknowledgment_sent, src.donor_tier, src.is_major_gift, src.ingested_at
);

-- =============================================================================
-- STEP 3: GOLD - Populate dim_donor
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_donor AS tgt
USING (
    SELECT
        donor_id AS donor_key, donor_id, name, email, donor_type,
        acquisition_source, first_donation_date, lifetime_amount,
        current_tier, tier_since_date
    FROM {{zone_prefix}}.silver.donors_tiered
) AS src
ON tgt.donor_key = src.donor_key
WHEN MATCHED THEN UPDATE SET
    tgt.lifetime_amount = src.lifetime_amount,
    tgt.current_tier    = src.current_tier,
    tgt.tier_since_date = src.tier_since_date
WHEN NOT MATCHED THEN INSERT (
    donor_key, donor_id, name, email, donor_type, acquisition_source,
    first_donation_date, lifetime_amount, current_tier, tier_since_date
) VALUES (
    src.donor_key, src.donor_id, src.name, src.email, src.donor_type,
    src.acquisition_source, src.first_donation_date, src.lifetime_amount,
    src.current_tier, src.tier_since_date
);

-- =============================================================================
-- STEP 4: GOLD - Populate dim_campaign
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_campaign AS tgt
USING (
    SELECT campaign_id AS campaign_key, campaign_id, campaign_name, campaign_type,
           start_date, end_date, goal_amount, channel
    FROM {{zone_prefix}}.bronze.raw_campaigns
) AS src
ON tgt.campaign_key = src.campaign_key
WHEN MATCHED THEN UPDATE SET
    tgt.campaign_name = src.campaign_name,
    tgt.goal_amount   = src.goal_amount
WHEN NOT MATCHED THEN INSERT (
    campaign_key, campaign_id, campaign_name, campaign_type, start_date, end_date, goal_amount, channel
) VALUES (
    src.campaign_key, src.campaign_id, src.campaign_name, src.campaign_type,
    src.start_date, src.end_date, src.goal_amount, src.channel
);

-- =============================================================================
-- STEP 5: GOLD - Populate dim_fund
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_fund AS tgt
USING (
    SELECT fund_id AS fund_key, fund_name, fund_type, restricted_flag, purpose
    FROM {{zone_prefix}}.bronze.raw_funds
) AS src
ON tgt.fund_key = src.fund_key
WHEN MATCHED THEN UPDATE SET
    tgt.fund_name       = src.fund_name,
    tgt.fund_type       = src.fund_type,
    tgt.restricted_flag = src.restricted_flag,
    tgt.purpose         = src.purpose
WHEN NOT MATCHED THEN INSERT (fund_key, fund_name, fund_type, restricted_flag, purpose)
VALUES (src.fund_key, src.fund_name, src.fund_type, src.restricted_flag, src.purpose);

-- =============================================================================
-- STEP 6: GOLD - Populate fact_donations
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_donations AS tgt
USING (
    SELECT
        donation_id AS donation_key,
        donor_id AS donor_key,
        campaign_id AS campaign_key,
        fund_id AS fund_key,
        donation_date,
        amount,
        payment_method,
        is_recurring,
        tax_deductible_flag,
        acknowledgment_sent
    FROM {{zone_prefix}}.silver.donations_enriched
) AS src
ON tgt.donation_key = src.donation_key
WHEN MATCHED THEN UPDATE SET
    tgt.amount = src.amount
WHEN NOT MATCHED THEN INSERT (
    donation_key, donor_key, campaign_key, fund_key, donation_date,
    amount, payment_method, is_recurring, tax_deductible_flag, acknowledgment_sent
) VALUES (
    src.donation_key, src.donor_key, src.campaign_key, src.fund_key,
    src.donation_date, src.amount, src.payment_method, src.is_recurring,
    src.tax_deductible_flag, src.acknowledgment_sent
);

-- =============================================================================
-- STEP 7: GOLD - KPI Fundraising with campaign effectiveness and retention
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_fundraising AS tgt
USING (
    WITH campaign_fund_period AS (
        SELECT
            f.campaign_key AS campaign_id,
            df.fund_name,
            CAST(EXTRACT(YEAR FROM f.donation_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM f.donation_date) AS STRING), 2, '0') AS period,
            CAST(SUM(f.amount) AS DECIMAL(14,2)) AS total_donations,
            COUNT(DISTINCT f.donor_key) AS unique_donors,
            CAST(AVG(f.amount) AS DECIMAL(10,2)) AS avg_donation,
            CAST(COUNT(*) FILTER (WHERE f.is_recurring = true) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS recurring_pct,
            -- Retention: donors who also donated in previous quarter
            CAST(
                COUNT(DISTINCT f.donor_key) FILTER (WHERE EXISTS (
                    SELECT 1 FROM {{zone_prefix}}.gold.fact_donations prev
                    WHERE prev.donor_key = f.donor_key
                      AND prev.donation_date < f.donation_date
                      AND prev.donation_date >= f.donation_date - INTERVAL '90 days'
                )) * 100.0 / NULLIF(COUNT(DISTINCT f.donor_key), 0)
            AS DECIMAL(5,2)) AS donor_retention_rate,
            dc.goal_amount,
            c.cost
        FROM {{zone_prefix}}.gold.fact_donations f
        JOIN {{zone_prefix}}.gold.dim_campaign dc ON f.campaign_key = dc.campaign_key
        JOIN {{zone_prefix}}.gold.dim_fund df ON f.fund_key = df.fund_key
        JOIN {{zone_prefix}}.bronze.raw_campaigns c ON f.campaign_key = c.campaign_id
        GROUP BY f.campaign_key, df.fund_name, dc.goal_amount, c.cost,
            CAST(EXTRACT(YEAR FROM f.donation_date) AS STRING) || '-' ||
                LPAD(CAST(EXTRACT(MONTH FROM f.donation_date) AS STRING), 2, '0')
    )
    SELECT
        campaign_id,
        fund_name,
        period,
        total_donations,
        unique_donors,
        avg_donation,
        recurring_pct,
        donor_retention_rate,
        CAST(total_donations * 100.0 / NULLIF(goal_amount, 0) AS DECIMAL(7,2)) AS goal_attainment_pct,
        CAST(cost / NULLIF(total_donations, 0) AS DECIMAL(8,4)) AS cost_per_dollar_raised,
        CAST((SELECT COUNT(*) FROM {{zone_prefix}}.gold.fact_donations mg
              WHERE mg.campaign_key = campaign_id AND mg.amount >= 10000
              AND CAST(EXTRACT(YEAR FROM mg.donation_date) AS STRING) || '-' ||
                  LPAD(CAST(EXTRACT(MONTH FROM mg.donation_date) AS STRING), 2, '0') = period) AS INT) AS major_gift_count
    FROM campaign_fund_period
) AS src
ON tgt.campaign_id = src.campaign_id AND tgt.fund_name = src.fund_name AND tgt.period = src.period
WHEN MATCHED THEN UPDATE SET
    tgt.total_donations       = src.total_donations,
    tgt.unique_donors         = src.unique_donors,
    tgt.avg_donation          = src.avg_donation,
    tgt.recurring_pct         = src.recurring_pct,
    tgt.donor_retention_rate  = src.donor_retention_rate,
    tgt.goal_attainment_pct   = src.goal_attainment_pct,
    tgt.cost_per_dollar_raised = src.cost_per_dollar_raised,
    tgt.major_gift_count      = src.major_gift_count
WHEN NOT MATCHED THEN INSERT (
    campaign_id, fund_name, period, total_donations, unique_donors, avg_donation,
    recurring_pct, donor_retention_rate, goal_attainment_pct, cost_per_dollar_raised,
    major_gift_count
) VALUES (
    src.campaign_id, src.fund_name, src.period, src.total_donations, src.unique_donors,
    src.avg_donation, src.recurring_pct, src.donor_retention_rate, src.goal_attainment_pct,
    src.cost_per_dollar_raised, src.major_gift_count
);

-- =============================================================================
-- STEP 8: OPTIMIZE and VACUUM
-- =============================================================================

OPTIMIZE {{zone_prefix}}.gold.fact_donations;
VACUUM {{zone_prefix}}.gold.fact_donations RETAIN 168 HOURS;
