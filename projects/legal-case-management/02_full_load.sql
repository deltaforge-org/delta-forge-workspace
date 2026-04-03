-- =============================================================================
-- Legal Case Management Pipeline - Full Load Transformation
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE legal_daily_schedule CRON '0 7 * * *' TIMEZONE 'America/New_York' RETRIES 2 TIMEOUT 3600 MAX_CONCURRENT 1 ACTIVE;

PIPELINE legal_billing_pipeline DESCRIPTION 'Daily legal billing pipeline with case profitability and graph analytics' SCHEDULE 'legal_daily_schedule' TAGS 'legal,billing,graph,utilization' SLA 3600 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP 1: Validate Bronze =====================

SELECT COUNT(*) AS raw_attorney_count FROM {{zone_prefix}}.bronze.raw_attorneys;
ASSERT VALUE raw_attorney_count = 8

SELECT COUNT(*) AS raw_case_count FROM {{zone_prefix}}.bronze.raw_cases;
ASSERT VALUE raw_case_count = 12

SELECT COUNT(*) AS raw_billing_count FROM {{zone_prefix}}.bronze.raw_billings;
-- ===================== STEP 2: Bronze -> Silver Billings Enriched =====================

-- Enrich billings with case type, practice area, and compute complexity score
MERGE INTO {{zone_prefix}}.silver.billings_enriched AS target
USING (
ASSERT VALUE raw_billing_count = 65
    WITH case_complexity AS (
        SELECT
            b.case_id,
            COUNT(DISTINCT b.attorney_id) AS attorney_count,
            SUM(b.hours) AS total_hours,
            COUNT(DISTINCT cc.client_id) AS party_count
        FROM {{zone_prefix}}.bronze.raw_billings b
        LEFT JOIN {{zone_prefix}}.bronze.raw_case_clients cc ON b.case_id = cc.case_id
        GROUP BY b.case_id
    )
    SELECT
        b.billing_id,
        b.case_id,
        b.attorney_id,
        b.client_id,
        c.case_type,
        a.practice_area,
        b.billing_date,
        b.hours,
        b.hourly_rate,
        b.amount,
        b.billable_flag,
        a.partner_flag,
        -- Complexity: log(total_hours) * attorney_count * party_count (normalized to 1-10)
        LEAST(10.0, ROUND(
            LN(GREATEST(1, cx.total_hours)) * cx.attorney_count * cx.party_count / 5.0
        , 2)) AS complexity_score
    FROM {{zone_prefix}}.bronze.raw_billings b
    JOIN {{zone_prefix}}.bronze.raw_cases c ON b.case_id = c.case_id
    JOIN {{zone_prefix}}.bronze.raw_attorneys a ON b.attorney_id = a.attorney_id
    JOIN case_complexity cx ON b.case_id = cx.case_id
) AS source
ON target.billing_id = source.billing_id
WHEN MATCHED THEN UPDATE SET
    case_type        = source.case_type,
    practice_area    = source.practice_area,
    amount           = source.amount,
    complexity_score = source.complexity_score,
    processed_at     = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    billing_id, case_id, attorney_id, client_id, case_type, practice_area,
    billing_date, hours, hourly_rate, amount, billable_flag, partner_flag,
    complexity_score, processed_at
) VALUES (
    source.billing_id, source.case_id, source.attorney_id, source.client_id,
    source.case_type, source.practice_area, source.billing_date, source.hours,
    source.hourly_rate, source.amount, source.billable_flag, source.partner_flag,
    source.complexity_score, CURRENT_TIMESTAMP
);

SELECT COUNT(*) AS silver_billing_count FROM {{zone_prefix}}.silver.billings_enriched;
-- ===================== STEP 3: Silver -> Gold Dimensions =====================

-- dim_case with complexity scores
MERGE INTO {{zone_prefix}}.gold.dim_case AS target
USING (
ASSERT VALUE silver_billing_count = 65
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.case_id) AS case_key,
        c.case_id,
        c.case_number,
        c.case_type,
        c.court,
        c.filing_date,
        c.status,
        COALESCE(MAX(s.complexity_score), 0) AS complexity_score
    FROM {{zone_prefix}}.bronze.raw_cases c
    LEFT JOIN {{zone_prefix}}.silver.billings_enriched s ON c.case_id = s.case_id
    GROUP BY c.case_id, c.case_number, c.case_type, c.court, c.filing_date, c.status
) AS source
ON target.case_id = source.case_id
WHEN MATCHED THEN UPDATE SET
    case_number      = source.case_number,
    case_type        = source.case_type,
    court            = source.court,
    status           = source.status,
    complexity_score = source.complexity_score,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    case_key, case_id, case_number, case_type, court, filing_date, status, complexity_score, loaded_at
) VALUES (
    source.case_key, source.case_id, source.case_number, source.case_type, source.court,
    source.filing_date, source.status, source.complexity_score, CURRENT_TIMESTAMP
);

-- dim_attorney
MERGE INTO {{zone_prefix}}.gold.dim_attorney AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY attorney_id) AS attorney_key,
        attorney_id, name, bar_number, practice_area, partner_flag,
        years_experience, hourly_rate
    FROM {{zone_prefix}}.bronze.raw_attorneys
) AS source
ON target.attorney_id = source.attorney_id
WHEN MATCHED THEN UPDATE SET
    name             = source.name,
    bar_number       = source.bar_number,
    practice_area    = source.practice_area,
    partner_flag     = source.partner_flag,
    years_experience = source.years_experience,
    hourly_rate      = source.hourly_rate,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    attorney_key, attorney_id, name, bar_number, practice_area, partner_flag,
    years_experience, hourly_rate, loaded_at
) VALUES (
    source.attorney_key, source.attorney_id, source.name, source.bar_number,
    source.practice_area, source.partner_flag, source.years_experience,
    source.hourly_rate, CURRENT_TIMESTAMP
);

-- dim_client
MERGE INTO {{zone_prefix}}.gold.dim_client AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY client_id) AS client_key,
        client_id, client_name, client_type, industry, jurisdiction
    FROM {{zone_prefix}}.bronze.raw_clients
) AS source
ON target.client_id = source.client_id
WHEN MATCHED THEN UPDATE SET
    client_name  = source.client_name,
    client_type  = source.client_type,
    industry     = source.industry,
    jurisdiction = source.jurisdiction,
    loaded_at    = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    client_key, client_id, client_name, client_type, industry, jurisdiction, loaded_at
) VALUES (
    source.client_key, source.client_id, source.client_name, source.client_type,
    source.industry, source.jurisdiction, CURRENT_TIMESTAMP
);

-- ===================== STEP 4: Silver -> Gold Fact Billings =====================

MERGE INTO {{zone_prefix}}.gold.fact_billings AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY be.billing_date, be.billing_id) AS billing_key,
        dc.case_key,
        da.attorney_key,
        dcl.client_key,
        be.billing_date,
        be.hours,
        be.hourly_rate,
        be.amount,
        be.billable_flag
    FROM {{zone_prefix}}.silver.billings_enriched be
    JOIN {{zone_prefix}}.gold.dim_case dc ON be.case_id = dc.case_id
    JOIN {{zone_prefix}}.gold.dim_attorney da ON be.attorney_id = da.attorney_id
    JOIN {{zone_prefix}}.gold.dim_client dcl ON be.client_id = dcl.client_id
) AS source
ON target.case_key = source.case_key AND target.attorney_key = source.attorney_key AND target.billing_date = source.billing_date AND target.hours = source.hours
WHEN MATCHED THEN UPDATE SET
    amount        = source.amount,
    billable_flag = source.billable_flag,
    loaded_at     = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    billing_key, case_key, attorney_key, client_key, billing_date,
    hours, hourly_rate, amount, billable_flag, loaded_at
) VALUES (
    source.billing_key, source.case_key, source.attorney_key, source.client_key,
    source.billing_date, source.hours, source.hourly_rate, source.amount,
    source.billable_flag, CURRENT_TIMESTAMP
);

-- ===================== STEP 5: Optimize Gold Tables =====================

OPTIMIZE {{zone_prefix}}.gold.dim_case;
OPTIMIZE {{zone_prefix}}.gold.dim_attorney;
OPTIMIZE {{zone_prefix}}.gold.dim_client;
OPTIMIZE {{zone_prefix}}.gold.fact_billings;
