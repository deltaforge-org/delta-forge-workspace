-- =============================================================================
-- Legal Case Management Pipeline - Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "billing_id > 'BIL065' AND billing_date > '2024-01-12'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.billings_enriched, billing_id, billing_date, 3)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.billings_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_billings
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.billings_enriched, billing_id, billing_date, 3)}};

-- ===================== STEP 1: Capture Current State =====================

SELECT COUNT(*) AS pre_billing_count FROM {{zone_prefix}}.silver.billings_enriched;
SELECT COUNT(*) AS pre_fact_count FROM {{zone_prefix}}.gold.fact_billings;
SELECT MAX(processed_at) AS current_watermark FROM {{zone_prefix}}.silver.billings_enriched;

-- ===================== STEP 2: Insert New Bronze Billing Entries =====================

INSERT INTO {{zone_prefix}}.bronze.raw_billings VALUES
    ('BIL066', 'CASE01', 'ATT01', 'CLI01', '2024-01-15', 6.0, 650.00, 3900.00, true, 'Post-merger regulatory filing', '2024-01-20T00:00:00'),
    ('BIL067', 'CASE06', 'ATT01', 'CLI06', '2024-01-15', 9.0, 650.00, 5850.00, true, 'SEC hearing preparation', '2024-01-20T00:00:00'),
    ('BIL068', 'CASE12', 'ATT08', 'CLI10', '2024-01-15', 5.0, 375.00, 1875.00, true, 'Discovery request drafting', '2024-01-20T00:00:00'),
    ('BIL069', 'CASE10', 'ATT06', 'CLI02', '2024-01-15', 7.5, 425.00, 3187.50, true, 'Class notice preparation', '2024-01-20T00:00:00'),
    ('BIL070', 'CASE02', 'ATT03', 'CLI07', '2024-01-15', 8.0, 580.00, 4640.00, true, 'Trial exhibit preparation', '2024-01-20T00:00:00'),
    ('BIL071', 'CASE09', 'ATT07', 'CLI09', '2024-01-15', 4.0, 520.00, 2080.00, true, 'Closing conditions review', '2024-01-20T00:00:00'),
    ('BIL072', 'CASE04', 'ATT02', 'CLI04', '2024-01-15', 5.5, 475.00, 2612.50, true, 'Pre-trial conference', '2024-01-20T00:00:00'),
    ('BIL073', 'CASE06', 'ATT07', 'CLI06', '2024-01-15', 3.0, 520.00, 1560.00, true, 'Document production review', '2024-01-20T00:00:00');

-- ===================== STEP 3: Incremental MERGE to Silver =====================

MERGE INTO {{zone_prefix}}.silver.billings_enriched AS target
USING (
    WITH case_complexity AS (
        SELECT
            b.case_id,
            COUNT(DISTINCT b.attorney_id) AS attorney_count,
            SUM(b.hours) AS total_hours,
            COUNT(DISTINCT cc.client_id) AS party_count
        FROM {{zone_prefix}}.bronze.raw_billings b
        LEFT JOIN {{zone_prefix}}.bronze.raw_case_clients cc ON b.case_id = cc.case_id
        GROUP BY b.case_id
    ),
    new_billings AS (
        SELECT b.*
        FROM {{zone_prefix}}.bronze.raw_billings b
        WHERE b.ingested_at > (
            SELECT COALESCE(MAX(processed_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.billings_enriched
        )
    )
    SELECT
        nb.billing_id, nb.case_id, nb.attorney_id, nb.client_id,
        c.case_type, a.practice_area, nb.billing_date, nb.hours,
        nb.hourly_rate, nb.amount, nb.billable_flag, a.partner_flag,
        LEAST(10.0, ROUND(LN(GREATEST(1, cx.total_hours)) * cx.attorney_count * cx.party_count / 5.0, 2)) AS complexity_score
    FROM new_billings nb
    JOIN {{zone_prefix}}.bronze.raw_cases c ON nb.case_id = c.case_id
    JOIN {{zone_prefix}}.bronze.raw_attorneys a ON nb.attorney_id = a.attorney_id
    JOIN case_complexity cx ON nb.case_id = cx.case_id
) AS source
ON target.billing_id = source.billing_id
WHEN MATCHED THEN UPDATE SET
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

-- ===================== STEP 4: Verify Incremental =====================

SELECT COUNT(*) AS post_billing_count FROM {{zone_prefix}}.silver.billings_enriched;

SELECT post.cnt - pre.cnt AS new_billings_added
FROM (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.billings_enriched) post,
     (SELECT 65 AS cnt) pre;

-- No duplicates
ASSERT VALUE new_billings_added = 8
SELECT COUNT(*) AS dup_check
FROM (
    SELECT billing_id, COUNT(*) AS cnt
    FROM {{zone_prefix}}.silver.billings_enriched
    GROUP BY billing_id
    HAVING COUNT(*) > 1
);
-- ===================== STEP 5: Refresh Gold Fact =====================

MERGE INTO {{zone_prefix}}.gold.fact_billings AS target
USING (
ASSERT VALUE dup_check = 0
    SELECT
        ROW_NUMBER() OVER (ORDER BY be.billing_date, be.billing_id) AS billing_key,
        dc.case_key, da.attorney_key, dcl.client_key,
        be.billing_date, be.hours, be.hourly_rate, be.amount, be.billable_flag
    FROM {{zone_prefix}}.silver.billings_enriched be
    JOIN {{zone_prefix}}.gold.dim_case dc ON be.case_id = dc.case_id
    JOIN {{zone_prefix}}.gold.dim_attorney da ON be.attorney_id = da.attorney_id
    JOIN {{zone_prefix}}.gold.dim_client dcl ON be.client_id = dcl.client_id
) AS source
ON target.case_key = source.case_key AND target.attorney_key = source.attorney_key AND target.billing_date = source.billing_date AND target.hours = source.hours
WHEN MATCHED THEN UPDATE SET
    amount    = source.amount,
    loaded_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    billing_key, case_key, attorney_key, client_key, billing_date,
    hours, hourly_rate, amount, billable_flag, loaded_at
) VALUES (
    source.billing_key, source.case_key, source.attorney_key, source.client_key,
    source.billing_date, source.hours, source.hourly_rate, source.amount,
    source.billable_flag, CURRENT_TIMESTAMP
);

SELECT COUNT(*) AS post_fact_count FROM {{zone_prefix}}.gold.fact_billings;
ASSERT VALUE post_fact_count >= 73
SELECT 'post_fact_count check passed' AS post_fact_count_status;

