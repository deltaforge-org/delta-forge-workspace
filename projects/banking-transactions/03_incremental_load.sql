-- =============================================================================
-- Banking Transactions Pipeline - Incremental Load
-- =============================================================================
-- Inserts 10 new February transactions (including 2 fraud-flagged), processes
-- via INCREMENTAL_FILTER macro, refreshes gold fact and KPIs.
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically with overlap_days for late-arriving
-- data. This eliminates the need to manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "transaction_id > 'TXN00070' AND transaction_date > '2024-01-13'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transactions_enriched, transaction_id, transaction_date, 1)}};

-- ===================== STEP 1: Capture Current Watermarks =====================

SELECT MAX(ingested_at) AS current_watermark FROM {{zone_prefix}}.silver.transactions_enriched;

SELECT COUNT(*) AS pre_silver_count FROM {{zone_prefix}}.silver.transactions_enriched;

SELECT COUNT(*) AS pre_gold_count FROM {{zone_prefix}}.gold.fact_transactions;

-- ===================== STEP 2: Insert New Bronze Transactions (10 rows, Feb 2024) =====================
-- Includes 2 fraud-flagged: one crypto + suspicious + high value, one velocity cluster

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
    ('TXN00071', 'ACC001', 'M001', '2024-02-01T09:00:00', 92.30, 'debit', 'pos', false, '2024-02-01T12:00:00'),
    ('TXN00072', 'ACC002', 'M005', '2024-02-01T14:20:00', 245.99, 'debit', 'online', false, '2024-02-01T12:00:00'),
    ('TXN00073', 'ACC003', NULL, '2024-02-02T08:00:00', 4000.00, 'credit', 'wire', false, '2024-02-01T12:00:00'),
    -- HIGH FRAUD: crypto + suspicious + late night + high value
    ('TXN00074', 'ACC004', 'M014', '2024-02-02T02:45:00', 6500.00, 'debit', 'online', true, '2024-02-01T12:00:00'),
    ('TXN00075', 'ACC005', 'M009', '2024-02-03T07:30:00', 1560.00, 'debit', 'online', false, '2024-02-01T12:00:00'),
    ('TXN00076', 'ACC007', 'M012', '2024-02-03T14:00:00', 3299.99, 'debit', 'pos', false, '2024-02-01T12:00:00'),
    -- HIGH FRAUD: crypto + suspicious for ACC010 dormant account
    ('TXN00077', 'ACC010', 'M014', '2024-02-04T03:15:00', 1200.00, 'debit', 'online', true, '2024-02-01T12:00:00'),
    ('TXN00078', 'ACC008', 'M010', '2024-02-04T19:30:00', 35.50, 'debit', 'online', false, '2024-02-01T12:00:00'),
    ('TXN00079', 'ACC006', 'M002', '2024-02-05T12:45:00', 67.80, 'debit', 'pos', false, '2024-02-01T12:00:00'),
    ('TXN00080', 'ACC011', 'M015', '2024-02-05T16:20:00', 199.99, 'debit', 'pos', false, '2024-02-01T12:00:00');

-- ===================== STEP 3: Incremental MERGE to Silver =====================
-- Uses INCREMENTAL_FILTER to process only new records with 1-day overlap

MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS target
USING (
    WITH new_txns AS (
        SELECT *
        FROM {{zone_prefix}}.bronze.raw_transactions
        WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transactions_enriched, transaction_id, transaction_date, 1)}}
    ),
    enriched AS (
        SELECT
            t.transaction_id,
            t.account_id,
            t.merchant_id,
            m.category AS merchant_category,
            m.risk_level AS merchant_risk,
            t.transaction_date,
            t.amount,
            t.transaction_type,
            t.channel,
            t.is_suspicious,
            t.ingested_at,
            LAG(t.amount) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date) AS prev_txn_amount,
            DATEDIFF(
                t.transaction_date,
                LAG(t.transaction_date) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date)
            ) * 86400 AS time_since_prev_sec,
            COUNT(*) OVER (
                PARTITION BY t.account_id
                ORDER BY t.transaction_date
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS txn_velocity_5,
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.ingested_at ASC) AS rn
        FROM new_txns t
        LEFT JOIN {{zone_prefix}}.bronze.raw_merchants m ON t.merchant_id = m.merchant_id
    ),
    scored AS (
        SELECT
            e.*,
            0.00 AS running_balance,
            CAST(LEAST(100, GREATEST(0,
                CASE WHEN e.amount > 5000 THEN 30 ELSE 0 END
                + CASE WHEN e.merchant_category = 'crypto' THEN 25 ELSE 0 END
                + CASE WHEN e.txn_velocity_5 > 5 AND e.time_since_prev_sec IS NOT NULL
                       AND e.time_since_prev_sec < 86400 THEN 20 ELSE 0 END
                + CASE WHEN e.is_suspicious = true THEN 15 ELSE 0 END
                + CASE WHEN EXTRACT(HOUR FROM e.transaction_date) BETWEEN 1 AND 5 THEN 10 ELSE 0 END
            )) AS INT) AS fraud_score
        FROM enriched e
        WHERE e.rn = 1
    )
    SELECT * FROM scored
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET
    merchant_category   = source.merchant_category,
    merchant_risk       = source.merchant_risk,
    fraud_score         = source.fraud_score,
    prev_txn_amount     = source.prev_txn_amount,
    time_since_prev_sec = source.time_since_prev_sec,
    txn_velocity_5      = source.txn_velocity_5,
    processed_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_id, account_id, merchant_id, merchant_category, merchant_risk,
    transaction_date, amount, transaction_type, channel, running_balance,
    fraud_score, is_suspicious, prev_txn_amount, time_since_prev_sec,
    txn_velocity_5, ingested_at, processed_at
) VALUES (
    source.transaction_id, source.account_id, source.merchant_id, source.merchant_category,
    source.merchant_risk, source.transaction_date, source.amount, source.transaction_type,
    source.channel, source.running_balance, source.fraud_score, source.is_suspicious,
    source.prev_txn_amount, source.time_since_prev_sec, source.txn_velocity_5,
    source.ingested_at, CURRENT_TIMESTAMP
);

-- ===================== STEP 4: Update CDF Customer Dim =====================
-- Update last_txn info in customer_dim for CDF tracking

MERGE INTO {{zone_prefix}}.silver.customer_dim AS target
USING (
    SELECT
        account_id,
        MAX(transaction_date) AS last_txn_date
    FROM {{zone_prefix}}.silver.transactions_enriched
    WHERE ingested_at > '2024-01-15T00:00:00'
    GROUP BY account_id
) AS source
ON target.account_id = source.account_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    updated_at = CURRENT_TIMESTAMP;

-- ===================== STEP 5: Verify Incremental Processing =====================

SELECT COUNT(*) AS post_silver_count FROM {{zone_prefix}}.silver.transactions_enriched;

ASSERT VALUE new_silver_records = 10
SELECT post.cnt - pre.cnt AS new_silver_records
FROM (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.transactions_enriched) post,
     (SELECT 70 AS cnt) pre;

-- Verify no duplicates
ASSERT VALUE dup_check = 0
SELECT COUNT(*) AS dup_check
FROM (
    SELECT transaction_id, COUNT(*) AS cnt
    FROM {{zone_prefix}}.silver.transactions_enriched
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
);

-- ===================== STEP 6: Refresh Gold Fact (incremental) =====================

MERGE INTO {{zone_prefix}}.gold.fact_transactions AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY te.transaction_date, te.transaction_id) AS transaction_key,
        da.account_key,
        dm.merchant_key,
        dd.date_key,
        te.transaction_date,
        te.amount,
        te.transaction_type,
        te.running_balance,
        te.fraud_score,
        te.channel
    FROM {{zone_prefix}}.silver.transactions_enriched te
    JOIN {{zone_prefix}}.gold.dim_account da ON te.account_id = da.account_id
    LEFT JOIN {{zone_prefix}}.gold.dim_merchant dm ON te.merchant_id = dm.merchant_id
    LEFT JOIN {{zone_prefix}}.gold.dim_date dd ON CAST(te.transaction_date AS DATE) = dd.full_date
) AS source
ON target.account_key = source.account_key
    AND target.transaction_date = source.transaction_date
    AND target.amount = source.amount
WHEN MATCHED THEN UPDATE SET
    merchant_key     = source.merchant_key,
    date_key         = source.date_key,
    fraud_score      = source.fraud_score,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_key, account_key, merchant_key, date_key, transaction_date, amount,
    transaction_type, running_balance, fraud_score, channel, loaded_at
) VALUES (
    source.transaction_key, source.account_key, source.merchant_key, source.date_key,
    source.transaction_date, source.amount, source.transaction_type, source.running_balance,
    source.fraud_score, source.channel, CURRENT_TIMESTAMP
);

ASSERT VALUE post_gold_count >= 80
SELECT COUNT(*) AS post_gold_count FROM {{zone_prefix}}.gold.fact_transactions;

-- ===================== STEP 7: Refresh KPI Daily Volumes =====================

MERGE INTO {{zone_prefix}}.gold.kpi_daily_volumes AS target
USING (
    WITH daily AS (
        SELECT
            CAST(transaction_date AS DATE) AS transaction_date,
            COUNT(*) AS total_txns,
            ROUND(SUM(amount), 2) AS total_amount,
            ROUND(AVG(amount), 2) AS avg_amount,
            SUM(CASE WHEN fraud_score >= 40 THEN 1 ELSE 0 END) AS fraud_flagged_count,
            ROUND(100.0 * SUM(CASE WHEN fraud_score >= 40 THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_pct
        FROM {{zone_prefix}}.gold.fact_transactions
        GROUP BY CAST(transaction_date AS DATE)
    )
    SELECT
        d.*,
        ROUND(AVG(d.total_txns) OVER (
            ORDER BY d.transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS running_7d_avg_txns,
        ROUND(AVG(d.total_amount) OVER (
            ORDER BY d.transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS running_7d_avg_amount
    FROM daily d
) AS source
ON target.transaction_date = source.transaction_date
WHEN MATCHED THEN UPDATE SET
    total_txns            = source.total_txns,
    total_amount          = source.total_amount,
    avg_amount            = source.avg_amount,
    fraud_flagged_count   = source.fraud_flagged_count,
    fraud_pct             = source.fraud_pct,
    running_7d_avg_txns   = source.running_7d_avg_txns,
    running_7d_avg_amount = source.running_7d_avg_amount,
    loaded_at             = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_date, total_txns, total_amount, avg_amount,
    fraud_flagged_count, fraud_pct, running_7d_avg_txns, running_7d_avg_amount, loaded_at
) VALUES (
    source.transaction_date, source.total_txns, source.total_amount, source.avg_amount,
    source.fraud_flagged_count, source.fraud_pct, source.running_7d_avg_txns,
    source.running_7d_avg_amount, CURRENT_TIMESTAMP
);
