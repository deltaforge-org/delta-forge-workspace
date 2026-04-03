-- =============================================================================
-- Banking Transactions Pipeline - Incremental Load
-- =============================================================================

-- ============================================================================
-- DYNAMIC INCREMENTAL FILTER (Delta Forge Macro)
-- ============================================================================
-- The INCREMENTAL_FILTER macro reads MAX values from the target silver table
-- and generates a WHERE clause dynamically. This eliminates the need to
-- manually track watermarks.
--
-- Usage: {{INCREMENTAL_FILTER(target_table, key_col, date_col, overlap_days)}}
-- Output: expands to e.g. "transaction_id > 'TXN00065' AND transaction_date > '2024-01-14'"
--         or "1=1" if the target table is empty (first run = full load)
-- ============================================================================

-- Preview the generated filter condition:
PRINT {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transactions_enriched, transaction_id, transaction_date, 1)}};

-- Use in a MERGE or INSERT ... SELECT:
-- INSERT INTO {{zone_prefix}}.silver.transactions_enriched
-- SELECT * FROM {{zone_prefix}}.bronze.raw_transactions
-- WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.silver.transactions_enriched, transaction_id, transaction_date, 1)}};

-- ===================== STEP 1: Capture Current Watermark =====================

SELECT MAX(ingested_at) AS current_watermark FROM {{zone_prefix}}.silver.transactions_enriched;

SELECT COUNT(*) AS pre_silver_count FROM {{zone_prefix}}.silver.transactions_enriched;
SELECT COUNT(*) AS pre_gold_count FROM {{zone_prefix}}.gold.fact_transactions;

-- ===================== STEP 2: Insert New Bronze Transactions =====================

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
    ('TXN00066', 'ACC001', 'M001', '2024-01-15T09:00:00', 92.30, 'debit', 'pos', false, '2024-01-15T12:00:00'),
    ('TXN00067', 'ACC002', 'M014', '2024-01-15T03:15:00', 3800.00, 'debit', 'online', true, '2024-01-15T12:00:00'),
    ('TXN00068', 'ACC003', NULL, '2024-01-15T08:00:00', 4000.00, 'credit', 'wire', false, '2024-01-15T12:00:00'),
    ('TXN00069', 'ACC005', 'M009', '2024-01-15T07:30:00', 1560.00, 'debit', 'online', false, '2024-01-15T12:00:00'),
    ('TXN00070', 'ACC007', 'M012', '2024-01-15T14:00:00', 3299.99, 'debit', 'pos', false, '2024-01-15T12:00:00'),
    ('TXN00071', 'ACC004', 'M014', '2024-01-15T02:45:00', 5000.00, 'debit', 'online', true, '2024-01-15T12:00:00'),
    ('TXN00072', 'ACC008', 'M010', '2024-01-15T19:30:00', 35.50, 'debit', 'online', false, '2024-01-15T12:00:00'),
    ('TXN00073', 'ACC006', 'M002', '2024-01-15T12:45:00', 67.80, 'debit', 'pos', false, '2024-01-15T12:00:00'),
    ('TXN00074', 'ACC009', 'M005', '2024-01-15T16:20:00', 199.99, 'debit', 'online', false, '2024-01-15T12:00:00'),
    ('TXN00075', 'ACC001', 'M004', '2024-01-15T17:00:00', 55.40, 'debit', 'pos', false, '2024-01-15T12:00:00');

-- ===================== STEP 3: Incremental MERGE to Silver =====================

MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS target
USING (
    WITH new_txns AS (
        SELECT *
        FROM {{zone_prefix}}.bronze.raw_transactions
        WHERE ingested_at > (
            SELECT COALESCE(MAX(ingested_at), '1970-01-01T00:00:00') FROM {{zone_prefix}}.silver.transactions_enriched
        )
    ),
    enriched AS (
        SELECT
            t.transaction_id,
            t.account_id,
            t.merchant_id,
            m.category AS merchant_category,
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
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.ingested_at ASC) AS rn
        FROM new_txns t
        LEFT JOIN {{zone_prefix}}.bronze.raw_merchants m ON t.merchant_id = m.merchant_id
    ),
    scored AS (
        SELECT
            e.*,
            0.00 AS running_balance,
            LEAST(100.0, GREATEST(0.0,
                CASE WHEN e.amount > 2000 THEN 30.0 ELSE 0.0 END
                + CASE WHEN e.merchant_category = 'crypto' THEN 25.0 ELSE 0.0 END
                + CASE WHEN e.time_since_prev_sec < 600 AND e.amount > 500 THEN 20.0 ELSE 0.0 END
                + CASE WHEN e.is_suspicious = true THEN 15.0 ELSE 0.0 END
                + CASE WHEN EXTRACT(HOUR FROM e.transaction_date) BETWEEN 0 AND 4 THEN 10.0 ELSE 0.0 END
            )) AS fraud_score
        FROM enriched e
        WHERE e.rn = 1
    )
    SELECT * FROM scored
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET
    merchant_category   = source.merchant_category,
    fraud_score         = source.fraud_score,
    prev_txn_amount     = source.prev_txn_amount,
    time_since_prev_sec = source.time_since_prev_sec,
    processed_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_id, account_id, merchant_id, merchant_category, transaction_date,
    amount, transaction_type, channel, running_balance, fraud_score, is_suspicious,
    prev_txn_amount, time_since_prev_sec, ingested_at, processed_at
) VALUES (
    source.transaction_id, source.account_id, source.merchant_id, source.merchant_category,
    source.transaction_date, source.amount, source.transaction_type, source.channel,
    source.running_balance, source.fraud_score, source.is_suspicious,
    source.prev_txn_amount, source.time_since_prev_sec, source.ingested_at, CURRENT_TIMESTAMP
);

-- ===================== STEP 4: Update CDF Account Balances =====================

MERGE INTO {{zone_prefix}}.silver.accounts_cdf AS target
USING (
    SELECT
        account_id,
        MAX(transaction_date) AS last_txn_date
    FROM {{zone_prefix}}.silver.transactions_enriched
    WHERE ingested_at > '2024-01-15T00:00:00'
    GROUP BY account_id
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    last_txn_date = source.last_txn_date,
    updated_at    = CURRENT_TIMESTAMP;

-- ===================== STEP 5: Verify Incremental Processing =====================

SELECT COUNT(*) AS post_silver_count FROM {{zone_prefix}}.silver.transactions_enriched;

SELECT post.cnt - pre.cnt AS new_silver_records
FROM (SELECT COUNT(*) AS cnt FROM {{zone_prefix}}.silver.transactions_enriched) post,
     (SELECT 65 AS cnt) pre;

-- Verify no duplicates
ASSERT VALUE new_silver_records = 10
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
ASSERT VALUE dup_check = 0
    SELECT
        ROW_NUMBER() OVER (ORDER BY te.transaction_date, te.transaction_id) AS transaction_key,
        da.account_key,
        dm.merchant_key,
        te.transaction_date,
        te.amount,
        te.transaction_type,
        te.running_balance,
        te.fraud_score,
        te.channel
    FROM {{zone_prefix}}.silver.transactions_enriched te
    JOIN {{zone_prefix}}.gold.dim_account da ON te.account_id = da.account_id
    LEFT JOIN {{zone_prefix}}.gold.dim_merchant dm ON te.merchant_id = dm.merchant_id
) AS source
ON target.account_key = source.account_key AND target.transaction_date = source.transaction_date AND target.amount = source.amount
WHEN MATCHED THEN UPDATE SET
    fraud_score = source.fraud_score,
    loaded_at   = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_key, account_key, merchant_key, transaction_date, amount,
    transaction_type, running_balance, fraud_score, channel, loaded_at
) VALUES (
    source.transaction_key, source.account_key, source.merchant_key, source.transaction_date,
    source.amount, source.transaction_type, source.running_balance, source.fraud_score,
    source.channel, CURRENT_TIMESTAMP
);

SELECT COUNT(*) AS post_gold_count FROM {{zone_prefix}}.gold.fact_transactions;
ASSERT VALUE post_gold_count >= 75
SELECT 'post_gold_count check passed' AS post_gold_count_status;

