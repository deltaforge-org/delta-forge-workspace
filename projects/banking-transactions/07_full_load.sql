-- =============================================================================
-- Banking Transactions Pipeline - Full Load Transformation
-- =============================================================================
-- 11-step DAG: validate -> build_customer_scd2 + enrich_transactions +
-- build_dim_merchant (parallel) -> materialize_cdf_snapshots ->
-- build_dim_account -> build_dim_date -> build_fact_transactions ->
-- compute_daily_kpi + compute_customer_health (parallel) ->
-- optimize_and_vacuum
-- =============================================================================

PIPELINE bank_transaction_pipeline
  DESCRIPTION 'Every-4-hour banking pipeline: SCD2 tier tracking, fraud scoring, CDF snapshots, tier migration matrix'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'banking,transactions,fraud,CDF,SCD2'
  SLA 1800
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze =====================

ASSERT ROW_COUNT >= 70
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_transactions;

ASSERT ROW_COUNT >= 12
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_accounts;

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_merchants;

-- ===================== build_customer_scd2 =====================
-- SCD Type 2: load initial customer records, then process tier upgrades

-- Pass 1: Load initial (oldest) version of each account
MERGE INTO bank.silver.customer_dim AS target
USING (
    WITH ranked AS (
        SELECT
            account_id,
            account_number,
            account_type,
            customer_name,
            branch,
            open_date,
            status,
            current_balance,
            customer_tier,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ingested_at ASC) AS version_num,
            ROW_NUMBER() OVER (ORDER BY account_id, ingested_at ASC) AS customer_id
        FROM bank.bronze.raw_accounts
    )
    SELECT * FROM ranked WHERE version_num = 1
) AS source
ON target.account_id = source.account_id AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
    customer_id, account_id, account_number, account_type, customer_name,
    branch, open_date, status, customer_tier,
    valid_from, valid_to, is_current, updated_at
) VALUES (
    source.customer_id, source.account_id, source.account_number, source.account_type,
    source.customer_name, source.branch, source.open_date, source.status,
    source.customer_tier,
    CAST('2024-01-15' AS DATE), NULL, true, CURRENT_TIMESTAMP
);

-- Pass 2: Expire current records for customers with tier upgrades
MERGE INTO bank.silver.customer_dim AS target
USING (
    WITH ranked AS (
        SELECT
            account_id,
            account_number,
            account_type,
            customer_name,
            branch,
            open_date,
            status,
            current_balance,
            customer_tier,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ingested_at ASC) AS version_num
        FROM bank.bronze.raw_accounts
    )
    SELECT * FROM ranked WHERE version_num > 1
) AS source
ON target.account_id = source.account_id AND target.is_current = true
    AND target.customer_tier <> source.customer_tier
WHEN MATCHED THEN UPDATE SET
    valid_to    = CAST('2024-02-01' AS DATE),
    is_current  = false,
    updated_at  = CURRENT_TIMESTAMP;

-- Pass 3: Insert new current records for upgraded customers
MERGE INTO bank.silver.customer_dim AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY r.account_id) + 100 AS customer_id,
        r.account_id,
        r.account_number,
        r.account_type,
        r.customer_name,
        r.branch,
        r.open_date,
        r.status,
        r.customer_tier,
        CAST('2024-02-01' AS DATE) AS valid_from,
        NULL AS valid_to,
        true AS is_current,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT
            account_id,
            account_number,
            account_type,
            customer_name,
            branch,
            open_date,
            status,
            current_balance,
            customer_tier,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ingested_at ASC) AS version_num
        FROM bank.bronze.raw_accounts
    ) r
    WHERE r.version_num > 1
      AND EXISTS (
          SELECT 1 FROM bank.silver.customer_dim c
          WHERE c.account_id = r.account_id
            AND c.is_current = false
            AND c.valid_to = CAST('2024-02-01' AS DATE)
      )
) AS source
ON target.account_id = source.account_id AND target.valid_from = CAST('2024-02-01' AS DATE) AND target.is_current = true
WHEN NOT MATCHED THEN INSERT (
    customer_id, account_id, account_number, account_type, customer_name,
    branch, open_date, status, customer_tier,
    valid_from, valid_to, is_current, updated_at
) VALUES (
    source.customer_id, source.account_id, source.account_number, source.account_type,
    source.customer_name, source.branch, source.open_date, source.status,
    source.customer_tier, source.valid_from, source.valid_to, source.is_current,
    source.updated_at
);

-- Verify: 12 accounts + 3 expired versions = 15 total
ASSERT VALUE customer_dim_count >= 12
SELECT COUNT(*) AS customer_dim_count FROM bank.silver.customer_dim;

-- Verify 3 expired records
ASSERT VALUE expired_count = 3
SELECT COUNT(*) AS expired_count FROM bank.silver.customer_dim WHERE is_current = false;

-- ===================== enrich_transactions =====================
-- Multi-rule fraud scoring engine with velocity detection

MERGE INTO bank.silver.transactions_enriched AS target
USING (
    WITH base AS (
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
            -- Previous transaction for velocity checks
            LAG(t.amount) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date) AS prev_txn_amount,
            DATEDIFF(
                CAST(t.transaction_date AS DATE),
                CAST(LAG(t.transaction_date) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date) AS DATE)
            ) * 86400 AS time_since_prev_sec,
            -- Velocity: count of txns in rolling 5-row window per account
            COUNT(*) OVER (
                PARTITION BY t.account_id
                ORDER BY t.transaction_date
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) AS txn_velocity_5,
            -- Running balance per account
            SUM(CASE WHEN t.transaction_type = 'credit' THEN t.amount ELSE -t.amount END)
                OVER (PARTITION BY t.account_id ORDER BY t.transaction_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_delta,
            -- Dedup
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.ingested_at ASC) AS rn
        FROM bank.bronze.raw_transactions t
        LEFT JOIN bank.bronze.raw_merchants m ON t.merchant_id = m.merchant_id
    ),
    -- NOTE: DataFusion does not support correlated subqueries in JOIN ON conditions.
    -- Use a CTE instead of: JOIN t ON ... AND t.col = (SELECT MIN(...) FROM t WHERE ...)
    earliest_accounts AS (
        SELECT a.*
        FROM bank.bronze.raw_accounts a
        INNER JOIN (
            SELECT account_id, MIN(ingested_at) AS min_ingested
            FROM bank.bronze.raw_accounts
            GROUP BY account_id
        ) ea ON a.account_id = ea.account_id AND a.ingested_at = ea.min_ingested
    ),
    scored AS (
        SELECT
            b.*,
            a.current_balance + b.balance_delta AS running_balance,
            -- Multi-rule fraud scoring (0-100):
            -- +30 if amount > 5000 (high value)
            -- +25 if crypto merchant (high-risk category)
            -- +20 if velocity_5 > 5 AND time_since_prev < 86400 (velocity clustering)
            -- +15 if suspicious flag set
            -- +10 if transaction between 1 AM and 5 AM (unusual hours)
            CAST(LEAST(100, GREATEST(0,
                CASE WHEN b.amount > 5000 THEN 30 ELSE 0 END
                + CASE WHEN b.merchant_category = 'crypto' THEN 25 ELSE 0 END
                + CASE WHEN b.txn_velocity_5 > 5 AND b.time_since_prev_sec IS NOT NULL
                       AND b.time_since_prev_sec < 86400 THEN 20 ELSE 0 END
                + CASE WHEN b.is_suspicious = true THEN 15 ELSE 0 END
                + CASE WHEN EXTRACT(HOUR FROM b.transaction_date) BETWEEN 1 AND 5 THEN 10 ELSE 0 END
            )) AS INT) AS fraud_score
        FROM base b
        JOIN earliest_accounts a ON b.account_id = a.account_id
        WHERE b.rn = 1
    )
    SELECT * FROM scored
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET
    merchant_category   = source.merchant_category,
    merchant_risk       = source.merchant_risk,
    running_balance     = source.running_balance,
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

ASSERT VALUE silver_txn_count = 70
SELECT COUNT(*) AS silver_txn_count FROM bank.silver.transactions_enriched;

-- ===================== build_dim_merchant =====================

MERGE INTO bank.gold.dim_merchant AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY merchant_id) AS merchant_key,
        merchant_id,
        merchant_name,
        category,
        city,
        country,
        risk_level
    FROM bank.bronze.raw_merchants
) AS source
ON target.merchant_id = source.merchant_id
WHEN MATCHED THEN UPDATE SET
    merchant_name = source.merchant_name,
    category      = source.category,
    city          = source.city,
    country       = source.country,
    risk_level    = source.risk_level,
    loaded_at     = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    merchant_key, merchant_id, merchant_name, category, city, country, risk_level, loaded_at
) VALUES (
    source.merchant_key, source.merchant_id, source.merchant_name, source.category,
    source.city, source.country, source.risk_level, CURRENT_TIMESTAMP
);

-- ===================== materialize_cdf_snapshots =====================
-- Read CDF changes from customer_dim and materialize into balance_snapshots

INSERT INTO bank.silver.balance_snapshots
SELECT
    ROW_NUMBER() OVER (ORDER BY _commit_timestamp, account_id) AS snapshot_id,
    account_id,
    customer_name,
    CASE
        WHEN _change_type = 'update_preimage' THEN customer_tier
        ELSE NULL
    END AS old_tier,
    CASE
        WHEN _change_type IN ('update_postimage', 'insert') THEN customer_tier
        ELSE NULL
    END AS new_tier,
    _change_type AS change_type,
    _commit_timestamp AS snapshot_timestamp
FROM table_changes('bank.silver.customer_dim', 0);

ASSERT VALUE snapshot_count >= 1
SELECT COUNT(*) AS snapshot_count FROM bank.silver.balance_snapshots;

-- ===================== build_dim_account =====================

MERGE INTO bank.gold.dim_account AS target
USING (
    SELECT
        ROW_NUMBER() OVER (ORDER BY account_id) AS account_key,
        account_id,
        account_number,
        account_type,
        customer_name,
        branch,
        open_date,
        status,
        customer_tier
    FROM bank.silver.customer_dim
    WHERE is_current = true
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    account_number  = source.account_number,
    account_type    = source.account_type,
    customer_name   = source.customer_name,
    branch          = source.branch,
    status          = source.status,
    customer_tier   = source.customer_tier,
    loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    account_key, account_id, account_number, account_type, customer_name,
    branch, open_date, status, customer_tier, loaded_at
) VALUES (
    source.account_key, source.account_id, source.account_number, source.account_type,
    source.customer_name, source.branch, source.open_date, source.status,
    source.customer_tier, CURRENT_TIMESTAMP
);

-- ===================== build_dim_date =====================
-- Generate a date dimension covering Jan 1 - Mar 31, 2024

MERGE INTO bank.gold.dim_date AS target
USING (
    WITH date_series AS (
        SELECT CAST('2024-01-01' AS DATE) AS full_date
        UNION ALL SELECT CAST('2024-01-02' AS DATE) UNION ALL SELECT CAST('2024-01-03' AS DATE)
        UNION ALL SELECT CAST('2024-01-04' AS DATE) UNION ALL SELECT CAST('2024-01-05' AS DATE)
        UNION ALL SELECT CAST('2024-01-06' AS DATE) UNION ALL SELECT CAST('2024-01-07' AS DATE)
        UNION ALL SELECT CAST('2024-01-08' AS DATE) UNION ALL SELECT CAST('2024-01-09' AS DATE)
        UNION ALL SELECT CAST('2024-01-10' AS DATE) UNION ALL SELECT CAST('2024-01-11' AS DATE)
        UNION ALL SELECT CAST('2024-01-12' AS DATE) UNION ALL SELECT CAST('2024-01-13' AS DATE)
        UNION ALL SELECT CAST('2024-01-14' AS DATE) UNION ALL SELECT CAST('2024-01-15' AS DATE)
        UNION ALL SELECT CAST('2024-01-16' AS DATE) UNION ALL SELECT CAST('2024-01-17' AS DATE)
        UNION ALL SELECT CAST('2024-01-18' AS DATE) UNION ALL SELECT CAST('2024-01-19' AS DATE)
        UNION ALL SELECT CAST('2024-01-20' AS DATE) UNION ALL SELECT CAST('2024-01-21' AS DATE)
        UNION ALL SELECT CAST('2024-01-22' AS DATE) UNION ALL SELECT CAST('2024-01-23' AS DATE)
        UNION ALL SELECT CAST('2024-01-24' AS DATE) UNION ALL SELECT CAST('2024-01-25' AS DATE)
        UNION ALL SELECT CAST('2024-01-26' AS DATE) UNION ALL SELECT CAST('2024-01-27' AS DATE)
        UNION ALL SELECT CAST('2024-01-28' AS DATE) UNION ALL SELECT CAST('2024-01-29' AS DATE)
        UNION ALL SELECT CAST('2024-01-30' AS DATE) UNION ALL SELECT CAST('2024-01-31' AS DATE)
        UNION ALL SELECT CAST('2024-02-01' AS DATE) UNION ALL SELECT CAST('2024-02-02' AS DATE)
        UNION ALL SELECT CAST('2024-02-03' AS DATE) UNION ALL SELECT CAST('2024-02-04' AS DATE)
        UNION ALL SELECT CAST('2024-02-05' AS DATE) UNION ALL SELECT CAST('2024-02-06' AS DATE)
        UNION ALL SELECT CAST('2024-02-07' AS DATE) UNION ALL SELECT CAST('2024-02-08' AS DATE)
        UNION ALL SELECT CAST('2024-02-09' AS DATE) UNION ALL SELECT CAST('2024-02-10' AS DATE)
        UNION ALL SELECT CAST('2024-02-11' AS DATE) UNION ALL SELECT CAST('2024-02-12' AS DATE)
        UNION ALL SELECT CAST('2024-02-13' AS DATE) UNION ALL SELECT CAST('2024-02-14' AS DATE)
        UNION ALL SELECT CAST('2024-02-15' AS DATE) UNION ALL SELECT CAST('2024-02-16' AS DATE)
        UNION ALL SELECT CAST('2024-02-17' AS DATE) UNION ALL SELECT CAST('2024-02-18' AS DATE)
        UNION ALL SELECT CAST('2024-02-19' AS DATE) UNION ALL SELECT CAST('2024-02-20' AS DATE)
        UNION ALL SELECT CAST('2024-02-21' AS DATE) UNION ALL SELECT CAST('2024-02-22' AS DATE)
        UNION ALL SELECT CAST('2024-02-23' AS DATE) UNION ALL SELECT CAST('2024-02-24' AS DATE)
        UNION ALL SELECT CAST('2024-02-25' AS DATE) UNION ALL SELECT CAST('2024-02-26' AS DATE)
        UNION ALL SELECT CAST('2024-02-27' AS DATE) UNION ALL SELECT CAST('2024-02-28' AS DATE)
        UNION ALL SELECT CAST('2024-02-29' AS DATE)
        UNION ALL SELECT CAST('2024-03-01' AS DATE) UNION ALL SELECT CAST('2024-03-02' AS DATE)
        UNION ALL SELECT CAST('2024-03-03' AS DATE) UNION ALL SELECT CAST('2024-03-04' AS DATE)
        UNION ALL SELECT CAST('2024-03-05' AS DATE) UNION ALL SELECT CAST('2024-03-06' AS DATE)
        UNION ALL SELECT CAST('2024-03-07' AS DATE) UNION ALL SELECT CAST('2024-03-08' AS DATE)
        UNION ALL SELECT CAST('2024-03-09' AS DATE) UNION ALL SELECT CAST('2024-03-10' AS DATE)
        UNION ALL SELECT CAST('2024-03-11' AS DATE) UNION ALL SELECT CAST('2024-03-12' AS DATE)
        UNION ALL SELECT CAST('2024-03-13' AS DATE) UNION ALL SELECT CAST('2024-03-14' AS DATE)
        UNION ALL SELECT CAST('2024-03-15' AS DATE) UNION ALL SELECT CAST('2024-03-16' AS DATE)
        UNION ALL SELECT CAST('2024-03-17' AS DATE) UNION ALL SELECT CAST('2024-03-18' AS DATE)
        UNION ALL SELECT CAST('2024-03-19' AS DATE) UNION ALL SELECT CAST('2024-03-20' AS DATE)
        UNION ALL SELECT CAST('2024-03-21' AS DATE) UNION ALL SELECT CAST('2024-03-22' AS DATE)
        UNION ALL SELECT CAST('2024-03-23' AS DATE) UNION ALL SELECT CAST('2024-03-24' AS DATE)
        UNION ALL SELECT CAST('2024-03-25' AS DATE) UNION ALL SELECT CAST('2024-03-26' AS DATE)
        UNION ALL SELECT CAST('2024-03-27' AS DATE) UNION ALL SELECT CAST('2024-03-28' AS DATE)
        UNION ALL SELECT CAST('2024-03-29' AS DATE) UNION ALL SELECT CAST('2024-03-30' AS DATE)
        UNION ALL SELECT CAST('2024-03-31' AS DATE)
    )
    SELECT
        CAST(EXTRACT(YEAR FROM full_date) * 10000
             + EXTRACT(MONTH FROM full_date) * 100
             + EXTRACT(DAY FROM full_date) AS INT) AS date_key,
        full_date,
        EXTRACT(DOW FROM full_date) AS day_of_week,
        CASE EXTRACT(DOW FROM full_date)
            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        CAST(EXTRACT(MONTH FROM full_date) AS INT) AS month,
        CASE CAST(EXTRACT(MONTH FROM full_date) AS INT)
            WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
            WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
            WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
            WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
        END AS month_name,
        CAST((EXTRACT(MONTH FROM full_date) - 1) / 3 + 1 AS INT) AS quarter,
        CAST(EXTRACT(YEAR FROM full_date) AS INT) AS year,
        CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN true ELSE false END AS is_weekend
    FROM date_series
) AS source
ON target.date_key = source.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day_of_week, day_name, month, month_name,
    quarter, year, is_weekend, loaded_at
) VALUES (
    source.date_key, source.full_date, source.day_of_week, source.day_name,
    source.month, source.month_name, source.quarter, source.year,
    source.is_weekend, CURRENT_TIMESTAMP
);

-- ===================== build_fact_transactions =====================

MERGE INTO bank.gold.fact_transactions AS target
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
    FROM bank.silver.transactions_enriched te
    JOIN bank.gold.dim_account da ON te.account_id = da.account_id
    LEFT JOIN bank.gold.dim_merchant dm ON te.merchant_id = dm.merchant_id
    LEFT JOIN bank.gold.dim_date dd ON CAST(te.transaction_date AS DATE) = dd.full_date
) AS source
ON target.account_key = source.account_key
    AND target.transaction_date = source.transaction_date
    AND target.amount = source.amount
WHEN MATCHED THEN UPDATE SET
    merchant_key     = source.merchant_key,
    date_key         = source.date_key,
    transaction_type = source.transaction_type,
    running_balance  = source.running_balance,
    fraud_score      = source.fraud_score,
    channel          = source.channel,
    loaded_at        = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    transaction_key, account_key, merchant_key, date_key, transaction_date, amount,
    transaction_type, running_balance, fraud_score, channel, loaded_at
) VALUES (
    source.transaction_key, source.account_key, source.merchant_key, source.date_key,
    source.transaction_date, source.amount, source.transaction_type, source.running_balance,
    source.fraud_score, source.channel, CURRENT_TIMESTAMP
);

-- ===================== compute_daily_kpi =====================

MERGE INTO bank.gold.kpi_daily_volumes AS target
USING (
    WITH daily AS (
        SELECT
            CAST(transaction_date AS DATE) AS transaction_date,
            COUNT(*) AS total_txns,
            ROUND(SUM(amount), 2) AS total_amount,
            ROUND(AVG(amount), 2) AS avg_amount,
            SUM(CASE WHEN fraud_score >= 40 THEN 1 ELSE 0 END) AS fraud_flagged_count,
            ROUND(100.0 * SUM(CASE WHEN fraud_score >= 40 THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_pct
        FROM bank.gold.fact_transactions
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

-- ===================== compute_customer_health =====================
-- Tier migration matrix: count customers who moved between tiers using CDF snapshots

MERGE INTO bank.gold.kpi_customer_health AS target
USING (
    -- Use balance_snapshots to identify tier transitions
    -- Match preimage (old tier) with postimage (new tier) by account_id + timestamp proximity
    WITH tier_changes AS (
        SELECT
            pre.account_id,
            pre.old_tier AS from_tier,
            post.new_tier AS to_tier,
            post.snapshot_timestamp
        FROM bank.silver.balance_snapshots pre
        JOIN bank.silver.balance_snapshots post
            ON pre.account_id = post.account_id
            AND pre.change_type = 'update_preimage'
            AND post.change_type = 'update_postimage'
            AND pre.snapshot_timestamp = post.snapshot_timestamp
        WHERE pre.old_tier IS NOT NULL AND post.new_tier IS NOT NULL
              AND pre.old_tier <> post.new_tier
    ),
    -- Also count customers who stayed at their tier (from current dim)
    stayed AS (
        SELECT
            customer_tier AS from_tier,
            customer_tier AS to_tier,
            COUNT(*) AS cnt
        FROM bank.silver.customer_dim
        WHERE is_current = true
          AND account_id NOT IN (SELECT account_id FROM tier_changes)
        GROUP BY customer_tier
    ),
    migrated AS (
        SELECT
            from_tier,
            to_tier,
            COUNT(*) AS cnt
        FROM tier_changes
        GROUP BY from_tier, to_tier
    ),
    combined AS (
        SELECT from_tier, to_tier, cnt FROM stayed
        UNION ALL
        SELECT from_tier, to_tier, cnt FROM migrated
    )
    SELECT
        c.from_tier,
        c.to_tier,
        SUM(c.cnt) AS migration_count,
        '2024-Q1' AS period,
        COALESCE(AVG(da.current_balance), 0) AS avg_balance
    FROM combined c
    LEFT JOIN (
        SELECT customer_tier, current_balance
        FROM bank.bronze.raw_accounts
        WHERE ingested_at = (SELECT MAX(ingested_at) FROM bank.bronze.raw_accounts)
    ) da ON c.to_tier = da.customer_tier
    GROUP BY c.from_tier, c.to_tier
) AS source
ON target.from_tier = source.from_tier AND target.to_tier = source.to_tier AND target.period = source.period
WHEN MATCHED THEN UPDATE SET
    migration_count = source.migration_count,
    avg_balance     = source.avg_balance,
    loaded_at       = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    from_tier, to_tier, migration_count, period, avg_balance, loaded_at
) VALUES (
    source.from_tier, source.to_tier, source.migration_count, source.period,
    source.avg_balance, CURRENT_TIMESTAMP
);

-- ===================== optimize_and_vacuum =====================

OPTIMIZE bank.gold.fact_transactions ZORDER BY (fraud_score);
OPTIMIZE bank.gold.dim_account;
OPTIMIZE bank.gold.dim_merchant;
OPTIMIZE bank.gold.dim_date;
OPTIMIZE bank.gold.kpi_daily_volumes;
OPTIMIZE bank.gold.kpi_customer_health;
OPTIMIZE bank.silver.transactions_enriched;
