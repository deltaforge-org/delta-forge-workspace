-- =============================================================================
-- Banking Transactions Pipeline - Full Load Transformation
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE bank_4h_schedule CRON '0 */4 * * *' TIMEZONE 'UTC' RETRIES 3 TIMEOUT 1800 MAX_CONCURRENT 1 ACTIVE;

PIPELINE bank_transaction_pipeline DESCRIPTION 'Every-4-hour banking transaction pipeline with fraud scoring and CDF balance tracking' SCHEDULE 'bank_4h_schedule' TAGS 'banking,transactions,fraud,CDF' SLA 1800 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP: validate_bronze =====================

STEP validate_bronze
  TIMEOUT '2m'
AS
  SELECT COUNT(*) AS raw_txn_count FROM {{zone_prefix}}.bronze.raw_transactions;
  ASSERT VALUE raw_txn_count >= 60

  SELECT COUNT(*) AS raw_acct_count FROM {{zone_prefix}}.bronze.raw_accounts;
  ASSERT VALUE raw_acct_count = 10

  SELECT COUNT(*) AS raw_merch_count FROM {{zone_prefix}}.bronze.raw_merchants;
  ASSERT VALUE raw_merch_count = 15;

-- ===================== STEP: load_silver_accounts =====================

STEP load_silver_accounts
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.silver.accounts_cdf AS target
  USING (
      SELECT
          a.account_id,
          a.account_number,
          a.account_type,
          a.customer_name,
          a.branch,
          a.open_date,
          a.status,
          a.current_balance,
          t.last_txn_date
      FROM {{zone_prefix}}.bronze.raw_accounts a
      LEFT JOIN (
          SELECT account_id, MAX(transaction_date) AS last_txn_date
          FROM {{zone_prefix}}.bronze.raw_transactions
          GROUP BY account_id
      ) t ON a.account_id = t.account_id
  ) AS source
  ON target.account_id = source.account_id
  WHEN MATCHED THEN UPDATE SET
      account_number  = source.account_number,
      account_type    = source.account_type,
      customer_name   = source.customer_name,
      branch          = source.branch,
      status          = source.status,
      current_balance = source.current_balance,
      last_txn_date   = source.last_txn_date,
      updated_at      = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      account_id, account_number, account_type, customer_name, branch,
      open_date, status, current_balance, last_txn_date, updated_at
  ) VALUES (
      source.account_id, source.account_number, source.account_type, source.customer_name,
      source.branch, source.open_date, source.status, source.current_balance,
      source.last_txn_date, CURRENT_TIMESTAMP
  );

-- ===================== STEP: enrich_transactions =====================

STEP enrich_transactions
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.silver.transactions_enriched AS target
  USING (
      WITH base AS (
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
              -- Previous transaction for velocity checks
              LAG(t.amount) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date) AS prev_txn_amount,
              DATEDIFF(
                  t.transaction_date,
                  LAG(t.transaction_date) OVER (PARTITION BY t.account_id ORDER BY t.transaction_date)
              ) * 86400 AS time_since_prev_sec,
              -- Running balance per account
              SUM(CASE WHEN t.transaction_type = 'credit' THEN t.amount ELSE -t.amount END)
                  OVER (PARTITION BY t.account_id ORDER BY t.transaction_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_delta,
              -- Dedup
              ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.ingested_at ASC) AS rn
          FROM {{zone_prefix}}.bronze.raw_transactions t
          LEFT JOIN {{zone_prefix}}.bronze.raw_merchants m ON t.merchant_id = m.merchant_id
      ),
      scored AS (
          SELECT
              b.*,
              a.current_balance + b.balance_delta AS running_balance,
              -- Fraud scoring rules (0-100):
              -- +30 if amount > 2000
              -- +25 if crypto merchant
              -- +20 if velocity < 600 seconds and amount > 500
              -- +15 if suspicious flag set
              -- +10 if transaction after midnight and before 5am
              LEAST(100.0, GREATEST(0.0,
                  CASE WHEN b.amount > 2000 THEN 30.0 ELSE 0.0 END
                  + CASE WHEN b.merchant_category = 'crypto' THEN 25.0 ELSE 0.0 END
                  + CASE WHEN b.time_since_prev_sec < 600 AND b.amount > 500 THEN 20.0 ELSE 0.0 END
                  + CASE WHEN b.is_suspicious = true THEN 15.0 ELSE 0.0 END
                  + CASE WHEN EXTRACT(HOUR FROM b.transaction_date) BETWEEN 0 AND 4 THEN 10.0 ELSE 0.0 END
              )) AS fraud_score
          FROM base b
          JOIN {{zone_prefix}}.bronze.raw_accounts a ON b.account_id = a.account_id
          WHERE b.rn = 1
      )
      SELECT * FROM scored
  ) AS source
  ON target.transaction_id = source.transaction_id
  WHEN MATCHED THEN UPDATE SET
      merchant_category   = source.merchant_category,
      running_balance     = source.running_balance,
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

  SELECT COUNT(*) AS silver_txn_count FROM {{zone_prefix}}.silver.transactions_enriched;
  ASSERT VALUE silver_txn_count = 65;

-- ===================== STEP: build_dim_account =====================

STEP build_dim_account
  DEPENDS ON (load_silver_accounts)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_account AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY account_id) AS account_key,
          account_id,
          account_number,
          account_type,
          customer_name,
          branch,
          open_date,
          status
      FROM {{zone_prefix}}.silver.accounts_cdf
  ) AS source
  ON target.account_id = source.account_id
  WHEN MATCHED THEN UPDATE SET
      account_number = source.account_number,
      account_type   = source.account_type,
      customer_name  = source.customer_name,
      branch         = source.branch,
      status         = source.status,
      loaded_at      = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      account_key, account_id, account_number, account_type, customer_name,
      branch, open_date, status, loaded_at
  ) VALUES (
      source.account_key, source.account_id, source.account_number, source.account_type,
      source.customer_name, source.branch, source.open_date, source.status, CURRENT_TIMESTAMP
  );

-- ===================== STEP: build_dim_merchant =====================

STEP build_dim_merchant
  DEPENDS ON (validate_bronze)
AS
  MERGE INTO {{zone_prefix}}.gold.dim_merchant AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY merchant_id) AS merchant_key,
          merchant_id,
          merchant_name,
          category,
          city,
          country
      FROM {{zone_prefix}}.bronze.raw_merchants
  ) AS source
  ON target.merchant_id = source.merchant_id
  WHEN MATCHED THEN UPDATE SET
      merchant_name = source.merchant_name,
      category      = source.category,
      city          = source.city,
      country       = source.country,
      loaded_at     = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      merchant_key, merchant_id, merchant_name, category, city, country, loaded_at
  ) VALUES (
      source.merchant_key, source.merchant_id, source.merchant_name, source.category,
      source.city, source.country, CURRENT_TIMESTAMP
  );

-- ===================== STEP: build_fact_transactions =====================

STEP build_fact_transactions
  DEPENDS ON (enrich_transactions, build_dim_account, build_dim_merchant)
  TIMEOUT '5m'
AS
  MERGE INTO {{zone_prefix}}.gold.fact_transactions AS target
  USING (
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
      merchant_key     = source.merchant_key,
      transaction_type = source.transaction_type,
      running_balance  = source.running_balance,
      fraud_score      = source.fraud_score,
      channel          = source.channel,
      loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      transaction_key, account_key, merchant_key, transaction_date, amount,
      transaction_type, running_balance, fraud_score, channel, loaded_at
  ) VALUES (
      source.transaction_key, source.account_key, source.merchant_key, source.transaction_date,
      source.amount, source.transaction_type, source.running_balance, source.fraud_score,
      source.channel, CURRENT_TIMESTAMP
  );

-- ===================== STEP: compute_kpi_daily_volumes =====================

STEP compute_kpi_daily_volumes
  DEPENDS ON (build_fact_transactions)
AS
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
          ), 2) AS running_7d_avg_txns
      FROM daily d
  ) AS source
  ON target.transaction_date = source.transaction_date
  WHEN MATCHED THEN UPDATE SET
      total_txns          = source.total_txns,
      total_amount        = source.total_amount,
      avg_amount          = source.avg_amount,
      fraud_flagged_count = source.fraud_flagged_count,
      fraud_pct           = source.fraud_pct,
      running_7d_avg_txns = source.running_7d_avg_txns,
      loaded_at           = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      transaction_date, total_txns, total_amount, avg_amount,
      fraud_flagged_count, fraud_pct, running_7d_avg_txns, loaded_at
  ) VALUES (
      source.transaction_date, source.total_txns, source.total_amount, source.avg_amount,
      source.fraud_flagged_count, source.fraud_pct, source.running_7d_avg_txns, CURRENT_TIMESTAMP
  );

-- ===================== STEP: optimize_and_vacuum =====================

STEP optimize_and_vacuum
  DEPENDS ON (compute_kpi_daily_volumes)
  CONTINUE ON FAILURE
AS
  OPTIMIZE {{zone_prefix}}.gold.dim_account;
  OPTIMIZE {{zone_prefix}}.gold.dim_merchant;
  OPTIMIZE {{zone_prefix}}.gold.fact_transactions;
  OPTIMIZE {{zone_prefix}}.gold.kpi_daily_volumes;
  OPTIMIZE {{zone_prefix}}.silver.transactions_enriched;
