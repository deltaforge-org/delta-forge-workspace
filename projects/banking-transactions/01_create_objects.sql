-- =============================================================================
-- Banking Transactions Pipeline - Object Creation & Bronze Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw transaction data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Enriched transaction data with fraud scores';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Transaction analytics star schema';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_accounts (
    account_id          STRING      NOT NULL,
    account_number      STRING      NOT NULL,
    account_type        STRING      NOT NULL,
    customer_name       STRING,
    branch              STRING,
    open_date           DATE,
    status              STRING,
    current_balance     DECIMAL(14,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/txn/raw_accounts';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_accounts TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_merchants (
    merchant_id         STRING      NOT NULL,
    merchant_name       STRING      NOT NULL,
    category            STRING,
    city                STRING,
    country             STRING,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/txn/raw_merchants';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_merchants TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_transactions (
    transaction_id      STRING      NOT NULL,
    account_id          STRING      NOT NULL,
    merchant_id         STRING,
    transaction_date    TIMESTAMP   NOT NULL,
    amount              DECIMAL(12,2) NOT NULL,
    transaction_type    STRING      NOT NULL,
    channel             STRING,
    is_suspicious       BOOLEAN,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/bronze/txn/raw_transactions';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_transactions TO USER {{current_user}};

-- ===================== BRONZE SEED: ACCOUNTS (10 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_accounts VALUES
    ('ACC001', '4521-0001-8834-2210', 'checking', 'Alice Whitmore', 'Downtown', '2021-03-15', 'active', 12450.75, '2024-01-15T00:00:00'),
    ('ACC002', '4521-0002-7712-3301', 'checking', 'Brian Kowalski', 'Midtown', '2020-08-22', 'active', 8920.30, '2024-01-15T00:00:00'),
    ('ACC003', '4521-0003-6698-4402', 'savings', 'Catherine Zhao', 'Westside', '2019-11-10', 'active', 45600.00, '2024-01-15T00:00:00'),
    ('ACC004', '4521-0004-5543-5503', 'checking', 'Daniel Okonkwo', 'Eastside', '2022-01-05', 'active', 3210.45, '2024-01-15T00:00:00'),
    ('ACC005', '4521-0005-4421-6604', 'savings', 'Elena Petrova', 'Downtown', '2020-06-18', 'active', 78200.00, '2024-01-15T00:00:00'),
    ('ACC006', '4521-0006-3309-7705', 'checking', 'Frank Nakamura', 'Northside', '2021-09-30', 'active', 15780.60, '2024-01-15T00:00:00'),
    ('ACC007', '4521-0007-2287-8806', 'business', 'Grace Sullivan', 'Midtown', '2018-04-12', 'active', 124500.00, '2024-01-15T00:00:00'),
    ('ACC008', '4521-0008-1165-9907', 'checking', 'Hassan Al-Rashid', 'Southside', '2023-02-28', 'active', 6890.15, '2024-01-15T00:00:00'),
    ('ACC009', '4521-0009-0043-1108', 'savings', 'Irene Johansson', 'Westside', '2021-12-01', 'active', 32100.80, '2024-01-15T00:00:00'),
    ('ACC010', '4521-0010-9921-2209', 'business', 'James Fernandez', 'Downtown', '2019-07-15', 'dormant', 890.00, '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 10
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_accounts;


-- ===================== BRONZE SEED: MERCHANTS (15 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_merchants VALUES
    ('M001', 'FreshMart Groceries', 'groceries', 'New York', 'US', '2024-01-15T00:00:00'),
    ('M002', 'Bistro Roma', 'dining', 'New York', 'US', '2024-01-15T00:00:00'),
    ('M003', 'JetBlue Airways', 'travel', 'Boston', 'US', '2024-01-15T00:00:00'),
    ('M004', 'Shell Gas Station', 'fuel', 'Chicago', 'US', '2024-01-15T00:00:00'),
    ('M005', 'Amazon.com', 'online', 'Seattle', 'US', '2024-01-15T00:00:00'),
    ('M006', 'Walgreens Pharmacy', 'pharmacy', 'New York', 'US', '2024-01-15T00:00:00'),
    ('M007', 'Netflix', 'subscription', 'Los Angeles', 'US', '2024-01-15T00:00:00'),
    ('M008', 'Whole Foods Market', 'groceries', 'San Francisco', 'US', '2024-01-15T00:00:00'),
    ('M009', 'Delta Airlines', 'travel', 'Atlanta', 'US', '2024-01-15T00:00:00'),
    ('M010', 'Uber Eats', 'dining', 'San Francisco', 'US', '2024-01-15T00:00:00'),
    ('M011', 'BP Gas Station', 'fuel', 'Houston', 'US', '2024-01-15T00:00:00'),
    ('M012', 'Best Buy Electronics', 'electronics', 'Minneapolis', 'US', '2024-01-15T00:00:00'),
    ('M013', 'Marriott Hotels', 'travel', 'Miami', 'US', '2024-01-15T00:00:00'),
    ('M014', 'Crypto Exchange XYZ', 'crypto', 'Offshore', 'KY', '2024-01-15T00:00:00'),
    ('M015', 'Target Retail', 'retail', 'Minneapolis', 'US', '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_merchants;


-- ===================== BRONZE SEED: TRANSACTIONS (65 rows) =====================

INSERT INTO {{zone_prefix}}.bronze.raw_transactions VALUES
    ('TXN00001', 'ACC001', 'M001', '2024-01-02T09:15:00', 87.42, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00002', 'ACC001', 'M002', '2024-01-02T12:30:00', 45.80, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00003', 'ACC001', 'M005', '2024-01-03T14:22:00', 329.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00004', 'ACC002', 'M004', '2024-01-02T07:45:00', 52.10, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00005', 'ACC002', 'M001', '2024-01-02T18:00:00', 124.55, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00006', 'ACC002', 'M007', '2024-01-03T00:01:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00007', 'ACC003', NULL, '2024-01-02T10:00:00', 2500.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
    ('TXN00008', 'ACC003', 'M003', '2024-01-04T06:30:00', 489.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00009', 'ACC004', 'M001', '2024-01-02T11:20:00', 63.25, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00010', 'ACC004', 'M014', '2024-01-02T23:55:00', 4500.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
    ('TXN00011', 'ACC004', 'M014', '2024-01-03T00:05:00', 3200.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
    ('TXN00012', 'ACC005', 'M009', '2024-01-05T08:15:00', 1245.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00013', 'ACC005', 'M013', '2024-01-05T16:00:00', 892.50, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00014', 'ACC006', 'M008', '2024-01-03T09:30:00', 156.78, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00015', 'ACC006', 'M011', '2024-01-03T17:45:00', 48.30, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00016', 'ACC006', 'M012', '2024-01-04T13:10:00', 1899.99, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00017', 'ACC007', NULL, '2024-01-02T08:00:00', 15000.00, 'credit', 'wire', false, '2024-01-15T00:00:00'),
    ('TXN00018', 'ACC007', 'M005', '2024-01-03T10:00:00', 2340.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00019', 'ACC007', 'M015', '2024-01-04T15:30:00', 567.89, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00020', 'ACC008', 'M002', '2024-01-02T19:00:00', 78.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00021', 'ACC008', 'M006', '2024-01-03T10:45:00', 34.21, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00022', 'ACC009', 'M001', '2024-01-02T16:30:00', 201.33, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00023', 'ACC009', 'M010', '2024-01-03T12:15:00', 42.75, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00024', 'ACC010', 'M004', '2024-01-02T08:30:00', 35.00, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00025', 'ACC001', 'M004', '2024-01-05T07:20:00', 41.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00026', 'ACC001', 'M010', '2024-01-05T20:30:00', 28.90, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00027', 'ACC002', 'M012', '2024-01-06T11:00:00', 749.99, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00028', 'ACC003', 'M008', '2024-01-06T09:45:00', 178.34, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00029', 'ACC004', 'M001', '2024-01-06T10:10:00', 55.60, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00030', 'ACC005', NULL, '2024-01-07T09:00:00', 5000.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
    ('TXN00031', 'ACC006', 'M002', '2024-01-07T13:00:00', 92.40, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00032', 'ACC007', 'M003', '2024-01-07T07:00:00', 3450.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00033', 'ACC008', 'M005', '2024-01-07T22:15:00', 189.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00034', 'ACC009', 'M006', '2024-01-08T14:30:00', 22.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00035', 'ACC001', NULL, '2024-01-08T08:00:00', 3200.00, 'credit', 'deposit', false, '2024-01-15T00:00:00'),
    ('TXN00036', 'ACC002', 'M010', '2024-01-08T19:45:00', 37.80, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00037', 'ACC004', 'M014', '2024-01-08T01:30:00', 2800.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
    ('TXN00038', 'ACC003', 'M009', '2024-01-09T06:00:00', 678.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00039', 'ACC006', 'M001', '2024-01-09T10:30:00', 112.45, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00040', 'ACC007', NULL, '2024-01-09T08:00:00', 8500.00, 'credit', 'wire', false, '2024-01-15T00:00:00'),
    ('TXN00041', 'ACC008', 'M004', '2024-01-09T07:15:00', 44.20, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00042', 'ACC005', 'M012', '2024-01-10T14:00:00', 2199.00, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00043', 'ACC001', 'M002', '2024-01-10T12:45:00', 68.90, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00044', 'ACC009', 'M015', '2024-01-10T16:20:00', 234.56, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00045', 'ACC002', 'M001', '2024-01-10T17:30:00', 98.72, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00046', 'ACC003', 'M005', '2024-01-11T10:00:00', 459.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00047', 'ACC004', 'M002', '2024-01-11T13:00:00', 56.30, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00048', 'ACC006', 'M007', '2024-01-11T00:02:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00049', 'ACC007', 'M013', '2024-01-11T17:00:00', 1567.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00050', 'ACC008', 'M001', '2024-01-12T09:30:00', 76.88, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00051', 'ACC010', 'M014', '2024-01-12T02:00:00', 850.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
    ('TXN00052', 'ACC005', 'M003', '2024-01-12T06:45:00', 1890.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00053', 'ACC001', 'M008', '2024-01-12T11:00:00', 143.67, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00054', 'ACC009', NULL, '2024-01-12T08:00:00', 1500.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
    ('TXN00055', 'ACC002', 'M004', '2024-01-13T07:00:00', 61.40, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00056', 'ACC003', 'M010', '2024-01-13T20:00:00', 55.25, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00057', 'ACC006', 'M015', '2024-01-13T15:00:00', 312.45, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00058', 'ACC007', 'M005', '2024-01-13T09:30:00', 1245.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00059', 'ACC004', 'M006', '2024-01-13T11:15:00', 18.75, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00060', 'ACC008', 'M002', '2024-01-14T19:30:00', 95.20, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00061', 'ACC001', 'M007', '2024-01-14T00:03:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00062', 'ACC005', 'M008', '2024-01-14T10:20:00', 267.34, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00063', 'ACC009', 'M002', '2024-01-14T13:00:00', 88.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
    ('TXN00064', 'ACC010', 'M005', '2024-01-14T16:00:00', 29.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
    ('TXN00065', 'ACC002', 'M009', '2024-01-14T07:30:00', 2100.00, 'debit', 'online', false, '2024-01-15T00:00:00');

ASSERT ROW_COUNT = 65
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_transactions;


-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.transactions_enriched (
    transaction_id      STRING          NOT NULL,
    account_id          STRING          NOT NULL,
    merchant_id         STRING,
    merchant_category   STRING,
    transaction_date    TIMESTAMP       NOT NULL,
    amount              DECIMAL(12,2)   NOT NULL CHECK (amount > 0),
    transaction_type    STRING          NOT NULL,
    channel             STRING,
    running_balance     DECIMAL(14,2),
    fraud_score         DECIMAL(5,2)    CHECK (fraud_score >= 0 AND fraud_score <= 100),
    is_suspicious       BOOLEAN,
    prev_txn_amount     DECIMAL(12,2),
    time_since_prev_sec BIGINT,
    ingested_at         TIMESTAMP       NOT NULL,
    processed_at        TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/silver/txn/transactions_enriched';

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.transactions_enriched TO USER {{current_user}};

-- CDF-enabled accounts table for balance change tracking
CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.accounts_cdf (
    account_id          STRING          NOT NULL,
    account_number      STRING          NOT NULL,
    account_type        STRING          NOT NULL,
    customer_name       STRING,
    branch              STRING,
    open_date           DATE,
    status              STRING,
    current_balance     DECIMAL(14,2),
    last_txn_date       TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/silver/txn/accounts_cdf'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.accounts_cdf TO USER {{current_user}};

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_account (
    account_key         INT             NOT NULL,
    account_id          STRING          NOT NULL,
    account_number      STRING          NOT NULL,
    account_type        STRING          NOT NULL,
    customer_name       STRING,
    branch              STRING,
    open_date           DATE,
    status              STRING,
    loaded_at           TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/gold/txn/dim_account';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_account TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_merchant (
    merchant_key        INT             NOT NULL,
    merchant_id         STRING          NOT NULL,
    merchant_name       STRING          NOT NULL,
    category            STRING,
    city                STRING,
    country             STRING,
    loaded_at           TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/gold/txn/dim_merchant';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_merchant TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_transactions (
    transaction_key     INT             NOT NULL,
    account_key         INT             NOT NULL,
    merchant_key        INT,
    transaction_date    TIMESTAMP       NOT NULL,
    amount              DECIMAL(12,2)   NOT NULL,
    transaction_type    STRING          NOT NULL,
    running_balance     DECIMAL(14,2),
    fraud_score         DECIMAL(5,2),
    channel             STRING,
    loaded_at           TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/gold/txn/fact_transactions';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_transactions TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_daily_volumes (
    transaction_date    DATE            NOT NULL,
    total_txns          INT             NOT NULL,
    total_amount        DECIMAL(14,2),
    avg_amount          DECIMAL(12,2),
    fraud_flagged_count INT,
    fraud_pct           DECIMAL(5,2),
    running_7d_avg_txns DECIMAL(10,2),
    loaded_at           TIMESTAMP       NOT NULL
) LOCATION '{{data_path}}/gold/txn/kpi_daily_volumes';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_daily_volumes TO USER {{current_user}};

-- ===================== PSEUDONYMISATION RULES =====================

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.accounts_cdf (account_number) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.silver.accounts_cdf (customer_name) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_account (account_number) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');

CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.gold.dim_account (customer_name) TRANSFORM keyed_hash PARAMS ('algorithm' = 'SHA256');
