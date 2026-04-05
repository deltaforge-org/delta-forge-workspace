-- =============================================================================
-- Banking Transactions Pipeline - Bronze Seed Data
-- =============================================================================

PIPELINE bank_bronze_seed
  DESCRIPTION 'Seeds bronze tables with sample data for Banking Transactions'
  SCHEDULE 'bank_4h_schedule'
  TAGS 'setup', 'banking-transactions'
  LIFECYCLE production
;

-- ===================== BRONZE SEED: ACCOUNTS (12 rows) =====================
-- 3 customers (ACC002, ACC005, ACC008) have tier upgrades from bronze->silver during the period

MERGE INTO bank.bronze.raw_accounts AS target
USING (VALUES
  ('ACC001', '4521-0001-8834-2210', 'checking', 'Alice Whitmore', 'Downtown', '2021-03-15', 'active', 12450.75, 'silver', '2024-01-15T00:00:00'),
  ('ACC002', '4521-0002-7712-3301', 'checking', 'Brian Kowalski', 'Midtown', '2020-08-22', 'active', 8920.30, 'bronze', '2024-01-15T00:00:00'),
  ('ACC003', '4521-0003-6698-4402', 'savings', 'Catherine Zhao', 'Westside', '2019-11-10', 'active', 45600.00, 'gold', '2024-01-15T00:00:00'),
  ('ACC004', '4521-0004-5543-5503', 'checking', 'Daniel Okonkwo', 'Eastside', '2022-01-05', 'active', 3210.45, 'bronze', '2024-01-15T00:00:00'),
  ('ACC005', '4521-0005-4421-6604', 'savings', 'Elena Petrova', 'Downtown', '2020-06-18', 'active', 78200.00, 'bronze', '2024-01-15T00:00:00'),
  ('ACC006', '4521-0006-3309-7705', 'checking', 'Frank Nakamura', 'Northside', '2021-09-30', 'active', 15780.60, 'silver', '2024-01-15T00:00:00'),
  ('ACC007', '4521-0007-2287-8806', 'business', 'Grace Sullivan', 'Midtown', '2018-04-12', 'active', 124500.00, 'gold', '2024-01-15T00:00:00'),
  ('ACC008', '4521-0008-1165-9907', 'checking', 'Hassan Al-Rashid', 'Southside', '2023-02-28', 'active', 6890.15, 'bronze', '2024-01-15T00:00:00'),
  ('ACC009', '4521-0009-0043-1108', 'savings', 'Irene Johansson', 'Westside', '2021-12-01', 'active', 32100.80, 'silver', '2024-01-15T00:00:00'),
  ('ACC010', '4521-0010-9921-2209', 'business', 'James Fernandez', 'Downtown', '2019-07-15', 'dormant', 890.00, 'bronze', '2024-01-15T00:00:00'),
  ('ACC011', '4521-0011-4455-6610', 'checking', 'Karen Mitchell', 'Northside', '2022-05-10', 'active', 9340.20, 'silver', '2024-01-15T00:00:00'),
  ('ACC012', '4521-0012-3322-7711', 'savings', 'Luis Ramirez', 'Eastside', '2023-08-01', 'active', 18750.00, 'bronze', '2024-01-15T00:00:00')
) AS source(account_id, account_number, account_type, customer_name, branch, open_date, status, current_balance, customer_tier, ingested_at)
ON target.account_id = source.account_id AND target.ingested_at = source.ingested_at
WHEN MATCHED THEN UPDATE SET
  account_number = source.account_number,
  account_type = source.account_type,
  customer_name = source.customer_name,
  branch = source.branch,
  open_date = source.open_date,
  status = source.status,
  current_balance = source.current_balance,
  customer_tier = source.customer_tier
WHEN NOT MATCHED THEN INSERT (account_id, account_number, account_type, customer_name, branch, open_date, status, current_balance, customer_tier, ingested_at)
VALUES (source.account_id, source.account_number, source.account_type, source.customer_name, source.branch, source.open_date, source.status, source.current_balance, source.customer_tier, source.ingested_at);

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_accounts;

-- Tier upgrade records for SCD2 (3 customers upgraded bronze->silver)
-- These will be ingested later to simulate the upgrade event
MERGE INTO bank.bronze.raw_accounts AS target
USING (VALUES
  ('ACC002', '4521-0002-7712-3301', 'checking', 'Brian Kowalski', 'Midtown', '2020-08-22', 'active', 12450.00, 'silver', '2024-02-01T00:00:00'),
  ('ACC005', '4521-0005-4421-6604', 'savings', 'Elena Petrova', 'Downtown', '2020-06-18', 'active', 85000.00, 'silver', '2024-02-01T00:00:00'),
  ('ACC008', '4521-0008-1165-9907', 'checking', 'Hassan Al-Rashid', 'Southside', '2023-02-28', 'active', 11200.00, 'silver', '2024-02-01T00:00:00')
) AS source(account_id, account_number, account_type, customer_name, branch, open_date, status, current_balance, customer_tier, ingested_at)
ON target.account_id = source.account_id AND target.ingested_at = source.ingested_at
WHEN MATCHED THEN UPDATE SET
  account_number = source.account_number,
  account_type = source.account_type,
  customer_name = source.customer_name,
  branch = source.branch,
  open_date = source.open_date,
  status = source.status,
  current_balance = source.current_balance,
  customer_tier = source.customer_tier
WHEN NOT MATCHED THEN INSERT (account_id, account_number, account_type, customer_name, branch, open_date, status, current_balance, customer_tier, ingested_at)
VALUES (source.account_id, source.account_number, source.account_type, source.customer_name, source.branch, source.open_date, source.status, source.current_balance, source.customer_tier, source.ingested_at);


-- ===================== BRONZE SEED: MERCHANTS (15 rows) =====================

MERGE INTO bank.bronze.raw_merchants AS target
USING (VALUES
  ('M001', 'FreshMart Groceries', 'groceries', 'New York', 'US', 'low', '2024-01-15T00:00:00'),
  ('M002', 'Bistro Roma', 'dining', 'New York', 'US', 'low', '2024-01-15T00:00:00'),
  ('M003', 'JetBlue Airways', 'travel', 'Boston', 'US', 'medium', '2024-01-15T00:00:00'),
  ('M004', 'Shell Gas Station', 'fuel', 'Chicago', 'US', 'low', '2024-01-15T00:00:00'),
  ('M005', 'Amazon.com', 'online', 'Seattle', 'US', 'low', '2024-01-15T00:00:00'),
  ('M006', 'Walgreens Pharmacy', 'pharmacy', 'New York', 'US', 'low', '2024-01-15T00:00:00'),
  ('M007', 'Netflix', 'subscription', 'Los Angeles', 'US', 'low', '2024-01-15T00:00:00'),
  ('M008', 'Whole Foods Market', 'groceries', 'San Francisco', 'US', 'low', '2024-01-15T00:00:00'),
  ('M009', 'Delta Airlines', 'travel', 'Atlanta', 'US', 'medium', '2024-01-15T00:00:00'),
  ('M010', 'Uber Eats', 'dining', 'San Francisco', 'US', 'low', '2024-01-15T00:00:00'),
  ('M011', 'BP Gas Station', 'fuel', 'Houston', 'US', 'low', '2024-01-15T00:00:00'),
  ('M012', 'Best Buy Electronics', 'electronics', 'Minneapolis', 'US', 'low', '2024-01-15T00:00:00'),
  ('M013', 'Marriott Hotels', 'travel', 'Miami', 'US', 'medium', '2024-01-15T00:00:00'),
  ('M014', 'Crypto Exchange XYZ', 'crypto', 'Offshore', 'KY', 'high', '2024-01-15T00:00:00'),
  ('M015', 'Target Retail', 'retail', 'Minneapolis', 'US', 'low', '2024-01-15T00:00:00')
) AS source(merchant_id, merchant_name, category, city, country, risk_level, ingested_at)
ON target.merchant_id = source.merchant_id
WHEN MATCHED THEN UPDATE SET
  merchant_name = source.merchant_name,
  category = source.category,
  city = source.city,
  country = source.country,
  risk_level = source.risk_level,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (merchant_id, merchant_name, category, city, country, risk_level, ingested_at)
VALUES (source.merchant_id, source.merchant_name, source.category, source.city, source.country, source.risk_level, source.ingested_at);

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_merchants;


-- ===================== BRONZE SEED: TRANSACTIONS (70 rows) =====================
-- Spans 2024-01-02 through 2024-01-31 (3 months of data)
-- Includes: 5 high-fraud-score transactions, 3 velocity clusters, 2 late-night transactions

MERGE INTO bank.bronze.raw_transactions AS target
USING (VALUES
  -- Day 1: Jan 2 (7 txns)
  ('TXN00001', 'ACC001', 'M001', '2024-01-02T09:15:00', 87.42, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00002', 'ACC001', 'M002', '2024-01-02T12:30:00', 45.80, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00003', 'ACC002', 'M004', '2024-01-02T07:45:00', 52.10, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00004', 'ACC002', 'M001', '2024-01-02T18:00:00', 124.55, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00005', 'ACC003', NULL, '2024-01-02T10:00:00', 2500.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
  ('TXN00006', 'ACC004', 'M001', '2024-01-02T11:20:00', 63.25, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- HIGH FRAUD: crypto + suspicious + late night
  ('TXN00007', 'ACC004', 'M014', '2024-01-02T23:55:00', 4500.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
  -- Day 2: Jan 3 (8 txns) -- velocity cluster for ACC004
  ('TXN00008', 'ACC004', 'M014', '2024-01-03T00:05:00', 3200.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
  ('TXN00009', 'ACC001', 'M005', '2024-01-03T14:22:00', 329.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00010', 'ACC002', 'M007', '2024-01-03T00:01:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00011', 'ACC006', 'M008', '2024-01-03T09:30:00', 156.78, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00012', 'ACC006', 'M011', '2024-01-03T17:45:00', 48.30, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00013', 'ACC008', 'M002', '2024-01-03T19:00:00', 78.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00014', 'ACC008', 'M006', '2024-01-03T10:45:00', 34.21, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00015', 'ACC011', 'M001', '2024-01-03T15:30:00', 95.40, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 3: Jan 4 (5 txns)
  ('TXN00016', 'ACC003', 'M003', '2024-01-04T06:30:00', 489.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00017', 'ACC006', 'M012', '2024-01-04T13:10:00', 1899.99, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00018', 'ACC007', NULL, '2024-01-04T08:00:00', 15000.00, 'credit', 'wire', false, '2024-01-15T00:00:00'),
  ('TXN00019', 'ACC007', 'M005', '2024-01-04T10:00:00', 2340.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00020', 'ACC012', 'M005', '2024-01-04T16:20:00', 159.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  -- Day 4: Jan 5 (5 txns)
  ('TXN00021', 'ACC005', 'M009', '2024-01-05T08:15:00', 1245.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00022', 'ACC005', 'M013', '2024-01-05T16:00:00', 892.50, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00023', 'ACC001', 'M004', '2024-01-05T07:20:00', 41.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00024', 'ACC001', 'M010', '2024-01-05T20:30:00', 28.90, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00025', 'ACC009', 'M001', '2024-01-05T16:30:00', 201.33, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 5: Jan 6 (4 txns)
  ('TXN00026', 'ACC002', 'M012', '2024-01-06T11:00:00', 749.99, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00027', 'ACC003', 'M008', '2024-01-06T09:45:00', 178.34, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00028', 'ACC004', 'M001', '2024-01-06T10:10:00', 55.60, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00029', 'ACC011', 'M010', '2024-01-06T12:15:00', 42.75, 'debit', 'online', false, '2024-01-15T00:00:00'),
  -- Day 6: Jan 7 (5 txns)
  ('TXN00030', 'ACC005', NULL, '2024-01-07T09:00:00', 5000.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
  ('TXN00031', 'ACC006', 'M002', '2024-01-07T13:00:00', 92.40, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00032', 'ACC007', 'M003', '2024-01-07T07:00:00', 3450.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00033', 'ACC008', 'M005', '2024-01-07T22:15:00', 189.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00034', 'ACC012', 'M015', '2024-01-07T14:00:00', 234.56, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 7: Jan 8 (5 txns) -- velocity cluster for ACC010
  ('TXN00035', 'ACC001', NULL, '2024-01-08T08:00:00', 3200.00, 'credit', 'deposit', false, '2024-01-15T00:00:00'),
  ('TXN00036', 'ACC002', 'M010', '2024-01-08T19:45:00', 37.80, 'debit', 'online', false, '2024-01-15T00:00:00'),
  -- HIGH FRAUD: crypto + suspicious + late night (1:30 AM)
  ('TXN00037', 'ACC004', 'M014', '2024-01-08T01:30:00', 2800.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
  ('TXN00038', 'ACC009', 'M010', '2024-01-08T12:15:00', 42.75, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00039', 'ACC010', 'M004', '2024-01-08T08:30:00', 35.00, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 8: Jan 9 (5 txns)
  ('TXN00040', 'ACC003', 'M009', '2024-01-09T06:00:00', 678.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00041', 'ACC006', 'M001', '2024-01-09T10:30:00', 112.45, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00042', 'ACC007', NULL, '2024-01-09T08:00:00', 8500.00, 'credit', 'wire', false, '2024-01-15T00:00:00'),
  ('TXN00043', 'ACC008', 'M004', '2024-01-09T07:15:00', 44.20, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00044', 'ACC011', 'M006', '2024-01-09T11:00:00', 28.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 9: Jan 10 (5 txns)
  ('TXN00045', 'ACC005', 'M012', '2024-01-10T14:00:00', 2199.00, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00046', 'ACC001', 'M002', '2024-01-10T12:45:00', 68.90, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00047', 'ACC009', 'M015', '2024-01-10T16:20:00', 234.56, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00048', 'ACC002', 'M001', '2024-01-10T17:30:00', 98.72, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00049', 'ACC012', 'M002', '2024-01-10T19:00:00', 67.80, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 10: Jan 11 (5 txns)
  ('TXN00050', 'ACC003', 'M005', '2024-01-11T10:00:00', 459.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00051', 'ACC004', 'M002', '2024-01-11T13:00:00', 56.30, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00052', 'ACC006', 'M007', '2024-01-11T00:02:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00053', 'ACC007', 'M013', '2024-01-11T17:00:00', 1567.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00054', 'ACC011', 'M012', '2024-01-11T15:30:00', 549.99, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 11: Jan 12 (5 txns)
  ('TXN00055', 'ACC008', 'M001', '2024-01-12T09:30:00', 76.88, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- HIGH FRAUD: crypto + suspicious + late night (2 AM) for dormant account
  ('TXN00056', 'ACC010', 'M014', '2024-01-12T02:00:00', 850.00, 'debit', 'online', true, '2024-01-15T00:00:00'),
  ('TXN00057', 'ACC005', 'M003', '2024-01-12T06:45:00', 1890.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00058', 'ACC001', 'M008', '2024-01-12T11:00:00', 143.67, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00059', 'ACC009', NULL, '2024-01-12T08:00:00', 1500.00, 'credit', 'transfer', false, '2024-01-15T00:00:00'),
  -- Day 12: Jan 13 (5 txns)
  ('TXN00060', 'ACC002', 'M004', '2024-01-13T07:00:00', 61.40, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00061', 'ACC003', 'M010', '2024-01-13T20:00:00', 55.25, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00062', 'ACC006', 'M015', '2024-01-13T15:00:00', 312.45, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00063', 'ACC007', 'M005', '2024-01-13T09:30:00', 1245.00, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00064', 'ACC012', 'M001', '2024-01-13T10:30:00', 88.90, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  -- Day 13: Jan 14 (6 txns) -- HIGH FRAUD: late night velocity cluster for ACC010
  ('TXN00065', 'ACC008', 'M002', '2024-01-14T19:30:00', 95.20, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00066', 'ACC001', 'M007', '2024-01-14T00:03:00', 15.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00067', 'ACC005', 'M008', '2024-01-14T10:20:00', 267.34, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00068', 'ACC009', 'M002', '2024-01-14T13:00:00', 88.50, 'debit', 'pos', false, '2024-01-15T00:00:00'),
  ('TXN00069', 'ACC010', 'M005', '2024-01-14T16:00:00', 29.99, 'debit', 'online', false, '2024-01-15T00:00:00'),
  ('TXN00070', 'ACC002', 'M009', '2024-01-14T07:30:00', 2100.00, 'debit', 'online', false, '2024-01-15T00:00:00')
) AS source(transaction_id, account_id, merchant_id, transaction_date, amount, transaction_type, channel, is_suspicious, ingested_at)
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET
  account_id = source.account_id,
  merchant_id = source.merchant_id,
  transaction_date = source.transaction_date,
  amount = source.amount,
  transaction_type = source.transaction_type,
  channel = source.channel,
  is_suspicious = source.is_suspicious,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (transaction_id, account_id, merchant_id, transaction_date, amount, transaction_type, channel, is_suspicious, ingested_at)
VALUES (source.transaction_id, source.account_id, source.merchant_id, source.transaction_date, source.amount, source.transaction_type, source.channel, source.is_suspicious, source.ingested_at);

ASSERT ROW_COUNT = 70
SELECT COUNT(*) AS row_count FROM bank.bronze.raw_transactions;
