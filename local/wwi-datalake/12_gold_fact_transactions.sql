-- ============================================================================
-- Gold Facts: Financial Transactions
-- ============================================================================
-- Customer + supplier transactions with settlement metrics.
-- ============================================================================

PIPELINE wwi_lake.gold_fact_transactions
    DESCRIPTION 'WWI gold - customer and supplier transaction facts'
    SCHEDULE 'wwi_lake_daily'
    TAGS 'wwi', 'medallion', 'gold', 'fact'
    FAIL_FAST true
    LIFECYCLE PRODUCTION;

-- fact_customer_transaction

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_customer_transaction (
    customer_transaction_id INT NOT NULL, customer_key INT NOT NULL,
    transaction_type_key INT NOT NULL, payment_method_key INT, invoice_id INT,
    transaction_date_key INT NOT NULL, finalization_date_key INT,
    amount_excluding_tax DECIMAL(18,2) NOT NULL, tax_amount DECIMAL(18,2) NOT NULL,
    transaction_amount DECIMAL(18,2) NOT NULL, outstanding_balance DECIMAL(18,2) NOT NULL,
    is_finalized BOOLEAN, days_to_finalize INT
) LOCATION 'wwi/gold/fact_customer_transaction';

MERGE INTO wwi_lake.gold.fact_customer_transaction AS tgt
USING (
    SELECT customer_transaction_id, customer_key, transaction_type_key,
        payment_method_key, invoice_id, transaction_date_key, finalization_date_key,
        amount_excluding_tax, tax_amount, transaction_amount, outstanding_balance,
        is_finalized, days_to_finalize
    FROM wwi_lake.silver.v_customer_transaction
) AS src ON tgt.customer_transaction_id = src.customer_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- fact_supplier_transaction

CREATE DELTA TABLE IF NOT EXISTS wwi_lake.gold.fact_supplier_transaction (
    supplier_transaction_id INT NOT NULL, supplier_key INT NOT NULL,
    transaction_type_key INT NOT NULL, payment_method_key INT, purchase_order_id INT,
    transaction_date_key INT NOT NULL, finalization_date_key INT,
    amount_excluding_tax DECIMAL(18,2) NOT NULL, tax_amount DECIMAL(18,2) NOT NULL,
    transaction_amount DECIMAL(18,2) NOT NULL, outstanding_balance DECIMAL(18,2) NOT NULL,
    is_finalized BOOLEAN, days_to_finalize INT
) LOCATION 'wwi/gold/fact_supplier_transaction';

MERGE INTO wwi_lake.gold.fact_supplier_transaction AS tgt
USING (
    SELECT supplier_transaction_id, supplier_key, transaction_type_key,
        payment_method_key, purchase_order_id, transaction_date_key, finalization_date_key,
        amount_excluding_tax, tax_amount, transaction_amount, outstanding_balance,
        is_finalized, days_to_finalize
    FROM wwi_lake.silver.v_supplier_transaction
) AS src ON tgt.supplier_transaction_id = src.supplier_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
