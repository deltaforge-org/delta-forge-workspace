-- ============================================================================
-- WWI Data Lake - Gold Facts: Financial Transactions
-- ============================================================================
-- Customer and supplier transaction facts from silver views.
-- Both carry settlement metrics (days_to_finalize) and the is_finalized
-- flag derived from finalization_date presence.
--
-- Transactions can update (outstanding_balance changes, finalization_date
-- gets set), so MERGE handles inserts + updates.
-- ============================================================================

-- Customer Transactions

MERGE INTO wwi_lake.gold.fact_customer_transaction AS tgt
USING (
    SELECT
        customer_transaction_id,
        customer_key,
        transaction_type_key,
        payment_method_key,
        invoice_id,
        transaction_date_key,
        finalization_date_key,
        amount_excluding_tax,
        tax_amount,
        transaction_amount,
        outstanding_balance,
        is_finalized,
        days_to_finalize
    FROM wwi_lake.silver.v_customer_transaction
) AS src ON tgt.customer_transaction_id = src.customer_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Supplier Transactions

MERGE INTO wwi_lake.gold.fact_supplier_transaction AS tgt
USING (
    SELECT
        supplier_transaction_id,
        supplier_key,
        transaction_type_key,
        payment_method_key,
        purchase_order_id,
        transaction_date_key,
        finalization_date_key,
        amount_excluding_tax,
        tax_amount,
        transaction_amount,
        outstanding_balance,
        is_finalized,
        days_to_finalize
    FROM wwi_lake.silver.v_supplier_transaction
) AS src ON tgt.supplier_transaction_id = src.supplier_transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
