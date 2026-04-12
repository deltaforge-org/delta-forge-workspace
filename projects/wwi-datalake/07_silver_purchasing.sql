-- ============================================================================
-- Silver Purchasing Views
-- ============================================================================
-- Supplier view: denormalizes category, contact, delivery method, geography.
-- Purchase view: PO-line grain with order/received totals, fulfillment %,
--   and supplier lead time in days.
-- Supplier transaction view: settlement metrics.
-- CREATE OR REPLACE VIEW is idempotent - safe to run on every schedule.
-- ============================================================================

PIPELINE wwi_lake.silver_purchasing
    DESCRIPTION 'WWI silver - supplier, purchase, and supplier transaction views'
    SCHEDULE 'wwi_lake_daily'
    TAGS 'wwi', 'medallion', 'silver', 'purchasing'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- Supplier with all lookups resolved

CREATE OR REPLACE VIEW wwi_lake.silver.v_supplier AS
SELECT
    su.supplier_id,
    su.supplier_name,
    sc.supplier_category_name         AS category_name,
    pc.full_name                      AS primary_contact,
    dm.delivery_method_name           AS delivery_method,

    dc.city_name                      AS delivery_city,
    dsp.state_province_name           AS delivery_state_province,
    dco.country_name                  AS delivery_country,

    su.supplier_reference,
    su.payment_days,
    su.phone_number,
    su.website_url
FROM wwi_lake.bronze.suppliers su
LEFT JOIN wwi_lake.bronze.supplier_categories sc
    ON su.supplier_category_id = sc.supplier_category_id
LEFT JOIN wwi_lake.bronze.people pc
    ON su.primary_contact_person_id = pc.person_id
LEFT JOIN wwi_lake.bronze.delivery_methods dm
    ON su.delivery_method_id = dm.delivery_method_id
LEFT JOIN wwi_lake.bronze.cities dc
    ON su.delivery_city_id = dc.city_id
LEFT JOIN wwi_lake.bronze.state_provinces dsp
    ON dc.state_province_id = dsp.state_province_id
LEFT JOIN wwi_lake.bronze.countries dco
    ON dsp.country_id = dco.country_id;

-- Purchase at PO-line grain with fulfillment metrics

CREATE OR REPLACE VIEW wwi_lake.silver.v_purchase AS
SELECT
    pol.purchase_order_line_id,
    pol.purchase_order_id,
    po.supplier_id                    AS supplier_key,
    po.delivery_method_id             AS delivery_method_key,

    CAST(DATE_FORMAT(po.order_date, '%Y%m%d') AS INT)              AS order_date_key,
    CAST(DATE_FORMAT(po.expected_delivery_date, '%Y%m%d') AS INT)  AS expected_delivery_date_key,
    CAST(DATE_FORMAT(pol.last_receipt_date, '%Y%m%d') AS INT)      AS last_receipt_date_key,

    pol.stock_item_id,
    pol.description,
    pol.ordered_outers,
    pol.received_outers,
    pol.expected_unit_price_per_outer,

    -- order and received value
    COALESCE(pol.ordered_outers, 0)
        * COALESCE(pol.expected_unit_price_per_outer, 0)           AS order_total,
    COALESCE(pol.received_outers, 0)
        * COALESCE(pol.expected_unit_price_per_outer, 0)           AS received_total,

    -- fulfillment percentage (received / ordered)
    CASE
        WHEN COALESCE(pol.ordered_outers, 0) > 0
        THEN ROUND(
            CAST(COALESCE(pol.received_outers, 0) AS DECIMAL(18,4))
          / CAST(pol.ordered_outers AS DECIMAL(18,4)) * 100, 2)
    END                               AS fulfillment_pct,

    -- supplier lead time (order to last receipt)
    CASE
        WHEN pol.last_receipt_date IS NOT NULL
        THEN CAST(pol.last_receipt_date - po.order_date AS INT)
    END                               AS days_to_deliver,

    po.is_order_finalized,
    pol.is_order_line_finalized

FROM wwi_lake.bronze.purchase_order_lines pol
INNER JOIN wwi_lake.bronze.purchase_orders po
    ON pol.purchase_order_id = po.purchase_order_id;

-- Supplier transactions with settlement metrics

CREATE OR REPLACE VIEW wwi_lake.silver.v_supplier_transaction AS
SELECT
    st.supplier_transaction_id,
    st.supplier_id                    AS supplier_key,
    st.transaction_type_id            AS transaction_type_key,
    st.payment_method_id              AS payment_method_key,
    st.purchase_order_id,

    CAST(DATE_FORMAT(st.transaction_date, '%Y%m%d') AS INT)  AS transaction_date_key,
    CAST(DATE_FORMAT(st.finalization_date, '%Y%m%d') AS INT) AS finalization_date_key,

    st.amount_excluding_tax,
    st.tax_amount,
    st.transaction_amount,
    st.outstanding_balance,

    st.finalization_date IS NOT NULL  AS is_finalized,

    CASE
        WHEN st.finalization_date IS NOT NULL
        THEN CAST(st.finalization_date - st.transaction_date AS INT)
    END                               AS days_to_finalize

FROM wwi_lake.bronze.supplier_transactions st;
