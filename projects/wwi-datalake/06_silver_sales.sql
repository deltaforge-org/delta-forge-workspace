-- ============================================================================
-- Silver Sales Views
-- ============================================================================
-- Customer view: denormalizes category, buying group, contact, delivery
--   method, and geography into a single wide row.
-- Sale view: joins invoice lines with invoice and order headers to produce
--   the invoice-line grain fact with profit margin and fulfillment metrics.
-- CREATE OR REPLACE VIEW is idempotent - safe to run on every schedule.
-- ============================================================================

PIPELINE wwi_lake.silver_sales
    DESCRIPTION 'WWI silver - customer and sale views over bronze'
    TAGS 'wwi', 'medallion', 'silver', 'sales'
    FAIL_FAST true
    STATUS DISABLED
    LIFECYCLE PRODUCTION;

-- Customer with all lookups resolved

CREATE OR REPLACE VIEW wwi_lake.silver.v_customer AS
SELECT
    cu.customer_id,
    cu.customer_name,
    cu.bill_to_customer_id,
    bill.customer_name                AS bill_to_customer_name,
    cc.customer_category_name         AS category_name,
    bg.buying_group_name,
    pc.full_name                      AS primary_contact,
    dm.delivery_method_name           AS delivery_method,

    -- delivery geography
    dc.city_name                      AS delivery_city,
    dsp.state_province_name           AS delivery_state_province,
    dco.country_name                  AS delivery_country,

    -- postal geography
    poc.city_name                     AS postal_city,

    cu.credit_limit,
    cu.account_opened_date,
    cu.standard_discount_percentage   AS standard_discount_pct,
    cu.is_on_credit_hold,
    cu.payment_days,
    cu.phone_number,
    cu.website_url
FROM wwi_lake.bronze.customers cu
LEFT JOIN wwi_lake.bronze.customers bill
    ON cu.bill_to_customer_id = bill.customer_id
LEFT JOIN wwi_lake.bronze.customer_categories cc
    ON cu.customer_category_id = cc.customer_category_id
LEFT JOIN wwi_lake.bronze.buying_groups bg
    ON cu.buying_group_id = bg.buying_group_id
LEFT JOIN wwi_lake.bronze.people pc
    ON cu.primary_contact_person_id = pc.person_id
LEFT JOIN wwi_lake.bronze.delivery_methods dm
    ON cu.delivery_method_id = dm.delivery_method_id
LEFT JOIN wwi_lake.bronze.cities dc
    ON cu.delivery_city_id = dc.city_id
LEFT JOIN wwi_lake.bronze.state_provinces dsp
    ON dc.state_province_id = dsp.state_province_id
LEFT JOIN wwi_lake.bronze.countries dco
    ON dsp.country_id = dco.country_id
LEFT JOIN wwi_lake.bronze.cities poc
    ON cu.postal_city_id = poc.city_id;

-- Sale at invoice-line grain with derived measures

CREATE OR REPLACE VIEW wwi_lake.silver.v_sale AS
SELECT
    il.invoice_line_id,
    il.invoice_id,
    inv.order_id,
    inv.customer_id                   AS customer_key,
    inv.bill_to_customer_id           AS bill_to_customer_key,
    inv.salesperson_person_id         AS salesperson_key,
    inv.delivery_method_id            AS delivery_method_key,
    cust.delivery_city_id             AS delivery_city_key,

    -- date keys (YYYYMMDD)
    CAST(DATE_FORMAT(inv.invoice_date, '%Y%m%d') AS INT) AS invoice_date_key,
    CAST(DATE_FORMAT(ord.order_date, '%Y%m%d') AS INT)   AS order_date_key,

    il.stock_item_id,
    il.description,
    il.quantity,
    il.unit_price,
    il.tax_rate,

    -- financial measures
    il.extended_price                 AS line_amount,
    il.tax_amount,
    il.extended_price + il.tax_amount AS total_including_tax,
    il.line_profit,
    CASE
        WHEN il.extended_price > 0
        THEN ROUND(il.line_profit / il.extended_price * 100, 2)
        ELSE 0
    END                               AS profit_margin_pct,

    inv.is_credit_note,
    COALESCE(ord.is_undersupply_backordered, false) AS is_backorder,

    -- days between order placed and invoice issued
    CASE
        WHEN ord.order_date IS NOT NULL
        THEN CAST(inv.invoice_date - ord.order_date AS INT)
    END                               AS days_to_fulfill,

    inv.total_dry_items,
    inv.total_chiller_items

FROM wwi_lake.bronze.invoice_lines il
INNER JOIN wwi_lake.bronze.invoices inv
    ON il.invoice_id = inv.invoice_id
LEFT JOIN wwi_lake.bronze.orders ord
    ON inv.order_id = ord.order_id
LEFT JOIN wwi_lake.bronze.customers cust
    ON inv.customer_id = cust.customer_id;

-- Customer transactions with settlement metrics

CREATE OR REPLACE VIEW wwi_lake.silver.v_customer_transaction AS
SELECT
    ct.customer_transaction_id,
    ct.customer_id                    AS customer_key,
    ct.transaction_type_id            AS transaction_type_key,
    ct.payment_method_id              AS payment_method_key,
    ct.invoice_id,

    CAST(DATE_FORMAT(ct.transaction_date, '%Y%m%d') AS INT)  AS transaction_date_key,
    CAST(DATE_FORMAT(ct.finalization_date, '%Y%m%d') AS INT) AS finalization_date_key,

    ct.amount_excluding_tax,
    ct.tax_amount,
    ct.transaction_amount,
    ct.outstanding_balance,

    ct.finalization_date IS NOT NULL  AS is_finalized,

    CASE
        WHEN ct.finalization_date IS NOT NULL
        THEN CAST(ct.finalization_date - ct.transaction_date AS INT)
    END                               AS days_to_finalize

FROM wwi_lake.bronze.customer_transactions ct;
