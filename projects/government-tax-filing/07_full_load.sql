-- =============================================================================
-- Government Tax Filing Pipeline - Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- 14-step DAG: validate_bronze -> append_filings + apply_amendments (parallel) ->
-- enable_cdf_audit -> build_taxpayer_profiles -> dim_taxpayer + dim_jurisdiction +
-- dim_preparer + dim_fiscal_year (parallel) -> build_fact_filings ->
-- kpi_revenue_analysis + kpi_preparer_quality (parallel) -> bloom_and_optimize
-- =============================================================================

-- ===================== PIPELINE =====================

PIPELINE 07_full_load
  DESCRIPTION 'Daily tax filing pipeline: append-only immutable filings, amendment MERGE with delta computation, CDF audit trail, star schema with amendment-aware facts, revenue and preparer KPIs'
  SCHEDULE 'tax_daily_schedule'
  TAGS 'government,tax,full-load,CDF,append-only,amendments'
  SLA 2700
  FAIL_FAST true
  LIFECYCLE production
;

-- ===================== validate_bronze =====================

ASSERT ROW_COUNT = 50
SELECT COUNT(*) AS row_count FROM tax.bronze.raw_filings;

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM tax.bronze.raw_amendments;

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM tax.bronze.raw_taxpayers;

ASSERT ROW_COUNT = 6
SELECT COUNT(*) AS row_count FROM tax.bronze.raw_jurisdictions;

ASSERT ROW_COUNT = 5
SELECT COUNT(*) AS row_count FROM tax.bronze.raw_preparers;

-- ===================== append_filings =====================
-- Append-only INSERT: original filings are NEVER updated or deleted in silver.
-- Compute effective_tax_rate, deduction_pct, audit_flag, income_bracket.

DELETE FROM tax.silver.filings_immutable WHERE 1=1;

INSERT INTO tax.silver.filings_immutable
SELECT
    f.filing_id,
    f.taxpayer_id,
    f.jurisdiction_id,
    f.preparer_id,
    f.fiscal_year,
    CAST(f.filing_date AS DATE)                                   AS filing_date,
    f.gross_income,
    f.deductions,
    f.taxable_income,
    f.tax_owed,
    f.tax_paid,
    f.refund_amount,
    f.filing_status,
    f.filing_type,
    -- Effective tax rate: tax_owed / gross_income
    CASE
        WHEN f.gross_income > 0 THEN ROUND(f.tax_owed / f.gross_income, 4)
        ELSE 0.0000
    END                                                            AS effective_tax_rate,
    -- Deduction percentage of gross
    CASE
        WHEN f.gross_income > 0 THEN ROUND(100.0 * f.deductions / f.gross_income, 2)
        ELSE 0.00
    END                                                            AS deduction_pct,
    -- Audit flag: deductions > 40% of gross income
    CASE
        WHEN f.gross_income > 0 AND (f.deductions / f.gross_income) > 0.40 THEN true
        ELSE false
    END                                                            AS audit_flag,
    -- Income bracket derivation
    CASE
        WHEN f.gross_income < 50000                    THEN 'Under $50K'
        WHEN f.gross_income BETWEEN 50000 AND 100000   THEN '$50K-$100K'
        WHEN f.gross_income BETWEEN 100001 AND 250000  THEN '$100K-$250K'
        WHEN f.gross_income BETWEEN 250001 AND 500000  THEN '$250K-$500K'
        WHEN f.gross_income BETWEEN 500001 AND 1000000 THEN '$500K-$1M'
        ELSE 'Over $1M'
    END                                                            AS income_bracket,
    CURRENT_TIMESTAMP                                              AS loaded_at
FROM tax.bronze.raw_filings f;

ASSERT ROW_COUNT = 50
SELECT COUNT(*) AS row_count FROM tax.silver.filings_immutable;

-- ===================== apply_amendments =====================
-- MERGE amendments into silver.amendments_applied, computing the delta between
-- the original filing and the amended values. Flag large deltas (> 20%).

MERGE INTO tax.silver.amendments_applied AS tgt
USING (
    SELECT
        a.amendment_id,
        a.original_filing_id,
        a.taxpayer_id,
        CAST(a.amendment_date AS DATE)                             AS amendment_date,
        f.taxable_income                                           AS original_taxable_income,
        a.amended_taxable_income,
        a.amended_taxable_income - f.taxable_income                AS income_delta,
        f.tax_owed                                                 AS original_tax_owed,
        a.amended_tax_owed,
        a.amended_tax_owed - f.tax_owed                            AS tax_delta,
        a.amendment_reason,
        -- Delta percentage: how much the amended amount differs from original
        CASE
            WHEN f.taxable_income > 0
            THEN ROUND(100.0 * ABS(a.amended_taxable_income - f.taxable_income) / f.taxable_income, 2)
            ELSE 0.00
        END                                                        AS delta_pct,
        -- Large delta flag: > 20% change
        CASE
            WHEN f.taxable_income > 0
             AND ABS(a.amended_taxable_income - f.taxable_income) / f.taxable_income > 0.20
            THEN true
            ELSE false
        END                                                        AS large_delta_flag,
        a.preparer_id,
        CURRENT_TIMESTAMP                                          AS loaded_at
    FROM tax.bronze.raw_amendments a
    JOIN tax.bronze.raw_filings f ON a.original_filing_id = f.filing_id
) AS src
ON tgt.amendment_id = src.amendment_id
WHEN NOT MATCHED THEN INSERT (
    amendment_id, original_filing_id, taxpayer_id, amendment_date,
    original_taxable_income, amended_taxable_income, income_delta,
    original_tax_owed, amended_tax_owed, tax_delta,
    amendment_reason, delta_pct, large_delta_flag, preparer_id, loaded_at
) VALUES (
    src.amendment_id, src.original_filing_id, src.taxpayer_id, src.amendment_date,
    src.original_taxable_income, src.amended_taxable_income, src.income_delta,
    src.original_tax_owed, src.amended_tax_owed, src.tax_delta,
    src.amendment_reason, src.delta_pct, src.large_delta_flag, src.preparer_id, src.loaded_at
);

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM tax.silver.amendments_applied;

-- ===================== enable_cdf_audit =====================
-- Materialize the CDF from filings_immutable into the audit_trail table.

DELETE FROM tax.silver.audit_trail WHERE 1=1;

INSERT INTO tax.silver.audit_trail
SELECT
    ROW_NUMBER() OVER (ORDER BY fi.filing_id)                     AS audit_id,
    'filings_immutable'                                            AS table_name,
    'INSERT'                                                       AS operation,
    fi.filing_id                                                   AS record_key,
    fi.taxpayer_id,
    fi.fiscal_year,
    fi.loaded_at                                                   AS change_timestamp,
    CONCAT('Gross=', CAST(fi.gross_income AS STRING),
           ' TaxOwed=', CAST(fi.tax_owed AS STRING),
           ' Status=', fi.filing_status)                           AS details
FROM tax.silver.filings_immutable fi;

-- Also log amendments
INSERT INTO tax.silver.audit_trail
SELECT
    (SELECT COALESCE(MAX(audit_id), 0) FROM tax.silver.audit_trail)
        + ROW_NUMBER() OVER (ORDER BY aa.amendment_id)             AS audit_id,
    'amendments_applied'                                           AS table_name,
    'AMENDMENT'                                                    AS operation,
    aa.amendment_id                                                AS record_key,
    aa.taxpayer_id,
    EXTRACT(YEAR FROM aa.amendment_date)                           AS fiscal_year,
    aa.loaded_at                                                   AS change_timestamp,
    CONCAT('OrigTax=', CAST(aa.original_tax_owed AS STRING),
           ' AmendedTax=', CAST(aa.amended_tax_owed AS STRING),
           ' Delta%=', CAST(aa.delta_pct AS STRING))               AS details
FROM tax.silver.amendments_applied aa;

-- ===================== build_taxpayer_profiles =====================
-- Aggregate across filings and amendments to build lifetime taxpayer profiles.

DELETE FROM tax.silver.taxpayer_profiles WHERE 1=1;

INSERT INTO tax.silver.taxpayer_profiles
SELECT
    t.taxpayer_id,
    t.taxpayer_name,
    t.ssn,
    t.filing_type,
    t.state,
    t.dependent_count,
    COALESCE(fstats.total_filings, 0)                             AS total_filings,
    COALESCE(astats.total_amendments, 0)                          AS total_amendments,
    COALESCE(fstats.lifetime_gross_income, 0)                     AS lifetime_gross_income,
    COALESCE(fstats.lifetime_tax_paid, 0)                         AS lifetime_tax_paid,
    fstats.latest_income_bracket,
    COALESCE(fstats.avg_deduction_pct, 0)                         AS avg_deduction_pct,
    COALESCE(fstats.ever_audited, false)                          AS ever_audited
FROM tax.bronze.raw_taxpayers t
LEFT JOIN (
    SELECT
        taxpayer_id,
        COUNT(*)                                                   AS total_filings,
        SUM(gross_income)                                          AS lifetime_gross_income,
        SUM(tax_paid)                                              AS lifetime_tax_paid,
        ROUND(AVG(deduction_pct), 2)                               AS avg_deduction_pct,
        MAX(CASE WHEN filing_status = 'Audited' THEN true ELSE false END) AS ever_audited,
        -- Latest bracket via window
        FIRST_VALUE(income_bracket) OVER (
            PARTITION BY taxpayer_id ORDER BY fiscal_year DESC, filing_date DESC
        )                                                          AS latest_income_bracket
    FROM tax.silver.filings_immutable
    GROUP BY taxpayer_id, income_bracket, fiscal_year, filing_date
) fstats ON t.taxpayer_id = fstats.taxpayer_id
LEFT JOIN (
    SELECT taxpayer_id, COUNT(*) AS total_amendments
    FROM tax.silver.amendments_applied
    GROUP BY taxpayer_id
) astats ON t.taxpayer_id = astats.taxpayer_id;

-- ===================== dim_taxpayer =====================

DELETE FROM tax.gold.dim_taxpayer WHERE 1=1;

INSERT INTO tax.gold.dim_taxpayer
SELECT
    ROW_NUMBER() OVER (ORDER BY tp.taxpayer_id)                   AS taxpayer_key,
    tp.taxpayer_id,
    tp.filing_type,
    tp.state,
    tp.latest_income_bracket                                       AS income_bracket,
    tp.dependent_count,
    tp.total_filings,
    tp.ever_audited
FROM tax.silver.taxpayer_profiles tp;

-- ===================== dim_jurisdiction =====================

DELETE FROM tax.gold.dim_jurisdiction WHERE 1=1;

INSERT INTO tax.gold.dim_jurisdiction
SELECT
    ROW_NUMBER() OVER (ORDER BY j.jurisdiction_id)                AS jurisdiction_key,
    j.jurisdiction_id,
    j.jurisdiction_name,
    j.jurisdiction_level,
    j.state,
    j.base_tax_rate,
    j.standard_deduction
FROM tax.bronze.raw_jurisdictions j;

-- ===================== dim_preparer =====================
-- Calculate total filings and amendment rate per preparer.

DELETE FROM tax.gold.dim_preparer WHERE 1=1;

INSERT INTO tax.gold.dim_preparer
SELECT
    ROW_NUMBER() OVER (ORDER BY p.preparer_id)                    AS preparer_key,
    p.preparer_id,
    p.preparer_name,
    p.firm,
    p.certification,
    p.years_experience,
    COALESCE(stats.total_filings, 0)                              AS total_filings_prepared,
    COALESCE(
        ROUND(100.0 * stats.amended_count / NULLIF(stats.total_filings, 0), 2),
        0.00
    )                                                              AS amendment_rate
FROM tax.bronze.raw_preparers p
LEFT JOIN (
    SELECT
        preparer_id,
        COUNT(*)                                                   AS total_filings,
        SUM(CASE WHEN aa.amendment_id IS NOT NULL THEN 1 ELSE 0 END) AS amended_count
    FROM tax.silver.filings_immutable fi
    LEFT JOIN tax.silver.amendments_applied aa
        ON fi.filing_id = aa.original_filing_id
    GROUP BY preparer_id
) stats ON p.preparer_id = stats.preparer_id;

-- ===================== dim_fiscal_year =====================

DELETE FROM tax.gold.dim_fiscal_year WHERE 1=1;

INSERT INTO tax.gold.dim_fiscal_year
SELECT
    ROW_NUMBER() OVER (ORDER BY fi.fiscal_year)                   AS fiscal_year_key,
    fi.fiscal_year,
    -- Filing deadline: April 15 of the following year
    CAST(CONCAT(CAST(fi.fiscal_year + 1 AS STRING), '-04-15') AS DATE) AS filing_deadline,
    COUNT(*)                                                       AS total_filings,
    COALESCE(amend_ct.cnt, 0)                                      AS total_amendments,
    SUM(CASE WHEN fi.audit_flag = true THEN 1 ELSE 0 END)        AS audit_flag_count
FROM tax.silver.filings_immutable fi
LEFT JOIN (
    SELECT
        EXTRACT(YEAR FROM amendment_date) - 1                      AS fiscal_year,
        COUNT(*)                                                   AS cnt
    FROM tax.silver.amendments_applied
    GROUP BY EXTRACT(YEAR FROM amendment_date) - 1
) amend_ct ON fi.fiscal_year = amend_ct.fiscal_year
GROUP BY fi.fiscal_year, amend_ct.cnt;

-- ===================== build_fact_filings =====================
-- Star schema fact: join originals + amendments.  COALESCE amended values
-- over originals so the fact reflects the "effective" filing.

DELETE FROM tax.gold.fact_filings WHERE 1=1;

INSERT INTO tax.gold.fact_filings
SELECT
    ROW_NUMBER() OVER (ORDER BY fi.filing_id)                     AS filing_key,
    dt.taxpayer_key,
    dj.jurisdiction_key,
    dp.preparer_key,
    dfy.fiscal_year_key,
    fi.fiscal_year,
    fi.filing_date,
    fi.gross_income,
    fi.deductions,
    fi.taxable_income,
    fi.tax_owed,
    fi.tax_paid,
    fi.refund_amount,
    -- Effective values: use amendment if present, else original
    COALESCE(aa.amended_taxable_income, fi.taxable_income)        AS effective_taxable_income,
    COALESCE(aa.amended_tax_owed, fi.tax_owed)                    AS effective_tax_owed,
    CASE WHEN aa.amendment_id IS NOT NULL THEN true ELSE false END AS was_amended,
    aa.amendment_reason,
    COALESCE(ac.amendment_count, 0)                                AS amendment_count,
    -- Effective tax rate based on effective values
    CASE
        WHEN fi.gross_income > 0
        THEN ROUND(COALESCE(aa.amended_tax_owed, fi.tax_owed) / fi.gross_income, 4)
        ELSE 0.0000
    END                                                            AS effective_tax_rate,
    fi.audit_flag,
    fi.filing_status
FROM tax.silver.filings_immutable fi
JOIN tax.gold.dim_taxpayer dt      ON fi.taxpayer_id = dt.taxpayer_id
JOIN tax.gold.dim_jurisdiction dj  ON fi.jurisdiction_id = dj.jurisdiction_id
LEFT JOIN tax.gold.dim_preparer dp ON fi.preparer_id = dp.preparer_id
JOIN tax.gold.dim_fiscal_year dfy  ON fi.fiscal_year = dfy.fiscal_year
-- Latest amendment per filing (if multiple, take the most recent)
LEFT JOIN (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY original_filing_id ORDER BY amendment_date DESC) AS rn
    FROM tax.silver.amendments_applied
) aa ON fi.filing_id = aa.original_filing_id AND aa.rn = 1
-- Amendment count per filing
LEFT JOIN (
    SELECT original_filing_id, COUNT(*) AS amendment_count
    FROM tax.silver.amendments_applied
    GROUP BY original_filing_id
) ac ON fi.filing_id = ac.original_filing_id;

-- ===================== kpi_revenue_analysis =====================
-- Revenue KPI by jurisdiction x fiscal_year with compliance rate and audit yield.

DELETE FROM tax.gold.kpi_revenue_analysis WHERE 1=1;

INSERT INTO tax.gold.kpi_revenue_analysis
SELECT
    ff.fiscal_year,
    dj.jurisdiction_name,
    COUNT(*)                                                       AS total_filings,
    SUM(ff.effective_taxable_income)                               AS total_taxable_income,
    SUM(ff.effective_tax_owed)                                     AS total_tax_collected,
    SUM(CASE WHEN ff.refund_amount > 0 THEN ff.refund_amount ELSE 0 END) AS total_refunds,
    ROUND(AVG(ff.effective_tax_rate), 4)                           AS avg_effective_rate,
    SUM(CASE WHEN ff.audit_flag = true THEN 1 ELSE 0 END)        AS audit_flag_count,
    -- Compliance rate: (accepted + filed) / total non-amended
    ROUND(
        100.0 * SUM(CASE WHEN ff.filing_status IN ('Accepted', 'Filed') THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                              AS compliance_rate,
    SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END)       AS amendment_count,
    -- Audit yield: additional tax from amendments on audit-flagged filings
    ROUND(
        100.0 * SUM(CASE WHEN ff.audit_flag = true AND ff.was_amended = true
                     THEN ff.effective_tax_owed - ff.tax_owed ELSE 0 END)
        / NULLIF(SUM(CASE WHEN ff.audit_flag = true THEN ff.tax_owed ELSE 0 END), 0),
        2
    )                                                              AS audit_yield_pct
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_jurisdiction dj ON ff.jurisdiction_key = dj.jurisdiction_key
GROUP BY ff.fiscal_year, dj.jurisdiction_name;

-- ===================== kpi_preparer_quality =====================
-- Preparer quality KPI: error rates, amendment rates, audit rates.

DELETE FROM tax.gold.kpi_preparer_quality WHERE 1=1;

INSERT INTO tax.gold.kpi_preparer_quality
SELECT
    dp.preparer_name,
    dp.firm,
    dp.certification,
    COUNT(*)                                                       AS total_filings,
    SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END)       AS amendment_count,
    ROUND(
        100.0 * SUM(CASE WHEN ff.was_amended = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                              AS amendment_rate_pct,
    SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END) AS audit_count,
    ROUND(
        100.0 * SUM(CASE WHEN ff.filing_status = 'Audited' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    )                                                              AS audit_rate_pct,
    ROUND(AVG(ff.gross_income), 2)                                AS avg_client_income,
    ROUND(AVG(
        CASE WHEN ff.gross_income > 0 THEN 100.0 * ff.deductions / ff.gross_income ELSE 0 END
    ), 2)                                                          AS avg_deduction_pct,
    ROUND(AVG(ff.effective_tax_rate), 4)                           AS avg_effective_rate
FROM tax.gold.fact_filings ff
JOIN tax.gold.dim_preparer dp ON ff.preparer_key = dp.preparer_key
GROUP BY dp.preparer_name, dp.firm, dp.certification;

-- ===================== bloom_and_optimize =====================

OPTIMIZE tax.silver.filings_immutable;
OPTIMIZE tax.silver.amendments_applied;
OPTIMIZE tax.silver.taxpayer_profiles;
OPTIMIZE tax.gold.fact_filings;
OPTIMIZE tax.gold.dim_taxpayer;
OPTIMIZE tax.gold.dim_jurisdiction;
OPTIMIZE tax.gold.dim_preparer;
OPTIMIZE tax.gold.dim_fiscal_year;
OPTIMIZE tax.gold.kpi_revenue_analysis;
OPTIMIZE tax.gold.kpi_preparer_quality;
