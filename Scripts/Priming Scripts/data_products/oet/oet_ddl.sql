-- ==============================================================================
-- DP1 — assist_dev.oet — Obligation & Expenditure Tracker
-- Grain: one row per CLIN × LOA × accounting period. Primary CFO financial reporting product.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.oet.fact_obligation_expenditure
-- Grain  : CLIN (line_item_sk) × LOA (loa_sk) × fiscal year/period
-- Central financial fact table — CLIN x LOA x period. All USG obligation, expenditure, and billing amo
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.oet.fact_obligation_expenditure
(
    obligation_sk                                  BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item (CLIN/SLIN).',
    loa_sk                                         BIGINT NOT NULL                COMMENT 'FK to common.dim_loa (Line of Accounting).',
    funding_sk                                     BIGINT NOT NULL                COMMENT 'FK to common.dim_funding.',
    award_sk                                       BIGINT NOT NULL                COMMENT 'FK to common.dim_award.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    snapshot_date_sk                               INT                            COMMENT 'FK to common.dim_date — accounting period snapshot date.',
    fiscal_year                                    INT NOT NULL                   COMMENT 'Federal fiscal year of the obligation (Oct-Sep).',
    fiscal_quarter                                 INT                            COMMENT 'Federal fiscal quarter (1–4).',
    fiscal_month                                   INT                            COMMENT 'Federal fiscal month (1–12, 1=October).',
    budget_fy                                      INT                            COMMENT 'Budget fiscal year from source LOA.',
    committed_amt                                  DECIMAL(15,2)                  COMMENT 'Committed amount — funds reserved before obligation.',
    certified_amt                                  DECIMAL(15,2)                  COMMENT 'Certified amount — approved for obligation.',
    obligated_amt                                  DECIMAL(15,2)                  COMMENT 'Obligated amount — legally binding commitment (FAR 1.602-1).',
    invoiced_amt                                   DECIMAL(15,2)                  COMMENT 'Total amount invoiced by contractor.',
    accepted_amt                                   DECIMAL(15,2)                  COMMENT 'Total amount accepted (client + GSA authorized).',
    billed_amt                                     DECIMAL(15,2)                  COMMENT 'Total amount billed to client agency.',
    accrued_amt                                    DECIMAL(15,2)                  COMMENT 'Accrual expense amount (month-end estimate of services rendered).',
    udo_amt                                        DECIMAL(15,2)                  COMMENT 'Unliquidated Obligation — obligated_amt minus accepted_amt.',
    service_charge_amt                             DECIMAL(15,2)                  COMMENT 'GSA service charge billed.',
    disbursed_amt                                  DECIMAL(15,2)                  COMMENT 'Amount disbursed to contractor via Pegasys payment.',
    undelivered_orders_amt                         DECIMAL(15,2)                  COMMENT 'Undelivered Orders — obligated but not yet invoiced.',
    loa_transaction_type_cd                        STRING                         COMMENT 'LOA ledger transaction type code.',
    invoice_count                                  INT                            COMMENT 'Number of invoices contributing to this snapshot.',
    acceptance_count                               INT                            COMMENT 'Number of acceptance records contributing.',
    source_line_item_id                            BIGINT                         COMMENT 'Source aasbs.line_item.id — for Silver join tracing.',
    source_loa_ledger_ids                          STRING                         COMMENT 'Comma-delimited source loa_ledger IDs contributing to this row.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, loa_sk, fiscal_year)
COMMENT 'Central financial fact table — CLIN x LOA x period. All USG obligation, expenditure, and billing amounts. Sources: aasbs.line_item_accepted, aasbs.loa_ledger, aasbs.invoice_item, aasbs.acceptance_item, aasbs.bill_item.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'oet',
    'pipeline.table' = 'fact_obligation_expenditure',
    'pipeline.data_product' = 'DP1',
    'pipeline.consumer' = 'CFO, OMB, FSD'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.oet.v_cfo_obligation_summary
-- CFO summary view — obligation, acceptance, billing, and UDO aggregated by agency, IA, CLIN, LOA. Sup
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.oet.v_cfo_obligation_summary
COMMENT 'CFO summary view — obligation, acceptance, billing, and UDO aggregated by agency, IA, CLIN, LOA. Supports OMB SF-133 and SF-132 reporting.'
AS
SELECT
    a.agency_name,
    a.bureau_name,
    f.fiscal_year,
    f.fiscal_quarter,
    ia.ia_num,
    ia.ia_type_desc,
    li.clin_num,
    li.line_item_type_desc,
    li.psc_cd,
    loa.fund_cd,
    loa.treasury_account_symbol,
    loa.object_class_cd,
    SUM(f.obligated_amt)       AS total_obligated,
    SUM(f.accepted_amt)        AS total_accepted,
    SUM(f.billed_amt)          AS total_billed,
    SUM(f.udo_amt)             AS total_udo,
    SUM(f.accrued_amt)         AS total_accrued,
    SUM(f.service_charge_amt)  AS total_service_charge
FROM assist_dev.oet.fact_obligation_expenditure f
JOIN assist_dev.common.dim_agency   a   ON f.agency_sk       = a.agency_sk   AND a.is_current_flag
JOIN assist_dev.common.dim_ia       ia  ON f.ia_sk           = ia.ia_sk      AND ia.is_current_flag
JOIN assist_dev.common.dim_line_item li  ON f.line_item_sk   = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_loa      loa ON f.loa_sk          = loa.loa_sk    AND loa.is_current_flag
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.oet.v_clin_expenditure_detail
-- CLIN-level expenditure detail view — denormalized for FSD analyst drill-down. All amounts, dates, an
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.oet.v_clin_expenditure_detail
COMMENT 'CLIN-level expenditure detail view — denormalized for FSD analyst drill-down. All amounts, dates, and decoded attributes in one flat projection.'
AS
SELECT
    f.*,
    li.clin_num, li.slin_num, li.line_item_type_desc, li.psc_cd, li.li_pop_start_dt, li.li_pop_end_dt,
    loa.tracking_num, loa.treasury_account_symbol, loa.fund_cd, loa.object_class_cd,
    aw.award_piid, aw.contract_type_desc,
    ia.ia_num, ia.instrument_type_desc,
    ag.agency_name, ag.bureau_name, ag.activity_address_cd,
    d.calendar_date AS snapshot_date, d.federal_fiscal_fy, d.federal_fiscal_qtr
FROM assist_dev.oet.fact_obligation_expenditure f
JOIN assist_dev.common.dim_line_item li  ON f.line_item_sk = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_loa      loa ON f.loa_sk        = loa.loa_sk     AND loa.is_current_flag
JOIN assist_dev.common.dim_award    aw  ON f.award_sk      = aw.award_sk    AND aw.is_current_flag
JOIN assist_dev.common.dim_ia       ia  ON f.ia_sk         = ia.ia_sk       AND ia.is_current_flag
JOIN assist_dev.common.dim_agency   ag  ON f.agency_sk     = ag.agency_sk   AND ag.is_current_flag
LEFT JOIN assist_dev.common.dim_date d  ON f.snapshot_date_sk = d.date_sk;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.oet.v_udo_aging
-- UDO aging view — unliquidated obligation balances bucketed by days past period of performance. Suppo
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.oet.v_udo_aging
COMMENT 'UDO aging view — unliquidated obligation balances bucketed by days past period of performance. Supports USG Scorecard UDO reduction metric.'
AS
SELECT
    ag.agency_name,
    ia.ia_num,
    li.clin_num,
    li.li_pop_end_dt,
    aw.award_piid,
    f.fiscal_year,
    SUM(f.udo_amt)        AS udo_amt,
    SUM(f.obligated_amt)  AS obligated_amt,
    SUM(f.accepted_amt)   AS accepted_amt,
    DATEDIFF(CURRENT_DATE(), CAST(li.li_pop_end_dt AS DATE)) AS days_past_pop,
    CASE
        WHEN li.li_pop_end_dt IS NULL THEN 'No PoP'
        WHEN CURRENT_DATE() <= CAST(li.li_pop_end_dt AS DATE) THEN 'Within PoP'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(li.li_pop_end_dt AS DATE)) <= 180 THEN '0-180 Days Past PoP'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(li.li_pop_end_dt AS DATE)) <= 365 THEN '181-365 Days Past PoP'
        ELSE 'Over 1 Year Past PoP'
    END AS udo_aging_bucket
FROM assist_dev.oet.fact_obligation_expenditure f
JOIN assist_dev.common.dim_line_item li ON f.line_item_sk = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_award    aw  ON f.award_sk     = aw.award_sk    AND aw.is_current_flag
JOIN assist_dev.common.dim_ia       ia  ON f.ia_sk        = ia.ia_sk       AND ia.is_current_flag
JOIN assist_dev.common.dim_agency   ag  ON f.agency_sk    = ag.agency_sk   AND ag.is_current_flag
WHERE f.udo_amt <> 0
GROUP BY ALL
ORDER BY udo_amt DESC;

