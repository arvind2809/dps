-- ==============================================================================
-- DP4 — assist_dev.air — Accrual Income & GSA Revenue Tracker
-- Grain: CLIN x LOA x accrual month. FASAB SFFAS 7 revenue recognition. BAAR transmission.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.air.dim_accrual_period
-- Accrual period dimension — monthly federal fiscal calendar. SCD Type 1.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.air.dim_accrual_period
(
    accrual_period_sk                              BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    accrual_year                                   INT NOT NULL                   COMMENT 'Calendar year of accrual period.',
    accrual_month                                  INT NOT NULL                   COMMENT 'Calendar month of accrual period (1–12).',
    accrual_fy                                     INT NOT NULL                   COMMENT 'Federal fiscal year of accrual period.',
    accrual_fy_month                               INT NOT NULL                   COMMENT 'Federal fiscal month (1=Oct … 12=Sep).',
    accrual_fy_quarter                             INT NOT NULL                   COMMENT 'Federal fiscal quarter (1–4).',
    period_label                                   STRING NOT NULL                COMMENT 'Human-readable label e.g. FY2025-M03 (Dec 2024).',
    period_start_dt                                DATE NOT NULL                  COMMENT 'First day of accrual period.',
    period_end_dt                                  DATE NOT NULL                  COMMENT 'Last day of accrual period.',
    is_year_end_flag                               BOOLEAN NOT NULL               COMMENT 'TRUE if September period (federal year-end accrual).',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (accrual_period_sk, accrual_fy, accrual_fy_month)
COMMENT 'Accrual period dimension — monthly federal fiscal calendar. SCD Type 1.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'air',
    'pipeline.table' = 'dim_accrual_period',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP4'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.air.dim_onefund_program
-- OneFund program dimension — AAC to OneFund fund/activity/program/BAAR doc type mapping. SCD Type 1. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.air.dim_onefund_program
(
    onefund_sk                                     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    aac                                            STRING NOT NULL                COMMENT 'Activity Address Code — natural key.',
    onefund_fund_cd                                STRING                         COMMENT 'OneFund fund code.',
    onefund_activity_cd                            STRING                         COMMENT 'OneFund activity code.',
    onefund_program_cd                             STRING                         COMMENT 'OneFund program code.',
    object_class_cd                                STRING                         COMMENT 'OMB Object Classification code.',
    baar_doc_type_cd                               STRING                         COMMENT 'BAAR document type for accrual income transmission.',
    baar_doc_type_desc                             STRING                         COMMENT 'BAAR document type description.',
    program_desc                                   STRING                         COMMENT 'OneFund program description.',
    activity_desc                                  STRING                         COMMENT 'OneFund activity description.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (onefund_sk, aac)
COMMENT 'OneFund program dimension — AAC to OneFund fund/activity/program/BAAR doc type mapping. SCD Type 1. Sources: aasbs.map_aac_onefund, map_onefund_program_accrual_income_doc_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'air',
    'pipeline.table' = 'dim_onefund_program',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP4'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.air.fact_accrual_income
-- Grain  : CLIN (line_item_sk) × LOA (loa_sk) × accrual month (accrual_period_sk)
-- Accrual income fact — monthly GSA revenue earned per CLIN x LOA. Feeds CFO P&L and BAAR transmission
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.air.fact_accrual_income
(
    accrual_income_sk                              BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item (CLIN).',
    loa_sk                                         BIGINT NOT NULL                COMMENT 'FK to common.dim_loa.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    accrual_period_sk                              BIGINT NOT NULL                COMMENT 'FK to air.dim_accrual_period.',
    onefund_sk                                     BIGINT                         COMMENT 'FK to air.dim_onefund_program.',
    accrual_income_id                              BIGINT                         COMMENT 'Source aasbs.accrual_income.id.',
    accrual_batch_id                               BIGINT                         COMMENT 'Source accrual.accrual_batch.id — BAAR batch.',
    calculated_income_amt                          DECIMAL(15,2)                  COMMENT 'System-calculated accrual income amount for period.',
    distributed_income_amt                         DECIMAL(15,2)                  COMMENT 'LOA-distributed income amount after burn order proration.',
    holdback_amt                                   DECIMAL(15,2)                  COMMENT 'FSD holdback amount excluded from this period accrual.',
    net_accrued_amt                                DECIMAL(15,2)                  COMMENT 'Net accrual income = distributed - holdback.',
    baar_transmitted_amt                           DECIMAL(15,2)                  COMMENT 'Amount transmitted to BAAR for this period.',
    baar_accepted_amt                              DECIMAL(15,2)                  COMMENT 'Amount accepted by BAAR after response processing.',
    baar_rejected_amt                              DECIMAL(15,2)                  COMMENT 'Amount rejected by BAAR — requires correction.',
    baar_transmission_status_cd                    STRING                         COMMENT 'BAAR transmission status — Pending, Transmitted, Accepted, Rejected.',
    baar_response_dt                               TIMESTAMP                      COMMENT 'Date BAAR response received.',
    inclusion_override_flag                        BOOLEAN                        COMMENT 'TRUE if FSD manually overrode default accrual inclusion.',
    inclusion_override_reason                      STRING                         COMMENT 'Reason text for FSD accrual inclusion override.',
    accrual_doc_type_cd                            STRING                         COMMENT 'BAAR document type code for this accrual record.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, loa_sk, accrual_period_sk)
COMMENT 'Accrual income fact — monthly GSA revenue earned per CLIN x LOA. Feeds CFO P&L and BAAR transmission. Sources: aasbs.accrual_income, accrual_income_dist, accrual.accrual_batch, accrual_header.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'air',
    'pipeline.table' = 'fact_accrual_income',
    'pipeline.data_product' = 'DP4',
    'pipeline.consumer' = 'CFO Revenue, BAAR, OMB A-136'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.air.v_revenue_vs_expense
-- Revenue vs expense matching view — compares GSA income accrual to expense accrual by IA and period. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.air.v_revenue_vs_expense
COMMENT 'Revenue vs expense matching view — compares GSA income accrual to expense accrual by IA and period. Supports CFO P&L and FASAB SFFAS 7 compliance.'
AS
SELECT
    ap.period_label, ap.accrual_fy, ap.accrual_fy_quarter,
    ag.agency_name,
    ia.ia_num,
    SUM(ai.net_accrued_amt)                         AS total_income_accrued,
    SUM(oe.accrued_amt)                             AS total_expense_accrued,
    SUM(ai.net_accrued_amt) - SUM(oe.accrued_amt)  AS net_revenue_margin,
    SUM(ai.baar_accepted_amt)                       AS baar_accepted,
    SUM(ai.baar_rejected_amt)                       AS baar_rejected
FROM assist_dev.air.fact_accrual_income ai
JOIN assist_dev.air.dim_accrual_period ap ON ai.accrual_period_sk = ap.accrual_period_sk
JOIN assist_dev.common.dim_ia          ia ON ai.ia_sk             = ia.ia_sk AND ia.is_current_flag
JOIN assist_dev.common.dim_agency      ag ON ai.agency_sk         = ag.agency_sk AND ag.is_current_flag
LEFT JOIN assist_dev.oet.fact_obligation_expenditure oe
    ON ai.line_item_sk = oe.line_item_sk AND ai.loa_sk = oe.loa_sk
    AND oe.fiscal_year = ap.accrual_fy AND oe.fiscal_month = ap.accrual_fy_month
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.air.v_baar_transmission_status
-- BAAR transmission status dashboard — accrual amounts by transmission state per period and agency.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.air.v_baar_transmission_status
COMMENT 'BAAR transmission status dashboard — accrual amounts by transmission state per period and agency.'
AS
SELECT
    ap.period_label, ap.accrual_fy,
    ag.agency_name, ia.ia_num,
    on2.baar_doc_type_cd, on2.baar_doc_type_desc,
    COUNT(*)                                        AS record_count,
    SUM(ai.net_accrued_amt)                         AS total_accrued,
    SUM(ai.baar_transmitted_amt)                    AS total_transmitted,
    SUM(ai.baar_accepted_amt)                       AS total_accepted,
    SUM(ai.baar_rejected_amt)                       AS total_rejected,
    SUM(CASE WHEN ai.baar_transmission_status_cd = 'PENDING'
             THEN ai.net_accrued_amt ELSE 0 END)    AS pending_transmission
FROM assist_dev.air.fact_accrual_income ai
JOIN assist_dev.air.dim_accrual_period  ap  ON ai.accrual_period_sk = ap.accrual_period_sk
JOIN assist_dev.air.dim_onefund_program on2 ON ai.onefund_sk        = on2.onefund_sk
JOIN assist_dev.common.dim_ia           ia  ON ai.ia_sk             = ia.ia_sk AND ia.is_current_flag
JOIN assist_dev.common.dim_agency       ag  ON ai.agency_sk         = ag.agency_sk AND ag.is_current_flag
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.air.v_accrual_holdback_overrides
-- FSD accrual holdback and override audit view — all CLINs with manual exclusions or holdback amounts 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.air.v_accrual_holdback_overrides
COMMENT 'FSD accrual holdback and override audit view — all CLINs with manual exclusions or holdback amounts applied.'
AS
SELECT
    ap.period_label, ag.agency_name, ia.ia_num,
    li.clin_num, li.line_item_type_desc,
    ai.net_accrued_amt,
    ai.holdback_amt,
    ai.inclusion_override_flag,
    ai.inclusion_override_reason,
    ai._gold_updated_at AS override_recorded_at
FROM assist_dev.air.fact_accrual_income ai
JOIN assist_dev.air.dim_accrual_period  ap ON ai.accrual_period_sk  = ap.accrual_period_sk
JOIN assist_dev.common.dim_line_item    li ON ai.line_item_sk        = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_ia           ia ON ai.ia_sk               = ia.ia_sk AND ia.is_current_flag
JOIN assist_dev.common.dim_agency       ag ON ai.agency_sk           = ag.agency_sk AND ag.is_current_flag
WHERE ai.inclusion_override_flag = TRUE OR ai.holdback_amt <> 0;
