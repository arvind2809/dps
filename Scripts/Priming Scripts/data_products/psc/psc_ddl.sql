-- ==============================================================================
-- DP6 — assist_dev.psc — CLIN Pricing & Service Charge Catalog
-- Grain: service charge schedule period per CLIN. FAR 15.404 price analysis support.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.psc.dim_clin_subtype
-- CLIN subtype attributes — differentiates deliverable from service charge CLINs. SCD Type 1. Sources:
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.psc.dim_clin_subtype
(
    clin_subtype_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_id                                   BIGINT NOT NULL                COMMENT 'NK — source aasbs.line_item.id.',
    clin_subtype_cd                                STRING NOT NULL                COMMENT 'CLIN subtype code — DELIVERABLE, SC_LABOR, SC_FIXED_FEE, SC_TRAVEL, AWARD_FEE.',
    proc_phase_cd                                  STRING                         COMMENT 'Procurement phase code — Acquisition, Solicitation, Award.',
    proc_phase_desc                                STRING                         COMMENT 'Procurement phase description.',
    project_num                                    STRING                         COMMENT 'Project number for this service charge CLIN.',
    is_credit_flag                                 BOOLEAN                        COMMENT 'TRUE if this is a credit service charge.',
    act_num                                        STRING                         COMMENT 'ACT (Financial Account) number associated with service charge.',
    severability_type_cd                           STRING                         COMMENT 'Severability classification.',
    psc_cd                                         STRING                         COMMENT 'Product/Service Code.',
    contract_type_cd                               STRING                         COMMENT 'CLIN-level contract type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (clin_subtype_sk, line_item_id)
COMMENT 'CLIN subtype attributes — differentiates deliverable from service charge CLINs. SCD Type 1. Sources: aasbs.li_deliverable, li_sc_labor, li_sc_fixed_fee, li_sc_travel, li_award_fee.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'psc',
    'pipeline.table' = 'dim_clin_subtype',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP6'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.psc.dim_cost_rate_type
-- Labor cost rate type dimension. SCD Type 1. Source: aasbs.lu_cost_rate_type.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.psc.dim_cost_rate_type
(
    cost_rate_type_sk                              BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    cost_rate_type_cd                              STRING NOT NULL                COMMENT 'NK — cost rate type code.',
    cost_rate_type_desc                            STRING                         COMMENT 'Cost rate type description — Actual, Provisional, Fixed, Predefined.',
    is_actual_flag                                 BOOLEAN                        COMMENT 'TRUE if this is an actual (not estimated) rate type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (cost_rate_type_sk, cost_rate_type_cd)
COMMENT 'Labor cost rate type dimension. SCD Type 1. Source: aasbs.lu_cost_rate_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'psc',
    'pipeline.table' = 'dim_cost_rate_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP6'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.psc.dim_billing_period
-- Service charge billing period dimension. SCD Type 1. Source: aasbs.service_charge_schedule.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.psc.dim_billing_period
(
    billing_period_sk                              BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    schedule_id                                    BIGINT NOT NULL                COMMENT 'NK — source aasbs.service_charge_schedule.id.',
    billing_period_label                           STRING                         COMMENT 'Billing period label (e.g. 2025-Q1).',
    period_start_dt                                DATE                           COMMENT 'Billing period start date.',
    period_end_dt                                  DATE                           COMMENT 'Billing period end date.',
    scheduled_amt                                  DECIMAL(15,2)                  COMMENT 'Scheduled billing amount for this period.',
    invoiced_amt                                   DECIMAL(15,2)                  COMMENT 'Amount actually invoiced this period.',
    variance_pct                                   DECIMAL(7,4)                   COMMENT 'Variance percentage between scheduled and invoiced.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (billing_period_sk, schedule_id)
COMMENT 'Service charge billing period dimension. SCD Type 1. Source: aasbs.service_charge_schedule.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'psc',
    'pipeline.table' = 'dim_billing_period',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP6'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.psc.fact_clin_pricing
-- Grain  : CLIN (line_item_sk) × CLIN subtype (clin_subtype_sk)
-- CLIN pricing fact — planned vs actual amounts per CLIN by subtype. Sources: aasbs.li_award_fee, li_d
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.psc.fact_clin_pricing
(
    clin_pricing_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item.',
    clin_subtype_sk                                BIGINT NOT NULL                COMMENT 'FK to psc.dim_clin_subtype.',
    cost_rate_type_sk                              BIGINT                         COMMENT 'FK to psc.dim_cost_rate_type (labor CLINs only).',
    award_sk                                       BIGINT NOT NULL                COMMENT 'FK to common.dim_award.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    loa_sk                                         BIGINT                         COMMENT 'FK to common.dim_loa (burn order).',
    planned_amt                                    DECIMAL(15,2)                  COMMENT 'Planned CLIN amount from pricing structure.',
    planned_hours                                  DECIMAL(10,2)                  COMMENT 'Planned labor hours (labor CLINs only).',
    planned_rate                                   DECIMAL(10,4)                  COMMENT 'Planned labor rate per hour.',
    award_fee_amt                                  DECIMAL(15,2)                  COMMENT 'Award fee amount (award fee CLINs only).',
    award_fee_surcharge_rate                       DECIMAL(7,4)                   COMMENT 'Award fee surcharge rate (percentage).',
    fixed_fee_amt                                  DECIMAL(15,2)                  COMMENT 'Fixed fee amount (fixed fee CLINs only).',
    travel_planned_amt                             DECIMAL(15,2)                  COMMENT 'Planned travel amount (travel CLINs only).',
    actual_amt                                     DECIMAL(15,2)                  COMMENT 'Actual tracked amount from tracking_item.',
    actual_hours                                   DECIMAL(10,2)                  COMMENT 'Actual labor hours tracked.',
    loa_burn_order                                 INT                            COMMENT 'LOA burn order sequence when multiple LOAs fund CLIN.',
    loa_proration_pct                              DECIMAL(7,4)                   COMMENT 'LOA proration percentage for burn order.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, clin_subtype_sk)
COMMENT 'CLIN pricing fact — planned vs actual amounts per CLIN by subtype. Sources: aasbs.li_award_fee, li_deliverable, li_sc_fixed_fee, li_sc_labor, li_sc_travel, tracking_item, loa_ledger_burn_order.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'psc',
    'pipeline.table' = 'fact_clin_pricing',
    'pipeline.data_product' = 'DP6',
    'pipeline.consumer' = 'Cost/Price Analysts, Billing Ops'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.psc.fact_service_charge_schedule
-- Grain  : CLIN (line_item_sk) × billing period (billing_period_sk)
-- Service charge schedule fact — billing schedule health per CLIN per period. Sources: aasbs.service_c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.psc.fact_service_charge_schedule
(
    svc_charge_sk                                  BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item.',
    billing_period_sk                              BIGINT NOT NULL                COMMENT 'FK to psc.dim_billing_period.',
    award_sk                                       BIGINT NOT NULL                COMMENT 'FK to common.dim_award.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    source_service_charge_id                       BIGINT                         COMMENT 'Source aasbs.service_charge.id.',
    schedule_status_cd                             STRING                         COMMENT 'Billing schedule status — Active, Complete, Cancelled.',
    scheduled_amt                                  DECIMAL(15,2)                  COMMENT 'Scheduled amount for this billing period.',
    invoiced_amt                                   DECIMAL(15,2)                  COMMENT 'Actual invoiced amount for this period.',
    accepted_amt                                   DECIMAL(15,2)                  COMMENT 'Accepted amount for this period.',
    on_schedule_flag                               BOOLEAN                        COMMENT 'TRUE if invoiced within 10% of scheduled amount.',
    travel_voucher_count                           INT                            COMMENT 'Number of travel vouchers for this period.',
    travel_voucher_amt                             DECIMAL(15,2)                  COMMENT 'Total travel voucher amount this period.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, billing_period_sk)
COMMENT 'Service charge schedule fact — billing schedule health per CLIN per period. Sources: aasbs.service_charge, service_charge_schedule, acceptance_dist, invoice_connector.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'psc',
    'pipeline.table' = 'fact_service_charge_schedule',
    'pipeline.data_product' = 'DP6'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.psc.v_billing_schedule_health
-- Billing schedule health view — scheduled vs actual invoice amounts per CLIN per period with variance
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.psc.v_billing_schedule_health
COMMENT 'Billing schedule health view — scheduled vs actual invoice amounts per CLIN per period with variance flag.'
AS
SELECT
    ia.ia_num, aw.award_piid, li.clin_num,
    cs.clin_subtype_cd, bp.billing_period_label,
    bp.period_start_dt, bp.period_end_dt,
    f.scheduled_amt, f.invoiced_amt, f.accepted_amt,
    f.invoiced_amt - f.scheduled_amt   AS schedule_variance,
    f.on_schedule_flag,
    f.travel_voucher_amt
FROM assist_dev.psc.fact_service_charge_schedule f
JOIN assist_dev.common.dim_line_item  li ON f.line_item_sk     = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_award      aw ON f.award_sk         = aw.award_sk     AND aw.is_current_flag
JOIN assist_dev.common.dim_ia         ia ON f.ia_sk            = ia.ia_sk        AND ia.is_current_flag
JOIN assist_dev.psc.dim_clin_subtype  cs ON li.line_item_id    = cs.line_item_id
JOIN assist_dev.psc.dim_billing_period bp ON f.billing_period_sk = bp.billing_period_sk
ORDER BY aw.award_piid, li.clin_num, bp.period_start_dt;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.psc.v_labor_planned_vs_actual
-- Labor planned vs actual view — hours and rates variance analysis for labor service charge CLINs. Sup
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.psc.v_labor_planned_vs_actual
COMMENT 'Labor planned vs actual view — hours and rates variance analysis for labor service charge CLINs. Supports FAR 15.404 price analysis.'
AS
SELECT
    ia.ia_num, aw.award_piid, li.clin_num,
    crt.cost_rate_type_desc,
    f.planned_amt, f.planned_hours, f.planned_rate,
    f.actual_amt, f.actual_hours,
    f.actual_amt - f.planned_amt               AS amount_variance,
    f.actual_hours - f.planned_hours           AS hours_variance,
    ROUND(100.0 * f.actual_amt / NULLIF(f.planned_amt, 0), 2) AS pct_of_plan
FROM assist_dev.psc.fact_clin_pricing f
JOIN assist_dev.common.dim_line_item  li  ON f.line_item_sk       = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_award      aw  ON f.award_sk           = aw.award_sk     AND aw.is_current_flag
JOIN assist_dev.common.dim_ia         ia  ON f.ia_sk              = ia.ia_sk        AND ia.is_current_flag
JOIN assist_dev.psc.dim_clin_subtype  cs  ON f.clin_subtype_sk    = cs.clin_subtype_sk
JOIN assist_dev.psc.dim_cost_rate_type crt ON f.cost_rate_type_sk = crt.cost_rate_type_sk
WHERE cs.clin_subtype_cd = 'SC_LABOR';

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.psc.v_award_fee_performance
-- Award fee performance view — fee pool vs earned fee and surcharge rate per award fee CLIN.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.psc.v_award_fee_performance
COMMENT 'Award fee performance view — fee pool vs earned fee and surcharge rate per award fee CLIN.'
AS
SELECT
    ia.ia_num, aw.award_piid, li.clin_num,
    f.planned_amt           AS total_award_fee_pool,
    f.award_fee_amt         AS earned_award_fee,
    f.award_fee_surcharge_rate,
    f.actual_amt            AS total_tracked_amt,
    ROUND(100.0 * f.award_fee_amt / NULLIF(f.planned_amt, 0), 2) AS pct_fee_earned
FROM assist_dev.psc.fact_clin_pricing f
JOIN assist_dev.common.dim_line_item  li ON f.line_item_sk  = li.line_item_sk AND li.is_current_flag
JOIN assist_dev.common.dim_award      aw ON f.award_sk      = aw.award_sk     AND aw.is_current_flag
JOIN assist_dev.common.dim_ia         ia ON f.ia_sk         = ia.ia_sk        AND ia.is_current_flag
JOIN assist_dev.psc.dim_clin_subtype  cs ON f.clin_subtype_sk = cs.clin_subtype_sk
WHERE cs.clin_subtype_cd = 'AWARD_FEE';

