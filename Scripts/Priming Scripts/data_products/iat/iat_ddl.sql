-- ==============================================================================
-- DP9 — assist_dev.iat — IA Lifecycle & Amendment Tracker
-- Grain: IA amendment. Economy Act compliance. Annual review cadence. FIN code history.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.iat.dim_amend_action
-- IA amendment action type dimension. SCD Type 1. Source: aasbs.lu_ia_amend_action.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.iat.dim_amend_action
(
    amend_action_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    amend_action_cd                                STRING NOT NULL                COMMENT 'NK — amendment action code.',
    amend_action_desc                              STRING                         COMMENT 'Amendment action description — Scope Increase, Cost Adjustment, Period Extension, Admin Correction, etc.',
    changes_scope_flag                             BOOLEAN                        COMMENT 'TRUE if this action type changes work scope.',
    changes_cost_flag                              BOOLEAN                        COMMENT 'TRUE if this action type changes cost or ceiling.',
    changes_period_flag                            BOOLEAN                        COMMENT 'TRUE if this action type changes period of performance.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (amend_action_sk, amend_action_cd)
COMMENT 'IA amendment action type dimension. SCD Type 1. Source: aasbs.lu_ia_amend_action.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'iat',
    'pipeline.table' = 'dim_amend_action',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP9'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.iat.dim_reviewer
-- IA reviewer dimension. SCD Type 1. Sourced from ia_review and aasbs users.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.iat.dim_reviewer
(
    reviewer_sk                                    BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    reviewer_user_id                               STRING NOT NULL                COMMENT 'NK — ASSIST user ID of reviewer.',
    reviewer_name                                  STRING                         COMMENT 'Reviewer full name.',
    reviewer_role_cd                               STRING                         COMMENT 'Reviewer role code.',
    reviewer_org                                   STRING                         COMMENT 'Reviewer organization.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if reviewer account is active.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (reviewer_sk, reviewer_user_id)
COMMENT 'IA reviewer dimension. SCD Type 1. Sourced from ia_review and aasbs users.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'iat',
    'pipeline.table' = 'dim_reviewer',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP9'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.iat.fact_ia_amendment
-- Grain  : IA amendment (source_ia_amendment_id) — one row per amendment
-- IA amendment fact — full amendment history per IA. Sources: aasbs.ia_amendment, award_ia, funding_am
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.iat.fact_ia_amendment
(
    ia_amendment_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    award_sk                                       BIGINT                         COMMENT 'FK to common.dim_award (when amendment relates to order).',
    amend_action_sk                                BIGINT NOT NULL                COMMENT 'FK to iat.dim_amend_action.',
    funding_sk                                     BIGINT                         COMMENT 'FK to common.dim_funding.',
    loa_sk                                         BIGINT                         COMMENT 'FK to common.dim_loa (LOA changed by amendment).',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    amend_date_sk                                  INT                            COMMENT 'FK to common.dim_date — amendment effective date.',
    source_ia_amendment_id                         BIGINT NOT NULL                COMMENT 'Source aasbs.ia_amendment.id.',
    amendment_num                                  INT                            COMMENT 'Sequential amendment number for this IA.',
    amendment_status_cd                            STRING                         COMMENT 'Amendment status — Draft, Pending, Final, Error.',
    cost_change_amt                                DECIMAL(15,2)                  COMMENT 'Dollar change to IA cost ceiling from this amendment.',
    revised_total_cost_amt                         DECIMAL(15,2)                  COMMENT 'Revised total cost after amendment.',
    pop_extension_days                             INT                            COMMENT 'Period of performance extension in calendar days.',
    revised_pop_end_dt                             TIMESTAMP                      COMMENT 'Revised period of performance end date.',
    servicing_aac_cd                               STRING                         COMMENT 'Servicing AAC assigned for this amendment.',
    mod_type_cds                                   STRING                         COMMENT 'Comma-delimited modification type codes.',
    validation_override_flag                       BOOLEAN                        COMMENT 'TRUE if a validation rule was overridden for this amendment.',
    validation_override_reason                     STRING                         COMMENT 'Reason for validation rule override.',
    loa_change_amt                                 DECIMAL(15,2)                  COMMENT 'LOA-level amount change from funding amendment.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (ia_sk, amend_date_sk)
COMMENT 'IA amendment fact — full amendment history per IA. Sources: aasbs.ia_amendment, award_ia, funding_amendment_loa, award_mod_type, award_servicing_aac, award_validation_override.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'iat',
    'pipeline.table' = 'fact_ia_amendment',
    'pipeline.data_product' = 'DP9',
    'pipeline.consumer' = 'IA Program Managers, CFO, OIG'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.iat.fact_ia_review
-- Grain  : IA review (source_ia_review_id) — one row per review event
-- IA annual review fact. OMB A-123 review cadence tracking. Source: aasbs.ia_review.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.iat.fact_ia_review
(
    ia_review_sk                                   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    reviewer_sk                                    BIGINT NOT NULL                COMMENT 'FK to iat.dim_reviewer.',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    review_date_sk                                 INT                            COMMENT 'FK to common.dim_date — review date.',
    source_ia_review_id                            BIGINT NOT NULL                COMMENT 'Source aasbs.ia_review.id.',
    review_fiscal_year                             INT                            COMMENT 'Fiscal year this review covers.',
    review_type_cd                                 STRING                         COMMENT 'Review type — Annual, Renewal, Ad-hoc.',
    has_attachment_flag                            BOOLEAN                        COMMENT 'TRUE if review has a supporting attachment.',
    review_completed_flag                          BOOLEAN                        COMMENT 'TRUE if review is marked complete.',
    days_overdue                                   INT                            COMMENT 'Calendar days past due for annual review (NULL if not overdue).',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (ia_sk, review_date_sk)
COMMENT 'IA annual review fact. OMB A-123 review cadence tracking. Source: aasbs.ia_review.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'iat',
    'pipeline.table' = 'fact_ia_review',
    'pipeline.data_product' = 'DP9'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.iat.v_ia_amendment_velocity
-- IA amendment velocity view — amendment frequency, cost change, and period extension summary by IA an
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.iat.v_ia_amendment_velocity
COMMENT 'IA amendment velocity view — amendment frequency, cost change, and period extension summary by IA and FY. High velocity may indicate scope instability.'
AS
SELECT
    ia.ia_num, ag.agency_name,
    d.federal_fiscal_fy,
    COUNT(f.ia_amendment_sk)             AS amendment_count,
    SUM(f.cost_change_amt)               AS total_cost_change,
    SUM(CASE WHEN da.changes_scope_flag  THEN 1 ELSE 0 END) AS scope_changes,
    SUM(CASE WHEN da.changes_cost_flag   THEN 1 ELSE 0 END) AS cost_changes,
    SUM(CASE WHEN da.changes_period_flag THEN 1 ELSE 0 END) AS period_extensions,
    SUM(f.pop_extension_days)            AS total_pop_extension_days,
    AVG(f.cost_change_amt)               AS avg_cost_change
FROM assist_dev.iat.fact_ia_amendment f
JOIN assist_dev.common.dim_ia       ia ON f.ia_sk          = ia.ia_sk          AND ia.is_current_flag
JOIN assist_dev.common.dim_agency   ag ON f.agency_sk      = ag.agency_sk      AND ag.is_current_flag
JOIN assist_dev.iat.dim_amend_action da ON f.amend_action_sk = da.amend_action_sk
JOIN assist_dev.common.dim_date      d ON f.amend_date_sk  = d.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.iat.v_ia_annual_review_status
-- IA annual review status view — completion and overdue tracking per IA. OMB A-123 governance complian
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.iat.v_ia_annual_review_status
COMMENT 'IA annual review status view — completion and overdue tracking per IA. OMB A-123 governance compliance.'
AS
SELECT
    ia.ia_num, ia.ia_status_desc, ia.ia_end_dt,
    ag.agency_name,
    d_review.federal_fiscal_fy                 AS review_fy,
    COUNT(r.ia_review_sk)                      AS reviews_completed,
    SUM(CASE WHEN r.days_overdue > 0 THEN 1 ELSE 0 END) AS overdue_reviews,
    MAX(r.days_overdue)                        AS max_days_overdue,
    MIN(CASE WHEN NOT r.review_completed_flag THEN r.days_overdue END) AS current_days_overdue
FROM assist_dev.iat.fact_ia_review r
JOIN assist_dev.common.dim_ia    ia      ON r.ia_sk          = ia.ia_sk        AND ia.is_current_flag
JOIN assist_dev.common.dim_agency ag     ON r.agency_sk      = ag.agency_sk    AND ag.is_current_flag
JOIN assist_dev.common.dim_date   d_review ON r.review_date_sk = d_review.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.iat.v_fin_code_history
-- FIN code and servicing AAC history view — full amendment trail per IA ordered by amendment number. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.iat.v_fin_code_history
COMMENT 'FIN code and servicing AAC history view — full amendment trail per IA ordered by amendment number. Supports financial system routing audit.'
AS
SELECT
    ia.ia_num,
    ag.agency_name,
    f.source_ia_amendment_id,
    f.amendment_num,
    d.calendar_date              AS amendment_dt,
    f.servicing_aac_cd,
    f.validation_override_flag,
    f.validation_override_reason,
    ia.ia_status_desc
FROM assist_dev.iat.fact_ia_amendment f
JOIN assist_dev.common.dim_ia    ia ON f.ia_sk         = ia.ia_sk    AND ia.is_current_flag
JOIN assist_dev.common.dim_agency ag ON f.agency_sk    = ag.agency_sk AND ag.is_current_flag
JOIN assist_dev.common.dim_date   d ON f.amend_date_sk = d.date_sk
ORDER BY ia.ia_num, f.amendment_num;

