-- ==============================================================================
-- DP2 — assist_dev.alt — Award Lifecycle Tracker
-- Grain: CLIN/SLIN per award. Pre-award through closeout lifecycle.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.alt.dim_acquisition
-- Pre-award acquisition context dimension. SCD Type 2. Sources: aasbs.acquisition, aasbs.acquisition_p
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.alt.dim_acquisition
(
    acquisition_sk                                 BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    acquisition_id                                 BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.acquisition.id.',
    acquisition_status_cd                          STRING                         COMMENT 'Acquisition status code.',
    acquisition_status_desc                        STRING                         COMMENT 'Status description decoded.',
    acquisition_type_cd                            STRING                         COMMENT 'Acquisition type — Service or Supply.',
    acquisition_type_desc                          STRING                         COMMENT 'Type description decoded.',
    competition_type_cd                            STRING                         COMMENT 'Competition type code (FAR Part 6).',
    competition_type_desc                          STRING                         COMMENT 'Competition type description.',
    commercial_type_cd                             STRING                         COMMENT 'Commercial/non-commercial determination.',
    processing_speed_cd                            STRING                         COMMENT 'Routine or Expedited.',
    expedited_type_cd                              STRING                         COMMENT 'Expedited processing cause code.',
    emergency_acq_cd                               STRING                         COMMENT 'FAR Part 18 emergency acquisition indicator.',
    cor_delegation_cd                              STRING                         COMMENT 'COR delegation method code.',
    consolidated_contract_cd                       STRING                         COMMENT 'FAR consolidated contract determination.',
    supply_chain_risk_cd                           STRING                         COMMENT 'CMMC level code (supply chain risk).',
    surveillance_spec_cd                           STRING                         COMMENT 'DD-254 security spec indicator.',
    intel_community_cd                             STRING                         COMMENT 'Intelligence Community member indicator.',
    igce_amt                                       DECIMAL(15,2)                  COMMENT 'Independent Government Cost Estimate amount.',
    igce_source                                    STRING                         COMMENT 'IGCE source description from acquisition plan.',
    performance_type_cd                            STRING                         COMMENT 'PBA/non-PBA performance type.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    created_dt                                     TIMESTAMP                      COMMENT 'Acquisition record creation date.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (acquisition_sk, acquisition_id)
COMMENT 'Pre-award acquisition context dimension. SCD Type 2. Sources: aasbs.acquisition, aasbs.acquisition_plan.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'alt',
    'pipeline.table' = 'dim_acquisition',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP2'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.alt.dim_solicitation
-- Solicitation dimension. SCD Type 1 — status and response counts updated in place. Sources: aasbs.sol
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.alt.dim_solicitation
(
    solicitation_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    solicit_id                                     BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.solicit.id.',
    solicit_num                                    STRING                         COMMENT 'Solicitation number (PIID-based).',
    sol_status_cd                                  STRING                         COMMENT 'Solicitation status code.',
    sol_status_desc                                STRING                         COMMENT 'Status description decoded.',
    sol_posting_type_cd                            STRING                         COMMENT 'Posting type — Direct Connect, Open, etc.',
    sol_posting_type_desc                          STRING                         COMMENT 'Posting type description.',
    open_dt                                        TIMESTAMP                      COMMENT 'Date solicitation opened for responses.',
    close_dt                                       TIMESTAMP                      COMMENT 'Date solicitation closed.',
    total_amendments                               INT                            COMMENT 'Total number of solicitation amendments issued.',
    total_responses                                INT                            COMMENT 'Total contractor responses received.',
    winning_response_id                            BIGINT                         COMMENT 'Source solicit_response.id of winning bid.',
    acquisition_sk                                 BIGINT                         COMMENT 'FK to alt.dim_acquisition.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (solicitation_sk, solicit_id)
COMMENT 'Solicitation dimension. SCD Type 1 — status and response counts updated in place. Sources: aasbs.solicit, aasbs.solicit_amendment.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'alt',
    'pipeline.table' = 'dim_solicitation',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP2'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.alt.dim_contractor
-- Contractor identity dimension. SCD Type 2 — tracks UEI/DUNS changes, name changes, business type rec
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.alt.dim_contractor
(
    contractor_sk                                  BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    award_mod_company_id                           BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.award_mod_company.id.',
    uei                                            STRING                         COMMENT 'Unique Entity Identifier (SAM.gov UEI — replaced DUNS Apr 2022).',
    duns_num                                       STRING                         COMMENT 'Legacy DUNS number (retained for historical records).',
    cage_code                                      STRING                         COMMENT 'Commercial and Government Entity (CAGE) code.',
    company_name                                   STRING NOT NULL                COMMENT 'Legal contractor company name.',
    dba_name                                       STRING                         COMMENT 'Doing Business As name.',
    parent_uei                                     STRING                         COMMENT 'UEI of ultimate parent entity.',
    parent_company_name                            STRING                         COMMENT 'Parent entity legal name.',
    business_type_cd                               STRING                         COMMENT 'SAM.gov business type code.',
    small_business_flag                            BOOLEAN                        COMMENT 'TRUE if certified small business.',
    woman_owned_flag                               BOOLEAN                        COMMENT 'TRUE if woman-owned small business.',
    veteran_owned_flag                             BOOLEAN                        COMMENT 'TRUE if veteran-owned small business.',
    hubzone_flag                                   BOOLEAN                        COMMENT 'TRUE if HUBZone certified.',
    sdvosb_flag                                    BOOLEAN                        COMMENT 'TRUE if service-disabled veteran-owned.',
    city                                           STRING                         COMMENT 'Contractor city.',
    state_cd                                       STRING                         COMMENT 'Contractor state code.',
    country_cd                                     STRING                         COMMENT 'Contractor country code.',
    zip_code                                       STRING                         COMMENT 'Contractor ZIP code.',
    congressional_district                         STRING                         COMMENT 'Congressional district.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (contractor_sk, award_mod_company_id)
COMMENT 'Contractor identity dimension. SCD Type 2 — tracks UEI/DUNS changes, name changes, business type recertification. Sources: aasbs.award_mod_company.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'alt',
    'pipeline.table' = 'dim_contractor',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP2'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.alt.dim_closeout
-- Acquisition closeout status dimension. SCD Type 1. Sources: aasbs.acquisition_closeout, aasbs.acquis
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.alt.dim_closeout
(
    closeout_sk                                    BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    acquisition_id                                 BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.acquisition_closeout.acquisition_id.',
    co_cert_dt                                     TIMESTAMP                      COMMENT 'Contracting Officer certification date.',
    fma_cert_dt                                    TIMESTAMP                      COMMENT 'Financial Management Analyst certification date.',
    client_cert_dt                                 TIMESTAMP                      COMMENT 'Client agency certification date.',
    final_invoice_dt                               TIMESTAMP                      COMMENT 'Date final invoice received.',
    closeout_complete_dt                           TIMESTAMP                      COMMENT 'Date closeout fully completed.',
    checklist_total_items                          INT                            COMMENT 'Total checklist items for this closeout type.',
    checklist_complete_items                       INT                            COMMENT 'Number of checklist items marked complete.',
    checklist_completion_pct                       DECIMAL(5,2)                   COMMENT 'Percentage of checklist items complete (0.00–100.00).',
    has_open_udo_flag                              BOOLEAN                        COMMENT 'TRUE if UDO balance remains at time of closeout.',
    has_open_invoice_flag                          BOOLEAN                        COMMENT 'TRUE if invoices are still pending.',
    closeout_prohibition_cds                       STRING                         COMMENT 'Comma-delimited open prohibition codes blocking closeout.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (closeout_sk, acquisition_id)
COMMENT 'Acquisition closeout status dimension. SCD Type 1. Sources: aasbs.acquisition_closeout, aasbs.acquisition_closeout_checklist.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'alt',
    'pipeline.table' = 'dim_closeout',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP2'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.alt.fact_award_lifecycle
-- Grain  : CLIN (line_item_sk) × acquisition (acquisition_sk) — current state snapshot
-- Award lifecycle fact — CLIN/SLIN level snapshot of current lifecycle state, phase, and amounts. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.alt.fact_award_lifecycle
(
    lifecycle_sk                                   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item.',
    award_sk                                       BIGINT NOT NULL                COMMENT 'FK to common.dim_award.',
    acquisition_sk                                 BIGINT NOT NULL                COMMENT 'FK to alt.dim_acquisition.',
    solicitation_sk                                BIGINT                         COMMENT 'FK to alt.dim_solicitation.',
    contractor_sk                                  BIGINT                         COMMENT 'FK to alt.dim_contractor (current mod contractor).',
    closeout_sk                                    BIGINT                         COMMENT 'FK to alt.dim_closeout.',
    ia_sk                                          BIGINT NOT NULL                COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    award_date_sk                                  INT                            COMMENT 'FK to common.dim_date — base award date.',
    last_mod_date_sk                               INT                            COMMENT 'FK to common.dim_date — latest modification date.',
    current_proc_phase_cd                          STRING                         COMMENT 'Current procurement phase code (Acquisition/Solicitation/Award/Post-Award/Closeout).',
    current_proc_phase_desc                        STRING                         COMMENT 'Current phase description.',
    days_in_pre_award                              INT                            COMMENT 'Calendar days from acquisition creation to award.',
    days_since_last_mod                            INT                            COMMENT 'Calendar days since most recent modification.',
    days_to_pop_end                                INT                            COMMENT 'Calendar days remaining in period of performance (negative = past PoP).',
    mods_count                                     INT                            COMMENT 'Total modifications issued.',
    total_invoices_count                           INT                            COMMENT 'Total invoices received.',
    pending_invoices_count                         INT                            COMMENT 'Invoices received but not yet accepted.',
    base_award_amt                                 DECIMAL(15,2)                  COMMENT 'Original base award amount.',
    current_obligated_amt                          DECIMAL(15,2)                  COMMENT 'Current total obligated amount.',
    current_accepted_amt                           DECIMAL(15,2)                  COMMENT 'Current total accepted amount.',
    current_udo_amt                                DECIMAL(15,2)                  COMMENT 'Current unliquidated obligation balance.',
    accrual_holdback_amt                           DECIMAL(15,2)                  COMMENT 'FSD-managed holdback from accrual calculations.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, acquisition_sk)
COMMENT 'Award lifecycle fact — CLIN/SLIN level snapshot of current lifecycle state, phase, and amounts. Sources: aasbs.line_item_accepted, aasbs.award_mod, aasbs.accrual_expense.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'alt',
    'pipeline.table' = 'fact_award_lifecycle',
    'pipeline.data_product' = 'DP2',
    'pipeline.consumer' = 'Program Managers, CO Supervisors'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.alt.v_co_active_awards
-- Active awards summary — CO-level portfolio view. Excludes closed and cancelled awards.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.alt.v_co_active_awards
COMMENT 'Active awards summary — CO-level portfolio view. Excludes closed and cancelled awards.'
AS
SELECT
    aw.award_piid, aw.contract_type_desc, aw.award_end_dt,
    ag.agency_name, ia.ia_num, ia.ia_type_desc,
    acq.acquisition_status_desc, acq.competition_type_desc,
    acq.processing_speed_cd,
    COUNT(DISTINCT f.line_item_sk)        AS clin_count,
    SUM(f.current_obligated_amt)          AS total_obligated,
    SUM(f.current_udo_amt)                AS total_udo,
    SUM(f.pending_invoices_count)         AS pending_invoices,
    MIN(f.days_to_pop_end)                AS min_days_to_pop_end
FROM assist_dev.alt.fact_award_lifecycle f
JOIN assist_dev.common.dim_award       aw  ON f.award_sk       = aw.award_sk       AND aw.is_current_flag
JOIN assist_dev.common.dim_ia          ia  ON f.ia_sk           = ia.ia_sk          AND ia.is_current_flag
JOIN assist_dev.common.dim_agency      ag  ON f.agency_sk       = ag.agency_sk      AND ag.is_current_flag
JOIN assist_dev.alt.dim_acquisition    acq ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
WHERE aw.award_status_cd NOT IN ('CLOSED','CANCELLED')
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.alt.v_acquisition_cycle_time
-- Acquisition cycle time analytics — average, median, and P90 pre-award days by type and agency. Suppo
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.alt.v_acquisition_cycle_time
COMMENT 'Acquisition cycle time analytics — average, median, and P90 pre-award days by type and agency. Supports Section 809 Panel reporting.'
AS
SELECT
    acq.acquisition_type_desc,
    acq.competition_type_desc,
    acq.processing_speed_cd,
    ag.agency_name,
    d_award.federal_fiscal_fy                       AS award_fy,
    COUNT(DISTINCT f.acquisition_sk)                AS acquisition_count,
    AVG(f.days_in_pre_award)                        AS avg_days_pre_award,
    PERCENTILE(f.days_in_pre_award, 0.5)            AS median_days_pre_award,
    PERCENTILE(f.days_in_pre_award, 0.9)            AS p90_days_pre_award,
    MIN(f.days_in_pre_award)                        AS min_days_pre_award,
    MAX(f.days_in_pre_award)                        AS max_days_pre_award
FROM assist_dev.alt.fact_award_lifecycle f
JOIN assist_dev.alt.dim_acquisition    acq ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
JOIN assist_dev.common.dim_agency      ag  ON f.agency_sk       = ag.agency_sk       AND ag.is_current_flag
JOIN assist_dev.common.dim_date        d_award ON f.award_date_sk = d_award.date_sk
WHERE f.days_in_pre_award IS NOT NULL
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.alt.v_closeout_status
-- Closeout status dashboard view — checklist completion, blocking conditions, and certification dates 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.alt.v_closeout_status
COMMENT 'Closeout status dashboard view — checklist completion, blocking conditions, and certification dates per award.'
AS
SELECT
    ag.agency_name, ia.ia_num, aw.award_piid,
    c.co_cert_dt, c.fma_cert_dt, c.client_cert_dt,
    c.checklist_completion_pct,
    c.has_open_udo_flag, c.has_open_invoice_flag,
    c.closeout_prohibition_cds,
    f.current_udo_amt,
    f.days_to_pop_end,
    CASE
        WHEN c.closeout_complete_dt IS NOT NULL THEN 'Complete'
        WHEN c.has_open_udo_flag OR c.has_open_invoice_flag THEN 'Blocked'
        WHEN c.checklist_completion_pct >= 100 THEN 'Pending Cert'
        ELSE 'In Progress'
    END AS closeout_status_bucket
FROM assist_dev.alt.fact_award_lifecycle f
JOIN assist_dev.alt.dim_closeout       c   ON f.closeout_sk   = c.closeout_sk
JOIN assist_dev.common.dim_award       aw  ON f.award_sk      = aw.award_sk  AND aw.is_current_flag
JOIN assist_dev.common.dim_ia          ia  ON f.ia_sk         = ia.ia_sk     AND ia.is_current_flag
JOIN assist_dev.common.dim_agency      ag  ON f.agency_sk     = ag.agency_sk AND ag.is_current_flag
WHERE f.closeout_sk IS NOT NULL;
