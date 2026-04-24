-- ==============================================================================
-- DP3 — assist_dev.fre — FPDS Reporting Extract
-- Grain: one row per award modification (Contract Action Report). FAR 4.604 compliance.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.fre.dim_fpds_award
-- FPDS award-level attributes required on every Contract Action Report. SCD Type 1. Sources: aasbs.awa
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.fre.dim_fpds_award
(
    fpds_award_sk                                  BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    award_id                                       BIGINT NOT NULL                COMMENT 'NK — source aasbs.award.id.',
    piid                                           STRING NOT NULL                COMMENT 'Base PIID.',
    idv_piid                                       STRING                         COMMENT 'Referenced IDV PIID (parent vehicle).',
    idv_agency_id                                  STRING                         COMMENT 'IDV issuing agency identifier.',
    referenced_idv_type_cd                         STRING                         COMMENT 'IDV type code — FSS, GWAC, BPA, etc.',
    contract_type_cd                               STRING                         COMMENT 'Contract type code per FPDS data dictionary.',
    bundled_contract_cd                            STRING                         COMMENT 'Bundled contract determination.',
    consolidated_contract_cd                       STRING                         COMMENT 'Consolidated contract determination.',
    multi_year_contract_cd                         STRING                         COMMENT 'Multi-year contract indicator.',
    performance_type_cd                            STRING                         COMMENT 'Performance-based acquisition indicator.',
    who_can_use_cd                                 STRING                         COMMENT 'Agency types eligible to use IDV.',
    fpds_car_status_cd                             STRING                         COMMENT 'FPDS CAR transmission status.',
    last_submitted_dt                              TIMESTAMP                      COMMENT 'Date last CAR submitted to FPDS-NG.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (fpds_award_sk, award_id)
COMMENT 'FPDS award-level attributes required on every Contract Action Report. SCD Type 1. Sources: aasbs.award, aasbs.award_mod.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'fre',
    'pipeline.table' = 'dim_fpds_award',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP3'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.fre.dim_fpds_contractor
-- FPDS contractor dimension. SCD Type 2 — tracks UEI, name, and SBA certification changes across mods.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.fre.dim_fpds_contractor
(
    fpds_contractor_sk                             BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    award_mod_company_id                           BIGINT NOT NULL                COMMENT 'NK — source aasbs.award_mod_company.id.',
    uei                                            STRING                         COMMENT 'Unique Entity Identifier (SAM.gov).',
    duns_num                                       STRING                         COMMENT 'Legacy DUNS (retained for pre-2022 records).',
    cage_code                                      STRING                         COMMENT 'CAGE code.',
    company_name                                   STRING NOT NULL                COMMENT 'Legal entity name.',
    parent_uei                                     STRING                         COMMENT 'Ultimate parent UEI.',
    parent_company_name                            STRING                         COMMENT 'Ultimate parent legal name.',
    sba_program_cd                                 STRING                         COMMENT 'SBA program certification — 8(a), HUBZone, WOSB, etc.',
    bus_type_cd                                    STRING                         COMMENT 'Business type code (multiple allowed).',
    city                                           STRING                         COMMENT 'City.',
    state_cd                                       STRING                         COMMENT 'State code.',
    country_cd                                     STRING                         COMMENT 'Country code.',
    congressional_district                         STRING                         COMMENT 'Congressional district code.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (fpds_contractor_sk, award_mod_company_id)
COMMENT 'FPDS contractor dimension. SCD Type 2 — tracks UEI, name, and SBA certification changes across mods. Sources: aasbs.award_mod_company.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'fre',
    'pipeline.table' = 'dim_fpds_contractor',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP3'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.fre.dim_fpds_agency
-- FPDS agency and contracting office dimension. SCD Type 1. Sources: aasbs.lu_agency, lu_bureau, lu_ac
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.fre.dim_fpds_agency
(
    fpds_agency_sk                                 BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    agency_code                                    STRING NOT NULL                COMMENT 'Two-digit FPDS agency code.',
    bureau_code                                    STRING                         COMMENT 'Bureau code.',
    aac                                            STRING                         COMMENT 'Activity Address Code.',
    cgac                                           STRING                         COMMENT 'Common Government-wide Accounting Classification code.',
    agency_name                                    STRING NOT NULL                COMMENT 'Agency name.',
    bureau_name                                    STRING                         COMMENT 'Bureau name.',
    contracting_office_name                        STRING                         COMMENT 'Contracting office name.',
    funding_agency_code                            STRING                         COMMENT 'Funding agency code (may differ from awarding).',
    funding_bureau_code                            STRING                         COMMENT 'Funding bureau code.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (fpds_agency_sk, agency_code)
COMMENT 'FPDS agency and contracting office dimension. SCD Type 1. Sources: aasbs.lu_agency, lu_bureau, lu_activity_address_code, table_master.lu_federal_agency.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'fre',
    'pipeline.table' = 'dim_fpds_agency',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP3'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.fre.dim_fpds_psc
-- Product and Service Code dimension. SCD Type 1. Source: aasbs.lus_psc, PSC Manual (GSA).
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.fre.dim_fpds_psc
(
    psc_sk                                         BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    psc_cd                                         STRING NOT NULL                COMMENT 'Four-character Product/Service Code.',
    psc_description                                STRING                         COMMENT 'PSC full description.',
    psc_category                                   STRING                         COMMENT 'PSC category — Product or Service.',
    supply_category                                STRING                         COMMENT 'Supply category (Products only).',
    service_category                               STRING                         COMMENT 'Service category (Services only).',
    naics_crosswalk                                STRING                         COMMENT 'Primary NAICS code crosswalk from PSC Manual.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (psc_sk, psc_cd)
COMMENT 'Product and Service Code dimension. SCD Type 1. Source: aasbs.lus_psc, PSC Manual (GSA).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'fre',
    'pipeline.table' = 'dim_fpds_psc',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP3'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.fre.fact_fpds_transaction
-- Grain  : Award modification (award_mod_id) — one CAR per modification
-- FPDS Contract Action Report fact — one row per award modification. Directly feeds FPDS-NG submission
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.fre.fact_fpds_transaction
(
    fpds_transaction_sk                            BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    fpds_award_sk                                  BIGINT NOT NULL                COMMENT 'FK to fre.dim_fpds_award.',
    fpds_contractor_sk                             BIGINT NOT NULL                COMMENT 'FK to fre.dim_fpds_contractor.',
    fpds_agency_sk                                 BIGINT NOT NULL                COMMENT 'FK to fre.dim_fpds_agency (awarding).',
    funding_agency_sk                              BIGINT                         COMMENT 'FK to fre.dim_fpds_agency (funding).',
    psc_sk                                         BIGINT                         COMMENT 'FK to fre.dim_fpds_psc.',
    award_sk                                       BIGINT NOT NULL                COMMENT 'FK to common.dim_award.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    loa_sk                                         BIGINT                         COMMENT 'FK to common.dim_loa (appropriation tracing).',
    action_date_sk                                 INT                            COMMENT 'FK to common.dim_date — mod action date.',
    award_mod_id                                   BIGINT NOT NULL                COMMENT 'Source aasbs.award_mod.id — one row per mod.',
    mod_num                                        STRING                         COMMENT 'Modification number (P00001, P00002, etc.).',
    sf30_mod_type_cd                               STRING                         COMMENT 'SF-30 modification type code.',
    fpds_reason_cd                                 STRING                         COMMENT 'FPDS modification reason code.',
    action_type_cd                                 STRING                         COMMENT 'FPDS action type — A (new), B (order), C (mod), D (term).',
    car_status_cd                                  STRING                         COMMENT 'Contract Action Report status — Draft, Submitted, Accepted, Error.',
    car_submitted_dt                               TIMESTAMP                      COMMENT 'Date CAR submitted to FPDS-NG.',
    car_accepted_dt                                TIMESTAMP                      COMMENT 'Date CAR accepted by FPDS-NG.',
    po_tracking_num                                STRING                         COMMENT 'Pegasys PO tracking number.',
    action_obligation_amt                          DECIMAL(15,2)                  COMMENT 'Dollar change this modification (positive or negative).',
    base_exercised_opts_amt                        DECIMAL(15,2)                  COMMENT 'Cumulative base and exercised options value.',
    base_all_opts_amt                              DECIMAL(15,2)                  COMMENT 'Total potential value including all unexercised options.',
    current_total_value_amt                        DECIMAL(15,2)                  COMMENT 'Current total obligated value.',
    ultimate_contract_value                        DECIMAL(15,2)                  COMMENT 'Ultimate maximum contract value.',
    is_small_business_flag                         BOOLEAN                        COMMENT 'TRUE if contractor is a small business at time of action.',
    competition_type_cd                            STRING                         COMMENT 'Competition type per FPDS — A, B, C, D, E, F.',
    reason_not_competed_cd                         STRING                         COMMENT 'J&A authority for other-than-full-and-open competition.',
    number_of_offers                               INT                            COMMENT 'Number of offers received per FAR 15.101.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (award_mod_id, fpds_award_sk)
COMMENT 'FPDS Contract Action Report fact — one row per award modification. Directly feeds FPDS-NG submission queue and USASpending.gov. Sources: aasbs.award_mod, snap_award_mod_amts, loa, aasbs_transmit.transmittal, po_summary.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'fre',
    'pipeline.table' = 'fact_fpds_transaction',
    'pipeline.data_product' = 'DP3',
    'pipeline.consumer' = 'FPDS-NG, USASpending.gov, OIG'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.fre.v_fpds_submission_queue
-- FPDS submission queue — CARs pending submission or in error state. Supports FPDS-NG operations team 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.fre.v_fpds_submission_queue
COMMENT 'FPDS submission queue — CARs pending submission or in error state. Supports FPDS-NG operations team daily triage.'
AS
SELECT
    f.award_mod_id, f.mod_num, f.car_status_cd,
    fa.piid, fa.fpds_car_status_cd,
    fc.uei, fc.company_name,
    fag.agency_name, fag.aac,
    f.action_obligation_amt, f.action_type_cd,
    d.calendar_date AS action_date,
    f.car_submitted_dt, f.car_accepted_dt,
    DATEDIFF(CURRENT_TIMESTAMP(), f.car_submitted_dt) AS days_since_submission
FROM assist_dev.fre.fact_fpds_transaction f
JOIN assist_dev.fre.dim_fpds_award      fa  ON f.fpds_award_sk      = fa.fpds_award_sk
JOIN assist_dev.fre.dim_fpds_contractor fc  ON f.fpds_contractor_sk = fc.fpds_contractor_sk AND fc.is_current_flag
JOIN assist_dev.fre.dim_fpds_agency     fag ON f.fpds_agency_sk     = fag.fpds_agency_sk
LEFT JOIN assist_dev.common.dim_date    d   ON f.action_date_sk     = d.date_sk
WHERE f.car_status_cd IN ('DRAFT','SUBMITTED','ERROR')
ORDER BY f.car_submitted_dt;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.fre.v_fpds_obligation_by_agency
-- FPDS obligation rollup by agency, FY, PSC, and small business status. Feeds OMB MAX, USASpending.gov
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.fre.v_fpds_obligation_by_agency
COMMENT 'FPDS obligation rollup by agency, FY, PSC, and small business status. Feeds OMB MAX, USASpending.gov, and Agency Annual Reports.'
AS
SELECT
    fag.agency_name, fag.bureau_name,
    d.federal_fiscal_fy, d.federal_fiscal_qtr,
    psc.psc_cd, psc.psc_description, psc.psc_category,
    fc.sba_program_cd,
    f.competition_type_cd, f.reason_not_competed_cd,
    COUNT(*)                              AS transaction_count,
    SUM(f.action_obligation_amt)          AS total_obligations,
    SUM(f.base_all_opts_amt)              AS total_potential_value,
    SUM(CASE WHEN f.is_small_business_flag THEN f.action_obligation_amt ELSE 0 END) AS sb_obligations
FROM assist_dev.fre.fact_fpds_transaction f
JOIN assist_dev.fre.dim_fpds_agency     fag ON f.fpds_agency_sk     = fag.fpds_agency_sk
JOIN assist_dev.fre.dim_fpds_contractor fc  ON f.fpds_contractor_sk = fc.fpds_contractor_sk AND fc.is_current_flag
JOIN assist_dev.fre.dim_fpds_psc        psc ON f.psc_sk             = psc.psc_sk
JOIN assist_dev.common.dim_date         d   ON f.action_date_sk     = d.date_sk
GROUP BY ALL;
