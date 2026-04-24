-- =============================================================================
-- ASSIST OCFO — Gold Layer DDL
-- Catalog  : assist_catalog
-- Schemas  : common | oet | alt | fre | air | tpp | psc | smi | wpr | iat | cat | ref
-- Layer    : Gold — Conformed star schemas, business-ready, consumer-facing
-- Runtime  : Databricks Runtime 14.x LTS
-- Format   : Delta Lake (USING DELTA)
-- Generated: 2026-03-05
--
-- Architecture:
--   ┌──────────────────────────────────────────────────────────────────┐
--   │  assist_catalog.common  — shared dimensions used by 2+ products  │
--   │  assist_catalog.oet     — DP1  Obligation & Expenditure Tracker  │
--   │  assist_catalog.alt     — DP2  Award Lifecycle Tracker           │
--   │  assist_catalog.fre     — DP3  FPDS Reporting Extract            │
--   │  assist_catalog.air     — DP4  Accrual Income & GSA Revenue      │
--   │  assist_catalog.tpp     — DP5  Treasury Transmission Pipeline    │
--   │  assist_catalog.psc     — DP6  CLIN Pricing & Service Charges    │
--   │  assist_catalog.smi     — DP7  Solicitation & Market Intel       │
--   │  assist_catalog.wpr     — DP8  Workforce & Personnel Registry    │
--   │  assist_catalog.iat     — DP9  IA Lifecycle & Amendment Tracker  │
--   │  assist_catalog.cat     — DP10 Collaboration, Review & Audit     │
--   │  assist_catalog.ref     — DP11 Conformed Reference Data Catalog  │
--   └──────────────────────────────────────────────────────────────────┘
--
-- Design patterns:
--   • Star schema per data product — fact + conformed dimensions
--   • Surrogate keys (BIGINT GENERATED ALWAYS AS IDENTITY) on all dims & facts
--   • SCD Type 2 on slowly changing dimensions (eff_start_dt / eff_end_dt / is_current_flag)
--   • SCD Type 1 on reference/lookup tables (simple overwrite)
--   • Shared dimensions in assist_catalog.common — referenced by FK across products
--   • Delta Liquid Clustering replaces PARTITION BY (DBR 14 best practice)
--   • delta.enableChangeDataFeed = true — feeds downstream MERGE operations
--   • Three standard gold audit columns on every table
--   • Views defined as CREATE OR REPLACE VIEW with business-readable SQL
--   • No physical FK enforcement — documented in COMMENT, enforced at query layer
--   • All monetary amounts in DECIMAL(15,2) — USG FMIS standard
--
-- Standard Gold Audit Columns (every table):
--   _gold_created_at  TIMESTAMP  — when first written to Gold
--   _gold_updated_at  TIMESTAMP  — when last updated in Gold
--   _source_batch_id  STRING     — Databricks job_run_id for tracing
--
-- SCD2 Columns (slowly changing dimensions only):
--   eff_start_dt      TIMESTAMP NOT NULL — version effective from
--   eff_end_dt        TIMESTAMP          — version effective to (NULL = current)
--   is_current_flag   BOOLEAN NOT NULL   — TRUE for single current row per NK
-- =============================================================================

-- Pre-requisites: catalog + all schemas
CREATE CATALOG IF NOT EXISTS assist_catalog
    COMMENT 'ASSIST OCFO Federal Acquisition data platform — Unity Catalog root';

CREATE SCHEMA IF NOT EXISTS assist_catalog.common
    COMMENT 'Shared conformed dimensions used across two or more Gold data products.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.oet
    COMMENT 'DP1 — Obligation and Expenditure Tracker. Grain: CLIN x LOA x period.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.alt
    COMMENT 'DP2 — Award Lifecycle Tracker. Grain: CLIN/SLIN per award modification.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.fre
    COMMENT 'DP3 — FPDS Reporting Extract. Grain: one award modification (CAR).';
CREATE SCHEMA IF NOT EXISTS assist_catalog.air
    COMMENT 'DP4 — Accrual Income and GSA Revenue. Grain: CLIN x LOA x accrual month.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.tpp
    COMMENT 'DP5 — Treasury Transmission and Payment Pipeline. Grain: transmittal batch.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.psc
    COMMENT 'DP6 — CLIN Pricing and Service Charge Catalog. Grain: service charge schedule period.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.smi
    COMMENT 'DP7 — Solicitation and Market Intelligence. Grain: contractor response per solicitation.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.wpr
    COMMENT 'DP8 — Workforce and Personnel Registry. Grain: user role assignment per entity.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.iat
    COMMENT 'DP9 — IA Lifecycle and Amendment Tracker. Grain: IA amendment.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.cat
    COMMENT 'DP10 — Collaboration, Review and Audit Trail. Grain: lifecycle chronology event.';
CREATE SCHEMA IF NOT EXISTS assist_catalog.ref
    COMMENT 'DP11 — Conformed Reference Data Catalog. Grain: lookup code per domain.';


-- ==============================================================================
-- SHARED COMMON DIMENSIONS — assist_catalog.common
-- Used by 2 or more data products. Referenced via FK surrogate key.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_date
-- Date dimension — full calendar and federal fiscal year spine. Covers FY2000–FY2035.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_date
(
    date_sk                                        INT NOT NULL                   COMMENT 'Surrogate key — YYYYMMDD integer format (e.g. 20241001).',
    calendar_date                                  DATE NOT NULL                  COMMENT 'Calendar date.',
    day_of_week                                    STRING NOT NULL                COMMENT 'Full day name (Monday … Sunday).',
    day_of_week_num                                INT NOT NULL                   COMMENT 'Day of week number (1=Monday, 7=Sunday).',
    day_of_month                                   INT NOT NULL                   COMMENT 'Day of month (1–31).',
    day_of_year                                    INT NOT NULL                   COMMENT 'Day of year (1–366).',
    week_of_year                                   INT NOT NULL                   COMMENT 'ISO week number of year.',
    calendar_month                                 INT NOT NULL                   COMMENT 'Calendar month (1–12).',
    month_name                                     STRING NOT NULL                COMMENT 'Full month name (January … December).',
    calendar_quarter                               INT NOT NULL                   COMMENT 'Calendar quarter (1–4).',
    calendar_year                                  INT NOT NULL                   COMMENT 'Calendar year (e.g. 2024).',
    federal_fiscal_fy                              INT NOT NULL                   COMMENT 'Federal fiscal year (Oct 1 start). FY2025 = Oct 2024 – Sep 2025.',
    federal_fiscal_qtr                             INT NOT NULL                   COMMENT 'Federal fiscal quarter within FY (1–4).',
    federal_fiscal_month                           INT NOT NULL                   COMMENT 'Federal fiscal month within FY (1–12, 1=October).',
    is_weekend_flag                                BOOLEAN NOT NULL               COMMENT 'TRUE if Saturday or Sunday.',
    is_federal_holiday                             BOOLEAN NOT NULL               COMMENT 'TRUE if a recognized US federal holiday.',
    is_last_day_of_month                           BOOLEAN NOT NULL               COMMENT 'TRUE if last calendar day of the month.',
    is_last_day_of_fy                              BOOLEAN NOT NULL               COMMENT 'TRUE if September 30 (last day of federal fiscal year).',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (date_sk, calendar_year)
COMMENT 'Date dimension — full calendar and federal fiscal year spine. Covers FY2000–FY2035.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_date',
    'scd.type' = '1',
    'pipeline.source' = 'generated'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_agency
-- Federal agency and activity address dimension. SCD Type 2 — tracks agency name and bureau changes. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_agency
(
    agency_sk                                      BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    agency_code                                    STRING NOT NULL                COMMENT 'Two-digit federal agency code (FPDS/SAM.gov).',
    bureau_code                                    STRING                         COMMENT 'Sub-agency / bureau code.',
    activity_address_cd                            STRING                         COMMENT 'Activity Address Code (AAC) — 6-digit DoD/GSA office identifier.',
    agency_name                                    STRING NOT NULL                COMMENT 'Full agency name as published in SAM.gov.',
    bureau_name                                    STRING                         COMMENT 'Bureau or sub-agency name.',
    aac_description                                STRING                         COMMENT 'Activity address description.',
    department_code                                STRING                         COMMENT 'Department-level code.',
    department_name                                STRING                         COMMENT 'Department-level name.',
    treasury_symbol                                STRING                         COMMENT 'Treasury account symbol prefix for this agency.',
    is_omb_agency_flag                             BOOLEAN                        COMMENT 'TRUE if agency appears in OMB MAX agency list.',
    is_intel_community                             BOOLEAN                        COMMENT 'TRUE if agency is a member of the Intelligence Community per DNI.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (agency_sk, agency_code)
COMMENT 'Federal agency and activity address dimension. SCD Type 2 — tracks agency name and bureau changes. Sources: aasbs.lu_agency, lu_bureau, lu_activity_address_code, table_master.lu_federal_agency.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_agency',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.lu_agency'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_ia
-- Interagency Agreement dimension. SCD Type 2 — tracks status, cost, and period changes. Sources: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_ia
(
    ia_sk                                          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    ia_id                                          BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.ia.id.',
    ia_num                                         STRING NOT NULL                COMMENT 'IA number / PIID (e.g. GS00Q17NSD3001).',
    ia_type_cd                                     STRING                         COMMENT 'IA type code — GT&C, Order, Intra-Agency.',
    ia_type_desc                                   STRING                         COMMENT 'IA type description decoded from lu_ia_type.',
    ia_status_cd                                   STRING                         COMMENT 'Current IA status code.',
    ia_status_desc                                 STRING                         COMMENT 'IA status description decoded.',
    instrument_type_cd                             STRING                         COMMENT 'Instrument type code — MIPR, Economy Act, etc.',
    instrument_type_desc                           STRING                         COMMENT 'Instrument type description.',
    fiscal_year                                    INT                            COMMENT 'IA fiscal year.',
    ia_start_dt                                    TIMESTAMP                      COMMENT 'IA period of performance start date.',
    ia_end_dt                                      TIMESTAMP                      COMMENT 'IA period of performance end date.',
    total_direct_cost_est_amt                      DECIMAL(15,2)                  COMMENT 'Total estimated direct cost of IA.',
    total_charges_est_amt                          DECIMAL(15,2)                  COMMENT 'Total estimated charges (including service charges).',
    servicing_agency_sk                            BIGINT                         COMMENT 'FK to dim_agency — servicing/executing agency.',
    requesting_agency_sk                           BIGINT                         COMMENT 'FK to dim_agency — requesting/client agency.',
    program_cd                                     STRING                         COMMENT 'GSA program code (AAS, ITS_NSD, etc.).',
    region_cd                                      STRING                         COMMENT 'GSA region code.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (ia_sk, ia_id)
COMMENT 'Interagency Agreement dimension. SCD Type 2 — tracks status, cost, and period changes. Sources: aasbs.ia.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_ia',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.ia'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_award
-- Award and contract dimension. SCD Type 2 — reflects each modification. Sources: aasbs.award, aasbs.a
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_award
(
    award_sk                                       BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    award_id                                       BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.award.id.',
    award_piid                                     STRING NOT NULL                COMMENT 'Procurement Instrument Identifier (PIID).',
    award_mod_num                                  STRING                         COMMENT 'Current modification number.',
    award_mod_id                                   BIGINT                         COMMENT 'Source aasbs.award_mod.id of latest modification.',
    fin_code                                       STRING                         COMMENT 'Financial Identification Number (FIN/ACT number).',
    award_status_cd                                STRING                         COMMENT 'Award status code.',
    award_status_desc                              STRING                         COMMENT 'Award status description decoded.',
    contract_type_cd                               STRING                         COMMENT 'Contract type code — FFP, T&M, CPFF, CPAF, etc.',
    contract_type_desc                             STRING                         COMMENT 'Contract type description.',
    vehicle_type_cd                                STRING                         COMMENT 'Contract vehicle type code.',
    vehicle_type_desc                              STRING                         COMMENT 'Vehicle type description.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia — governing interagency agreement.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency — administering agency.',
    award_start_dt                                 TIMESTAMP                      COMMENT 'Award period of performance start date.',
    award_end_dt                                   TIMESTAMP                      COMMENT 'Award period of performance end date (current mod).',
    base_award_dt                                  TIMESTAMP                      COMMENT 'Date of original (base) award.',
    last_mod_dt                                    TIMESTAMP                      COMMENT 'Date of most recent modification.',
    total_mods_count                               INT                            COMMENT 'Total number of modifications against this award.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (award_sk, award_id)
COMMENT 'Award and contract dimension. SCD Type 2 — reflects each modification. Sources: aasbs.award, aasbs.award_mod.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_award',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.award'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_line_item
-- CLIN/SLIN dimension — one row per contract line item per effective version. SCD Type 2. Sources: aas
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_line_item
(
    line_item_sk                                   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_id                                   BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.line_item.id.',
    line_item_accepted_id                          BIGINT                         COMMENT 'Source aasbs.line_item_accepted.id — accepted instance.',
    li_tracking_num                                BIGINT                         COMMENT 'CLIN/SLIN tracking number — stable across mods.',
    award_sk                                       BIGINT                         COMMENT 'FK to common.dim_award.',
    clin_num                                       STRING                         COMMENT 'Contract Line Item Number (e.g. 0001, 0002AA).',
    slin_num                                       STRING                         COMMENT 'Sub-Line Item Number if applicable.',
    line_item_type_cd                              STRING                         COMMENT 'CLIN type code — Deliverable, Service Charge, Travel, etc.',
    line_item_type_desc                            STRING                         COMMENT 'CLIN type description decoded.',
    psc_cd                                         STRING                         COMMENT 'Product/Service Code (PSC) — 4-character FAR code.',
    unit_of_measure_cd                             STRING                         COMMENT 'Unit of measure code.',
    contract_type_cd                               STRING                         COMMENT 'CLIN-level contract type override.',
    severability_type_cd                           STRING                         COMMENT 'Severability classification — Severable, Nonseverable, Mix.',
    li_pop_start_dt                                TIMESTAMP                      COMMENT 'CLIN period of performance start date.',
    li_pop_end_dt                                  TIMESTAMP                      COMMENT 'CLIN period of performance end date.',
    obligated_ceiling_amt                          DECIMAL(15,2)                  COMMENT 'Total obligated ceiling amount for this CLIN.',
    exercise_this_yn                               BOOLEAN                        COMMENT 'TRUE if this option CLIN has been exercised.',
    bona_fide_need_fy                              INT                            COMMENT 'Fiscal year of bona fide need.',
    budget_fy                                      INT                            COMMENT 'Budget fiscal year.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, line_item_id)
COMMENT 'CLIN/SLIN dimension — one row per contract line item per effective version. SCD Type 2. Sources: aasbs.line_item, aasbs.line_item_accepted.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_line_item',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.line_item'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_loa
-- Line of Accounting (LOA) dimension. SCD Type 2 — tracks fund status and agreement changes. Sources: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_loa
(
    loa_sk                                         BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    loa_id                                         BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.loa.id.',
    tracking_num                                   BIGINT                         COMMENT 'LOA tracking number — unique per LOA string.',
    loa_status_cd                                  STRING                         COMMENT 'LOA status code — Active, Expired, Cancelled.',
    loa_status_desc                                STRING                         COMMENT 'LOA status description decoded.',
    agreement_number                               STRING                         COMMENT 'BAAR/IPAC agreement number for this LOA.',
    agreement_line_num                             STRING                         COMMENT 'Agreement line number within BAAR agreement.',
    treasury_account_symbol                        STRING                         COMMENT 'Full Treasury Account Symbol (TAS).',
    fund_cd                                        STRING                         COMMENT 'Fund code — 285X, 285F, etc.',
    fund_type_cd                                   STRING                         COMMENT 'Fund type — Annual, Multi-year, No-year.',
    object_class_cd                                STRING                         COMMENT 'OMB Object Classification code (3-digit).',
    program_activity_cd                            STRING                         COMMENT 'Program activity code.',
    budget_activity_cd                             STRING                         COMMENT 'Budget activity code.',
    org_code                                       STRING                         COMMENT 'Organization code for accounting.',
    cost_center                                    STRING                         COMMENT 'Cost center code.',
    project_code                                   STRING                         COMMENT 'Project code.',
    bbfy                                           INT                            COMMENT 'Beginning Budget Fiscal Year for multi-year funds.',
    ebfy                                           INT                            COMMENT 'Ending Budget Fiscal Year for multi-year funds.',
    agreement_end_dt                               TIMESTAMP                      COMMENT 'Agreement expiration date.',
    transmitted_dt                                 TIMESTAMP                      COMMENT 'Date LOA was transmitted to BAAR.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (loa_sk, loa_id)
COMMENT 'Line of Accounting (LOA) dimension. SCD Type 2 — tracks fund status and agreement changes. Sources: aasbs.loa.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_loa',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.loa'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.dim_funding
-- Funding package dimension. SCD Type 2 — tracks amendment changes. Sources: aasbs.funding, aasbs.fund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.dim_funding
(
    funding_sk                                     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    funding_id                                     BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.funding.id.',
    funding_amendment_id                           BIGINT                         COMMENT 'Latest amendment ID — source aasbs.funding_amendment.id.',
    fund_status_cd                                 STRING                         COMMENT 'Funding status code.',
    fund_status_desc                               STRING                         COMMENT 'Funding status description decoded.',
    fund_category_cd                               STRING                         COMMENT 'Fund category code.',
    fund_category_desc                             STRING                         COMMENT 'Fund category description decoded.',
    billing_type_cd                                STRING                         COMMENT 'Billing type — AD (Advance Deposit) or RA (Reimbursable Agreement).',
    billing_type_desc                              STRING                         COMMENT 'Billing type description.',
    fund_type_cd                                   STRING                         COMMENT 'Fund type — Annual, Multi-year, No-year.',
    fund_type_desc                                 STRING                         COMMENT 'Fund type description.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency — funding agency.',
    total_funded_amt                               DECIMAL(15,2)                  COMMENT 'Total amount funded in this package.',
    fiscal_year                                    INT                            COMMENT 'Fiscal year of funding package.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (funding_sk, funding_id)
COMMENT 'Funding package dimension. SCD Type 2 — tracks amendment changes. Sources: aasbs.funding, aasbs.funding_amendment.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'dim_funding',
    'scd.type' = '2',
    'pipeline.source' = 'aasbs.funding'
);


-- ==============================================================================
-- DP1 — assist_catalog.oet — Obligation & Expenditure Tracker
-- Grain: one row per CLIN × LOA × accounting period. Primary CFO financial reporting product.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.oet.fact_obligation_expenditure
-- Grain  : CLIN (line_item_sk) × LOA (loa_sk) × fiscal year/period
-- Central financial fact table — CLIN x LOA x period. All USG obligation, expenditure, and billing amo
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.oet.fact_obligation_expenditure
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
-- VIEW: assist_catalog.oet.v_cfo_obligation_summary
-- CFO summary view — obligation, acceptance, billing, and UDO aggregated by agency, IA, CLIN, LOA. Sup
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.oet.v_cfo_obligation_summary
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
FROM assist_catalog.oet.fact_obligation_expenditure f
JOIN assist_catalog.common.dim_agency   a   ON f.agency_sk       = a.agency_sk   AND a.is_current_flag
JOIN assist_catalog.common.dim_ia       ia  ON f.ia_sk           = ia.ia_sk      AND ia.is_current_flag
JOIN assist_catalog.common.dim_line_item li  ON f.line_item_sk   = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_loa      loa ON f.loa_sk          = loa.loa_sk    AND loa.is_current_flag
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.oet.v_clin_expenditure_detail
-- CLIN-level expenditure detail view — denormalized for FSD analyst drill-down. All amounts, dates, an
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.oet.v_clin_expenditure_detail
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
FROM assist_catalog.oet.fact_obligation_expenditure f
JOIN assist_catalog.common.dim_line_item li  ON f.line_item_sk = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_loa      loa ON f.loa_sk        = loa.loa_sk     AND loa.is_current_flag
JOIN assist_catalog.common.dim_award    aw  ON f.award_sk      = aw.award_sk    AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia       ia  ON f.ia_sk         = ia.ia_sk       AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency   ag  ON f.agency_sk     = ag.agency_sk   AND ag.is_current_flag
LEFT JOIN assist_catalog.common.dim_date d  ON f.snapshot_date_sk = d.date_sk;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.oet.v_udo_aging
-- UDO aging view — unliquidated obligation balances bucketed by days past period of performance. Suppo
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.oet.v_udo_aging
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
FROM assist_catalog.oet.fact_obligation_expenditure f
JOIN assist_catalog.common.dim_line_item li ON f.line_item_sk = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_award    aw  ON f.award_sk     = aw.award_sk    AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia       ia  ON f.ia_sk        = ia.ia_sk       AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency   ag  ON f.agency_sk    = ag.agency_sk   AND ag.is_current_flag
WHERE f.udo_amt <> 0
GROUP BY ALL
ORDER BY udo_amt DESC;


-- ==============================================================================
-- DP2 — assist_catalog.alt — Award Lifecycle Tracker
-- Grain: CLIN/SLIN per award. Pre-award through closeout lifecycle.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.alt.dim_acquisition
-- Pre-award acquisition context dimension. SCD Type 2. Sources: aasbs.acquisition, aasbs.acquisition_p
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.alt.dim_acquisition
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
-- assist_catalog.alt.dim_solicitation
-- Solicitation dimension. SCD Type 1 — status and response counts updated in place. Sources: aasbs.sol
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.alt.dim_solicitation
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
-- assist_catalog.alt.dim_contractor
-- Contractor identity dimension. SCD Type 2 — tracks UEI/DUNS changes, name changes, business type rec
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.alt.dim_contractor
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
-- assist_catalog.alt.dim_closeout
-- Acquisition closeout status dimension. SCD Type 1. Sources: aasbs.acquisition_closeout, aasbs.acquis
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.alt.dim_closeout
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
-- assist_catalog.alt.fact_award_lifecycle
-- Grain  : CLIN (line_item_sk) × acquisition (acquisition_sk) — current state snapshot
-- Award lifecycle fact — CLIN/SLIN level snapshot of current lifecycle state, phase, and amounts. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.alt.fact_award_lifecycle
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
-- VIEW: assist_catalog.alt.v_co_active_awards
-- Active awards summary — CO-level portfolio view. Excludes closed and cancelled awards.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.alt.v_co_active_awards
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
FROM assist_catalog.alt.fact_award_lifecycle f
JOIN assist_catalog.common.dim_award       aw  ON f.award_sk       = aw.award_sk       AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia          ia  ON f.ia_sk           = ia.ia_sk          AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency      ag  ON f.agency_sk       = ag.agency_sk      AND ag.is_current_flag
JOIN assist_catalog.alt.dim_acquisition    acq ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
WHERE aw.award_status_cd NOT IN ('CLOSED','CANCELLED')
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.alt.v_acquisition_cycle_time
-- Acquisition cycle time analytics — average, median, and P90 pre-award days by type and agency. Suppo
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.alt.v_acquisition_cycle_time
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
FROM assist_catalog.alt.fact_award_lifecycle f
JOIN assist_catalog.alt.dim_acquisition    acq ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
JOIN assist_catalog.common.dim_agency      ag  ON f.agency_sk       = ag.agency_sk       AND ag.is_current_flag
JOIN assist_catalog.common.dim_date        d_award ON f.award_date_sk = d_award.date_sk
WHERE f.days_in_pre_award IS NOT NULL
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.alt.v_closeout_status
-- Closeout status dashboard view — checklist completion, blocking conditions, and certification dates 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.alt.v_closeout_status
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
FROM assist_catalog.alt.fact_award_lifecycle f
JOIN assist_catalog.alt.dim_closeout       c   ON f.closeout_sk   = c.closeout_sk
JOIN assist_catalog.common.dim_award       aw  ON f.award_sk      = aw.award_sk  AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia          ia  ON f.ia_sk         = ia.ia_sk     AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency      ag  ON f.agency_sk     = ag.agency_sk AND ag.is_current_flag
WHERE f.closeout_sk IS NOT NULL;


-- ==============================================================================
-- DP3 — assist_catalog.fre — FPDS Reporting Extract
-- Grain: one row per award modification (Contract Action Report). FAR 4.604 compliance.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.fre.dim_fpds_award
-- FPDS award-level attributes required on every Contract Action Report. SCD Type 1. Sources: aasbs.awa
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.fre.dim_fpds_award
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
-- assist_catalog.fre.dim_fpds_contractor
-- FPDS contractor dimension. SCD Type 2 — tracks UEI, name, and SBA certification changes across mods.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.fre.dim_fpds_contractor
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
-- assist_catalog.fre.dim_fpds_agency
-- FPDS agency and contracting office dimension. SCD Type 1. Sources: aasbs.lu_agency, lu_bureau, lu_ac
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.fre.dim_fpds_agency
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
-- assist_catalog.fre.dim_fpds_psc
-- Product and Service Code dimension. SCD Type 1. Source: aasbs.lus_psc, PSC Manual (GSA).
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.fre.dim_fpds_psc
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
-- assist_catalog.fre.fact_fpds_transaction
-- Grain  : Award modification (award_mod_id) — one CAR per modification
-- FPDS Contract Action Report fact — one row per award modification. Directly feeds FPDS-NG submission
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.fre.fact_fpds_transaction
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
-- VIEW: assist_catalog.fre.v_fpds_submission_queue
-- FPDS submission queue — CARs pending submission or in error state. Supports FPDS-NG operations team 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.fre.v_fpds_submission_queue
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
FROM assist_catalog.fre.fact_fpds_transaction f
JOIN assist_catalog.fre.dim_fpds_award      fa  ON f.fpds_award_sk      = fa.fpds_award_sk
JOIN assist_catalog.fre.dim_fpds_contractor fc  ON f.fpds_contractor_sk = fc.fpds_contractor_sk AND fc.is_current_flag
JOIN assist_catalog.fre.dim_fpds_agency     fag ON f.fpds_agency_sk     = fag.fpds_agency_sk
LEFT JOIN assist_catalog.common.dim_date    d   ON f.action_date_sk     = d.date_sk
WHERE f.car_status_cd IN ('DRAFT','SUBMITTED','ERROR')
ORDER BY f.car_submitted_dt;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.fre.v_fpds_obligation_by_agency
-- FPDS obligation rollup by agency, FY, PSC, and small business status. Feeds OMB MAX, USASpending.gov
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.fre.v_fpds_obligation_by_agency
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
FROM assist_catalog.fre.fact_fpds_transaction f
JOIN assist_catalog.fre.dim_fpds_agency     fag ON f.fpds_agency_sk     = fag.fpds_agency_sk
JOIN assist_catalog.fre.dim_fpds_contractor fc  ON f.fpds_contractor_sk = fc.fpds_contractor_sk AND fc.is_current_flag
JOIN assist_catalog.fre.dim_fpds_psc        psc ON f.psc_sk             = psc.psc_sk
JOIN assist_catalog.common.dim_date         d   ON f.action_date_sk     = d.date_sk
GROUP BY ALL;


-- ==============================================================================
-- DP4 — assist_catalog.air — Accrual Income & GSA Revenue Tracker
-- Grain: CLIN x LOA x accrual month. FASAB SFFAS 7 revenue recognition. BAAR transmission.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.air.dim_accrual_period
-- Accrual period dimension — monthly federal fiscal calendar. SCD Type 1.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.air.dim_accrual_period
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
-- assist_catalog.air.dim_onefund_program
-- OneFund program dimension — AAC to OneFund fund/activity/program/BAAR doc type mapping. SCD Type 1. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.air.dim_onefund_program
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
-- assist_catalog.air.fact_accrual_income
-- Grain  : CLIN (line_item_sk) × LOA (loa_sk) × accrual month (accrual_period_sk)
-- Accrual income fact — monthly GSA revenue earned per CLIN x LOA. Feeds CFO P&L and BAAR transmission
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.air.fact_accrual_income
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
-- VIEW: assist_catalog.air.v_revenue_vs_expense
-- Revenue vs expense matching view — compares GSA income accrual to expense accrual by IA and period. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.air.v_revenue_vs_expense
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
FROM assist_catalog.air.fact_accrual_income ai
JOIN assist_catalog.air.dim_accrual_period ap ON ai.accrual_period_sk = ap.accrual_period_sk
JOIN assist_catalog.common.dim_ia          ia ON ai.ia_sk             = ia.ia_sk AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency      ag ON ai.agency_sk         = ag.agency_sk AND ag.is_current_flag
LEFT JOIN assist_catalog.oet.fact_obligation_expenditure oe
    ON ai.line_item_sk = oe.line_item_sk AND ai.loa_sk = oe.loa_sk
    AND oe.fiscal_year = ap.accrual_fy AND oe.fiscal_month = ap.accrual_fy_month
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.air.v_baar_transmission_status
-- BAAR transmission status dashboard — accrual amounts by transmission state per period and agency.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.air.v_baar_transmission_status
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
FROM assist_catalog.air.fact_accrual_income ai
JOIN assist_catalog.air.dim_accrual_period  ap  ON ai.accrual_period_sk = ap.accrual_period_sk
JOIN assist_catalog.air.dim_onefund_program on2 ON ai.onefund_sk        = on2.onefund_sk
JOIN assist_catalog.common.dim_ia           ia  ON ai.ia_sk             = ia.ia_sk AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency       ag  ON ai.agency_sk         = ag.agency_sk AND ag.is_current_flag
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.air.v_accrual_holdback_overrides
-- FSD accrual holdback and override audit view — all CLINs with manual exclusions or holdback amounts 
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.air.v_accrual_holdback_overrides
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
FROM assist_catalog.air.fact_accrual_income ai
JOIN assist_catalog.air.dim_accrual_period  ap ON ai.accrual_period_sk  = ap.accrual_period_sk
JOIN assist_catalog.common.dim_line_item    li ON ai.line_item_sk        = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_ia           ia ON ai.ia_sk               = ia.ia_sk AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency       ag ON ai.agency_sk           = ag.agency_sk AND ag.is_current_flag
WHERE ai.inclusion_override_flag = TRUE OR ai.holdback_amt <> 0;


-- ==============================================================================
-- DP5 — assist_catalog.tpp — Treasury Transmission & Payment Pipeline
-- Grain: transmittal batch event. Prompt Payment Act compliance. IPAC reconciliation.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.dim_transmittal_type
-- Transmittal type and specification dimension. SCD Type 1. Sources: aasbs_transmit.transmittal_envelo
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.dim_transmittal_type
(
    transmittal_type_sk                            BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    transmittal_type_cd                            STRING NOT NULL                COMMENT 'Transmittal type code — PO, RR, INV, BAAR, etc.',
    transmittal_type_desc                          STRING                         COMMENT 'Transmittal type description.',
    flat_file_code                                 STRING                         COMMENT 'Flat-file format code used in Pegasys transmissions.',
    spec_version                                   STRING                         COMMENT 'Current active specification version.',
    envelope_format                                STRING                         COMMENT 'Envelope structure format — EDI, CSV, XML.',
    poc_name                                       STRING                         COMMENT 'Point of contact name for this transmittal type.',
    poc_email                                      STRING                         COMMENT 'Point of contact email.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (transmittal_type_sk, transmittal_type_cd)
COMMENT 'Transmittal type and specification dimension. SCD Type 1. Sources: aasbs_transmit.transmittal_envelope, transmittal_specification, transmittal_spec_version, transmittal_poc.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_transmittal_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.dim_aac_envelope
-- AAC-level transmission envelope dimension. SCD Type 1. Sources: aasbs_transmit.aac_envelope, transmi
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.dim_aac_envelope
(
    aac_envelope_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    aac                                            STRING NOT NULL                COMMENT 'Activity Address Code — natural key.',
    envelope_status_cd                             STRING                         COMMENT 'Envelope status code.',
    poc_name                                       STRING                         COMMENT 'Transmission point of contact name.',
    poc_email                                      STRING                         COMMENT 'Transmission point of contact email.',
    last_transmission_dt                           TIMESTAMP                      COMMENT 'Date of most recent successful transmission.',
    is_suspended_flag                              BOOLEAN                        COMMENT 'TRUE if transmissions currently suspended for this AAC.',
    suspension_start_dt                            TIMESTAMP                      COMMENT 'Suspension window start date.',
    suspension_end_dt                              TIMESTAMP                      COMMENT 'Suspension window end date.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (aac_envelope_sk, aac)
COMMENT 'AAC-level transmission envelope dimension. SCD Type 1. Sources: aasbs_transmit.aac_envelope, transmittal_suspension.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_aac_envelope',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.dim_agreement
-- BAAR agreement dimension. SCD Type 1. Sources: agreement.agreement, agreement_line, agreement_log.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.dim_agreement
(
    agreement_sk                                   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    agreement_id                                   BIGINT NOT NULL                COMMENT 'NK — source agreement.agreement.id.',
    agreement_number                               STRING NOT NULL                COMMENT 'BAAR agreement number.',
    fund_status_cd                                 STRING                         COMMENT 'Agreement fund status.',
    obligations_available                          DECIMAL(15,2)                  COMMENT 'Available obligation authority remaining.',
    accounting_period                              STRING                         COMMENT 'BAAR accounting period.',
    total_agreement_amt                            DECIMAL(15,2)                  COMMENT 'Total agreement ceiling amount.',
    line_count                                     INT                            COMMENT 'Number of agreement lines.',
    last_transmitted_dt                            TIMESTAMP                      COMMENT 'Date agreement last transmitted to BAAR.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (agreement_sk, agreement_id)
COMMENT 'BAAR agreement dimension. SCD Type 1. Sources: agreement.agreement, agreement_line, agreement_log.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_agreement',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.dim_receiving_report
-- Receiving report dimension. SCD Type 1. Sources: aasbs_transmit.rr_summary, rr_detail.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.dim_receiving_report
(
    rr_sk                                          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    rr_summary_id                                  BIGINT NOT NULL                COMMENT 'NK — source aasbs_transmit.rr_summary.id.',
    pegasys_invoice_num                            STRING                         COMMENT 'Pegasys invoice number from receiving report.',
    rr_acceptance_dt                               TIMESTAMP                      COMMENT 'Receiving report acceptance date.',
    total_rr_amt                                   DECIMAL(15,2)                  COMMENT 'Total receiving report amount.',
    rr_line_count                                  INT                            COMMENT 'Number of lines on this receiving report.',
    mdl_amt                                        DECIMAL(15,2)                  COMMENT 'Maximum Dollar Limit amount per RR line.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (rr_sk, rr_summary_id)
COMMENT 'Receiving report dimension. SCD Type 1. Sources: aasbs_transmit.rr_summary, rr_detail.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_receiving_report',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.fact_transmission_event
-- Grain  : Transmittal batch (source_billing_summary_id) × transmission attempt
-- Treasury transmission event fact — one row per batch transmission attempt. Sources: aasbs_transmit.b
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.fact_transmission_event
(
    transmission_event_sk                          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    transmittal_type_sk                            BIGINT NOT NULL                COMMENT 'FK to tpp.dim_transmittal_type.',
    aac_envelope_sk                                BIGINT                         COMMENT 'FK to tpp.dim_aac_envelope.',
    agreement_sk                                   BIGINT                         COMMENT 'FK to tpp.dim_agreement.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    event_date_sk                                  INT                            COMMENT 'FK to common.dim_date — transmission date.',
    source_billing_summary_id                      BIGINT                         COMMENT 'Source aasbs_transmit.billing_summary.id.',
    transmittal_stage_cd                           STRING                         COMMENT 'Transmission pipeline stage — Staged, Transmitted, Confirmed, Error.',
    transmittal_status_cd                          STRING                         COMMENT 'Transmission status code.',
    transmitted_dt                                 TIMESTAMP                      COMMENT 'Timestamp transmission sent to Pegasys/VITAP.',
    confirmed_dt                                   TIMESTAMP                      COMMENT 'Timestamp confirmation received.',
    error_code                                     STRING                         COMMENT 'Error code if transmission failed.',
    error_description                              STRING                         COMMENT 'Error description text.',
    retry_count                                    INT                            COMMENT 'Number of retry attempts for failed transmissions.',
    batch_record_count                             INT                            COMMENT 'Number of records in this transmission batch.',
    transmitted_amt                                DECIMAL(15,2)                  COMMENT 'Total dollar amount in this transmission batch.',
    confirmed_amt                                  DECIMAL(15,2)                  COMMENT 'Amount confirmed by receiving system.',
    variance_amt                                   DECIMAL(15,2)                  COMMENT 'Variance between transmitted and confirmed amounts.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (transmittal_type_sk, aac_envelope_sk, event_date_sk)
COMMENT 'Treasury transmission event fact — one row per batch transmission attempt. Sources: aasbs_transmit.billing_summary, aac_envelope, transmit_error, agreement.agreement_log.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'fact_transmission_event',
    'pipeline.data_product' = 'DP5',
    'pipeline.consumer' = 'Treasury Ops, OIG, IPAC'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.tpp.fact_payment_reconciliation
-- Grain  : Invoice (source_inv_summary_id) × CLIN (line_item_sk)
-- Payment reconciliation fact — invoice to PO to RR to Pegasys payment. Prompt Payment Act compliance.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.tpp.fact_payment_reconciliation
(
    payment_recon_sk                               BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item (CLIN).',
    rr_sk                                          BIGINT                         COMMENT 'FK to tpp.dim_receiving_report.',
    agreement_sk                                   BIGINT                         COMMENT 'FK to tpp.dim_agreement.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    invoice_date_sk                                INT                            COMMENT 'FK to common.dim_date — invoice date.',
    payment_date_sk                                INT                            COMMENT 'FK to common.dim_date — Pegasys payment date.',
    source_inv_summary_id                          BIGINT                         COMMENT 'Source aasbs_transmit.inv_summary.id.',
    pegasys_invoice_num                            STRING                         COMMENT 'Pegasys invoice number.',
    invoice_amt                                    DECIMAL(15,2)                  COMMENT 'Invoice amount from inv_summary.',
    po_amt                                         DECIMAL(15,2)                  COMMENT 'PO line amount from po_line.',
    rr_amt                                         DECIMAL(15,2)                  COMMENT 'Receiving report accepted amount.',
    payment_amt                                    DECIMAL(15,2)                  COMMENT 'Actual Pegasys disbursement amount.',
    billing_amt                                    DECIMAL(15,2)                  COMMENT 'BAAR billing record amount.',
    invoice_to_payment_days                        INT                            COMMENT 'Calendar days from invoice date to payment (Prompt Payment metric).',
    is_prompt_payment_compliant                    BOOLEAN                        COMMENT 'TRUE if paid within 30 days per 31 U.S.C. 3901.',
    po_to_rr_variance_amt                          DECIMAL(15,2)                  COMMENT 'Variance between PO amount and RR accepted amount.',
    invoice_to_billing_variance_amt                DECIMAL(15,2)                  COMMENT 'Variance between invoice amount and billed amount.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, invoice_date_sk)
COMMENT 'Payment reconciliation fact — invoice to PO to RR to Pegasys payment. Prompt Payment Act compliance. Sources: aasbs_transmit.inv_summary, inv_detail, po_line, po_accounting, rr_summary, billing.billing.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'fact_payment_reconciliation',
    'pipeline.data_product' = 'DP5',
    'pipeline.consumer' = 'CFO Payment Ops, OIG, Prompt Payment'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.tpp.v_prompt_payment_sla
-- Prompt Payment Act SLA dashboard — compliance rate and late payment amounts by agency and FY quarter
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.tpp.v_prompt_payment_sla
COMMENT 'Prompt Payment Act SLA dashboard — compliance rate and late payment amounts by agency and FY quarter. Required for OMB A-123 Appendix C reporting.'
AS
SELECT
    ag.agency_name, ia.ia_num,
    d_inv.federal_fiscal_fy, d_inv.federal_fiscal_qtr,
    COUNT(*)                                                            AS invoice_count,
    SUM(f.invoice_amt)                                                  AS total_invoiced,
    SUM(CASE WHEN f.is_prompt_payment_compliant THEN f.invoice_amt ELSE 0 END) AS compliant_amt,
    SUM(CASE WHEN NOT f.is_prompt_payment_compliant THEN f.invoice_amt ELSE 0 END) AS late_amt,
    AVG(f.invoice_to_payment_days)                                      AS avg_payment_days,
    MAX(f.invoice_to_payment_days)                                      AS max_payment_days,
    ROUND(100.0 * SUM(CASE WHEN f.is_prompt_payment_compliant THEN 1 ELSE 0 END) / COUNT(*), 2) AS compliance_pct
FROM assist_catalog.tpp.fact_payment_reconciliation f
JOIN assist_catalog.common.dim_agency  ag    ON f.agency_sk      = ag.agency_sk    AND ag.is_current_flag
JOIN assist_catalog.common.dim_ia      ia    ON f.ia_sk          = ia.ia_sk        AND ia.is_current_flag
JOIN assist_catalog.common.dim_date    d_inv ON f.invoice_date_sk = d_inv.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.tpp.v_transmit_error_log
-- Active transmission error log — all failed batches with error codes and retry counts. Operational tr
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.tpp.v_transmit_error_log
COMMENT 'Active transmission error log — all failed batches with error codes and retry counts. Operational triage view for Financial Systems team.'
AS
SELECT
    tt.transmittal_type_cd, tt.transmittal_type_desc,
    ae.aac, ag.agency_name,
    f.transmitted_dt, f.error_code, f.error_description,
    f.retry_count, f.transmitted_amt,
    d.calendar_date AS transmission_date,
    DATEDIFF(CURRENT_TIMESTAMP(), f.transmitted_dt) AS hours_since_error
FROM assist_catalog.tpp.fact_transmission_event f
JOIN assist_catalog.tpp.dim_transmittal_type tt ON f.transmittal_type_sk = tt.transmittal_type_sk
JOIN assist_catalog.tpp.dim_aac_envelope     ae ON f.aac_envelope_sk     = ae.aac_envelope_sk
JOIN assist_catalog.common.dim_agency        ag ON f.agency_sk           = ag.agency_sk AND ag.is_current_flag
JOIN assist_catalog.common.dim_date          d  ON f.event_date_sk       = d.date_sk
WHERE f.transmittal_status_cd = 'ERROR'
ORDER BY f.transmitted_dt DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.tpp.v_billing_vs_transmitted
-- Billing vs transmitted reconciliation — BAAR billed amounts compared to invoiced and paid. Major aud
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.tpp.v_billing_vs_transmitted
COMMENT 'Billing vs transmitted reconciliation — BAAR billed amounts compared to invoiced and paid. Major audit finding prevention view.'
AS
SELECT
    ia.ia_num, ag.agency_name,
    d.federal_fiscal_fy, d.federal_fiscal_qtr,
    SUM(f.billing_amt)         AS total_billed_baar,
    SUM(f.invoice_amt)         AS total_invoiced,
    SUM(f.payment_amt)         AS total_paid,
    SUM(f.invoice_to_billing_variance_amt) AS total_variance,
    SUM(f.po_to_rr_variance_amt)           AS total_po_rr_variance
FROM assist_catalog.tpp.fact_payment_reconciliation f
JOIN assist_catalog.common.dim_ia    ia ON f.ia_sk          = ia.ia_sk    AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency ag ON f.agency_sk     = ag.agency_sk AND ag.is_current_flag
JOIN assist_catalog.common.dim_date   d ON f.invoice_date_sk = d.date_sk
GROUP BY ALL;


-- ==============================================================================
-- DP6 — assist_catalog.psc — CLIN Pricing & Service Charge Catalog
-- Grain: service charge schedule period per CLIN. FAR 15.404 price analysis support.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.psc.dim_clin_subtype
-- CLIN subtype attributes — differentiates deliverable from service charge CLINs. SCD Type 1. Sources:
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.psc.dim_clin_subtype
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
-- assist_catalog.psc.dim_cost_rate_type
-- Labor cost rate type dimension. SCD Type 1. Source: aasbs.lu_cost_rate_type.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.psc.dim_cost_rate_type
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
-- assist_catalog.psc.dim_billing_period
-- Service charge billing period dimension. SCD Type 1. Source: aasbs.service_charge_schedule.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.psc.dim_billing_period
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
-- assist_catalog.psc.fact_clin_pricing
-- Grain  : CLIN (line_item_sk) × CLIN subtype (clin_subtype_sk)
-- CLIN pricing fact — planned vs actual amounts per CLIN by subtype. Sources: aasbs.li_award_fee, li_d
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.psc.fact_clin_pricing
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
-- assist_catalog.psc.fact_service_charge_schedule
-- Grain  : CLIN (line_item_sk) × billing period (billing_period_sk)
-- Service charge schedule fact — billing schedule health per CLIN per period. Sources: aasbs.service_c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.psc.fact_service_charge_schedule
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
-- VIEW: assist_catalog.psc.v_billing_schedule_health
-- Billing schedule health view — scheduled vs actual invoice amounts per CLIN per period with variance
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.psc.v_billing_schedule_health
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
FROM assist_catalog.psc.fact_service_charge_schedule f
JOIN assist_catalog.common.dim_line_item  li ON f.line_item_sk     = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_award      aw ON f.award_sk         = aw.award_sk     AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia         ia ON f.ia_sk            = ia.ia_sk        AND ia.is_current_flag
JOIN assist_catalog.psc.dim_clin_subtype  cs ON li.line_item_id    = cs.line_item_id
JOIN assist_catalog.psc.dim_billing_period bp ON f.billing_period_sk = bp.billing_period_sk
ORDER BY aw.award_piid, li.clin_num, bp.period_start_dt;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.psc.v_labor_planned_vs_actual
-- Labor planned vs actual view — hours and rates variance analysis for labor service charge CLINs. Sup
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.psc.v_labor_planned_vs_actual
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
FROM assist_catalog.psc.fact_clin_pricing f
JOIN assist_catalog.common.dim_line_item  li  ON f.line_item_sk       = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_award      aw  ON f.award_sk           = aw.award_sk     AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia         ia  ON f.ia_sk              = ia.ia_sk        AND ia.is_current_flag
JOIN assist_catalog.psc.dim_clin_subtype  cs  ON f.clin_subtype_sk    = cs.clin_subtype_sk
JOIN assist_catalog.psc.dim_cost_rate_type crt ON f.cost_rate_type_sk = crt.cost_rate_type_sk
WHERE cs.clin_subtype_cd = 'SC_LABOR';

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.psc.v_award_fee_performance
-- Award fee performance view — fee pool vs earned fee and surcharge rate per award fee CLIN.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.psc.v_award_fee_performance
COMMENT 'Award fee performance view — fee pool vs earned fee and surcharge rate per award fee CLIN.'
AS
SELECT
    ia.ia_num, aw.award_piid, li.clin_num,
    f.planned_amt           AS total_award_fee_pool,
    f.award_fee_amt         AS earned_award_fee,
    f.award_fee_surcharge_rate,
    f.actual_amt            AS total_tracked_amt,
    ROUND(100.0 * f.award_fee_amt / NULLIF(f.planned_amt, 0), 2) AS pct_fee_earned
FROM assist_catalog.psc.fact_clin_pricing f
JOIN assist_catalog.common.dim_line_item  li ON f.line_item_sk  = li.line_item_sk AND li.is_current_flag
JOIN assist_catalog.common.dim_award      aw ON f.award_sk      = aw.award_sk     AND aw.is_current_flag
JOIN assist_catalog.common.dim_ia         ia ON f.ia_sk         = ia.ia_sk        AND ia.is_current_flag
JOIN assist_catalog.psc.dim_clin_subtype  cs ON f.clin_subtype_sk = cs.clin_subtype_sk
WHERE cs.clin_subtype_cd = 'AWARD_FEE';


-- ==============================================================================
-- DP7 — assist_catalog.smi — Solicitation & Market Intelligence
-- Grain: contractor response per solicitation. Competition analytics. Should-cost benchmarking.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.smi.dim_posting_type
-- Solicitation posting type dimension. SCD Type 1. Sources: aasbs.solicit_posting_connect, solicit_pos
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.smi.dim_posting_type
(
    posting_type_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    solicit_id                                     BIGINT NOT NULL                COMMENT 'NK — source aasbs.solicit.id.',
    posting_type_cd                                STRING NOT NULL                COMMENT 'Posting type code — DIRECT_CONNECT, DIRECT, OPEN.',
    posting_type_desc                              STRING                         COMMENT 'Posting type description.',
    open_dt                                        TIMESTAMP                      COMMENT 'Date solicitation opened.',
    pm_responsible_user_id                         STRING                         COMMENT 'Program Manager responsible for this posting.',
    ja_reason_cd                                   STRING                         COMMENT 'J&A reason code for direct (other-than-full-and-open).',
    external_source_ref                            STRING                         COMMENT 'External source reference for direct posting.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (posting_type_sk, solicit_id)
COMMENT 'Solicitation posting type dimension. SCD Type 1. Sources: aasbs.solicit_posting_connect, solicit_posting_direct.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'dim_posting_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP7'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.smi.dim_piid
-- PIID dimension — generated procurement instrument identifiers with component decomposition. SCD Type
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.smi.dim_piid
(
    piid_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    piid_id                                        BIGINT NOT NULL                COMMENT 'NK — source aasbs.piid.id.',
    piid_value                                     STRING NOT NULL                COMMENT 'Full PIID string.',
    base_piid                                      STRING                         COMMENT 'Base PIID without extension.',
    piid_ext                                       STRING                         COMMENT 'Modification/amendment extension.',
    aac                                            STRING                         COMMENT 'AAC component of PIID.',
    instrument_type_cd                             STRING                         COMMENT 'Instrument type component.',
    sequence_num                                   STRING                         COMMENT 'Sequence number component.',
    piid_fiscal_year                               INT                            COMMENT 'Fiscal year component of PIID.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (piid_sk, piid_id)
COMMENT 'PIID dimension — generated procurement instrument identifiers with component decomposition. SCD Type 1. Sources: aasbs.piid, piid_ext, vw_procurement_order.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'dim_piid',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP7'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.smi.fact_solicitation_response
-- Grain  : Contractor response (source_response_id) × solicitation (solicitation_sk)
-- Solicitation response fact — one row per contractor per solicitation. Captures winning and losing bi
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.smi.fact_solicitation_response
(
    sol_response_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    solicitation_sk                                BIGINT NOT NULL                COMMENT 'FK to alt.dim_solicitation.',
    acquisition_sk                                 BIGINT NOT NULL                COMMENT 'FK to alt.dim_acquisition.',
    contractor_sk                                  BIGINT                         COMMENT 'FK to alt.dim_contractor (responding company).',
    posting_type_sk                                BIGINT                         COMMENT 'FK to smi.dim_posting_type.',
    piid_sk                                        BIGINT                         COMMENT 'FK to smi.dim_piid (resulting award PIID).',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    response_date_sk                               INT                            COMMENT 'FK to common.dim_date — response received date.',
    award_date_sk                                  INT                            COMMENT 'FK to common.dim_date — award date (winners only).',
    source_response_id                             BIGINT                         COMMENT 'Source aasbs.solicit_response.id.',
    response_status_cd                             STRING                         COMMENT 'Response status — Submitted, Withdrawn, Awarded, Not Selected.',
    is_winner_flag                                 BOOLEAN                        COMMENT 'TRUE if this response resulted in an award.',
    is_small_business_flag                         BOOLEAN                        COMMENT 'TRUE if responding company was small business at time.',
    bid_total_amt                                  DECIMAL(15,2)                  COMMENT 'Total bid price from contractor response.',
    bid_clin_count                                 INT                            COMMENT 'Number of CLINs priced in this response.',
    award_total_amt                                DECIMAL(15,2)                  COMMENT 'Awarded amount (NULL for losing bids).',
    bid_vs_award_variance                          DECIMAL(15,2)                  COMMENT 'Variance between bid and award amount (winners only).',
    competition_rank                               INT                            COMMENT 'Rank of this bid by price among all responses.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (solicitation_sk, contractor_sk)
COMMENT 'Solicitation response fact — one row per contractor per solicitation. Captures winning and losing bids. Sources: aasbs.solicit_response, solicit_company, line_item_response, award_referenced_solicit.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'fact_solicitation_response',
    'pipeline.data_product' = 'DP7',
    'pipeline.consumer' = 'Contracting Officers, Cost/Price Analysts'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.smi.v_competition_analytics
-- Competition analytics view — response rates, small business participation, and award amounts by soli
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.smi.v_competition_analytics
COMMENT 'Competition analytics view — response rates, small business participation, and award amounts by solicitation type and FY. Supports CICA compliance and Section 809 reporting.'
AS
SELECT
    ag.agency_name, ia.ia_num,
    acq.acquisition_type_desc, acq.competition_type_desc,
    d_open.federal_fiscal_fy                        AS solicitation_fy,
    COUNT(DISTINCT f.solicitation_sk)               AS solicitation_count,
    AVG(COUNT(f.sol_response_sk)) OVER
        (PARTITION BY f.solicitation_sk)            AS avg_responses_per_sol,
    SUM(CASE WHEN f.is_small_business_flag THEN 1 ELSE 0 END) AS sb_responses,
    ROUND(100.0 * SUM(CASE WHEN f.is_small_business_flag THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2)                   AS sb_response_pct,
    SUM(f.award_total_amt)                          AS total_awarded
FROM assist_catalog.smi.fact_solicitation_response f
JOIN assist_catalog.alt.dim_acquisition     acq    ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
JOIN assist_catalog.common.dim_agency       ag     ON f.agency_sk       = ag.agency_sk       AND ag.is_current_flag
JOIN assist_catalog.common.dim_ia           ia     ON f.ia_sk           = ia.ia_sk           AND ia.is_current_flag
JOIN assist_catalog.common.dim_date         d_open ON f.response_date_sk = d_open.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.smi.v_bid_price_distribution
-- Bid price distribution view — min, max, median, and winning bid amounts per solicitation. Supports s
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.smi.v_bid_price_distribution
COMMENT 'Bid price distribution view — min, max, median, and winning bid amounts per solicitation. Supports should-cost analysis and price reasonableness benchmarking (FAR 15.404).'
AS
SELECT
    sol.solicit_num, sol.sol_status_desc,
    ag.agency_name, ia.ia_num,
    COUNT(f.sol_response_sk)                        AS total_responses,
    MIN(f.bid_total_amt)                            AS min_bid,
    MAX(f.bid_total_amt)                            AS max_bid,
    AVG(f.bid_total_amt)                            AS avg_bid,
    PERCENTILE(f.bid_total_amt, 0.5)               AS median_bid,
    MAX(CASE WHEN f.is_winner_flag THEN f.bid_total_amt END) AS winning_bid,
    MAX(CASE WHEN f.is_winner_flag THEN f.bid_total_amt END)
        - MIN(f.bid_total_amt)                      AS winning_vs_low_spread
FROM assist_catalog.smi.fact_solicitation_response f
JOIN assist_catalog.alt.dim_solicitation    sol ON f.solicitation_sk = sol.solicitation_sk
JOIN assist_catalog.common.dim_ia           ia  ON f.ia_sk           = ia.ia_sk  AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency       ag  ON f.agency_sk       = ag.agency_sk AND ag.is_current_flag
GROUP BY ALL;


-- ==============================================================================
-- DP8 — assist_catalog.wpr — Workforce & Personnel Registry
-- Grain: user role assignment per entity. COR delegation tracking. Workload analytics.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_user
-- ASSIST user dimension. SCD Type 2 — tracks org, region, and role changes. Sources: assist.users, ass
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_user
(
    user_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    user_id                                        STRING NOT NULL                COMMENT 'NK — ASSIST user login ID.',
    first_name                                     STRING                         COMMENT 'User first name.',
    last_name                                      STRING                         COMMENT 'User last name.',
    full_name                                      STRING                         COMMENT 'Full display name.',
    email                                          STRING                         COMMENT 'Work email address.',
    phone                                          STRING                         COMMENT 'Work phone number.',
    org_code                                       STRING                         COMMENT 'Organization code.',
    org_name                                       STRING                         COMMENT 'Organization name.',
    region_cd                                      STRING                         COMMENT 'GSA region code.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if user account is currently active.',
    procurement_system_id                          STRING                         COMMENT 'Linked procurement system user ID.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (user_sk, user_id)
COMMENT 'ASSIST user dimension. SCD Type 2 — tracks org, region, and role changes. Sources: assist.users, assist.procurement.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_user',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_org_hierarchy
-- Employee org hierarchy dimension — full tree structure for workload by region/division. SCD Type 1. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_org_hierarchy
(
    org_sk                                         BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    org_id                                         BIGINT NOT NULL                COMMENT 'NK — source assist.lu_emp_orgs.id.',
    org_code                                       STRING NOT NULL                COMMENT 'Organization code.',
    org_name                                       STRING NOT NULL                COMMENT 'Organization name.',
    parent_org_id                                  BIGINT                         COMMENT 'Parent organization ID for hierarchy traversal.',
    org_level                                      INT                            COMMENT 'Depth level in org tree (1=root, 2=division, 3=branch, etc.).',
    org_path                                       STRING                         COMMENT 'Full org path string — root/division/branch/etc.',
    region_cd                                      STRING                         COMMENT 'GSA region code.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this org node is currently active.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (org_sk, org_id)
COMMENT 'Employee org hierarchy dimension — full tree structure for workload by region/division. SCD Type 1. Sources: assist.lu_emp_orgs, assist.lu_emp_org_xref.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_org_hierarchy',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_responsible_role
-- Responsible role dimension — application business roles with FAR delegation requirements. SCD Type 1
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_responsible_role
(
    responsible_role_sk                            BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    responsible_role_cd                            STRING NOT NULL                COMMENT 'NK — role code: CO, COR, FMA, PM, CLIENT_REP, etc.',
    responsible_role_desc                          STRING                         COMMENT 'Role description — Contracting Officer, COR, etc.',
    requires_delegation_flag                       BOOLEAN                        COMMENT 'TRUE if this role requires written delegation per FAR 1.602-2(d).',
    far_citation                                   STRING                         COMMENT 'Applicable FAR clause for this role.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (responsible_role_sk, responsible_role_cd)
COMMENT 'Responsible role dimension — application business roles with FAR delegation requirements. SCD Type 1. Source: aasbs.lu_responsible_role.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_responsible_role',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_entity_type
-- Entity type dimension — types of entities a user can be responsible for. SCD Type 1.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_entity_type
(
    entity_type_sk                                 BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    entity_type_cd                                 STRING NOT NULL                COMMENT 'NK — entity type: AWARD, IA, FUNDING, SOLICITATION, ACQUISITION, COLLAB.',
    entity_type_desc                               STRING                         COMMENT 'Entity type description.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (entity_type_sk, entity_type_cd)
COMMENT 'Entity type dimension — types of entities a user can be responsible for. SCD Type 1.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_entity_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_client
-- Client agency registry dimension. SCD Type 2. Source: table_master.clients.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_client
(
    client_sk                                      BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    client_id                                      BIGINT NOT NULL                COMMENT 'NK — source table_master.clients.id.',
    client_name                                    STRING NOT NULL                COMMENT 'Client agency name.',
    client_code                                    STRING                         COMMENT 'Client agency code.',
    contact_name                                   STRING                         COMMENT 'Primary contact name.',
    contact_email                                  STRING                         COMMENT 'Primary contact email.',
    contact_phone                                  STRING                         COMMENT 'Primary contact phone.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (client_sk, client_id)
COMMENT 'Client agency registry dimension. SCD Type 2. Source: table_master.clients.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_client',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.dim_industry_partner
-- Industry partner registry dimension. SCD Type 2. Sources: table_master.industry_partners, table_mast
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.dim_industry_partner
(
    partner_sk                                     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    partner_id                                     BIGINT NOT NULL                COMMENT 'NK — source table_master.industry_partners.id.',
    company_name                                   STRING NOT NULL                COMMENT 'Company legal name.',
    contract_status_cd                             STRING                         COMMENT 'Contract status from table_master.contracts.',
    ftin                                           STRING                         COMMENT 'Federal Tax Identification Number.',
    address_city                                   STRING                         COMMENT 'City.',
    address_state_cd                               STRING                         COMMENT 'State code.',
    address_country_cd                             STRING                         COMMENT 'Country code.',
    eff_start_dt                                   TIMESTAMP NOT NULL             COMMENT 'SCD2: Effective start date — when this version of the record became active.',
    eff_end_dt                                     TIMESTAMP                      COMMENT 'SCD2: Effective end date — NULL means this is the current active version.',
    is_current_flag                                BOOLEAN NOT NULL               COMMENT 'SCD2: TRUE for the single current active row per natural key.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (partner_sk, partner_id)
COMMENT 'Industry partner registry dimension. SCD Type 2. Sources: table_master.industry_partners, table_master.contracts.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'dim_industry_partner',
    'scd.type' = '2',
    'pipeline.data_product' = 'DP8'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.wpr.fact_role_assignment
-- Grain  : User (user_sk) × role (responsible_role_sk) × entity (source_entity_id)
-- Role assignment fact — all CO/COR/FMA/PM personnel assignments across awards, IAs, funding, solicita
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.wpr.fact_role_assignment
(
    role_assignment_sk                             BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    user_sk                                        BIGINT NOT NULL                COMMENT 'FK to wpr.dim_user.',
    responsible_role_sk                            BIGINT NOT NULL                COMMENT 'FK to wpr.dim_responsible_role.',
    entity_type_sk                                 BIGINT NOT NULL                COMMENT 'FK to wpr.dim_entity_type.',
    org_sk                                         BIGINT                         COMMENT 'FK to wpr.dim_org_hierarchy.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    award_sk                                       BIGINT                         COMMENT 'FK to common.dim_award (when entity is AWARD).',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia (when entity is IA or FUNDING).',
    assigned_date_sk                               INT                            COMMENT 'FK to common.dim_date — assignment effective date.',
    source_entity_id                               BIGINT NOT NULL                COMMENT 'Source entity primary key (award_id, ia_id, funding_id, etc.).',
    assignment_status_cd                           STRING                         COMMENT 'Assignment status — Active, Superseded, Removed.',
    delegation_letter_ref                          STRING                         COMMENT 'COR delegation letter reference number (FAR 1.602-2(d)).',
    has_delegation_flag                            BOOLEAN                        COMMENT 'TRUE if written delegation document is on file.',
    awards_assigned_count                          INT                            COMMENT 'Total awards this user is currently assigned to in this role.',
    active_clin_count                              INT                            COMMENT 'Total active CLINs under assignments for this user-role-entity.',
    total_obligated_amt                            DECIMAL(15,2)                  COMMENT 'Sum of obligated amounts across user-role-entity assignments.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (user_sk, responsible_role_sk, source_entity_id)
COMMENT 'Role assignment fact — all CO/COR/FMA/PM personnel assignments across awards, IAs, funding, solicitations. Sources: aasbs.award_responsible, award_mod_responsible, ia_responsible, funding_responsible, solicit_responsible, acquisition_mod_responsible.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'wpr',
    'pipeline.table' = 'fact_role_assignment',
    'pipeline.data_product' = 'DP8',
    'pipeline.consumer' = 'HR, Regional Directors, COR Program Office'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.wpr.v_cor_delegation_compliance
-- COR delegation compliance view — identifies COR assignments missing written delegation per FAR 1.602
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.wpr.v_cor_delegation_compliance
COMMENT 'COR delegation compliance view — identifies COR assignments missing written delegation per FAR 1.602-2(d). Required for OFPP COR Program annual reporting.'
AS
SELECT
    u.full_name, u.email, u.org_name, u.region_cd,
    ag.agency_name, ia.ia_num,
    rr.responsible_role_desc,
    f.has_delegation_flag,
    f.delegation_letter_ref,
    f.awards_assigned_count,
    f.total_obligated_amt,
    CASE WHEN rr.requires_delegation_flag AND NOT f.has_delegation_flag
         THEN TRUE ELSE FALSE END AS delegation_gap_flag
FROM assist_catalog.wpr.fact_role_assignment f
JOIN assist_catalog.wpr.dim_user              u   ON f.user_sk              = u.user_sk              AND u.is_current_flag
JOIN assist_catalog.wpr.dim_responsible_role  rr  ON f.responsible_role_sk  = rr.responsible_role_sk
JOIN assist_catalog.common.dim_agency         ag  ON f.agency_sk            = ag.agency_sk           AND ag.is_current_flag
JOIN assist_catalog.common.dim_ia             ia  ON f.ia_sk                = ia.ia_sk               AND ia.is_current_flag
WHERE f.assignment_status_cd = 'Active'
  AND rr.responsible_role_cd = 'COR';

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.wpr.v_workload_by_region
-- Workload by region view — staff counts, award loads, and obligation totals by org and role. Supports
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.wpr.v_workload_by_region
COMMENT 'Workload by region view — staff counts, award loads, and obligation totals by org and role. Supports workforce planning and span-of-control analysis.'
AS
SELECT
    org.region_cd, org.org_name, org.org_level,
    rr.responsible_role_desc,
    COUNT(DISTINCT f.user_sk)           AS staff_count,
    SUM(f.awards_assigned_count)        AS total_awards,
    AVG(f.awards_assigned_count)        AS avg_awards_per_person,
    SUM(f.active_clin_count)            AS total_active_clins,
    SUM(f.total_obligated_amt)          AS total_obligated
FROM assist_catalog.wpr.fact_role_assignment f
JOIN assist_catalog.wpr.dim_org_hierarchy     org ON f.org_sk             = org.org_sk
JOIN assist_catalog.wpr.dim_responsible_role  rr  ON f.responsible_role_sk = rr.responsible_role_sk
WHERE f.assignment_status_cd = 'Active'
GROUP BY ALL
ORDER BY org.region_cd, rr.responsible_role_desc;


-- ==============================================================================
-- DP9 — assist_catalog.iat — IA Lifecycle & Amendment Tracker
-- Grain: IA amendment. Economy Act compliance. Annual review cadence. FIN code history.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.iat.dim_amend_action
-- IA amendment action type dimension. SCD Type 1. Source: aasbs.lu_ia_amend_action.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.iat.dim_amend_action
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
-- assist_catalog.iat.dim_reviewer
-- IA reviewer dimension. SCD Type 1. Sourced from ia_review and aasbs users.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.iat.dim_reviewer
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
-- assist_catalog.iat.fact_ia_amendment
-- Grain  : IA amendment (source_ia_amendment_id) — one row per amendment
-- IA amendment fact — full amendment history per IA. Sources: aasbs.ia_amendment, award_ia, funding_am
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.iat.fact_ia_amendment
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
-- assist_catalog.iat.fact_ia_review
-- Grain  : IA review (source_ia_review_id) — one row per review event
-- IA annual review fact. OMB A-123 review cadence tracking. Source: aasbs.ia_review.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.iat.fact_ia_review
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
-- VIEW: assist_catalog.iat.v_ia_amendment_velocity
-- IA amendment velocity view — amendment frequency, cost change, and period extension summary by IA an
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.iat.v_ia_amendment_velocity
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
FROM assist_catalog.iat.fact_ia_amendment f
JOIN assist_catalog.common.dim_ia       ia ON f.ia_sk          = ia.ia_sk          AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency   ag ON f.agency_sk      = ag.agency_sk      AND ag.is_current_flag
JOIN assist_catalog.iat.dim_amend_action da ON f.amend_action_sk = da.amend_action_sk
JOIN assist_catalog.common.dim_date      d ON f.amend_date_sk  = d.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.iat.v_ia_annual_review_status
-- IA annual review status view — completion and overdue tracking per IA. OMB A-123 governance complian
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.iat.v_ia_annual_review_status
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
FROM assist_catalog.iat.fact_ia_review r
JOIN assist_catalog.common.dim_ia    ia      ON r.ia_sk          = ia.ia_sk        AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency ag     ON r.agency_sk      = ag.agency_sk    AND ag.is_current_flag
JOIN assist_catalog.common.dim_date   d_review ON r.review_date_sk = d_review.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.iat.v_fin_code_history
-- FIN code and servicing AAC history view — full amendment trail per IA ordered by amendment number. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.iat.v_fin_code_history
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
FROM assist_catalog.iat.fact_ia_amendment f
JOIN assist_catalog.common.dim_ia    ia ON f.ia_sk         = ia.ia_sk    AND ia.is_current_flag
JOIN assist_catalog.common.dim_agency ag ON f.agency_sk    = ag.agency_sk AND ag.is_current_flag
JOIN assist_catalog.common.dim_date   d ON f.amend_date_sk = d.date_sk
ORDER BY ia.ia_num, f.amendment_num;


-- ==============================================================================
-- DP10 — assist_catalog.cat — Collaboration, Review & Audit Trail
-- Grain: lifecycle chronology event. OIG document production. Pre-award review compliance.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.dim_event_type
-- Lifecycle event type dimension. SCD Type 1. Source: aasbs.lu_event_type.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.dim_event_type
(
    event_type_sk                                  BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    event_type_cd                                  STRING NOT NULL                COMMENT 'NK — event type code from lu_event_type.',
    event_type_desc                                STRING                         COMMENT 'Event type description.',
    lifecycle_phase_cd                             STRING                         COMMENT 'Lifecycle phase — PRE_AWARD, AWARD, POST_AWARD, CLOSEOUT.',
    is_system_event_flag                           BOOLEAN                        COMMENT 'TRUE if generated by system; FALSE if user-initiated.',
    is_audit_significant                           BOOLEAN                        COMMENT 'TRUE if this event type is significant for OIG/audit purposes.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (event_type_sk, event_type_cd)
COMMENT 'Lifecycle event type dimension. SCD Type 1. Source: aasbs.lu_event_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'dim_event_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP10'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.dim_review_type
-- Pre-award review type dimension. SCD Type 1. Sources: aasbs.lu_review_type.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.dim_review_type
(
    review_type_sk                                 BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    review_type_cd                                 STRING NOT NULL                COMMENT 'NK — review type code.',
    review_type_desc                               STRING                         COMMENT 'Review type description — Legal, Technical, Price, OIG.',
    is_pre_award_flag                              BOOLEAN                        COMMENT 'TRUE if this review type applies pre-award.',
    is_mandatory_flag                              BOOLEAN                        COMMENT 'TRUE if this review type is mandatory per GSA policy.',
    far_basis                                      STRING                         COMMENT 'FAR clause or policy basis for this review requirement.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (review_type_sk, review_type_cd)
COMMENT 'Pre-award review type dimension. SCD Type 1. Sources: aasbs.lu_review_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'dim_review_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP10'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.dim_reviewer_type
-- Reviewer type dimension. SCD Type 1. Source: aasbs.lu_reviewer_type.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.dim_reviewer_type
(
    reviewer_type_sk                               BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    reviewer_type_cd                               STRING NOT NULL                COMMENT 'NK — reviewer type code.',
    reviewer_type_desc                             STRING                         COMMENT 'Reviewer type description — Primary, Secondary, Approval.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (reviewer_type_sk, reviewer_type_cd)
COMMENT 'Reviewer type dimension. SCD Type 1. Source: aasbs.lu_reviewer_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'dim_reviewer_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP10'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.dim_finding
-- Review finding dimension. SCD Type 1. Source: aasbs.lu_review_finding.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.dim_finding
(
    finding_sk                                     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    finding_cd                                     STRING NOT NULL                COMMENT 'NK — review finding code.',
    finding_desc                                   STRING                         COMMENT 'Finding description — Approved, Approved with Comments, Returned, etc.',
    is_blocking_flag                               BOOLEAN                        COMMENT 'TRUE if this finding blocks progression to next lifecycle phase.',
    requires_remediation                           BOOLEAN                        COMMENT 'TRUE if remediation action is required.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (finding_sk, finding_cd)
COMMENT 'Review finding dimension. SCD Type 1. Source: aasbs.lu_review_finding.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'dim_finding',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP10'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.fact_lifecycle_event
-- Grain  : Lifecycle event (source_chronology_id / source_collab_id) — one row per event
-- Lifecycle event fact — every key acquisition event with timestamp and user. Feeds OIG audit trail an
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.fact_lifecycle_event
(
    lifecycle_event_sk                             BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    event_type_sk                                  BIGINT NOT NULL                COMMENT 'FK to cat.dim_event_type.',
    entity_type_sk                                 BIGINT NOT NULL                COMMENT 'FK to wpr.dim_entity_type — entity this event applies to.',
    award_sk                                       BIGINT                         COMMENT 'FK to common.dim_award (when event relates to award).',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia (when event relates to IA).',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    event_date_sk                                  INT                            COMMENT 'FK to common.dim_date — event timestamp date.',
    source_entity_id                               BIGINT NOT NULL                COMMENT 'Source entity primary key.',
    source_chronology_id                           BIGINT                         COMMENT 'Source aasbs.chronology.id.',
    source_collab_id                               BIGINT                         COMMENT 'Source aasbs.central_collab.id (when collab event).',
    event_timestamp                                TIMESTAMP                      COMMENT 'Precise event timestamp.',
    event_user_id                                  STRING                         COMMENT 'ASSIST user who triggered event.',
    event_description                              STRING                         COMMENT 'Event description text.',
    collab_type_cd                                 STRING                         COMMENT 'Collaboration type code (collab events only).',
    collab_status_cd                               STRING                         COMMENT 'Collaboration status at time of event.',
    collab_due_dt                                  TIMESTAMP                      COMMENT 'Collaboration due date.',
    comment_type_cd                                STRING                         COMMENT 'Comment type code (comment events only).',
    comment_text                                   STRING                         COMMENT 'Comment text — redacted for PII in Gold (full text in Silver).',
    email_status_cd                                STRING                         COMMENT 'Email status code (email events only).',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (source_entity_id, event_date_sk)
COMMENT 'Lifecycle event fact — every key acquisition event with timestamp and user. Feeds OIG audit trail and process mining. Sources: aasbs.chronology, central_collab, central_comment, email_log.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'fact_lifecycle_event',
    'pipeline.data_product' = 'DP10',
    'pipeline.consumer' = 'OIG, Compliance, Legal Counsel'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.cat.fact_review_determination
-- Grain  : Review determination (source_determination_id) — one row per reviewer determination
-- Pre-award review determination fact — reviewer decisions with findings and determination timelines. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.cat.fact_review_determination
(
    review_determination_sk                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    review_type_sk                                 BIGINT NOT NULL                COMMENT 'FK to cat.dim_review_type.',
    reviewer_type_sk                               BIGINT NOT NULL                COMMENT 'FK to cat.dim_reviewer_type.',
    finding_sk                                     BIGINT                         COMMENT 'FK to cat.dim_finding.',
    award_sk                                       BIGINT                         COMMENT 'FK to common.dim_award.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    review_date_sk                                 INT                            COMMENT 'FK to common.dim_date — determination date.',
    source_review_id                               BIGINT NOT NULL                COMMENT 'Source aasbs.review.id.',
    source_determination_id                        BIGINT                         COMMENT 'Source aasbs.review_determination.id.',
    reviewer_user_id                               STRING                         COMMENT 'Reviewer ASSIST user ID.',
    review_assigned_dt                             TIMESTAMP                      COMMENT 'Date review was assigned to reviewer.',
    determination_dt                               TIMESTAMP                      COMMENT 'Date determination was made.',
    days_to_determination                          INT                            COMMENT 'Calendar days from assignment to determination.',
    determination_comments                         STRING                         COMMENT 'Reviewer determination comments.',
    is_blocking_finding_flag                       BOOLEAN                        COMMENT 'TRUE if finding blocked progression.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (source_review_id, review_date_sk)
COMMENT 'Pre-award review determination fact — reviewer decisions with findings and determination timelines. Sources: aasbs.review, aasbs.review_determination.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'cat',
    'pipeline.table' = 'fact_review_determination',
    'pipeline.data_product' = 'DP10'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.cat.v_pre_award_review_compliance
-- Pre-award review compliance view — mandatory review completion rates, determination timelines, and b
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.cat.v_pre_award_review_compliance
COMMENT 'Pre-award review compliance view — mandatory review completion rates, determination timelines, and blocking findings by review type. Supports FAR 1.602-1 compliance tracking.'
AS
SELECT
    ag.agency_name, ia.ia_num, aw.award_piid,
    rt.review_type_desc, rt.is_mandatory_flag,
    d.federal_fiscal_fy,
    COUNT(f.review_determination_sk)                     AS total_reviews,
    AVG(f.days_to_determination)                         AS avg_days_to_determination,
    SUM(CASE WHEN fi.is_blocking_flag THEN 1 ELSE 0 END) AS blocking_findings,
    SUM(CASE WHEN f.is_blocking_finding_flag THEN 1 ELSE 0 END) AS awards_blocked
FROM assist_catalog.cat.fact_review_determination f
JOIN assist_catalog.cat.dim_review_type   rt ON f.review_type_sk   = rt.review_type_sk
JOIN assist_catalog.cat.dim_finding       fi ON f.finding_sk       = fi.finding_sk
JOIN assist_catalog.common.dim_agency     ag ON f.agency_sk        = ag.agency_sk  AND ag.is_current_flag
JOIN assist_catalog.common.dim_ia         ia ON f.ia_sk            = ia.ia_sk      AND ia.is_current_flag
JOIN assist_catalog.common.dim_award      aw ON f.award_sk         = aw.award_sk   AND aw.is_current_flag
JOIN assist_catalog.common.dim_date       d  ON f.review_date_sk   = d.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_catalog.cat.v_event_timeline
-- Full event timeline view — chronologically ordered lifecycle events per entity with phase labels. Pr
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_catalog.cat.v_event_timeline
COMMENT 'Full event timeline view — chronologically ordered lifecycle events per entity with phase labels. Primary OIG document production view.'
AS
SELECT
    et.lifecycle_phase_cd,
    et.event_type_desc,
    ia.ia_num,
    aw.award_piid,
    ag.agency_name,
    d.calendar_date         AS event_date,
    f.event_timestamp,
    f.event_user_id,
    f.event_description,
    f.collab_type_cd,
    f.collab_status_cd,
    f.email_status_cd
FROM assist_catalog.cat.fact_lifecycle_event f
JOIN assist_catalog.cat.dim_event_type   et ON f.event_type_sk  = et.event_type_sk
JOIN assist_catalog.common.dim_agency    ag ON f.agency_sk      = ag.agency_sk AND ag.is_current_flag
JOIN assist_catalog.common.dim_date      d  ON f.event_date_sk  = d.date_sk
LEFT JOIN assist_catalog.common.dim_ia   ia ON f.ia_sk          = ia.ia_sk     AND ia.is_current_flag
LEFT JOIN assist_catalog.common.dim_award aw ON f.award_sk      = aw.award_sk  AND aw.is_current_flag
ORDER BY f.source_entity_id, f.event_timestamp;


-- ==============================================================================
-- DP11 — assist_catalog.ref — Conformed Reference Data Catalog
-- Grain: lookup code per domain. 61 lu_* tables published as versioned reference product.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_acquisition_status
-- Reference data: Acquisition status codes and descriptions. SCD Type 1 — full refresh on change. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_acquisition_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_acquisition_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_acquisition_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Acquisition status codes and descriptions. SCD Type 1 — full refresh on change. Source: aasbs.lu_acquisition_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_acquisition_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_acquisition_type
-- Reference data: Acquisition type — Service or Supply. SCD Type 1 — full refresh on change. Source: a
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_acquisition_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_acquisition_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_acquisition_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Acquisition type — Service or Supply. SCD Type 1 — full refresh on change. Source: aasbs.lu_acquisition_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_acquisition_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fund_type
-- Reference data: Funding types — Annual, Multi-year, No-year, Revolving. SCD Type 1 — full refresh on
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fund_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fund_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fund_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Funding types — Annual, Multi-year, No-year, Revolving. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fund_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fund_status
-- Reference data: Funding status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_sta
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fund_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fund_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fund_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Funding status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fund_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fund_category
-- Reference data: Fund category codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_cate
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fund_category
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fund_category.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fund_category.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Fund category codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_category.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fund_category',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_billing_type
-- Reference data: Billing types — Advance Deposit (AD) or Reimbursable Agreement (RA). SCD Type 1 — fu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_billing_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_billing_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_billing_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Billing types — Advance Deposit (AD) or Reimbursable Agreement (RA). SCD Type 1 — full refresh on change. Source: aasbs.lu_billing_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_billing_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_contract_type
-- Reference data: Contract types — FFP, T&M, CPFF, CPAF, IDIQ, etc. Includes accrual_holdback_pct. SCD
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_contract_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_contract_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_contract_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Contract types — FFP, T&M, CPFF, CPAF, IDIQ, etc. Includes accrual_holdback_pct. SCD Type 1 — full refresh on change. Source: aasbs.lu_contract_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_contract_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_competition_type
-- Reference data: FAR Part 6 competition type codes. SCD Type 1 — full refresh on change. Source: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_competition_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_competition_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_competition_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: FAR Part 6 competition type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_competition_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_competition_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_instrument_type
-- Reference data: Instrument types — MIPR, Economy Act, Assisted Acquisition, etc. SCD Type 1 — full r
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_instrument_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_instrument_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_instrument_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Instrument types — MIPR, Economy Act, Assisted Acquisition, etc. SCD Type 1 — full refresh on change. Source: aasbs.lu_instrument_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_instrument_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sol_status
-- Reference data: Solicitation status codes and descriptions. SCD Type 1 — full refresh on change. Sou
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sol_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sol_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sol_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Solicitation status codes and descriptions. SCD Type 1 — full refresh on change. Source: aasbs.lu_sol_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sol_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sol_amend_status
-- Reference data: Solicitation amendment status codes. SCD Type 1 — full refresh on change. Source: aa
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sol_amend_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sol_amend_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sol_amend_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Solicitation amendment status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_sol_amend_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sol_amend_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sol_posting_type
-- Reference data: Solicitation posting type codes. SCD Type 1 — full refresh on change. Source: aasbs.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sol_posting_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sol_posting_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sol_posting_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Solicitation posting type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_sol_posting_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sol_posting_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sol_response_aac_visibility
-- Reference data: AAC visibility settings for solicitation responses. SCD Type 1 — full refresh on cha
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sol_response_aac_visibility
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sol_response_aac_visibility.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sol_response_aac_visibility.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: AAC visibility settings for solicitation responses. SCD Type 1 — full refresh on change. Source: aasbs.lu_sol_response_aac_visibility.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sol_response_aac_visibility',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sol_response_cli_visibility
-- Reference data: Client visibility settings for solicitation responses. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sol_response_cli_visibility
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sol_response_cli_visibility.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sol_response_cli_visibility.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Client visibility settings for solicitation responses. SCD Type 1 — full refresh on change. Source: aasbs.lu_sol_response_cli_visibility.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sol_response_cli_visibility',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_performance_type
-- Reference data: Performance-based acquisition type codes. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_performance_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_performance_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_performance_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Performance-based acquisition type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_performance_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_performance_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_acquisition_mod_status
-- Reference data: Acquisition modification status codes. SCD Type 1 — full refresh on change. Source: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_acquisition_mod_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_acquisition_mod_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_acquisition_mod_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Acquisition modification status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_acquisition_mod_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_acquisition_mod_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_review_type
-- Reference data: Pre-award review type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_r
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_review_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_review_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_review_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Pre-award review type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_review_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_review_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_review_finding
-- Reference data: Review finding codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_review_f
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_review_finding
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_review_finding.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_review_finding.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Review finding codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_review_finding.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_review_finding',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_reviewer_type
-- Reference data: Reviewer type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_reviewer_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_reviewer_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_reviewer_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_reviewer_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Reviewer type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_reviewer_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_reviewer_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_responsible_role
-- Reference data: Application business role codes — CO, COR, FMA, PM, CLIENT_REP. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_responsible_role
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_responsible_role.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_responsible_role.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Application business role codes — CO, COR, FMA, PM, CLIENT_REP. SCD Type 1 — full refresh on change. Source: aasbs.lu_responsible_role.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_responsible_role',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_cor_delegation
-- Reference data: COR delegation method codes per FAR 1.602-2(d). SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_cor_delegation
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_cor_delegation.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_cor_delegation.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: COR delegation method codes per FAR 1.602-2(d). SCD Type 1 — full refresh on change. Source: aasbs.lu_cor_delegation.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_cor_delegation',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_ia_status
-- Reference data: Interagency agreement status codes. SCD Type 1 — full refresh on change. Source: aas
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_ia_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_ia_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_ia_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Interagency agreement status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_ia_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_ia_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_ia_amend_action
-- Reference data: IA amendment action type codes. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_ia_amend_action
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_ia_amend_action.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_ia_amend_action.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: IA amendment action type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_ia_amend_action.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_ia_amend_action',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_loa_status
-- Reference data: Line of Accounting status codes. SCD Type 1 — full refresh on change. Source: aasbs.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_loa_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_loa_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_loa_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Line of Accounting status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_loa_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_loa_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_loa_change_reason
-- Reference data: Reasons a change can be made to an LOA. SCD Type 1 — full refresh on change. Source:
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_loa_change_reason
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_loa_change_reason.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_loa_change_reason.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Reasons a change can be made to an LOA. SCD Type 1 — full refresh on change. Source: aasbs.lu_loa_change_reason.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_loa_change_reason',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_line_item_type
-- Reference data: CLIN/SLIN type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_line_ite
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_line_item_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_line_item_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_line_item_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: CLIN/SLIN type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_line_item_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_line_item_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_vehicle_type
-- Reference data: Contract vehicle type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_v
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_vehicle_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_vehicle_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_vehicle_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Contract vehicle type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_vehicle_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_vehicle_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_proc_phase
-- Reference data: Procurement phase codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_proc_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_proc_phase
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_proc_phase.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_proc_phase.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Procurement phase codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_proc_phase.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_proc_phase',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_program
-- Reference data: GSA program codes — AAS, ITS_NSD, etc. SCD Type 1 — full refresh on change. Source: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_program
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_program.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_program.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: GSA program codes — AAS, ITS_NSD, etc. SCD Type 1 — full refresh on change. Source: aasbs.lu_program.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_program',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_region
-- Reference data: GSA region codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_region.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_region
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_region.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_region.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: GSA region codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_region.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_region',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_state
-- Reference data: US state codes, territories, and foreign entity codes. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_state
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_state.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_state.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: US state codes, territories, and foreign entity codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_state.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_state',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_country
-- Reference data: Country codes for GSA business locations. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_country
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_country.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_country.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Country codes for GSA business locations. SCD Type 1 — full refresh on change. Source: aasbs.lu_country.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_country',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_address_type
-- Reference data: Address type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_address_ty
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_address_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_address_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_address_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Address type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_address_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_address_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_agency
-- Reference data: Federal agency codes and names. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_agency
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_agency.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_agency.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Federal agency codes and names. SCD Type 1 — full refresh on change. Source: aasbs.lu_agency.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_agency',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_award_mod_fpds_reason
-- Reference data: FPDS modification reason codes. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_award_mod_fpds_reason
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_award_mod_fpds_reason.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_award_mod_fpds_reason.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: FPDS modification reason codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_award_mod_fpds_reason.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_award_mod_fpds_reason',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_sf30_mod_type
-- Reference data: SF-30 modification type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_sf30_mod_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_sf30_mod_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_sf30_mod_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: SF-30 modification type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_sf30_mod_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_sf30_mod_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fpds_car_status
-- Reference data: FPDS Contract Action Report status codes. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fpds_car_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fpds_car_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fpds_car_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: FPDS Contract Action Report status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fpds_car_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fpds_car_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_bundled_contract
-- Reference data: Bundled contract determination codes (FAR 7.107). SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_bundled_contract
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_bundled_contract.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_bundled_contract.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Bundled contract determination codes (FAR 7.107). SCD Type 1 — full refresh on change. Source: aasbs.lu_bundled_contract.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_bundled_contract',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_idv_type_of_idc
-- Reference data: IDV type of Indefinite Delivery Contract codes. SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_idv_type_of_idc
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_idv_type_of_idc.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_idv_type_of_idc.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: IDV type of Indefinite Delivery Contract codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_idv_type_of_idc.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_idv_type_of_idc',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_idv_referenced_class
-- Reference data: IDV referenced class codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_id
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_idv_referenced_class
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_idv_referenced_class.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_idv_referenced_class.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: IDV referenced class codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_idv_referenced_class.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_idv_referenced_class',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_idv_who_can_use
-- Reference data: Agency types that can use an IDV vehicle. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_idv_who_can_use
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_idv_who_can_use.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_idv_who_can_use.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Agency types that can use an IDV vehicle. SCD Type 1 — full refresh on change. Source: aasbs.lu_idv_who_can_use.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_idv_who_can_use',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_small_business_admin
-- Reference data: SBA program codes — 8(a), HUBZone, WOSB, SDVOSB. SCD Type 1 — full refresh on change
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_small_business_admin
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_small_business_admin.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_small_business_admin.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: SBA program codes — 8(a), HUBZone, WOSB, SDVOSB. SCD Type 1 — full refresh on change. Source: aasbs.lu_small_business_admin.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_small_business_admin',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_reason_for_direct
-- Reference data: J&A basis codes for other-than-full-and-open competition. SCD Type 1 — full refresh 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_reason_for_direct
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_reason_for_direct.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_reason_for_direct.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: J&A basis codes for other-than-full-and-open competition. SCD Type 1 — full refresh on change. Source: aasbs.lu_reason_for_direct.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_reason_for_direct',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_cost_rate_type
-- Reference data: Labor cost rate types — Actual, Provisional, Fixed, Predefined. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_cost_rate_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_cost_rate_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_cost_rate_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Labor cost rate types — Actual, Provisional, Fixed, Predefined. SCD Type 1 — full refresh on change. Source: aasbs.lu_cost_rate_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_cost_rate_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_unit_of_measure
-- Reference data: Units of measure codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_unit_o
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_unit_of_measure
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_unit_of_measure.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_unit_of_measure.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Units of measure codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_unit_of_measure.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_unit_of_measure',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_severability_type
-- Reference data: Contract severability codes — Severable, Nonseverable, Mix. SCD Type 1 — full refres
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_severability_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_severability_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_severability_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Contract severability codes — Severable, Nonseverable, Mix. SCD Type 1 — full refresh on change. Source: aasbs.lu_severability_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_severability_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_commercial_type
-- Reference data: Client commercial type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_commercial_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_commercial_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_commercial_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Client commercial type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_commercial_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_commercial_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fob_type
-- Reference data: Free on Board (FOB) type codes — Origin, Destination. SCD Type 1 — full refresh on c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fob_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fob_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fob_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Free on Board (FOB) type codes — Origin, Destination. SCD Type 1 — full refresh on change. Source: aasbs.lu_fob_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fob_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fund
-- Reference data: GSA internal fund codes — 285X, 285F, etc. SCD Type 1 — full refresh on change. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fund
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fund.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fund.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: GSA internal fund codes — 285X, 285F, etc. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fund',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_fund_amend_status
-- Reference data: Funding amendment status codes — Draft, Error, Final, Pending. SCD Type 1 — full ref
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_fund_amend_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_fund_amend_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_fund_amend_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Funding amendment status codes — Draft, Error, Final, Pending. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_amend_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_fund_amend_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_collab_type
-- Reference data: Collaboration type codes — Resume, Technical Report, Fund Request. SCD Type 1 — full
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_collab_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_collab_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_collab_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Collaboration type codes — Resume, Technical Report, Fund Request. SCD Type 1 — full refresh on change. Source: aasbs.lu_collab_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_collab_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_collab_status
-- Reference data: Collaboration status codes by collaboration type. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_collab_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_collab_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_collab_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Collaboration status codes by collaboration type. SCD Type 1 — full refresh on change. Source: aasbs.lu_collab_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_collab_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_checklist_type
-- Reference data: Closeout checklist type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_checklist_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_checklist_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_checklist_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Closeout checklist type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_checklist_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_checklist_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_checklist_item
-- Reference data: Closeout checklist items with tooltip text and sort order. SCD Type 1 — full refresh
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_checklist_item
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_checklist_item.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_checklist_item.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Closeout checklist items with tooltip text and sort order. SCD Type 1 — full refresh on change. Source: aasbs.lu_checklist_item.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_checklist_item',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_checklist_value
-- Reference data: Dropdown answer values for checklist items. SCD Type 1 — full refresh on change. Sou
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_checklist_value
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_checklist_value.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_checklist_value.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Dropdown answer values for checklist items. SCD Type 1 — full refresh on change. Source: aasbs.lu_checklist_value.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_checklist_value',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_closeout_prohibition
-- Reference data: Closeout prohibition codes blocking award closeout. SCD Type 1 — full refresh on cha
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_closeout_prohibition
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_closeout_prohibition.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_closeout_prohibition.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Closeout prohibition codes blocking award closeout. SCD Type 1 — full refresh on change. Source: aasbs.lu_closeout_prohibition.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_closeout_prohibition',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_event_type
-- Reference data: Lifecycle event types for chronology audit trail. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_event_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_event_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_event_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Lifecycle event types for chronology audit trail. SCD Type 1 — full refresh on change. Source: aasbs.lu_event_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_event_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_email_status
-- Reference data: Email status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_email_stat
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_email_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_email_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_email_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Email status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_email_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_email_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_table_name
-- Reference data: AASBS table registry — used by CENTRAL_COMMENT cross-reference. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_table_name
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_table_name.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_table_name.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: AASBS table registry — used by CENTRAL_COMMENT cross-reference. SCD Type 1 — full refresh on change. Source: aasbs.lu_table_name.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_table_name',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transaction_type
-- Reference data: LOA ledger transaction type codes. SCD Type 1 — full refresh on change. Source: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transaction_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transaction_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transaction_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: LOA ledger transaction type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_transaction_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transaction_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_emergency_acq
-- Reference data: FAR Part 18 emergency acquisition procedure indicator codes. SCD Type 1 — full refre
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_emergency_acq
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_emergency_acq.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_emergency_acq.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: FAR Part 18 emergency acquisition procedure indicator codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_emergency_acq.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_emergency_acq',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_expedited_type
-- Reference data: Expedited processing cause codes. SCD Type 1 — full refresh on change. Source: aasbs
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_expedited_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_expedited_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_expedited_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Expedited processing cause codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_expedited_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_expedited_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_continuity_type
-- Reference data: Continuity type codes with previous acquisition reference. SCD Type 1 — full refresh
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_continuity_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_continuity_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_continuity_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Continuity type codes with previous acquisition reference. SCD Type 1 — full refresh on change. Source: aasbs.lu_continuity_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_continuity_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_processing_speed
-- Reference data: Processing speed codes — Routine, Expedited. SCD Type 1 — full refresh on change. So
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_processing_speed
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_processing_speed.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_processing_speed.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Processing speed codes — Routine, Expedited. SCD Type 1 — full refresh on change. Source: aasbs.lu_processing_speed.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_processing_speed',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_intel_community
-- Reference data: Intelligence Community member indicator codes per DNI. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_intel_community
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_intel_community.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_intel_community.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Intelligence Community member indicator codes per DNI. SCD Type 1 — full refresh on change. Source: aasbs.lu_intel_community.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_intel_community',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_subsystem
-- Reference data: ASSIST subsystem codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_subsys
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_subsystem
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_subsystem.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_subsystem.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: ASSIST subsystem codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_subsystem.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_subsystem',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_supply_chain_risk
-- Reference data: CMMC supply chain risk level codes (1–5). SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_supply_chain_risk
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_supply_chain_risk.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_supply_chain_risk.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: CMMC supply chain risk level codes (1–5). SCD Type 1 — full refresh on change. Source: aasbs.lu_supply_chain_risk.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_supply_chain_risk',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_surveillance_spec
-- Reference data: DD-254 security classification specification indicator codes. SCD Type 1 — full refr
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_surveillance_spec
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_surveillance_spec.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_surveillance_spec.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: DD-254 security classification specification indicator codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_surveillance_spec.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_surveillance_spec',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transfer
-- Reference data: Contract administration transfer indicator codes. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transfer
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transfer.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transfer.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Contract administration transfer indicator codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_transfer.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transfer',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_office_type
-- Reference data: Issuing office type codes — Procurement, Administration. SCD Type 1 — full refresh o
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_office_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_office_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_office_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Issuing office type codes — Procurement, Administration. SCD Type 1 — full refresh on change. Source: aasbs.lu_office_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_office_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_accrual_income_doc_type
-- Reference data: Accrual income document type codes for BAAR transmission. SCD Type 1 — full refresh 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_accrual_income_doc_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_accrual_income_doc_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_accrual_income_doc_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Accrual income document type codes for BAAR transmission. SCD Type 1 — full refresh on change. Source: aasbs.lu_accrual_income_doc_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_accrual_income_doc_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_onefund_program
-- Reference data: OneFund program codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_onefund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_onefund_program
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_onefund_program.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_onefund_program.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: OneFund program codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_onefund_program.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_onefund_program',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_onefund_activity
-- Reference data: OneFund activity codes with associated fund code. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_onefund_activity
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_onefund_activity.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_onefund_activity.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: OneFund activity codes with associated fund code. SCD Type 1 — full refresh on change. Source: aasbs.lu_onefund_activity.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_onefund_activity',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_group_contact_type
-- Reference data: Group contact category codes — CONTRACTING, NATIONAL_FSD, etc. SCD Type 1 — full ref
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_group_contact_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_group_contact_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_group_contact_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Group contact category codes — CONTRACTING, NATIONAL_FSD, etc. SCD Type 1 — full refresh on change. Source: aasbs.lu_group_contact_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_group_contact_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_award_validation
-- Reference data: Award validation criteria with remedy and sort order. SCD Type 1 — full refresh on c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_award_validation
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_award_validation.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_award_validation.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Award validation criteria with remedy and sort order. SCD Type 1 — full refresh on change. Source: aasbs.lu_award_validation.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_award_validation',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_funds_usage
-- Reference data: Fund usage purpose codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_funds_usage
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_funds_usage.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_funds_usage.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Fund usage purpose codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_funds_usage.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_funds_usage',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_central_comment_type
-- Reference data: Comment type codes for CENTRAL_COMMENT table. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_central_comment_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_central_comment_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_central_comment_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Comment type codes for CENTRAL_COMMENT table. SCD Type 1 — full refresh on change. Source: aasbs.lu_central_comment_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_central_comment_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_consolidated_contract
-- Reference data: FAR consolidated contract determination values. SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_consolidated_contract
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_consolidated_contract.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_consolidated_contract.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: FAR consolidated contract determination values. SCD Type 1 — full refresh on change. Source: aasbs.lu_consolidated_contract.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_consolidated_contract',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_idv_who_can_use
-- Reference data: Agency eligibility types for IDV vehicle use. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_idv_who_can_use
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_idv_who_can_use.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_idv_who_can_use.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Agency eligibility types for IDV vehicle use. SCD Type 1 — full refresh on change. Source: aasbs.lu_idv_who_can_use.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_idv_who_can_use',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transmittal_record_type
-- Reference data: Transmittal record type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transmittal_record_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transmittal_record_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transmittal_record_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Transmittal record type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_transmittal_record_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transmittal_record_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transmittal_stage
-- Reference data: Stages of releasing a transmission to Pegasys. SCD Type 1 — full refresh on change. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transmittal_stage
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transmittal_stage.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transmittal_stage.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Stages of releasing a transmission to Pegasys. SCD Type 1 — full refresh on change. Source: aasbs.lu_transmittal_stage.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transmittal_stage',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transmittal_status
-- Reference data: Transmission status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_tra
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transmittal_status
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transmittal_status.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transmittal_status.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Transmission status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_transmittal_status.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transmittal_status',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.ref.lu_transmittal_type
-- Reference data: Transmittal types with flat-file format code. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.ref.lu_transmittal_type
(
    code_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    code_value                                     STRING NOT NULL                COMMENT 'Natural key — the code value stored in transactional tables. Source: lu_transmittal_type.',
    code_desc                                      STRING                         COMMENT 'Decoded description of the code value.',
    short_desc                                     STRING                         COMMENT 'Short display label for UI and reports.',
    sort_order                                     INT                            COMMENT 'Display sort order.',
    is_active_flag                                 BOOLEAN                        COMMENT 'TRUE if this code is currently in active use.',
    effective_dt                                   DATE                           COMMENT 'Date this code became effective.',
    expiry_dt                                      DATE                           COMMENT 'Date this code was retired (NULL = still active).',
    source_table                                   STRING                         COMMENT 'Source lookup table name — lu_transmittal_type.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (code_sk, code_value)
COMMENT 'Reference data: Transmittal types with flat-file format code. SCD Type 1 — full refresh on change. Source: aasbs.lu_transmittal_type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'ref',
    'pipeline.table' = 'lu_transmittal_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP11',
    'pipeline.refresh' = 'weekly_or_on_change'
);


-- ==============================================================================
-- PIPELINE INFRASTRUCTURE — assist_catalog.common.pipeline_audit
-- Shared run log for all Bronze→Silver→Gold pipeline executions.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_catalog.common.pipeline_audit
-- Pipeline execution audit log — one row per task execution. Tracks watermarks, row counts, and errors
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_catalog.common.pipeline_audit
(
    run_sk                                         BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    job_run_id                                     STRING NOT NULL                COMMENT 'Databricks job_run_id — unique per execution.',
    job_name                                       STRING NOT NULL                COMMENT 'Databricks job name.',
    task_name                                      STRING                         COMMENT 'Task within the job (e.g. bronze_ingest, silver_transform, gold_merge).',
    data_product                                   STRING                         COMMENT 'Data product code — DP1 through DP11.',
    target_layer                                   STRING                         COMMENT 'Target layer — bronze, silver, gold.',
    target_table                                   STRING                         COMMENT 'Fully qualified target table name.',
    source_schema                                  STRING                         COMMENT 'Source Postgres schema.',
    source_table                                   STRING                         COMMENT 'Source Postgres table.',
    run_type                                       STRING NOT NULL                COMMENT 'Run type — FULL_PRIME or DELTA_REFRESH.',
    run_status                                     STRING NOT NULL                COMMENT 'Run status — RUNNING, SUCCESS, FAILED, PARTIAL.',
    start_ts                                       TIMESTAMP                      COMMENT 'Pipeline run start timestamp.',
    end_ts                                         TIMESTAMP                      COMMENT 'Pipeline run end timestamp.',
    duration_seconds                               INT                            COMMENT 'Run duration in seconds.',
    rows_read                                      BIGINT                         COMMENT 'Rows read from source.',
    rows_written                                   BIGINT                         COMMENT 'Rows written to target.',
    rows_inserted                                  BIGINT                         COMMENT 'Net new rows inserted (MERGE/INSERT).',
    rows_updated                                   BIGINT                         COMMENT 'Rows updated (MERGE UPDATE).',
    rows_soft_deleted                              BIGINT                         COMMENT 'Rows marked is_deleted=true.',
    watermark_col                                  STRING                         COMMENT 'Watermark column used for delta extraction (e.g. updated_dt).',
    watermark_from                                 TIMESTAMP                      COMMENT 'Watermark lower bound used in this run.',
    watermark_to                                   TIMESTAMP                      COMMENT 'Watermark upper bound used in this run (becomes next lower bound on success).',
    error_message                                  STRING                         COMMENT 'Error message if run_status = FAILED.',
    error_stack                                    STRING                         COMMENT 'Stack trace (truncated to 4000 chars) if run failed.',
    databricks_cluster_id                          STRING                         COMMENT 'Cluster ID that executed this run.',
    spark_app_id                                   STRING                         COMMENT 'Spark application ID for log correlation.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (job_run_id, target_table)
COMMENT 'Pipeline execution audit log — one row per task execution. Tracks watermarks, row counts, and errors for all 11 data products. Used by delta refresh jobs to determine next watermark.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'infrastructure',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'common',
    'pipeline.table' = 'pipeline_audit',
    'pipeline.managed_by' = 'all_pipelines'
);
