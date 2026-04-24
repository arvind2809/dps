-- ==============================================================================
-- SHARED COMMON DIMENSIONS — assist_dev.common
-- Used by 2 or more data products. Referenced via FK surrogate key.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.common.dim_date
-- Date dimension — full calendar and federal fiscal year spine. Covers FY2000–FY2035.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_date
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
-- assist_dev.common.dim_agency
-- Federal agency and activity address dimension. SCD Type 2 — tracks agency name and bureau changes. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_agency
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
-- assist_dev.common.dim_ia
-- Interagency Agreement dimension. SCD Type 2 — tracks status, cost, and period changes. Sources: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_ia
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
-- assist_dev.common.dim_award
-- Award and contract dimension. SCD Type 2 — reflects each modification. Sources: aasbs.award, aasbs.a
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_award
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
-- assist_dev.common.dim_line_item
-- CLIN/SLIN dimension — one row per contract line item per effective version. SCD Type 2. Sources: aas
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_line_item
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
-- assist_dev.common.dim_loa
-- Line of Accounting (LOA) dimension. SCD Type 2 — tracks fund status and agreement changes. Sources: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_loa
(
    loa_sk                                         BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    loa_id                                         BIGINT NOT NULL                COMMENT 'Natural key — source aasbs.loa.id.',
    tracking_num                                   STRING                         COMMENT 'LOA tracking number — unique per LOA string.',
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
-- assist_dev.common.dim_funding
-- Funding package dimension. SCD Type 2 — tracks amendment changes. Sources: aasbs.funding, aasbs.fund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.dim_funding
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
-- PIPELINE INFRASTRUCTURE — assist_dev.common.pipeline_audit
-- Shared run log for all Bronze→Silver→Gold pipeline executions.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.common.pipeline_audit
-- Pipeline execution audit log — one row per task execution. Tracks watermarks, row counts, and errors
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.common.pipeline_audit
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
