-- =============================================================================
-- ASSIST OCFO — Silver Layer DDL
-- Catalog  : assist_dev
-- Schema   : assist_finance
-- Tables   : silver_<source_schema>_<table_name>  (flat namespace)
-- Source   : PostgreSQL (ASSIST OCFO operational database)
-- Runtime  : Databricks Runtime 14.x LTS
-- Format   : Delta Lake
-- Generated: 2026-03-05
--
-- Design principles applied:
--   • Medallion architecture — Silver is cleansed, typed, audit-enriched source copy
--   • All Postgres types mapped to Delta-native types (see type map below)
--   • Column comments preserved from source DDL COMMENT ON COLUMN statements
--   • Soft-delete tracking: is_deleted + deleted_at (rows never physically removed)
--   • Four standard pipeline audit columns on every table
--   • Delta Liquid Clustering replaces PARTITION BY for DBR 14 best practice
--   • delta.enableChangeDataFeed = true on all tables (feeds Gold MERGE operations)
--   • autoOptimize enabled for write amplification control
--   • No FK constraints enforced at Silver — integrity enforced at Gold layer
--   • NOT NULL retained only on PK columns at Silver
--
-- Type mapping applied (Postgres → Delta):
--   bigint / bigserial          → BIGINT
--   integer / smallint / serial → INT
--   numeric(p,s)                → DECIMAL(p,s)  | bare numeric → DECIMAL(15,2)
--   character varying(n)        → STRING        | length dropped, enforced upstream
--   character(n)                → STRING        | trimmed on ingest
--   timestamp without time zone → TIMESTAMP
--   boolean                     → BOOLEAN
--   date                        → DATE
--   text / jsonb / uuid         → STRING
--   double precision / real     → DOUBLE
--
-- Soft-delete columns (every table):
--   is_deleted  BOOLEAN    — TRUE when row removed from source
--   deleted_at  TIMESTAMP  — when soft-delete was detected (NULL = active)
--
-- Pipeline audit columns (every table):
--   _ingested_at   TIMESTAMP — when row landed in Silver
--   _source_system STRING    — always 'assist_postgres'
--   _batch_id      STRING    — Databricks job_run_id for tracing
--   _checksum      STRING    — MD5 of business key columns
-- =============================================================================

-- Pre-requisite: ensure catalog and schema exist
CREATE CATALOG IF NOT EXISTS assist_dev
  COMMENT 'ASSIST OCFO Federal Acquisition data platform — Unity Catalog root';

CREATE SCHEMA IF NOT EXISTS assist_dev.assist_finance
  COMMENT 'ASSIST OCFO Silver layer — cleansed, typed, audit-enriched copies of all 211 Postgres source tables. Flat namespace: silver_<source_schema>_<table>.';



-- =============================================================================
-- SOURCE SCHEMA: aasbs  (166 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acceptance
-- Source : aasbs.acceptance
-- Comment: Contains high-level invoice details for all invoices within the ASSIST application
-- Columns: 19 source + 6 audit = 25 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acceptance
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_SEQ',
    invoice_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE',
    gsa_acceptance_status_cd                      STRING             COMMENT 'Foreign key to AASBS.LU_ACCEPTANCE_STATUS',
    total_approved_invoice_amt                    DECIMAL(15,2)             COMMENT 'Total approved amount for this Invoice',
    client_acceptance_status_cd                   STRING             COMMENT 'Foreign key to AASBS.LU_ACCEPTANCE_STATUS',
    client_receipt_dt                             TIMESTAMP             COMMENT 'Client Receipt date of Invoice',
    client_acceptance_dt                          TIMESTAMP             COMMENT 'Client Acceptance date of Invoice',
    client_authorized_dt                          TIMESTAMP             COMMENT 'Client Authorized date of Invoice',
    client_authorized_user_id                     STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    gsa_receipt_dt                                TIMESTAMP             COMMENT 'GSA Receipt date of Invoice',
    gsa_acceptance_dt                             TIMESTAMP             COMMENT 'GSA Acceptance date of Invoice',
    gsa_authorized_user_id                        STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    gsa_authorized_dt                             TIMESTAMP             COMMENT 'Date GSA Authorized the Acceptance Report',
    draft_acceptance_status_cd                    STRING             COMMENT 'Foreign Key to table AASBS.LU_DRAFT_ACCEPTANCE_STATUS',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    determination_reason                          STRING             COMMENT 'Reason for Rejection or Partial Acceptance from GSA user or VITAP',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains high-level invoice details for all invoices within the ASSIST application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acceptance',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acceptance',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acceptance_dist
-- Source : aasbs.acceptance_dist
-- Comment: Contains the one or more Line Items under a single Invoice
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acceptance_dist
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_DIST_SEQ',
    acceptance_id                                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE',
    acceptance_item_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE_ITEM',
    distribute_amt                                DECIMAL(15,2)             COMMENT 'Amount to be billed for an acceptance item',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the one or more Line Items under a single Invoice'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acceptance_dist',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acceptance_dist',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acceptance_item
-- Source : aasbs.acceptance_item
-- Comment: Contains the one or more Line Items under a single Invoice
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acceptance_item
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_ITEM_SEQ',
    acceptance_id                                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE',
    invoice_item_id                               BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE_ITEM',
    acceptance_item_type_cd                       STRING             COMMENT 'Foreign Key to table AASBS.LU_ACCEPTANCE_ITEM_TYPE',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    item_client_recommended_amt                   DECIMAL(15,2)             COMMENT 'Client-Recommended amount for this Invoiced Item',
    item_gsa_approved_amt                         DECIMAL(15,2)             COMMENT 'GSA-Approved amount for this Invoiced Item',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    distribution_status_cd                        STRING             COMMENT 'Foreign key to AASBS.LU_AR_DISTRIBUTION_STATUS',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the one or more Line Items under a single Invoice'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acceptance_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acceptance_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_accrual_expense
-- Source : aasbs.accrual_expense
-- Comment: Monthly Accrual Expense calculations for deliverable line items
-- Columns: 34 source + 6 audit = 40 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_accrual_expense
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_EXPENSE_SEQ',
    accrual_year                                  INT             COMMENT '4 digit calendar year for accrual run.',
    accrual_month                                 STRING             COMMENT '2 character calendar month for accrual run.',
    accrual_month_end_dt                          TIMESTAMP             COMMENT 'Last calendar date of the month for accrual run.',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    line_item_start_dt                            TIMESTAMP             COMMENT 'Service Start date for the latest awarded line item',
    line_item_end_dt                              TIMESTAMP             COMMENT 'Service End date for the latest awarded line item',
    ceiling_amt                                   DECIMAL(15,2)             COMMENT 'Ceiling amount for the latest awarded line item',
    obligated_amt                                 DECIMAL(15,2)             COMMENT 'Funded (Obligated) amount for the latest awarded line item',
    invoice_approved_amt                          DECIMAL(15,2)             COMMENT 'Sum of Approved Invoice amounts from Acceptance Reports',
    udo_amt                                       DECIMAL(15,2)             COMMENT 'Undelivered Order (UDO) = Line Item Funded/Obligated Cost amount minus Invoice Approved amount',
    pop_completed_pct                             DECIMAL(11,8)             COMMENT 'Percentage of the Period of Performance that has passed:  (Accrual month end date - POP Start) / (POP End - POP Start)',
    pop_delivered_amt                             DECIMAL(15,2)             COMMENT 'Delivered amount based on POP Percent Completed:  (POP Percent Completed * Ceiling amount)',
    prelim_completed_accrual_amt                  DECIMAL(15,2)             COMMENT 'Preliminary Completed Accrual amount-can be negative:  (POP Delivered amount – Invoice Approved amount)',
    accrual_expense_amt                           DECIMAL(15,2)             COMMENT 'Expense Accrual amount:  If Prelim Completed Accrual < 0, then 0.  If Prelim Completed Accrual > UDO then UDO else Prelim Completed Accrual.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    include_in_transmission_yn                    STRING             COMMENT 'Specifies if record will be included in transmission',
    invalid_udo_amt                               DECIMAL(15,2)             COMMENT 'Invalid udo amount',
    contract_type_cd                              STRING             COMMENT 'Contract type code',
    accrual_universal_holdback_pct                DECIMAL(19,2)             COMMENT 'Accrual universal holdback percent',
    usr_keyed_holdback                            DECIMAL(19,2)             COMMENT 'User accrual holdback percent',
    udo_reduced_by_invalid_amt                    DECIMAL(15,2)             COMMENT 'Udo reduced by invalid udo amount',
    accrual_expense_amt_before_holdback           DECIMAL(15,2)             COMMENT 'Accrual expense before holdback applied',
    accrual_expense_amt_with_holdback_applied     DECIMAL(15,2)             COMMENT 'Accrual expense with holdback applied',
    accrual_expense_calc_comment                  STRING             COMMENT 'Accrual expense calculation comments',
    accrual_holdback_type_cd                      STRING             COMMENT 'Accrual Holdback Type',
    accrual_transmission_status_cd                STRING             COMMENT 'Accrual Transmission Status Code',
    udo_saf_amt                                   DECIMAL(15,2)             COMMENT 'Line item total SAF amount. When aasbs.loa.subject_to_availability_yn = Y, then SAF',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Monthly Accrual Expense calculations for deliverable line items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'accrual_expense',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_accrual_expense',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_accrual_inclusion_tracker
-- Source : aasbs.accrual_inclusion_tracker
-- Comment: Via Financial Services, stores FSD decisions to override the default selection of line items for Acc
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_accrual_inclusion_tracker
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCLUSION_TRACKER_SEQ',
    li_tracking_num                               BIGINT             COMMENT 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCLUSION_TRACKER_SEQ',
    accrual_inclusion_status_cd                   STRING             COMMENT 'Foreign Key to table AASBS.LU_ACCRUAL_INCLUSION_TRACKER_STATUS',
    was_manually_set_yn                           STRING             COMMENT 'Flag indicating if the acrrual inclusion status was manually set by a user (=Y) or not (=N).  Not/N indicates the status was set automatically',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    invalid_udo_amt                               DECIMAL(15,2),
    usr_keyed_holdback                            DECIMAL(19,2),
    comments                                      STRING,
    accrual_holdback_type_cd                      STRING             COMMENT 'Accrual Holdback Type',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Via Financial Services, stores FSD decisions to override the default selection of line items for Accrual calculations'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'accrual_inclusion_tracker',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_accrual_inclusion_tracker',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_accrual_income
-- Source : aasbs.accrual_income
-- Comment: Monthly Accrual Income calculations for deliverable and surcharge line items
-- Columns: 19 source + 6 audit = 25 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_accrual_income
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_SEQ',
    accrual_expense_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCRUAL_EXPENSE',
    accrual_year                                  INT             COMMENT '4 digit calendar year for accrual run.',
    accrual_month                                 STRING             COMMENT '2 character calendar month for accrual run.',
    accrual_month_end_dt                          TIMESTAMP             COMMENT 'Last calendar date of the month for accrual run.',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    surcharge_rate_pct                            DECIMAL(7,4)             COMMENT 'Surcharge percentage rate from LI_AWARD.SURCHARGE_RATE_PCT',
    accrual_income_amt                            DECIMAL(15,2)             COMMENT 'Income Accrual amount:  For Cost, ACCRUAL_EXPENSE.ACCRUAL_EXPEENSE_AMT.  For Fee, ACCRUAL_EXPENSE.ACCRUAL_EXPENSE_AMT * Surcharge Rate.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    include_in_transmission_yn                    STRING             COMMENT 'Specifies if record will be included in transmission',
    li_obligated_amt                              DECIMAL(15,2)             COMMENT 'Line item obligated amount',
    li_billed_amt                                 DECIMAL(15,2)             COMMENT 'Line item billed amount',
    li_unbilled_amt                               DECIMAL(15,2)             COMMENT 'Line item unbilled amount',
    li_surcharge_yn                               STRING             COMMENT 'Line item is LI_AWARD_SURCHG_PCT',
    accrual_income_calc_comment                   STRING             COMMENT 'Accrual income calculation comments',
    accrual_transmission_status_cd                STRING             COMMENT 'Accrual Transmission Status Code',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Monthly Accrual Income calculations for deliverable and surcharge line items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'accrual_income',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_accrual_income',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_accrual_income_dist
-- Source : aasbs.accrual_income_dist
-- Comment: Monthly Accrual Income calculations for deliverable and surcharge line items
-- Columns: 18 source + 6 audit = 24 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_accrual_income_dist
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_DIST_SEQ',
    accrual_income_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCRUAL_INCOME',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num',
    distribute_amt                                DECIMAL(15,2)             COMMENT 'Amount from the LOA to distribute/allocate to this Income Accrual amount.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    include_in_transmission_yn                    STRING             COMMENT 'Specifies if record will be included in transmission',
    loa_obligated_amt                             DECIMAL(15,2)             COMMENT 'Loa obligated amount',
    loa_billed_amt                                DECIMAL(15,2)             COMMENT 'Loa billed amount',
    loa_unbilled_amt                              DECIMAL(15,2)             COMMENT 'Loa unbilled amount',
    burn_order                                    BIGINT             COMMENT 'Order of depletion',
    proration_pct                                 DECIMAL(19,16)             COMMENT 'The percent of the total unbilled amount of the line item this LOA presents',
    proration_dist_amt                            DECIMAL(15,2)             COMMENT 'LOA Unbilled per Line Item / Total LOA Unbilled Amount for a given Line Item * Line Item Income Accrual Amount',
    accrual_income_dist_comment                   STRING             COMMENT 'Accrual income distribution comments',
    accrual_transmission_status_cd                STRING             COMMENT 'Accrual Transmission Status Code',
    subject_to_availability_yn                    STRING             COMMENT 'Flag indicating whether funds were Subject to Availability for the LOA (=Y) or not (=N)',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Monthly Accrual Income calculations for deliverable and surcharge line items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'accrual_income_dist',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_accrual_income_dist',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_accrual_income_dist_summary
-- Source : aasbs.accrual_income_dist_summary
-- Comment: Monthly Accrual Income calculations for deliverable and surcharge line items
-- Columns: 13 source + 6 audit = 19 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_accrual_income_dist_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_DIST_SUM_SEQ',
    accrual_expense_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCRUAL_EXPENSE',
    deliverable_accrual_income_dist_id            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCRUAL_INCOME_DIST for deliverable line item',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num',
    distribute_amt                                DECIMAL(15,2)             COMMENT 'Amount from the LOA to distribute/allocate to this Income Accrual amount which is a sum of distributed amounts for deliverable and surcharge line items.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    include_in_transmission_yn                    STRING             COMMENT 'Specifies if record will be included in transmission',
    surcharge_accrual_income_dist_id              BIGINT             COMMENT '[FK] Foreign Key to table aasbs.ACCRUAL_INCOME_DIST for surcharge line item',
    accrual_income_dist_summary_comment           STRING             COMMENT 'Accrual income distribution summary comments',
    accrual_transmission_status_cd                STRING             COMMENT 'Accrual Transmission Status Code',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Monthly Accrual Income calculations for deliverable and surcharge line items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'accrual_income_dist_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_accrual_income_dist_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition
-- Source : aasbs.acquisition
-- Comment: CENTRAL TABLE - this table is the foundation for Assist and parent table to the SOLICITATION table
-- Columns: 37 source + 6 audit = 43 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_SEQ',
    piid                                          STRING             COMMENT 'Procurement Instrument Identifier',
    piid_ext                                      STRING             COMMENT 'Acquisition PIID Extension',
    procurement_id                                BIGINT             COMMENT '[FK] Foreign Key to table ASSIST.PROCUREMENT',
    acquisition_status_cd                         STRING             COMMENT 'Foreign Key to table AASBS.LU_ACQUISITION_STATUS',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS',
    region_cd                                     STRING             COMMENT 'Foreign Key to table AASBS.LU_REGION',
    title                                         STRING             COMMENT 'Acquisition Title',
    description                                   STRING             COMMENT 'Description of the Acquisition',
    program_cd                                    STRING             COMMENT 'Foreign Key to table AASBS.LU_PROGRAM',
    agreement_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_AGREEMENT_TYPE',
    fund_category_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_CATEGORY',
    fund_cd                                       STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND',
    client_id                                     STRING             COMMENT '[FK] Foreign Key to table TABLE_MASTER.CLIENTS',
    acquisition_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACQUISITION_TYPE',
    performance_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_PERFORMANCE_TYPE',
    severability_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE',
    includes_options_cnt                          INT             COMMENT 'Number of option periods included (when INCLUDES_OPTIONS_YN = Y)',
    requirements_received_dt                      TIMESTAMP             COMMENT 'Date requirements were received',
    requirements_finalized_dt                     TIMESTAMP             COMMENT 'Date requirements were finalized',
    requirements_description                      STRING             COMMENT 'Free text description of the Acquisition Requirements',
    parent_acquisition_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION.  When this column has a value, it indicates that this Acquisition falls under a BPA and the Foreign Key to the "parent" BPA Acquisition is captured in this column',
    continuity_type_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_CONTINUITY_TYPE',
    transitional_bridge_yn                        STRING             COMMENT 'Flag indicating whether a Acqusition is a Transitional Bridge Acquisition (=Y)  or not (=N)',
    opportunity_info                              STRING             COMMENT 'Free text field to hold Opportunity ID(s)',
    preceding_reference_info                      STRING             COMMENT 'Free text field to hold Preceeding Reference ID(s)',
    incumbent_info                                STRING             COMMENT 'Free text field to hold Incumbent Name',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    draft_frm_mgmt_yn                             STRING             COMMENT 'Flag indicating whether Acquisition Management Form is (=Y) or is not (=N) in DRAFT state',
    draft_frm_reqs_yn                             STRING             COMMENT 'Flag indicating whether Acquisition Requirements Form is (=Y) or is not (=N) in DRAFT state',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    multi_tenant_yn                               STRING             COMMENT 'Flag indicating whether an Acquisition is Multitenant (=Y) or not (=N)',
    acquisition_psc_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LUS_PSC',
    apex_emp_org_id                               BIGINT             COMMENT '[FK] References ASSIST.LU_EMP_ORGS.EMP_ORG_ID',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'CENTRAL TABLE - this table is the foundation for Assist and parent table to the SOLICITATION table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_closeout
-- Source : aasbs.acquisition_closeout
-- Comment: Central Acquisition Closeout Table - One record per Acquisition Closeout
-- Columns: 18 source + 6 audit = 24 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_closeout
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_CLOSEOUT_SEQ',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    prohibition_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_CLOSEOUT_PROHIBITION',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    co_checked_certified_dt                       TIMESTAMP             COMMENT 'Date and time that the Contracting Officer Certification checkbox was checked',
    fma_certified_user_id                         STRING             COMMENT '[FK] Financial Management Analyst that certified the Closeout',
    fma_certified_dt                              TIMESTAMP             COMMENT 'Date and time that the Financial Management Analyst certified the Closeout',
    last_call_or_order_num                        STRING             COMMENT 'The Award PIID of the final Call or Order issued - for IDVs only - Not Applicable otherwise',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    lead_closeout_rep_user_id                     STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    ytd_total_vendor_invoiced_amt                 DECIMAL(15,2)             COMMENT 'Year to Date Total Vendor Invoiced Amount',
    ytd_total_customer_billed_amt                 DECIMAL(15,2)             COMMENT 'Year to Date Total Customer Billed Amount',
    contract_funds_reconciled_yn                  STRING             COMMENT 'Flag indicating whether Contract Funds have (=Y) or have not (=N) been reconciled',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    draft_frm_closeout_yn                         STRING             COMMENT 'Flag indicating whether the Closeout Form is (=Y) or is not (=N) in DRAFT state',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Central Acquisition Closeout Table - One record per Acquisition Closeout'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_closeout',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_closeout',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_closeout_checklist
-- Source : aasbs.acquisition_closeout_checklist
-- Comment: Join table - links between Acquisition Closeout, Checklist Items, and their related values
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_closeout_checklist
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQ_CLOSEOUT_CHECKLIST_SEQ',
    acquisition_closeout_id                       BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION_CLOSEOUT',
    checklist_item_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_CHECKLIST_ITEM',
    checklist_value_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_CHECKLIST_VALUE',
    checklist_item_dt                             TIMESTAMP             COMMENT 'Checklist Item Date either chosen or automatically set',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table - links between Acquisition Closeout, Checklist Items, and their related values'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_closeout_checklist',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_closeout_checklist',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_mod
-- Source : aasbs.acquisition_mod
-- Comment: Acquisition Mod
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_mod
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_SEQ',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    mod_num                                       STRING             COMMENT 'This column represented Award Mods during the initial rollout of ASSIST2.0.  That functionality will now be in AWARD_MOD.MOD_NUM',
    acquisition_mod_status_cd                     STRING             COMMENT 'Foreign Key to table AASBS.LU_ACQUISITION_MOD_STATUS',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Acquisition Mod'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_mod',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_mod',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_mod_address
-- Source : aasbs.acquisition_mod_address
-- Comment: Join table - links between Acquisition and associated addresses of various types (shipping, acceptin
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_mod_address
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_ADDRESS_SEQ',
    acquisition_mod_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION_MOD',
    address_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    address_type_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_ADDRESS_TYPE',
    multi_address_large_data_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.MULTI_ADDRESS_LARGE_DATA',
    same_as_client_address_yn                     STRING             COMMENT 'Flag indicating whether the address is the same as the client address (=Y) or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table - links between Acquisition and associated addresses of various types (shipping, accepting, etc)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_mod_address',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_mod_address',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_mod_ia
-- Source : aasbs.acquisition_mod_ia
-- Comment: Join table associating Acquisition Mods with Interagency Agreements
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_mod_ia
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_IA_SEQ',
    acquisition_mod_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION_MOD',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
    legacy_ia_num                                 STRING             COMMENT 'Original IA Number used in Legacy ASSIST',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    created_from_ia_yn                            STRING             COMMENT 'Flag indicating whether the acquisition was created from an IA or from Acquisition Search page.',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table associating Acquisition Mods with Interagency Agreements'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_mod_ia',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_mod_ia',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_mod_responsible
-- Source : aasbs.acquisition_mod_responsible
-- Comment: Contains list of personnel associated with an Acquisition
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_mod_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_RESPONSIBLE_SEQ',
    acquisition_mod_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table AASBS.USER',
    responsible_role_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ROLE',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with an Acqusition is the primary for their role (=Y)  or not (=N)',
    group_contact_id                              BIGINT             COMMENT '[FK] Foreign Key to table AASBS.GROUP_CONTACT',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains list of personnel associated with an Acquisition'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_mod_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_mod_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_acquisition_plan
-- Source : aasbs.acquisition_plan
-- Comment: Contains Acquisition Plans related to a parent Acquisition
-- Columns: 65 source + 6 audit = 71 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_acquisition_plan
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_PLAN_SEQ',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    instrument_type_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE',
    vehicle_type_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_VEHICLE_TYPE',
    contract_type_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_CONTRACT_TYPE',
    acquisition_start_dt                          TIMESTAMP             COMMENT 'Date of the Acquisition Start',
    expedited_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_EXPEDITED_TYPE',
    planned_fy_of_award                           INT             COMMENT 'Planned fiscal year of the Award',
    expedited_reqd_award_dt                       TIMESTAMP             COMMENT 'Date of Required Award Date (only applicable when Acquisition Plan is "Expedited"',
    expedited_reason                              STRING             COMMENT 'Free text containing the reason an Acquisition Plan is being expedited',
    expedited_comments                            STRING             COMMENT 'Free text Comments about the Expedited aspects of the Acquisition Plan',
    negotiated_award_dt                           TIMESTAMP             COMMENT 'Negotiated Award Date',
    planned_palt_days_cnt                         BIGINT             COMMENT 'Planned PALT (this column is going to go away - dynamically calculated days between two other column dates)',
    funding_received_dt                           TIMESTAMP             COMMENT 'Date Funding was received',
    funding_end_dt                                TIMESTAMP             COMMENT 'Date of end of Funding',
    comments                                      STRING             COMMENT 'Free text Comments about the Acquisition Plan',
    idv_referenced_class_cd                       STRING             COMMENT 'Foreign Key to table AASBS.LU_IDV_REFERENCED_CLASS',
    idv_type_of_idc_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_IDV_TYPE_OF_IDC',
    idv_who_can_use_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_IDV_WHO_CAN_USE',
    idv_fips95_other_stmt                         STRING             COMMENT 'Explanatory Text related to FIPS Codes',
    idv_funds_obligated_yn                        STRING             COMMENT 'Flag indicating whether Funds are Obligated for this Acquisition Plan (=Y)  or not (=N)',
    idv_family_name                               STRING             COMMENT 'IDV Family Name',
    competition_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_COMPETITION_TYPE',
    processing_speed_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_PROCESSING_SPEED',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    national_emergency_yn                         STRING             COMMENT 'Flag indicating whether a Acqusition Plan is a national emergency (=Y)  or not (=N)',
    independent_gov_estimate_amt                  DECIMAL(15,2)             COMMENT 'Independent Government Estimate (IGE) of Acquisition Cost',
    commercial_type_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_COMMERCIAL_TYPE',
    draft_frm_plan_yn                             STRING             COMMENT 'Flag indicating whether Acquisition Plan Form is (=Y) or is not (=N) in DRAFT state',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    ap_bundled_contract_cd                        STRING             COMMENT 'Identify whether this acquisition meets the definition of a bundled requirement, as defined in the FAR.',
    ap_consolidated_contract_cd                   STRING             COMMENT 'Identify whether this acquisition meets the definition of a consolidated requirement, as defined in the FAR.',
    ap_emergency_acq_cd                           STRING             COMMENT 'Identify whether this acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.',
    ap_sm_business_admin_cd                       STRING,
    ap_transfer_cd                                STRING             COMMENT 'Identify whether this action represent the transfer of contract administration responsibilities.',
    ap_perf_based_svc_contract_cd                 STRING             COMMENT 'Identify whether this acquisition meets the requirements for performance-based, as defined in the FAR.',
    ap_transitional_bridge_yn                     STRING             COMMENT 'Identify whether this is a new short-term acquisition on a sole source basis to avoid lapses in service caused by delays in awarding subsequent contracts.',
    ap_commercial_solution_open_yn                STRING             COMMENT 'Identify whether this acquisition is being conducted using Commercial Solutions Opening (CSO) procedures, in accordance with GSAM Part 571.',
    ap_cor_delegation_cd                          STRING             COMMENT 'Identify the method of Contracting Officer\'s Representative (COR) delegation being used for this acquisition.',
    ap_intel_community_cd                         STRING             COMMENT 'Identify whether this acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence',
    ap_supply_chain_risk_cd                       STRING             COMMENT 'Identify whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5',
    ap_surveillance_spec_cd                       STRING             COMMENT 'Identify whether this acquisition requires a Contractor Security Classification Specification (DD-254).',
    ap_oconus_support_yn                          STRING             COMMENT 'Identify whether this acquisition involves the performance of work outside the Continental United States',
    ap_tech_dir_of_letters_yn                     STRING             COMMENT 'Identify whether this acquisition will use technical directions (or similar method) to further clarify work requirements in the post-award administration.',
    ap_other_flags                                STRING             COMMENT 'This field may be used to identify additional user-specific flags, as needed.',
    idv_referenced_family_id                      INT             COMMENT '[FK] Foreign key to table table_master.contract_families.',
    ap_dpas_rated_yn                              STRING             COMMENT 'Defense Priorities and Allocations Systems (DPAS), Flag indicating whether DPAS Rated should be Yes (=Y) or NO (=N)',
    combatant_command_cd                          STRING             COMMENT 'identify whether this acquisition is being conducted on behalf of a u.s. department of defense combatant command',
    isr_cd                                        STRING             COMMENT 'Identify whether this acquisition has an associated ISR code',
    sbir_sttr_p_i_sol_topic_code                  STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Code(s)',
    sbir_sttr_p_i_sol_topic_name                  STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Name',
    sbir_sttr_p_i_funding_agr_num                 STRING             COMMENT 'Additional SBIR and STTR data: Phase I Funding Agreement Number(s)',
    sbir_sttr_p_i_sol_topic_agency_branch         STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Agency/Agencies and Branch(es)',
    sbir_sttr_p_ii_sol_topic_code                 STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Code(s)',
    sbir_sttr_p_ii_sol_topic_name                 STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Name',
    sbir_sttr_p_ii_funding_agr_num                STRING             COMMENT 'Additional SBIR and STTR data: Phase II Funding Agreement Number(s)',
    sbir_sttr_p_ii_sol_topic_agency_branch        STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Agency/Agencies and Branch(es)',
    sbir_sttr_p_iii_fund_agr_award_office         STRING             COMMENT 'Additional SBIR and STTR data: Phase III Funding Agreement Awarding Office(s)',
    sbir_sttr_p_iii_fund_agr_number               STRING             COMMENT 'Additional SBIR and STTR data: Phase III Funding Agreement Number(s)',
    service_delivery_model_cd                     STRING             COMMENT 'Foreign Key to table AASBS.LU_SERVICE_DELIVERY_MODEL',
    no_cost_contract_yn                           STRING             COMMENT 'Flag indicating whether the acquisition is associated to an award that has no associated costs. Should be Yes (=Y) or NO (=N)',
    commerciality_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_commerciality',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Acquisition Plans related to a parent Acquisition'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'acquisition_plan',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_acquisition_plan',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_address
-- Source : aasbs.address
-- Comment: Contains all address information in Assist
-- Columns: 18 source + 6 audit = 24 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_address
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.ADDRESS_SEQ',
    office_name                                   STRING             COMMENT 'Office name associated with Address record',
    attention                                     STRING             COMMENT 'Person designated as the contact person with Address record',
    email                                         STRING             COMMENT 'Email associated with Address record',
    address1                                      STRING             COMMENT 'Address Line 1',
    address2                                      STRING             COMMENT 'Address Line 2',
    city                                          STRING             COMMENT 'Address City',
    state_cd                                      STRING             COMMENT 'Foreign Key to table AASBS.LU_STATE',
    country_cd                                    STRING             COMMENT 'Foreign Key to table AASBS.LU_COUNTRY',
    zip                                           STRING             COMMENT 'Zip Code',
    fax_num                                       STRING             COMMENT 'Address Fax Number',
    phone_num                                     STRING             COMMENT 'Address Phone Number',
    phone_ext                                     STRING             COMMENT 'Address Phone Extension',
    address_info                                  STRING             COMMENT 'Free text field to hold general information for each Address record',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains all address information in Assist'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'address',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_address',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award
-- Source : aasbs.award
-- Comment: Contains high-level award details for all awards within the ASSIST application
-- Columns: 54 source + 6 audit = 60 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_SEQ',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    lead_svc_activity_address_cd                  STRING             COMMENT 'Lead Servicing AAC - same as the ACTIVITY_ADDRESS_CD on the parent ACQUISITION record',
    latest_award_mod_id                           BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    solicit_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    solicit_response_id                           BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_RESPONSE',
    award_status_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_STATUS',
    award_piid                                    STRING             COMMENT 'Award Procurement Instrument Identifier (PIID)',
    award_title                                   STRING             COMMENT 'User-entered Award Title',
    award_fy                                      INT             COMMENT 'Award Fiscal Year',
    award_fin                                     STRING             COMMENT 'Foreign Key to table AASBS.AWARD_FIN_LOG. Financial Identification Number - Award Level (AWARD_FIN = Award FIN) - see LINE_ITEM_ACCEPTED table for SC_FIN (Service Charge FIN)',
    multi_tenant_yn                               STRING             COMMENT 'Indicates if the Award is for a Multi-Tenant Acquisition',
    fpds_cancelled_yn                             STRING             COMMENT 'Flag indicating whether FPDS was cancelled (=Y) or not (=N)',
    fpds_cancel_reason_cd                         STRING             COMMENT 'Foreign Key to table AASBS.LU_FPDS_CANCEL_REASON',
    project_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.PROJECT',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    far_attachments_yn                            STRING             COMMENT 'Flag indicating whether Contract/Purchase Order FAR Attachments exist (=Y) or not (=N)',
    cor_delegation_cd                             STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify the method of Contracting Officer\'s Representative (COR) delegation being used for this acquisition.',
    intel_community_cd                            STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence',
    supply_chain_risk_cd                          STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5',
    surveillance_spec_cd                          STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition requires a Contractor Security Classification Specification (DD-254).',
    oconus_support_yn                             STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition involves the performance of work outside the Continental United States',
    tech_dir_of_letters_yn                        STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition will use technical directions (or similar method) to further clarify work requirements in the post-award administration.',
    other_flags                                   STRING             COMMENT 'Copied from Acquisition upon award creation, then editable. This field may be used to identify additional user-specific flags, as needed.',
    bundled_contract_cd                           STRING             COMMENT 'Identify whether this acquisition meets the definition of a bundled requirement, as defined in the FAR.',
    consolidated_contract_cd                      STRING             COMMENT 'Identify whether this acquisition meets the definition of a consolidated requirement, as defined in the FAR.',
    emergency_acq_cd                              STRING             COMMENT 'Identify whether this acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.',
    sm_business_admin_cd                          STRING,
    transfer_cd                                   STRING             COMMENT 'Identify whether this action represent the transfer of contract administration responsibilities.',
    perf_based_svc_contract_cd                    STRING             COMMENT 'Identify whether this acquisition meets the requirements for performance-based, as defined in the FAR.',
    transitional_bridge_yn                        STRING             COMMENT 'Identify whether this is a new short-term acquisition on a sole source basis to avoid lapses in service caused by delays in awarding subsequent contracts.',
    commercial_solution_opening_yn                STRING             COMMENT 'Identify whether this acquisition is being conducted using Commercial Solutions Opening (CSO) procedures, in accordance with GSAM Part 571.',
    award_aac_visibility_cd                       STRING,
    cor_report_suppress_reason_cd                 STRING             COMMENT 'FOREIGN KEY TO TABLE AASBS.LU_AWARD_COR_REPORT_SUPPRESS_REASON',
    show_award_cor_report_yn                      STRING             COMMENT 'Flag indicating whether cor report should be created and displayed (=Y) or suppressed (=N)',
    dpas_rated_yn                                 STRING             COMMENT 'Defense Priorities and Allocations Systems (DPAS), Flag indicating whether DPAS Rated should be Yes (=Y) or NO (=N)',
    apex_emp_org_id                               BIGINT             COMMENT '[FK] Apex office associated to this award',
    combatant_command_cd                          STRING             COMMENT 'copied from acquisition upon award creation, then editable. identify whether this acquisition is being conducted on behalf of a u.s. department of defense combatant command',
    isr_cd                                        STRING             COMMENT 'copied from acquisition upon award creation, then editable. identify whether this acquisition has an associated isr code',
    sbir_sttr_p_i_sol_topic_code                  STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Code(s)',
    sbir_sttr_p_i_sol_topic_name                  STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Name',
    sbir_sttr_p_i_funding_agr_num                 STRING             COMMENT 'Additional SBIR and STTR data: Phase I Funding Agreement Number(s)',
    sbir_sttr_p_i_sol_topic_agency_branch         STRING             COMMENT 'Additional SBIR and STTR data: Phase I Solicitation Topic Agency/Agencies and Branch(es)',
    sbir_sttr_p_ii_sol_topic_code                 STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Code(s)',
    sbir_sttr_p_ii_sol_topic_name                 STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Name',
    sbir_sttr_p_ii_funding_agr_num                STRING             COMMENT 'Additional SBIR and STTR data: Phase II Funding Agreement Number(s)',
    sbir_sttr_p_ii_sol_topic_agency_branch        STRING             COMMENT 'Additional SBIR and STTR data: Phase II Solicitation Topic Agency/Agencies and Branch(es)',
    sbir_sttr_p_iii_fund_agr_award_office         STRING             COMMENT 'Additional SBIR and STTR data: Phase III Funding Agreement Awarding Office(s)',
    sbir_sttr_p_iii_fund_agr_number               STRING             COMMENT 'Additional SBIR and STTR data: Phase III Funding Agreement Number(s)',
    service_delivery_model_cd                     STRING             COMMENT 'Foreign Key to table AASBS.LU_SERVICE_DELIVERY_MODEL',
    commerciality_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_commerciality',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains high-level award details for all awards within the ASSIST application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_fin_log
-- Source : aasbs.award_fin_log
-- Comment: Award-level Financial Identification Number values, including all legacy ACT Numbers
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_fin_log
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_FIN_LOG_SEQ',
    fin                                           STRING             COMMENT 'Alphanumeric Financial Identification Number (FIN) - (up to) 8-digit base 36 value (optional A prefix). Unique system-wide.',
    fin_base_10                                   BIGINT             COMMENT 'Base 10 value of the (up to) 8-character base 36 FIN (without the optional A prefix)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Award-level Financial Identification Number values, including all legacy ACT Numbers'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_fin_log',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_fin_log',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_ia
-- Source : aasbs.award_ia
-- Comment: Join table associating Awards with Interagency Agreements
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_ia
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_IA_SEQ',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table associating Awards with Interagency Agreements'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_ia',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_ia',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_mod
-- Source : aasbs.award_mod
-- Comment: Contains high-level Award Mod details for all Award Mod within the ASSIST application
-- Columns: 48 source + 6 audit = 54 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_mod
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_SEQ',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    mod_num                                       STRING             COMMENT 'Award Mod Number',
    award_piid_ext                                STRING             COMMENT 'Award Mod Number / Procurement Instrument Identifier Extention (PIID Extension)',
    award_mod_status_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_MOD_STATUS',
    fpds_car_status_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_FPDS_CAR_STATUS',
    mod_reviews_required_yn                       STRING             COMMENT 'Flag indicating if additional Award Mod reviews are required (=Y) or not (=N)',
    m1p_award_mod_fpds_reason_cd                  STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_MOD_FPDS_REASON - not applicable to Mod 00000',
    m1p_sf30_mod_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SF30_MOD_TYPE - not applicable to Mod 00000',
    m1p_expedited_type_cd                         STRING             COMMENT 'Foreign Key to table AASBS.LU_EXPEDITED_TYPE - not applicable to Mod 00000',
    m1p_processing_speed_cd                       STRING             COMMENT 'Foreign Key to table AASBS.LU_PROCESSING_SPEED - not applicable to Mod 00000',
    m1p_mod_start_dt                              TIMESTAMP             COMMENT 'Award Mod Start Date - not applicable to Mod 00000',
    m1p_required_award_dt                         TIMESTAMP             COMMENT 'Award Mod Required Award Date - not applicable to Mod 00000',
    m1p_rationale                                 STRING             COMMENT 'Award Mod Rationale - not applicable to Mod 00000',
    m1p_award_mod_authority_text                  STRING             COMMENT 'Award Mod Authority - not applicable to Mod 00000',
    award_mod_title                               STRING             COMMENT 'User-entered Award Mod Title',
    award_form_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_FORM',
    award_form_version                            STRING             COMMENT 'Version of the Award Form that was en vogue at the time of Award Signing',
    budget_fy                                     INT             COMMENT 'Award Mod Budget Fiscal Year',
    at_award_cost_recovery_cd                     STRING             COMMENT 'Foreign Key to table AASBS.LU_COST_RECOVERY',
    post_award_cost_recovery_cd                   STRING             COMMENT 'Foreign Key to table AASBS.LU_COST_RECOVERY',
    contractor_signature_reqd_yn                  STRING             COMMENT 'Flag indicating whether a Contractor Signature is required (=Y) or not (=N)',
    net_days_cnt                                  BIGINT             COMMENT 'Number of days defining the discount period',
    discount_days_cnt                             BIGINT             COMMENT 'Number of days to achieve the discount percentage (DISCOUNT_PCT column)',
    discount_pct                                  DECIMAL(6,3)             COMMENT 'Discount percentage if paid within specified number of days (DISCOUNT_DAYS_CNT column)',
    award_mod_description                         STRING             COMMENT 'User-entered Award Mod Description',
    explanation_if_no_fee                         STRING             COMMENT 'If NO_FEE is selected, explanation is required',
    award_doc_text_large_data_id                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LARGE_DATA',
    co_signature_user_id                          STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    co_signature_dt                               TIMESTAMP             COMMENT 'Date the Award Mod was signed by Contracting Officer',
    contractor_signature_user_id                  STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    contractor_signature_dt                       TIMESTAMP             COMMENT 'Date the Award Mod was signed by Contractor',
    draft_mod_line_items_yn                       STRING             COMMENT 'Flag indicating if the Line Items under this Award Mod are currently in Draft state (=Y) or not (=N)',
    po_transmitted_yn                             STRING             COMMENT 'Flag indicating whether the Award Mod has been packaged in a flat file and transmitted',
    li_migrated_yn                                STRING             COMMENT 'Flag indicating if a Line Item record was Migrated from Legacy Assist (=Y) or not (=N)',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    is_assist2_migration_yn                       STRING             COMMENT 'Flag indicating whether an Award Mod was (=Y) migrated or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    m1p_fpds_mod_num                              STRING             COMMENT 'Mod Number value to be sent to FPDS-NG for pre-uPIID orders migrated from legacy.',
    notes_to_contracting_large_data_id            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LARGE_DATA',
    negotiated_award_dt                           TIMESTAMP             COMMENT 'The Negotiated Award Dt is initially populated from the associated Acquisition Planning form',
    apex_emp_org_id                               BIGINT             COMMENT '[FK] Apex office associated to this award mod',
    version                                       BIGINT             COMMENT 'This is the version attribute which is used by the app to detect concurrent update actions.',
    esign_yn                                      STRING             COMMENT 'Flag indicating whether the user has selected the eSign method (=Y) or not (=N)',
    esign_document_id                             BIGINT             COMMENT '[FK] ID of the related record in AASBS_ESIGN_DOCUMENT.DOCUMENT and created index idx_aasbs_award_mod_esign_document_id',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains high-level Award Mod details for all Award Mod within the ASSIST application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_mod',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_mod',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_mod_address
-- Source : aasbs.award_mod_address
-- Comment: Join table associating Award Mods with zero or more Addresses
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_mod_address
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_ADDRESS_SEQ',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    address_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    address_type_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_ADDRESS_TYPE',
    multi_address_large_data_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LARGE_DATA',
    same_as_client_address_yn                     STRING             COMMENT 'Flag indicating whether the address is the same as the client address (=Y) or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table associating Award Mods with zero or more Addresses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_mod_address',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_mod_address',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_mod_company
-- Source : aasbs.award_mod_company
-- Comment: Contains Awarded Company and Remittance Company info
-- Columns: 21 source + 6 audit = 27 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_mod_company
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_COMPANY_SEQ',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    solicit_company_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_COMPANY',
    solicit_company_name                          STRING             COMMENT 'The name of the Awarded Company',
    contract_id                                   STRING             COMMENT '[FK] Foreign Key to table TABLE_MASTER.CONTRACTS',
    remittance_company_name                       STRING             COMMENT 'Name of the Remittance Company',
    contractor_classification_cd                  STRING             COMMENT 'Foreign Key to table AASBS.LU_CONTRACTOR_CLASSIFICATION',
    business_type_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_BUSINESS_TYPE',
    ipartner_id                                   STRING             COMMENT '[FK] Foreign Key to table TABLE_MASTER.INDUSTRY_PARTNERS',
    contractor_tax_id                             STRING             COMMENT '[FK] Contractor Tax ID',
    fob_type_cd                                   STRING             COMMENT 'Foreign Key to table AASBS.LU_FOB_TYPE',
    company_duns                                  STRING             COMMENT 'The Awarded Company\'s DUNS Number',
    company_duns_plus4                            STRING             COMMENT 'DUNS number assigned to a Client',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    company_uei                                   STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    company_uei_plus4                             STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    company_vuei                                  STRING             COMMENT 'Generated column:  show COMPANY_UEI if present, otherwise show COMPANY_DUNS',
    company_vuei_plus4                            STRING             COMMENT 'Generated column:  show COMPANY_UEI_PLUS4 if present, otherwise show COMPANY_DUNS_PLUS4',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Awarded Company and Remittance Company info'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_mod_company',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_mod_company',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_mod_responsible
-- Source : aasbs.award_mod_responsible
-- Comment: Join table associating Award Mods with zero or more Responsible People
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_mod_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_RESPONSIBLE_SEQ',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - the responsible user',
    responsible_role_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with an Award is the primary for their role (=Y)  or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table associating Award Mods with zero or more Responsible People'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_mod_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_mod_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_mod_type
-- Source : aasbs.award_mod_type
-- Comment: Associates an Award Mod with the "types" (i.e., reasons) for an Award Mod
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_mod_type
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_TYPE_SEQ',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    award_mod_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_MOD_TYPE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Associates an Award Mod with the "types" (i.e., reasons) for an Award Mod'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_mod_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_mod_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_referenced_solicit
-- Source : aasbs.award_referenced_solicit
-- Comment: Solictations that are referenced from an Award
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_referenced_solicit
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_REFERENCED_SOLICIT_SEQ',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    referenced_solicit_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Solictations that are referenced from an Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_referenced_solicit',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_referenced_solicit',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_responsible
-- Source : aasbs.award_responsible
-- Comment: Contains the always-current list of contacts for an Award
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.award_responsible_SEQ',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - the responsible user',
    responsible_role_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with an Award is the primary for their role (=Y)  or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the always-current list of contacts for an Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_servicing_aac
-- Source : aasbs.award_servicing_aac
-- Comment: Contains Servicing AACs for the Award
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_servicing_aac
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_SERVICING_AAC_SEQ',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Servicing AACs for the Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_servicing_aac',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_servicing_aac',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_award_validation_override
-- Source : aasbs.award_validation_override
-- Comment: Contains the always-current list of contacts for an Award
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_award_validation_override
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_VALIDATION_OVERRIDE_SEQ',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    award_validation_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_AWARD_VALIDATION',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the always-current list of contacts for an Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'award_validation_override',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_award_validation_override',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_bill_correction
-- Source : aasbs.bill_correction
-- Comment: For a correction to a BILL_ITEM entry, capture user entered values
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_bill_correction
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.BILL_CORRECTION_SEQ',
    financial_services_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FINANCIAL_SERVICES',
    cost_amt                                      DECIMAL(10,2)             COMMENT 'Correcting amount related to cost/deliverable billings',
    service_charge_amt                            DECIMAL(10,2)             COMMENT 'Correcting amount related to service charge billings',
    to_loa_tracking_num                           STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it\'s Tracking_Num',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'For a correction to a BILL_ITEM entry, capture user entered values'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'bill_correction',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_bill_correction',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_bill_item
-- Source : aasbs.bill_item
-- Comment: Contains the individual billed amounts against LOA_Ledger records
-- Columns: 22 source + 6 audit = 28 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_bill_item
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.BILL_ITEM_SEQ',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it\'s Tracking_Num',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    actual_bill_dt                                TIMESTAMP             COMMENT 'Date a Bill was succesfully submitted to PEGASYS',
    eligible_for_transmit_yn                      STRING             COMMENT 'Indicator that determines whether a BILL_ITEM record should be transmitted to Pegasys (=Y) or not (=N)',
    bill_item_amt                                 DECIMAL(15,2)             COMMENT 'Amount billed against LOA_Ledger "Line_Item_Obligated_Amt" or "Service_Charge_Amt" column',
    bill_item_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_BILL_ITEM_TYPE',
    funding_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
    billing_id                                    BIGINT             COMMENT '[FK] Foreign Key to table BILLING.BILLING',
    bill_item_source_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_BILL_ITEM_SOURCE',
    src_service_charge_schedule_id                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SERVICE_CHARGE_SCHEDULE',
    src_tkeeping_monthly_hours_id                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TIMEKEEPING.MONTHLY_HOURS',
    src_financial_services_id                     BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FINANCIAL_SERVICES',
    src_preassist_order_loa_map_id                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.PREASSIST_ORDER_LOA_MAP',
    src_cost_acceptance_dist_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE_DIST',
    src_surchg_acceptance_dist_id                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE_DIST',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    src_travel_voucher_id                         BIGINT             COMMENT '[FK]',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the individual billed amounts against LOA_Ledger records'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'bill_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_bill_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_bill_item_correction
-- Source : aasbs.bill_item_correction
-- Comment: Captures AASBS.BILL_ITEM records credited by a Financial Services transaction.  Child table to AASBS
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_bill_item_correction
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.BILL_ITEM_CORRECTION_SEQ',
    bill_correction_id                            BIGINT             COMMENT '[FK] Foreign Key to AASBS.BILL_CORRECTION - Link to parent table.',
    bill_item_id                                  BIGINT             COMMENT '[FK] Foreign Key to AASBS.BILL_ITEM - Link to record being corrected.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    tracking_item_id                              BIGINT             COMMENT '[FK] Foreign Key to AASBS.TRACKING_ITEM - Link to tracking record being corrected.',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Captures AASBS.BILL_ITEM records credited by a Financial Services transaction.  Child table to AASBS.BILL_CORRECTION.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'bill_item_correction',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_bill_item_correction',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_collab
-- Source : aasbs.central_collab
-- Comment: Central Collaboration details shared by all tables using Central Collab system
-- Columns: 17 source + 6 audit = 23 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_collab
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_SEQ',
    collab_type_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_COLLAB_TYPE',
    collab_status_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_COLLAB_STATUS',
    collab_num                                    BIGINT             COMMENT 'The Collaboration numbering 0001,0002,0003,..n - sequence repeats for each distinct PROCUREMENT_ID in this table',
    due_dt                                        TIMESTAMP             COMMENT 'Collaboration Due Date',
    subject                                       STRING             COMMENT 'User-entered Subject of the Collaboration',
    contractor_access_yn                          STRING             COMMENT 'Flag indicating whether Contractor can (=Y) or cannot (=N) access this Collaboration',
    client_access_yn                              STRING             COMMENT 'Flag indicating whether Client can (=Y) or cannot (=N) access this Collaboration',
    attach_form_name                              STRING             COMMENT 'Form Name attached to the Collaboration',
    table_name_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_TABLE_NAME',
    table_record_id                               BIGINT             COMMENT '[FK] Logical foreign key from the table in CENTRAL_COLLAB.TABLE_NAME_CD column (enforced via triggers)',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    submitted_by_name                             STRING             COMMENT 'Free-text Full Name of the person who created the collaboration',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Central Collaboration details shared by all tables using Central Collab system'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_collab',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_collab',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_collab_acquisition
-- Source : aasbs.central_collab_acquisition
-- Comment: This table connects collaborations to related acquisitions
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_collab_acquisition
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_ACQUISITION_SEQ',
    central_collab_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.CENTRAL_COLLAB',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    funds_usage_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_FUNDS_USAGE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'This table connects collaborations to related acquisitions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_collab_acquisition',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_collab_acquisition',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_collab_ia
-- Source : aasbs.central_collab_ia
-- Comment: This table contains additional data captured for an IA Fund Request Collaboration
-- Columns: 13 source + 6 audit = 19 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_collab_ia
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_IA_SEQ',
    central_collab_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.CENTRAL_COLLAB.ID',
    fund_return_yn                                STRING             COMMENT 'Is this request returning funds?',
    funding_num                                   STRING             COMMENT 'Client funding document number',
    amendment_num                                 STRING             COMMENT 'Client funding document amendment number',
    acquisition                                   STRING             COMMENT 'Supporting Acquisition',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    funds_usage_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_FUNDS_USAGE',
    service_charge_info                           STRING             COMMENT 'This column is not used - always has one value - Service Charge',
    modified_by_fm_user_id                        STRING             COMMENT '[FK] Foreign key to ASSIST Users userid',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'This table contains additional data captured for an IA Fund Request Collaboration'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_collab_ia',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_collab_ia',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_collab_post_award
-- Source : aasbs.central_collab_post_award
-- Comment: Central Collaboration details specific to only Post Award
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_collab_post_award
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_POST_AWARD_SEQ',
    central_collab_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.CENTRAL_COLLAB',
    resume_emp_num                                STRING             COMMENT 'Hold Employee Number only for Collab Type = RESUME',
    resume_emp_name                               STRING             COMMENT 'Employee Name only for Collab Type = RESUME',
    funds_intake_fund_doc_num                     STRING             COMMENT 'Contains the related Funding Document Number for FUNDS_INTAKE Collab Types',
    funds_intake_amend_num                        INT             COMMENT 'Contains the related Funding Package Amendment Number for FUNDS_INTAKE Collab Types',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Central Collaboration details specific to only Post Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_collab_post_award',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_collab_post_award',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_collab_responsible
-- Source : aasbs.central_collab_responsible
-- Comment: This table connects collaborating people to the forms on which they are working together
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_collab_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key for this table.  Filled with sequence:  CENTRAL_COLLAB_RESPONSIBLE_SEQ',
    central_collab_id                             BIGINT             COMMENT '[FK] Foreign Key to AASBS.CENTRAL_COLLAB table',
    responsible_user_id                           STRING             COMMENT '[FK] Collaborating User - Foreign key to ASSIST.USERS(ID)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_id                            STRING             COMMENT '[FK] Record last updated by this user - Foreign key to ASSIST.USERS(ID)',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'This table connects collaborating people to the forms on which they are working together'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_collab_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_collab_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_central_comment
-- Source : aasbs.central_comment
-- Comment: Central Comment table that is used by all other tables that have comments associated with their reco
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_central_comment
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COMMENT_SEQ',
    table_name_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_TABLE_NAME',
    table_record_id                               BIGINT             COMMENT '[FK] Logically foreign key from the table in CENTRAL_COMMENT.TABLE_NAME_CD column (enforced via triggers)',
    comment_type_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_COMMENT_TYPE',
    comment_text                                  STRING             COMMENT 'Holds actual text of a comment',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Central Comment table that is used by all other tables that have comments associated with their records'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'central_comment',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_central_comment',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_chronology
-- Source : aasbs.chronology
-- Comment: Events captured at key points in each Order\'s lifecycle
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_chronology
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.CHRONOLOGY_SEQ',
    source_table_name_cd                          STRING             COMMENT 'Name of table containing the record that acted as the cause of this Chronology Event',
    source_record_id                              STRING             COMMENT '[FK] ID of the record that acted as the cause of this Chronology Event',
    event_table_name_cd                           STRING             COMMENT 'Name of the table in which a record was created as a result of this Chronology Event',
    event_record_id                               STRING             COMMENT '[FK] ID of the record created as a result of this Chronology Event',
    event_type_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_EVENT_TYPE',
    entry_description                             STRING             COMMENT 'Description of Chronology Event',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Events captured at key points in each Order\\''s lifecycle'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'chronology',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_chronology',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_email_log
-- Source : aasbs.email_log
-- Comment: Contains details of emails generated in ASSIST
-- Columns: 13 source + 6 audit = 19 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_email_log
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.EMAIL_LOG_SEQ',
    email_status_cd                               STRING             COMMENT 'Foreign Key to table AASBS.LU_EMAIL_STATUS',
    from_email                                    STRING             COMMENT 'Contains the email address of the email sender',
    subject                                       STRING             COMMENT 'Email Subject',
    body                                          STRING             COMMENT 'Body of the email',
    attachments_yn                                STRING             COMMENT 'Flag indicating whether an email had attachments (=Y) or not (=N)',
    body_large_data_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LU_BODY_LARGE_DATA only used for emails with BODY text length > 4000 chars',
    sent_dt                                       TIMESTAMP             COMMENT 'Date email was sent to server',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    retry_count                                   INT             COMMENT 'The number of times the email entry has attempted to be sent.',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains details of emails generated in ASSIST'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'email_log',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_email_log',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_financial_services
-- Source : aasbs.financial_services
-- Comment: Captures user entered transaction details for a financial correction
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_financial_services
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.FINANCIAL_SERVICES_SEQ',
    financial_services_action_cd                  STRING             COMMENT 'Foreign Key to table AASBS.LU_FINANCIAL_SERVICES_ACTION',
    transmit_yn                                   STRING             COMMENT 'Flag indicating whether transaction should be transmitted on to Pegasys (via AASBS_TRANSMIT tables)',
    comments                                      STRING             COMMENT 'Comments for this transaction',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    labor_correction_action_cd                    STRING             COMMENT 'Foreign Key to table AASBS.LU_LABOR_CORRECTION_ACTION',
    approved_by_id                                STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - BM/SM who has authorized the Labor Correction action.',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Captures user entered transaction details for a financial correction'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'financial_services',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_financial_services',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_funding
-- Source : aasbs.funding
-- Comment: Funding Package details - acts as the container for the LOA Amendments and LOAs
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_funding
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_SEQ',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
    fund_status_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_STATUS',
    funding_num                                   STRING             COMMENT 'Client funding document number',
    fund_category_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_CATEGORY',
    billing_type_cd                               STRING             COMMENT 'Billing method (e.g., Standard, Advanced, Pre-Paid, etc)',
    latest_funding_amendment_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING_AMENDMENT to the last amendment or adjustment submitted - column is regularly updated',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Funding Package details - acts as the container for the LOA Amendments and LOAs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'funding',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_funding',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_funding_amendment
-- Source : aasbs.funding_amendment
-- Comment: Funding Amendment details
-- Columns: 18 source + 6 audit = 24 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_funding_amendment
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_AMENDMENT_SEQ',
    funding_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING',
    amend_num                                     INT             COMMENT 'Funding Amendment Number',
    adjustment_num                                INT             COMMENT 'Funding Adjustment Number',
    fund_amend_action_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_AMEND_ACTION',
    fund_amend_status_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_AMEND_STATUS',
    action_description                            STRING             COMMENT 'User-entered Action Description of the event causing the amendment/adjustment',
    requirement_description                       STRING             COMMENT 'User-entered Requirement Description of the event causing the amendment/adjustment',
    customer_signature_dt                         TIMESTAMP             COMMENT 'Date/time the Funding Amendment was received',
    acceptance_dt                                 TIMESTAMP             COMMENT 'Acceptance date of the Funding Amendment',
    refd_prev_funding_amendment_id                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING_AMENDMENT',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    ginv_pop_start_dt                             TIMESTAMP             COMMENT 'Period of Performance start date sourced from GInvoice',
    ginv_pop_end_dt                               TIMESTAMP             COMMENT 'Period of Performance end date from GInvoice',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Funding Amendment details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'funding_amendment',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_funding_amendment',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_funding_amendment_loa
-- Source : aasbs.funding_amendment_loa
-- Comment: Join table allowing many to many relationship between the Funding_Amendment and LOA tables
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_funding_amendment_loa
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_AMENDMENT_LOA_SEQ',
    funding_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING_AMENDMENT',
    loa_id                                        BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LOA',
    loa_change_amt                                DECIMAL(15,2)             COMMENT 'The dollar amount of change to the referenced AASBS.LOA record',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table allowing many to many relationship between the Funding_Amendment and LOA tables'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'funding_amendment_loa',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_funding_amendment_loa',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_funding_responsible
-- Source : aasbs.funding_responsible
-- Comment: Contains list of personnel associated with a Funding Package
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_funding_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_RESPONSIBLE_SEQ',
    funding_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    role_cd                                       STRING             COMMENT 'Foreign Key to table AASBS.LU_ROLE',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with a Funding Package is the primary for their role (=Y)  or not (=N)',
    full_name                                     STRING             COMMENT 'Free-text Full Name of Responsible User',
    email                                         STRING             COMMENT 'Email of the Responsible person',
    phone                                         STRING             COMMENT 'Phone Number',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains list of personnel associated with a Funding Package'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'funding_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_funding_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_ia
-- Source : aasbs.ia
-- Comment: Interagency Agreements
-- Columns: 25 source + 6 audit = 31 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_ia
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.IA_SEQ',
    piid                                          STRING             COMMENT 'IA procurement instrument identifier',
    ia_status_cd                                  STRING             COMMENT 'IA Status - Foreign key to AASBS.LU_IA_STATUS.CD',
    activity_address_cd                           STRING             COMMENT 'Lead Servicing IA - Foreign key to AASBS.LU_ACTIVITY_ADDRESS_CODE.CD',
    fund_cd                                       STRING             COMMENT 'Fund - Foreign key to AASBS.LU_FUND.CD',
    instrument_type_cd                            STRING             COMMENT 'IA Instrument Type - Foreign key to AASBS.LU_INSTRUMENT_TYPE.CD',
    title                                         STRING             COMMENT 'Interagency Agreement Title',
    description                                   STRING             COMMENT 'Description of the Interagecy Agreement',
    section_emp_org_id                            BIGINT             COMMENT '[FK] This column is now used for all EMP ORGs, not just SECTIONs.  Foreign key to ASSIST.LU_EMP_ORGS.EMP_ORG_ID',
    fiscal_year                                   INT             COMMENT 'Fiscal Year of IA',
    ia_began_dt                                   TIMESTAMP             COMMENT 'Date IA process began',
    serv_agency_cd                                STRING             COMMENT 'Servicing Agency - Foreign key to AASBS.LU_AGENCY.CD',
    serv_bureau_cd                                STRING             COMMENT 'Servicing Bureau - Foreign key to AASBS.LU_BUREAU.CD and .AGENCY_CD',
    piid_req_agency                               STRING             COMMENT 'Requesting Agency IA Number',
    client_id                                     STRING             COMMENT '[FK] Requesting Agency Client Organization - Foreign Key to table TABLE_MASTER.CLIENTS.CLIENT_ID',
    legacy_ia_num                                 STRING             COMMENT 'Legacy IA number.  Either migrated from OMIS.BAS.BA_ID or imported by RBA data call.',
    omis_bas_seq_no                               BIGINT             COMMENT 'For IAs which also exist in OMIS, foreign key to OMIS.BAS.SEQ_NO',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    latest_ia_amendment_id                        BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LATEST_IA_AMENDMENT',
    gtc_num                                       STRING             COMMENT 'General Terms and Conditions GTC Number From G-INVOICING System',
    svcg_agmt_trkg_num                            STRING             COMMENT 'Servicing Agreement Tracking Number',
    emp_org_id                                    BIGINT             COMMENT '[FK] References ASSIST.LU_EMP_ORGS.EMP_ORG_ID',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Interagency Agreements'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'ia',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_ia',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_ia_amendment
-- Source : aasbs.ia_amendment
-- Comment: Contains Amendments for an Interagency Agreement
-- Columns: 24 source + 6 audit = 30 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_ia_amendment
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.IA_AMENDMENT_SEQ',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA.ID',
    piid_ext                                      STRING             COMMENT 'Amendment Number/PIID extension',
    ia_amend_action_cd                            STRING             COMMENT 'Amendment action - Foreign key to AASBS.LU_IA_AMEND_ACTION.CD',
    action_description                            STRING             COMMENT 'Description of the Amendment action',
    start_dt                                      TIMESTAMP             COMMENT 'Start Date for IA',
    end_dt                                        TIMESTAMP             COMMENT 'End Date for IA',
    direct_cost_est_amt                           DECIMAL(23,2)             COMMENT 'Estimated Direct Costs',
    charges_est_amt                               DECIMAL(23,2)             COMMENT 'Estimated Charges',
    request_agency_scope                          STRING             COMMENT 'Requesting Agency scope of work to be performed by the Servicing Agency',
    service_agency_signer_id                      STRING             COMMENT '[FK] Servicing Agency Signer - Foreign Key to table AASBS.USER.ID',
    service_agency_signed_dt                      TIMESTAMP             COMMENT 'Servicing Agency Date Signed',
    request_agency_signer                         STRING             COMMENT 'Requesting Agency Signer name',
    request_agency_signer_email                   STRING             COMMENT 'Requesting Agency Signer email',
    request_agency_signer_phone                   STRING             COMMENT 'Requesting Agency Signer phone number',
    request_agency_signed_dt                      TIMESTAMP             COMMENT 'Requesting Agency Date Signed',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    gtc_service_agency_signer                     STRING             COMMENT 'GTC Servicing Final Approval Signed Name From G-INVOICING System',
    gtc_service_agency_signer_email               STRING             COMMENT 'GTC Servicing Final Approval Signed Email From G-INVOICING System',
    gtc_service_agency_signer_phone               STRING             COMMENT 'GTC Servicing Final Approval Signed Phone From G-INVOICING System',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Amendments for an Interagency Agreement'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'ia_amendment',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_ia_amendment',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_ia_responsible
-- Source : aasbs.ia_responsible
-- Comment: Contains list of personnel associated with an Interagency Agreement
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_ia_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.IA_RESPONSIBLE_SEQ',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA.ID',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table AASBS.USER.ID',
    responsible_role_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE.CD',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with the IA is the primary for their role (=Y) or alternate (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains list of personnel associated with an Interagency Agreement'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'ia_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_ia_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_ia_review
-- Source : aasbs.ia_review
-- Comment: Contains annual client reviews for an Interagency Agreement
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_ia_review
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.IA_REVIEW_SEQ',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA.ID',
    review_num                                    STRING             COMMENT 'Review Number',
    review_dt                                     TIMESTAMP             COMMENT 'Review Date for IA',
    service_agency_reviewer_id                    STRING             COMMENT '[FK] Servicing Agency Reviewer - Foreign Key to table AASBS.USER.ID',
    request_agency_reviewer                       STRING             COMMENT 'Requesting Agency Reviewer name',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains annual client reviews for an Interagency Agreement'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'ia_review',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_ia_review',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_invoice
-- Source : aasbs.invoice
-- Comment: Contains high-level invoice details for all invoices within the ASSIST application
-- Columns: 20 source + 6 audit = 26 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_invoice
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_SEQ',
    invoice_number                                STRING             COMMENT 'Invoice Number',
    award_id                                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    award_mod_company_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD_COMPANY',
    invoice_status_cd                             STRING             COMMENT 'Foreign key to AASBS.LU_INVOICE_STATUS',
    total_invoice_amt                             DECIMAL(15,2)             COMMENT 'Combined total amount of all Invoice_Items under this Invoice',
    invoice_dt                                    TIMESTAMP             COMMENT 'Date of Invoice submission',
    invoice_processed_dt                          TIMESTAMP             COMMENT 'Date the Invoice was processed',
    invoice_net_days_cnt                          DECIMAL(19,2)             COMMENT 'Contractor-specified discount Net-Days',
    invoice_discount_pct                          DECIMAL(6,3)             COMMENT 'Contractor-specified discount percentage if paid within specified number of days (INVOICE_DISCOUNT_DAYS_CNT column)',
    invoice_discount_days_cnt                     DECIMAL(19,2)             COMMENT 'Contractor-specified number of days to achieve the discount percentage (INVOICE_DISCOUNT_PCT column)',
    invoice_contractor_comments                   STRING             COMMENT 'Contractor-entered comments',
    vitap_payment_status_cd                       STRING             COMMENT 'Foreign Key to table AASBS.LU_VITAP_PAYMENT_STATUS',
    vitap_processed_dt                            TIMESTAMP             COMMENT 'Date the VITAP payment was processed',
    vitap_payment_amt                             DECIMAL(15,2)             COMMENT 'Amount of the VITAP payment',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains high-level invoice details for all invoices within the ASSIST application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'invoice',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_invoice',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_invoice_connector
-- Source : aasbs.invoice_connector
-- Comment: For an Invoice corrected via Financial Services, this table links the correcting Invoice with the or
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_invoice_connector
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_CONNECTOR_SEQ',
    original_invoice_id                           BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE',
    correction_invoice_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE',
    invoice_correction_type_cd                    STRING             COMMENT 'Foreign Key to table AASBS.LU_INVOICE_CORRECTION_TYPE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'For an Invoice corrected via Financial Services, this table links the correcting Invoice with the original Invoice'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'invoice_connector',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_invoice_connector',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_invoice_item
-- Source : aasbs.invoice_item
-- Comment: Contains the one or more Line Items under a single Invoice
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_invoice_item
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_ITEM_SEQ',
    invoice_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE',
    cost_line_item_accepted_id                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    invoice_item_begin_dt                         TIMESTAMP             COMMENT 'Invoiced Item "Begin Date"',
    invoice_item_end_dt                           TIMESTAMP             COMMENT 'Invoiced Item "End/Ship Date"',
    invoice_item_amt                              DECIMAL(15,2)             COMMENT 'Amount billed against LOA_Ledger "Line_Item_Obligated_Amt" or "Service_Charge_Amt" column',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the one or more Line Items under a single Invoice'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'invoice_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_invoice_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_li_award_fee
-- Source : aasbs.li_award_fee
-- Comment: Line Item extension table for Travel and Travel Tracking Details
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_li_award_fee
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LI_AWARD_FEE_SEQ',
    surcharge_rate_pct                            DECIMAL(19,16)             COMMENT 'Percentage of Award to be charged for this Award Fee',
    fee_amt                                       DECIMAL(15,2)             COMMENT 'Fixed Amount for this Award Fee',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    fee_adj                                       DECIMAL(15,2)             COMMENT 'Fee adjustment amount used by business line to correct rounding discrepancies from fee surcharge line item calculation',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Item extension table for Travel and Travel Tracking Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'li_award_fee',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_li_award_fee',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_li_deliverable
-- Source : aasbs.li_deliverable
-- Comment: Deliverables - an extension of the LINE_ITEM table for records of LI_DELIVERABLE line item type
-- Columns: 16 source + 6 audit = 22 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_li_deliverable
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.DELIVERABLE_SEQ',
    psc_cd                                        STRING             COMMENT 'Foreign Key to table AASBS.LUS_PSC',
    contract_type_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_CONTRACT_TYPE',
    acquisition_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACQUISITION_TYPE',
    severability_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE',
    unit_of_measure_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_UNIT_OF_MEASURE',
    li_pop_address_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    li_accepting_address_id                       BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    required_yn                                   STRING             COMMENT 'Flag indicating whether the Delivarble Line Item is required (=Y) or not (=N)',
    bona_fide_need_fy                             INT             COMMENT 'Fiscal year of the bona fide need',
    draft_li_frm_yn                               STRING             COMMENT 'Flag indicating whether Line Item Entry form is (=Y) or is not (=N) in DRAFT state',
    is_migrated_single_clin_yn                    STRING             COMMENT 'Indicates if this is was migrated as part of a single CLIN award (=Y) or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Deliverables - an extension of the LINE_ITEM table for records of LI_DELIVERABLE line item type'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'li_deliverable',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_li_deliverable',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_li_sc_fixed_fee
-- Source : aasbs.li_sc_fixed_fee
-- Comment: Line Item extension table for Fixed Fee Details
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_li_sc_fixed_fee
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_FIXED_FEE_SEQ',
    manager_user_id                               STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Manager of the Fixed Fee Line Item',
    proc_phase_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_PROC_PHASE',
    project_num                                   STRING             COMMENT 'Account or PEP number, required for FEDSIM IAs',
    planned_amt                                   DECIMAL(15,2)             COMMENT 'Planned dollar amount for the Fixed Fee Line Item',
    severability_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Item extension table for Fixed Fee Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'li_sc_fixed_fee',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_li_sc_fixed_fee',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_li_sc_labor
-- Source : aasbs.li_sc_labor
-- Comment: Line Item extension table for Labor and Labor Tracking Details
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_li_sc_labor
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_LABOR_SEQ',
    manager_user_id                               STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Manager of the Labor Service Charge Line Item',
    proc_phase_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_PROC_PHASE',
    project_num                                   STRING             COMMENT 'User chosen Project Number',
    planned_amt                                   DECIMAL(15,2)             COMMENT 'The planned funding amount for this Labor Service Charge',
    cost_rate_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_COST_RATE_TYPE',
    severability_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE. (Always NA for Labor and Labor Tracking service charges.)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Item extension table for Labor and Labor Tracking Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'li_sc_labor',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_li_sc_labor',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_li_sc_travel
-- Source : aasbs.li_sc_travel
-- Comment: Line Item extension table for Travel and Travel Tracking Details
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_li_sc_travel
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_TRAVEL_SEQ',
    manager_user_id                               STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Manager of the Fixed Fee Line Item',
    proc_phase_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_PROC_PHASE',
    project_num                                   STRING             COMMENT 'Account or PEP number, required for FEDSIM IAs',
    planned_amt                                   DECIMAL(15,2)             COMMENT 'Planned dollar amount for the Fixed Fee Line Item',
    severability_type_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE. (Always NA for Travel Tracking service charges.)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Item extension table for Travel and Travel Tracking Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'li_sc_travel',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_li_sc_travel',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_line_item
-- Source : aasbs.line_item
-- Comment: Line Items main table - uses extension tables Service_Charge, etc.
-- Columns: 35 source + 6 audit = 41 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_line_item
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_SEQ',
    line_item_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_LINE_ITEM_TYPE',
    line_item_num                                 STRING             COMMENT 'Line Item Number uniquely identifies a Line Item within a Solicitation / Award',
    quantity                                      DECIMAL(10,2)             COMMENT 'Quantity of items (units) on this Line Item',
    line_item_unit_amt                            DECIMAL(15,2)             COMMENT 'Dollar amount per item (unit) on this Line Item',
    line_item_total_amt                           DECIMAL(15,2)             COMMENT 'Total dollar amount for this Line Item = quantity x item (unit) amount',
    line_item_start_dt                            TIMESTAMP,
    line_item_end_dt                              TIMESTAMP,
    service_charge_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SERVICE_CHARGE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    li_tracking_num                               BIGINT             COMMENT 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"',
    title                                         STRING             COMMENT 'Title of Line Item',
    description                                   STRING             COMMENT 'Description of Line Item',
    line_item_status_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_LINE_ITEM_STATUS',
    project_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.PROJECT - The PROJECT to which this Line Item is associated',
    prev_line_item_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.PREV_LINE_ITEM',
    parent_ia_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA - The IA to which this Line Item is associated',
    parent_award_mod_id                           BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD - The Award Mod to which this Line Item is associated',
    parent_solicit_amendment_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_AMENDMENT - The Solicitation Amendment to which this Line Item is associated',
    exercised_1st_in_award_mod_id                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS.EXERCISED_1ST_IN_AWARD_MOD',
    is_base_deliverable_yn                        STRING             COMMENT 'Flag indicating whether the Line Item Deliverable was/is in the Base Mod (=Y) or not (=N)',
    line_item_fy                                  INT             COMMENT 'Line Item Fiscal Year',
    li_deliverable_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LI_DELIVERABLE',
    li_sc_fixed_fee_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LI_SC_FIXED_FEE',
    li_sc_labor_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LI_SC_LABOR',
    li_sc_travel_id                               BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LI_SC_TRAVEL',
    li_award_fee_id                               BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LI_AWARD_FEE',
    ceiling_change_amt                            DECIMAL(15,2)             COMMENT 'Ceiling Amount change since previous Mod',
    ceiling_prior_amt                             DECIMAL(15,2)             COMMENT 'Prior Maximum Funding Amount for this Line Item',
    planned_funded_change_amt                     DECIMAL(15,2)             COMMENT 'The planned funding amount change for this Line Item',
    planned_funded_prior_amt                      DECIMAL(15,2)             COMMENT 'The previous planned funding amount for the previous version of this Line Item',
    solicit_line_item_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS_DOCUMENTS.LINE_ITEM representing the origin of line item for those that started in Solictiation',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Items main table - uses extension tables Service_Charge, etc.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'line_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_line_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_line_item_accepted
-- Source : aasbs.line_item_accepted
-- Comment: Line Items ACCEPTED table - Line Items must be in this table to be accepted by an LOA in the LOA_LED
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_line_item_accepted
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_SEQ',
    line_item_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM',
    li_tracking_num                               BIGINT             COMMENT 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    sc_fin                                        STRING             COMMENT 'Financial Identification Number - for Service Charges Only (SC_FIN = Service Charge FIN) - see AWARD table for AWARD_FIN',
    budget_fy                                     INT             COMMENT 'Budget fiscal year',
    exercise_this_yn                              STRING             COMMENT 'Flag indicating whether the Line Item must be included in the Award and Funded (=Y) or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    commitment_note                               STRING             COMMENT 'User note created while "Committing" LOA funds to a Line Item',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Items ACCEPTED table - Line Items must be in this table to be accepted by an LOA in the LOA_LEDGER table.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'line_item_accepted',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_line_item_accepted',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_line_item_connector
-- Source : aasbs.line_item_connector
-- Comment: This table links Line Items to other Line Items.  E.g., Deliverable to it\'s related "% of cost" Ser
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_line_item_connector
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_CONNECTOR_SEQ',
    from_line_item_id                             BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM',
    to_line_item_id                               BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'This table links Line Items to other Line Items.  E.g., Deliverable to it\\''s related "% of cost" Service Charge'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'line_item_connector',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_line_item_connector',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_line_item_response
-- Source : aasbs.line_item_response
-- Comment: Line Items Responses from each Contractor to the Line Items in aasbs.LINE_ITEM created during a Soli
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_line_item_response
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_RESPONSE_SEQ',
    line_item_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM',
    solicit_response_id                           BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_RESPONSE',
    response_quantity                             DECIMAL(10,2)             COMMENT 'Quantity of items (units) on this Line Item',
    response_line_item_unit_amt                   DECIMAL(15,2)             COMMENT 'Dollar amount per item (unit) on this Line Item',
    response_line_item_total_amt                  DECIMAL(15,2)             COMMENT 'Total dollar amount for this Line Item = quantity x item (unit) amount',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line Items Responses from each Contractor to the Line Items in aasbs.LINE_ITEM created during a Solicitation Response'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'line_item_response',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_line_item_response',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_loa
-- Source : aasbs.loa
-- Comment: Line of Accounting details
-- Columns: 54 source + 6 audit = 60 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_loa
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LOA_SEQ',
    loa_status_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_LOA_STATUS',
    creator_funding_amendment_id                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING_AMENDMENT',
    agreement_number                              STRING             COMMENT 'Agreement Number on a Citation',
    agreement_end_dt                              TIMESTAMP             COMMENT 'Agreement date of the Line of Accounting (LOA)',
    transmitted_dt                                TIMESTAMP             COMMENT 'Date/time the LOA was transmitted',
    agreement_accepted_yn                         STRING             COMMENT 'Flag indicating whether the LOA Agreement is Accepted (=Y) or not (=N)',
    agreement_converted_yn                        STRING             COMMENT 'Flag indicating whether the LOA Agreement is Converted (=Y) or not (=N)',
    legacy_sync_status_cd                         STRING             COMMENT 'Foreign Key to table AASBS.LU_LEGACY_SYNC_STATUS',
    legacy_sync_dt                                TIMESTAMP             COMMENT 'Used during Convergence period in which ASSIST2.0 data is being synchronized into Legacy ASSIST tables',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it\'s Tracking_Num',
    line_of_accounting                            STRING             COMMENT 'Complex Line of Accounting String with CLINS, DUNS, etc',
    program_cd                                    STRING             COMMENT 'Foreign Key to table AASBS.LU_PROGRAM',
    funds_usage_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_FUNDS_USAGE',
    loa_unique_restrictions_yn                    STRING             COMMENT 'Flag indicating whether there are unique restrictions on this LOA (=Y) or not (=N)',
    fund_restriction                              STRING             COMMENT 'User-entered free-text Restrictions on LOA use',
    client_duns_number                            STRING             COMMENT 'Client DUNS. The requesting agency DUNS number',
    client_duns_plus4                             STRING             COMMENT 'DUNS number assigned to a Client',
    appropriation                                 STRING             COMMENT 'The order appropriation code',
    treasury_sub_account                          STRING             COMMENT 'Treasury Sub Account. The Treasury Sub-account identifier',
    tsym_alloc_trans_agency_cd                    STRING             COMMENT 'Foreign Key to table AASBS.LU_AGENCY',
    tsym_customer_sublevel_prefix                 STRING             COMMENT 'TSYM specific data',
    boac1                                         STRING             COMMENT 'The ordering agency BOAC code',
    boac2                                         STRING             COMMENT 'The billing agency BOAC code',
    agency_location_code                          STRING             COMMENT 'Identifying code of the paying office for treasury when an agency is using OPAC billing',
    requesting_agency_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_REQUESTING_AGENCY',
    loa_change_reason_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_LOA_CHANGE_REASON',
    loa_change_remarks                            STRING             COMMENT 'Reason why the LOA was modified',
    pop_start_dt                                  TIMESTAMP             COMMENT 'Period of Performance start date',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    pop_end_dt                                    TIMESTAMP             COMMENT 'Period of Performance end date',
    fund_type_cd                                  STRING             COMMENT 'Foreign Key to table AASBS.LU_FUND_TYPE',
    first_fy_available_year                       INT             COMMENT 'First Fiscal Year the LOA money will be available',
    last_fy_available_year                        INT             COMMENT 'Last Fiscal Year the LOA money will be available',
    tsym_availability_type                        STRING             COMMENT 'TSYM specific data',
    loa_expiration_dt                             TIMESTAMP             COMMENT 'Expiration date of the LOA',
    subject_to_availability_yn                    STRING             COMMENT 'Flag indicating whether funds are Subject to Availability for this LOA (=Y) or not (=N)',
    loa_dollar_amt                                DECIMAL(15,2)             COMMENT 'Total dollar amount remaining on the LOA (when it is the latest LOA record for a single Tracking_Num)',
    customer_act_num                              STRING             COMMENT 'IA Type S2 - Internal GSA Funding Document Number',
    customer_mdl                                  STRING             COMMENT 'The client\'s MDL',
    customer_fund                                 STRING             COMMENT 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.',
    fed_code                                      STRING             COMMENT 'The fed code associated with the funding citation',
    cost_element                                  STRING             COMMENT 'Client Cost Element',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    client_uei                                    STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    client_uei_plus4                              STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    sub_allocation                                STRING             COMMENT 'LOA embedded data that facilitates DoD monthly reconciliations',
    ginv_line_num                                 INT             COMMENT 'G-Invoicing Order Line Number (GINV_ORD_LNUM)',
    ginv_schedule_number                          INT             COMMENT 'G-Invoicing Order Schedule Number (GINV_ORD_SCHL_NUM)',
    bona_fide_need                                STRING             COMMENT 'Description of (bona fide) need for funding. If gInvoicing, then sourced from PROD_NEED_DSCR.',
    client_vuei                                   STRING,
    client_vuei_plus4                             STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Line of Accounting details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'loa',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_loa',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_loa_acquisition
-- Source : aasbs.loa_acquisition
-- Comment: Join table associating LOAs with Acquisitions
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_loa_acquisition
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LOA_ACQUISITION_SEQ',
    loa_id                                        BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LOA',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Join table associating LOAs with Acquisitions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'loa_acquisition',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_loa_acquisition',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_loa_ledger
-- Source : aasbs.loa_ledger
-- Comment: Table Description:  Ledger tracking all LOA related monitary transfers.  All amounts in this table a
-- Columns: 19 source + 6 audit = 25 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_loa_ledger
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LOA_LEDGER_SEQ',
    loa_id                                        BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LOA',
    transaction_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_TRANSACTION_TYPE',
    tracking_num                                  STRING             COMMENT 'The Line Of Accounting (LOA) Tracking Number - uniquely identifies a single LOA - sometimes across multple aasbs.LOA records',
    line_item_committed_amt                       DECIMAL(15,2)             COMMENT 'Committed amount for any type of Line Item - Deliverable or Service Charge',
    line_item_certified_amt                       DECIMAL(15,2)             COMMENT 'Certified amount for any type of Line Item - Deliverable or Service Charge',
    line_item_obligated_amt                       DECIMAL(15,2)             COMMENT 'Obligated amount for Deliverables Line Items.  Note that Service Charges do not go in this column - they go in Service_Charge_Amt',
    service_charge_amt                            DECIMAL(15,2)             COMMENT 'The Service Charge amount that can be billed',
    loa_funded_amt                                DECIMAL(15,2)             COMMENT 'The amounts added or subtracted from an LOA via Funding Amendments',
    funding_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING_AMENDMENT',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    draft_obligation_adjust_amt                   DECIMAL(15,2)             COMMENT 'Draft Obligation Adjustment Amount',
    li_tracking_num                               BIGINT             COMMENT 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"',
    funding_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FUNDING',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Table Description:  Ledger tracking all LOA related monitary transfers.  All amounts in this table are relative to the LOA.  If an amount column is positive - money is being added to the LOA.  If an amount column is negative - money is being removed from the LOA.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'loa_ledger',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_loa_ledger',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_loa_ledger_burn_order
-- Source : aasbs.loa_ledger_burn_order
-- Comment: The order in which LOAs are to be depleted.  Applies only to cases of multiple LOAs funding a single
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_loa_ledger_burn_order
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.LOA_LEDGER_BURN_ORDER_SEQ',
    line_item_accepted_id                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM_ACCEPTED',
    tracking_num                                  STRING             COMMENT 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it\'s Tracking_Num',
    burn_order                                    BIGINT             COMMENT 'Defined order of depletion',
    loa_subtask                                   STRING             COMMENT 'Subtask used to identify Line Item / LOA combinations',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'The order in which LOAs are to be depleted.  Applies only to cases of multiple LOAs funding a single line item'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'loa_ledger_burn_order',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_loa_ledger_burn_order',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_accrual_income_doc_type
-- Source : aasbs.lu_accrual_income_doc_type
-- Comment: lookup table - all accrual income document types
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_accrual_income_doc_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] alphanumeric primary key for this table',
    description                                   STRING             COMMENT 'description of the lookup value',
    active_yn                                     STRING             COMMENT 'flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'oracle login user id who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'oracle login username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'lookup table - all accrual income document types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_accrual_income_doc_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_accrual_income_doc_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_acquisition_mod_status
-- Source : aasbs.lu_acquisition_mod_status
-- Comment: This table contains all possible statuses of an acquisition mod
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_acquisition_mod_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] State or status code of acquisition mod',
    description                                   STRING             COMMENT 'Display names for acquisition mod status',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'This table contains all possible statuses of an acquisition mod'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_acquisition_mod_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_acquisition_mod_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_acquisition_status
-- Source : aasbs.lu_acquisition_status
-- Comment: Lookup table - All possible statuses of an Acquisition
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_acquisition_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] State or status code on acquisition',
    description                                   STRING             COMMENT 'Display names for acquisition status',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All possible statuses of an Acquisition'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_acquisition_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_acquisition_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_acquisition_type
-- Source : aasbs.lu_acquisition_type
-- Comment: Lookup table - Type of Acquistion, e.g., Service or Supply
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_acquisition_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Type of Acquistion, e.g., Service or Supply'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_acquisition_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_acquisition_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_activity_address_code
-- Source : aasbs.lu_activity_address_code
-- Comment: Lookup table - Activity Address Codes - uniform way to identify organizations in federal agencies
-- Columns: 13 source + 6 audit = 19 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_activity_address_code
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    region_cd                                     STRING             COMMENT 'Foreign Key to table AASBS.LU_REGION',
    program_cd                                    STRING             COMMENT 'Foreign Key to table AASBS.LU_PROGRAM',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    emp_org_id                                    BIGINT             COMMENT '[FK] Foreign key to table ASSIST.LU_EMP_ORGS.EMP_ORG_ID (values should be "Office" from Registration LU_EMP_ORGS.TIER_ID = 3)',
    regional_office_address_id                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    fpds_ng_agency_identifier                     STRING             COMMENT 'These "Agency Identifiers" are a concatenation ASSIST2 Agency and Bureau codes - specifically used by FPDS-NG (Federal Procurement Data System - Next Generation)',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Activity Address Codes - uniform way to identify organizations in federal agencies'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_activity_address_code',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_activity_address_code',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_address_type
-- Source : aasbs.lu_address_type
-- Comment: Lookup table - Address type for a single Address record, e.g., Client, Shipping, Accepting
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_address_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Address type for a single Address record, e.g., Client, Shipping, Accepting'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_address_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_address_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_agency
-- Source : aasbs.lu_agency
-- Comment: Lookup table - All Agencies referenced in Assist
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_agency
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Primary key for the table',
    cd_2char                                      STRING             COMMENT 'Legacy 2-char agency code',
    description                                   STRING             COMMENT 'Agency Name (sourced from FAST Book II - March 2017 https://www.fiscal.treasury.gov/fsreports/ref/fastBook/fastbook_home.htm )',
    is_dod_agency_yn                              STRING             COMMENT 'Flag indicating whether an Agency is associated with DOD (=Y)  or not (=N)',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    comments                                      STRING             COMMENT 'Comments',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
    sort_order                                    BIGINT,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Agencies referenced in Assist'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_agency',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_agency',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_agreement_type
-- Source : aasbs.lu_agreement_type
-- Comment: Lookup table - GSA Agency Agreement types, e.g., Inter-Agency, Intra-Agency
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_agreement_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - GSA Agency Agreement types, e.g., Inter-Agency, Intra-Agency'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_agreement_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_agreement_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_award_mod_fpds_reason
-- Source : aasbs.lu_award_mod_fpds_reason
-- Comment: Lookup table - All Possible FPDS Modification Reasons
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_award_mod_fpds_reason
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Possible FPDS Modification Reasons'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_award_mod_fpds_reason',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_award_mod_fpds_reason',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_award_validation
-- Source : aasbs.lu_award_validation
-- Comment: Lookup table - Award Validation Criteria
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_award_validation
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the Award Validation',
    title                                         STRING             COMMENT 'Title of the Award Validation',
    validation_type                               STRING             COMMENT 'Validation Type - Hard or Soft',
    remedy                                        STRING             COMMENT 'Instructions to remedy an unfulfilled Award Validation',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Award Validation Criteria'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_award_validation',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_award_validation',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_billing_type
-- Source : aasbs.lu_billing_type
-- Comment: Lookup table - All Billing Types for a Funding Package (e.g, Standard, Advanced, Pre-Paid, Pre-Valid
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_billing_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Billing Types for a Funding Package (e.g, Standard, Advanced, Pre-Paid, Pre-Validation)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_billing_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_billing_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_bundled_contract
-- Source : aasbs.lu_bundled_contract
-- Comment: Lookup table - Options for whether the acquisition meets the definition of a bundled requirement, as
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_bundled_contract
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition meets the definition of a bundled requirement, as defined in the FAR.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_bundled_contract',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_bundled_contract',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_bureau
-- Source : aasbs.lu_bureau
-- Comment: Lookup table - All Bureaus referenced in Assist
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_bureau
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Primary key for the table',
    agency_cd                                     STRING NOT NULL    COMMENT '[PK] Foreign Key to table AASBS.LU_AGENCY',
    description                                   STRING             COMMENT 'Description of the client type',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Bureaus referenced in Assist'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_bureau',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_bureau',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_central_comment_type
-- Source : aasbs.lu_central_comment_type
-- Comment: Lookup table - Types of Comments in CENTRAL_COMMENT table
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_central_comment_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    table_name_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_TABLE_NAME',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of Comments in CENTRAL_COMMENT table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_central_comment_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_central_comment_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_checklist_item
-- Source : aasbs.lu_checklist_item
-- Comment: Lookup table - Checklist Item - invidivual line items to which answers are attributed manually or au
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_checklist_item
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    checklist_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_CHECKLIST_TYPE - used to group CHECKLIST ITEMS that display together on the same UI',
    description                                   STRING             COMMENT 'Display name the checklist item',
    tooltip                                       STRING             COMMENT 'Tooltip for the Checklist Item',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Checklist Item - invidivual line items to which answers are attributed manually or automatically'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_checklist_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_checklist_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_checklist_type
-- Source : aasbs.lu_checklist_type
-- Comment: Lookup table - Checklist Type - used to create distinct lists of Checklist Items - Acquisition Close
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_checklist_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Display name the checklist type',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Checklist Type - used to create distinct lists of Checklist Items - Acquisition Closeout, etc'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_checklist_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_checklist_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_checklist_value
-- Source : aasbs.lu_checklist_value
-- Comment: Lookup table - Checklist Values - dropdown answers that can be attributed to a Checklist Item
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_checklist_value
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Display name the checklist value',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Checklist Values - dropdown answers that can be attributed to a Checklist Item'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_checklist_value',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_checklist_value',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_closeout_prohibition
-- Source : aasbs.lu_closeout_prohibition
-- Comment: Lookup table - Prohibitions in General - currently only used for Closeout
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_closeout_prohibition
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Display name for the prohibition',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Prohibitions in General - currently only used for Closeout'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_closeout_prohibition',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_closeout_prohibition',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_collab_status
-- Source : aasbs.lu_collab_status
-- Comment: Lookup table - Collaboration Statuses - specific to Collaboration Type - see MAP_COLLAB_TYPE_STATUS 
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_collab_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Collaboration Statuses - specific to Collaboration Type - see MAP_COLLAB_TYPE_STATUS table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_collab_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_collab_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_collab_type
-- Source : aasbs.lu_collab_type
-- Comment: Lookup table - Collaboration Types, e.g., Resume, Technical Report
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_collab_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Collaboration Types, e.g., Resume, Technical Report'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_collab_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_collab_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_commercial_type
-- Source : aasbs.lu_commercial_type
-- Comment: This table contains all possible client types
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_commercial_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Primary key for the table',
    description                                   STRING             COMMENT 'Description of commercialization type',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'This table contains all possible client types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_commercial_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_commercial_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_competition_type
-- Source : aasbs.lu_competition_type
-- Comment: Lookup table - Types of bid competition (e.g., Full and Open Competition, Sole source SDVOSB)
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_competition_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    level_num                                     INT             COMMENT 'Not a business column - indicates the hierarchy level in this self-referencing table',
    parent_competition_type_cd                    STRING             COMMENT 'Foreign Key to table AASBS.LU_PARENT_COMPETITION_TYPE',
    next_level_dropdown_label                     STRING             COMMENT 'Not a business column - controls Acquisition Planning UI labels',
    next_level_tooltip                            STRING             COMMENT 'This is an internally used column - helps to define behavior of ASSIST2.0\'s Competition Types UI',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of bid competition (e.g., Full and Open Competition, Sole source SDVOSB)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_competition_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_competition_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_consolidated_contract
-- Source : aasbs.lu_consolidated_contract
-- Comment: Lookup table - Options for whether the acquisition meets the definition of a consolidated requiremen
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_consolidated_contract
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition meets the definition of a consolidated requirement, as defined in the FAR.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_consolidated_contract',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_consolidated_contract',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_continuity_type
-- Source : aasbs.lu_continuity_type
-- Comment: Lookup table - Types of continuity with previous Acquisition
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_continuity_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of continuity with previous Acquisition'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_continuity_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_continuity_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_contract_type
-- Source : aasbs.lu_contract_type
-- Comment: Lookup table - Acquisition contract type - e.g, Firm Fixed Price, Time and Materials
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_contract_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    accrual_universal_holdback_pct                INT             COMMENT 'Accrual Universal Holdback Percent',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Acquisition contract type - e.g, Firm Fixed Price, Time and Materials'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_contract_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_contract_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_cor_delegation
-- Source : aasbs.lu_cor_delegation
-- Comment: Lookup table - Options for identifying the method of Contracting Officer\'s Representative (COR) del
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_cor_delegation
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for identifying the method of Contracting Officer\\''s Representative (COR) delegation being used for this acquisition.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_cor_delegation',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_cor_delegation',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_cost_rate_type
-- Source : aasbs.lu_cost_rate_type
-- Comment: Lookup table - All Labor Cost Rate Types
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_cost_rate_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    ui_visible_yn                                 STRING             COMMENT 'Flag indicating whether this value should be visible in the UI',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Labor Cost Rate Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_cost_rate_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_cost_rate_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_country
-- Source : aasbs.lu_country
-- Comment: Lookup table - List of countries for which GSA has business ties
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_country
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - List of countries for which GSA has business ties'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_country',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_country',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_email_status
-- Source : aasbs.lu_email_status
-- Comment: Lookup table - Creation/Delivery status of emails in EMAIL_LOG table
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_email_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Creation/Delivery status of emails in EMAIL_LOG table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_email_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_email_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_emergency_acq
-- Source : aasbs.lu_emergency_acq
-- Comment: Lookup table - Options for whether the acquisition is being conducted under FAR Part 18 Emergency Ac
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_emergency_acq
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_emergency_acq',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_emergency_acq',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_event_type
-- Source : aasbs.lu_event_type
-- Comment: Lookup table - Event Types related to Chronology
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_event_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Event Types related to Chronology'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_event_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_event_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_expedited_type
-- Source : aasbs.lu_expedited_type
-- Comment: Lookup table - Driving cause of processing speed for an Acquisition
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_expedited_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Numeric code representing a territory',
    description                                   STRING             COMMENT 'City-State representing the territory',
    national_emergency_yn                         STRING             COMMENT 'Classification of the Expedited type',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Driving cause of processing speed for an Acquisition'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_expedited_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_expedited_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fob_type
-- Source : aasbs.lu_fob_type
-- Comment: Lookup table - Free/Freight on board (FOB) values
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fob_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Free/Freight on board (FOB) values'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fob_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fob_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fpds_car_status
-- Source : aasbs.lu_fpds_car_status
-- Comment: Lookup table - FDSP Contract Action Report Statuses
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fpds_car_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - FDSP Contract Action Report Statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fpds_car_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fpds_car_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fund
-- Source : aasbs.lu_fund
-- Comment: Lookup table - GSA Funds, e.g., 285X, 285F
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fund
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - GSA Funds, e.g., 285X, 285F'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fund',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fund',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fund_amend_status
-- Source : aasbs.lu_fund_amend_status
-- Comment: Lookup table - All Funding Amendment statuses (e.g., Draft, Error, Final, Pending)
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fund_amend_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Funding Amendment statuses (e.g., Draft, Error, Final, Pending)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fund_amend_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fund_amend_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fund_category
-- Source : aasbs.lu_fund_category
-- Comment: Lookup table - GSA Funding Categories, e.g., DIRECT_CITE, REIMBURSE
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fund_category
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - GSA Funding Categories, e.g., DIRECT_CITE, REIMBURSE'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fund_category',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fund_category',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fund_status
-- Source : aasbs.lu_fund_status
-- Comment: Lookup table - All Funding Package Statuses (e.g., Active, Closed, Draft)
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fund_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Funding Package Statuses (e.g., Active, Closed, Draft)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fund_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fund_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_fund_type
-- Source : aasbs.lu_fund_type
-- Comment: Lookup table - All Funding Types for  a Funding Package (e.g., Annual, Multi-year, No-year)
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_fund_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Funding Types for  a Funding Package (e.g., Annual, Multi-year, No-year)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_fund_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_fund_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_funds_usage
-- Source : aasbs.lu_funds_usage
-- Comment: Lookup table - All purposes for allocating a funding source
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_funds_usage
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All purposes for allocating a funding source'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_funds_usage',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_funds_usage',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_group_contact_type
-- Source : aasbs.lu_group_contact_type
-- Comment: Lookup table - Category of Group Contact, e.g., CONTRACTING, NATIONAL_FSD
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_group_contact_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Category of Group Contact, e.g., CONTRACTING, NATIONAL_FSD'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_group_contact_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_group_contact_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_ia_amend_action
-- Source : aasbs.lu_ia_amend_action
-- Comment: This table contains all possible actions for an Interagency Agreement Amendment
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_ia_amend_action
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Action code for an Interagency Agreement Amendment action',
    description                                   STRING             COMMENT 'Display names for an Interagency Agreement Amendment action',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'This table contains all possible actions for an Interagency Agreement Amendment'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_ia_amend_action',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_ia_amend_action',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_ia_status
-- Source : aasbs.lu_ia_status
-- Comment: This table contains all possible statuses of an Interagency Agreement
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_ia_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] State or status code on an Interagency Agreement',
    description                                   STRING             COMMENT 'Display names for an Interagency Agreement status',
    active_yn                                     STRING             COMMENT 'Indicator of whether the record is an active lookup record that can still be used for new/updated records',
    sort_order                                    BIGINT             COMMENT 'Sort order for UI display',
    created_by_user_name                          STRING             COMMENT 'Record created by this user',
    created_dt                                    TIMESTAMP             COMMENT 'Record created on this date',
    updated_by_user_name                          STRING             COMMENT 'Record last updated by this user',
    updated_dt                                    TIMESTAMP             COMMENT 'Record last updated on this date',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'This table contains all possible statuses of an Interagency Agreement'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_ia_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_ia_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_idv_referenced_class
-- Source : aasbs.lu_idv_referenced_class
-- Comment: Lookup table - Types of contract vehicles (e.g., AAS Vehicle, GSA Vehicle)
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_idv_referenced_class
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of contract vehicles (e.g., AAS Vehicle, GSA Vehicle)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_idv_referenced_class',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_idv_referenced_class',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_idv_type_of_idc
-- Source : aasbs.lu_idv_type_of_idc
-- Comment: Lookup table - Types of Delivery/Quantity (e.g., IDDQ, IDIQ)
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_idv_type_of_idc
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of Delivery/Quantity (e.g., IDDQ, IDIQ)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_idv_type_of_idc',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_idv_type_of_idc',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_idv_who_can_use
-- Source : aasbs.lu_idv_who_can_use
-- Comment: Lookup table - Types of Agencies that can use the IDV
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_idv_who_can_use
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Types of Agencies that can use the IDV'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_idv_who_can_use',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_idv_who_can_use',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_instrument_type
-- Source : aasbs.lu_instrument_type
-- Comment: Lookup table - Procurement Instrument types
-- Columns: 16 source + 6 audit = 22 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_instrument_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    instrument_type_1char                         STRING             COMMENT 'Procurement instrument type 1 character code for use in PIID generation',
    description                                   STRING             COMMENT 'Description of the lookup value',
    proc_phase_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_PROC_PHASE',
    ui_visible_yn                                 STRING             COMMENT 'Flag indicating whether this value should be visible in the UI',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    short_description                             STRING             COMMENT 'Short Description',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    can_associate_idv_yn                          STRING             COMMENT 'Flag indicating whether the INSTRUMENT_TYPE record can be associated with IDV (=Y) or not (=N)',
    fpds_ng_award_type                            STRING             COMMENT 'Federal Procurement Data System - Next Generation (FPDS) Award Type',
    fpds_ng_award_code                            STRING             COMMENT 'Federal Procurement Data System - Next Generation (FPDS) Award Code',
    fpds_ng_award_description                     STRING             COMMENT 'Federal Procurement Data System - Next Generation (FPDS) Award Description',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Procurement Instrument types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_instrument_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_instrument_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_intel_community
-- Source : aasbs.lu_intel_community
-- Comment: Lookup table - Options for whether the acquisition is being conducted on behalf of a member of the I
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_intel_community
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_intel_community',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_intel_community',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_line_item_type
-- Source : aasbs.lu_line_item_type
-- Comment: Lookup table - All Line Item Types
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_line_item_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Line Item Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_line_item_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_line_item_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_loa_change_reason
-- Source : aasbs.lu_loa_change_reason
-- Comment: Lookup table - All Reasons a change could be made on an LOA
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_loa_change_reason
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Order in which these valuse are to be displayed',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Reasons a change could be made on an LOA'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_loa_change_reason',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_loa_change_reason',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_loa_status
-- Source : aasbs.lu_loa_status
-- Comment: Lookup table - All LOA statuses
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_loa_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All LOA statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_loa_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_loa_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_office_type
-- Source : aasbs.lu_office_type
-- Comment: Lookup table - Issuing Office Type - Procurement or Administration
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_office_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Issuing Office Type - Procurement or Administration'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_office_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_office_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_onefund_activity
-- Source : aasbs.lu_onefund_activity
-- Comment: Lookup table - All Activity codes
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_onefund_activity
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    fund_cd                                       STRING             COMMENT 'Fund - Foreign key to AASBS.LU_FUND.CD',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Activity codes'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_onefund_activity',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_onefund_activity',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_onefund_program
-- Source : aasbs.lu_onefund_program
-- Comment: lookup table - all program codes from aasbs.lu_onefund
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_onefund_program
(
    cd                                            STRING NOT NULL    COMMENT '[PK] alphanumeric primary key for this table',
    description                                   STRING             COMMENT 'description of the lookup value',
    active_yn                                     STRING             COMMENT 'flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'oracle login user id who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'oracle login user id who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'lookup table - all program codes from aasbs.lu_onefund'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_onefund_program',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_onefund_program',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_performance_type
-- Source : aasbs.lu_performance_type
-- Comment: Lookup table - Acquisition performance type, e.g., Service where PBA is used, not used
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_performance_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Acquisition performance type, e.g., Service where PBA is used, not used'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_performance_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_performance_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_proc_phase
-- Source : aasbs.lu_proc_phase
-- Comment: Lookup table - Procurement Phases, e.g., Acquisition, Solicitation, Award
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_proc_phase
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    extension_name                                STRING             COMMENT 'Phase-specific business terminology for the Extention/Modification/Amendment',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    parent_cd                                     STRING             COMMENT 'Foreign Key to table AASBS.LU_PROC_PHASE - self referencing to indicate a phase parent',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    short_description                             STRING             COMMENT 'Short description',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Procurement Phases, e.g., Acquisition, Solicitation, Award'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_proc_phase',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_proc_phase',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_processing_speed
-- Source : aasbs.lu_processing_speed
-- Comment: Lookup table - Speed of processing the Acquisition - Routine or Expedited
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_processing_speed
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Speed of processing the Acquisition - Routine or Expedited'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_processing_speed',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_processing_speed',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_program
-- Source : aasbs.lu_program
-- Comment: Lookup table - GSA Programs, e.g., AAS, ITS_NSD
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_program
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - GSA Programs, e.g., AAS, ITS_NSD'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_program',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_program',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_reason_for_direct
-- Source : aasbs.lu_reason_for_direct
-- Comment: Lookup table - All LU_REASON_FOR_DIRECT
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_reason_for_direct
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All LU_REASON_FOR_DIRECT'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_reason_for_direct',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_reason_for_direct',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_region
-- Source : aasbs.lu_region
-- Comment: Lookup table - Regions
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_region
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Regions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_region',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_region',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_responsible_role
-- Source : aasbs.lu_responsible_role
-- Comment: Lookup table - User Application/Business Roles, e.g., Client Representative, Contract Specialist
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_responsible_role
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - User Application/Business Roles, e.g., Client Representative, Contract Specialist'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_responsible_role',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_responsible_role',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_review_finding
-- Source : aasbs.lu_review_finding
-- Comment: Lookup table - All Possible Review Findings
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_review_finding
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Possible Review Findings'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_review_finding',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_review_finding',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_review_type
-- Source : aasbs.lu_review_type
-- Comment: Lookup table - All Possible Review Types
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_review_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Possible Review Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_review_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_review_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_reviewer_type
-- Source : aasbs.lu_reviewer_type
-- Comment: Lookup table - All Possible Reviewer Types
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_reviewer_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Possible Reviewer Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_reviewer_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_reviewer_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_severability_type
-- Source : aasbs.lu_severability_type
-- Comment: Lookup table - Contract severability, e.g., Severable/Nonseverable/Mix services
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_severability_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Contract severability, e.g., Severable/Nonseverable/Mix services'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_severability_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_severability_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sf30_mod_type
-- Source : aasbs.lu_sf30_mod_type
-- Comment: Lookup table - All Invoice Statuses
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sf30_mod_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Invoice Statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sf30_mod_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sf30_mod_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_small_business_admin
-- Source : aasbs.lu_small_business_admin
-- Comment: Silver copy of aasbs.lu_small_business_admin from ASSIST Postgres source.
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_small_business_admin
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Silver copy of aasbs.lu_small_business_admin from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_small_business_admin',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_small_business_admin',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sol_amend_status
-- Source : aasbs.lu_sol_amend_status
-- Comment: Lookup table - All Solicitation Amendment Statuses
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sol_amend_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Solicitation Amendment Statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sol_amend_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sol_amend_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sol_posting_type
-- Source : aasbs.lu_sol_posting_type
-- Comment: Lookup table - All Solicitation Posting Types
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sol_posting_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Solicitation Posting Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sol_posting_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sol_posting_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sol_response_aac_visibility
-- Source : aasbs.lu_sol_response_aac_visibility
-- Comment: Lookup table - All Solicitation Response visibilities for AACs
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sol_response_aac_visibility
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Solicitation Response visibilities for AACs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sol_response_aac_visibility',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sol_response_aac_visibility',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sol_response_cli_visibility
-- Source : aasbs.lu_sol_response_cli_visibility
-- Comment: Lookup table - All Solicitation Response visibilities for Clients
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sol_response_cli_visibility
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Solicitation Response visibilities for Clients'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sol_response_cli_visibility',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sol_response_cli_visibility',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_sol_status
-- Source : aasbs.lu_sol_status
-- Comment: Lookup table - All Solicitation Statuses
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_sol_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Solicitation Statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_sol_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_sol_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_state
-- Source : aasbs.lu_state
-- Comment: Lookup table - 50 US States + entities outside the United States, e.g., Virgin Islands
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_state
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    is_usa_yn                                     STRING             COMMENT 'Flag indicating whether a state is one of the 50 US States (=Y) or not (=N)',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - 50 US States + entities outside the United States, e.g., Virgin Islands'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_state',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_state',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_subsystem
-- Source : aasbs.lu_subsystem
-- Comment: Lookup table - SubSystems within the ASSIST Application
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_subsystem
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    system_cd                                     STRING             COMMENT 'Foreign Key to table AASBS.LU_SYSTEM',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - SubSystems within the ASSIST Application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_subsystem',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_subsystem',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_supply_chain_risk
-- Source : aasbs.lu_supply_chain_risk
-- Comment: Lookup table - Options for whether the acquisition is required at Cybersecurity Maturity Model Certi
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_supply_chain_risk
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_supply_chain_risk',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_supply_chain_risk',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_surveillance_spec
-- Source : aasbs.lu_surveillance_spec
-- Comment: Lookup table - Options for whether the acquisition requires a Contractor Security Classification Spe
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_surveillance_spec
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition requires a Contractor Security Classification Specification (DD-254).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_surveillance_spec',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_surveillance_spec',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_table_name
-- Source : aasbs.lu_table_name
-- Comment: Lookup table - List of AASBS tables and views that are referenced by "CENTRAL" tables, currently CEN
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_table_name
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - List of AASBS tables and views that are referenced by "CENTRAL" tables, currently CENTRAL_COMMENT'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_table_name',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_table_name',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_transaction_type
-- Source : aasbs.lu_transaction_type
-- Comment: Lookup table - All Transaction Types in LOA_Ledger table
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_transaction_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Transaction Types in LOA_Ledger table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_transaction_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_transaction_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_transfer
-- Source : aasbs.lu_transfer
-- Comment: Lookup table - Options for whether the acquisition represents the transfer of contract administratio
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_transfer
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Options for whether the acquisition represents the transfer of contract administration responsibilities.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_transfer',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_transfer',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_unit_of_measure
-- Source : aasbs.lu_unit_of_measure
-- Comment: Lookup table - All Units of Measure
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_unit_of_measure
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Units of Measure'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_unit_of_measure',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_unit_of_measure',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lu_vehicle_type
-- Source : aasbs.lu_vehicle_type
-- Comment: Lookup table - Contract Vehicle Types, e.g., Single Award / Multiple Agency Use
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lu_vehicle_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - Contract Vehicle Types, e.g., Single Award / Multiple Agency Use'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lu_vehicle_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lu_vehicle_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_lus_psc
-- Source : aasbs.lus_psc
-- Comment: Synchronized Lookup Table ("LUS_") - Product Service Codes (PSC) - Updated nightly by the ASSIST app
-- Columns: 20 source + 6 audit = 26 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_lus_psc
(
    psc_id                                        BIGINT             COMMENT '[FK] Numeric ID column but all foreign key references to this table should point to the PSC_CODE column',
    psc_code                                      STRING             COMMENT 'GSA-defined codes - this should be considered the Primary Key column for this table',
    psc_name                                      STRING             COMMENT 'Externally defined column',
    psc_full_name                                 STRING             COMMENT 'Externally defined column',
    psc_include                                   STRING             COMMENT 'Externally defined column',
    active                                        STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N) (Externally defined and imported to this table)',
    psc_exclue                                    STRING             COMMENT 'Externally defined column',
    parent_psc_code                               STRING             COMMENT 'Externally defined column',
    active_start_dt                               TIMESTAMP             COMMENT 'Date indicating when this value became Active (Externally defined and imported to this table)',
    update_dt                                     TIMESTAMP             COMMENT 'Externally defined column',
    level_1_category_name                         STRING             COMMENT 'Externally defined column',
    level_2_category_name                         STRING             COMMENT 'Externally defined column',
    psc_type                                      STRING             COMMENT 'Externally defined column',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    level_1_category                              STRING             COMMENT 'The level 1 category of the PSC Code. A number.',
    level_2_category                              STRING             COMMENT 'The level 2 category of the PSC Code. A number.',
    active_end_dt                                 TIMESTAMP             COMMENT 'End date of the PSC Code',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (psc_id, updated_dt)
COMMENT 'Synchronized Lookup Table ("LUS_") - Product Service Codes (PSC) - Updated nightly by the ASSIST application layer'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'lus_psc',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_lus_psc',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_map_aac_onefund
-- Source : aasbs.map_aac_onefund
-- Comment: Silver copy of aasbs.map_aac_onefund from ASSIST Postgres source.
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_map_aac_onefund
(
    activity_address_cd                           STRING NOT NULL    COMMENT '[PK]',
    onefund_cd                                    STRING NOT NULL    COMMENT '[PK]',
    cost_element_cd                               STRING,
    object_class_cd                               STRING,
    onefund_program_cd                            STRING,
    onefund_activity_cd                           STRING,
    fund_cd                                       STRING,
    active_yn                                     STRING             COMMENT 'Contains whether the mapping is active',
    created_by_user_name                          STRING             COMMENT 'User that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'User that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (activity_address_cd, updated_dt)
COMMENT 'Silver copy of aasbs.map_aac_onefund from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'map_aac_onefund',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_map_aac_onefund',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_map_onefund_program_accrual_income_doc_type
-- Source : aasbs.map_onefund_program_accrual_income_doc_type
-- Comment: Map table - Mapping of onefund program code to accrual income document type
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_map_onefund_program_accrual_income_doc_type
(
    onefund_program_cd                            STRING             COMMENT 'Foreign Key to table aasbs.lu_onefund_program',
    accrual_income_doc_type_cd                    STRING             COMMENT 'Foreign Key to table aasbs.lu_accrual_income_doc_type',
    created_by_user_name                          STRING             COMMENT 'oracle login user id who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'oracle login user id who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'most recent date this database record was updated',
    active_yn                                     STRING             COMMENT 'flag indicating whether a record is active (=Y) or not (=N)',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (onefund_program_cd, updated_dt)
COMMENT 'Map table - Mapping of onefund program code to accrual income document type'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'map_onefund_program_accrual_income_doc_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_map_onefund_program_accrual_income_doc_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_piid
-- Source : aasbs.piid
-- Comment: Generated Procurement Instrument IDs
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_piid
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.PIID_SEQ',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    fiscal_year                                   INT             COMMENT 'Fiscal Year of PIID',
    instrument_type_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE',
    instrument_type_1char                         STRING             COMMENT 'Single digit instrument type code used in PIID',
    sequence                                      BIGINT             COMMENT 'Unique sequence number allocated for PIID for combination of AAC, FY, Instrument Code 1 Char',
    piid                                          STRING             COMMENT 'The generated unique PIID',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    apex_emp_org_id                               BIGINT             COMMENT '[FK] Apex office associated to this piid',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Generated Procurement Instrument IDs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'piid',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_piid',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_piid_ext
-- Source : aasbs.piid_ext
-- Comment: Generated next sequential mod/amendment extension to a Procurement Instrument IDs
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_piid_ext
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.PIID_EXT_SEQ',
    piid_id                                       BIGINT             COMMENT '[FK] Foreign Key to table AASBS.PIID',
    parent_ext_id                                 BIGINT             COMMENT '[FK] Self Referencing Foreigh key -',
    office_type_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_OFFICE_TYPE',
    extension_sequence                            BIGINT             COMMENT 'Extension / Mod sequence - purpose of this table',
    extension                                     STRING             COMMENT 'The generated extension for the PIID',
    subsystem_cd                                  STRING             COMMENT 'Foreign Key to table AASBS.LU_SYSTEM - system that requested the PIID',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Generated next sequential mod/amendment extension to a Procurement Instrument IDs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'piid_ext',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_piid_ext',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_review
-- Source : aasbs.review
-- Comment: Contains high-level review details for all reviews within the ASSIST application
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_review
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.REVIEW_SEQ',
    review_type_cd                                STRING             COMMENT 'Foreign Key to table AASBS.LU_SOL_REVIEW_TYPE',
    review_parent_table_name_cd                   STRING             COMMENT 'Foreign Key to table AASBS.LU_TABLE_NAME',
    solicit_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.AWARD_MOD',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    draft_review_yn                               STRING             COMMENT 'Flag indicating whether the Review Form is (=Y) or is not (=N) in DRAFT state',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains high-level review details for all reviews within the ASSIST application'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'review',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_review',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_review_determination
-- Source : aasbs.review_determination
-- Comment: Contains review details entered for each individual reviewer within the larger Review
-- Columns: 15 source + 6 audit = 21 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_review_determination
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.REVIEW_SEQ',
    review_id                                     BIGINT             COMMENT '[FK] Foreign Key to table AASBS.REVIEW',
    reviewer_determination_num                    STRING             COMMENT 'Reviewer number within the larger Review',
    review_finding_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_REVIEW_FINDING',
    reviewer_type_cd                              STRING             COMMENT 'Foreign Key to table AASBS.LU_REVIEWER_TYPE',
    review_is_required_yn                         STRING             COMMENT 'Flag indicating whether a Review is required (=Y) or not (=N)',
    reviewer_comments                             STRING             COMMENT 'Reviewer Comments',
    reviewer_user_id                              STRING             COMMENT '[FK] Foreign Key to table AASBS.REVIEWER_USER - when this column has a value - REVIEWER_FULL_NAME must be null',
    reviewer_full_name                            STRING             COMMENT 'Reviewer\'s full name - when this column has a value - REVIEWER_USER_ID must be null',
    reviewer_started_dt                           TIMESTAMP             COMMENT 'Date the Solicitation Review was started',
    reviewer_completed_dt                         TIMESTAMP             COMMENT 'Date the Solicitation Review was completed',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains review details entered for each individual reviewer within the larger Review'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'review_determination',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_review_determination',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_service_charge
-- Source : aasbs.service_charge
-- Comment: Service Charge details
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_service_charge
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SERVICE_CHARGE_SEQ',
    act_number                                    STRING             COMMENT 'Accounting Control Transaction number',
    is_credit_yn                                  STRING             COMMENT 'Identifies if charge is a bill or a credit',
    credit_to_service_charge_id                   BIGINT             COMMENT '[FK] If a credit, identifies the SERVICE_CHARGE.ID which is credited.',
    omis_billings_bill_id                         BIGINT             COMMENT '[FK] For Service Charges which also exist in OMIS, foreign key to OMIS.BILLINGS.BILL_ID',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Service Charge details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'service_charge',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_service_charge',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_service_charge_schedule
-- Source : aasbs.service_charge_schedule
-- Comment: This table defines the billing schedule for scheduled (fixed fee) service charges.
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_service_charge_schedule
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SERVICE_CHARGE_SCHEDULE_SEQ',
    line_item_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LINE_ITEM',
    service_start_dt                              TIMESTAMP             COMMENT 'The date on which this scheduled service starts',
    service_end_dt                                TIMESTAMP             COMMENT 'The date on which this scheduled service ends and will be billed',
    scheduled_amt                                 DECIMAL(15,2)             COMMENT 'The amount to be billed for this scheduled service charge',
    description                                   STRING             COMMENT 'The description of this scheduled service charge',
    sc_schedule_status_cd                         STRING             COMMENT 'Foreign Key to table AASBS.SC_SCHEDULE_STATUS',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'This table defines the billing schedule for scheduled (fixed fee) service charges.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'service_charge_schedule',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_service_charge_schedule',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_snap_award_mod_amts
-- Source : aasbs.snap_award_mod_amts
-- Comment: Silver copy of aasbs.snap_award_mod_amts from ASSIST Postgres source.
-- Columns: 14 source + 6 audit = 20 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_snap_award_mod_amts
(
    source_system                                 STRING,
    award_mod_id                                  BIGINT NOT NULL    COMMENT '[PK]',
    award_piid                                    STRING,
    mod_num                                       STRING,
    pop_end_dt                                    TIMESTAMP,
    cum_funded_amt                                DECIMAL(15,2),
    base_all_opt_amt                              DECIMAL(15,2),
    base_exc_opt_amt                              DECIMAL(19,2),
    cost_to_gsa_diff_amt                          DECIMAL(15,2),
    cost_to_gsa_total_amt                         DECIMAL(15,2),
    service_charges_diff_amt                      DECIMAL(15,2),
    service_charges_total_amt                     DECIMAL(15,2),
    cost_to_client_diff_amt                       DECIMAL(15,2),
    cost_to_client_total_amt                      DECIMAL(15,2),
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (award_mod_id)
COMMENT 'Silver copy of aasbs.snap_award_mod_amts from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'snap_award_mod_amts',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_snap_award_mod_amts',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit
-- Source : aasbs.solicit
-- Comment: Solicitation Details
-- Columns: 22 source + 6 audit = 28 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_SEQ',
    acquisition_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACQUISITION',
    instrument_type_cd                            STRING             COMMENT 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE',
    lastfinal_solicit_amendment_id                BIGINT             COMMENT '[FK] Foreign Key to table AASBS.LATEST_SOLICIT_AMENDMENT',
    solicit_piid                                  STRING             COMMENT 'Solicitation procurement instrument identifier',
    solicit_title                                 STRING             COMMENT 'Title of the Solicitation',
    solicit_description                           STRING             COMMENT 'Description of Solicitation',
    sol_status_cd                                 STRING             COMMENT 'Foreign Key to table AASBS.LU_SOL_STATUS',
    sol_response_cli_visibility_cd                STRING             COMMENT 'Foreign Key to table AASBS.LU_SOL_RESPONSE_CLI_VISIBILITY',
    sol_response_aac_visibility_cd                STRING             COMMENT 'Foreign Key to table AASBS.LU_SOL_RESPONSE_AAC_VISIBILITY',
    at_award_bid_cnt                              INT             COMMENT 'Number of Responses/Bids from Contractors on a Solicitation at the time it is Awarded',
    winner_selection_start_dt                     TIMESTAMP             COMMENT 'The start date of the period of selecting a Solicitation Winner',
    winner_selection_end_dt                       TIMESTAMP             COMMENT 'The end date of the period of selecting a Solicitation Winner',
    draft_frm_selection_yn                        STRING             COMMENT 'Flag indicating whether Solicitation Selection Form is (=Y) or is not (=N) in DRAFT state',
    draft_frm_presol_pkg_yn                       STRING             COMMENT 'Flag indicating whether Solicitation Presolicitation Package Form is (=Y) or is not (=N) in DRAFT state',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    migration_type                                STRING             COMMENT 'Internal use only - defines the data structure for the Solicitation',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    apex_emp_org_id                               BIGINT             COMMENT '[FK] References ASSIST.LU_EMP_ORGS.EMP_ORG_ID',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Solicitation Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_amendment
-- Source : aasbs.solicit_amendment
-- Comment: Solicitation Amendment Details - child of SOLICIT table
-- Columns: 13 source + 6 audit = 19 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_amendment
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_AMENDMENT_SEQ',
    solicit_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    amendment_num                                 STRING             COMMENT 'Solicitation Amendment Number',
    sol_posting_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_SOL_POSTING_TYPE',
    posting_round_num                             BIGINT             COMMENT 'Posting Round Number',
    close_dt                                      TIMESTAMP             COMMENT 'Close Date of the Solicitation Period',
    draft_frm_solamend_yn                         STRING             COMMENT 'Flag indicating whether Solictation Amendment Form is (=Y) or is not (=N) in DRAFT state',
    closing_comment                               STRING             COMMENT 'User comments made during the closing of the Solicitation Amendment',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Solicitation Amendment Details - child of SOLICIT table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_amendment',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_amendment',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_company
-- Source : aasbs.solicit_company
-- Comment: Company/Contract associated with a Solicitation
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_company
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_COMPANY_SEQ',
    solicit_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_AMENDMENT',
    ipartner_id                                   STRING             COMMENT '[FK] Foreign Key to table TABLE_MASTER.INDUSTRY_PARTNERS',
    contract_id                                   STRING             COMMENT '[FK] Foreign Key to table TABLE_MASTER.CONTRACTS',
    distribution_list_id                          BIGINT             COMMENT '[FK] Foreign Key to table TABLE_MASTER.DISTRIBUTION_LIST',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Company/Contract associated with a Solicitation'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_company',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_company',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_posting_connect
-- Source : aasbs.solicit_posting_connect
-- Comment: Direct Connect Posting Details
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_posting_connect
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_POSTING_CONNECT_SEQ',
    solicit_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_AMENDMENT',
    posting_connect_instructions                  STRING             COMMENT 'Posting Connect Instructions',
    pm_responsible_user_id                        STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Responsible Project Manager for this Posting Connect Solicitation',
    pm_responsible_email                          STRING             COMMENT 'Responsible Project Manager email',
    pm_responsible_phone                          STRING             COMMENT 'Responsible Project Manager phone',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    open_dt                                       TIMESTAMP             COMMENT 'Date of opening Posting',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Direct Connect Posting Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_posting_connect',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_posting_connect',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_posting_direct
-- Source : aasbs.solicit_posting_direct
-- Comment: Direct Connect Posting Details
-- Columns: 10 source + 6 audit = 16 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_posting_direct
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_POSTING_DIRECT_SEQ',
    solicit_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_AMENDMENT',
    reason_for_direct_cd                          STRING             COMMENT 'Foreign Key to table AASBS.LU_REASON_FOR_DIRECT',
    external_source_ref                           STRING             COMMENT 'User entered external sourcing reference',
    external_source_desc                          STRING             COMMENT 'User entered external source description',
    open_dt                                       TIMESTAMP             COMMENT 'Date of opening Posting',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Direct Connect Posting Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_posting_direct',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_posting_direct',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_response
-- Source : aasbs.solicit_response
-- Comment: Solicitation Response from Contractor
-- Columns: 34 source + 6 audit = 40 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_response
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_RESPONSE_SEQ',
    solicit_amendment_id                          BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_AMENDMENT',
    solicit_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    solicit_response_num                          STRING             COMMENT 'System generated Solicitation Response Number - unique within a single Solicitation',
    good_through_dt                               TIMESTAMP             COMMENT 'Contractor-entered expiration of their Solicitation Response',
    solicit_company_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT_COMPANY',
    latest_response_update_dt                     TIMESTAMP             COMMENT 'Date of latest update to the submitted Solicitation Response',
    winning_response_yn                           STRING             COMMENT 'Flag indicating whether the Solicitation Response has been chosen as the Winning Bid (=Y) or not (=N)',
    net_days_cnt                                  BIGINT             COMMENT 'Number of days set by Contractor defining the discount period',
    discount_days_cnt                             BIGINT             COMMENT 'Contractor-specified number of days to achieve the discount percentage (DISCOUNT_PCT column)',
    discount_pct                                  DECIMAL(6,3)             COMMENT 'Contractor-specified discount percentage if paid within specified number of days (DISCOUNT_DAYS_CNT column)',
    fob_type_cd                                   STRING             COMMENT 'Foreign Key to table AASBS.LU_FOB_TYPE',
    origin_transport_cost_amt                     DECIMAL(15,2)             COMMENT 'Cost of Transportation in the case that transportation is necessary from Origin',
    is_editable_yn                                STRING             COMMENT 'Flag indicating if the Solicitation Response is still Editable (=Y) or not (=N)',
    response_description                          STRING             COMMENT 'Contractor-entered Solicitation Reponse Description',
    response_company_name                         STRING             COMMENT 'Name of the company that responded to the solicitation',
    response_company_duns                         STRING             COMMENT 'The Responding Company\'s DUNS Number',
    response_company_duns_plus4                   STRING             COMMENT 'DUNS number assigned to a Client',
    response_poc_user_id                          STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS',
    response_poc_email                            STRING             COMMENT 'Email of the Point of Contact (POC) of the company that responded to the solicitation',
    response_poc_phone                            STRING             COMMENT 'Phone Number of the Point of Contact (POC) of the company that responded to the solicitation',
    response_poc_fax                              STRING             COMMENT 'Fax Number of the Point of Contact (POC) of the company that responded to the solicitation',
    response_company_address_id                   BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ADDRESS',
    attach_hash_id                                STRING             COMMENT '[FK] This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    no_response_reason                            STRING,
    no_response_yn                                STRING,
    response_company_uei                          STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    response_company_uei_plus4                    STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    response_company_vuei                         STRING,
    response_company_vuei_plus4                   STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Solicitation Response from Contractor'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_response',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_response',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_solicit_responsible
-- Source : aasbs.solicit_responsible
-- Comment: Personnel associated with a Solicitation
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_solicit_responsible
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_RESPONSIBLE_SEQ',
    solicit_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.SOLICIT',
    user_id                                       STRING             COMMENT '[FK] Foreign Key to table AASBS.USER',
    responsible_role_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE',
    is_primary_yn                                 STRING             COMMENT 'Flag indicating whether a person associated with an Solicitation is the primary for their role (=Y)  or not (=N)',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Personnel associated with a Solicitation'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'solicit_responsible',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_solicit_responsible',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_tracking_item
-- Source : aasbs.tracking_item
-- Comment: Contains dollar amount tracking for Tracking Line Items
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_tracking_item
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.TRACKING_ITEM_SEQ',
    li_tracking_num                               BIGINT             COMMENT 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"',
    line_item_type_cd                             STRING             COMMENT 'Foreign Key to table AASBS.LU_LINE_ITEM_TYPE - can only be LI_IA_SC_TRAVEL_TRACKING, LI_IA_SC_LABOR_TRACKING',
    tracking_item_amt                             DECIMAL(15,2)             COMMENT 'Amount being Tracked',
    ia_id                                         BIGINT             COMMENT '[FK] Foreign Key to table AASBS.IA',
    src_tkeeping_monthly_hours_id                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TIMEKEEPING.MONTHLY_HOURS',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    src_travel_voucher_id                         BIGINT             COMMENT '[FK]',
    src_financial_services_id                     BIGINT             COMMENT '[FK] Foreign Key to table AASBS.FINANCIAL_SERVICES',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains dollar amount tracking for Tracking Line Items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'tracking_item',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_tracking_item',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_travel_voucher
-- Source : aasbs.travel_voucher
-- Comment: Contains the details of Travel Vouchers submitted for billing
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_travel_voucher
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS.TRAVEL_VOUCHER_SEQ',
    line_item_id                                  BIGINT             COMMENT '[FK]',
    travel_start_dt                               TIMESTAMP,
    travel_end_dt                                 TIMESTAMP,
    destination                                   STRING,
    travel_auth_num                               STRING,
    traveling_user_id                             STRING             COMMENT '[FK]',
    created_by_user_id                            STRING             COMMENT '[FK]',
    created_dt                                    TIMESTAMP,
    updated_by_user_id                            STRING             COMMENT '[FK]',
    updated_dt                                    TIMESTAMP,
    voucher_amt                                   DECIMAL(15,2),
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains the details of Travel Vouchers submitted for billing'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'travel_voucher',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_travel_voucher',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_vw_procurement_order
-- Source : aasbs.vw_procurement_order
-- Comment: Silver copy of aasbs.vw_procurement_order from ASSIST Postgres source.
-- Columns: 5 source + 6 audit = 11 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_vw_procurement_order
(
    procurement_id                                DOUBLE             COMMENT '[FK]',
    realm                                         STRING,
    front_end_order_id                            STRING             COMMENT '[FK]',
    order_id                                      STRING             COMMENT '[FK]',
    piid                                          STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (procurement_id)
COMMENT 'Silver copy of aasbs.vw_procurement_order from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs',
    'pipeline.source_table'               = 'vw_procurement_order',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_vw_procurement_order',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: aasbs_transmit  (23 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_aac_envelope
-- Source : aasbs_transmit.aac_envelope
-- Comment: Activity Address Code Envelope
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_aac_envelope
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.AAC_ENVELOPE_SEQ',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    transmittal_poc_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TRANSMIT.TRANSMITTAL_POC',
    transmittal_envelope_id                       BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TRANSMIT.TRANSMITTAL_ENVELOPE',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS',
    transmittal_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE',
    transmittal_prefix                            STRING             COMMENT 'Transmittal Prefix',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Activity Address Code Envelope'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'aac_envelope',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_aac_envelope',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_accrual_expense_summary
-- Source : aasbs_transmit.accrual_expense_summary
-- Comment: Expense Accruals to send to VITAP
-- Columns: 20 source + 6 audit = 26 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_accrual_expense_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_EXPENSE_SUMMARY_SEQ',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.',
    transmittal_status_cd                         STRING             COMMENT 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS',
    accrual_expense_id                            BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCRUAL_EXPENSE',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    ui_description                                STRING             COMMENT 'User Entered Description',
    act_number                                    STRING             COMMENT 'Award FIN',
    mdl_subtask                                   STRING             COMMENT 'Multiple Distribution Line/Subtask',
    adjustment_credit_indicator                   STRING             COMMENT 'Adjustment/Credit Indicator.  Blank for plus or "-" for minus.',
    organization_code                             STRING             COMMENT 'Organization Code',
    amount                                        DECIMAL(15,2)             COMMENT 'Expense Accrual Amount.  Must be greater than 0.',
    service_my                                    STRING             COMMENT 'Represents calendar month and year service was provided (Format: MMYY)',
    income_expense_indicator                      STRING             COMMENT 'Income/Expense Indicator.  Valid entries are "I" for Income, "E" for Expense.',
    task_order                                    STRING             COMMENT 'Task Order',
    contract_number                               STRING             COMMENT 'Contract Number',
    function_code                                 STRING             COMMENT 'Function Code',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Expense Accruals to send to VITAP'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'accrual_expense_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_accrual_expense_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_accrual_income_acct_line
-- Source : aasbs_transmit.accrual_income_acct_line
-- Comment: Income Accrual Accounting Lines to send to BAAR
-- Columns: 46 source + 6 audit = 52 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_accrual_income_acct_line
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_INCOME_ACCT_LINE_SEQ',
    accrual_income_header_id                      BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TRANSMIT.ACCRUAL_INCOME_HEADER',
    batch_type_indicator                          STRING             COMMENT 'Identifies the Batch Type indicator for AAS Revenue Accruals',
    information_indicator                         STRING             COMMENT 'Information Indicator.  Identifies the line as a Detail.',
    accomplished_dt                               TIMESTAMP             COMMENT 'ACMP_DT_CH - Accomplished Date.  Format: MM/DD/YYYY',
    activity_code                                 STRING             COMMENT 'ACTY - Activity Code',
    agreement_line_num                            BIGINT             COMMENT 'AGRE_LNUM_CH - Agreement Line Number',
    agreement_num                                 STRING             COMMENT 'AGRE_NUM - Agreement Number',
    bbfy                                          INT             COMMENT 'BBFY - Beginning Budget Fiscal Year.  Fiscal Year from the Accounting Period (ACPD from Accrual Header)  Format: YYYY',
    commodity_inc_dec_indicator                   STRING             COMMENT 'CMDT_INCR_DCRS_IN - Commodity Increase/Decrease Indicator.  If the accrual amount is positive or zero, set to "I". If negative, set to "D".',
    contract_blanket_agreement                    STRING             COMMENT 'CTRC_FL - Contract or Blanket Agreement',
    tsym_customer_sublevel_prefix                 STRING             COMMENT 'CUST_SLVL_PRFX_CH - Customer Sublevel Prefix.',
    division_region                               STRING             COMMENT 'DIV - Region/Division.  Valid values are 00 through 11.  Positions 2 and 3 of the organization code associated with the agreement',
    description                                   STRING             COMMENT 'DSCR - Description',
    fund                                          STRING             COMMENT 'FUND - Fund',
    increase_decrease_indicator                   STRING             COMMENT 'INCR_DCRS_IN - Increase/Decrease Indicator.  If the accrual amount is positive or zero, set to "I". If negative, set to "D".',
    line_num                                      BIGINT             COMMENT 'LNUM_CH - Line Number.  A sequential number beginning with 1. Must be unique to the document.',
    organization_code                             STRING             COMMENT 'ORGN - Organization Code.  Based on Agreement.',
    program_code                                  STRING             COMMENT 'PROG - Program Code.  Based on Agreement.',
    posting_event                                 STRING             COMMENT 'PSTG_EVNT - Posting Event',
    revenue_source                                STRING             COMMENT 'REV_SRCE - Revenue Source',
    transaction_currency                          STRING             COMMENT 'TRAN_CRCY - Transaction Currency',
    transaction_currency_amt                      DECIMAL(28,2)             COMMENT 'TRAN_CRCY_AM_CH - Transaction Currency Amount.  Unsigned accrual amount with up to two decimal places. A decimal point must be included if the amount is not a whole dollar. Example: $1,134.00 can be sent as 1134 or 1134.00',
    transaction_exchange_rate                     DECIMAL(25,0)             COMMENT 'TRAN_EXCH_RT_CH - Transaction to system currency exchange rate',
    transaction_type                              STRING             COMMENT 'TT - Transaction Type.  Indicates accounting events.',
    vendor_address_code                           STRING             COMMENT 'VEND_ADDR_CD - Vendor Address Code.  BOAC2 from the Agreement.',
    vendor_code                                   STRING             COMMENT 'VEND_CD - Vendor Code.  BOAC2 from the Agreement.',
    accounting_line_type                          STRING             COMMENT 'LINE_TYP_CH - Accounting Line Type',
    tsym_agency_identifier                        STRING             COMMENT 'TRFR_AID - Transfer Treasury Symbol: Agency Identifier.  The three-digit agency identifier.',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    tsym_allocation_trans_agency                  STRING             COMMENT 'TSYM Allocation Trasmit Agency',
    tsym_availability_type                        STRING             COMMENT 'TRFR_AVAL_TYP - Transfer Treasury Symbol: Availability Type.  For funding that is not fiscal year based, identifies no-year accounts "X", clearing/suspense accounts "F", and Treasurys central summary general ledger accounts "A".',
    tsym_begin_period                             STRING             COMMENT 'TRFR_EPOA - Transfer Treasury Symbol: Ending Period of Availability.  For fiscal year-based funding.  Last fiscal year of fund availability.',
    tsym_end_period                               STRING             COMMENT 'TSYM End Period',
    tsym_main_account                             STRING             COMMENT 'TRFR_MAIN_ACCT - Transfer Treasury Symbol: Main Account.  Identifies the appropriation.',
    tsym_sub_account                              STRING             COMMENT 'TRFR_SUB_ACCT - Transfer Treasury Symbol: Sub Account',
    cust_tsym_agy_identifier                      STRING             COMMENT 'XTRN_AID - Customer Treasury Symbol: Agency Identifier.  The three-digit agency identifier.',
    cust_tsym_alloc_trans_agy                     STRING             COMMENT 'XTRN_ATA - Customer Treasury Symbol: Allocation Transfer Agency.  The three-digit Agency Identifier of the agency receiving funds through an allocation transfer.',
    cust_tsym_avail_type                          STRING             COMMENT 'XTRN_AVAL_TYP - Customer Treasury Symbol: Availability Type.  For funding that is not fiscal year based, identifies no-year accounts "X", clearing/suspense accounts "F", and Treasurys central summary general ledger accounts "A".',
    cust_tsym_begin_period                        STRING             COMMENT 'XTRN_BPOA - Customer Treasury Symbol: Beginning Period of Availability.  For fiscal year-based funding. First fiscal year of fund availability.',
    cust_tsym_end_period                          STRING             COMMENT 'XTRN_EPOA - Customer Treasury Symbol: Ending Period of Availability.  For fiscal year-based funding.  Last fiscal year of fund availability.',
    cust_tsym_main_acct                           STRING             COMMENT 'XTRN_MAIN_ACCT - Customer Treasury Symbol: Main Account.  Identifies the appropriation.',
    cust_tsym_sub_acct                            STRING             COMMENT 'XTRN_SUB_ACCT - Customer Treasury Symbol: Sub Account',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Income Accrual Accounting Lines to send to BAAR'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'accrual_income_acct_line',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_accrual_income_acct_line',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_accrual_income_header
-- Source : aasbs_transmit.accrual_income_header
-- Comment: Income Accrual Header to send to BAAR
-- Columns: 28 source + 6 audit = 34 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_accrual_income_header
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_INCOME_HEADER_SEQ',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.',
    transmittal_status_cd                         STRING             COMMENT 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS',
    accrual_income_dist_id                        BIGINT             COMMENT '[FK] Foreign key to AASBS.ACCRUAL_INCOME_DIST',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    ui_description                                STRING             COMMENT 'User Entered Description',
    act_number                                    STRING             COMMENT 'The order ACT number.',
    batch_type_indicator                          STRING             COMMENT 'Identifies the Batch Type indicator for AAS Revenue Accruals',
    information_indicator                         STRING             COMMENT 'Information Indicator.  Identifies the line as a Header.',
    accounting_period                             STRING             COMMENT 'ACPD - Accounting Period.  Fiscal month and Fiscal year to which the accrual applies.  Format: MM/YYYY',
    auto_reverse                                  STRING             COMMENT 'AUTM_RVER - Indicates that the voucher should be automatically reversed',
    correction_flag                               STRING             COMMENT 'CORR_FL - Indicates that the form represents a correction to an existing document. (T = correction, X = cancellation, F = new form)',
    document_num                                  STRING             COMMENT 'DOC_NUM_CH - Document Number, which is unique in Pegasys.  DTYP value + YYYYMMDD document date + ##### sequential number within document type and date',
    document_status                               STRING             COMMENT 'DOC_STUS - Document Status',
    document_type                                 STRING             COMMENT 'DTYP - Document Type.  Based on Program Code.',
    reset_document_date_flag                      STRING             COMMENT 'RSET_DOC_DT_FL - Indicates that original document date should be reset to the date on this form',
    reversed_flag                                 STRING             COMMENT 'RVRD_FL - Indicates whether the voucher has been reversed',
    reverse_after_periods                         DECIMAL(15,2)             COMMENT 'RVRE_AFTR_PRDS - Reverse after this number of accounting periods',
    security_organization                         STRING             COMMENT 'SCTY_ORGN - Security Organization',
    title                                         STRING             COMMENT 'TITL - Title of the transaction.  ASSIST will populate with IA PIID',
    external_system_doc_num                       STRING             COMMENT 'XSYS_DOC_NUM_CH - External System Document Number',
    external_system_id                            STRING             COMMENT '[FK] XSYS_ID - ID of the external system in which the transaction originated',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    external_accrual_income_id                    BIGINT             COMMENT '[FK] FK to ACCRUAL.ACCRUAL_INCOME.ACCRUAL_INCOME_ID',
    accrual_income_dist_summary_id                BIGINT             COMMENT '[FK] FK to AASBS.ACCRUAL_INCOME_DIST_SUMMARY.ID',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Income Accrual Header to send to BAAR'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'accrual_income_header',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_accrual_income_header',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_billing_summary
-- Source : aasbs_transmit.billing_summary
-- Comment: Billing Summary of External Billing transmissions
-- Columns: 69 source + 6 audit = 75 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_billing_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS.PO_SUMMARY_SEQ.',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign key to AASBS.TRANSMITTAL.ID.',
    bill_item_id                                  BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TRANSMIT.BILL_ITEM',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to LU_FLAT_FILE_TRANSMIT_STATUS.',
    acct_classification_code                      STRING             COMMENT 'The code that represents the 6-digits provided by FTW accounting office. Previosly known as BOAC1_CODE.',
    acct_classification_ref_num                   STRING             COMMENT 'Task Order/Cust. ID + Subtask. Subtask is not used for NBA, so this field will contain only Task Order/Cust. ID for NBA',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_ACTIVITY_ADDRESS',
    activity_code                                 STRING             COMMENT 'OneFund ACTIVITY for this region. RBA-Purchase Order Function Code: F11->AF127 F31->AF123 F51->AF151 F81->AF121 F99->AF120',
    additional_text                               STRING             COMMENT 'ASSIST2: Foreign key to AASBS.LOA_LEDGER.ID. NBA: Foreign key to OMIS.BID_BAAR.BID_BAAR_ID ~ .BID_ID ~ .AGREEMENT_NUM. For RBA, the FRONT_END_ORDER_ID.',
    advance_indicator                             STRING             COMMENT 'T for advance (a bill PDF will be created), O for advance offset (reate a null-posting bill that provides the amount that the Finance Center should use to offset the advance. No bill PDF will be created.), F (regular bill) otherwise',
    agency_code                                   STRING             COMMENT 'The requesting agency.',
    agreement_line_num                            STRING             COMMENT 'All line numbers are currently 1',
    agreement_num                                 STRING             COMMENT 'The agreement number on an LOA. Foreign key to AGREEMENT.AGREEMENT.',
    bbfy                                          STRING             COMMENT 'The beginning budget fiscal year.',
    billing_amount                                DECIMAL(28,2)             COMMENT 'The billing amount.',
    billing_record_id                             STRING             COMMENT '[FK] RNB + MMDDYYYY#######, where MMDDYYYY is the date, and ####### is a sequence number within the date.',
    budget_fy                                     INT             COMMENT 'Budget fiscal year',
    generated_dt                                  TIMESTAMP             COMMENT 'Date Billing Summary was generated',
    credit_indicator                              STRING,
    cust_sublevel_prefix                          STRING             COMMENT 'TYSM/Custome Sublevel Prefix.',
    customer_fund                                 STRING             COMMENT 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.',
    description                                   STRING             COMMENT 'Currently being set to the LOA.LINE_OF_ACCOUNTING: formerly the OMIS.MIPRS.ACCT_DATA or TABLE_MASTER.FUNDING_CITATION.FUND_CITE_CODE.',
    description_of_services                       STRING             COMMENT 'Task order or charge description.',
    designated_agent_code                         STRING             COMMENT 'LOA BOAC1: formerly found on the RBA citation or the NBA funding.',
    duns                                          STRING             COMMENT 'Intended to hold GSA Duns, which we do not capture, so we stopped populating this around May 2018. Formerly, set to DUNS number.',
    duns_plus_four                                STRING             COMMENT 'Intended to hold GSA Duns, which we do not capture, so we stopped populating this around May 2018. Formerly, set to DUNS Plus Four number.',
    external_system_doc_num                       STRING,
    fiscal_station_num                            INT,
    fund                                          STRING             COMMENT 'All records are currently being set to 285F, NBA pre-BAAR was set to 295X or 299X.',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    funding_doc_num                               STRING             COMMENT 'The funding num.',
    group_number                                  STRING             COMMENT 'May be populated for situations where multiple sequential records should be loaded as one unit of work (all are loaded or none are loaded). This field must be left blank unless there are 2 or more sequential records in the file with the same value. Note, at present, June 2019, always null.',
    interfund_indicator                           STRING             COMMENT 'If Transaction Type is N, set to F. If Transaction Type is Y, set to A for intrafund transactions or T for interfund transactions',
    org_code                                      STRING             COMMENT 'New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code. AKA, onefund organization.',
    pop_end                                       DATE             COMMENT 'Period of performance end date. Set to the last day of the month corresponding to the Service Month/Year.',
    pop_start                                     DATE             COMMENT 'Period of performance start date. Set to the first day of the month corresponding to the Service Month/Year',
    program_code                                  STRING             COMMENT 'Budget Act Code or Onefund Program: B3->IT23 F1->AA20 F2->AA10 FL->IT31 FQ->IT14 FR->GS14 P1->AA20 P2->AA10.',
    quantity                                      INT             COMMENT 'Task order quantity.',
    reference_doc_line_num                        STRING             COMMENT 'Formerly known as the MDL or CUST_MDL.',
    reference_doc_num                             STRING             COMMENT 'Formerly know as the CUST_ACT or CUST_ACT_NUM.',
    reference_doc_type                            STRING,
    region_division                               STRING             COMMENT 'Region or organization_id.',
    related_statement_num                         STRING             COMMENT 'Identifies the related Pegasys statement number for credits and rebills. At present, June 2019, always null.',
    revenue_source                                STRING             COMMENT 'NBA1 if the Program Code is AA10 or IT31, RBA1 if the Program Code is AA20, GS13, IT14, or IT23 (i.e. NBA1 for NBA, RBA1 for RBA).',
    statement_num                                 STRING             COMMENT 'Alphanumeric unique identifier for grouping of bills. Set depending on the budget act code by a call to BILLING.GET_STATEMENT_NUM (p_budg_act_code VARCHAR2).',
    system_type                                   STRING,
    title                                         STRING             COMMENT 'RBA Funding Document-IAA Number NBA-OMIS Funding Doc-IA.',
    tysm_agency_identifier                        STRING             COMMENT 'Customer Treasury Symbol: Agency Identifier. Formerly known as the Customer Agency Id (Cust_AID) or Requesting_Agency_Id.',
    tysm_allocation_trans_agency                  STRING             COMMENT 'Customer Treasury Symbol: Allocation Transfer Agency',
    tysm_avalability_type                         STRING             COMMENT 'Customer Treasury Symbol: Availability Type. Formerly known as the Cust_Avail_Type or Loa_Availability_Type.',
    tysm_begin_period                             STRING             COMMENT 'Customer Treasury Symbol: Beginning Period of Availability. Formerly known as the Cust_BPOA or LOA_Period_First.',
    tysm_end_period                               STRING             COMMENT 'Customer Treasury Symbol: Ending Period of Availability. Formerly known as the Cust_EPOA or LOA_Period_Last.',
    tysm_main_acct                                STRING             COMMENT 'Customer Treasury Symbol: Main Account. Formerly know as the Cust_Main_Account or the Appropriation_LOA.',
    tysm_sub_acct                                 STRING             COMMENT 'This will be a new field on the Funding Citation. Customer Treasury Symbol: Sub Account. Formerly known as the Cust_Sub_Account or Treasury_Subaccount.',
    unit_of_issue                                 STRING             COMMENT 'Default to EA for Each. Presently, June 2019, EA is the only distinct value stored in the db for this column.',
    vendor_customer_code                          STRING             COMMENT 'Formerly known as BOAC2.',
    version_num                                   STRING             COMMENT 'The version used to generate the flat file.',
    billing_id                                    BIGINT             COMMENT '[FK] Foreign key to BILLING.BILLING.BILLING_ID.',
    loa_subtask                                   STRING             COMMENT 'Unique Identifier for a combination of Line Item and an LOA that funds it',
    ui_description                                STRING             COMMENT 'Detailed, Formatted description that displays in the Transmit module UI',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    uei_plus4                                     STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    ginv_ref_performance_num                      STRING             COMMENT 'G-Invoicing Ref Performance Number (ginv_ref_performance_num)',
    ginv_ref_performance_num_detail               STRING             COMMENT 'G-Invoicing Ref Performance Number Detail (ginv_ref_performance_num_detail)',
    vuei                                          STRING,
    vuei_plus4                                    STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Billing Summary of External Billing transmissions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'billing_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_billing_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_inv_detail
-- Source : aasbs_transmit.inv_detail
-- Comment: Billing Invoice Details
-- Columns: 18 source + 6 audit = 24 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_inv_detail
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.INV_DETAIL_SEQ',
    inv_summary_id                                BIGINT             COMMENT '[FK] Foreign Key to table AASBS_TRANSMIT.INV_SUMMARY',
    act_number                                    STRING             COMMENT 'ACT Number from the award document purchase order form',
    delivery_dt                                   TIMESTAMP             COMMENT 'Task item delivery date.',
    description                                   STRING             COMMENT 'Populated with INVOICE_ITEM.DESCRIPTION',
    invoice_number                                STRING             COMMENT 'Populated with INVOICE.INVOICE_NUM',
    item_num                                      STRING             COMMENT 'Populated with INVOICE_ITEM.ITEM_NUM',
    quantity                                      BIGINT             COMMENT 'Populated with INVOICE_ITEM.QUANTITY',
    service_period_beginning                      TIMESTAMP             COMMENT 'Populated with INVOICE_ITEM.SERVICE_PERIOD_BEGINNING',
    service_period_ending                         TIMESTAMP             COMMENT 'INVOICE_ITEM.SERVICE_PERIOD_ENDING',
    shipment_dt                                   TIMESTAMP             COMMENT 'Populated with INVOICE_ITEM.SHIPMENT_DATE',
    total_detail_amount                           DECIMAL(15,2)             COMMENT 'Sum of all task items invoice amount.',
    unit_of_issue                                 STRING             COMMENT 'Populated with INVOICE_ITEM.UNIT_OF_ISSUE',
    unit_price                                    DECIMAL(14,2)             COMMENT 'Populated with INVOICE_ITEM.UNIT_PRICE',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Billing Invoice Details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'inv_detail',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_inv_detail',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_inv_summary
-- Source : aasbs_transmit.inv_summary
-- Comment: Invoice Summary
-- Columns: 44 source + 6 audit = 50 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_inv_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary Key. Set by sequence AASBS_TRANSMIT.INV_SUMMARY_SEQ.',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign Key to AASBS_TRANSMIT.TRANSMITTAL.ID.',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to LU_transmittal_STATUS.',
    invoice_id                                    BIGINT             COMMENT '[FK] Foreign Key to AASBS.INVOICE',
    aba_routing_num                               STRING             COMMENT 'ABA Routing Number',
    account_num                                   STRING             COMMENT 'Account Number',
    account_type                                  STRING             COMMENT 'Account Type',
    act_number                                    STRING             COMMENT 'Populated with INVOICE.ACT_NUM',
    company_name                                  STRING             COMMENT 'Company Name. Populated with INVOICE_DETAILS. CONTRACTING_ORG_ID or INVOICE_DETAILS. REMIT_COMPANY',
    companys_phone_num                            STRING             COMMENT 'Company Phone Number',
    contact_name                                  STRING             COMMENT 'Contact Name',
    contract_num                                  STRING             COMMENT 'Contract Number. Populated with INVOICE_DETAILS.CONTRACT_NUM',
    discount_days_due                             INT             COMMENT 'Discount Days Due. Populated with INVOICE. DISCOUNT_DAYS_DUE',
    discount_percent                              DECIMAL(6,3)             COMMENT 'Discount Percentage. Populated with INVOICE. DISCOUNT_PERCENTAGE',
    invoice_dt                                    TIMESTAMP             COMMENT 'Invoice Date',
    invoice_generation_dt                         TIMESTAMP             COMMENT 'Date the Invoice was generated',
    invoice_receipt_dt                            TIMESTAMP             COMMENT 'Invoice Receipt Date. Populated with INVOICE.INVOICE_DATE',
    invoice_transmitted_dt                        TIMESTAMP             COMMENT 'Date the Invoice was transmitted',
    invoice_number                                STRING             COMMENT 'Populated with INVOICE. INVOICE_NUM',
    misc_charges_and_credits                      DECIMAL(14,2)             COMMENT 'Misc. Charges and Credits',
    net_days                                      INT             COMMENT 'Net Days',
    number_of_detail_lines                        BIGINT             COMMENT 'Number of Detail Lines',
    pegasys_doc_num                               STRING             COMMENT 'Pegasys Document Number',
    purchase_order_number                         STRING             COMMENT 'Purchase Order Number / PIID',
    rem_vendor_address1                           STRING             COMMENT 'REMITTANCE VENDOR LINE 1. Per INV FF spec, optional second line for remittance vendor name (despite poor name)',
    rem_vendor_address2                           STRING             COMMENT 'Remittance Vendor Address2. Per INV FF spec, manditory address line. Populated with INVOICE_DETAILS.REMIT_STREET1',
    rem_vendor_address3                           STRING             COMMENT 'Remittance Vendor Address3. Per INV FF spec, optional address line. Populated with INVOICE_DETAILS.REMIT_STREET2',
    rem_vendor_city                               STRING             COMMENT 'Remittance Vendor City. Populated with INVOICE_DETAILS.REMIT_CITY',
    rem_vendor_name                               STRING             COMMENT 'Remittance Vendor Name.',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    rem_vendor_state                              STRING             COMMENT 'Remittance Vendor State. Populated with INVOICE_DETAILS.REMIT_STATE',
    rem_vendor_zip_code                           STRING             COMMENT 'Remittance Vendor Zip Code. Populated with INVOICE_DETAILS.REMIT_ZIP',
    shipping_amount                               DECIMAL(14,2)             COMMENT 'Shipping Amount',
    system                                        STRING             COMMENT 'To Be determined',
    tax_amount                                    DECIMAL(14,2)             COMMENT 'Tax Amount',
    tax_id_num                                    STRING             COMMENT 'Tax Identification Number',
    tax_id_qualifier                              STRING             COMMENT 'Tax Identificaiton Qualifier',
    total_invoice_amount                          DECIMAL(14,2)             COMMENT 'Total Invoice Amount. Populated with Sum of all INVOICE_ITEM.INVOICE_ITEM_AMT',
    activity_address_cd                           STRING             COMMENT 'Region or National program creator. Foreign Key to table AASBS_TRANSMIT.LU_ACTIVITY_ADDRESS',
    specification_version                         STRING             COMMENT 'The version used to generate the Transmittal.',
    description                                   STRING             COMMENT 'The order description.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Invoice Summary'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'inv_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_inv_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_record_type
-- Source : aasbs_transmit.lu_transmittal_record_type
-- Comment: Lookup table - All Transmittal Record Types
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_record_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Transmittal Record Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'lu_transmittal_record_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_lu_transmittal_record_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_stage
-- Source : aasbs_transmit.lu_transmittal_stage
-- Comment: Lookup Table - Stages of releasing a Transmision to Pegasys
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_stage
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup Table - Stages of releasing a Transmision to Pegasys'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'lu_transmittal_stage',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_lu_transmittal_stage',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_status
-- Source : aasbs_transmit.lu_transmittal_status
-- Comment: Lookup table - All Transmission Statuses
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_status
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Transmission Statuses'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'lu_transmittal_status',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_lu_transmittal_status',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_type
-- Source : aasbs_transmit.lu_transmittal_type
-- Comment: Lookup table - All Transmittal Types
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_lu_transmittal_type
(
    cd                                            STRING NOT NULL    COMMENT '[PK] Alphanumeric Primary Key for this table',
    description                                   STRING             COMMENT 'Description of the lookup value',
    flatfile_cd                                   STRING             COMMENT '2 char code sent as a transmittal type identifier to pegasys',
    active_yn                                     STRING             COMMENT 'Flag indicating whether a record is active (=Y) or not (=N)',
    sort_order                                    BIGINT             COMMENT 'Sort order used for UI Display',
    created_by_user_name                          STRING             COMMENT 'Oracle Login Username who created this database table record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_name                          STRING             COMMENT 'Oracle Login Username who most recently updated this database table record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (cd, updated_dt)
COMMENT 'Lookup table - All Transmittal Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'lu_transmittal_type',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_lu_transmittal_type',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_po_accounting
-- Source : aasbs_transmit.po_accounting
-- Comment: Purchase Order Accounting
-- Columns: 22 source + 6 audit = 28 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_po_accounting
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS_TRANSMIT.PO_ACCOUNTING_SEQ',
    po_summary_id                                 BIGINT             COMMENT '[FK] Foreign key to AASBS.PO_SUMMARY.',
    appropriation_code                            STRING             COMMENT 'The order appropriation code.',
    budget_activity_code                          STRING             COMMENT 'After OneFund activation: OneFund PROGRAM code. Before OneFund activation: BUDGET_ACT_CODE.',
    ss_cost_ctr_a_work_authoriz                   STRING             COMMENT 'The cost center A.',
    ss_cost_ctr_b_building_number                 STRING             COMMENT 'The cost center B.',
    cost_element                                  STRING             COMMENT 'The order cost element code.',
    ss_craft_code_plant_number                    STRING             COMMENT 'The craft code.',
    ss_federal_indicator                          STRING             COMMENT 'The federal indicator.',
    function_code                                 STRING             COMMENT 'After OneFund activation: OneFund ACTIVITY code. Before OneFund activation: FUNCTION_CODE.',
    mdl                                           STRING             COMMENT 'The multiple distribution lines for GSA clients.',
    mdl_amt                                       DECIMAL(15,2)             COMMENT 'The amount on the MDL.',
    object_class                                  STRING             COMMENT 'The order object class.',
    organization_code                             STRING             COMMENT 'The order organization code.',
    ss_project_number                             STRING             COMMENT 'The project prospectus number.',
    ss_document_number                            STRING             COMMENT 'The SS document number.',
    ss_labor_hours                                STRING             COMMENT 'The SS labor hours.',
    ss_work_item_location_code                    STRING             COMMENT 'The work item.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Purchase Order Accounting'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'po_accounting',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_po_accounting',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_po_line
-- Source : aasbs_transmit.po_line
-- Comment: Purchase Order Line Item
-- Columns: 11 source + 6 audit = 17 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_po_line
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS_TRANSMIT.PO_LINE_SEQ.',
    po_summary_id                                 BIGINT             COMMENT '[FK] Foreign key to AASBS_TRANSMIT.PO_SUMMARY.',
    description_stock_number                      STRING             COMMENT 'Description Stock Number',
    quantity                                      BIGINT             COMMENT 'Quantity of items (units)',
    total_line_amt                                DECIMAL(15,2)             COMMENT 'Total_Line_Amount = Quantity * Unit_Price',
    unit_of_issue                                 STRING             COMMENT 'Units of Measure for this',
    unit_price                                    DECIMAL(14,4)             COMMENT 'Line Item Unit Price',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Purchase Order Line Item'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'po_line',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_po_line',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_po_summary
-- Source : aasbs_transmit.po_summary
-- Comment: Purchase Order Summary
-- Columns: 65 source + 6 audit = 71 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_po_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.PO_SUMMARY_SEQ',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign Key to AASBS_TRANSMIT.TRANSMITTAL',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to AASBS_TRANSMIT.LU_transmittal_STATUS.',
    activity_address_cd                           STRING             COMMENT 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE',
    acceptance_terms                              DECIMAL(15,2)             COMMENT 'The acceptance terms and conditions.',
    number_of_accounting_lines                    BIGINT             COMMENT 'The number of accounting lines in NEAR_PO_ACCOUNTINGS.',
    act_number                                    STRING             COMMENT 'The order ACT number.',
    budget_fy                                     INT             COMMENT 'Budget fiscal year',
    generated_dt                                  TIMESTAMP             COMMENT 'Date Purchase Order summary was generated',
    billing_office_city                           STRING             COMMENT 'The city line of address.',
    billing_office_name                           STRING             COMMENT 'The name of the billing office.',
    billing_office_state                          STRING             COMMENT 'The state line of address.',
    billing_office_street_1                       STRING             COMMENT 'The first line in address.',
    billing_office_street_2                       STRING             COMMENT 'The second line in address.',
    billing_office_zip                            STRING             COMMENT 'The zipcode of address. The first 5 numbers are required.',
    contract_number                               STRING             COMMENT 'The contract number.',
    contract_officer_name                         STRING             COMMENT 'The name of the Contract Officer.',
    contract_officer_phone                        STRING             COMMENT 'The phone number of Contract Officer.',
    dba_indicator                                 STRING             COMMENT 'The DBA indicator.',
    number_of_detail_lines                        BIGINT             COMMENT 'The number of detail lines in NEAR_PO_DETAILS.',
    discount_days                                 BIGINT             COMMENT 'The days to receive discount.',
    discount_percent                              DECIMAL(6,3)             COMMENT 'The discount percentage offered for prompt payment.',
    duns_number                                   STRING             COMMENT 'The industry partner DUNS number.',
    fob_shipment_method                           STRING             COMMENT 'The shipment method.',
    ipartner_city                                 STRING             COMMENT 'The city line of address.',
    tax_id_number                                 BIGINT             COMMENT 'The tax ID number.',
    ipartner_name                                 STRING             COMMENT 'The name of company or organization.',
    ipartner_name_2                               STRING             COMMENT 'The overflow of company name.',
    ipartner_phone                                STRING             COMMENT 'The phone number of company.',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',    
    ipartner_state                                STRING             COMMENT 'The state line of address.',
    ipartner_street_1                             STRING             COMMENT 'The first line in address.',
    ipartner_street_2                             STRING             COMMENT 'The second line in address.',
    ipartner_zip                                  STRING             COMMENT 'The zipcode line of address. The first 5 numbers are required.',
    mod_number                                    STRING             COMMENT 'The modification number.',
    net_days                                      BIGINT             COMMENT 'The net days to pay invoice.',
    nish_nib_ind                                  STRING             COMMENT 'The NISH NIB indicator.',
    order_dt                                      TIMESTAMP             COMMENT 'The calendar date that denotes the date of order.',
    purchase_order_number                         STRING             COMMENT 'The order number/order PIID.',
    po_amount                                     DECIMAL(15,2)             COMMENT 'The total value of purchase order.',
    prepayment_authorized_flag                    STRING             COMMENT 'The identifier that indicates whether prepayment is authorized.',
    recv_office_name                              STRING             COMMENT 'The Client receiving office name.',
    recv_office_phone                             STRING             COMMENT 'The Client receiving office phone number.',
    rem_vendor_city                               STRING             COMMENT 'The city line of remittance address.',
    rem_vendor_name                               STRING             COMMENT 'The name of company or organization.',
    rem_vendor_name_2                             STRING             COMMENT 'The overflow for vendor name.',
    rem_vendor_state                              STRING             COMMENT 'The state line of remittance address.',
    rem_vendor_address_1                          STRING             COMMENT 'REMITTANCE VENDOR LINE 1. Per INV FF spec, optional second line for remittance vendor name',
    rem_vendor_address_2                          STRING             COMMENT 'The first line of remittance address.',
    rem_vendor_address_3                          STRING             COMMENT 'The second line of remittance address.',
    rem_vendor_zip_code                           STRING             COMMENT 'The zipcode line of remittance address.',
    rem_vendor_number                             BIGINT             COMMENT 'Pegasys vendor number',
    business_classification_code                  STRING             COMMENT 'The small business classification.',
    tax_id_qualifier                              STRING             COMMENT 'The tax ID qualifier.',
    type_of_modification_code_ind                 STRING             COMMENT 'The type of modification.',
    version_num                                   STRING             COMMENT 'The version used to generate the Transmittal.',
    description                                   STRING             COMMENT 'The order description.',
    award_mod_id                                  BIGINT             COMMENT '[FK] Foreign Key to AASBS.AWARD_MOD',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    vendor_code                                   STRING             COMMENT 'Vendor Code of the Contractor to which the PO is awarded',
    vendor_addr_code                              STRING             COMMENT 'Vendor Address Code of the Contractor to which the PO is awarded',
    vuei                                          STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Purchase Order Summary'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'po_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_po_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_rr_detail
-- Source : aasbs_transmit.rr_detail
-- Comment: Receiving Report details
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_rr_detail
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS_TRANSMIT.RR_DETAIL_SEQ.',
    rr_summary_id                                 BIGINT             COMMENT '[FK] Foreign key to AASBS_TRANSMIT.RR_SUMMARY.ID.',
    deduction_amt                                 DECIMAL(15,2)             COMMENT 'The amount deducted from line total amount.',
    description_stock_num                         STRING             COMMENT 'The text description and ID number of item.',
    mdl                                           STRING             COMMENT 'The GSA client MDL.',
    quantity                                      DECIMAL(10,2)             COMMENT 'The quantity ordered.',
    total_mdl_amount                              DECIMAL(15,2)             COMMENT 'The sum of quantity * unit price.',
    unit_price                                    DECIMAL(15,4)             COMMENT 'The unit price of the item.',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Receiving Report details'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'rr_detail',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_rr_detail',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_rr_summary
-- Source : aasbs_transmit.rr_summary
-- Comment: Receving Report Summary
-- Columns: 34 source + 6 audit = 40 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_rr_summary
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS_TRANSMIT.RR_SUMMARY_SEQ.',
    transmittal_id                                BIGINT             COMMENT '[FK] Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS.',
    accepted_dt                                   TIMESTAMP             COMMENT 'The date the items were accepted.',
    act_number                                    STRING             COMMENT 'The order ACT number.',
    amendment_ind                                 STRING             COMMENT 'The identifier that indicates if amendment.',
    contract_number                               STRING             COMMENT 'The order contract number.',
    deduction_amt                                 DECIMAL(15,2)             COMMENT 'The deductions for non-performance/non-receipt.',
    description                                   STRING             COMMENT 'The order description.',
    number_of_detail_lines                        BIGINT             COMMENT 'The number of detail lines.',
    invoice_dt                                    TIMESTAMP             COMMENT 'The date on invoice.',
    pegasys_invoice_number                        STRING             COMMENT 'The pegasys version of the invoice number for receiving report.',
    acceptance_id                                 BIGINT             COMMENT '[FK] Foreign Key to table AASBS.ACCEPTANCE',
    invoice_id                                    BIGINT             COMMENT '[FK] Foreign Key to table AASBS.INVOICE',
    max_approved_amt                              DECIMAL(15,2)             COMMENT 'The maximum amount approved for payment.',
    max_payment_amt                               DECIMAL(15,2)             COMMENT 'The maximum payment amount.',
    purchase_order_number                         STRING             COMMENT 'The Purchase Order number/piid.',
    received_dt                                   TIMESTAMP             COMMENT 'The date the items were received.',
    recv_office_symbol                            STRING             COMMENT 'The office symbol of receiving person.',
    recv_person_name                              STRING             COMMENT 'The name of person signing receiving report.',
    recv_person_phone                             STRING             COMMENT 'The phone number of receiving person.',
    rem_vendor_name                               STRING             COMMENT 'The remittance vendor\'s name.',
    rem_office_symbol_2                           STRING             COMMENT 'The office symbol of second receiver.',
    second_recv_name                              STRING             COMMENT 'The name of the second certifier, if required.',
    second_recv_phone                             STRING             COMMENT 'The phone number of second receiver.',
    type_of_delivery                              STRING             COMMENT 'The type of delivery.',
    activity_address_cd                           STRING             COMMENT 'Region or National program creator. Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS',
    version_num                                   STRING             COMMENT 'The version used to generate the flat file.',
    generated_dt                                  TIMESTAMP             COMMENT 'Date the Summary Report was Generated',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
    ui_description                                STRING             COMMENT 'Description of the Summary Report to be displayed to the User',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Receving Report Summary'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'rr_summary',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_rr_summary',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmit_error
-- Source : aasbs_transmit.transmit_error
-- Comment: Log of Errors generatred while transmitting files to external systems (e.g., Pegasys)
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmit_error
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SEQ',
    transmittal_status_cd                         STRING             COMMENT 'Foreign Key to table AASBS.AWARD_MOD',
    transmittal_stage_cd                          STRING             COMMENT 'Foreign Key to table AASBS.AWARD_MOD',
    transmittal_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS.AWARD_MOD',
    source_table_name_cd                          STRING             COMMENT 'Foreign Key to table AASBS.AWARD_MOD',
    source_record_id                              BIGINT             COMMENT '[FK] Primary key ID of the record "SOURCE_TABLE_NAME_CD" table assoicated with this error',
    retry_cnt                                     INT             COMMENT 'Number of times retransmission was attempted',
    error_description                             STRING             COMMENT 'Error description',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Log of Errors generatred while transmitting files to external systems (e.g., Pegasys)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmit_error',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmit_error',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal
-- Source : aasbs_transmit.transmittal
-- Comment: Transmittal / Flat File parent table
-- Columns: 14 source + 6 audit = 20 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SEQ',
    transmittal_name                              STRING             COMMENT 'Name of Transmittal / Flat File',
    transmittal_type_cd                           STRING             COMMENT 'Foreign Key to aasbs_transmit.LU_TRANSMITTAL_TYPE',
    batch_id                                      STRING             COMMENT '[FK] Id specified in batch header of flat file',
    sent_dt                                       TIMESTAMP             COMMENT 'Date Transmittal was sent',
    batch_dt                                      DATE             COMMENT 'Date Only version of sent_dt used for getting batch sequence',
    batch_sequence                                BIGINT             COMMENT 'Unique by sent_dt, 3 digit sequence number used for file naming',
    release_count                                 BIGINT             COMMENT 'The number of main/summary records (i.e. po_summary, inv_summary) included in the batch',
    transmit_return_msg                           STRING             COMMENT 'Returned message',
    transmit_status                               STRING             COMMENT 'Status for transmitted batch. Values: ERROR, CONNECTION_ERROR',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Transmittal / Flat File parent table'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal_envelope
-- Source : aasbs_transmit.transmittal_envelope
-- Comment: Contains Transmittal Envelope records
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal_envelope
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_ENVELOPE_SEQ',
    envelope_version                              STRING             COMMENT 'Transmittal Envelope Version',
    file_header_indicator                         STRING             COMMENT 'Transmittal file header indicator',
    file_trailer_indicator                        STRING             COMMENT 'Transmittal file trailer indicator',
    batch_header_indicator                        STRING             COMMENT 'Transmittal batch header indicator',
    batch_trailer_indicator                       STRING             COMMENT 'Transmittal batch trailer indicator',
    file_poc_id                                   BIGINT             COMMENT '[FK] Foreign key to TRANSMITTAL_PO',
    transmittal_prefix                            STRING             COMMENT 'Envelope Prefix - FEDS',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Transmittal Envelope records'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal_envelope',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal_envelope',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal_poc
-- Source : aasbs_transmit.transmittal_poc
-- Comment: Contains Transmittal Envelope records
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal_poc
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_POC_SEQ',
    poc_name                                      STRING             COMMENT 'Point of Contact name',
    poc_phone                                     STRING             COMMENT 'Point of Contact phone number',
    poc_email                                     STRING             COMMENT 'Point of Contact email address',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Contains Transmittal Envelope records'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal_poc',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal_poc',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal_spec_version
-- Source : aasbs_transmit.transmittal_spec_version
-- Comment: Transmittal / provides the current specification version per transmittal type.
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal_spec_version
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SPEC_VERSION_SEQ',
    transmittal_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE',
    specification_version                         STRING             COMMENT 'Version of Transmittal Specification',
    transmit_file_prefix                          STRING             COMMENT 'Transmit File Prefix',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Transmittal / provides the current specification version per transmittal type.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal_spec_version',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal_spec_version',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal_specification
-- Source : aasbs_transmit.transmittal_specification
-- Comment: Billing Transmittal Specification
-- Columns: 16 source + 6 audit = 22 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal_specification
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SPECIFICATION_SEQ',
    specification_version                         STRING             COMMENT 'AAC envelope version',
    transmittal_type_cd                           STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE - Transmittal (batch) type',
    transmittal_record_type_cd                    STRING             COMMENT 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_RECORD_TYPE - Transmittal Record Type specifier',
    begin_position                                BIGINT             COMMENT 'Starting column within a flat file of a field of data',
    end_position                                  BIGINT             COMMENT 'Ending column within a flat file of a field of data',
    field_length                                  BIGINT             COMMENT 'Length of the field of data',
    pad_direction                                 STRING             COMMENT 'Left justify vs right justify indicator',
    pad_char                                      STRING             COMMENT 'Padding character that fills extra space in the field of data',
    format_mask                                   STRING             COMMENT 'Format mask for formatting dates, currency, and zip codes',
    source_table_name                             STRING             COMMENT 'Name of table from which the data is pulled into this field',
    source_column_name                            STRING             COMMENT 'Name of column within "source_table_name" from which the data is pulled into this field',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Billing Transmittal Specification'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal_specification',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal_specification',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_transmit_transmittal_suspension
-- Source : aasbs_transmit.transmittal_suspension
-- Comment: Indicates suspension of transmission of specific Transmittal Types
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_transmit_transmittal_suspension
(
    id                                            BIGINT NOT NULL    COMMENT '[PK] Primary key. Set by sequence AASBS_TRANSMIT.TRANSMITTAL_SUSPENSION_SEQ.',
    transmittal_type_cd                           STRING             COMMENT 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE',
    suspend_start_dt                              TIMESTAMP             COMMENT 'Date to stop transmitting',
    suspend_end_dt                                TIMESTAMP             COMMENT 'Date to resume transmitting',
    created_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that created this record',
    created_dt                                    TIMESTAMP             COMMENT 'Date this database record was created',
    updated_by_user_id                            STRING             COMMENT '[FK] Foreign Key to table ASSIST.USERS - Application user that most recently updated this record',
    updated_dt                                    TIMESTAMP             COMMENT 'Most recent date this database record was updated',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Indicates suspension of transmission of specific Transmittal Types'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_transmit',
    'pipeline.source_table'               = 'transmittal_suspension',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_transmit_transmittal_suspension',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: accrual  (6 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_batch
-- Source : accrual.accrual_batch
-- Comment: Silver copy of accrual.accrual_batch from ASSIST Postgres source.
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_batch
(
    accrual_batch_id                              BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key for the table',
    accrual_file_id                               BIGINT             COMMENT '[FK] UI Element: None. Foreign key to ACCRUAL_FILE table.',
    batch_header_indicator                        STRING             COMMENT 'UI Element: None. This indicates the line is the batch header. Value: BHDR',
    batch_type                                    STRING             COMMENT 'UI Element: None. Refers to the configured Batch Type. Note the Batch Type value will be validated against FMESB and Pegasys reference tables (BATCH_TYPE and GSA_INCG_BATC_TYP, respectively). Value: RV',
    batch_id                                      STRING             COMMENT '[FK] UI Element: None. Used with Batch Type to determine batch sequence or batch duplication. The format is XXXYDDD### where XXX = Batch ID Prefix, YDDD = Date and ### = Batch Sequence Number. For AAS the batch id prefix is RRV, therefore format is RRV + YDDD + ###',
    batch_date                                    TIMESTAMP             COMMENT 'UI Element: None. Date of batch YYYYMMDD',
    batch_trailer                                 STRING             COMMENT 'UI Element: None. The trailer string for this batch envelope. Format: BTRLRV RRVYDDD###[#lines in 6 digits][$ value of batch with two digit decimal point in 15 digits. For negative values the first digit must be a minus sign].',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_batch_id)
COMMENT 'Silver copy of accrual.accrual_batch from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_batch',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_batch',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_header
-- Source : accrual.accrual_header
-- Comment: Silver copy of accrual.accrual_header from ASSIST Postgres source.
-- Columns: 20 source + 6 audit = 26 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_header
(
    accrual_header_id                             BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key for the table',
    accrual_batch_id                              BIGINT             COMMENT '[FK] UI Element: None. Foreign key to ACCRUAL_BATCH table.',
    batch_type_indicator                          STRING             COMMENT 'UI Element: None. Identifies the Batch Type of the record. Batch Type indicator for AAS Revenue Accruals is RV.',
    information_indicator                         STRING             COMMENT 'UI Element: None. Identifies the line as a Header. Should always be the value 1',
    auto_reverse                                  STRING             COMMENT 'UI Element: None. Indicates that the voucher should be automatically reversed. T or F.',
    correction_flag                               STRING             COMMENT 'UI Element: None. Indicates that the form represents a correction to an existing document. (T, X, F). Values: T (if a correction), X (if a cancellation), or F (if a new form)',
    document_number                               STRING             COMMENT 'UI Element: None. Document Number, which is unique in Pegasys. Format: DTYP + YYYYMMDD + #####. Where DTYP is the value from DOCUMENT_TYPE column, YYYYMMDD = Document date, #####= sequential number within document type and date',
    document_status                               STRING             COMMENT 'UI Element: None. AAS will always use NEW',
    reset_doc_date_flag                           STRING             COMMENT 'UI Element: None. Indicates that original document date should be reset to the date on this form. AAS will always use F.',
    reversed_flag                                 STRING             COMMENT 'UI Element: None. Indicates whether the voucher has been reversed. AAS will always use F.',
    security_organization                         STRING             COMMENT 'UI Element: None. AAS will always use GSA.',
    external_document_number                      STRING             COMMENT 'UI Element: None. Act No/Task Number',
    document_type                                 STRING             COMMENT 'UI Element: None. The document type is based on the Program code from the agreement (citation), as follows: AA10=SDA, AA20=EDA, GS14=KDA, IT14=NDA, IT23=XDA, IT31=HDA',
    system_id                                     STRING             COMMENT '[FK] UI Element: None. ID of the external system in which the transaction originated. Example: RBA',
    accounting_period                             STRING             COMMENT 'UI Element: None. Fiscal month and Fiscal year to which the accrual applies. Example: Accruals for February 2012 would have Accounting Period of 05/2012',
    created_by_user_id                            STRING             COMMENT '[FK]',
    created_dt                                    TIMESTAMP,
    updated_by_user_id                            STRING             COMMENT '[FK]',
    updated_dt                                    TIMESTAMP,
    title                                         STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_header_id, updated_dt)
COMMENT 'Silver copy of accrual.accrual_header from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_header',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_header',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_income_line
-- Source : accrual.accrual_income_line
-- Comment: Silver copy of accrual.accrual_income_line from ASSIST Postgres source.
-- Columns: 3 source + 6 audit = 9 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_income_line
(
    accrual_income_line_id                        BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key for the table',
    accrual_header_id                             BIGINT             COMMENT '[FK] UI Element: None. Foreign key to ACCRUAL_HEADER table.',
    accrual_income_id                             BIGINT             COMMENT '[FK] UI Element: None. Foreign key to ACCRUAL_INCOME table.',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_income_line_id)
COMMENT 'Silver copy of accrual.accrual_income_line from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_income_line',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_income_line',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_response_detail
-- Source : accrual.accrual_response_detail
-- Comment: This table logs the informational reponses from ACCRUAL transmission.  How do we tie this record bac
-- Columns: 5 source + 6 audit = 11 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_response_detail
(
    accrual_response_det_id                       BIGINT NOT NULL    COMMENT '[PK] Primary Key',
    accrual_response_header_id                    BIGINT             COMMENT '[FK] Foreign Key to the ACCRUAL response header',
    accrual_doc_num                               STRING             COMMENT 'Uniquely identifies this bill detail record in the Pegasys Detail ACCRUAL Record table; this will reflect the value from the input file',
    accrual_response_detail                       STRING             COMMENT 'The raw response',
    accepted_status                               STRING             COMMENT 'Valid values ACCEPTED and ERROR, whether or not this record was accepted by Pegasys or rejected with an error',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_response_det_id)
COMMENT 'This table logs the informational reponses from ACCRUAL transmission.  How do we tie this record back to a ACCRUAL entry, though?!'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_response_detail',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_response_detail',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_response_header
-- Source : accrual.accrual_response_header
-- Comment: This table logs the reponse header and trailer from ACCRUAL transmission
-- Columns: 9 source + 6 audit = 15 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_response_header
(
    accrual_response_header_id                    BIGINT NOT NULL    COMMENT '[PK] Primary Key',
    pegasys_load_num                              DECIMAL(19,2)             COMMENT 'There is no Pegasys load num for accrual reposnses.',
    start_date                                    TIMESTAMP,
    accrual_response_header                       STRING             COMMENT 'The raw header value returned from Pegasys.',
    accrual_batch_id                              BIGINT             COMMENT '[FK] Foreign Key back to the ACCRUAL batch header that was sent.',
    response_file_name                            STRING             COMMENT 'Name of the response file on disk',
    accrual_response_trailer                      STRING             COMMENT 'The raw trailer value returned from Pegasys.',
    total_num_of_records                          DECIMAL(19,2)             COMMENT 'The total number of records returned by the trailer',
    end_date                                      TIMESTAMP,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_response_header_id)
COMMENT 'This table logs the reponse header and trailer from ACCRUAL transmission'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_response_header',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_response_header',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_accrual_accrual_response_message
-- Source : accrual.accrual_response_message
-- Comment: This table logs the message rows for each record in the rejected and accepted files
-- Columns: 6 source + 6 audit = 12 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_accrual_accrual_response_message
(
    accrual_response_msg_id                       BIGINT NOT NULL    COMMENT '[PK] Primary Key',
    accrual_response_det_id                       BIGINT             COMMENT '[FK] This is a foreign key to ACCRUAL_RESPONSE_DETAIL to tie this Message to the row that was processed previously',
    accrual_response_msg                          STRING             COMMENT 'The raw error value returned from Pegasys.',
    error_code                                    STRING             COMMENT 'The seven digit error code from Pegasys',
    error_label                                   STRING             COMMENT 'Pegasys Error label',
    error_text                                    STRING             COMMENT 'Pegasys Error text',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (accrual_response_msg_id)
COMMENT 'This table logs the message rows for each record in the rejected and accepted files'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'accrual',
    'pipeline.source_table'               = 'accrual_response_message',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_accrual_accrual_response_message',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: agreement  (3 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_agreement_agreement
-- Source : agreement.agreement
-- Comment: Silver copy of agreement.agreement from ASSIST Postgres source.
-- Columns: 31 source + 6 audit = 37 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_agreement_agreement
(
    agreement_id                                  BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key with sequence',
    agreement_num                                 STRING             COMMENT 'UI Element: AGREEMENT_NUM. Agreement numbers are generated by AAS and must be unique. RBA agreement numbers will begin with X and NBA agreement numbers will begin with Z. Note: different agreement numbers are needed for each combination of Funding Document, BOAC2 and Citation/LOA (reflecting the customer supplied funding).',
    agreement_name                                STRING             COMMENT 'UI Element: Funding Doc Number NBA-OMIS Funding Document-Funding Doc #. Populate with the funding doc number',
    agreement_end_date                            TIMESTAMP             COMMENT 'UI Element: AGREEMENT_END_DATE. Populate with the period of performance end date, citation expiration date, or appropriation expiration date from the funding document. If no specific date is available, then populate with the prepared date from the funding document plus a reasonable interval.',
    active_status                                 STRING             COMMENT 'UI Element: None. For the purposes of this interface this field will always be true (active agreement).',
    funding_status                                STRING             COMMENT 'UI Element: None. Indicates the status of the funding for this agreement. Funds can be marked as ESTIMATED or ACTUAL.',
    document_type                                 STRING             COMMENT 'UI Element: None. The ED document type for AAS is UED.',
    security_org                                  STRING             COMMENT 'UI Element: None. GSA',
    boac_code                                     STRING             COMMENT 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2',
    addr_code                                     STRING             COMMENT 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2',
    title                                         STRING             COMMENT 'UI Element: NBA-OMIS Funding Document-IA. IAA Number',
    obligations_avail_amt                         STRING             COMMENT 'UI Element: None. True',
    commitments_avail_amt                         STRING             COMMENT 'UI Element: None. False',
    agreement_prepared_date                       TIMESTAMP             COMMENT 'UI Element: RBA-Funding Citation Submitted Date  NBA-. If this document is being submitted to establish a new agreement, populate this field with the Prepared Date on the Funding Document. 			If this document is being submitted to update an existing agreement, then populate with the current date. The date of the document being processed. There can be multiple documents processed against an agreement entity. The first agreement document, used to create the agreement, will use the document date to set the Agreement Start Date. The Document Date of subsequent agreement documents will update the Last Agreement Date on the Agreement.',
    acct_period                                   STRING             COMMENT 'UI Element: RBA-Funding Citation Fiscal Year. Populate with the current fiscal year accounting period. MM/YYYY',
    suppress_printing                             STRING             COMMENT 'UI Element: Suppress Printing (checkbox). False',
    funding_source                                STRING             COMMENT 'UI Element: Populate with F or N for Federal or Non-Federal.',
    maximum_agreement_amt                         DECIMAL(38,2)             COMMENT 'UI Element: Maximum Agreement Amount',
    transmission_status                           STRING             COMMENT 'UI Element: None. The success or failure of transmission to BAAR.',
    system_type                                   STRING             COMMENT 'UI Element: None. RBA or NBA.',
    doctype_defined_header_field                  STRING             COMMENT 'UI Element: None. TBD',
    novation_date                                 TIMESTAMP             COMMENT 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with effective date for BOAC2 (vendor) change. 			NOTE: Novation Date, Customer Novation Code and Customer Novation Address Code are ONLY populated when AAS wishes to associate an existing agreement with a different BOAC2 code. Used to update the vendor on an existing Agreement. 			To change the vendor (BOAC2 value) on an existing agreement, these fields (NVT_DT_CH, NVT_VEND_CD and NVT_VEND_ADDR_CD) are populated with the date and new vendor codes. Once processed, new transactions should use the new vendor code.',
    customer_novation_code                        STRING             COMMENT 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with new BOAC2 code only if the vendor on an existing agreement needs to be changed.',
    customer_novation_addr_code                   STRING             COMMENT 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with new BOAC2 code only if the vendor on an existing agreement needs to be changed.',
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    last_updated_by                               STRING,
    last_update_date                              TIMESTAMP,
    reimbursable                                  STRING,
    sub_allocation                                STRING             COMMENT 'LOA embedded data that facilitates DoD monthly reconciliations',
    agency_location_code                          STRING             COMMENT 'Identifying code of the paying office for treasury when an agency is using OPAC billing',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (agreement_id)
COMMENT 'Silver copy of agreement.agreement from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'agreement',
    'pipeline.source_table'               = 'agreement',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_agreement_agreement',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_agreement_agreement_line
-- Source : agreement.agreement_line
-- Comment: Silver copy of agreement.agreement_line from ASSIST Postgres source.
-- Columns: 61 source + 6 audit = 67 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_agreement_agreement_line
(
    agreement_line_id                             BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key with sequence',
    agreement_id                                  BIGINT             COMMENT '[FK] UI Element: None. Foreign key the AGREEMENT table',
    agreement_line_num                            BIGINT             COMMENT 'UI Element: None. This value represents the document line number. Each new ED or ID document can have only one line so this value will always be 1.',
    agreement_line_amt                            DECIMAL(38,2)             COMMENT 'UI Element: AGREEMENT_LINE_AMT. For a new agreement, supply the agreement amount. When changing the line amount on an existing agreement, supply the value the agreement is to be increased or decreased by (delta). The submission of a negative value will cause the supplied (negative) amount to be subtracted from the existing Agreements line¿s balance.',
    agreement_line_state                          STRING             COMMENT 'UI Element: None. Manages the state of an Agreement line (open or closed). Valid values are O for open or C for closed. AAS will always send O for this field.',
    task_order_id                                 STRING             COMMENT '[FK] UI Element: None. This field will be used for the NBA Task ID and will not be populated for RBA agreements. In the DES this field is identified as Doctype Defined Accounting Line Field #6',
    transaction_type                              STRING             COMMENT 'UI Element: None. Populate with 01 for federal and 02 for non-federal.',
    actual_cost_flag                              STRING             COMMENT 'UI Element: None. Establishes actual cost as the basis of billing. Pegasys requires a value here, but it won¿t be used because we¿re not billing.',
    revenue_control                               STRING             COMMENT 'UI Element: None. Refers to the level of an error the system will return if an agreement line is referenced for a total receivables line that exceeds the total spending amount. Use N (none) for all AAS agreements.',
    spending_control                              STRING             COMMENT 'UI Element: None. Spending control errors are off for AAS Agreements.',
    agreement_charge_flag                         STRING             COMMENT 'UI Element: None. True.',
    prior_year_adjustment                         STRING             COMMENT 'UI Element: None. Indicates whether the line is a prior year adjustment. AAS will use the Not a Prior Year Adjustment (X) value.',
    customer_bbfy                                 STRING             COMMENT 'UI Element: None. agreement start date four digit year',
    customer_fund                                 STRING             COMMENT 'UI Element: None. Customer fund code from obligating document (i.e., 192X, 455F, etc.).',
    referenced_doc_num                            STRING             COMMENT 'UI Element: None. This is the Pegasys document number of the referenced, obligating document. 			This field is not used since PCAS will not be generating NV documents for AAS. However, AAS can supply the 			obligating document number for informational purposes. If a document number is supplied, the document must 			actually exist in Pegasys. This field is used in conjunction with Referenced Document Type and Referenced Accounting Line Number.',
    referenced_doc_type                           STRING             COMMENT 'UI Element: None. Pegasys document type of referenced obligating document.',
    partial_final_flag                            STRING             COMMENT 'UI Element: None. Always set to ¿P¿ for partial. Setting this flag to final indicates that the document is the last in the chain and liquidates the chains funding.',
    customer_vendor_code                          STRING             COMMENT 'UI Element: NBA-OMIS Funding Document-BOAC2. BOAC2 code of the customer.',
    customer_vendor_address_code                  STRING             COMMENT 'UI Element: NBA-OMIS Funding Document-BOAC2. BOAC2 code of the customer.',
    novation_date                                 TIMESTAMP             COMMENT 'UI Element: None. The effective date for Customer vendor code change.',
    novation_vendor_code                          STRING             COMMENT 'UI Element: NBA-OMIS Funding Document-BOAC2. The new Customer vendor code (BOAC2).',
    novation_vendor_address_code                  STRING             COMMENT 'UI Element: NBA-OMIS Funding Document-BOAC2. The new Customer vendor code (BOAC2).',
    bbfy                                          STRING             COMMENT 'UI Element: None. agreement start date four digit year',
    fund                                          STRING             COMMENT 'UI Element: None. 285F?!',
    region_division                               STRING             COMMENT 'UI Element: None. Two-digit Region Code. Valid values are 00 through 11.',
    org_code                                      STRING             COMMENT 'UI Element: None. New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code.',
    program_code                                  STRING             COMMENT 'UI Element: None. New Program Code, which can be derived from the old two-character Budget Activity Code',
    activity_code                                 STRING             COMMENT 'UI Element: None. New Activity Code, which can be derived from the old three-character Function Code',
    revenue_source                                STRING             COMMENT 'UI Element: None. NBA1 if the Program Code is AA10 or IT31.  RBA1 if the Program Code is AA20, GS13, IT14, or IT23',
    billing_start_date                            TIMESTAMP             COMMENT 'UI Element: RBA-Funding Citation Pop start Date NBA-OMIS Funding Document-POP Start. Populate with Agreement start date.',
    billing_end_date                              TIMESTAMP             COMMENT 'UI Element: RBA-Funding Citation pop end Date NBA-OMIS Funding Document-POP END. Populate with the agreement end date plus 5 years. For no-year funds, populate with agreement end date plus 30 years.',
    bill_type                                     STRING             COMMENT 'UI Element: None. Set to MANUAL to prevent the generation of bills by PCAS.',
    bill_print                                    STRING             COMMENT 'UI Element: None. set to value: NO',
    billing_control                               STRING             COMMENT 'UI Element: None. set to: N',
    bill_cycle                                    STRING             COMMENT 'UI Element: None. Set to: AT COMPLETION. Determines how frequently the billing process should consider billing the customer on this agreement. This field is not used for AAS agreements but must be populated with a valid value.',
    cust_long_line_of_acct                        STRING             COMMENT 'UI Element: RBA Funding Citation Customer long line of accounting  NBA-OMIS Funding Document-Accounting Data. Populate with the cust long line of accounting.',
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    last_updated_by                               STRING,
    last_update_date                              TIMESTAMP,
    referenced_line_num                           INT,
    arts_srvs_dscr                                STRING             COMMENT 'UI Element: RBA-Funding Citation, Citation Code, populate with funding_citations.fund_cite_code. NBA-OMIS-Funding Summary, Accounting Data. Populate with MIPRS.ACCT_DATA. Do not populate for Agency 047.',
    tsym_agency_identifier                        STRING             COMMENT 'UI Element: RBA-Funding Citation, Requesting Agency, populate with funding_citations.requesting_agency_id. NBA-OMIS-Funding Summary, Request Agy Code, populate with MIPRS.REQUEST_AGENCY. Do not populate for UIDs',
    tsym_begin_period                             STRING             COMMENT 'UI Element: RBA-Funding Citation, First FY Available, populate with funding_citations.loa_period_first. NBA-OMIS-Funding Summary, Available FY From, populate with MIPRS.FY_FIRST_AVAIL. Do not populate for UIDs',
    tsym_end_period                               STRING             COMMENT 'UI Element: RBA-Funding Citation, Last FY Available, populate with funding_citations.loa_period_last. NBA-OMIS-Funding Summary, Available FY To, populate with MIPRS.FY_LAST_AVAIL. Do not populate for UIDs',
    tsym_availability_type                        STRING,
    tsym_main_acct                                STRING             COMMENT 'UI Element: RBA-Funding Citation, Appropriation, populate with funding_citations.appropriation_loa. NBA-OMIS-Funding Summary, Appropriation, populate with MIPRS.APPROPRIATION. Do not populate for UIDs',
    tsym_sub_acct                                 STRING             COMMENT 'UI Element: RBA-Funding Citation, Treasury Sub Account, populate with funding_citations.treasury_subaccount. NBA-OMIS-Funding Summary, Sub-Acct, populate with MIPRS.TREASURY_SUB_ACCOUNT.',
    tsym_allocation_trans_agency                  STRING,
    tsym_sublevel_prefix                          STRING             COMMENT 'UI Element: RBA-Funding Citation, TSYM Sublevel Prefix. NBA-OMIS Funding TSYM Sublevel Prefix',
    trfr_aid                                      STRING             COMMENT 'Transfer Agency Identifier',
    trfr_ata                                      STRING             COMMENT 'Transfer Allocation Transfer Agency',
    trfr_aval_typ                                 STRING             COMMENT 'Transfer Availability Type',
    trfr_bpoa                                     STRING             COMMENT 'Transfer Beginning Period of Availability',
    trfr_epoa                                     STRING             COMMENT 'Transfer Ending Period of Availability',
    trfr_main_acct                                STRING             COMMENT 'Transfer Main Account',
    trfr_sub_acct                                 STRING             COMMENT 'Transfer Sub Account',
    ginv_order_line_num                           INT             COMMENT 'ginv_order_line_num From G-INVOICING System',
    ginv_order_num                                STRING             COMMENT 'ginv_order_num From G-INVOICING System',
    ginv_order_sched_num                          INT             COMMENT 'ginv_order_sched_num From G-INVOICING System',
    ginv_rqst_svcg_type                           STRING             COMMENT 'ginv_rqst_svcg_type From G-INVOICING System',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (agreement_line_id)
COMMENT 'Silver copy of agreement.agreement_line from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'agreement',
    'pipeline.source_table'               = 'agreement_line',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_agreement_agreement_line',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_agreement_agreement_log
-- Source : agreement.agreement_log
-- Comment: Silver copy of agreement.agreement_log from ASSIST Postgres source.
-- Columns: 12 source + 6 audit = 18 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_agreement_agreement_log
(
    agreement_log_id                              BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key with sequence',
    document_num                                  STRING             COMMENT 'UI Element: None. This will be generated by Assist. Document number format:UEDYYYYMMDD####             UED is the ED document type, #### is a 4-digit sequential number beginning with 0000 (can start at 0000 for each new date)             For example, the document number for the first external direct document created on 12/16/2014 would be UED201412160001',
    agreement_id                                  BIGINT             COMMENT '[FK] UI Element: None. Links to Agreement table PK',
    request_date                                  TIMESTAMP             COMMENT 'UI Element: None. This is the date-time that the SOAP request message was made.',
    response_date                                 TIMESTAMP             COMMENT 'UI Element: None. This is the date-time that the SOAP response message was made.',
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    last_updated_by                               STRING,
    last_update_date                              TIMESTAMP,
    transmission_status                           STRING,
    request                                       STRING,
    response                                      STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (agreement_log_id)
COMMENT 'Silver copy of agreement.agreement_log from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'agreement',
    'pipeline.source_table'               = 'agreement_log',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_agreement_agreement_log',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: assist  (5 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_assist_award
-- Source : assist.award
-- Comment: Top level table for Award Documents
-- Columns: 97 source + 6 audit = 103 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_assist_award
(
    award_id                                      BIGINT NOT NULL    COMMENT '[PK]',
    procurement_id                                BIGINT             COMMENT '[FK]',
    form_type                                     STRING,
    form_revision_num                             STRING,
    status                                        STRING,
    order_mod_id                                  STRING             COMMENT '[FK]',
    mod_num                                       STRING,
    type_of_order                                 STRING,
    certification_of_funds_id                     DECIMAL(22,0)             COMMENT '[FK]',
    contract_num                                  STRING,
    contract_id                                   STRING             COMMENT '[FK]',
    order_num                                     STRING,
    order_date                                    TIMESTAMP,
    payment_term                                  STRING,
    po_discount_terms                             STRING,
    net_days                                      DECIMAL(19,2),
    discount_percent                              DECIMAL(6,3),
    discount_days                                 INT,
    payment_inquiry_name                          STRING,
    payment_inquiry_phone                         STRING,
    certified_date                                TIMESTAMP,
    certified_correct_by                          STRING,
    contract_officer_name                         STRING,
    contract_officer_phone                        STRING,
    contract_officer_phone_ext                    STRING,
    contracting_office_id                         STRING             COMMENT '[FK]',
    signed_by_id                                  STRING             COMMENT '[FK]',
    signature_name                                STRING,
    signature_date                                TIMESTAMP,
    lookup_emp_name                               STRING,
    lading_bill_num                               STRING,
    fob_point_name                                STRING,
    fob_date                                      DATE,
    small_business_class                          STRING,
    business_type                                 STRING,
    duns_num                                      STRING,
    ipartner_ftin                                 STRING,
    project_number                                STRING,
    amount                                        STRING,
    item_num                                      STRING,
    supplies_or_services                          STRING,
    unit_price                                    STRING,
    units                                         STRING,
    quantity_ordered                              STRING,
    description_of_amendment                      STRING,
    schedule_appropriation_text                   STRING,
    ip_sign_by_id                                 STRING             COMMENT '[FK]',
    ip_signature_date                             DATE,
    organization_code                             STRING,
    quantity_status                               STRING,
    quote_or_proposal_date                        DATE,
    ccr_exempt_flag                               STRING,
    ipartner_name                                 STRING,
    lookup_ip_poc_name                            STRING,
    ip_address_id                                 BIGINT             COMMENT '[FK]',
    different_remit_addr_flag                     STRING,
    place_of_accept_name                          STRING,
    place_of_accept_address_id                    BIGINT             COMMENT '[FK]',
    invoice_office                                STRING,
    invoice_address_id                            BIGINT             COMMENT '[FK]',
    invoice_instructions                          STRING,
    different_invoice_addr_flag                   STRING,
    remit_to_company                              STRING,
    remit_address_id                              BIGINT             COMMENT '[FK]',
    issuing_office                                STRING,
    issuing_address_id                            BIGINT             COMMENT '[FK]',
    requisition_office                            STRING,
    requisition_name                              STRING,
    requisition_address_id                        BIGINT             COMMENT '[FK]',
    ship_address_id                               BIGINT             COMMENT '[FK]',
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    submitted_by                                  STRING,
    submitted_date                                TIMESTAMP,
    last_updated_by                               STRING,
    last_update_date                              TIMESTAMP,
    delete_flag                                   STRING,
    ipartner_sign_reqd_flag                       STRING,
    ip_signed_flag                                STRING,
    num_copies_ip_return                          DECIMAL(19,2),
    contractor_agrees_flag                        STRING,
    ship_num                                      STRING,
    payment_status                                STRING,
    admin_by_name                                 STRING,
    admin_by_office                               STRING,
    admin_by_address_id                           BIGINT             COMMENT '[FK]',
    ecf_approval_flag                             STRING,
    agreement_number                              STRING,
    status_change_date                            DATE,
    act_num                                       STRING,
    contractor_comments                           STRING,
    shipping_location                             STRING,
    shipping_office                               STRING,
    ipartner_id                                   STRING             COMMENT '[FK]',
    small_business_pre_assist                     STRING,
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    vuei                                          STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (award_id)
COMMENT 'Top level table for Award Documents'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'assist',
    'pipeline.source_table'               = 'award',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_assist_award',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_assist_lu_emp_org_xref
-- Source : assist.lu_emp_org_xref
-- Comment: The assist.lu_emp_orgs parent-child relationships
-- Columns: 2 source + 6 audit = 8 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_assist_lu_emp_org_xref
(
    emp_org_id                                    INT NOT NULL    COMMENT '[PK]',
    emp_org_child_id                              INT NOT NULL    COMMENT '[PK]',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (emp_org_child_id)
COMMENT 'The assist.lu_emp_orgs parent-child relationships'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'assist',
    'pipeline.source_table'               = 'lu_emp_org_xref',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_assist_lu_emp_org_xref',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_assist_lu_emp_orgs
-- Source : assist.lu_emp_orgs
-- Comment: EMPloyee user ORGanizational heirarchy, each record is one node in the tree
-- Columns: 8 source + 6 audit = 14 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_assist_lu_emp_orgs
(
    emp_org_id                                    INT NOT NULL    COMMENT '[PK]',
    emp_org_name                                  STRING,
    emp_org_short_name                            STRING,
    tier_id                                       INT             COMMENT '[FK]',
    gsa_org_id                                    INT             COMMENT '[FK]',
    nba_org_id                                    INT             COMMENT '[FK]',
    nba_group_id                                  INT             COMMENT '[FK]',
    emp_org_version_cd                            STRING             COMMENT 'GSA Employee Hierarchy Version',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (emp_org_id)
COMMENT 'EMPloyee user ORGanizational heirarchy, each record is one node in the tree'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'assist',
    'pipeline.source_table'               = 'lu_emp_orgs',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_assist_lu_emp_orgs',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_assist_procurement
-- Source : assist.procurement
-- Comment: Silver copy of assist.procurement from ASSIST Postgres source.
-- Columns: 5 source + 6 audit = 11 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_assist_procurement
(
    procurement_id                                BIGINT NOT NULL    COMMENT '[PK]',
    system                                        STRING,
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    system_id                                     STRING             COMMENT '[FK]',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (procurement_id)
COMMENT 'Silver copy of assist.procurement from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'assist',
    'pipeline.source_table'               = 'procurement',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_assist_procurement',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_assist_users
-- Source : assist.users
-- Comment: Silver copy of assist.users from ASSIST Postgres source.
-- Columns: 45 source + 6 audit = 51 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_assist_users
(
    id                                            STRING NOT NULL    COMMENT '[PK]',
    reg_status                                    STRING,
    username                                      STRING,
    created_by                                    STRING             COMMENT 'The system-generated ID of person creating record.',
    creation_date                                 TIMESTAMP             COMMENT 'The calendar date when record was created.',
    last_updated_by                               STRING             COMMENT 'The ID number of person who last modified record.',
    last_update_date                              TIMESTAMP             COMMENT 'System-generated date record was last modified.',
    delete_flag                                   STRING             COMMENT 'The indicator of whether the record is to be deleted.',
    first_name                                    STRING,
    middle_initial                                STRING,
    last_name                                     STRING,
    address_id                                    BIGINT             COMMENT '[FK]',
    phone                                         STRING,
    phone_ext                                     STRING,
    fax                                           STRING,
    email                                         STRING,
    last_login                                    TIMESTAMP             COMMENT 'The date the user registered or last successfully logged in.',
    last_failed_login                             TIMESTAMP             COMMENT 'The date the user last failed at a login attempt.',
    success_ip_address                            STRING             COMMENT 'The IP address for the last successful login attempt.',
    fail_ip_address                               STRING             COMMENT 'The IP address for the last failed login attempt.',
    authentication_type                           STRING             COMMENT 'The type of authentication performed. For example - Windows or Domino.',
    inactive_date                                 TIMESTAMP             COMMENT 'The date a user is no longer registered.',
    remarks                                       STRING             COMMENT 'The text that describes comments about user.',
    password_reset_token                          STRING,
    password_reset_expiration_date                TIMESTAMP,
    user_type                                     STRING,
    password                                      STRING,
    login_attempts                                INT,
    last_failed_login_timestamp                   TIMESTAMP,
    last_reg_status_updated_date                  TIMESTAMP             COMMENT 'The last updated date when the reg status has been change.',
    default_module                                STRING,
    password_change_flag                          STRING,
    password_change_date                          TIMESTAMP,
    activation_date                               TIMESTAMP,
    special_instructions                          STRING             COMMENT 'Special Instructions related to the creation of the user.',
    password_expirable                            STRING             COMMENT 'Y if the password can expire, otherwise N.',
    departure_date                                DATE             COMMENT 'Used by Reg to auto-reject user on specified date',
    otp_secret                                    STRING,
    otp_last_login_time                           STRING,
    otp_successful_login_yn                       STRING,
    allow_mfa_by_email_yn                         STRING,
    login_email                                   STRING,
    account_to_retain_yn                          STRING             COMMENT 'Boolean indicator to identify the account to retain for users with multiple accounts',
    requires_fas_email_yn                         STRING             COMMENT 'Boolean indicator to flag if a FAS ID login email needs to be requested for the user',
    okta_user_id                                  STRING             COMMENT '[FK] Unique Okta-generated ID of User',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id)
COMMENT 'Silver copy of assist.users from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'assist',
    'pipeline.source_table'               = 'users',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_assist_users',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: billing  (3 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_billing_billing
-- Source : billing.billing
-- Comment: Silver copy of billing.billing from ASSIST Postgres source.
-- Columns: 61 source + 6 audit = 67 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_billing_billing
(
    billing_id                                    BIGINT NOT NULL    COMMENT '[PK] UI Element: None. Primary key with sequence',
    agreement_num                                 STRING             COMMENT 'UI Element: AGREEMENT_NUM  This is the foreign key to the AGREEMENT table',
    agreement_line_num                            STRING             COMMENT 'UI Element: NONE.  All line numbers are currently 1',
    system_type                                   STRING             COMMENT 'UI Element: NONE.  RBA or NBA',
    billing_amount                                DECIMAL(28,2)             COMMENT 'UI Element: ITOMS - multiple fields.  Billing amount',
    vendor_customer_code                          STRING             COMMENT 'UI Element: RBA-Funding Citation-BOAC2 NBA-OMIS Funding Doc-BOAC 2.  BOAC2',
    designated_agent_code                         STRING             COMMENT 'UI Element: RBA-Funding Citation-BOAC1 NBA-OMIS Funding Doc-BOAC 1',
    agency_code                                   STRING             COMMENT 'UI Element: NONE. Agency code.',
    title                                         STRING             COMMENT 'UI Element: Funding Document-IAA Number NBA-OMIS Funding Doc-IA',
    external_system_doc_num                       STRING             COMMENT 'UI Element: NBA-TASK ORDER or RBA-CUSTOMER ID.  Required. The XSYS_DOC_NUM field is mapped to the ACT_NO on the FMIS VAT.',
    activity_code                                 STRING             COMMENT 'UI Element: RBA-Purchase Order Function Code.  F11->AF127 F31->AF123 F51->AF151 F81->AF121 F99->AF120',
    fund                                          STRING             COMMENT 'UI Element: Fund. 285F?!',
    region_division                               STRING             COMMENT 'UI Element: RBA-COI Organization Code NBA-Not visible.  Required. New Organization Codes are implemented as part of the FAS One-Fund reorganization. Examples: 			If Region is 03, Program Code is AA20, and Organization Code is A03VR110, then set the new Organization Code to Q03FA000. 			If Region is 30, Program Code is AA20, and Organization Code is A03VR114, then set the new Organization Code to Q03FA100.',
    org_code                                      STRING             COMMENT 'UI Element: RBA-Funding Citation NBA-Not visible - Budget Activity Code. New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code.',
    program_code                                  STRING             COMMENT 'UI Element: NONE.  B3->IT23 F1->AA20 F2->AA10 FL->IT31 FQ->IT14 FR->GS14 P1->AA20 P2->AA10',
    revenue_source                                STRING             COMMENT 'UI Element: NONE.  NBA1 if the Program Code (row 15 in this table) is AA10 or IT31, RBA1 if the Program Code (row 15 in this table) is AA20, GS13, IT14, or IT23',
    credit_indicator                              STRING             COMMENT 'UI Element: NONE.  Required for credits.',
    advance_indicator                             STRING             COMMENT 'UI Element: NONE.  T for advance (a bill PDF will be created), O for advance offset (reate a null-posting bill that provides the amount that the Finance Center should use to offset the advance. No bill PDF will be created.), F (regular bill) otherwise',
    pop_start                                     TIMESTAMP             COMMENT 'UI Element: RBA-Purchase Order Pop start date NBA-OMIS POP Start.  Set to the first day of the month corresponding to the Service Month/Year',
    pop_end                                       TIMESTAMP             COMMENT 'UI Element: RBA-Purchase Order Pop End Date NBA-OMIS POP End.  Set to the last day of the month corresponding to the Service Month/Year',
    related_statement_num                         STRING             COMMENT 'UI Element: NONE.  Identifies the related Pegasys statement number for credits and rebills.',
    interfund_indicator                           STRING             COMMENT 'UI Element: NONE.  If Transaction Type is N, set to F If Transaction Type is Y, set to A for intrafund transactions or T for interfund transactions',
    reference_doc_type                            STRING             COMMENT 'UI Element: NONE.  If the Interfund Indicator is T or A, set to the Document type of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank',
    reference_doc_num                             STRING             COMMENT 'UI Element: NONE.  If the Interfund Indicator is T or A, set to the Document Number of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank',
    reference_doc_line_num                        STRING             COMMENT 'UI Element: NONE.  If the Interfund Indicator (row 24 in this table) is T or A, set to the accounting line number of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank',
    acct_classification_code                      STRING             COMMENT 'UI Element: NONE.  This may be not needed if this will just be BOAC1',
    acct_classification_ref_num                   STRING             COMMENT 'UI Element: NONE.  Task Order/Cust. ID + Subtask. Subtask is not used for NBA, so this field will contain only Task Order/Cust. ID for NBA',
    fiscal_station_num                            INT             COMMENT 'UI Element: NONE.  This is defaulted to 0',
    description                                   STRING             COMMENT 'UI Element: NONE.  If the Interfund Indicator is F, set to Customer Purchase Order, otherwise, leave blank',
    cust_sublevel_prefix                          STRING             COMMENT 'UI Element: NONE.  Customer Sublevel Prefix',
    tysm_allocation_trans_agency                  STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Allocation Transfer Agency',
    tysm_agency_identifier                        STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Agency Identifier',
    tysm_begin_period                             STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Beginning Period of Availability',
    tysm_end_period                               STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Ending Period of Availability',
    tysm_avalability_type                         STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Availability Type',
    tysm_main_acct                                STRING             COMMENT 'UI Element: NONE.  Customer Treasury Symbol: Main Account',
    tysm_sub_acct                                 STRING             COMMENT 'UI Element: This will be a new field on the Funding Citation.  Customer Treasury Symbol: Sub Account',
    duns                                          STRING             COMMENT 'UI Element: RBA-Contractor DUNS NBA-OMIS Funding Document-Client DUNS#.  Agency DUNS',
    duns_plus_four                                STRING             COMMENT 'UI Element: RBA-Contractor DUNS+4 NBA-OMIS Funding Document-Client DUNS#.  Agency DUNS+4',
    funding_doc_num                               STRING             COMMENT 'UI Element: RBA-Fundings Funding Document Number NBA-Funding Document Screen-Funding Doc#. FNDG_DOCSRC_NUM',
    quantity                                      INT             COMMENT 'UI Element: RBA-Task Order Quantity',
    unit_of_issue                                 STRING             COMMENT 'UI Element: RBA-Task Order Unit. Default to EA for EAch',
    description_of_services                       STRING             COMMENT 'UI Element: RBA-Task Order Description',
    group_number                                  STRING             COMMENT 'UI Element: NONE. May be populated for situations where multiple sequential records should be loaded as one unit of work (all are loaded or none are loaded). This field must be left blank unless there are 2 or more sequential records in the file with the same value.',
    additional_text                               STRING             COMMENT 'ASSIST2:  Foreign key to AASBS.LOA_LEDGER.ID.  NBA:  Foreign key to OMIS.BID_BAAR.BID_BAAR_ID ~ .BID_ID ~ .AGREEMENT_NUM',
    created_by                                    STRING,
    creation_date                                 TIMESTAMP,
    last_updated_by                               STRING,
    last_update_date                              TIMESTAMP,
    bbfy                                          STRING,
    assist_ext_billing_id                         BIGINT             COMMENT '[FK]',
    statement_num                                 STRING             COMMENT 'Alphanumeric unique identifier for grouping of bills.',
    customer_fund                                 STRING             COMMENT 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.',
    agcy_uei                                      STRING             COMMENT 'GSA UEI - Unique Entity Identifier',
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    uei_plus4                                     STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    ginv_ref_performance_num                      STRING             COMMENT 'G-Invoice Performance Number',
    ginv_ref_performance_num_detail               STRING             COMMENT 'G-Invoice Performance Number Detail',
    vuei                                          STRING,
    vuei_plus4                                    STRING,
    billing_summary_id                            BIGINT             COMMENT '[FK] Foreign Key to AASBS_TRANSMIT.BILLING_SUMMARY.ID',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (billing_id)
COMMENT 'Silver copy of billing.billing from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'billing',
    'pipeline.source_table'               = 'billing',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_billing_billing',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_billing_billing_batch_detail
-- Source : billing.billing_batch_detail
-- Comment: UI Element: None. This table links details about a batch transmission with the specific billing item
-- Columns: 6 source + 6 audit = 12 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_billing_billing_batch_detail
(
    billing_batch_detail_id                       BIGINT             COMMENT '[FK] UI Element: None. PK for this table',
    billing_id                                    BIGINT             COMMENT '[FK] UI Element: None. FK link to BILLING table',
    batch_id                                      STRING             COMMENT '[FK] UI Element: None. FK link to BILLING_BATCH_HEADER',
    billing_record_id                             STRING             COMMENT '[FK] UI Element: None. RNB + MMDDYYYY#######, where MMDDYYYY is the date, and ####### is a sequence number within the date.',
    detail_trailer                                STRING             COMMENT 'UI Element: None. Trailer string for the detail records',
    batch_trailer                                 STRING             COMMENT 'UI Element: None. Trailer string for this batch envelope',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (billing_batch_detail_id)
COMMENT 'UI Element: None. This table links details about a batch transmission with the specific billing item sent. See BAAR 31b AAS Billing DES-Final, exhibit 5.1 and 5.7 for file/batch/detail structure'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'billing',
    'pipeline.source_table'               = 'billing_batch_detail',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_billing_billing_batch_detail',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_billing_billing_response_info
-- Source : billing.billing_response_info
-- Comment: This table logs the informational reponses from billing transmission.  How do we tie this record bac
-- Columns: 5 source + 6 audit = 11 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_billing_billing_response_info
(
    billing_response_info_id                      BIGINT NOT NULL    COMMENT '[PK] Primary Key',
    billing_response_header_id                    BIGINT             COMMENT '[FK] Foreign Key to the billing response header',
    billing_record_id                             STRING             COMMENT '[FK] Uniquely identifies this bill detail record in the Pegasys Detail Billing Record table; this will reflect the value from the input file TLC + MMDDYYYY + #######',
    billing_response_info                         STRING             COMMENT 'The raw information response',
    accepted_status                               STRING             COMMENT 'Valid values: ACCEPTED=Pegasys verified ERROR=Pegasys rejected INVALID,DELETE=Do not send HOLD=temporarily hold to release later',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (billing_response_info_id)
COMMENT 'This table logs the informational reponses from billing transmission.  How do we tie this record back to a BILLING entry, though?!'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'billing',
    'pipeline.source_table'               = 'billing_response_info',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_billing_billing_response_info',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: table_master  (4 tables)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_table_master_clients
-- Source : table_master.clients
-- Comment: Silver copy of table_master.clients from ASSIST Postgres source.
-- Columns: 35 source + 6 audit = 41 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_table_master_clients
(
    client_id                                     STRING NOT NULL    COMMENT '[PK] The ID number of client organization.',
    client_name                                   STRING             COMMENT 'The name of client organization.',
    reg_status                                    STRING             COMMENT 'The registration status of the client.',
    created_by                                    STRING             COMMENT 'The system ID of client generating record.',
    creation_date                                 TIMESTAMP             COMMENT 'The calendar date the record was generated.',
    last_updated_by                               STRING             COMMENT 'The system ID of person last modifying record.',
    last_update_date                              TIMESTAMP             COMMENT 'The calendar date when the record was last modified.',
    client_code                                   STRING             COMMENT 'The 2 digit agency code + 5 digit zipcode + 2 digit sequence.',
    phone                                         STRING             COMMENT 'The phone number associated with address.',
    phone_ext                                     STRING             COMMENT 'The phone number extension associated with address.',
    fax                                           STRING             COMMENT 'The client\'s facsimile number.',
    email                                         STRING             COMMENT 'The E-mail address of client.',
    street_1                                      STRING             COMMENT 'The first line of the address.',
    street_2                                      STRING             COMMENT 'The second line of address.',
    city                                          STRING             COMMENT 'The city line of address.',
    state                                         STRING             COMMENT 'The state line of address.',
    zip                                           STRING             COMMENT 'The zipcode line of address.',
    country                                       STRING             COMMENT 'The country line of address.',
    agency_code                                   STRING             COMMENT 'The code that represents the FIPS agency.',
    bureau_code                                   STRING             COMMENT 'The code that represents the FIPS Bureau.',
    ship_to_attention                             STRING             COMMENT 'The default ship to address attention line.',
    ship_to_street_1                              STRING             COMMENT 'The first line of the default ship to address.',
    ship_to_street_2                              STRING             COMMENT 'The second line of default ship to address.',
    ship_to_city                                  STRING             COMMENT 'The default ship to address city line.',
    ship_to_state                                 STRING             COMMENT 'The default ship to address state line.',
    ship_to_zip                                   STRING             COMMENT 'The default ship to address zipcode line.',
    ship_to_country                               STRING             COMMENT 'The default ship to address country line.',
    client_acct_num                               STRING             COMMENT 'The client\'s account number.',
    client_remarks                                STRING             COMMENT 'The text that describes the client\'s comments.',
    gsa_client_flag                               STRING             COMMENT 'The identifier that indicates if  this is a GSA client.',
    client_active_flag                            STRING             COMMENT 'The identifier that indicates if the client is active.',
    gsa_contact                                   STRING             COMMENT 'The GSA contact to notify when an order is started.',
    mou_number                                    STRING             COMMENT 'The MOU number associated with client.',
    delete_flag                                   STRING             COMMENT 'The identifier that indicates if the record is to be deleted.',
    contract_country                              STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (client_id)
COMMENT 'Silver copy of table_master.clients from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'table_master',
    'pipeline.source_table'               = 'clients',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_table_master_clients',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_table_master_contracts
-- Source : table_master.contracts
-- Comment: Silver copy of table_master.contracts from ASSIST Postgres source.
-- Columns: 60 source + 6 audit = 66 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_table_master_contracts
(
    contract_id                                   STRING NOT NULL    COMMENT '[PK] The system-generated ID of contract.',
    created_by                                    STRING             COMMENT 'The system ID of person generating record.',
    creation_date                                 TIMESTAMP             COMMENT 'The calendar date when the record was generated.',
    last_updated_by                               STRING             COMMENT 'The ID number of person who last modified record.',
    last_update_date                              TIMESTAMP             COMMENT 'The calendar date that denotes when the record last modified.',
    delete_flag                                   STRING             COMMENT 'The identifier that indicates if the record is to be deleted.',
    ipartner_id                                   STRING             COMMENT '[FK] The ID number of the Industry Partner owning contract.',
    contract_num                                  STRING             COMMENT 'The contract number.',
    reg_status                                    STRING             COMMENT 'The registration status of the contract.',
    ipartner_ftin                                 STRING             COMMENT 'The tax ID number of the Industry Partner.',
    contract_to_date                              TIMESTAMP             COMMENT 'The calendar date that denotes when the contract expires.',
    contact_street_1                              STRING             COMMENT 'The contact address first line.',
    contact_city                                  STRING             COMMENT 'The contact address city line.',
    contact_state                                 STRING             COMMENT 'The contact address state line.',
    contact_zip                                   STRING             COMMENT 'The contact address zipcode line.',
    contact                                       STRING             COMMENT 'The point of contact for the contract.',
    contact_street_2                              STRING             COMMENT 'The contact address second line.',
    contact_country                               STRING             COMMENT 'The contact address country line.',
    contract_name                                 STRING             COMMENT 'The name of the contract.',
    contract_from_date                            TIMESTAMP             COMMENT 'The calendar date that denotes the start of contract.',
    duns_num                                      STRING             COMMENT 'The industry partner DUNS number.',
    co_emp_id                                     STRING             COMMENT '[FK]',
    remarks                                       STRING             COMMENT 'The text that describes comments about the contract.',
    lookup_ipartner_company_name                  STRING,
    lookup_emp_name                               STRING             COMMENT 'Used by ITSS to lookup unique name.',
    lookup_contract_number                        STRING             COMMENT 'The unique contract number, used by ITSS to create foreign key links.',
    cage_code                                     STRING             COMMENT 'The cage code of the company.',
    contract_expire_date                          TIMESTAMP             COMMENT 'The expiration date of the contract.',
    contract_street_1                             STRING             COMMENT 'The street address 1 on the contract.',
    contract_street_2                             STRING             COMMENT 'The street address 2 on the contract.',
    contract_city                                 STRING             COMMENT 'The city address on the contract.',
    contract_state                                STRING             COMMENT 'The state address on the contract.',
    contract_zip                                  STRING             COMMENT 'The zip code address on the contract.',
    contract_company                              STRING             COMMENT 'The company name on the contract.',
    contract_status                               STRING             COMMENT 'The status of the contract.',
    company_org_type                              STRING             COMMENT 'The type of organization the company is.',
    company_classification                        STRING             COMMENT 'The company classification.',
    industry_type                                 STRING             COMMENT 'The type of industry on the contract.',
    email                                         STRING             COMMENT 'The email address on the contract.',
    fss_contract_flag                             STRING             COMMENT 'The identifier that indicates the contract as an FSS contract.',
    reseller_id                                   STRING             COMMENT '[FK] The Industry Partner ID of the reseller.',
    reseller_company_name                         STRING             COMMENT 'The Industry Partner Name of the reseller.',
    contract_family_id                            BIGINT             COMMENT '[FK] The unique identifier for the contract family the contract belongs to.',
    fss_contract_update_date                      TIMESTAMP             COMMENT 'The date the record was last updated with FSS-19 contract data.',
    phone                                         STRING,
    discount_terms                                STRING             COMMENT 'The discount terms.',
    issuing_agency                                STRING,
    payment_terms                                 STRING,
    parent_name                                   STRING,
    government_co                                 STRING,
    government_co_phone                           STRING,
    naics                                         STRING,
    contact_fax                                   STRING,
    duns_plus4                                    STRING,
    assist_idv                                    STRING,
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    uei_plus4                                     STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    cohort_num                                    BIGINT             COMMENT 'The cohort number that the contract was awarded in.',
    vuei                                          STRING,
    vuei_plus4                                    STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (contract_id)
COMMENT 'Silver copy of table_master.contracts from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'table_master',
    'pipeline.source_table'               = 'contracts',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_table_master_contracts',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_table_master_industry_partners
-- Source : table_master.industry_partners
-- Comment: Silver copy of table_master.industry_partners from ASSIST Postgres source.
-- Columns: 80 source + 6 audit = 86 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_table_master_industry_partners
(
    ipartner_id                                   STRING NOT NULL    COMMENT '[PK] The ITSS system-generated Primary Key.',
    reg_status                                    STRING             COMMENT 'The registration status.',
    created_by                                    STRING             COMMENT 'The system ID of the person generating the record.',
    creation_date                                 TIMESTAMP             COMMENT 'The calendar date that denotes when record was generated.',
    last_updated_by                               STRING             COMMENT 'The ID number of person who last modified record.',
    last_update_date                              TIMESTAMP             COMMENT 'The system-generated date the record was last modified.',
    delete_flag                                   STRING             COMMENT 'The identifier that indicates whether the record is to be deleted.',
    ipartner_company_name                         STRING             COMMENT 'The industry partner company name.',
    ipartner_location                             STRING             COMMENT 'The operating location of company.',
    street_1                                      STRING             COMMENT 'The first line of the address.',
    street_2                                      STRING             COMMENT 'The second line of address.',
    city                                          STRING             COMMENT 'The city line of address.',
    state                                         STRING             COMMENT 'The state line of address.',
    zip                                           STRING             COMMENT 'The zipcode line of address.  The first 5 numbers are required.',
    country                                       STRING             COMMENT 'The country line of address.',
    phone                                         STRING             COMMENT 'The phone number associated with the address.',
    phone_ext                                     STRING             COMMENT 'The phone number extension associated with the address.',
    fax                                           STRING             COMMENT 'The facsimile number associated with the industry partner.',
    email                                         STRING             COMMENT 'The E-mail address.',
    duns_num                                      STRING             COMMENT 'The industry partner DUNS number.',
    small_business_class                          STRING             COMMENT 'The small business code, 8a.',
    ipartner_inc_status                           STRING             COMMENT 'The incorporation status (corporation, partnership, etc).',
    industry_type                                 STRING             COMMENT 'The industry type.',
    ipartner_cec                                  STRING             COMMENT 'The code that represents the contractor establishment.',
    certified_8a_flag                             STRING             COMMENT 'The identifier that indicates if company is 8A certified.',
    sba_contact                                   STRING             COMMENT 'The name of SBA contact.',
    sba_street_1                                  STRING             COMMENT 'The SBA contact first line of the address.',
    sba_street_2                                  STRING             COMMENT 'The SBA contact second line of address.',
    sba_city                                      STRING             COMMENT 'The SBA contact city line.',
    sba_state                                     STRING             COMMENT 'The SBA contact state line.',
    sba_zip                                       STRING             COMMENT 'The SBA contact zipcode.  The first 5 numbers are required.',
    sba_country                                   STRING             COMMENT 'The SBA contact country line.',
    sba_phone                                     STRING             COMMENT 'The SBA contact phone number associated with address.',
    sba_phone_ext                                 STRING             COMMENT 'The SBA contact phone extension associated with address.',
    sba_fax                                       STRING             COMMENT 'The SBA contact facsimile number associated with address.',
    sba_email                                     STRING             COMMENT 'The SBA contact E-mail address.',
    open_market_flag                              STRING             COMMENT 'The identifier that indicates whether the industry partner participates in open market orders.',
    ipartner_cage_code                            STRING             COMMENT 'The code that represents the industry partner\'s cage code.',
    ipartner_web_site                             STRING             COMMENT 'The industry partner\'s web page address.',
    ipartner_eft_bank_name                        STRING             COMMENT 'The industry partner\'s bank name.',
    ipartner_eft_num                              DECIMAL(19,2)             COMMENT 'The industry partner\'s electronic funds transfer account number.',
    ipartner_eft_acct_type                        STRING             COMMENT 'The identifier that indicates whether this is a savings or checking account.',
    ipartner_eft_transfer_num                     STRING             COMMENT 'The 9-digit routing code for an account.',
    ipartner_ftin                                 STRING             COMMENT 'The industry partner\'s federal taxpayer ID number.',
    ipartner_parent_name                          STRING             COMMENT 'The industry partner\'s parent company name.',
    ipartner_parent_duns_num                      STRING             COMMENT 'The industry partner\'s parent company\'s DUNS number.',
    ipartner_parent_tin_num                       STRING             COMMENT 'The industry partner\'s parent company\'s Taxpayer Identification Number.',
    ipartner_contact                              STRING             COMMENT 'The industry partner point of contact name.',
    net_days                                      DECIMAL(19,2)             COMMENT 'The default days to pay an invoice before a penalty.',
    discount_days                                 DECIMAL(19,2)             COMMENT 'The default discount days to populate order.',
    discount_percentage                           DECIMAL(6,3)             COMMENT 'The default discount percentage to populate order.',
    remit_street_1                                STRING             COMMENT 'The first line of the address.',
    remit_street_2                                STRING             COMMENT 'The second line of address.',
    remit_city                                    STRING             COMMENT 'The remmittance city of address.',
    remit_state                                   STRING             COMMENT 'The remmittance state line of address.',
    remit_zip                                     STRING             COMMENT 'The remmittance zipcode line of address.',
    remit_country                                 STRING             COMMENT 'The remmittance country line of address.',
    remit_phone                                   STRING             COMMENT 'The remmittance phone number associated with address.',
    remit_phone_ext                               STRING             COMMENT 'The remmittance phone extension associated with address.',
    remit_fax                                     STRING             COMMENT 'The remmittance facsimile number associated with address.',
    remit_email                                   STRING             COMMENT 'The remmittance E-mail address.',
    remarks                                       STRING             COMMENT 'The text that describes comments about industry partner.',
    products_and_services                         STRING             COMMENT 'The industry partner list of products and services.',
    order_street_1                                STRING             COMMENT 'The first line of the address.',
    order_street_2                                STRING             COMMENT 'The second line of the address.',
    order_city                                    STRING             COMMENT 'The city of address line.',
    order_state                                   STRING             COMMENT 'The state line of address.',
    order_zip                                     STRING             COMMENT 'The zipcode line of address.  The first 5 numbers are required.',
    order_country                                 STRING             COMMENT 'The country line of address.',
    coverage_areas                                STRING             COMMENT 'The geographic areas the industry partner supports.',
    buyers_role                                   STRING             COMMENT 'The Buyers.gov role, if applicable.',
    duns_plus4                                    STRING,
    sam_last_update_date                          TIMESTAMP,
    native_american_company                       STRING,
    ipartner_parent_uei                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    uei                                           STRING             COMMENT 'Unique Entity Identifier (UEI) - replaces DUNS',
    uei_plus4                                     STRING             COMMENT 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4',
    ipartner_parent_vuei                          STRING,
    vuei                                          STRING             COMMENT 'Generated column:  show UEI if present, otherwise show DUNS_NUM',
    vuei_plus4                                    STRING             COMMENT 'Generated column:  show UEI_PLUS4 if present, otherwise show DUNS_PLUS4',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (ipartner_id)
COMMENT 'Silver copy of table_master.industry_partners from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'table_master',
    'pipeline.source_table'               = 'industry_partners',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_table_master_industry_partners',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);


-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_table_master_lu_federal_agency
-- Source : table_master.lu_federal_agency
-- Comment: Silver copy of table_master.lu_federal_agency from ASSIST Postgres source.
-- Columns: 7 source + 6 audit = 13 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_table_master_lu_federal_agency
(
    agency_code                                   STRING NOT NULL    COMMENT '[PK] The code that represents the FIPS agency.',
    bureau_code                                   STRING NOT NULL    COMMENT '[PK] The code that represents the FIPS Bureau.',
    agency_description                            STRING             COMMENT 'The text that describes the Agency.',
    bureau_description                            STRING             COMMENT 'The text that describes the Bureau.',
    inactive_date                                 TIMESTAMP             COMMENT 'The calendar date that denotes when inactive.',
    agency_code_3                                 STRING             COMMENT 'UI Element: Requesting Agency pick-list. New 3-digit code for requesting agency on citations.',
    agency_type                                   STRING             COMMENT 'Use for Requesting Agency pick-list on funding citation and OMIS to differentiate between DOD vs other agencies.  Value is DOD or null',
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (agency_code)
COMMENT 'Silver copy of table_master.lu_federal_agency from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'table_master',
    'pipeline.source_table'               = 'lu_federal_agency',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_table_master_lu_federal_agency',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- SOURCE SCHEMA: aasbs_history  (1 table)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- assist_dev.assist_finance.silver_aasbs_history_accrual_inclusion_tracker
-- Source : aasbs_history.accrual_inclusion_tracker
-- Comment: Silver copy of aasbs_history.accrual_inclusion_tracker from ASSIST Postgres source.
-- Columns: 20 source + 6 audit = 26 total
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assist_dev.assist_finance.silver_aasbs_history_accrual_inclusion_tracker
(
    id                                            BIGINT,
    li_tracking_num                               BIGINT,
    accrual_inclusion_status_cd                   STRING,
    was_manually_set_yn                           STRING,
    created_by_user_id                            STRING             COMMENT '[FK]',
    created_dt                                    TIMESTAMP,
    updated_by_user_id                            STRING             COMMENT '[FK]',
    updated_dt                                    TIMESTAMP,
    history_id                                    DOUBLE             COMMENT '[FK]',
    hrec_archive_ts                               TIMESTAMP,
    hrec_action_cd                                STRING,
    hrec_deleted_dt                               TIMESTAMP,
    hrec_user_id                                  STRING             COMMENT '[FK]',
    usr_keyed_holdback                            DECIMAL(19,2),
    comments                                      STRING,
    accrual_holdback_type_cd                      STRING,
    invalid_udo_amt                               DECIMAL(15,2),
    local_transaction_id                          STRING             COMMENT '[FK]',
    client_host                                   STRING,
    client_ip_address                             STRING,
-- ── Pipeline audit & soft-delete columns ─────────────────────────────────,
    _ingested_at                                  TIMESTAMP    COMMENT 'Timestamp when this row was written to the Silver layer by the ingestion pipeline.',
    _source_system                                STRING       COMMENT 'Source system identifier. Always assist_postgres for ASSIST OCFO source.',
    _batch_id                                     STRING       COMMENT 'Pipeline job run ID (Databricks job_run_id) for end-to-end lineage tracing.',
    _checksum                                     STRING       COMMENT 'MD5 hash of key business columns used for change detection in delta refresh.',
    is_deleted                                    BOOLEAN      COMMENT 'Soft-delete flag. TRUE when row has been removed from the Postgres source. Row is never physically deleted from Silver.',
    deleted_at                                    TIMESTAMP    COMMENT 'Timestamp when the soft-delete was detected by the pipeline. NULL for active rows.'
)
USING DELTA
CLUSTER BY (id, updated_dt)
COMMENT 'Silver copy of aasbs_history.accrual_inclusion_tracker from ASSIST Postgres source.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.minReaderVersion'              = '1',
    'delta.minWriterVersion'              = '4',
    'pipeline.layer'                      = 'silver',
    'pipeline.source_schema'              = 'aasbs_history',
    'pipeline.source_table'               = 'accrual_inclusion_tracker',
    'pipeline.source_system'              = 'assist_postgres',
    'pipeline.silver_table'               = 'silver_aasbs_history_accrual_inclusion_tracker',
    'quality.soft_delete_enabled'         = 'true',
    'quality.cdf_enabled'                 = 'true'
);



-- =============================================================================
-- END OF SILVER DDL
-- Tables generated : 211
-- Total columns    : 4251
--   Source columns : 2985
--   Audit columns  : 1266 (6 per table)
-- Catalog          : assist_dev
-- Schema           : assist_finance
-- =============================================================================
