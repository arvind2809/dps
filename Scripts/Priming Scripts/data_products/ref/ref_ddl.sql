-- ==============================================================================
-- DP11 — assist_dev.ref — Conformed Reference Data Catalog
-- Grain: lookup code per domain. 61 lu_* tables published as versioned reference product.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.ref.lu_acquisition_status
-- Reference data: Acquisition status codes and descriptions. SCD Type 1 — full refresh on change. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_acquisition_status
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
-- assist_dev.ref.lu_acquisition_type
-- Reference data: Acquisition type — Service or Supply. SCD Type 1 — full refresh on change. Source: a
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_acquisition_type
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
-- assist_dev.ref.lu_fund_type
-- Reference data: Funding types — Annual, Multi-year, No-year, Revolving. SCD Type 1 — full refresh on
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fund_type
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
-- assist_dev.ref.lu_fund_status
-- Reference data: Funding status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_sta
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fund_status
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
-- assist_dev.ref.lu_fund_category
-- Reference data: Fund category codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund_cate
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fund_category
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
-- assist_dev.ref.lu_billing_type
-- Reference data: Billing types — Advance Deposit (AD) or Reimbursable Agreement (RA). SCD Type 1 — fu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_billing_type
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
-- assist_dev.ref.lu_contract_type
-- Reference data: Contract types — FFP, T&M, CPFF, CPAF, IDIQ, etc. Includes accrual_holdback_pct. SCD
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_contract_type
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
-- assist_dev.ref.lu_competition_type
-- Reference data: FAR Part 6 competition type codes. SCD Type 1 — full refresh on change. Source: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_competition_type
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
-- assist_dev.ref.lu_instrument_type
-- Reference data: Instrument types — MIPR, Economy Act, Assisted Acquisition, etc. SCD Type 1 — full r
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_instrument_type
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
-- assist_dev.ref.lu_sol_status
-- Reference data: Solicitation status codes and descriptions. SCD Type 1 — full refresh on change. Sou
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sol_status
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
-- assist_dev.ref.lu_sol_amend_status
-- Reference data: Solicitation amendment status codes. SCD Type 1 — full refresh on change. Source: aa
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sol_amend_status
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
-- assist_dev.ref.lu_sol_posting_type
-- Reference data: Solicitation posting type codes. SCD Type 1 — full refresh on change. Source: aasbs.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sol_posting_type
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
-- assist_dev.ref.lu_sol_response_aac_visibility
-- Reference data: AAC visibility settings for solicitation responses. SCD Type 1 — full refresh on cha
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sol_response_aac_visibility
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
-- assist_dev.ref.lu_sol_response_cli_visibility
-- Reference data: Client visibility settings for solicitation responses. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sol_response_cli_visibility
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
-- assist_dev.ref.lu_performance_type
-- Reference data: Performance-based acquisition type codes. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_performance_type
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
-- assist_dev.ref.lu_acquisition_mod_status
-- Reference data: Acquisition modification status codes. SCD Type 1 — full refresh on change. Source: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_acquisition_mod_status
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
-- assist_dev.ref.lu_review_type
-- Reference data: Pre-award review type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_r
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_review_type
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
-- assist_dev.ref.lu_review_finding
-- Reference data: Review finding codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_review_f
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_review_finding
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
-- assist_dev.ref.lu_reviewer_type
-- Reference data: Reviewer type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_reviewer_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_reviewer_type
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
-- assist_dev.ref.lu_responsible_role
-- Reference data: Application business role codes — CO, COR, FMA, PM, CLIENT_REP. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_responsible_role
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
-- assist_dev.ref.lu_cor_delegation
-- Reference data: COR delegation method codes per FAR 1.602-2(d). SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_cor_delegation
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
-- assist_dev.ref.lu_ia_status
-- Reference data: Interagency agreement status codes. SCD Type 1 — full refresh on change. Source: aas
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_ia_status
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
-- assist_dev.ref.lu_ia_amend_action
-- Reference data: IA amendment action type codes. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_ia_amend_action
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
-- assist_dev.ref.lu_loa_status
-- Reference data: Line of Accounting status codes. SCD Type 1 — full refresh on change. Source: aasbs.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_loa_status
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
-- assist_dev.ref.lu_loa_change_reason
-- Reference data: Reasons a change can be made to an LOA. SCD Type 1 — full refresh on change. Source:
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_loa_change_reason
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
-- assist_dev.ref.lu_line_item_type
-- Reference data: CLIN/SLIN type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_line_ite
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_line_item_type
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
-- assist_dev.ref.lu_vehicle_type
-- Reference data: Contract vehicle type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_v
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_vehicle_type
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
-- assist_dev.ref.lu_proc_phase
-- Reference data: Procurement phase codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_proc_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_proc_phase
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
-- assist_dev.ref.lu_program
-- Reference data: GSA program codes — AAS, ITS_NSD, etc. SCD Type 1 — full refresh on change. Source: 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_program
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
-- assist_dev.ref.lu_region
-- Reference data: GSA region codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_region.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_region
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
-- assist_dev.ref.lu_state
-- Reference data: US state codes, territories, and foreign entity codes. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_state
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
-- assist_dev.ref.lu_country
-- Reference data: Country codes for GSA business locations. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_country
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
-- assist_dev.ref.lu_address_type
-- Reference data: Address type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_address_ty
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_address_type
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
-- assist_dev.ref.lu_agency
-- Reference data: Federal agency codes and names. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_agency
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
-- assist_dev.ref.lu_award_mod_fpds_reason
-- Reference data: FPDS modification reason codes. SCD Type 1 — full refresh on change. Source: aasbs.l
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_award_mod_fpds_reason
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
-- assist_dev.ref.lu_sf30_mod_type
-- Reference data: SF-30 modification type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_sf30_mod_type
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
-- assist_dev.ref.lu_fpds_car_status
-- Reference data: FPDS Contract Action Report status codes. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fpds_car_status
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
-- assist_dev.ref.lu_bundled_contract
-- Reference data: Bundled contract determination codes (FAR 7.107). SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_bundled_contract
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
-- assist_dev.ref.lu_idv_type_of_idc
-- Reference data: IDV type of Indefinite Delivery Contract codes. SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_idv_type_of_idc
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
-- assist_dev.ref.lu_idv_referenced_class
-- Reference data: IDV referenced class codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_id
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_idv_referenced_class
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
-- assist_dev.ref.lu_idv_who_can_use
-- Reference data: Agency types that can use an IDV vehicle. SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_idv_who_can_use
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
-- assist_dev.ref.lu_small_business_admin
-- Reference data: SBA program codes — 8(a), HUBZone, WOSB, SDVOSB. SCD Type 1 — full refresh on change
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_small_business_admin
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
-- assist_dev.ref.lu_reason_for_direct
-- Reference data: J&A basis codes for other-than-full-and-open competition. SCD Type 1 — full refresh 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_reason_for_direct
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
-- assist_dev.ref.lu_cost_rate_type
-- Reference data: Labor cost rate types — Actual, Provisional, Fixed, Predefined. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_cost_rate_type
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
-- assist_dev.ref.lu_unit_of_measure
-- Reference data: Units of measure codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_unit_o
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_unit_of_measure
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
-- assist_dev.ref.lu_severability_type
-- Reference data: Contract severability codes — Severable, Nonseverable, Mix. SCD Type 1 — full refres
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_severability_type
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
-- assist_dev.ref.lu_commercial_type
-- Reference data: Client commercial type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_commercial_type
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
-- assist_dev.ref.lu_fob_type
-- Reference data: Free on Board (FOB) type codes — Origin, Destination. SCD Type 1 — full refresh on c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fob_type
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
-- assist_dev.ref.lu_fund
-- Reference data: GSA internal fund codes — 285X, 285F, etc. SCD Type 1 — full refresh on change. Sour
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fund
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
-- assist_dev.ref.lu_fund_amend_status
-- Reference data: Funding amendment status codes — Draft, Error, Final, Pending. SCD Type 1 — full ref
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_fund_amend_status
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
-- assist_dev.ref.lu_collab_type
-- Reference data: Collaboration type codes — Resume, Technical Report, Fund Request. SCD Type 1 — full
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_collab_type
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
-- assist_dev.ref.lu_collab_status
-- Reference data: Collaboration status codes by collaboration type. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_collab_status
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
-- assist_dev.ref.lu_checklist_type
-- Reference data: Closeout checklist type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_checklist_type
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
-- assist_dev.ref.lu_checklist_item
-- Reference data: Closeout checklist items with tooltip text and sort order. SCD Type 1 — full refresh
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_checklist_item
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
-- assist_dev.ref.lu_checklist_value
-- Reference data: Dropdown answer values for checklist items. SCD Type 1 — full refresh on change. Sou
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_checklist_value
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
-- assist_dev.ref.lu_closeout_prohibition
-- Reference data: Closeout prohibition codes blocking award closeout. SCD Type 1 — full refresh on cha
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_closeout_prohibition
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
-- assist_dev.ref.lu_event_type
-- Reference data: Lifecycle event types for chronology audit trail. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_event_type
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
-- assist_dev.ref.lu_email_status
-- Reference data: Email status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_email_stat
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_email_status
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
-- assist_dev.ref.lu_table_name
-- Reference data: AASBS table registry — used by CENTRAL_COMMENT cross-reference. SCD Type 1 — full re
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_table_name
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
-- assist_dev.ref.lu_transaction_type
-- Reference data: LOA ledger transaction type codes. SCD Type 1 — full refresh on change. Source: aasb
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transaction_type
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
-- assist_dev.ref.lu_emergency_acq
-- Reference data: FAR Part 18 emergency acquisition procedure indicator codes. SCD Type 1 — full refre
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_emergency_acq
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
-- assist_dev.ref.lu_expedited_type
-- Reference data: Expedited processing cause codes. SCD Type 1 — full refresh on change. Source: aasbs
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_expedited_type
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
-- assist_dev.ref.lu_continuity_type
-- Reference data: Continuity type codes with previous acquisition reference. SCD Type 1 — full refresh
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_continuity_type
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
-- assist_dev.ref.lu_processing_speed
-- Reference data: Processing speed codes — Routine, Expedited. SCD Type 1 — full refresh on change. So
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_processing_speed
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
-- assist_dev.ref.lu_intel_community
-- Reference data: Intelligence Community member indicator codes per DNI. SCD Type 1 — full refresh on 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_intel_community
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
-- assist_dev.ref.lu_subsystem
-- Reference data: ASSIST subsystem codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_subsys
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_subsystem
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
-- assist_dev.ref.lu_supply_chain_risk
-- Reference data: CMMC supply chain risk level codes (1–5). SCD Type 1 — full refresh on change. Sourc
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_supply_chain_risk
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
-- assist_dev.ref.lu_surveillance_spec
-- Reference data: DD-254 security classification specification indicator codes. SCD Type 1 — full refr
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_surveillance_spec
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
-- assist_dev.ref.lu_transfer
-- Reference data: Contract administration transfer indicator codes. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transfer
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
-- assist_dev.ref.lu_office_type
-- Reference data: Issuing office type codes — Procurement, Administration. SCD Type 1 — full refresh o
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_office_type
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
-- assist_dev.ref.lu_accrual_income_doc_type
-- Reference data: Accrual income document type codes for BAAR transmission. SCD Type 1 — full refresh 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_accrual_income_doc_type
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
-- assist_dev.ref.lu_onefund_program
-- Reference data: OneFund program codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_onefund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_onefund_program
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
-- assist_dev.ref.lu_onefund_activity
-- Reference data: OneFund activity codes with associated fund code. SCD Type 1 — full refresh on chang
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_onefund_activity
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
-- assist_dev.ref.lu_group_contact_type
-- Reference data: Group contact category codes — CONTRACTING, NATIONAL_FSD, etc. SCD Type 1 — full ref
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_group_contact_type
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
-- assist_dev.ref.lu_award_validation
-- Reference data: Award validation criteria with remedy and sort order. SCD Type 1 — full refresh on c
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_award_validation
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
-- assist_dev.ref.lu_funds_usage
-- Reference data: Fund usage purpose codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_fund
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_funds_usage
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
-- assist_dev.ref.lu_central_comment_type
-- Reference data: Comment type codes for CENTRAL_COMMENT table. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_central_comment_type
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
-- assist_dev.ref.lu_consolidated_contract
-- Reference data: FAR consolidated contract determination values. SCD Type 1 — full refresh on change.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_consolidated_contract
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
-- assist_dev.ref.lu_idv_who_can_use
-- Reference data: Agency eligibility types for IDV vehicle use. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_idv_who_can_use
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
-- assist_dev.ref.lu_transmittal_record_type
-- Reference data: Transmittal record type codes. SCD Type 1 — full refresh on change. Source: aasbs.lu
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transmittal_record_type
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
-- assist_dev.ref.lu_transmittal_stage
-- Reference data: Stages of releasing a transmission to Pegasys. SCD Type 1 — full refresh on change. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transmittal_stage
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
-- assist_dev.ref.lu_transmittal_status
-- Reference data: Transmission status codes. SCD Type 1 — full refresh on change. Source: aasbs.lu_tra
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transmittal_status
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
-- assist_dev.ref.lu_transmittal_type
-- Reference data: Transmittal types with flat-file format code. SCD Type 1 — full refresh on change. S
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.ref.lu_transmittal_type
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
