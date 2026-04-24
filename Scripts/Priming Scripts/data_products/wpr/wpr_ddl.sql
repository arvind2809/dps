-- ==============================================================================
-- DP8 — assist_dev.wpr — Workforce & Personnel Registry
-- Grain: user role assignment per entity. COR delegation tracking. Workload analytics.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.wpr.dim_user
-- ASSIST user dimension. SCD Type 2 — tracks org, region, and role changes. Sources: assist.users, ass
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_user
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
-- assist_dev.wpr.dim_org_hierarchy
-- Employee org hierarchy dimension — full tree structure for workload by region/division. SCD Type 1. 
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_org_hierarchy
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
-- assist_dev.wpr.dim_responsible_role
-- Responsible role dimension — application business roles with FAR delegation requirements. SCD Type 1
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_responsible_role
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
-- assist_dev.wpr.dim_entity_type
-- Entity type dimension — types of entities a user can be responsible for. SCD Type 1.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_entity_type
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
-- assist_dev.wpr.dim_client
-- Client agency registry dimension. SCD Type 2. Source: table_master.clients.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_client
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
-- assist_dev.wpr.dim_industry_partner
-- Industry partner registry dimension. SCD Type 2. Sources: table_master.industry_partners, table_mast
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.dim_industry_partner
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
-- assist_dev.wpr.fact_role_assignment
-- Grain  : User (user_sk) × role (responsible_role_sk) × entity (source_entity_id)
-- Role assignment fact — all CO/COR/FMA/PM personnel assignments across awards, IAs, funding, solicita
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.wpr.fact_role_assignment
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
-- VIEW: assist_dev.wpr.v_cor_delegation_compliance
-- COR delegation compliance view — identifies COR assignments missing written delegation per FAR 1.602
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.wpr.v_cor_delegation_compliance
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
FROM assist_dev.wpr.fact_role_assignment f
JOIN assist_dev.wpr.dim_user              u   ON f.user_sk              = u.user_sk              AND u.is_current_flag
JOIN assist_dev.wpr.dim_responsible_role  rr  ON f.responsible_role_sk  = rr.responsible_role_sk
JOIN assist_dev.common.dim_agency         ag  ON f.agency_sk            = ag.agency_sk           AND ag.is_current_flag
JOIN assist_dev.common.dim_ia             ia  ON f.ia_sk                = ia.ia_sk               AND ia.is_current_flag
WHERE f.assignment_status_cd = 'Active'
  AND rr.responsible_role_cd = 'COR';

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.wpr.v_workload_by_region
-- Workload by region view — staff counts, award loads, and obligation totals by org and role. Supports
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.wpr.v_workload_by_region
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
FROM assist_dev.wpr.fact_role_assignment f
JOIN assist_dev.wpr.dim_org_hierarchy     org ON f.org_sk             = org.org_sk
JOIN assist_dev.wpr.dim_responsible_role  rr  ON f.responsible_role_sk = rr.responsible_role_sk
WHERE f.assignment_status_cd = 'Active'
GROUP BY ALL
ORDER BY org.region_cd, rr.responsible_role_desc;

