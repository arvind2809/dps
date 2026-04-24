# Databricks notebook source
# =============================================================================
# common/prime_dim_loa.py
# Primes assist_dev.common.dim_loa
#
# Source tables (Silver):
#   silver_aasbs_loa            → base LOA record
#   silver_aasbs_lu_loa_status  → loa_status_desc decoded
#   silver_aasbs_lu_fund_type   → fund_type_desc decoded
#
# Grain       : One row per loa.id
# SCD Type    : 2 — eff_start_dt=NOW, eff_end_dt=NULL, is_current_flag=TRUE
# Idempotent  : YES — TRUNCATE then INSERT
# Dependencies: dim_agency (for agency_sk)
#
# Treasury Account Symbol (TAS) construction:
#   The ASSIST loa table stores TAS components individually:
#     tsym_alloc_trans_agency_cd  — Allocation transfer agency (3 chars)
#     requesting_agency_cd         — Agency identifier (3 chars)
#     tsym_availability_type       — X, /, null (availability type)
#     appropriation                — Main account (4 chars)
#     treasury_sub_account         — Sub-account (3 chars)
#   Assembled TAS: {alloc_agency}-{agency_id}-{avail_type}{main_account}-{sub_account}
#   e.g. "014-014-X-0510-000"
#
# Fields not in source loa row (populated in delta refresh from billing/LOA detail):
#   agreement_line_num       → loa.ginv_line_num (CAST INT→STRING)
#   fund_cd                  → loa.customer_fund
#   object_class_cd          → NOT on loa; sourced from billing records (NULL on prime)
#   program_activity_cd      → NOT on loa (NULL on prime)
#   budget_activity_cd       → NOT on loa (NULL on prime)
#   org_code                 → loa.customer_act_num (approximation)
#   cost_center              → NOT on loa (NULL on prime)
#   project_code             → NOT on loa (NULL on prime)
#   bbfy                     → loa.first_fy_available_year
#   ebfy                     → loa.last_fy_available_year
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET     = gold("common", "dim_loa")
TASK       = "prime_dim_loa"

S_LOA      = silver("aasbs", "loa")
S_STATUS   = silver("aasbs", "lu_loa_status")
S_FUNDTYPE = silver("aasbs", "lu_fund_type")
G_AGENCY   = gold("common", "dim_agency")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                        source_schema="aasbs", source_table="loa")

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    # loa_sk is GENERATED ALWAYS AS IDENTITY — excluded from INSERT col list.
    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            loa_id, tracking_num,
            loa_status_cd, loa_status_desc,
            agreement_number, agreement_line_num,
            treasury_account_symbol,
            fund_cd, fund_type_cd, 
            -- fund_type_desc,
            object_class_cd,
            program_activity_cd, budget_activity_cd,
            org_code, cost_center, project_code,
            bbfy, ebfy,
            agreement_end_dt, transmitted_dt,
            agency_sk,
            eff_start_dt, eff_end_dt, is_current_flag,
            _gold_created_at, _gold_updated_at, _source_batch_id
        )
        WITH loa_base AS (
            SELECT
                loa.id                              AS loa_id,
                loa.tracking_num,
                loa.loa_status_cd,
                loa.agreement_number,
                -- ginv_line_num is the BAAR/GINV line number (INTEGER → STRING)
                CAST(loa.ginv_line_num AS STRING)   AS agreement_line_num,

                -- ── Treasury Account Symbol assembly ─────────────────────────
                -- Standard federal TAS format: ATA-CGAC-AVAIL-MAIN-SUB
                -- ATA  = Allocation Transfer Agency (tsym_alloc_trans_agency_cd)
                -- CGAC = Common Govt-wide Accounting Classification (requesting_agency_cd)
                -- AVAIL= Availability type: X=no-year, /=annual, numeric=multi-year
                -- MAIN = Main account number (appropriation, 4 chars)
                -- SUB  = Sub-account (treasury_sub_account, 3 chars)
                CONCAT_WS('-',
                    NULLIF(loa.tsym_alloc_trans_agency_cd, ''),
                    NULLIF(loa.requesting_agency_cd, ''),
                    CONCAT(
                        COALESCE(loa.tsym_availability_type, ''),
                        COALESCE(loa.appropriation, '')
                    ),
                    NULLIF(loa.treasury_sub_account, '')
                )                                   AS treasury_account_symbol,

                -- Fund code: customer_fund is the agency's internal fund identifier
                loa.customer_fund                   AS fund_cd,
                loa.fund_type_cd,

                -- Fields not on loa row — placeholders for delta refresh enrichment
                CAST(NULL AS STRING)                AS object_class_cd,       -- from billing/BAAR
                CAST(NULL AS STRING)                AS program_activity_cd,   -- from budget data
                CAST(NULL AS STRING)                AS budget_activity_cd,    -- from budget data
                loa.customer_act_num                AS org_code,              -- ACT number
                CAST(NULL AS STRING)                AS cost_center,
                CAST(NULL AS STRING)                AS project_code,

                -- Fiscal year availability window
                loa.first_fy_available_year         AS bbfy,
                loa.last_fy_available_year          AS ebfy,

                loa.agreement_end_dt,
                loa.transmitted_dt,
                -- requesting_agency_cd is the agency owning this LOA (3-char)
                loa.requesting_agency_cd            AS requesting_agency_cd_raw
            FROM {S_LOA} loa
            WHERE loa.is_deleted = FALSE
        ),
        with_decoded AS (
            SELECT
                lb.*,
                COALESCE(st.description, lb.loa_status_cd)   AS loa_status_desc,
                COALESCE(ft.description, lb.fund_type_cd)     AS fund_type_desc
            FROM loa_base lb
            LEFT JOIN {S_STATUS}   st ON lb.loa_status_cd = st.cd
                AND st.is_deleted = FALSE
            LEFT JOIN {S_FUNDTYPE} ft ON lb.fund_type_cd   = ft.cd
                AND ft.is_deleted = FALSE
        ),
        with_agency_sk AS (
            SELECT
                wd.*,
                -- Resolve agency_sk from dim_agency.
                -- LOA requesting_agency_cd is a 3-char code; dim_agency.agency_code
                -- is the 2-char FPDS code.  Match on the LEFT 2 chars as best-effort.
                -- NOTE: A more precise match uses the AAC stored in the LOA's
                --       creator_funding_amendment → funding → ia → activity_address_cd.
                --       That multi-hop join is deferred to delta refresh.
                ag.agency_sk
            FROM with_decoded wd
            LEFT JOIN {G_AGENCY} ag
                ON LEFT(wd.requesting_agency_cd_raw, 2) = ag.agency_code
                AND ag.is_current_flag = TRUE
        )
        SELECT
            loa_id,
            tracking_num,
            loa_status_cd,
            loa_status_desc,
            agreement_number,
            agreement_line_num,
            treasury_account_symbol,
            fund_cd,
            fund_type_cd,
            -- fund_type_desc,
            object_class_cd,
            program_activity_cd,
            budget_activity_cd,
            org_code,
            cost_center,
            project_code,
            bbfy,
            ebfy,
            agreement_end_dt,
            transmitted_dt,
            agency_sk,
            -- SCD2 initial values
            CURRENT_TIMESTAMP()             AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)         AS eff_end_dt,
            TRUE                            AS is_current_flag,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM with_agency_sk
    """)

    n = row_count(spark, TARGET)
    no_tas = spark.sql(
        f"SELECT COUNT(*) AS n FROM {TARGET} WHERE treasury_account_symbol IS NULL"
    ).collect()[0]["n"]
    print(f"  [OK] Inserted {n:,} LOA rows ({no_tas} with NULL TAS — check appropriation fields)")

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
