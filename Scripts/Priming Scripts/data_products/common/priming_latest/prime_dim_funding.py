# Databricks notebook source
# =============================================================================
# common/prime_dim_funding.py
# Primes assist_dev.common.dim_funding
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per funding.id joined to its latest funding_amendment
# SCD Type : 2  (eff_start_dt, eff_end_dt, is_current_flag)
#
# Source tables (Silver):
#   silver_aasbs_funding               → base funding record
#   silver_aasbs_funding_amendment     → latest amendment status, fiscal year
#   silver_aasbs_funding_amendment_loa → SUM(loa_change_amt) for total_funded_amt
#   silver_aasbs_lu_fund_status        → fund_status_desc decode
#   silver_aasbs_lu_fund_category      → fund_category_desc decode
#   silver_aasbs_lu_billing_type       → billing_type_desc decode
#   assist_dev.common.dim_ia       → ia_sk resolution
#
# IMPROVEMENT #4 (v1.1.0):
#   total_funded_amt is now derived on prime.
#   Path: funding → funding_amendment → funding_amendment_loa → SUM(loa_change_amt)
#   Each loa_change_amt is the dollar amount added to a LOA by a specific
#   funding amendment tranche.  Summing all loa_change_amt for all amendments
#   under a funding package gives the total funded amount.
#   silver_aasbs_funding_amendment_loa confirmed present in Silver DDL with
#   columns: id, funding_amendment_id, loa_id, loa_change_amt.
#   Previously NULL on prime (described as "computed from loa_ledger in fact
#   notebook") — that was incorrect; the correct source is funding_amendment_loa.
#
# NULL on prime (by design — enriched by delta refresh):
#   fund_type_cd   — not on funding row; inherited from LOA fund_type_cd
#   fund_type_desc — same
#   fiscal_year    — from funding_amendment.ginv_pop_start_dt in delta refresh
#                    (amendment status provides fund_amend_status_cd not FY)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_funding")
TASK     = "prime_dim_funding"

# Silver sources
S_FUND   = silver("aasbs", "funding")
S_AMEND  = silver("aasbs", "funding_amendment")
S_AMLOA  = silver("aasbs", "funding_amendment_loa")   # IMPROVEMENT #4
S_FSTAT  = silver("aasbs", "lu_fund_status")
S_FCAT   = silver("aasbs", "lu_fund_category")
S_BT     = silver("aasbs", "lu_billing_type")

# Gold dim (must exist — primed by prime_dim_ia earlier in DP1 job)
G_IA     = gold("common", "dim_ia")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="aasbs", source_table="funding",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    # ─────────────────────────────────────────────────────────────────────
    # Step 1: IMPROVEMENT #4 — derive total_funded_amt from amendment LOAs
    #
    # Path: funding_amendment.funding_id → funding_amendment_loa.funding_amendment_id
    #       → SUM(loa_change_amt) per funding_id
    #
    # loa_change_amt is the dollar amount added to a specific LOA by this
    # amendment tranche.  Summing across all amendments and all LOAs under a
    # funding package gives the total amount funded against that package.
    # Negative loa_change_amt values represent LOA reductions (valid).
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_total_funded AS
        SELECT
            fa.funding_id,
            SUM(COALESCE(fal.loa_change_amt, 0.00)) AS total_funded_amt
        FROM {S_AMEND} fa
        JOIN {S_AMLOA} fal
            ON  fal.funding_amendment_id = fa.id
            AND COALESCE(fal.is_deleted, FALSE) = FALSE
        WHERE COALESCE(fa.is_deleted, FALSE) = FALSE
        GROUP BY fa.funding_id
    """)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            funding_id,
            funding_amendment_id,
            fund_status_cd,
            fund_status_desc,
            fund_category_cd,
            fund_category_desc,
            billing_type_cd,
            billing_type_desc,
            fund_type_cd,
            fund_type_desc,
            ia_sk,
            agency_sk,
            total_funded_amt,
            fiscal_year,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 2: Funding base with latest amendment
        -- ─────────────────────────────────────────────────────────────────
        WITH funding_base AS (
            SELECT
                f.id                                AS funding_id,
                f.ia_id,
                f.fund_status_cd,
                f.fund_category_cd,
                f.billing_type_cd,
                f.latest_funding_amendment_id       AS funding_amendment_id
            FROM {S_FUND} f
            WHERE COALESCE(f.is_deleted, FALSE) = FALSE
        ),

        with_amendment AS (
            SELECT
                fb.*,
                fa.fund_amend_status_cd,
                -- fiscal_year: not directly on amendment; sourced from
                -- ginv_pop_start_dt in delta refresh. NULL on prime by design.
                CAST(NULL AS INT)                   AS fiscal_year
            FROM funding_base fb
            LEFT JOIN {S_AMEND} fa
                ON  fa.id = fb.funding_amendment_id
                AND COALESCE(fa.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 3: Decode status/category/billing descriptions
        -- ─────────────────────────────────────────────────────────────────
        with_decoded AS (
            SELECT
                wa.*,
                COALESCE(fs.description, wa.fund_status_cd)    AS fund_status_desc,
                COALESCE(fc.description, wa.fund_category_cd)  AS fund_category_desc,
                COALESCE(bt.description, wa.billing_type_cd)   AS billing_type_desc,
                -- fund_type_cd: inherited from LOA; not on funding row.
                -- NULL on prime by design.
                CAST(NULL AS STRING)                           AS fund_type_cd,
                CAST(NULL AS STRING)                           AS fund_type_desc
            FROM with_amendment wa
            LEFT JOIN {S_FSTAT} fs
                ON  fs.cd = wa.fund_status_cd
                AND COALESCE(fs.is_deleted, FALSE) = FALSE
            LEFT JOIN {S_FCAT} fc
                ON  fc.cd = wa.fund_category_cd
                AND COALESCE(fc.is_deleted, FALSE) = FALSE
            LEFT JOIN {S_BT} bt
                ON  bt.cd = wa.billing_type_cd
                AND COALESCE(bt.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 4: Resolve ia_sk and agency_sk from dim_ia
        -- ─────────────────────────────────────────────────────────────────
        with_ia_sk AS (
            SELECT
                wd.*,
                COALESCE(ia.ia_sk, -1)              AS ia_sk,
                -- agency_sk: inherited from ia.servicing_agency_sk
                COALESCE(ia.servicing_agency_sk, -1) AS agency_sk
            FROM with_decoded wd
            LEFT JOIN {G_IA} ia
                ON  ia.ia_id           = wd.ia_id
                AND ia.is_current_flag = TRUE
        )

        SELECT
            wia.funding_id,
            funding_amendment_id,
            fund_status_cd,
            fund_status_desc,
            fund_category_cd,
            fund_category_desc,
            billing_type_cd,
            billing_type_desc,
            fund_type_cd,           -- NULL on prime — see header
            fund_type_desc,         -- NULL on prime — see header
            ia_sk,
            agency_sk,
            -- IMPROVEMENT #4: total_funded_amt now derived from amendment LOAs
            -- Previously CAST(NULL AS DECIMAL(15,2)) — see header for derivation
            COALESCE(tf.total_funded_amt, 0.00) AS total_funded_amt,
            fiscal_year,            -- NULL on prime — see header
            CURRENT_TIMESTAMP()     AS eff_start_dt,
            CAST(NULL AS TIMESTAMP) AS eff_end_dt,
            TRUE                    AS is_current_flag,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM with_ia_sk wia
        -- IMPROVEMENT #4: join pre-aggregated total_funded_amt
        LEFT JOIN v_total_funded tf
            ON  tf.funding_id = wia.funding_id
    """)

    # ── Post-load quality checks ───────────────────────────────────────────
    n = row_count(spark, TARGET)

    quality = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total_rows,
            SUM(CASE WHEN ia_sk = -1                      THEN 1 ELSE 0 END)    AS sentinel_ia_sk,
            SUM(CASE WHEN total_funded_amt IS NOT NULL
                     AND total_funded_amt  > 0            THEN 1 ELSE 0 END)    AS positive_funded,
            SUM(CASE WHEN total_funded_amt IS NULL        THEN 1 ELSE 0 END)    AS null_funded,
            ROUND(SUM(COALESCE(total_funded_amt, 0)), 2)                        AS grand_total_funded
        FROM {TARGET}
    """).collect()[0]

    print(f"  [OK] Inserted {n:,} funding rows")
    print(f"  sentinel ia_sk=-1     : {quality['sentinel_ia_sk']:,}")
    print(f"  positive total_funded : {quality['positive_funded']:,}  "
          f"(IMPROVEMENT #4 — was 0 in v1.0)")
    print(f"  null total_funded     : {quality['null_funded']:,}  "
          f"(funding packages with no LOA amendments)")
    print(f"  grand total funded    : ${quality['grand_total_funded']:,.2f}")

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
