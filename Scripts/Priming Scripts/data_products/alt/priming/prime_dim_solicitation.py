# Databricks notebook source
# =============================================================================
# alt/prime_dim_solicitation.py
# Primes assist_dev.alt.dim_solicitation
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per solicit.id
# SCD Type : 1 — status and counts updated in-place
#
# Source tables (Silver):
#   silver_aasbs_solicit                → base solicitation record
#   silver_aasbs_solicit_amendment      → amendment count (aggregated)
#   silver_aasbs_solicit_response       → total_responses (IMPROVEMENT #8)
#   silver_aasbs_lu_sol_status          → sol_status_desc decode
#   silver_aasbs_lu_sol_posting_type    → sol_posting_type_desc decode
#   assist_dev.alt.dim_acquisition  → acquisition_sk lookup
#   assist_dev.common.dim_agency    → agency_sk lookup
#
# IMPROVEMENT #8 (v1.1.0):
#   total_responses now populated on prime from silver_aasbs_solicit_response.
#   silver_aasbs_solicit_response is confirmed present in Silver DDL with
#   columns: id, solicit_id, solicit_response_num, winning_response_yn, etc.
#   COUNT(*) per solicit_id gives the accurate response count at prime time.
#   Previously seeded as 0 with note "solicit_response not in DP2 Silver scope"
#   — the Silver table IS present; the scope limitation was incorrect.
#
# NULL on prime (by design — enriched by CDC):
#   winning_response_id — post-award response-to-award linkage (CDC scope)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP2"
NOTEBOOK     = "prime_dim_solicitation"
TARGET_TABLE = "assist_dev.alt.dim_solicitation"

SILVER = "assist_dev.assist_finance"
GOLD   = "assist_dev.alt"
COMMON = "assist_dev.common"

# ADDED for utils
TASK   = "prime_dim_closeout"
dbutils.widgets.text("job_name", "dp2_prime_full", "Job Name")
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_solicit")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_solicit"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver solicit row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Pre-aggregate amendment counts (fan-out prevention)
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_amendment_counts AS
        SELECT
            solicit_id,
            COUNT(*) AS total_amendments
        FROM {SILVER}.silver_aasbs_solicit_amendment
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY solicit_id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — IMPROVEMENT #8: pre-aggregate response counts
    #
    # silver_aasbs_solicit_response confirmed in Silver DDL with solicit_id FK.
    # COUNT(*) per solicit_id gives the total number of vendor responses.
    # Previously this was always 0 (incorrect scope restriction removed).
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_response_counts AS
        SELECT
            solicit_id,
            COUNT(*)                                             AS total_responses,
            -- winning_response_yn: flag on solicit_response for the award winner.
            -- Capture the winning record's id for winning_response_id resolution.
            -- NULL if no winning response has been designated yet.
            MAX(CASE WHEN winning_response_yn = 'Y' THEN id ELSE NULL END)
                                                                AS winning_response_id
        FROM {SILVER}.silver_aasbs_solicit_response
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY solicit_id
    """)

    print(f"[{NOTEBOOK}] Amendment and response counts aggregated.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            solicit_id,
            solicit_num,
            sol_status_cd,
            sol_status_desc,
            sol_posting_type_cd,
            sol_posting_type_desc,
            open_dt,
            close_dt,
            total_amendments,
            total_responses,
            winning_response_id,
            acquisition_sk,
            agency_sk,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            s.id                                                        AS solicit_id,
            s.at_award_bid_cnt,
            s.sol_status_cd                                                AS sol_status_cd,
            COALESCE(s.solicit_description,   s.sol_status_cd)              AS sol_status_desc,
            -- s.sol_posting_type                                          AS sol_posting_type_cd,
            lsa.sol_posting_type_cd,
            COALESCE(lspt.description, lsa.sol_posting_type_cd)   AS sol_posting_type_desc,
            -- s.open_dt,
            -- s.close_dt,
            lsa.created_dt,
            lsa.close_dt,
            -- Amendment count (0 if no amendments exist for this solicitation)
            COALESCE(ac.total_amendments, 0)                           AS total_amendments,
            -- IMPROVEMENT #8: total_responses now sourced from solicit_response.
            -- Previously hard-coded 0.  Now COUNT(*) per solicit_id.
            COALESCE(rc.total_responses, 0)                            AS total_responses,
            -- winning_response_id: first designated winner from solicit_response.
            -- NULL if no response marked winning_response_yn = 'Y' yet.
            rc.winning_response_id                                     AS winning_response_id,
            -- acquisition_sk: resolved from alt.dim_acquisition (must already exist)
            COALESCE(da.acquisition_sk, -1)                            AS acquisition_sk,
            -- agency_sk: resolved from common.dim_agency via AAC
            COALESCE(dag.agency_sk, -1)                                AS agency_sk,
            current_timestamp()                                        AS _gold_created_at,
            current_timestamp()                                        AS _gold_updated_at,
            '{RUN_ID}'                                                 AS _source_batch_id

        FROM {SILVER}.silver_aasbs_solicit s

        LEFT JOIN v_amendment_counts ac
            ON  ac.solicit_id = s.id

        -- IMPROVEMENT #8: join response counts
        LEFT JOIN v_response_counts rc
            ON  rc.solicit_id = s.id

        -- LEFT JOIN {SILVER}.silver_aasbs_lu_sol_status lss
        --    ON  lss.sol_status = s.sol_status
            
        LEFT JOIN {SILVER}.silver_aasbs_solicit_amendment lsa
            ON  lsa.solicit_id = s.id

        JOIN {SILVER}.silver_aasbs_lu_sol_posting_type lspt
            ON  lspt.cd = lsa.sol_posting_type_cd

        LEFT JOIN {GOLD}.dim_acquisition da
            ON  da.acquisition_id  = s.acquisition_id
            AND da.is_current_flag = TRUE

        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = s.sol_response_aac_visibility_cd
            AND dag.is_current_flag       = TRUE

        WHERE COALESCE(s.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    quality = spark.sql(f"""
        SELECT
            SUM(CASE WHEN acquisition_sk = -1   THEN 1 ELSE 0 END) AS unresolved_acq,
            SUM(CASE WHEN agency_sk      = -1   THEN 1 ELSE 0 END) AS unresolved_agency,
            SUM(CASE WHEN total_responses > 0   THEN 1 ELSE 0 END) AS solicitations_with_responses,
            SUM(total_responses)                                    AS grand_total_responses,
            SUM(CASE WHEN winning_response_id IS NOT NULL
                     THEN 1 ELSE 0 END)                             AS with_winner
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(f"[{NOTEBOOK}] Sentinel FKs — "
          f"acquisition_sk=-1: {quality[0]:,}, agency_sk=-1: {quality[1]:,}")
    print(f"[{NOTEBOOK}] Solicitations with responses : {quality[2]:,}  "
          f"(IMPROVEMENT #8 — was 0 in v1.0)")
    print(f"[{NOTEBOOK}] Grand total responses        : {quality[3]:,}  "
          f"(IMPROVEMENT #8)")
    print(f"[{NOTEBOOK}] With winning response        : {quality[4]:,}")

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)

    raise
