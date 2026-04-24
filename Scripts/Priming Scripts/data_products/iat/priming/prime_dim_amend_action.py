# Databricks notebook source
# =============================================================================
# iat/prime_dim_amend_action.py
# Primes assist_dev.iat.dim_amend_action
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per lu_ia_amend_action.cd  (natural key)
# SCD Type : 1 — reference data updated in-place
#
# Source tables (Silver):
#   silver_aasbs_lu_ia_amend_action  →  base reference rows
#
# Field mapping (confirmed against Silver DDL):
#   amend_action_cd    ← lu_ia_amend_action.cd           (NK)
#   amend_action_desc  ← lu_ia_amend_action.description
#   changes_scope_flag ← BUSINESS RULE derivation from amend_action_cd
#   changes_cost_flag  ← BUSINESS RULE derivation from amend_action_cd
#   changes_period_flag← BUSINESS RULE derivation from amend_action_cd
#
# IMPROVEMENT I-DP9-1 — Boolean flag business-rule derivation (built-in):
#   lu_ia_amend_action carries only cd, description, and active_yn — identical
#   to every other ASSIST lookup table.  There are no flag columns in Silver.
#   The three boolean flags must be derived from amend_action_cd using a CASE
#   expression based on known amendment action codes.
#
#   Because the exact production code values are not separately documented,
#   the post-load check prints a FULL DISTRIBUTION of every distinct cd loaded
#   with its flag assignments.  Operators must validate this output and update
#   the CASE expression if production codes differ from the assumed values.
#
#   Assumed code-to-flag mapping (adjust if production codes differ):
#     contains 'SCOPE'   or 'WORK'    → changes_scope_flag  = TRUE
#     contains 'COST'    or 'BUDGET'  → changes_cost_flag   = TRUE
#     contains 'PERIOD'  or 'EXTEND'  or 'POP' → changes_period_flag = TRUE
#     contains 'ADMIN'   or 'CORRECT' → all three flags = FALSE
#
#   This notebook is TRUNCATE→INSERT — idempotent.  After reviewing the
#   post-load distribution, re-run the driver to apply corrected flag logic.
#
# Ref: FAR Part 17.5 (Economy Act orders), 31 U.S.C. §1535
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP9"
NOTEBOOK     = "prime_dim_amend_action"
TARGET_TABLE = "assist_dev.iat.dim_amend_action"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_lu_ia_amend_action")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_ia_amend_action
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_ia_amend_action rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            amend_action_cd,
            amend_action_desc,
            changes_scope_flag,
            changes_cost_flag,
            changes_period_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: amendment action code
            laa.cd                                              AS amend_action_cd,

            -- Full description of the amendment action type
            laa.description                                     AS amend_action_desc,

            -- IMPROVEMENT I-DP9-1: Boolean flags derived from amend_action_cd.
            -- No flag columns exist on lu_ia_amend_action in Silver.
            -- CASE expression uses UPPER() for case-insensitive matching.
            -- Post-load distribution validates this derivation against actual
            -- production codes — adjust the CASE expressions if codes differ.

            -- changes_scope_flag: TRUE when amendment modifies the scope of work
            CASE
                WHEN UPPER(TRIM(laa.cd)) LIKE '%SCOPE%'
                  OR UPPER(TRIM(laa.cd)) LIKE '%WORK%'
                THEN TRUE
                ELSE FALSE
            END                                                 AS changes_scope_flag,

            -- changes_cost_flag: TRUE when amendment changes cost or ceiling
            CASE
                WHEN UPPER(TRIM(laa.cd)) LIKE '%COST%'
                  OR UPPER(TRIM(laa.cd)) LIKE '%BUDGET%'
                  OR UPPER(TRIM(laa.cd)) LIKE '%FEE%'
                THEN TRUE
                ELSE FALSE
            END                                                 AS changes_cost_flag,

            -- changes_period_flag: TRUE when amendment extends or changes PoP
            CASE
                WHEN UPPER(TRIM(laa.cd)) LIKE '%PERIOD%'
                  OR UPPER(TRIM(laa.cd)) LIKE '%EXTEND%'
                  OR UPPER(TRIM(laa.cd)) LIKE '%POP%'
                THEN TRUE
                ELSE FALSE
            END                                                 AS changes_period_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lu_ia_amend_action laa
        WHERE COALESCE(laa.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # IMPROVEMENT I-DP9-1: full distribution for operator validation
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP9-1 — amend_action_cd flag distribution:")
    print(f"  {'Code':<35} {'Scope':>6} {'Cost':>6} {'Period':>7}  Description")
    print(f"  {'─'*35} {'─'*6} {'─'*6} {'─'*7}  {'─'*40}")

    dist = spark.sql(f"""
        SELECT
            amend_action_cd,
            amend_action_desc,
            changes_scope_flag,
            changes_cost_flag,
            changes_period_flag
        FROM {TARGET_TABLE}
        ORDER BY amend_action_cd
    """).collect()

    scope_cnt  = 0
    cost_cnt   = 0
    period_cnt = 0
    all_false  = 0

    for row in dist:
        s = "TRUE " if row[2] else "FALSE"
        c = "TRUE " if row[3] else "FALSE"
        p = "TRUE " if row[4] else "FALSE"
        if row[2]:  scope_cnt  += 1
        if row[3]:  cost_cnt   += 1
        if row[4]:  period_cnt += 1
        if not any([row[2], row[3], row[4]]):
            all_false += 1
        print(f"  {str(row[0]):<35} {s:>6} {c:>6} {p:>7}  {str(row[1])[:40]}")

    print(f"\n  Codes with scope_flag=TRUE  : {scope_cnt}")
    print(f"  Codes with cost_flag=TRUE   : {cost_cnt}")
    print(f"  Codes with period_flag=TRUE : {period_cnt}")
    print(f"  Codes with ALL flags=FALSE  : {all_false}")

    if scope_cnt == 0 and cost_cnt == 0 and period_cnt == 0:
        print(
            f"\n  ⚠ WARNING: all {rows_written} codes have ALL boolean flags = FALSE. "
            f"This indicates the CASE expression does not match production "
            f"amend_action_cd values. Review the distribution above, update "
            f"the CASE expressions in this notebook, then re-prime (idempotent)."
        )
    else:
        print(
            f"\n  ✓ At least one flag = TRUE. "
            f"Review distribution above to confirm flag assignments are correct."
        )

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
