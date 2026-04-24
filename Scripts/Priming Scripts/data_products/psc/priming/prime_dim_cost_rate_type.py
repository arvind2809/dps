# Databricks notebook source
# =============================================================================
# psc/prime_dim_cost_rate_type.py
# Primes assist_dev.psc.dim_cost_rate_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per cost_rate_type_cd  (natural key)
# SCD Type : 1 — reference data updated in-place
#
# Source tables (Silver):
#   silver_aasbs_lu_cost_rate_type  → all cost rate type reference rows
#
# Field mapping (confirmed against Silver DDL):
#   cost_rate_type_cd   ← lu_cost_rate_type.cd                  (NK)
#   cost_rate_type_desc ← lu_cost_rate_type.description
#   is_actual_flag      ← CASE WHEN UPPER(cd) IN ('ACTUAL','ACT','ACTUAL_RATE')
#                              THEN TRUE ELSE FALSE END
#                         [IMPROVEMENT I-DP6-4: post-load distribution printed
#                          for operator validation of actual-rate code values]
#
# IMPROVEMENT I-DP6-4 (built-in):
#   is_actual_flag is derived from cost_rate_type_cd using a CASE expression
#   that matches known "actual" rate codes.  Because this is a lookup table,
#   the exact cd values may vary between ASSIST environments.  A post-load
#   distribution check prints every distinct cd with its is_actual_flag
#   assignment so operators can confirm the derivation is correct.
#   If unexpected codes appear, update the IN() list in this notebook.
#
# Ref: FAR 15.404 (price reasonableness) — cost rate types determine whether
#      labour rates represent actual incurred costs or pre-negotiated estimates.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP6"
NOTEBOOK     = "prime_dim_cost_rate_type"
TARGET_TABLE = "assist_dev.psc.dim_cost_rate_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_lu_cost_rate_type")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_cost_rate_type
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_cost_rate_type rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            cost_rate_type_cd,
            cost_rate_type_desc,
            is_actual_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            lc.cd                                               AS cost_rate_type_cd,

            -- Human-readable description of the rate type
            lc.description                                      AS cost_rate_type_desc,

            -- IMPROVEMENT I-DP6-4: is_actual_flag derived from cd.
            -- 'Actual' rate types represent incurred costs (FAR 15.404).
            -- Known actual-rate codes listed below.  Post-load distribution
            -- check will flag any unhandled codes for operator review.
            CASE
                WHEN UPPER(TRIM(lc.cd)) IN (
                    'ACTUAL', 'ACT', 'ACTUAL_RATE', 'ACTUALS'
                ) THEN TRUE
                ELSE FALSE
            END                                                 AS is_actual_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lu_cost_rate_type lc
        WHERE COALESCE(lc.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP6-4: full distribution check for operator validation
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP6-4 — cost_rate_type_cd distribution:")
    dist = spark.sql(f"""
        SELECT
            cost_rate_type_cd,
            cost_rate_type_desc,
            is_actual_flag,
            COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY cost_rate_type_cd, cost_rate_type_desc, is_actual_flag
        ORDER BY is_actual_flag DESC, cost_rate_type_cd
    """).collect()
    for row in dist:
        flag_label = "✓ ACTUAL" if row[2] else "  estimated"
        print(f"  {str(row[0]):<20}  {str(row[1]):<35}  {flag_label}")

    actual_count = sum(1 for row in dist if row[2])
    if actual_count == 0:
        print(
            f"\n  ⚠ WARNING: no cost_rate_type_cd matched the is_actual_flag=TRUE "
            f"CASE expression. Review the IN() list in this notebook against the "
            f"actual source code values printed above."
        )
    else:
        print(f"\n  ✓ {actual_count} rate type(s) classified as actual-cost rates.")

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
