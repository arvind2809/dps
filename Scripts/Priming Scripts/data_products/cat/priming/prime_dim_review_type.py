# Databricks notebook source
# =============================================================================
# cat/prime_dim_review_type.py
# Primes assist_dev.cat.dim_review_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_review_type.cd
#
# Source: silver_aasbs_lu_review_type  (cd, description, sort_order, active_yn)
#
# IMPROVEMENT I-DP10-1 — Business-rule derived fields:
#   is_pre_award_flag  ← CASE on review_type_cd
#     Legal, Technical, Price, Competition reviews apply pre-award.
#   is_mandatory_flag  ← CASE on review_type_cd
#     Some review types are mandatory per GSA policy (Legal, Price, J&A).
#   far_basis          ← CASE on review_type_cd
#     FAR clause or GSA policy basis for each review type.
#
# Post-load distribution validates all three assignments.
# Ref: FAR Part 15 (price/technical review), FAR 6.302 (J&A), OMB A-123
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP10"
NOTEBOOK     = "prime_dim_review_type"
TARGET_TABLE = "assist_dev.cat.dim_review_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_review_type
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_review_type rows: {rows_read:,}")

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (review_type_cd, review_type_desc, is_pre_award_flag,
         is_mandatory_flag, far_basis,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            lrt.cd                                              AS review_type_cd,
            lrt.description                                     AS review_type_desc,

            -- IMPROVEMENT I-DP10-1: is_pre_award_flag — no Silver source.
            -- Legal, Technical, Price, Competition reviews apply pre-award.
            CASE
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%LEGAL%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%TECH%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%PRICE%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%COMPETITION%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%JA%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%JUSTIF%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS is_pre_award_flag,

            -- IMPROVEMENT I-DP10-1: is_mandatory_flag — no Silver source.
            -- Legal review, Price review, and J&A review are mandatory per GSA policy.
            CASE
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%LEGAL%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%PRICE%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%JA%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%JUSTIF%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS is_mandatory_flag,

            -- IMPROVEMENT I-DP10-1: far_basis — no Silver source.
            -- FAR clause or GSA policy basis for each review type.
            CASE
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%LEGAL%'
                    THEN 'GSA Legal Review Policy'
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%PRICE%'
                    THEN 'FAR 15.404'
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%TECH%'
                    THEN 'FAR 15.304'
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%JA%'
                  OR UPPER(TRIM(lrt.cd)) LIKE '%JUSTIF%'
                    THEN 'FAR 6.302'
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%COMPETITION%'
                    THEN 'FAR 6.101'
                WHEN UPPER(TRIM(lrt.cd)) LIKE '%OIG%'
                    THEN '5 U.S.C. App. 3'
                ELSE NULL
            END                                                 AS far_basis,

            current_timestamp(), current_timestamp(), '{RUN_ID}'

        FROM {SILVER}.silver_aasbs_lu_review_type lrt
        WHERE COALESCE(lrt.is_deleted, FALSE) = FALSE
    """)

    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP10-1: distribution check
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP10-1 — review_type_cd distribution:")
    print(f"  {'Code':<30} {'Pre-Award':>10} {'Mandatory':>10} {'FAR Basis':<30}")
    print(f"  {'─'*30} {'─'*10} {'─'*10} {'─'*30}")
    dist = spark.sql(f"""
        SELECT review_type_cd, is_pre_award_flag, is_mandatory_flag, far_basis
        FROM {TARGET_TABLE} ORDER BY review_type_cd
    """).collect()
    mand_cnt = sum(1 for r in dist if r[2])
    for row in dist:
        p = "TRUE " if row[1] else "FALSE"
        m = "TRUE " if row[2] else "FALSE"
        f = str(row[3])[:28] if row[3] else "—"
        print(f"  {str(row[0]):<30} {p:>10} {m:>10} {f:<30}")
    print(f"\n  mandatory_count={mand_cnt}")
    if mand_cnt == 0:
        print(
            f"  ⚠ WARNING: no review types flagged as mandatory. "
            f"Update CASE expressions and re-prime (idempotent)."
        )

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
