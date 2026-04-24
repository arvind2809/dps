# Databricks notebook source
# =============================================================================
# cat/prime_dim_finding.py
# Primes assist_dev.cat.dim_finding
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_review_finding.cd
#
# Source: silver_aasbs_lu_review_finding  (cd, description, sort_order, active_yn)
#
# IMPROVEMENT I-DP10-1 — Business-rule derived fields:
#   is_blocking_flag     ← CASE on finding_cd
#     TRUE when the finding blocks lifecycle progression (e.g. RETURNED).
#   requires_remediation ← CASE on finding_cd
#     TRUE when remediation action required before re-review.
#
#   Assumed mappings:
#     RETURNED / REJECTED          → blocking=TRUE,  remediation=TRUE
#     APPROVED_WITH_COMMENTS       → blocking=FALSE, remediation=TRUE
#     APPROVED / WAIVED / ACCEPTED → blocking=FALSE, remediation=FALSE
#
# Post-load distribution validates against actual production codes.
# Ref: OMB A-123 (review determination requirements), Inspector General Act
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP10"
NOTEBOOK     = "prime_dim_finding"
TARGET_TABLE = "assist_dev.cat.dim_finding"

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
        FROM {SILVER}.silver_aasbs_lu_review_finding
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_review_finding rows: {rows_read:,}")

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (finding_cd, finding_desc, is_blocking_flag, requires_remediation,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            lrf.cd                                              AS finding_cd,
            lrf.description                                     AS finding_desc,

            -- IMPROVEMENT I-DP10-1: is_blocking_flag — no Silver source.
            -- TRUE when the finding blocks lifecycle progression.
            CASE
                WHEN UPPER(TRIM(lrf.cd)) LIKE '%RETURN%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%REJECT%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%DENY%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%DISAPPROVE%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS is_blocking_flag,

            -- IMPROVEMENT I-DP10-1: requires_remediation — no Silver source.
            -- TRUE when remediation action required before re-review.
            CASE
                WHEN UPPER(TRIM(lrf.cd)) LIKE '%RETURN%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%REJECT%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%COMMENT%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%REVISE%'
                  OR UPPER(TRIM(lrf.cd)) LIKE '%DENY%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS requires_remediation,

            current_timestamp(), current_timestamp(), '{RUN_ID}'

        FROM {SILVER}.silver_aasbs_lu_review_finding lrf
        WHERE COALESCE(lrf.is_deleted, FALSE) = FALSE
    """)

    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP10-1 — finding_cd distribution:")
    print(f"  {'Code':<35} {'Blocking':>9} {'Remediation':>12}  Description")
    print(f"  {'─'*35} {'─'*9} {'─'*12}  {'─'*35}")
    dist = spark.sql(f"""
        SELECT finding_cd, finding_desc, is_blocking_flag, requires_remediation
        FROM {TARGET_TABLE} ORDER BY is_blocking_flag DESC, finding_cd
    """).collect()
    blocking_cnt = sum(1 for r in dist if r[2])
    for row in dist:
        bl = "TRUE " if row[2] else "FALSE"
        rm = "TRUE " if row[3] else "FALSE"
        print(f"  {str(row[0]):<35} {bl:>9} {rm:>12}  {str(row[1])[:35]}")
    print(f"\n  blocking_count={blocking_cnt}")
    if blocking_cnt == 0:
        print(
            f"  ⚠ WARNING: no finding codes flagged as blocking. "
            f"Review distribution and update CASE expressions, then re-prime."
        )

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
