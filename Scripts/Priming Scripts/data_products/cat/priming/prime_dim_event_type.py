# Databricks notebook source
# =============================================================================
# cat/prime_dim_event_type.py
# Primes assist_dev.cat.dim_event_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_event_type.cd  (natural key)
#
# Source tables (Silver):
#   silver_aasbs_lu_event_type  →  event type reference rows
#
# IMPROVEMENT I-DP10-1 — Business-rule derivation + post-load distribution:
#   lu_event_type has only cd, description, active_yn — standard lookup.
#   Three Gold fields require CASE derivation from event_type_cd:
#
#   lifecycle_phase_cd:
#     PRE_AWARD  — solicitation, review, J&A events
#     AWARD      — award creation, modification events
#     POST_AWARD — invoice, acceptance, CLIN, COR events
#     CLOSEOUT   — final payment, close, deobligation events
#
#   is_system_event_flag:
#     TRUE  — events generated automatically by ASSIST (status changes,
#             workflow transitions, system notifications)
#     FALSE — events triggered by user actions
#
#   is_audit_significant:
#     TRUE  — events significant for OIG/audit purposes:
#             award creation, contract modification, CO/COR changes,
#             closeout, J&A approvals
#     FALSE — routine workflow events
#
#   Post-load distribution prints every distinct cd with all three derived
#   values. WARNING if both is_system_event_flag and is_audit_significant
#   are all FALSE (indicates production codes differ from assumed values).
#   Ref: Inspector General Act (5 U.S.C. App. 3) — audit significance.
#
# Ref: 5 U.S.C. App. 3, OMB A-123, FAR Part 4 (contract records)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP10"
NOTEBOOK     = "prime_dim_event_type"
TARGET_TABLE = "assist_dev.cat.dim_event_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_event_type
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_event_type rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # INSERT with IMPROVEMENT I-DP10-1 business-rule derivations
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            event_type_cd,
            event_type_desc,
            lifecycle_phase_cd,
            is_system_event_flag,
            is_audit_significant,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            let.cd                                              AS event_type_cd,
            let.description                                     AS event_type_desc,

            -- IMPROVEMENT I-DP10-1: lifecycle_phase_cd — no Silver source.
            -- CASE on event_type_cd. Post-load distribution validates.
            -- Ref: ASSIST lifecycle phases (pre-award → award → post-award → closeout)
            CASE
                WHEN UPPER(TRIM(let.cd)) LIKE '%SOLICIT%'
                  OR UPPER(TRIM(let.cd)) LIKE '%REVIEW%'
                  OR UPPER(TRIM(let.cd)) LIKE '%JA%'
                  OR UPPER(TRIM(let.cd)) LIKE '%PRE%AWARD%'
                    THEN 'PRE_AWARD'
                WHEN UPPER(TRIM(let.cd)) LIKE '%AWARD%'
                  OR UPPER(TRIM(let.cd)) LIKE '%CONTRACT%'
                  OR UPPER(TRIM(let.cd)) LIKE '%MOD%'
                    THEN 'AWARD'
                WHEN UPPER(TRIM(let.cd)) LIKE '%INVOICE%'
                  OR UPPER(TRIM(let.cd)) LIKE '%ACCEPT%'
                  OR UPPER(TRIM(let.cd)) LIKE '%CLIN%'
                  OR UPPER(TRIM(let.cd)) LIKE '%POST%AWARD%'
                  OR UPPER(TRIM(let.cd)) LIKE '%PAYMENT%'
                    THEN 'POST_AWARD'
                WHEN UPPER(TRIM(let.cd)) LIKE '%CLOSE%'
                  OR UPPER(TRIM(let.cd)) LIKE '%FINAL%'
                  OR UPPER(TRIM(let.cd)) LIKE '%DEOBLIG%'
                    THEN 'CLOSEOUT'
                ELSE NULL
            END                                                 AS lifecycle_phase_cd,

            -- IMPROVEMENT I-DP10-1: is_system_event_flag — no Silver source.
            -- TRUE for events generated automatically by ASSIST workflow engine.
            CASE
                WHEN UPPER(TRIM(let.cd)) LIKE '%SYSTEM%'
                  OR UPPER(TRIM(let.cd)) LIKE '%AUTO%'
                  OR UPPER(TRIM(let.cd)) LIKE '%NOTIF%'
                  OR UPPER(TRIM(let.cd)) LIKE '%STATUS%CHANGE%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS is_system_event_flag,

            -- IMPROVEMENT I-DP10-1: is_audit_significant — no Silver source.
            -- TRUE for OIG/audit-significant events per 5 U.S.C. App. 3.
            CASE
                WHEN UPPER(TRIM(let.cd)) LIKE '%AWARD%CREATE%'
                  OR UPPER(TRIM(let.cd)) LIKE '%MOD%APPROV%'
                  OR UPPER(TRIM(let.cd)) LIKE '%COR%ASSIGN%'
                  OR UPPER(TRIM(let.cd)) LIKE '%CO%SIGN%'
                  OR UPPER(TRIM(let.cd)) LIKE '%JA%APPROV%'
                  OR UPPER(TRIM(let.cd)) LIKE '%CLOSE%FINAL%'
                  OR UPPER(TRIM(let.cd)) LIKE '%DEOBLIG%'
                    THEN TRUE
                ELSE FALSE
            END                                                 AS is_audit_significant,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lu_event_type let
        WHERE COALESCE(let.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Post-load: IMPROVEMENT I-DP10-1 distribution check
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP10-1 — event_type_cd distribution:")
    print(
        f"  {'Code':<35} {'Phase':<15} {'System':>7} {'Audit':>7}  Description"
    )
    print(f"  {'─'*35} {'─'*15} {'─'*7} {'─'*7}  {'─'*35}")

    dist = spark.sql(f"""
        SELECT event_type_cd, event_type_desc,
               lifecycle_phase_cd, is_system_event_flag, is_audit_significant
        FROM {TARGET_TABLE} ORDER BY event_type_cd
    """).collect()

    system_cnt  = sum(1 for r in dist if r[3])
    audit_cnt   = sum(1 for r in dist if r[4])
    no_phase    = sum(1 for r in dist if not r[2])

    for row in dist:
        ph = str(row[2])[:14] if row[2] else "—"
        sy = "TRUE " if row[3] else "FALSE"
        au = "TRUE " if row[4] else "FALSE"
        print(f"  {str(row[0]):<35} {ph:<15} {sy:>7} {au:>7}  {str(row[1])[:35]}")

    print(f"\n  system_events={system_cnt} | audit_significant={audit_cnt} | no_phase={no_phase}")

    if system_cnt == 0 and audit_cnt == 0:
        print(
            f"\n  ⚠ WARNING: all {rows_written} event codes have both "
            f"is_system_event_flag=FALSE and is_audit_significant=FALSE. "
            f"Review distribution above and update CASE expressions, then re-prime."
        )
    if no_phase > 0:
        print(
            f"\n  ⚠ WARNING: {no_phase} event codes have no lifecycle_phase_cd "
            f"(fell through all CASE branches). Review distribution above."
        )

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
