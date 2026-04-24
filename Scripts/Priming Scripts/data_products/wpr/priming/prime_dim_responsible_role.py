# Databricks notebook source
# =============================================================================
# wpr/prime_dim_responsible_role.py
# Primes assist_dev.wpr.dim_responsible_role
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_responsible_role.cd  (natural key)
#
# Source tables (Silver):
#   silver_aasbs_lu_responsible_role  →  role reference rows
#
# IMPROVEMENT I-DP8-4 — Business-rule derivation with post-load distribution
# BUSINESS RULE: requires_delegation_flag + far_citation derived from role_cd.
# (built-in):
#   lu_responsible_role has only cd, description, active_yn — standard lookup.
#   requires_delegation_flag and far_citation have no Silver source.
#   Both are derived via CASE on responsible_role_cd.
#
#   Known mappings per FAR:
#     COR         → requires_delegation=TRUE,  citation='FAR 1.602-2(d)'
#     CO          → requires_delegation=FALSE, citation='FAR 1.602-1'
#     FMA         → requires_delegation=FALSE, citation='FAR 42.202'
#     PM          → requires_delegation=FALSE, citation=NULL
#     CLIENT_REP  → requires_delegation=FALSE, citation=NULL
#     SUPERVISOR  → requires_delegation=FALSE, citation=NULL
#
#   Post-load full distribution: prints every distinct cd with its flag
#   and citation assignments. WARNING issued if no codes produce
#   requires_delegation=TRUE (indicates unexpected code values).
#   Consistent with DP9 I-DP9-1 and DP6 I-DP6-4 pattern.
#
# Ref: FAR 1.602-1 (CO warrant), FAR 1.602-2(d) (COR written delegation),
#      FAR 42.202 (assignment of contract administration),
#      FAR Part 19 (SB programs — FMA role)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_responsible_role"
TARGET_TABLE = "assist_dev.wpr.dim_responsible_role"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_lu_responsible_role")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_responsible_role
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_responsible_role rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # INSERT with business-rule derivation  (IMPROVEMENT I-DP8-4)
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            responsible_role_cd,
            responsible_role_desc,
            requires_delegation_flag,
            far_citation,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            lrr.cd                                              AS responsible_role_cd,
            lrr.description                                     AS responsible_role_desc,

            -- IMPROVEMENT I-DP8-4: requires_delegation_flag — business rule.
            -- No Silver source. CASE on role code per FAR requirements.
            -- COR requires written delegation per FAR 1.602-2(d).
            -- CO has warrant authority per FAR 1.602-1 (no separate delegation).
            -- Post-load distribution validates against actual production codes.
            CASE
                WHEN UPPER(TRIM(lrr.cd)) IN ('COR', 'COTR', 'COR_DELEGATE')
                THEN TRUE
                ELSE FALSE
            END                                                 AS requires_delegation_flag,

            -- IMPROVEMENT I-DP8-4: far_citation — business rule.
            -- Maps each role code to its governing FAR clause or part.
            CASE
                WHEN UPPER(TRIM(lrr.cd)) IN ('COR', 'COTR', 'COR_DELEGATE')
                    THEN 'FAR 1.602-2(d)'
                WHEN UPPER(TRIM(lrr.cd)) IN ('CO', 'PCO', 'ACO', 'TCO')
                    THEN 'FAR 1.602-1'
                WHEN UPPER(TRIM(lrr.cd)) IN ('FMA', 'FINANCE_MANAGER')
                    THEN 'FAR 42.202'
                ELSE NULL
            END                                                 AS far_citation,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lu_responsible_role lrr
        WHERE COALESCE(lrr.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Post-load: IMPROVEMENT I-DP8-4 distribution check
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP8-4 — Role code distribution:")
    print(f"  {'Code':<25} {'Delegation':>12} {'FAR Citation':<25}  Description")
    print(f"  {'─'*25} {'─'*12} {'─'*25}  {'─'*40}")

    dist = spark.sql(f"""
        SELECT
            responsible_role_cd,
            responsible_role_desc,
            requires_delegation_flag,
            far_citation
        FROM {TARGET_TABLE}
        ORDER BY requires_delegation_flag DESC, responsible_role_cd
    """).collect()

    delegation_cnt = 0
    for row in dist:
        d = "TRUE " if row[2] else "FALSE"
        f_cit = str(row[3]) if row[3] else "—"
        if row[2]:
            delegation_cnt += 1
        print(f"  {str(row[0]):<25} {d:>12} {f_cit:<25}  {str(row[1])[:40]}")

    print(f"\n  Roles requiring delegation: {delegation_cnt}")

    if delegation_cnt == 0:
        print(
            f"\n  ⚠ WARNING: no role codes matched requires_delegation_flag=TRUE. "
            f"This indicates production role codes differ from assumed values. "
            f"Review distribution above, update the CASE expressions, then "
            f"re-prime (TRUNCATE→INSERT is idempotent)."
        )
    else:
        print(f"  ✓ At least one role code requires delegation — review distribution above.")

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
