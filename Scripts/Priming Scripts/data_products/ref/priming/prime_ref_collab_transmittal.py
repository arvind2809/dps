# Databricks notebook source
# =============================================================================
# ref/prime_ref_collab_transmittal.py
# Primes 20 assist_dev.ref.lu_* tables — Collaboration & Transmittal domain
#
# Strategy : TRUNCATE → INSERT per table  (fully idempotent)
# Pattern  : Parameterised bulk load  (IMPROVEMENT I-DP11-1 / I-DP11-2)
#
# Tables in this notebook (20):
#   aasbs schema (16):
#     lu_collab_type, lu_collab_status, lu_central_comment_type,
#     lu_event_type, lu_review_type, lu_review_finding, lu_reviewer_type,
#     lu_responsible_role, lu_cor_delegation, lu_closeout_prohibition,
#     lu_checklist_type, lu_checklist_item, lu_checklist_value,
#     lu_subsystem, lu_table_name, lu_email_status
#   aasbs_transmit schema (4):
#     lu_transmittal_record_type, lu_transmittal_stage,
#     lu_transmittal_status, lu_transmittal_type
#
# Key difference from Groups 1 & 2:
#   4 tables source from silver_aasbs_transmit_* (not silver_aasbs_*).
#   Their source_table lineage field uses 'aasbs_transmit.<table>' prefix
#   per IMPROVEMENT I-DP11-5.
#
# Field mapping (identical to other groups — see Group 1 for full annotation):
#   code_value    ← lu.cd
#   code_desc     ← lu.description
#   short_desc    ← CAST(NULL AS STRING)  (no short_description in this group)
#   sort_order    ← lu.sort_order  (9 of 20 tables; NULL for 11)
#   is_active_flag← UPPER(TRIM(active_yn)) = 'Y'
#   effective_dt  ← CAST(NULL AS DATE)  [SILVER GAP — I-DP11-4]
#   expiry_dt     ← CAST(NULL AS DATE)  [SILVER GAP — I-DP11-4]
#   source_table  ← 'aasbs.<table>' for aasbs-schema tables
#                   'aasbs_transmit.<table>' for transmit-schema tables
#
# IMPROVEMENT I-DP11-1: single parameterised bulk notebook
# IMPROVEMENT I-DP11-2: collab/transmittal domain grouping
# IMPROVEMENT I-DP11-4: effective_dt / expiry_dt NULL on prime (Silver gap)
# IMPROVEMENT I-DP11-5: source_table lineage — 'aasbs_transmit.*' for 4 tables
# IMPROVEMENT I-DP11-6: consolidated post-load summary
#
# Ref: FAR 1.602-2(d) (COR delegation), OMB A-123 (internal review),
#      Inspector General Act 5 U.S.C. App. 3, ASSIST transmittal specification
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID  = dbutils.widgets.get("run_id")
ENV     = dbutils.widgets.get("env")
PRODUCT = "DP11"
NOTEBOOK= "prime_ref_collab_transmittal"

SILVER  = "assist_dev.assist_finance"
GOLD    = "assist_dev.ref"

# COMMAND ----------
# =============================================================================
# REF_TABLES config  (IMPROVEMENT I-DP11-1)
# (gold_table, silver_table, has_sort_order, has_short_desc, blocked,
#  source_schema)
#
# source_schema: 'aasbs' or 'aasbs_transmit' — drives the source_table
#   lineage literal value.  (IMPROVEMENT I-DP11-5)
#   aasbs_transmit tables use silver_aasbs_transmit_* naming.
# =============================================================================
REF_TABLES = [
    # gold_table                     silver_table                                    sort   short  blk    src_schema
    # ── Collaboration codes (aasbs schema) ───────────────────────────────────
    ("lu_collab_type",               "silver_aasbs_lu_collab_type",                  False, False, False, "aasbs"),
    ("lu_collab_status",             "silver_aasbs_lu_collab_status",                False, False, False, "aasbs"),
    ("lu_central_comment_type",      "silver_aasbs_lu_central_comment_type",         False, False, False, "aasbs"),
    # ── Review & audit codes ──────────────────────────────────────────────────
    ("lu_event_type",                "silver_aasbs_lu_event_type",                   False, False, False, "aasbs"),
    ("lu_review_type",               "silver_aasbs_lu_review_type",                  True,  False, False, "aasbs"),
    ("lu_review_finding",            "silver_aasbs_lu_review_finding",               True,  False, False, "aasbs"),
    ("lu_reviewer_type",             "silver_aasbs_lu_reviewer_type",                True,  False, False, "aasbs"),
    ("lu_responsible_role",          "silver_aasbs_lu_responsible_role",             False, False, False, "aasbs"),
    ("lu_cor_delegation",            "silver_aasbs_lu_cor_delegation",               True,  False, False, "aasbs"),
    # ── Closeout & checklist codes ────────────────────────────────────────────
    ("lu_closeout_prohibition",      "silver_aasbs_lu_closeout_prohibition",         False, False, False, "aasbs"),
    ("lu_checklist_type",            "silver_aasbs_lu_checklist_type",               False, False, False, "aasbs"),
    ("lu_checklist_item",            "silver_aasbs_lu_checklist_item",               True,  False, False, "aasbs"),
    ("lu_checklist_value",           "silver_aasbs_lu_checklist_value",              True,  False, False, "aasbs"),
    # ── System metadata codes ─────────────────────────────────────────────────
    ("lu_subsystem",                 "silver_aasbs_lu_subsystem",                    False, False, False, "aasbs"),
    ("lu_table_name",                "silver_aasbs_lu_table_name",                   False, False, False, "aasbs"),
    ("lu_email_status",              "silver_aasbs_lu_email_status",                 False, False, False, "aasbs"),
    # ── Transmittal codes (aasbs_transmit schema) ─────────────────────────────
    # IMPORTANT: Silver table naming uses 'silver_aasbs_transmit_' prefix.
    # source_table lineage = 'aasbs_transmit.<table>' (not 'aasbs.*').
    # (IMPROVEMENT I-DP11-5)
    ("lu_transmittal_record_type",   "silver_aasbs_transmit_lu_transmittal_record_type",  True,  False, False, "aasbs_transmit"),
    ("lu_transmittal_stage",         "silver_aasbs_transmit_lu_transmittal_stage",         False, False, False, "aasbs_transmit"),
    ("lu_transmittal_status",        "silver_aasbs_transmit_lu_transmittal_status",        True,  False, False, "aasbs_transmit"),
    ("lu_transmittal_type",          "silver_aasbs_transmit_lu_transmittal_type",          True,  False, False, "aasbs_transmit"),
]

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK,
    f"{GOLD}.*  (collab/transmittal group)",
    run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}")
print(f"[{NOTEBOOK}] Tables to process: {len(REF_TABLES)}")

aasbs_count    = sum(1 for r in REF_TABLES if r[5] == "aasbs")
transmit_count = sum(1 for r in REF_TABLES if r[5] == "aasbs_transmit")
print(f"[{NOTEBOOK}] Source schemas — aasbs: {aasbs_count} | aasbs_transmit: {transmit_count}")


def load_ref_table(gold_tbl, silver_tbl, has_sort_order, has_short_desc, src_schema):
    """
    TRUNCATE → INSERT for one ref.lu_* table.
    Returns (rows_read, rows_written).

    src_schema drives the source_table lineage literal:
      'aasbs'         → source_table = 'aasbs.<gold_tbl>'
      'aasbs_transmit'→ source_table = 'aasbs_transmit.<gold_tbl>'
    This is the only difference between aasbs and aasbs_transmit tables.
    All other field mapping is identical.  (IMPROVEMENT I-DP11-5)
    """
    target     = f"{GOLD}.{gold_tbl}"
    source     = f"{SILVER}.{silver_tbl}"
    sort_expr  = "lu.sort_order"        if has_sort_order else "CAST(NULL AS INT)"
    short_expr = "lu.short_description" if has_short_desc  else "CAST(NULL AS STRING)"
    # IMPROVEMENT I-DP11-5: correct source_table prefix per schema
    src_literal = f"{src_schema}.{gold_tbl}"

    spark.sql(f"TRUNCATE TABLE {target}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {source} WHERE COALESCE(is_deleted, FALSE) = FALSE"
    ).collect()[0][0]

    spark.sql(f"""
        INSERT INTO {target}
        (
            code_value,
            code_desc,
            short_desc,
            sort_order,
            is_active_flag,
            effective_dt,
            expiry_dt,
            source_table,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            lu.cd                                               AS code_value,
            lu.description                                      AS code_desc,
            -- No table in this group has short_description — always NULL
            {short_expr}                                        AS short_desc,
            -- sort_order: 9 of 20 tables in this group
            {sort_expr}                                         AS sort_order,
            -- is_active_flag: UPPER+TRIM guards against case / whitespace
            CASE
                WHEN UPPER(TRIM(COALESCE(lu.active_yn, 'N'))) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_active_flag,
            -- SILVER GAP: effective_dt — no Silver lu_* table carries this.
            -- NULL on prime across all 85 ref tables. (IMPROVEMENT I-DP11-4)
            CAST(NULL AS DATE)                                  AS effective_dt,
            -- SILVER GAP: expiry_dt — same gap as effective_dt.
            CAST(NULL AS DATE)                                  AS expiry_dt,
            -- IMPROVEMENT I-DP11-5: lineage literal — schema-aware prefix.
            -- aasbs tables: 'aasbs.lu_collab_type' etc.
            -- aasbs_transmit tables: 'aasbs_transmit.lu_transmittal_type' etc.
            '{src_literal}'                                     AS source_table,
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id
        FROM {source} lu
        WHERE COALESCE(lu.is_deleted, FALSE) = FALSE
    """)

    rows_written = spark.sql(f"SELECT COUNT(*) FROM {target}").collect()[0][0]
    return rows_read, rows_written


# ─────────────────────────────────────────────────────────────────────────────
# Main execution loop
# ─────────────────────────────────────────────────────────────────────────────
summary       = []
failed_tbls   = []
total_read    = 0
total_written = 0

try:
    for gold_tbl, silver_tbl, has_sort, has_short, blocked, src_schema in REF_TABLES:
        if blocked:
            print(f"  [BLOCKED] {gold_tbl}")
            summary.append((gold_tbl, 0, 0, 0, 0, "BLOCKED"))
            continue

        try:
            rows_read, rows_written = load_ref_table(
                gold_tbl, silver_tbl, has_sort, has_short, src_schema
            )
            total_read    += rows_read
            total_written += rows_written

            counts = spark.sql(f"""
                SELECT
                    SUM(CASE WHEN is_active_flag = TRUE  THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN is_active_flag = FALSE THEN 1 ELSE 0 END) AS inactive
                FROM {GOLD}.{gold_tbl}
            """).collect()[0]

            status = "OK" if rows_written > 0 else "WARN_ZERO"
            summary.append((gold_tbl, rows_read, rows_written,
                            counts[0] or 0, counts[1] or 0, status))

        except Exception as tbl_err:
            failed_tbls.append(gold_tbl)
            summary.append((gold_tbl, 0, 0, 0, 0, "FAILED"))
            print(f"  [FAILED] {gold_tbl}: {str(tbl_err)[:200]}")

    # ─────────────────────────────────────────────────────────────────────────
    # Post-load summary  (IMPROVEMENT I-DP11-6)
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n[{NOTEBOOK}] ── POST-LOAD SUMMARY ──────────────────────────────────")
    print(f"{'Table':<45} {'Read':>7} {'Written':>8} {'Active':>8} {'Inactive':>9} {'Status'}")
    print(f"{'─'*45} {'─'*7} {'─'*8} {'─'*8} {'─'*9} {'─'*7}")
    for tbl, rd, wr, act, inact, stat in summary:
        warn_flag = " ⚠" if stat in ("WARN_ZERO", "FAILED", "BLOCKED") else ""
        schema_tag = " [tx]" if any(
            r[0] == tbl and r[5] == "aasbs_transmit" for r in REF_TABLES
        ) else ""
        print(f"  {tbl:<43} {rd:>7,} {wr:>8,} {act:>8,} {inact:>9,}  {stat}{warn_flag}{schema_tag}")

    print(f"\n  Totals — read={total_read:,} | written={total_written:,} | "
          f"failed={len(failed_tbls)}")
    print(f"  Source schemas — aasbs: {aasbs_count} tables | "
          f"aasbs_transmit: {transmit_count} tables ([tx] in summary above)")

    zero_rows = [s[0] for s in summary if s[1] > 0 and s[2] == 0]
    if zero_rows:
        print(f"\n  ⚠ Tables with Silver rows but 0 written: {zero_rows}")

    # IMPROVEMENT I-DP11-4: Silver gap assertions
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP11-4 — Silver gap assertions:")
    for tbl, _, wr, _, _, stat in summary:
        if stat in ("BLOCKED", "FAILED") or wr == 0:
            continue
        non_null_eff = spark.sql(
            f"SELECT COUNT(*) FROM {GOLD}.{tbl} WHERE effective_dt IS NOT NULL"
        ).collect()[0][0]
        non_null_exp = spark.sql(
            f"SELECT COUNT(*) FROM {GOLD}.{tbl} WHERE expiry_dt IS NOT NULL"
        ).collect()[0][0]
        assert non_null_eff == 0, \
            f"ASSERT FAILED: {tbl}.effective_dt must be NULL on prime (Silver gap)"
        assert non_null_exp == 0, \
            f"ASSERT FAILED: {tbl}.expiry_dt must be NULL on prime (Silver gap)"
    print(f"  ✓ effective_dt = NULL for all rows in all tables")
    print(f"  ✓ expiry_dt    = NULL for all rows in all tables")

    # IMPROVEMENT I-DP11-5: verify transmit-schema source_table lineage
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP11-5 — Transmittal source_table lineage check:")
    for gold_tbl, _, _, _, _, src_schema in REF_TABLES:
        if src_schema != "aasbs_transmit":
            continue
        expected_prefix = "aasbs_transmit."
        wrong = spark.sql(f"""
            SELECT COUNT(*) FROM {GOLD}.{gold_tbl}
            WHERE source_table NOT LIKE '{expected_prefix}%'
        """).collect()[0][0]
        assert wrong == 0, \
            f"ASSERT FAILED: {gold_tbl}.source_table must start with 'aasbs_transmit.' — found {wrong} wrong rows"
    print(f"  ✓ All 4 transmit-schema tables carry 'aasbs_transmit.*' source_table prefix")

    if failed_tbls:
        err_msg = f"Failed tables: {failed_tbls}"
        audit_failure(spark, RUN_ID, NOTEBOOK,
                      f"{GOLD}.* (collab/transmittal group)",
                      start_ts, err_msg)
        raise RuntimeError(err_msg)

    audit_success(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (collab/transmittal group)",
                  start_ts,
                  sum(s[1] for s in summary),
                  sum(s[2] for s in summary))
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (collab/transmittal group)",
                  start_ts, err)
    raise
