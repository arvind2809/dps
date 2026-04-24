# Databricks notebook source
# =============================================================================
# ref/prime_ref_financial_operational.py
# Primes 32 assist_dev.ref.lu_* tables — Financial & Operational domain
#
# Strategy : TRUNCATE → INSERT per table  (fully idempotent)
# Pattern  : Parameterised bulk load  (IMPROVEMENT I-DP11-1 / I-DP11-2)
#
# Tables in this notebook (32):
#   lu_fund, lu_fund_status, lu_fund_type, lu_fund_category,
#   lu_billing_type, lu_fund_amend_status, lu_funds_usage,
#   lu_transaction_type, lu_loa_status, lu_loa_change_reason,
#   lu_ia_status, lu_ia_amend_action,
#   lu_line_item_type, lu_severability_type, lu_unit_of_measure,
#   lu_fob_type, lu_cost_rate_type, lu_proc_phase,
#   lu_accrual_income_doc_type, lu_onefund_program, lu_onefund_activity,
#   lu_agency, lu_activity_address_code, lu_agreement_type,
#   lu_region, lu_state, lu_country, lu_address_type,
#   lu_office_type, lu_group_contact_type, lu_transfer, lu_program
#
# No blocked tables in this group — all 32 Silver sources confirmed.
#
# Field mapping (see prime_ref_acquisition_contract.py for full annotation):
#   code_value    ← lu.cd
#   code_desc     ← lu.description
#   short_desc    ← lu.short_description  (lu_proc_phase only; NULL others)
#   sort_order    ← lu.sort_order         (9 of 32 tables)
#   is_active_flag← UPPER(TRIM(active_yn)) = 'Y'
#   effective_dt  ← CAST(NULL AS DATE)  [SILVER GAP — I-DP11-4]
#   expiry_dt     ← CAST(NULL AS DATE)  [SILVER GAP — I-DP11-4]
#   source_table  ← 'aasbs.<table>'      [I-DP11-5 lineage literal]
#
# IMPROVEMENT I-DP11-1: single parameterised bulk notebook
# IMPROVEMENT I-DP11-2: financial/operational domain grouping
# IMPROVEMENT I-DP11-4: effective_dt / expiry_dt NULL on prime (Silver gap)
# IMPROVEMENT I-DP11-5: source_table hardcoded lineage literal per table
# IMPROVEMENT I-DP11-6: consolidated post-load summary
#
# Ref: FAR Part 4, OMB A-11 (fund categories), 31 U.S.C. §1535 (Economy Act),
#      TFM Volume I (LOA/fund codes), FAR 52.232-7 (T&M — proc phase)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID  = dbutils.widgets.get("run_id")
ENV     = dbutils.widgets.get("env")
PRODUCT = "DP11"
NOTEBOOK= "prime_ref_financial_operational"

SILVER  = "assist_dev.assist_finance"
GOLD    = "assist_dev.ref"

# COMMAND ----------
# =============================================================================
# REF_TABLES config  (IMPROVEMENT I-DP11-1)
# (gold_table, silver_table, has_sort_order, has_short_desc, blocked)
# =============================================================================
REF_TABLES = [
    # ── Fund & billing codes ──────────────────────────────────────────────────
    ("lu_fund",                  "silver_aasbs_lu_fund",                  False, False, False),
    ("lu_fund_status",           "silver_aasbs_lu_fund_status",           False, False, False),
    ("lu_fund_type",             "silver_aasbs_lu_fund_type",             False, False, False),
    ("lu_fund_category",         "silver_aasbs_lu_fund_category",         False, False, False),
    ("lu_billing_type",          "silver_aasbs_lu_billing_type",          False, False, False),
    ("lu_fund_amend_status",     "silver_aasbs_lu_fund_amend_status",     False, False, False),
    ("lu_funds_usage",           "silver_aasbs_lu_funds_usage",           False, False, False),
    ("lu_transaction_type",      "silver_aasbs_lu_transaction_type",      False, False, False),
    # ── LOA & IA codes ────────────────────────────────────────────────────────
    ("lu_loa_status",            "silver_aasbs_lu_loa_status",            False, False, False),
    ("lu_loa_change_reason",     "silver_aasbs_lu_loa_change_reason",     True,  False, False),
    ("lu_ia_status",             "silver_aasbs_lu_ia_status",             True,  False, False),
    ("lu_ia_amend_action",       "silver_aasbs_lu_ia_amend_action",       False, False, False),
    # ── Line item & pricing codes ─────────────────────────────────────────────
    ("lu_line_item_type",        "silver_aasbs_lu_line_item_type",        False, False, False),
    ("lu_severability_type",     "silver_aasbs_lu_severability_type",     False, False, False),
    ("lu_unit_of_measure",       "silver_aasbs_lu_unit_of_measure",       True,  False, False),
    ("lu_fob_type",              "silver_aasbs_lu_fob_type",              True,  False, False),
    ("lu_cost_rate_type",        "silver_aasbs_lu_cost_rate_type",        True,  False, False),
    # lu_proc_phase: has BOTH sort_order AND short_description
    ("lu_proc_phase",            "silver_aasbs_lu_proc_phase",            True,  True,  False),
    # ── Accrual / OneFund codes ───────────────────────────────────────────────
    ("lu_accrual_income_doc_type","silver_aasbs_lu_accrual_income_doc_type",False,False, False),
    ("lu_onefund_program",       "silver_aasbs_lu_onefund_program",       False, False, False),
    ("lu_onefund_activity",      "silver_aasbs_lu_onefund_activity",      False, False, False),
    # ── Organisational & geographic codes ─────────────────────────────────────
    ("lu_agency",                "silver_aasbs_lu_agency",                True,  False, False),
    ("lu_activity_address_code", "silver_aasbs_lu_activity_address_code", True,  False, False),
    ("lu_agreement_type",        "silver_aasbs_lu_agreement_type",        False, False, False),
    ("lu_region",                "silver_aasbs_lu_region",                False, False, False),
    ("lu_state",                 "silver_aasbs_lu_state",                 False, False, False),
    ("lu_country",               "silver_aasbs_lu_country",               False, False, False),
    ("lu_address_type",          "silver_aasbs_lu_address_type",          False, False, False),
    ("lu_office_type",           "silver_aasbs_lu_office_type",           False, False, False),
    ("lu_group_contact_type",    "silver_aasbs_lu_group_contact_type",    False, False, False),
    ("lu_transfer",              "silver_aasbs_lu_transfer",              True,  False, False),
    ("lu_program",               "silver_aasbs_lu_program",               False, False, False),
]

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK,
    f"{GOLD}.*  (financial/operational group)",
    run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}")
print(f"[{NOTEBOOK}] Tables to process: {len(REF_TABLES)}")


def load_ref_table(gold_tbl, silver_tbl, has_sort_order, has_short_desc):
    """
    TRUNCATE → INSERT for one ref.lu_* table.
    Returns (rows_read, rows_written).
    All field mapping decisions documented in prime_ref_acquisition_contract.py.
    """
    target     = f"{GOLD}.{gold_tbl}"
    source     = f"{SILVER}.{silver_tbl}"
    sort_expr  = "lu.sort_order"      if has_sort_order else "CAST(NULL AS INT)"
    short_expr = "lu.short_description" if has_short_desc else "CAST(NULL AS STRING)"

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
            -- short_desc: lu_proc_phase only in this group (has short_description)
            {short_expr}                                        AS short_desc,
            -- sort_order: 9 of 32 tables in this group have sort_order
            {sort_expr}                                         AS sort_order,
            -- is_active_flag: UPPER+TRIM guards against case / whitespace variants
            CASE
                WHEN UPPER(TRIM(COALESCE(lu.active_yn, 'N'))) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_active_flag,
            -- SILVER GAP: effective_dt — no Silver lu_* table carries this field
            -- Null on prime across all 85 ref tables. (IMPROVEMENT I-DP11-4)
            CAST(NULL AS DATE)                                  AS effective_dt,
            -- SILVER GAP: expiry_dt — same limitation as effective_dt
            CAST(NULL AS DATE)                                  AS expiry_dt,
            -- Lineage: hardcoded source table name (IMPROVEMENT I-DP11-5)
            'aasbs.{gold_tbl}'                                  AS source_table,
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
    for gold_tbl, silver_tbl, has_sort, has_short, blocked in REF_TABLES:
        # No blocked tables in this group — guard retained for consistency
        if blocked:
            print(f"  [BLOCKED] {gold_tbl}")
            summary.append((gold_tbl, 0, 0, 0, 0, "BLOCKED"))
            continue

        try:
            rows_read, rows_written = load_ref_table(
                gold_tbl, silver_tbl, has_sort, has_short
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
        print(f"  {tbl:<43} {rd:>7,} {wr:>8,} {act:>8,} {inact:>9,}  {stat}{warn_flag}")

    print(f"\n  Totals — read={total_read:,} | written={total_written:,} | "
          f"failed={len(failed_tbls)}")

    zero_rows = [s[0] for s in summary if s[1] > 0 and s[2] == 0]
    if zero_rows:
        print(f"\n  ⚠ Tables with Silver rows but 0 written: {zero_rows}")

    # IMPROVEMENT I-DP11-4: Silver gap assertions for effective/expiry dates
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

    if failed_tbls:
        err_msg = f"Failed tables: {failed_tbls}"
        audit_failure(spark, RUN_ID, NOTEBOOK,
                      f"{GOLD}.* (financial/operational group)",
                      start_ts, err_msg)
        raise RuntimeError(err_msg)

    audit_success(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (financial/operational group)",
                  start_ts,
                  sum(s[1] for s in summary),
                  sum(s[2] for s in summary))
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (financial/operational group)",
                  start_ts, err)
    raise
