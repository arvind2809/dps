# Databricks notebook source
# =============================================================================
# ref/prime_ref_acquisition_contract.py
# Primes 33 assist_dev.ref.lu_* tables — Acquisition & Contract domain
#
# Strategy : TRUNCATE → INSERT per table  (fully idempotent for all 33 tables)
# Pattern  : Parameterised bulk load  (IMPROVEMENT I-DP11-1 / I-DP11-2)
#
# Tables in this notebook (33):
#   lu_acquisition_status, lu_acquisition_type, lu_acquisition_mod_status,
#   lu_competition_type, lu_commercial_type, lu_performance_type,
#   lu_processing_speed, lu_expedited_type, lu_emergency_acq,
#   lu_supply_chain_risk, lu_surveillance_spec, lu_intel_community,
#   lu_consolidated_contract, lu_bundled_contract, lu_reason_for_direct,
#   lu_small_business_admin, lu_multi_year_contract [BLOCKED],
#   lu_award_mod_fpds_reason, lu_sf30_mod_type, lu_fpds_car_status,
#   lu_contract_type, lu_vehicle_type, lu_instrument_type,
#   lu_idv_type_of_idc, lu_idv_referenced_class, lu_idv_who_can_use,
#   lu_continuity_type, lu_award_validation,
#   lu_sol_status, lu_sol_amend_status, lu_sol_posting_type,
#   lu_sol_response_aac_visibility, lu_sol_response_cli_visibility
#
# Universal Gold schema (all 85 ref tables are identical):
#   code_sk       BIGINT IDENTITY  (auto-generated — not in INSERT list)
#   code_value    ← Silver: lu.cd                            (NOT NULL)
#   code_desc     ← Silver: lu.description
#   short_desc    ← Silver: lu.short_description  (where present; NULL otherwise)
#   sort_order    ← Silver: lu.sort_order          (where present; NULL otherwise)
#   is_active_flag← UPPER(TRIM(lu.active_yn)) = 'Y'
#   effective_dt  ← CAST(NULL AS DATE)  [SILVER GAP — no Silver source exists]
#   expiry_dt     ← CAST(NULL AS DATE)  [SILVER GAP — no Silver source exists]
#   source_table  ← hardcoded string literal per table
#   _gold_*       ← pipeline audit columns
#
# IMPROVEMENT I-DP11-1 (built-in):
#   Single parameterised notebook instead of 33 individual notebooks.
#   REF_TABLES config list drives a shared load_ref_table() helper.
#   Field mapping flags (has_sort_order, has_short_desc) are set per table
#   in the config — the helper builds the INSERT SQL dynamically.
#
# IMPROVEMENT I-DP11-3 (built-in):
#   lu_multi_year_contract has no Silver source.
#   Handled via blocked=True flag — the table is skipped, 0 rows loaded,
#   and a BLOCKED audit record is written to pipeline_audit.
#
# IMPROVEMENT I-DP11-4 (built-in):
#   effective_dt and expiry_dt are CAST(NULL) for every table.
#   Post-load assertion verifies both remain NULL across this group.
#
# IMPROVEMENT I-DP11-5 (built-in):
#   source_table hardcoded as 'aasbs.<table>' per table lineage field.
#
# IMPROVEMENT I-DP11-6 (built-in):
#   Consolidated post-load summary table printed at end of notebook.
#
# Ref: FAR Part 4 (contract reporting codes), FAR Part 19 (SB codes),
#      FAR 1.602 (CO authority), FPDS data dictionary
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID  = dbutils.widgets.get("run_id")
ENV     = dbutils.widgets.get("env")
PRODUCT = "DP11"
NOTEBOOK= "prime_ref_acquisition_contract"

SILVER  = "assist_dev.assist_finance"
GOLD    = "assist_dev.ref"

# COMMAND ----------
# =============================================================================
# REF_TABLES config  (IMPROVEMENT I-DP11-1)
# Each tuple: (gold_table, silver_table, has_sort_order, has_short_desc, blocked)
# silver_table: None when blocked=True
# source_label: displayed in source_table lineage field — always 'aasbs.<table>'
# =============================================================================
REF_TABLES = [
    # (gold_table,                       silver_table,                                    sort,  short, blocked)
    ("lu_acquisition_status",            "silver_aasbs_lu_acquisition_status",            False, False, False),
    ("lu_acquisition_type",              "silver_aasbs_lu_acquisition_type",              False, False, False),
    ("lu_acquisition_mod_status",        "silver_aasbs_lu_acquisition_mod_status",        False, False, False),
    ("lu_competition_type",              "silver_aasbs_lu_competition_type",              False, False, False),
    ("lu_commercial_type",               "silver_aasbs_lu_commercial_type",               False, False, False),
    ("lu_performance_type",              "silver_aasbs_lu_performance_type",              False, False, False),
    ("lu_processing_speed",              "silver_aasbs_lu_processing_speed",              False, False, False),
    ("lu_expedited_type",                "silver_aasbs_lu_expedited_type",                False, False, False),
    ("lu_emergency_acq",                 "silver_aasbs_lu_emergency_acq",                 True,  False, False),
    ("lu_supply_chain_risk",             "silver_aasbs_lu_supply_chain_risk",             True,  False, False),
    ("lu_surveillance_spec",             "silver_aasbs_lu_surveillance_spec",             True,  False, False),
    ("lu_intel_community",               "silver_aasbs_lu_intel_community",               True,  False, False),
    ("lu_consolidated_contract",         "silver_aasbs_lu_consolidated_contract",         True,  False, False),
    ("lu_bundled_contract",              "silver_aasbs_lu_bundled_contract",              True,  False, False),
    ("lu_reason_for_direct",             "silver_aasbs_lu_reason_for_direct",             True,  False, False),
    ("lu_small_business_admin",          "silver_aasbs_lu_small_business_admin",          True,  False, False),
    # BLOCKED — no Silver source; aasbs.lu_multi_year_contract not yet ingested
    ("lu_multi_year_contract",           None,                                            False, False, True),
    ("lu_award_mod_fpds_reason",         "silver_aasbs_lu_award_mod_fpds_reason",         True,  False, False),
    ("lu_sf30_mod_type",                 "silver_aasbs_lu_sf30_mod_type",                 True,  False, False),
    ("lu_fpds_car_status",               "silver_aasbs_lu_fpds_car_status",               True,  False, False),
    ("lu_contract_type",                 "silver_aasbs_lu_contract_type",                 False, False, False),
    ("lu_vehicle_type",                  "silver_aasbs_lu_vehicle_type",                  False, False, False),
    # lu_instrument_type: has BOTH sort_order AND short_description
    ("lu_instrument_type",               "silver_aasbs_lu_instrument_type",               True,  True,  False),
    ("lu_idv_type_of_idc",               "silver_aasbs_lu_idv_type_of_idc",               False, False, False),
    ("lu_idv_referenced_class",          "silver_aasbs_lu_idv_referenced_class",          False, False, False),
    ("lu_idv_who_can_use",               "silver_aasbs_lu_idv_who_can_use",               False, False, False),
    ("lu_continuity_type",               "silver_aasbs_lu_continuity_type",               False, False, False),
    ("lu_award_validation",              "silver_aasbs_lu_award_validation",              True,  False, False),
    ("lu_sol_status",                    "silver_aasbs_lu_sol_status",                    True,  False, False),
    ("lu_sol_amend_status",              "silver_aasbs_lu_sol_amend_status",              True,  False, False),
    ("lu_sol_posting_type",              "silver_aasbs_lu_sol_posting_type",              True,  False, False),
    ("lu_sol_response_aac_visibility",   "silver_aasbs_lu_sol_response_aac_visibility",   True,  False, False),
    ("lu_sol_response_cli_visibility",   "silver_aasbs_lu_sol_response_cli_visibility",   True,  False, False),
]

# COMMAND ----------
import time

start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK,
    f"{GOLD}.*  (acquisition/contract group)",
    run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}")
print(f"[{NOTEBOOK}] Tables to process: {len(REF_TABLES)}")

# ─────────────────────────────────────────────────────────────────────────────
# Shared helper: load one reference table
# ─────────────────────────────────────────────────────────────────────────────
def load_ref_table(gold_tbl, silver_tbl, has_sort_order, has_short_desc):
    """
    TRUNCATE → INSERT for one ref.lu_* table.
    Returns (rows_read, rows_written).

    Field mapping (IMPROVEMENT I-DP11-1):
      code_value   ← lu.cd                       (always present)
      code_desc    ← lu.description               (always present)
      short_desc   ← lu.short_description         (only when has_short_desc=True)
      sort_order   ← lu.sort_order                (only when has_sort_order=True)
      is_active_flag← UPPER(TRIM(active_yn))='Y'  (always present)
      effective_dt  ← NULL  (SILVER GAP — I-DP11-4)
      expiry_dt     ← NULL  (SILVER GAP — I-DP11-4)
      source_table  ← 'aasbs.<gold_tbl>'          (I-DP11-5 lineage literal)
    """
    target = f"{GOLD}.{gold_tbl}"
    source = f"{SILVER}.{silver_tbl}"

    spark.sql(f"TRUNCATE TABLE {target}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {source} WHERE COALESCE(is_deleted, FALSE) = FALSE"
    ).collect()[0][0]

    # Build sort_order and short_desc expressions based on flags
    sort_expr  = "lu.sort_order" if has_sort_order else "CAST(NULL AS INT)"
    short_expr = "lu.short_description" if has_short_desc else "CAST(NULL AS STRING)"

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
            -- Natural key: the code string used in transactional columns
            lu.cd                                               AS code_value,

            -- Full description for UI display and reporting
            lu.description                                      AS code_desc,

            -- Short description: only on lu_instrument_type and lu_proc_phase
            -- All other tables: CAST(NULL AS STRING)
            {short_expr}                                        AS short_desc,

            -- Display sort order: available on 36 of 85 lu_ tables
            -- All other tables: CAST(NULL AS INT)
            {sort_expr}                                         AS sort_order,

            -- Active flag: UPPER + TRIM guards against 'Y','y',' Y' variants
            CASE
                WHEN UPPER(TRIM(COALESCE(lu.active_yn, 'N'))) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_active_flag,

            -- SILVER GAP: effective_dt — no Silver lu_* table carries this field.
            -- Intended for CDC lifecycle tracking of reference code validity.
            -- NULL on prime across all 85 tables. (IMPROVEMENT I-DP11-4)
            CAST(NULL AS DATE)                                  AS effective_dt,

            -- SILVER GAP: expiry_dt — same limitation as effective_dt.
            CAST(NULL AS DATE)                                  AS expiry_dt,

            -- Lineage: hardcoded source table name. (IMPROVEMENT I-DP11-5)
            -- Enables downstream consumers to identify Silver origin without
            -- querying metadata.
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
# Main execution loop  (IMPROVEMENT I-DP11-1)
# ─────────────────────────────────────────────────────────────────────────────
summary     = []   # (gold_tbl, rows_read, rows_written, active, inactive, status)
blocked_cnt = 0
failed_tbls = []
total_read  = 0
total_written = 0

try:
    for gold_tbl, silver_tbl, has_sort, has_short, blocked in REF_TABLES:

        if blocked:
            # IMPROVEMENT I-DP11-3: blocked table — no Silver source
            blocked_cnt += 1
            print(f"  [BLOCKED] {gold_tbl}  — no Silver source (aasbs.{gold_tbl} not ingested). "
                  f"Writing BLOCKED audit record.")
            spark.sql(f"""
                INSERT INTO assist_dev.common.pipeline_audit
                    (run_id, product, notebook, target_table, run_type,
                     status, started_at, error_message)
                VALUES (
                    '{RUN_ID}', '{PRODUCT}', '{NOTEBOOK}',
                    '{GOLD}.{gold_tbl}', 'FULL_PRIME',
                    'BLOCKED', current_timestamp(),
                    'No Silver source: aasbs.{gold_tbl} not ingested into Silver DDL. '
                    'Table remains empty until Silver pipeline is extended.'
                )
            """)
            summary.append((gold_tbl, 0, 0, 0, 0, "BLOCKED"))
            continue

        try:
            rows_read, rows_written = load_ref_table(
                gold_tbl, silver_tbl, has_sort, has_short
            )
            total_read    += rows_read
            total_written += rows_written

            # Active/inactive breakdown for summary
            counts = spark.sql(f"""
                SELECT
                    SUM(CASE WHEN is_active_flag = TRUE  THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN is_active_flag = FALSE THEN 1 ELSE 0 END) AS inactive
                FROM {GOLD}.{gold_tbl}
            """).collect()[0]
            active   = counts[0] or 0
            inactive = counts[1] or 0

            status = "OK" if rows_written > 0 else "WARN_ZERO"
            summary.append((gold_tbl, rows_read, rows_written, active, inactive, status))

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
        warn_flag = " ⚠" if stat in ("WARN_ZERO","FAILED","BLOCKED") else ""
        print(f"  {tbl:<43} {rd:>7,} {wr:>8,} {act:>8,} {inact:>9,}  {stat}{warn_flag}")

    print(f"\n  Totals — read={total_read:,} | written={total_written:,} | "
          f"blocked={blocked_cnt} | failed={len(failed_tbls)}")

    # Zero-row warnings
    zero_rows = [s[0] for s in summary if s[1] > 0 and s[2] == 0]
    if zero_rows:
        print(f"\n  ⚠ Tables with Silver rows but 0 written: {zero_rows}")

    # IMPROVEMENT I-DP11-4: effective_dt / expiry_dt NULL assertions
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP11-4 — Silver gap assertions:")
    for tbl, _, wr, _, _, stat in summary:
        if stat in ("BLOCKED","FAILED") or wr == 0:
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
    print(f"  ✓ effective_dt = NULL for all rows in all non-blocked tables")
    print(f"  ✓ expiry_dt    = NULL for all rows in all non-blocked tables")

    if failed_tbls:
        err_msg = f"Failed tables: {failed_tbls}"
        audit_failure(spark, RUN_ID, NOTEBOOK,
                      f"{GOLD}.* (acquisition/contract group)",
                      start_ts, err_msg)
        raise RuntimeError(err_msg)

    rows_read_total   = sum(s[1] for s in summary)
    rows_written_total= sum(s[2] for s in summary)
    audit_success(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (acquisition/contract group)",
                  start_ts, rows_read_total, rows_written_total)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK,
                  f"{GOLD}.* (acquisition/contract group)",
                  start_ts, err)
    raise
