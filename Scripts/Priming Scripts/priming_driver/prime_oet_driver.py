# Databricks notebook source
# =============================================================================
# driver/prime_dp1_driver.py
# DP1 Full Priming Driver — Obligation & Expenditure Tracker
#
# Orchestrates all child notebooks in strict dependency order.
# Entry point for the Databricks DP1 prime job.
#
# Dependency graph:
#   [1] prime_dim_date          (no deps — generated spine)
#        │
#   [2] prime_dim_agency        (no deps — Silver lookup tables)
#        │
#   [3] prime_dim_ia            (depends on: dim_agency)  ← IMP #1 #2
#        │
#   [4] prime_dim_loa           (depends on: dim_agency)
#        │
#   [5] prime_dim_award         (depends on: dim_ia, dim_agency)  ← IMP #3
#        │
#   [6] prime_dim_funding       (depends on: dim_ia)  ← IMP #4
#        │
#   [7] prime_dim_line_item     (depends on: dim_award)  ← IMP #5
#        │
#   [8] prime_fact_obligation_expenditure  (depends on: ALL dims)  ← IMP #6
#
# IMPROVEMENT #7 (v1.1.0) — force_from_step widget:
#   Adds a force_from_step widget (integer, default 1) that allows operators
#   to restart the job from any step, skipping all prior steps cleanly.
#   Use cases:
#     force_from_step=5 → re-run dim_award onward (skip date/agency/ia/loa)
#     force_from_step=8 → re-run fact only (all dims already loaded)
#   The driver writes a DRIVER_WARNING audit row when the override is active,
#   listing which steps were skipped.  This distinguishes override runs from
#   clean full runs in the pipeline_audit table.
#
#   Safety: steps before force_from_step are NOT truncated.  Their Gold tables
#   must already be populated.  If they are empty, the fact notebook will produce
#   sentinel -1 FK values.  The driver emits a pre-flight row count check for
#   any skipped critical dims to warn the operator.
#
# All steps are critical=True.  On failure the job halts and writes DRIVER_FAILURE.
# Idempotency: every child notebook issues TRUNCATE before INSERT.
#              Re-running the full driver (force_from_step=1) is always safe.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import time

dbutils.widgets.text("run_id",           "", "Pipeline Run ID (leave blank to auto-generate)")
dbutils.widgets.text("job_name",         "dp1_prime_full", "Job Name")
dbutils.widgets.text("timeout_seconds",  "3600", "Per-notebook timeout in seconds")
# IMPROVEMENT #7: force_from_step widget for partial restart
dbutils.widgets.text("force_from_step",  "1",
    "Start from step N — skip steps 1 through N-1 (default 1 = full run)")

RUN_ID          = dbutils.widgets.get("run_id") or f"dp1-prime-{get_spark_app_id()}"
JOB             = dbutils.widgets.get("job_name")
TIMEOUT         = int(dbutils.widgets.get("timeout_seconds"))
FORCE_FROM_STEP = int(dbutils.widgets.get("force_from_step") or "1")

PARAMS = {"run_id": RUN_ID, "job_name": JOB}

print("=" * 65)
print(f"  DP1 Full Prime — Obligation & Expenditure Tracker")
print(f"  Run ID         : {RUN_ID}")
print(f"  Job            : {JOB}")
print(f"  Timeout        : {TIMEOUT}s per notebook")
print(f"  force_from_step: {FORCE_FROM_STEP}  "
      f"{'(FULL RUN)' if FORCE_FROM_STEP == 1 else '⚠ PARTIAL RESTART — steps 1-' + str(FORCE_FROM_STEP - 1) + ' SKIPPED'}")
print(f"  Catalog        : {CATALOG}")
print("=" * 65)

# COMMAND ----------

# ── Notebook paths ────────────────────────────────────────────────────────────
COMMON = "../data_products/common/priming"
OET    = "../data_products/oet/priming"

# Ordered task list: (step_num, label, relative_path)
# All steps are critical — the job halts on any failure.
TASKS = [
    (1, "prime_dim_date",                    f"{COMMON}/prime_dim_date"),
    (2, "prime_dim_agency",                  f"{COMMON}/prime_dim_agency"),
    (3, "prime_dim_ia",                      f"{COMMON}/prime_dim_ia"),
    (4, "prime_dim_loa",                     f"{COMMON}/prime_dim_loa"),
    (5, "prime_dim_award",                   f"{COMMON}/prime_dim_award"),
    (6, "prime_dim_funding",                 f"{COMMON}/prime_dim_funding"),
    (7, "prime_dim_line_item",               f"{COMMON}/prime_dim_line_item"),
    (8, "prime_fact_obligation_expenditure", f"{OET}/prime_fact_obligation_expenditure"),
]

# ── IMPROVEMENT #7: pre-flight check for skipped critical dims ────────────────
# When force_from_step > 1, warn if any skipped dim table appears empty.
# An empty skipped dim will cause the fact to emit all-sentinel -1 FKs.
CRITICAL_DIM_TABLES = {
    2: gold("common", "dim_agency"),
    3: gold("common", "dim_ia"),
    4: gold("common", "dim_loa"),
    5: gold("common", "dim_award"),
    6: gold("common", "dim_funding"),
    7: gold("common", "dim_line_item"),
}

if FORCE_FROM_STEP > 1:
    print(f"\n[PARTIAL RESTART] Skipping steps 1–{FORCE_FROM_STEP - 1}.")
    print(f"[PRE-FLIGHT]  Checking skipped dim tables are populated...")
    empty_dims = []
    for step_n, tbl in CRITICAL_DIM_TABLES.items():
        if step_n < FORCE_FROM_STEP:
            cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
            status = "OK" if cnt > 0 else "EMPTY ⚠"
            print(f"  Step {step_n}  {tbl:<55}  rows={cnt:>10,}  [{status}]")
            if cnt == 0:
                empty_dims.append((step_n, tbl))

    if empty_dims:
        warn_msg = (
            f"Pre-flight WARNING: the following skipped dim tables are EMPTY — "
            f"the fact step will produce all-sentinel FKs for these dimensions: "
            f"{[t for _, t in empty_dims]}. "
            f"Set force_from_step=1 to re-run all steps, or run the missing dims first."
        )
        print(f"\n  ⚠  {warn_msg}")
        # Write a WARNING audit row so the issue is visible in pipeline_audit
        spark.sql(f"""
            INSERT INTO {AUDIT_TABLE}
            {_AUDIT_COLS}
            VALUES (
                '{RUN_ID}', '{JOB}', 'DRIVER_WARNING', '{DATA_PRODUCT}', 'gold',
                'dp1_prime_driver', '', '', 'FULL_PRIME',
                'WARNING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
                0, 0, 0, 0, 0, NULL, NULL, NULL,
                '{warn_msg[:900].replace("'", "''")}', NULL,
                '{get_cluster_id()}', '{get_spark_app_id()}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
            )
        """)
    else:
        print(f"  ✓  All skipped dims are populated — safe to proceed from step {FORCE_FROM_STEP}.")

# COMMAND ----------

# ── Execution engine ──────────────────────────────────────────────────────────
results   = []   # (step_num, label, status, duration_s, error)
job_start = time.time()

for step_num, label, path in TASKS:

    # IMPROVEMENT #7: skip steps before force_from_step
    if step_num < FORCE_FROM_STEP:
        print(f"\n  ↷  Step {step_num}/{len(TASKS)} : {label}  [SKIPPED — force_from_step={FORCE_FROM_STEP}]")
        results.append((step_num, label, "SKIPPED", 0.0, None))
        continue

    print(f"\n{'─' * 65}")
    print(f"  Step {step_num}/{len(TASKS)} : {label}")
    print(f"  Path            : {path}")
    print(f"{'─' * 65}")

    t0     = time.time()
    status = "PENDING"
    err    = None

    try:
        result  = dbutils.notebook.run(path, timeout_seconds=TIMEOUT, arguments=PARAMS)
        elapsed = round(time.time() - t0, 1)

        if "SUCCESS" in str(result):
            status = "SUCCESS"
            print(f"  ✓  {label} completed in {elapsed}s")
        else:
            status = "WARN"
            err    = f"Unexpected exit value: {result}"
            print(f"  ⚠  {label} returned '{result}' in {elapsed}s")

    except Exception as e:
        elapsed = round(time.time() - t0, 1)
        status  = "FAILED"
        err     = str(e)
        print(f"  ✗  {label} FAILED after {elapsed}s")
        print(f"     Error: {err[:300]}")

        results.append((step_num, label, status, elapsed, err))

        # All steps are critical — halt on any failure
        fail_msg = f"Step {step_num} ({label}) failed: {err[:500]}"
        try:
            spark.sql(f"""
                INSERT INTO {AUDIT_TABLE}
                {_AUDIT_COLS}
                VALUES (
                    '{RUN_ID}', '{JOB}', 'DRIVER_FAILURE', '{DATA_PRODUCT}', 'gold',
                    'dp1_prime_driver', '', '', 'FULL_PRIME',
                    'FAILED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
                    {int(time.time() - job_start)},
                    0, 0, 0, 0, 0, NULL, NULL, NULL,
                    '{fail_msg.replace("'", "''")}', NULL,
                    '{get_cluster_id()}', '{get_spark_app_id()}',
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
                )
            """)
        except Exception as audit_err:
            print(f"  [WARN] Could not write driver failure to audit: {audit_err}")

        print(f"\n{'=' * 65}")
        print(f"  JOB HALTED at Step {step_num}: {label}")
        completed = [r[1] for r in results if r[2] == "SUCCESS"]
        print(f"  Completed steps: {completed}")
        if FORCE_FROM_STEP > 1:
            skipped = [r[1] for r in results if r[2] == "SKIPPED"]
            print(f"  Skipped steps  : {skipped}  (force_from_step={FORCE_FROM_STEP})")
        print(f"{'=' * 65}")
        raise RuntimeError(fail_msg) from e

    results.append((step_num, label, status, elapsed, err))

# COMMAND ----------

# ── Final summary ─────────────────────────────────────────────────────────────
total_elapsed = round(time.time() - job_start, 1)

print(f"\n{'=' * 65}")
print(f"  DP1 FULL PRIME COMPLETE")
print(f"  Total elapsed : {total_elapsed}s  ({total_elapsed / 60:.1f} min)")
print(f"  Run ID        : {RUN_ID}")
if FORCE_FROM_STEP > 1:
    print(f"  ⚠  Partial restart: steps 1–{FORCE_FROM_STEP - 1} were skipped")
print(f"{'=' * 65}")
print(f"  {'Step':<4}  {'Label':<44}  {'Status':<8}  {'Secs':>6}")
print(f"  {'─' * 4}  {'─' * 44}  {'─' * 8}  {'─' * 6}")

all_success = True
for step_num, label, status, elapsed, err in results:
    icon = "✓" if status == "SUCCESS" else ("↷" if status == "SKIPPED" else ("⚠" if status == "WARN" else "✗"))
    print(f"  {icon} {step_num:<3}  {label:<44}  {status:<8}  {elapsed:>6.1f}s")
    if status not in ("SUCCESS", "SKIPPED", "WARN"):
        all_success = False

print(f"{'=' * 65}")

# ── Write driver-level SUCCESS to pipeline_audit ──────────────────────────────
if all_success:
    n_run     = sum(1 for r in results if r[2] == "SUCCESS")
    n_skipped = sum(1 for r in results if r[2] == "SKIPPED")

    # IMPROVEMENT #7: if steps were skipped, record as DRIVER_WARNING not DRIVER_COMPLETE
    driver_event  = "DRIVER_COMPLETE" if FORCE_FROM_STEP == 1 else "DRIVER_WARNING"
    driver_note   = (
        None if FORCE_FROM_STEP == 1
        else f"Partial restart: steps 1-{FORCE_FROM_STEP - 1} were skipped (force_from_step={FORCE_FROM_STEP})"
    )

    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE}
        {_AUDIT_COLS}
        VALUES (
            '{RUN_ID}', '{JOB}', '{driver_event}', '{DATA_PRODUCT}', 'gold',
            'dp1_prime_driver', 'aasbs,billing', 'all_dp1_sources', 'FULL_PRIME',
            '{'WARNING' if FORCE_FROM_STEP > 1 else 'SUCCESS'}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            {int(total_elapsed)}, 0,
            {n_run}, {n_run}, 0, 0,
            NULL, NULL, NULL,
            {'NULL' if driver_note is None else "'" + driver_note.replace("'","''") + "'"},
            NULL,
            '{get_cluster_id()}', '{get_spark_app_id()}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        )
    """)
    print(f"\n  pipeline_audit driver record written: {driver_event} ✓")
    dbutils.notebook.exit("SUCCESS")
else:
    print(f"\n  ⚠  Some steps failed — review errors above")
    dbutils.notebook.exit("PARTIAL_SUCCESS")
