# Databricks notebook source
# =============================================================================
# driver/prime_alt_driver.py
# alt Full Priming Driver — Award Lifecycle Tracker
#
# Orchestrates five child notebooks in dependency order.
# Entry point for the Databricks alt prime job.
#
# Execution order:
#   Step 1  prime_dim_acquisition     (SCD2 — needs DP1 common dims)
#   Step 2  prime_dim_solicitation    (SCD1 — needs dim_acquisition)  ← IMP #8
#   Step 3  prime_dim_contractor      (SCD2 — independent)            ← IMP #9
#   Step 4  prime_dim_closeout        (SCD1 — independent)            ← IMP #10
#   Step 5  prime_fact_award_lifecycle (INSERT-only — needs all dims)
#
# Pre-requisites:
#   DP1 must have completed. assist_dev.common dims must be populated.
#
# Gate logic:
#   Steps 1–4 (dims) must ALL succeed before Step 5 (fact) runs.
#   force_fact=true widget bypasses the dim gate for recovery scenarios.
#
# IMPROVEMENT #11 (v1.1.0) — force_fact audit trail:
#   When force_fact=true is used, a DRIVER_WARNING row is written to
#   pipeline_audit listing the failed dim steps that were bypassed.
#   Previously the driver reported DRIVER_COMPLETE regardless of whether
#   dims had failed, making override runs indistinguishable from clean runs.
#   Now:
#     Clean run (all dims SUCCESS)     → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)  → DRIVER_WARNING   status=WARNING
#     Hard failure (exception raised)  → DRIVER_FAILURE   status=FAILED
#     Fact BLOCKED (no override set)   → DRIVER_FAILURE   status=FAILED
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
import time
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (leave blank to auto-generate)")
dbutils.widgets.text("env",         "dev", "Environment (prod / dev / test)")
# IMPROVEMENT #11: force_fact now writes audit warning when used
dbutils.widgets.text("force_fact",  "false",
    "Force fact run even if a dim failed (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"alt_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "alt"
DRIVER_NB    = "prime_alt_driver"
DRIVER_TABLE = "assist_dev.alt.DRIVER"

print(f"{'=' * 70}")
print(f"  alt PRIME DRIVER — Award Lifecycle Tracker")
print(f"  run_id      : {RUN_ID}")
print(f"  env         : {ENV}")
print(f"  force_fact  : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — dim gate will be bypassed if dims fail' if FORCE_FACT else ''}")
print(f"  timeout_sec : {TIMEOUT_SEC}")
print(f"  started_at  : {datetime.now(timezone.utc).isoformat()}")
print(f"{'=' * 70}")

# ─────────────────────────────────────────────────────────────────────────────
# Notebook paths
# ─────────────────────────────────────────────────────────────────────────────
NB = {
    "dim_acquisition"      : "../data_products/alt/priming/prime_dim_acquisition",
    "dim_solicitation"     : "../data_products/alt/priming/prime_dim_solicitation",
    "dim_contractor"       : "../data_products/alt/priming/prime_dim_contractor",
    "dim_closeout"         : "../data_products/alt/priming/prime_dim_closeout",
    "fact_award_lifecycle" : "../data_products/alt/priming/prime_fact_award_lifecycle",
}

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}

results    = {}   # notebook_key → "SUCCESS" | "FAILED" | "SKIPPED" | "BLOCKED"
failed_nbs = []

driver_start_ts = audit_start(spark, RUN_ID, DRIVER_NB, PRODUCT, DRIVER_TABLE,
                        source_schema="aasbs", source_table="silver")
#audit_start(spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME")

# ─────────────────────────────────────────────────────────────────────────────
# Helper: run one child notebook
# ─────────────────────────────────────────────────────────────────────────────
def run_notebook(key: str, path: str, params: dict) -> str:
    """Execute a child notebook. Returns 'SUCCESS' or 'FAILED'."""
    print(f"\n  → Launching: {key}")
    try:
        result = dbutils.notebook.run(path, TIMEOUT_SEC, params)
        status = "SUCCESS" if "SUCCESS" in str(result) else "SKIPPED"
        results[key] = status
        print(f"  ✓ {key}: {status}")
        return status
    except Exception as e:
        results[key] = "FAILED"
        failed_nbs.append(key)
        print(f"  ✗ {key}: FAILED — {str(e)[:300]}")
        return "FAILED"


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — dim_acquisition  (SCD2; depends on DP1 common dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/5] prime_dim_acquisition")
run_notebook("dim_acquisition", NB["dim_acquisition"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — dim_solicitation  (SCD1; depends on dim_acquisition)
#   IMPROVEMENT #8: total_responses now populated from solicit_response
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 2/5] prime_dim_solicitation")
run_notebook("dim_solicitation", NB["dim_solicitation"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — dim_contractor  (SCD2; independent)
#   IMPROVEMENT #9: post-load business_type format mismatch check
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 3/5] prime_dim_contractor")
run_notebook("dim_contractor", NB["dim_contractor"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — dim_closeout  (SCD1; independent)
#   IMPROVEMENT #10: safer checklist completion + value distribution check
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 4/5] prime_dim_closeout")
run_notebook("dim_closeout", NB["dim_closeout"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — fact_award_lifecycle  (INSERT-only; gate on all 4 dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 5/5] prime_fact_award_lifecycle")

dim_ok = all(
    results.get(k) in ("SUCCESS", "SKIPPED")
    for k in ("dim_acquisition", "dim_solicitation", "dim_contractor", "dim_closeout")
)

if dim_ok:
    # Clean run — all dims succeeded
    run_notebook("fact_award_lifecycle", NB["fact_award_lifecycle"], COMMON_PARAMS)

elif FORCE_FACT:
    # IMPROVEMENT #11: override is active — proceed but write a WARNING audit row
    # so the bypass is clearly visible in pipeline_audit.
    bypass_msg = (
        f"force_fact=true — proceeding despite failed dims: {failed_nbs}. "
        f"Fact rows for these dims will have sentinel FK values (-1)."
    )
    print(f"  ⚠ {bypass_msg}")

    # Write DRIVER_WARNING so the override is auditable — not silently SUCCESS
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_alt_driver',
            '{DRIVER_TABLE}', 'FULL_PRIME',
            'WARNING', current_timestamp(),
            '{bypass_msg.replace("'", "''")}'
        )
    """)

    run_notebook("fact_award_lifecycle", NB["fact_award_lifecycle"], COMMON_PARAMS)

else:
    # Dim gate blocked — fact cannot run
    results["fact_award_lifecycle"] = "BLOCKED"
    block_msg = (
        f"Fact step BLOCKED — failed dims: {failed_nbs}. "
        f"Fix the dim failure and re-run, or set force_fact=true to override. "
        f"When force_fact is used, a DRIVER_WARNING audit record will be written."
    )
    print(f"  ✗ fact_award_lifecycle: BLOCKED — {block_msg}")
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_fact_award_lifecycle',
            'assist_dev.alt.fact_award_lifecycle', 'FULL_PRIME',
            'BLOCKED', current_timestamp(),
            '{block_msg.replace("'", "''")}'
        )
    """)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print(f"  alt PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'=' * 70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status in ("SKIPPED","WARNING") else "✗")
    print(f"  {icon}  {nb:<35s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# ── IMPROVEMENT #11: distinguish clean / override / failed runs in audit ──────
if all_success and not failed_nbs:
    # Clean run: all steps succeeded with no dim failures
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = None

elif all_success and failed_nbs:
    # force_fact was used: fact succeeded but dims had failed — record as WARNING
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = (
        f"force_fact=true was used. Failed dims bypassed: {failed_nbs}. "
        f"Fact rows for these dims have sentinel FK values (-1)."
    )

else:
    # Something failed outright
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = f"Failed steps: {failed_nbs}"

print(f"\n  Driver status : {driver_status}")
if driver_msg:
    print(f"  Note          : {driver_msg}")
print(f"  Finished at   : {datetime.now(timezone.utc).isoformat()}")
print(f"{'=' * 70}")

if audit_run_status == "SUCCESS":
    audit_success(spark, RUN_ID, DRIVER_TABLE, 0, 0, driver_start_ts)

else:
    
    audit_fail(spark, RUN_ID, DRIVER_TABLE, str(driver_msg) if driver_msg else "See individual notebook audit rows", "look error in notebooks", driver_start_ts)


dbutils.notebook.exit(driver_status)
