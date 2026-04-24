# Databricks notebook source
# =============================================================================
# driver/prime_dp3_driver.py
# DP3 Full Priming Driver — FPDS Reporting Extract
#
# Orchestrates five child notebooks in dependency order.
# Entry point for the Databricks DP3 prime job.
#
# Execution order:
#   Step 1  prime_dim_fpds_psc         (SCD1 — independent)
#   Step 2  prime_dim_fpds_agency      (SCD1 — independent)
#   Step 3  prime_dim_fpds_award       (SCD1 — needs DP1)   ← I-DP3-4
#   Step 4  prime_dim_fpds_contractor  (SCD2 — needs DP2)   ← B-DP3-1, I-DP3-5
#   Step 5  prime_fact_fpds_transaction (INSERT-only — needs all dims + DP1 + DP2)
#                                       ← B-DP3-2, B-DP3-3, B-DP3-4,
#                                          I-DP3-1, I-DP3-2
#
# Pre-requisites:
#   DP1 must have completed: common.{dim_award, dim_ia, dim_loa, dim_date}
#   DP2 must have completed: alt.dim_contractor
#
# Gate logic:
#   Steps 1–4 (dims) must ALL succeed before Step 5 (fact) runs.
#   force_fact=true bypasses the dim gate for recovery scenarios.
#
# BUG FIX B-DP3-1 (v1.2.0 — in driver pre-flight):
#   Pre-flight DP2 check corrected from alt.dim_fpds_contractor (non-existent)
#   to alt.dim_contractor.  This was fixed in v1.1.0; confirmed correct here.
#
# IMPROVEMENT I-DP3-3 (v1.2.0):
#   force_fact=true bypass now writes a DRIVER_WARNING audit record listing
#   which dim steps were bypassed.  Previously the driver reported
#   DRIVER_COMPLETE / SUCCESS regardless of whether dims had failed, making
#   override runs indistinguishable from clean runs in pipeline_audit.
#   Now:
#     Clean run (all dims SUCCESS)      → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)   → DRIVER_WARNING   status=WARNING
#     Hard failure (exception raised)   → DRIVER_FAILURE   status=FAILED
#     Fact BLOCKED (no override)        → DRIVER_FAILURE   status=FAILED
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
import time
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-DP3-3: force_fact now writes DRIVER_WARNING audit record
dbutils.widgets.text("force_fact",  "false",
    "Force fact run despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp3_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP3"
DRIVER_NB    = "prime_dp3_driver"
DRIVER_TABLE = "assist_dev.fre.DRIVER"

print(f"{'=' * 70}")
print(f"  DP3 PRIME DRIVER — FPDS Reporting Extract")
print(f"  run_id      : {RUN_ID}")
print(f"  env         : {ENV}")
print(f"  force_fact  : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — dim gate bypassed if dims fail' if FORCE_FACT else ''}")
print(f"  timeout_sec : {TIMEOUT_SEC}")
print(f"  started_at  : {datetime.now(timezone.utc).isoformat()}")
print(f"{'=' * 70}")

# ─────────────────────────────────────────────────────────────────────────────
# Notebook paths
# ─────────────────────────────────────────────────────────────────────────────
NB = {
    "dim_fpds_psc"          : "../data_products/fre/priming/prime_dim_fpds_psc",
    "dim_fpds_agency"       : "../data_products/fre/priming/prime_dim_fpds_agency",
    "dim_fpds_award"        : "../data_products/fre/priming/prime_dim_fpds_award",
    "dim_fpds_contractor"   : "../data_products/fre/priming/prime_dim_fpds_contractor",
    "fact_fpds_transaction" : "../data_products/fre/priming/prime_fact_fpds_transaction",
}

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}

results    = {}
failed_nbs = []

#driver_start_ts = audit_start(spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME")
driver_start_ts = audit_start(spark, RUN_ID, DRIVER_NB, PRODUCT, DRIVER_TABLE,
                        source_schema="aasbs", source_table="silver")

# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify DP1 and DP2 prerequisite tables are populated
# BUG FIX B-DP3-1 (driver side): DP2 check confirmed as alt.dim_contractor
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 and DP2 pre-requisites...")

dp1_checks = {
    "common.dim_award" : "assist_dev.common.dim_award",
    "common.dim_ia"    : "assist_dev.common.dim_ia",
    "common.dim_loa"   : "assist_dev.common.dim_loa",
    "common.dim_date"  : "assist_dev.common.dim_date",
}
# BUG FIX B-DP3-1: was alt.dim_fpds_contractor (does not exist)
dp2_checks = {
    "alt.dim_contractor" : "assist_dev.alt.dim_contractor",
}

preflight_ok = True
for label, tbl in {**dp1_checks, **dp2_checks}.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:35s}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more DP1/DP2 prerequisite tables are empty. "
        "Ensure DP1 and DP2 prime jobs have completed before running DP3."
    )
    print(f"\n  ✗ {msg}")
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
    dbutils.notebook.exit("PREFLIGHT_FAILURE")

print(f"\n  ✓ Pre-flight passed — all DP1/DP2 prerequisite tables populated.")

# ─────────────────────────────────────────────────────────────────────────────
# Helper: run one child notebook
# ─────────────────────────────────────────────────────────────────────────────
def run_notebook(key: str, path: str, params: dict) -> str:
    """Execute a child notebook.  Returns 'SUCCESS' or 'FAILED'."""
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
# Step 1 — dim_fpds_psc  (SCD1, independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/5] prime_dim_fpds_psc")
run_notebook("dim_fpds_psc", NB["dim_fpds_psc"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — dim_fpds_agency  (SCD1, independent)
#   place_of_performance_state/country added as Silver-pending NULLs (v1.1.0)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 2/5] prime_dim_fpds_agency")
run_notebook("dim_fpds_agency", NB["dim_fpds_agency"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — dim_fpds_award  (SCD1, requires DP1)
#   IMPROVEMENT I-DP3-4: contract_type_cd distribution check added
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 3/5] prime_dim_fpds_award")
run_notebook("dim_fpds_award", NB["dim_fpds_award"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — dim_fpds_contractor  (SCD2, requires DP2 alt.dim_contractor)
#   BUG FIX B-DP3-1: source table corrected in notebook
#   IMPROVEMENT I-DP3-5: Silver join de-duplicated in notebook
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 4/5] prime_dim_fpds_contractor")
run_notebook("dim_fpds_contractor", NB["dim_fpds_contractor"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — fact_fpds_transaction  (INSERT-only; gate on all 4 dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 5/5] prime_fact_fpds_transaction")

dim_ok = all(
    results.get(k) in ("SUCCESS", "SKIPPED")
    for k in ("dim_fpds_psc", "dim_fpds_agency", "dim_fpds_award", "dim_fpds_contractor")
)

if dim_ok:
    # Clean run — all dims succeeded
    run_notebook("fact_fpds_transaction", NB["fact_fpds_transaction"], COMMON_PARAMS)

elif FORCE_FACT:
    # IMPROVEMENT I-DP3-3: override is active — write DRIVER_WARNING before proceeding
    bypass_msg = (
        f"force_fact=true — proceeding despite failed dims: {failed_nbs}. "
        f"Fact rows referencing these dims will have sentinel FK values (-1)."
    )
    print(f"  ⚠ {bypass_msg}")

    # Write inline WARNING audit row so the bypass is permanently visible
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_dp3_driver',
            '{DRIVER_TABLE}', 'FULL_PRIME',
            'WARNING', current_timestamp(),
            '{bypass_msg.replace("'", "''")}'
        )
    """)

    run_notebook("fact_fpds_transaction", NB["fact_fpds_transaction"], COMMON_PARAMS)

else:
    # Dim gate blocked — fact cannot run safely
    results["fact_fpds_transaction"] = "BLOCKED"
    block_msg = (
        f"Fact step BLOCKED — failed dims: {failed_nbs}. "
        f"Fix the dim failure and re-run, or set force_fact=true to override. "
        f"When force_fact is used a DRIVER_WARNING audit record is written."
    )
    print(f"  ✗ fact_fpds_transaction: BLOCKED — {block_msg}")
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_fact_fpds_transaction',
            'assist_dev.fre.fact_fpds_transaction', 'FULL_PRIME',
            'BLOCKED', current_timestamp(),
            '{block_msg.replace("'", "''")}'
        )
    """)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print(f"  DP3 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'=' * 70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status in ("SKIPPED", "WARNING") else "✗")
    print(f"  {icon}  {nb:<35s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# ── IMPROVEMENT I-DP3-3: three distinct audit outcomes ────────────────────────
if all_success and not failed_nbs:
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = None
elif all_success and failed_nbs:
    # force_fact used: fact succeeded but dims had failed → record as WARNING
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = (
        f"force_fact=true used.  Failed dims bypassed: {failed_nbs}.  "
        f"Fact rows for these dims have sentinel FK values (-1)."
    )
else:
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
