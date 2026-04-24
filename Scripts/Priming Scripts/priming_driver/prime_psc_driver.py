# Databricks notebook source
# =============================================================================
# driver/prime_dp6_driver.py
# DP6 Full Priming Driver — CLIN Pricing & Service Charge Catalog (PSC)
#
# Orchestrates five child notebooks in dependency order.
#
# Execution order:
#   Step 1  prime_dim_cost_rate_type          (SCD1 — independent)
#   Step 2  prime_dim_clin_subtype            (SCD1 — independent)  ← I-DP6-1
#   Step 3  prime_dim_billing_period          (SCD1 — independent)  ← I-DP6-3
#   Step 4  prime_fact_clin_pricing           (INSERT-only guard)    ← I-DP6-2/5
#   Step 5  prime_fact_service_charge_schedule (INSERT-only guard)   ← I-DP6-7
#
# Pre-requisites (DP1 common dims must be populated):
#   common.dim_line_item, common.dim_award, common.dim_ia, common.dim_loa
#
# Gate logic:
#   Steps 1–3 (dims) must all succeed before Steps 4–5 (facts) run.
#   force_fact=true bypasses the dim gate for recovery scenarios.
#
# IMPROVEMENT I-DP6-6 (built-in from first generation):
#   Three distinct driver audit outcomes — consistent with DP2–DP5 standards:
#     Clean run (all dims SUCCESS)     → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)  → DRIVER_WARNING   status=WARNING
#     Hard failure / exception         → DRIVER_FAILURE   status=FAILED
#   A DRIVER_WARNING inline audit row is written to pipeline_audit BEFORE
#   fact notebooks run when force_fact is active, listing the bypassed dims.
#   This ensures override runs are permanently distinguishable from clean runs.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-DP6-6: force_fact DRIVER_WARNING built in from first generation
dbutils.widgets.text("force_fact",  "false",
    "Force fact runs despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp6_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP6"
DRIVER_NB    = "prime_dp6_driver"
DRIVER_TABLE = "assist_dev.psc.DRIVER"

print(f"{'='*70}")
print(f"  DP6 PRIME DRIVER — CLIN Pricing & Service Charge Catalog")
print(f"  run_id     : {RUN_ID}")
print(f"  env        : {ENV}")
print(f"  force_fact : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — writes DRIVER_WARNING on use' if FORCE_FACT else ''}")
print(f"  started_at : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

NB = {
    "dim_cost_rate_type"          : "../data_products/psc/priming/prime_dim_cost_rate_type",
    "dim_clin_subtype"            : "../data_products/psc/priming/prime_dim_clin_subtype",
    "dim_billing_period"          : "../data_products/psc/priming/prime_dim_billing_period",
    "fact_clin_pricing"           : "../data_products/psc/priming/prime_fact_clin_pricing",
    "fact_service_charge_schedule": "../data_products/psc/priming/prime_fact_service_charge_schedule",
}

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}
results       = {}
failed_nbs    = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify DP1 common dims
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 common dim pre-requisites...")

dp1_checks = {
    "common.dim_line_item" : "assist_dev.common.dim_line_item",
    "common.dim_award"     : "assist_dev.common.dim_award",
    "common.dim_ia"        : "assist_dev.common.dim_ia",
    "common.dim_loa"       : "assist_dev.common.dim_loa",
}

preflight_ok = True
for label, tbl in dp1_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:30s}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = "Pre-flight FAILED — one or more DP1 common dim tables are empty."
    print(f"\n  ✗ {msg}")
    #audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(msg), msg, start_ts)
    dbutils.notebook.exit("PREFLIGHT_FAILURE")

print(f"\n  ✓ Pre-flight passed.")

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────
def run_notebook(key: str, path: str, params: dict) -> str:
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
# Steps 1–3: Dims (independent of each other)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/5] prime_dim_cost_rate_type")
run_notebook("dim_cost_rate_type", NB["dim_cost_rate_type"], COMMON_PARAMS)

print(f"\n[STEP 2/5] prime_dim_clin_subtype")
run_notebook("dim_clin_subtype", NB["dim_clin_subtype"], COMMON_PARAMS)

print(f"\n[STEP 3/5] prime_dim_billing_period")
run_notebook("dim_billing_period", NB["dim_billing_period"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Steps 4–5: Facts — gated on dim success
# ─────────────────────────────────────────────────────────────────────────────
DIM_KEYS = ("dim_cost_rate_type", "dim_clin_subtype", "dim_billing_period")
dims_ok  = all(results.get(k) in ("SUCCESS", "SKIPPED") for k in DIM_KEYS)

def run_fact_step(step_num: int, key: str):
    """Run a fact notebook with the standard dim gate / DRIVER_WARNING logic."""
    print(f"\n[STEP {step_num}/5] prime_{key}")

    if dims_ok:
        run_notebook(key, NB[key], COMMON_PARAMS)

    elif FORCE_FACT:
        # IMPROVEMENT I-DP6-6: write DRIVER_WARNING before fact proceeds
        bypass_msg = (
            f"force_fact=true — proceeding despite failed dims: {failed_nbs}. "
            f"Fact rows referencing failed dims will carry sentinel FK values (-1)."
        )
        print(f"  ⚠ {bypass_msg}")
        spark.sql(f"""
            INSERT INTO assist_dev.common.pipeline_audit
                (run_id, product, notebook, target_table, run_type,
                 status, started_at, error_message)
            VALUES (
                '{RUN_ID}', '{PRODUCT}', 'prime_dp6_driver',
                '{DRIVER_TABLE}', 'FULL_PRIME',
                'WARNING', current_timestamp(),
                '{bypass_msg.replace("'", "''")}'
            )
        """)
        run_notebook(key, NB[key], COMMON_PARAMS)

    else:
        results[key] = "BLOCKED"
        block_msg = (
            f"Fact step BLOCKED — failed dims: {failed_nbs}. "
            f"Fix dim failure then re-run, or set force_fact=true to override."
        )
        print(f"  ✗ {key}: BLOCKED — {block_msg}")
        spark.sql(f"""
            INSERT INTO assist_dev.common.pipeline_audit
                (run_id, product, notebook, target_table, run_type,
                 status, started_at, error_message)
            VALUES (
                '{RUN_ID}', '{PRODUCT}', 'prime_{key}',
                'assist_dev.psc.{key}', 'FULL_PRIME',
                'BLOCKED', current_timestamp(),
                '{block_msg.replace("'", "''")}'
            )
        """)

run_fact_step(4, "fact_clin_pricing")
run_fact_step(5, "fact_service_charge_schedule")

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP6 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status == "SKIPPED" else "✗")
    print(f"  {icon}  {nb:<40s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# IMPROVEMENT I-DP6-6: three distinct audit outcomes
if all_success and not failed_nbs:
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = None
elif all_success and failed_nbs:
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = (
        f"force_fact=true used. Failed dims bypassed: {failed_nbs}. "
        f"Fact rows for these dims carry sentinel FK values (-1)."
    )
else:
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = f"Failed steps: {failed_nbs}"

print(f"\n  Driver status : {driver_status}")
if driver_msg:
    print(f"  Note          : {driver_msg}")
print(f"  Finished at   : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

if audit_run_status == "SUCCESS":
    audit_success(spark, RUN_ID, DRIVER_TABLE, 0, 0, driver_start_ts)
else:
    audit_fail(spark, RUN_ID, DRIVER_TABLE, str(driver_msg) if driver_msg else "See individual notebook audit rows", "look error in notebooks", driver_start_ts)

dbutils.notebook.exit(driver_status)
