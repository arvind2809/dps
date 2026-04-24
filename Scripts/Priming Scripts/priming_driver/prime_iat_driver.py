# Databricks notebook source
# =============================================================================
# driver/prime_dp9_driver.py
# DP9 Full Priming Driver — IA Lifecycle & Amendment Tracker (IAT)
#
# Orchestrates five child notebooks in strict dependency order.
#
# Execution order:
#   Step 1  prime_dim_amend_action      (SCD1 — independent)  ← I-DP9-1
#   Step 2  prime_dim_reviewer          (SCD1 — independent)  ← I-DP9-2
#   Step 3  prime_dim_fin_code_history  (SCD1 — independent)  ← I-DP9-3
#   Step 4  prime_fact_ia_amendment     (INSERT-only)          ← I-DP9-4/5/6
#   Step 5  prime_fact_ia_review        (INSERT-only)          ← I-DP9-7
#
# IMPROVEMENT I-DP9-8 — DP1 + IAT-dim pre-flight guard (built-in):
#   Pre-flight verifies the following tables are non-empty before any
#   notebook runs:
#     DP1 deps: common.dim_ia, common.dim_agency, common.dim_date,
#               common.dim_funding, common.dim_loa
#   Both fact notebooks additionally require iat.dim_amend_action and
#   iat.dim_reviewer — the driver gate logic enforces Steps 1–3 success
#   before Steps 4–5 run.
#
# IMPROVEMENT I-DP9-9 — Three-outcome audit trail with force_fact (built-in):
#   Consistent with DP5, DP6, DP7 standards — built in from first generation:
#     Clean run (all dims succeed)    → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure) → DRIVER_WARNING   status=WARNING
#     Hard failure / exception        → DRIVER_FAILURE   status=FAILED
#   DRIVER_WARNING audit row written to pipeline_audit before fact steps
#   run when force_fact is active. This permanently distinguishes override
#   runs from clean runs.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-DP9-9: force_fact / DRIVER_WARNING built in from first generation
dbutils.widgets.text("force_fact",  "false",
    "Force fact runs despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp9_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP9"
DRIVER_NB    = "prime_dp9_driver"
DRIVER_TABLE = "assist_dev.iat.DRIVER"

print(f"{'='*70}")
print(f"  DP9 PRIME DRIVER — IA Lifecycle & Amendment Tracker")
print(f"  run_id     : {RUN_ID}")
print(f"  env        : {ENV}")
print(f"  force_fact : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — writes DRIVER_WARNING on use' if FORCE_FACT else ''}")
print(f"  started_at : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

NB = {
    "dim_amend_action"      : "../data_products/iat/prime_dim_amend_action",
    "dim_reviewer"          : "../data_products/iat/prime_dim_reviewer",
    "dim_fin_code_history"  : "../data_products/iat/prime_dim_fin_code_history",
    "fact_ia_amendment"     : "../data_products/iat/prime_fact_ia_amendment",
    "fact_ia_review"        : "../data_products/iat/prime_fact_ia_review",
}
COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}
results       = {}
failed_nbs    = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# IMPROVEMENT I-DP9-8: Pre-flight — DP1 common dims must be populated
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 common dim pre-requisites...")
dp1_checks = {
    "common.dim_ia"      : "assist_dev.common.dim_ia",
    "common.dim_agency"  : "assist_dev.common.dim_agency",
    "common.dim_date"    : "assist_dev.common.dim_date",
    "common.dim_funding" : "assist_dev.common.dim_funding",
    "common.dim_loa"     : "assist_dev.common.dim_loa",
}
preflight_ok = True
for label, tbl in dp1_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:<25}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more DP1 common dim tables are empty. "
        "Ensure DP1 prime has completed before running DP9."
    )
    print(f"\n  ✗ {msg}")
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
    dbutils.notebook.exit("PREFLIGHT_FAILURE")

print(f"\n  ✓ Pre-flight passed.")

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────
def run_notebook(key, path, params):
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
# Steps 1–3: dims (independent of each other)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/5] prime_dim_amend_action")
run_notebook("dim_amend_action", NB["dim_amend_action"], COMMON_PARAMS)

print(f"\n[STEP 2/5] prime_dim_reviewer")
run_notebook("dim_reviewer", NB["dim_reviewer"], COMMON_PARAMS)

print(f"\n[STEP 3/5] prime_dim_fin_code_history")
run_notebook("dim_fin_code_history", NB["dim_fin_code_history"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Steps 4–5: facts — gated on dims 1–3
# IMPROVEMENT I-DP9-9: force_fact / DRIVER_WARNING pattern
# ─────────────────────────────────────────────────────────────────────────────
DIM_KEYS = ("dim_amend_action", "dim_reviewer", "dim_fin_code_history")
dims_ok  = all(results.get(k) in ("SUCCESS", "SKIPPED") for k in DIM_KEYS)

def run_fact_step(step_num, key):
    print(f"\n[STEP {step_num}/5] prime_{key}")
    if dims_ok:
        run_notebook(key, NB[key], COMMON_PARAMS)
    elif FORCE_FACT:
        # IMPROVEMENT I-DP9-9: DRIVER_WARNING before fact proceeds on override
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
                '{RUN_ID}', '{PRODUCT}', 'prime_dp9_driver',
                '{DRIVER_TABLE}', 'FULL_PRIME', 'WARNING',
                current_timestamp(),
                '{bypass_msg.replace("'", "''")}'
            )
        """)
        run_notebook(key, NB[key], COMMON_PARAMS)
    else:
        results[key] = "BLOCKED"
        block_msg = (
            f"Fact step BLOCKED — failed dims: {failed_nbs}. "
            f"Fix dim failure and re-run, or set force_fact=true to override."
        )
        print(f"  ✗ {key}: BLOCKED — {block_msg}")
        spark.sql(f"""
            INSERT INTO assist_dev.common.pipeline_audit
                (run_id, product, notebook, target_table, run_type,
                 status, started_at, error_message)
            VALUES (
                '{RUN_ID}', '{PRODUCT}', 'prime_{key}',
                'assist_dev.iat.{key.replace("_","")}', 'FULL_PRIME',
                'BLOCKED', current_timestamp(),
                '{block_msg.replace("'", "''")}'
            )
        """)

run_fact_step(4, "fact_ia_amendment")
run_fact_step(5, "fact_ia_review")

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP9 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status == "SKIPPED" else "✗")
    print(f"  {icon}  {nb:<40s}  {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# IMPROVEMENT I-DP9-9: three distinct audit outcomes
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
    audit_success(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE,
                  driver_start_ts, rows_read=0, rows_written=0)
else:
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE,
                  driver_start_ts,
                  error_msg=str(driver_msg) if driver_msg
                            else "See individual notebook audit rows")

dbutils.notebook.exit(driver_status)
