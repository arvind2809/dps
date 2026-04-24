# Databricks notebook source
# =============================================================================
# driver/prime_dp7_driver.py
# DP7 Full Priming Driver — Solicitation & Market Intelligence (SMI)
#
# Orchestrates three child notebooks in dependency order.
#
# Execution order:
#   Step 1  prime_dim_posting_type        (SCD1 — independent)   ← I-DP7-5
#   Step 2  prime_dim_piid                (SCD1 — independent)   ← I-DP7-2
#   Step 3  prime_fact_solicitation_response (INSERT-only guard)  ← I-DP7-1/3/4/6
#
# Pre-requisites:
#   DP1: common.dim_date, common.dim_agency, common.dim_ia
#   DP2: alt.dim_solicitation, alt.dim_acquisition, alt.dim_contractor
#        (pre-flight inside the fact notebook — IMPROVEMENT I-DP7-1)
#
# Gate logic:
#   Steps 1–2 (dims) must succeed before Step 3 (fact) runs.
#   force_fact=true bypasses the dim gate for recovery scenarios.
#
# IMPROVEMENT I-DP7-7 (built-in from first generation):
#   Three distinct driver audit outcomes — consistent with DP2–DP6 standards:
#     Clean run (all dims SUCCESS)     → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)  → DRIVER_WARNING   status=WARNING
#     Hard failure / exception         → DRIVER_FAILURE   status=FAILED
#   DRIVER_WARNING inline audit row written before the fact step runs.
#   Pre-flight PREFLIGHT_FAILURE is a separate outcome from DRIVER_FAILURE:
#   it indicates a dependency is absent, not that a notebook raised an error.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-DP7-7: DRIVER_WARNING on force_fact built in from first generation
dbutils.widgets.text("force_fact",  "false",
    "Force fact run despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp7_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP7"
DRIVER_NB    = "prime_dp7_driver"
DRIVER_TABLE = "assist_dev.smi.DRIVER"

print(f"{'='*70}")
print(f"  DP7 PRIME DRIVER — Solicitation & Market Intelligence")
print(f"  run_id     : {RUN_ID}")
print(f"  env        : {ENV}")
print(f"  force_fact : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — writes DRIVER_WARNING on use' if FORCE_FACT else ''}")
print(f"  started_at : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

NB = {
    "dim_posting_type"           : "../data_products/smi/priming/prime_dim_posting_type",
    "dim_piid"                   : "../data_products/smi/priming/prime_dim_piid",
    "fact_solicitation_response" : "../data_products/smi/priming/prime_fact_solicitation_response",
}

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}
results       = {}
failed_nbs    = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify DP1 and DP2 common dims
# DP2 dims are also verified inside the fact notebook (I-DP7-1) but
# checking here catches the issue before any notebook runs.
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 + DP2 pre-requisites...")

prereq_checks = {
    "common.dim_date"       : "assist_dev.common.dim_date",
    "common.dim_agency"     : "assist_dev.common.dim_agency",
    "common.dim_ia"         : "assist_dev.common.dim_ia",
    "alt.dim_solicitation"  : "assist_dev.alt.dim_solicitation",
    "alt.dim_acquisition"   : "assist_dev.alt.dim_acquisition",
    "alt.dim_contractor"    : "assist_dev.alt.dim_contractor",
}

preflight_ok = True
for label, tbl in prereq_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:30s}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more DP1 or DP2 prerequisite tables are empty. "
        "Ensure DP1 and DP2 prime jobs have completed before running DP7."
    )
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
# Step 1 — dim_posting_type (independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/3] prime_dim_posting_type")
run_notebook("dim_posting_type", NB["dim_posting_type"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — dim_piid (independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 2/3] prime_dim_piid")
run_notebook("dim_piid", NB["dim_piid"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — fact_solicitation_response (INSERT-only; gate on dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 3/3] prime_fact_solicitation_response")

DIM_KEYS = ("dim_posting_type", "dim_piid")
dims_ok  = all(results.get(k) in ("SUCCESS", "SKIPPED") for k in DIM_KEYS)

if dims_ok:
    run_notebook("fact_solicitation_response", NB["fact_solicitation_response"], COMMON_PARAMS)

elif FORCE_FACT:
    # IMPROVEMENT I-DP7-7: DRIVER_WARNING before fact runs on override
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
            '{RUN_ID}', '{PRODUCT}', 'prime_dp7_driver',
            '{DRIVER_TABLE}', 'FULL_PRIME',
            'WARNING', current_timestamp(),
            '{bypass_msg.replace("'", "''")}'
        )
    """)
    run_notebook("fact_solicitation_response", NB["fact_solicitation_response"], COMMON_PARAMS)

else:
    results["fact_solicitation_response"] = "BLOCKED"
    block_msg = (
        f"Fact step BLOCKED — failed dims: {failed_nbs}. "
        f"Fix dim failure then re-run, or set force_fact=true to override."
    )
    print(f"  ✗ fact_solicitation_response: BLOCKED — {block_msg}")
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_fact_solicitation_response',
            'assist_dev.smi.fact_solicitation_response', 'FULL_PRIME',
            'BLOCKED', current_timestamp(),
            '{block_msg.replace("'", "''")}'
        )
    """)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP7 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status == "SKIPPED" else "✗")
    print(f"  {icon}  {nb:<40s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# IMPROVEMENT I-DP7-7: three distinct audit outcomes
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
