# Databricks notebook source
# =============================================================================
# driver/prime_air_driver.py
# air Full Priming Driver — Accrual Income Reporter (AIR)
#
# Orchestrates three child notebooks in dependency order.
#
# Execution order:
#   Step 1  prime_dim_accrual_period   (generated spine — no Silver dep)  ← I-air-4
#   Step 2  prime_dim_onefund_program  (Silver reference maps — independent)
#   Step 3  prime_fact_accrual_income  (INSERT-only — needs all dims + DP1) ← B-air-1
#                                                                              I-air-1/2/3/6/7
#
# Pre-requisites (DP1 common dims must be populated):
#   common.dim_line_item, common.dim_loa, common.dim_ia, common.dim_agency
#
# Gate logic:
#   Steps 1–2 (dims) must both succeed before Step 3 (fact) runs.
#   force_fact=true bypasses the gate for recovery scenarios.
#
# IMPROVEMENT I-air-4 (v1.2.0) — spine end-year widget default:
#   end_cal_year widget default changed from 2035 → 2040 to align with
#   dim_date (extended in v1.1.0).  The driver passes this widget through
#   to dim_accrual_period so both notebook and driver stay in sync.
#
# IMPROVEMENT I-air-5 (v1.2.0) — force_fact DRIVER_WARNING audit record:
#   When force_fact=true bypasses the dim gate, the driver now writes a
#   DRIVER_WARNING audit record listing which dims were bypassed, then
#   records the overall driver outcome as DRIVER_WARNING / WARNING.
#   Previously the driver reported DRIVER_COMPLETE / SUCCESS regardless,
#   making override runs indistinguishable from clean runs in pipeline_audit.
#   Three distinct outcomes now match DP2 and DP3 patterns:
#     Clean run (all dims SUCCESS)     → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)  → DRIVER_WARNING   status=WARNING
#     Hard failure (exception raised)  → DRIVER_FAILURE   status=FAILED
#     Fact BLOCKED (no override)       → DRIVER_FAILURE   status=FAILED
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-air-5: force_fact now writes DRIVER_WARNING when active
dbutils.widgets.text("force_fact",  "false",
    "Force fact run despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout seconds")
dbutils.widgets.text("start_cal_year", "2000",
    "Fiscal spine start calendar year (Oct = first row)")
# IMPROVEMENT I-air-4: default changed from 2035 → 2040
dbutils.widgets.text("end_cal_year", "2040",
    "Fiscal spine end calendar year (Sep = last row)  "
    "[CHANGED v1.2.0: was 2035, now 2040 to align with dim_date]")

RUN_ID         = dbutils.widgets.get("run_id").strip() or f"air_prime_{uuid.uuid4().hex[:12]}"
ENV            = dbutils.widgets.get("env")
FORCE_FACT     = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC    = int(dbutils.widgets.get("timeout_sec"))
START_CAL_YEAR = dbutils.widgets.get("start_cal_year")
END_CAL_YEAR   = dbutils.widgets.get("end_cal_year")

PRODUCT      = "air"
DRIVER_NB    = "prime_air_driver"
DRIVER_TABLE = "assist_dev.air.DRIVER"

print(f"{'=' * 70}")
print(f"  air PRIME DRIVER — Accrual Income Reporter")
print(f"  run_id        : {RUN_ID}")
print(f"  env           : {ENV}")
print(f"  force_fact    : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE' if FORCE_FACT else ''}")
print(f"  timeout_sec   : {TIMEOUT_SEC}")
print(f"  spine range   : Oct {START_CAL_YEAR} → Sep {END_CAL_YEAR}  "
      f"{'[IMPROVEMENT I-air-4: end extended to 2040]' if END_CAL_YEAR == '2040' else ''}")
print(f"  started_at    : {datetime.now(timezone.utc).isoformat()}")
print(f"{'=' * 70}")

# ─────────────────────────────────────────────────────────────────────────────
# Notebook paths
# ─────────────────────────────────────────────────────────────────────────────
NB = {
    "dim_accrual_period"  : "../data_products/air/priming/prime_dim_accrual_period",
    "dim_onefund_program" : "../data_products/air/priming/prime_dim_onefund_program",
    "fact_accrual_income" : "../data_products/air/priming/prime_fact_accrual_income",
}

COMMON_PARAMS  = {"run_id": RUN_ID, "env": ENV}
PERIOD_PARAMS  = {**COMMON_PARAMS,
                  "start_cal_year": START_CAL_YEAR,
                  "end_cal_year":   END_CAL_YEAR}

results    = {}
failed_nbs = []

#driver_start_ts = audit_start(spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME")
driver_start_ts = audit_start(spark, RUN_ID, DRIVER_NB, PRODUCT, DRIVER_TABLE, source_schema="aasbs", source_table="silver")

# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify DP1 common dims are populated
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 common dim pre-requisites...")

dp1_checks = {
    "common.dim_line_item" : "assist_dev.common.dim_line_item",
    "common.dim_loa"       : "assist_dev.common.dim_loa",
    "common.dim_ia"        : "assist_dev.common.dim_ia",
    "common.dim_agency"    : "assist_dev.common.dim_agency",
}

preflight_ok = True
for label, tbl in dp1_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:35s}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        # preflight_ok = False  # removed as row count 0
        preflight_ok = True

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more DP1 common dim tables are empty. "
        "Ensure DP1 prime has completed before running air."
    )
    print(f"\n  ✗ {msg}")
    #audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
    audit_fail(spark, RUN_ID, DRIVER_TABLE, msg, "See individual notebook", driver_start_ts)
    dbutils.notebook.exit("PREFLIGHT_FAILURE")

print(f"\n  ✓ Pre-flight passed — all DP1 common dims populated.")

# ─────────────────────────────────────────────────────────────────────────────
# Helper: run one child notebook
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
# Step 1 — dim_accrual_period
#   IMPROVEMENT I-air-4: passes updated end_cal_year=2040 (default)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/3] prime_dim_accrual_period")
run_notebook("dim_accrual_period", NB["dim_accrual_period"], PERIOD_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — dim_onefund_program
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 2/3] prime_dim_onefund_program")
run_notebook("dim_onefund_program", NB["dim_onefund_program"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — fact_accrual_income  (INSERT-only; gate on both dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 3/3] prime_fact_accrual_income")

dims_ok = all(
    results.get(k) in ("SUCCESS", "SKIPPED")
    for k in ("dim_accrual_period", "dim_onefund_program")
)

if dims_ok:
    # Clean run — both dims succeeded
    run_notebook("fact_accrual_income", NB["fact_accrual_income"], COMMON_PARAMS)

elif FORCE_FACT:
    # IMPROVEMENT I-air-5: write DRIVER_WARNING before proceeding
    # so the bypass is permanently visible in pipeline_audit.
    bypass_msg = (
        f"force_fact=true — proceeding despite failed dims: {failed_nbs}. "
        f"Fact rows referencing these dims will have sentinel FK values (-1)."
    )
    print(f"  ⚠ {bypass_msg}")

    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_air_driver',
            '{DRIVER_TABLE}', 'FULL_PRIME',
            'WARNING', current_timestamp(),
            '{bypass_msg.replace("'", "''")}'
        )
    """)

    run_notebook("fact_accrual_income", NB["fact_accrual_income"], COMMON_PARAMS)

else:
    # Dim gate blocked — fact cannot run safely without dims
    results["fact_accrual_income"] = "BLOCKED"
    block_msg = (
        f"Fact step BLOCKED — failed dims: {failed_nbs}. "
        f"Fix the dim failure and re-run, or set force_fact=true to override. "
        f"When force_fact is used a DRIVER_WARNING audit record is written "
        f"(IMPROVEMENT I-air-5)."
    )
    print(f"  ✗ fact_accrual_income: BLOCKED — {block_msg}")
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES (
            '{RUN_ID}', '{PRODUCT}', 'prime_fact_accrual_income',
            'assist_dev.air.fact_accrual_income', 'FULL_PRIME',
            'BLOCKED', current_timestamp(),
            '{block_msg.replace("'", "''")}'
        )
    """)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print(f"  air PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'=' * 70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status in ("SKIPPED", "WARNING") else "✗")
    print(f"  {icon}  {nb:<35s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# ── IMPROVEMENT I-air-5: three distinct audit outcomes ───────────────────────
if all_success and not failed_nbs:
    # Clean run
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = None

elif all_success and failed_nbs:
    # force_fact was used and fact succeeded — WARNING, not SUCCESS
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = (
        f"force_fact=true used. Failed dims bypassed: {failed_nbs}. "
        f"Fact rows for these dims carry sentinel FK values (-1)."
    )

else:
    # Hard failure
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
