# Databricks notebook source
# =============================================================================
# driver/prime_dp5_driver.py
# DP5 Full Priming Driver — Treasury Transmission & Payment Pipeline (TPP)
#
# Orchestrates six child notebooks in strict dependency order.
# Entry point for the Databricks DP5 prime job.
#
# Execution order:
#   Step 1  prime_dim_transmittal_type     (SCD1 — independent)
#   Step 2  prime_dim_aac_envelope         (SCD1 — requires Step 1)
#   Step 3  prime_dim_agreement            (SCD1 — independent)
#   Step 4  prime_dim_receiving_report     (SCD1 — independent)
#   Step 5  prime_fact_transmission_event  (INSERT-only — requires Steps 1-4 + DP1)
#   Step 6  prime_fact_payment_reconciliation (INSERT-only — requires Steps 1-4 + DP1)
#
# Pre-requisites (DP1 common dims must be populated):
#   common.dim_date, common.dim_agency, common.dim_ia, common.dim_line_item
#
# Gate logic:
#   Steps 1–4 (dims) must ALL succeed before Steps 5–6 (facts) run.
#   force_fact=true bypasses the dim gate for recovery scenarios.
#
# IMPROVEMENT I-DP5-2 (built-in from first generation):
#   Three distinct driver audit outcomes — matching DP2, DP3, DP4 standards:
#     Clean run (all dims SUCCESS)     → DRIVER_COMPLETE  status=SUCCESS
#     force_fact bypass (dim failure)  → DRIVER_WARNING   status=WARNING
#     Hard failure (exception raised)  → DRIVER_FAILURE   status=FAILED
#   When force_fact is active, a DRIVER_WARNING audit row is written to
#   pipeline_audit BEFORE the fact notebooks run, listing the bypassed dims.
#   This ensures override runs are always distinguishable from clean runs.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------

import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
# IMPROVEMENT I-DP5-2: force_fact writes DRIVER_WARNING — built-in from generation
dbutils.widgets.text("force_fact",  "false",
    "Force fact runs despite dim failures (true/false). "
    "Writes DRIVER_WARNING audit record when active.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp5_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP5"
DRIVER_NB    = "prime_dp5_driver"
DRIVER_TABLE = "assist_dev.tpp.DRIVER"

print(f"{'=' * 70}")
print(f"  DP5 PRIME DRIVER — Treasury Transmission & Payment Pipeline")
print(f"  run_id      : {RUN_ID}")
print(f"  env         : {ENV}")
print(f"  force_fact  : {FORCE_FACT}  "
      f"{'⚠ OVERRIDE ACTIVE — writes DRIVER_WARNING on use' if FORCE_FACT else ''}")
print(f"  timeout_sec : {TIMEOUT_SEC}")
print(f"  started_at  : {datetime.now(timezone.utc).isoformat()}")
print(f"{'=' * 70}")

# ─────────────────────────────────────────────────────────────────────────────
# Notebook paths
# ─────────────────────────────────────────────────────────────────────────────
NB = {
    "dim_transmittal_type"        : "../data_products/tpp/priming/prime_dim_transmittal_type",
    "dim_aac_envelope"            : "../data_products/tpp/priming/prime_dim_aac_envelope",
    "dim_agreement"               : "../data_products/tpp/priming/prime_dim_agreement",
    "dim_receiving_report"        : "../data_products/tpp/priming/prime_dim_receiving_report",
    "fact_transmission_event"     : "../data_products/tpp/priming/prime_fact_transmission_event",
    "fact_payment_reconciliation" : "../data_products/tpp/priming/prime_fact_payment_reconciliation",
}

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}

results    = {}
failed_nbs = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight: verify DP1 common dims are populated
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 common dim pre-requisites...")

dp1_checks = {
    "common.dim_date"      : "assist_dev.common.dim_date",
    "common.dim_agency"    : "assist_dev.common.dim_agency",
    "common.dim_ia"        : "assist_dev.common.dim_ia",
    "common.dim_line_item" : "assist_dev.common.dim_line_item",
}

preflight_ok = True
for label, tbl in dp1_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  {label:35s}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more DP1 common dim tables are empty. "
        "Ensure DP1 prime has completed before running DP5."
    )
    print(f"\n  ✗ {msg}")
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
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
# Step 1 — dim_transmittal_type  (SCD1, independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 1/6] prime_dim_transmittal_type")
run_notebook("dim_transmittal_type", NB["dim_transmittal_type"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — dim_aac_envelope  (SCD1, requires Step 1 — pre-flight guard in NB)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 2/6] prime_dim_aac_envelope")
run_notebook("dim_aac_envelope", NB["dim_aac_envelope"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — dim_agreement  (SCD1, independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 3/6] prime_dim_agreement")
run_notebook("dim_agreement", NB["dim_agreement"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — dim_receiving_report  (SCD1, independent)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 4/6] prime_dim_receiving_report")
run_notebook("dim_receiving_report", NB["dim_receiving_report"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Steps 5 & 6 — Facts  (INSERT-only; gate on all 4 dims)
# ─────────────────────────────────────────────────────────────────────────────
DIM_KEYS = ("dim_transmittal_type", "dim_aac_envelope",
            "dim_agreement",        "dim_receiving_report")
dims_ok = all(results.get(k) in ("SUCCESS", "SKIPPED") for k in DIM_KEYS)

def run_fact_step(step_num: int, key: str):
    """Run a fact notebook with the dim gate applied."""
    print(f"\n[STEP {step_num}/6] prime_{key}")

    if dims_ok:
        run_notebook(key, NB[key], COMMON_PARAMS)

    elif FORCE_FACT:
        # IMPROVEMENT I-DP5-2: write DRIVER_WARNING before proceeding
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
                '{RUN_ID}', '{PRODUCT}', 'prime_dp5_driver',
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
            f"Fix dim failure then re-run, or set force_fact=true to override. "
            f"When force_fact is used a DRIVER_WARNING audit record is written."
        )
        print(f"  ✗ {key}: BLOCKED — {block_msg}")
        spark.sql(f"""
            INSERT INTO assist_dev.common.pipeline_audit
                (run_id, product, notebook, target_table, run_type,
                 status, started_at, error_message)
            VALUES (
                '{RUN_ID}', '{PRODUCT}', 'prime_{key}',
                'assist_dev.tpp.{key.replace("_", "")}', 'FULL_PRIME',
                'BLOCKED', current_timestamp(),
                '{block_msg.replace("'", "''")}'
            )
        """)

run_fact_step(5, "fact_transmission_event")
run_fact_step(6, "fact_payment_reconciliation")

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print(f"  DP5 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'=' * 70}")
for nb, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status in ("SKIPPED", "WARNING") else "✗")
    print(f"  {icon}  {nb:<38s} {status}")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# IMPROVEMENT I-DP5-2: three distinct audit outcomes
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
print(f"{'=' * 70}")

if audit_run_status == "SUCCESS":
    audit_success(spark, RUN_ID, DRIVER_TABLE, 0, 0, driver_start_ts)

else:
    audit_fail(spark, RUN_ID, DRIVER_TABLE, str(driver_msg) if driver_msg else "See individual notebook audit rows", "look error in notebooks", driver_start_ts)

dbutils.notebook.exit(driver_status)
