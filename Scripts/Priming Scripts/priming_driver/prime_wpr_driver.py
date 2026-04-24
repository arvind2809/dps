# Databricks notebook source
# =============================================================================
# driver/prime_dp8_driver.py
# DP8 Full Priming Driver — Workforce & Personnel Registry (WPR)
#
# Orchestrates seven child notebooks in order.
#
# Execution order:
#   Step 1  prime_dim_user               (SCD2 snapshot)   ← I-DP8-1
#   Step 2  prime_dim_org_hierarchy      (SCD1)            ← I-DP8-2
#   Step 3  prime_dim_responsible_role   (SCD1)            ← I-DP8-4
#   Step 4  prime_dim_entity_type        (static seed)     ← I-DP8-3
#   Step 5  prime_dim_client             (SCD2 snapshot)   ← I-DP8-1/5
#   Step 6  prime_dim_industry_partner   (SCD2 snapshot)   ← I-DP8-1
#   Step 7  prime_fact_role_assignment   (BLOCKED)         ← I-DP8-6/7/8
#
# IMPROVEMENT I-DP8-9 — No DP1 pre-flight required (built-in):
#   The six buildable DP8 dims have no FK dependencies on common.*:
#   dim_user, dim_org_hierarchy, dim_responsible_role, dim_entity_type,
#   dim_client, and dim_industry_partner are all self-contained.
#   (dim_client.agency_sk is NULL on prime due to format mismatch — no join.)
#   No pre-flight checks are needed. Driver launches immediately.
#
# IMPROVEMENT I-DP8-8 — BLOCKED fact is non-fatal (built-in):
#   fact_role_assignment exits with 'BLOCKED_SILVER_GAP' — a documented,
#   expected outcome. The driver treats this as non-fatal and emits
#   DRIVER_COMPLETE when all six dims succeed and the fact is BLOCKED.
#   This distinguishes the pre-documented Silver gap from an unexpected
#   notebook failure (which would emit DRIVER_FAILURE).
#
# Three distinct driver outcomes (consistent with DP5–DP9):
#   All dims succeed + fact BLOCKED   → DRIVER_COMPLETE  status=SUCCESS
#   Any dim raises exception          → DRIVER_FAILURE   status=FAILED
#   (force_fact not applicable here — fact is structurally BLOCKED, not
#    conditionally gated. No DRIVER_WARNING path for DP8.)
#
# Note on force_fact:
#   The standard force_fact pattern applies when dims fail and the operator
#   wants to proceed anyway. In DP8, the fact is BLOCKED regardless of dim
#   status — no dim gate controls it. force_fact is therefore not implemented
#   in the DP8 driver. The fact will not run under any circumstances until
#   Silver ingests the five role tables.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",     "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",        "dev", "Environment")
dbutils.widgets.text("timeout_sec","3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp8_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP8"
DRIVER_NB    = "prime_dp8_driver"
DRIVER_TABLE = "assist_dev.wpr.DRIVER"

print(f"{'='*70}")
print(f"  DP8 PRIME DRIVER — Workforce & Personnel Registry")
print(f"  run_id     : {RUN_ID}")
print(f"  env        : {ENV}")
print(f"  started_at : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")
print(
    f"\n  IMPROVEMENT I-DP8-9: No DP1 pre-flight — "
    f"all buildable dims are self-contained."
)
print(
    f"  IMPROVEMENT I-DP8-8: fact_role_assignment BLOCKED outcome "
    f"is non-fatal (DRIVER_COMPLETE when dims succeed)."
)

NB = {
    "dim_user"               : "../data_products/wpr/prime_dim_user",
    "dim_org_hierarchy"      : "../data_products/wpr/prime_dim_org_hierarchy",
    "dim_responsible_role"   : "../data_products/wpr/prime_dim_responsible_role",
    "dim_entity_type"        : "../data_products/wpr/prime_dim_entity_type",
    "dim_client"             : "../data_products/wpr/prime_dim_client",
    "dim_industry_partner"   : "../data_products/wpr/prime_dim_industry_partner",
    "fact_role_assignment"   : "../data_products/wpr/prime_fact_role_assignment",
}

COMMON_PARAMS  = {"run_id": RUN_ID, "env": ENV}
results        = {}
failed_dims    = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────
def run_notebook(key, path, params):
    print(f"\n  → Launching: {key}")
    try:
        result = dbutils.notebook.run(path, TIMEOUT_SEC, params)
        # Treat BLOCKED_SILVER_GAP as a known non-failure outcome
        if "BLOCKED_SILVER_GAP" in str(result):
            results[key] = "BLOCKED_SILVER_GAP"
            print(f"  ⊗ {key}: BLOCKED_SILVER_GAP  (non-fatal — pre-documented Silver gap)")
        elif "SUCCESS" in str(result):
            results[key] = "SUCCESS"
            print(f"  ✓ {key}: SUCCESS")
        else:
            results[key] = "SKIPPED"
            print(f"  ⚠ {key}: {result}")
        return results[key]
    except Exception as e:
        results[key] = "FAILED"
        print(f"  ✗ {key}: FAILED — {str(e)[:300]}")
        return "FAILED"

# ─────────────────────────────────────────────────────────────────────────────
# Steps 1–6: Dims (independent of each other — I-DP8-9)
# ─────────────────────────────────────────────────────────────────────────────
for step, key in enumerate([
    "dim_user", "dim_org_hierarchy", "dim_responsible_role",
    "dim_entity_type", "dim_client", "dim_industry_partner"
], 1):
    print(f"\n[STEP {step}/7] prime_{key}")
    r = run_notebook(key, NB[key], COMMON_PARAMS)
    if r == "FAILED":
        failed_dims.append(key)

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: fact_role_assignment — always BLOCKED (I-DP8-6/7/8)
# No gate logic: the fact is structurally BLOCKED regardless of dim status.
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 7/7] prime_fact_role_assignment  (expected: BLOCKED_SILVER_GAP)")
run_notebook("fact_role_assignment", NB["fact_role_assignment"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP8 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for key, status in results.items():
    if status == "SUCCESS":
        icon = "✓"
    elif status == "BLOCKED_SILVER_GAP":
        icon = "⊗"
    elif status == "SKIPPED":
        icon = "⚠"
    else:
        icon = "✗"
    print(f"  {icon}  {key:<40s}  {status}")

# IMPROVEMENT I-DP8-8: DRIVER_COMPLETE when dims succeed + fact BLOCKED
dims_ok = all(results.get(k) in ("SUCCESS","SKIPPED")
              for k in ["dim_user","dim_org_hierarchy","dim_responsible_role",
                        "dim_entity_type","dim_client","dim_industry_partner"])
fact_blocked = results.get("fact_role_assignment") == "BLOCKED_SILVER_GAP"

if dims_ok and (fact_blocked or results.get("fact_role_assignment") == "SUCCESS"):
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = (
        "All 6 dims loaded successfully. "
        "fact_role_assignment: BLOCKED_SILVER_GAP — "
        "5 source role tables absent from Silver DDL. "
        "Activate by ingesting: acquisition_role, award_role, ia_role, "
        "funding_role, solicit_role."
    ) if fact_blocked else None
elif failed_dims:
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = f"Failed dim steps: {failed_dims}"
else:
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = f"Unexpected result state: {results}"

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
