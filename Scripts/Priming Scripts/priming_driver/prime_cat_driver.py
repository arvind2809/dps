# Databricks notebook source
# =============================================================================
# driver/prime_dp10_driver.py
# DP10 Full Priming Driver — Collaboration, Review & Audit Trail (CAT)
#
# Execution order:
#   Step 1  prime_dim_event_type           (SCD1)   ← I-DP10-1
#   Step 2  prime_dim_review_type          (SCD1)   ← I-DP10-1
#   Step 3  prime_dim_reviewer_type        (SCD1)   ← I-DP10-1
#   Step 4  prime_dim_finding              (SCD1)   ← I-DP10-1
#   Step 5  prime_fact_lifecycle_event     (INSERT) ← I-DP10-2/3/4/6
#   Step 6  prime_fact_review_determination(BLOCKED)← I-DP10-6/7
#
# IMPROVEMENT I-DP10-5 — Dual pre-flight (DP1 + DP8) (built-in):
#   fact_lifecycle_event requires:
#     DP1: common.dim_date, common.dim_award, common.dim_ia, common.dim_agency
#     DP8: wpr.dim_entity_type   ← FIRST cross-product FK in the DP series
#   All five checked before any notebook runs.
#   PREFLIGHT_FAILURE is a distinct exit status — distinguishes dependency
#   absence (pre-flight) from notebook error (DRIVER_FAILURE).
#
# IMPROVEMENT I-DP10-6 — BLOCKED fact non-fatal / DRIVER_COMPLETE (built-in):
#   fact_review_determination always exits BLOCKED_SILVER_GAP.
#   DRIVER_COMPLETE when all four dims succeed + lifecycle fact succeeds +
#   review determination is BLOCKED. No force_fact path for blocked facts —
#   the block is structural, not operator-overridable.
#
# Three driver outcomes:
#   Dims succeed + lifecycle fact succeeds + review BLOCKED → DRIVER_COMPLETE
#   Any dim or lifecycle fact raises exception              → DRIVER_FAILURE
#   Pre-flight table missing                               → PREFLIGHT_FAILURE
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",      "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",         "dev", "Environment")
dbutils.widgets.text("force_fact",  "false",
    "Bypass dim gate for fact_lifecycle_event (true/false). "
    "Writes DRIVER_WARNING when active. Does NOT apply to BLOCKED fact.")
dbutils.widgets.text("timeout_sec", "3600", "Per-notebook timeout in seconds")

RUN_ID      = dbutils.widgets.get("run_id").strip() or f"dp10_prime_{uuid.uuid4().hex[:12]}"
ENV         = dbutils.widgets.get("env")
FORCE_FACT  = dbutils.widgets.get("force_fact").strip().lower() == "true"
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP10"
DRIVER_NB    = "prime_dp10_driver"
DRIVER_TABLE = "assist_dev.cat.DRIVER"

print(f"{'='*70}")
print(f"  DP10 PRIME DRIVER — Collaboration, Review & Audit Trail")
print(f"  run_id     : {RUN_ID}")
print(f"  env        : {ENV}")
print(f"  force_fact : {FORCE_FACT}  "
      f"{'⚠ DRIVER_WARNING on use' if FORCE_FACT else ''}")
print(f"  started_at : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

NB = {
    "dim_event_type"           : "../data_products/cat/prime_dim_event_type",
    "dim_review_type"          : "../data_products/cat/prime_dim_review_type",
    "dim_reviewer_type"        : "../data_products/cat/prime_dim_reviewer_type",
    "dim_finding"              : "../data_products/cat/prime_dim_finding",
    "fact_lifecycle_event"     : "../data_products/cat/prime_fact_lifecycle_event",
    "fact_review_determination": "../data_products/cat/prime_fact_review_determination",
}
COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}
results       = {}
failed_dims   = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# IMPROVEMENT I-DP10-5: Dual pre-flight — DP1 + DP8
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[PRE-FLIGHT] Checking DP1 + DP8 prerequisites...")
preflight_checks = {
    "common.dim_date"       : "assist_dev.common.dim_date",
    "common.dim_award"      : "assist_dev.common.dim_award",
    "common.dim_ia"         : "assist_dev.common.dim_ia",
    "common.dim_agency"     : "assist_dev.common.dim_agency",
    "wpr.dim_entity_type"   : "assist_dev.wpr.dim_entity_type",   # DP8!
}
preflight_ok = True
for label, tbl in preflight_checks.items():
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
    status = "OK" if cnt > 0 else "EMPTY"
    src = "(DP8)" if "wpr" in label else "(DP1)"
    print(f"  {label:<28}  {src}  rows={cnt:>10,}  [{status}]")
    if cnt == 0:
        preflight_ok = False

if not preflight_ok:
    msg = (
        "Pre-flight FAILED — one or more prerequisite tables are empty. "
        "DP1 must be primed before DP10. DP8 must be primed before DP10 "
        "(fact_lifecycle_event.entity_type_sk requires wpr.dim_entity_type)."
    )
    print(f"\n  ✗ {msg}")
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE, driver_start_ts, msg)
    dbutils.notebook.exit("PREFLIGHT_FAILURE")

print(f"\n  ✓ Pre-flight passed (DP1 + DP8 confirmed).")

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────
def run_nb(key, path, params):
    print(f"\n  → Launching: {key}")
    try:
        result = dbutils.notebook.run(path, TIMEOUT_SEC, params)
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
# Steps 1–4: dims
# ─────────────────────────────────────────────────────────────────────────────
DIM_STEPS = [
    ("dim_event_type",  1), ("dim_review_type",  2),
    ("dim_reviewer_type", 3), ("dim_finding",   4),
]
for key, step in DIM_STEPS:
    print(f"\n[STEP {step}/6] prime_{key}")
    r = run_nb(key, NB[key], COMMON_PARAMS)
    if r == "FAILED":
        failed_dims.append(key)

dims_ok = all(results.get(k) in ("SUCCESS","SKIPPED") for k,_ in DIM_STEPS)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: fact_lifecycle_event (gated on dims)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 5/6] prime_fact_lifecycle_event")
if dims_ok:
    run_nb("fact_lifecycle_event", NB["fact_lifecycle_event"], COMMON_PARAMS)
elif FORCE_FACT:
    bypass_msg = (
        f"force_fact=true — proceeding despite failed dims: {failed_dims}. "
        f"Fact rows for failed dim FKs will carry sentinel values (-1)."
    )
    print(f"  ⚠ {bypass_msg}")
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, error_message)
        VALUES ('{RUN_ID}','{PRODUCT}','{DRIVER_NB}','{DRIVER_TABLE}',
                'FULL_PRIME','WARNING',current_timestamp(),
                '{bypass_msg.replace("'","''")}')
    """)
    run_nb("fact_lifecycle_event", NB["fact_lifecycle_event"], COMMON_PARAMS)
else:
    results["fact_lifecycle_event"] = "BLOCKED_DIM_FAILURE"
    print(
        f"  ✗ fact_lifecycle_event BLOCKED — failed dims: {failed_dims}. "
        f"Fix dim failure and re-run, or set force_fact=true."
    )

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: fact_review_determination (always BLOCKED — I-DP10-6)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[STEP 6/6] prime_fact_review_determination  (expected: BLOCKED_SILVER_GAP)")
run_nb("fact_review_determination", NB["fact_review_determination"], COMMON_PARAMS)

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP10 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for key, status in results.items():
    icon = "✓" if status=="SUCCESS" else ("⊗" if "BLOCKED" in status else ("⚠" if status=="SKIPPED" else "✗"))
    print(f"  {icon}  {key:<45s}  {status}")

lifecycle_ok  = results.get("fact_lifecycle_event") in ("SUCCESS","SKIPPED")
review_blocked= results.get("fact_review_determination") == "BLOCKED_SILVER_GAP"
all_dims_ok   = not failed_dims

# IMPROVEMENT I-DP10-6: DRIVER_COMPLETE when dims + lifecycle OK + review BLOCKED
if all_dims_ok and lifecycle_ok and review_blocked:
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = (
        "fact_review_determination: BLOCKED_SILVER_GAP — "
        "central_collab_request absent from Silver DDL. "
        "Activate by ingesting aasbs.central_collab_request."
    )
elif FORCE_FACT and lifecycle_ok and not all_dims_ok:
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = f"force_fact=true used. Failed dims: {failed_dims}."
else:
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = (
        f"Failed: dims={failed_dims}, "
        f"lifecycle={results.get('fact_lifecycle_event','—')}"
    )

print(f"\n  Driver status : {driver_status}")
print(f"  Note          : {driver_msg}")
print(f"  Finished at   : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")

if audit_run_status == "SUCCESS":
    audit_success(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE,
                  driver_start_ts, rows_read=0, rows_written=0)
else:
    audit_failure(spark, RUN_ID, DRIVER_NB, DRIVER_TABLE,
                  driver_start_ts, error_msg=str(driver_msg))

dbutils.notebook.exit(driver_status)
