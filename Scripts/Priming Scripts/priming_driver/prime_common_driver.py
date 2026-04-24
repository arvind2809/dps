# Databricks notebook source
# =============================================================================
# driver/prime_dp11_driver.py
# DP11 Full Priming Driver — Conformed Reference Data Catalog (ref schema)
#
# Orchestrates three child notebooks in sequence.
#
# Execution order:
#   Step 1  prime_ref_acquisition_contract   (33 tables — I-DP11-1/2/3)
#   Step 2  prime_ref_financial_operational  (32 tables — I-DP11-1/2)
#   Step 3  prime_ref_collab_transmittal     (20 tables — I-DP11-1/2/5)
#
# No pre-flight checks required:
#   DP11 has no FK dependencies on any other Gold data product.
#   All 85 tables are self-contained TRUNCATE→INSERT reference loads.
#
# Gate logic — force_continue (IMPROVEMENT I-DP11-7):
#   Unlike DP5–DP10, all DP11 notebooks are TRUNCATE→INSERT dims —
#   there are no facts and no hard inter-notebook dependency.
#   force_fact does NOT apply here.  Instead, force_continue=true allows
#   the driver to advance past a failed group notebook and continue loading
#   the remaining domain groups.  This is appropriate because a failure in
#   the acquisition/contract group does not block loading financial codes.
#
# IMPROVEMENT I-DP11-7 (built-in from first generation):
#   Three distinct driver audit outcomes:
#     All groups succeeded        → DRIVER_COMPLETE  status=SUCCESS
#     force_continue used         → DRIVER_WARNING   status=WARNING
#     Unrecovered exception       → DRIVER_FAILURE   status=FAILED
#   A DRIVER_WARNING inline audit row is written to pipeline_audit when
#   force_continue is active, listing the group that was bypassed.
#   This ensures partial-load runs are permanently distinguishable.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("run_id",          "", "Pipeline Run ID (auto-generated if blank)")
dbutils.widgets.text("env",             "dev", "Environment")
# IMPROVEMENT I-DP11-7: force_continue replaces force_fact for all-dim product
dbutils.widgets.text("force_continue",  "false",
    "Continue past a failed group (true/false). "
    "Writes DRIVER_WARNING audit record when active. "
    "Use when one domain group fails and you want remaining groups to load.")
dbutils.widgets.text("timeout_sec",     "7200", "Per-notebook timeout in seconds")

RUN_ID          = dbutils.widgets.get("run_id").strip() or f"dp11_prime_{uuid.uuid4().hex[:12]}"
ENV             = dbutils.widgets.get("env")
FORCE_CONTINUE  = dbutils.widgets.get("force_continue").strip().lower() == "true"
TIMEOUT_SEC     = int(dbutils.widgets.get("timeout_sec"))

PRODUCT      = "DP11"
DRIVER_NB    = "prime_dp11_driver"
DRIVER_TABLE = "assist_dev.ref.DRIVER"

print(f"{'='*70}")
print(f"  DP11 PRIME DRIVER — Conformed Reference Data Catalog")
print(f"  run_id         : {RUN_ID}")
print(f"  env            : {ENV}")
print(f"  force_continue : {FORCE_CONTINUE}  "
      f"{'⚠ OVERRIDE ACTIVE — writes DRIVER_WARNING on use' if FORCE_CONTINUE else ''}")
print(f"  timeout_sec    : {TIMEOUT_SEC}")
print(f"  started_at     : {datetime.now(timezone.utc).isoformat()}")
print(f"{'='*70}")
print(f"\n  No pre-flight checks needed — DP11 has no FK dependencies on other products.")

# ─────────────────────────────────────────────────────────────────────────────
# Notebook paths
# ─────────────────────────────────────────────────────────────────────────────
STEPS = [
    # (step_num, label,                          notebook_path,                  table_count)
    (1, "prime_ref_acquisition_contract",  "../ref/prime_ref_acquisition_contract",  33),
    (2, "prime_ref_financial_operational", "../ref/prime_ref_financial_operational",  32),
    (3, "prime_ref_collab_transmittal",    "../ref/prime_ref_collab_transmittal",     20),
]

COMMON_PARAMS = {"run_id": RUN_ID, "env": ENV}
results       = {}
failed_steps  = []

driver_start_ts = audit_start(
    spark, RUN_ID, PRODUCT, DRIVER_NB, DRIVER_TABLE, run_type="FULL_PRIME"
)

# ─────────────────────────────────────────────────────────────────────────────
# Execute steps
# ─────────────────────────────────────────────────────────────────────────────
for step_num, label, nb_path, tbl_count in STEPS:
    print(f"\n[STEP {step_num}/3] {label}  ({tbl_count} tables)")

    try:
        result = dbutils.notebook.run(nb_path, TIMEOUT_SEC, COMMON_PARAMS)
        status = "SUCCESS" if "SUCCESS" in str(result) else "SKIPPED"
        results[label] = status
        print(f"  ✓ {label}: {status}")

    except Exception as step_err:
        results[label] = "FAILED"
        failed_steps.append(label)
        print(f"  ✗ {label}: FAILED — {str(step_err)[:300]}")

        if FORCE_CONTINUE:
            # IMPROVEMENT I-DP11-7: write DRIVER_WARNING before continuing
            bypass_msg = (
                f"force_continue=true — step '{label}' failed but driver is "
                f"continuing to next group. Failed tables in this group may be "
                f"missing from ref schema. Review pipeline_audit for details."
            )
            print(f"\n  ⚠ {bypass_msg}")
            spark.sql(f"""
                INSERT INTO assist_dev.common.pipeline_audit
                    (run_id, product, notebook, target_table, run_type,
                     status, started_at, error_message)
                VALUES (
                    '{RUN_ID}', '{PRODUCT}', '{DRIVER_NB}',
                    '{DRIVER_TABLE}', 'FULL_PRIME',
                    'WARNING', current_timestamp(),
                    '{bypass_msg.replace("'", "''")}'
                )
            """)
        else:
            print(
                f"\n  Aborting — remaining groups not attempted. "
                f"Set force_continue=true to skip this group and continue."
            )
            break

# ─────────────────────────────────────────────────────────────────────────────
# Driver summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  DP11 PRIME DRIVER — EXECUTION SUMMARY")
print(f"{'='*70}")
for label, status in results.items():
    icon = "✓" if status == "SUCCESS" else ("⚠" if status == "SKIPPED" else "✗")
    print(f"  {icon}  {label:<42s}  {status}")

# Steps not reached (aborted without force_continue)
not_reached = [s[1] for s in STEPS if s[1] not in results]
if not_reached:
    for label in not_reached:
        print(f"  –  {label:<42s}  NOT_RUN")

all_success = all(v in ("SUCCESS", "SKIPPED") for v in results.values())

# IMPROVEMENT I-DP11-7: three distinct audit outcomes
if all_success and not failed_steps:
    driver_status    = "DRIVER_COMPLETE"
    audit_run_status = "SUCCESS"
    driver_msg       = None
elif FORCE_CONTINUE and failed_steps:
    driver_status    = "DRIVER_WARNING"
    audit_run_status = "WARNING"
    driver_msg       = (
        f"force_continue=true used. Failed steps: {failed_steps}. "
        f"Tables in failed groups may be missing from ref schema. "
        f"Review individual notebook audit rows for details."
    )
else:
    driver_status    = "DRIVER_FAILURE"
    audit_run_status = "FAILED"
    driver_msg       = (
        f"Failed steps: {failed_steps}"
        + (f"; not reached: {not_reached}" if not_reached else "")
    )

# Post-load cross-group totals
print(f"\n  Cross-group ref table totals:")
try:
    total_ref_rows = spark.sql("""
        SELECT
            SUM(cnt) AS total_rows,
            SUM(active_cnt) AS total_active,
            SUM(tbl_count) AS total_tables
        FROM (
            SELECT 'acquisition_contract' AS grp,
                COUNT(*) AS cnt,
                SUM(CASE WHEN is_active_flag = TRUE THEN 1 ELSE 0 END) AS active_cnt,
                COUNT(DISTINCT source_table) AS tbl_count
            FROM assist_dev.ref.lu_acquisition_status
            UNION ALL
            SELECT 'financial_operational',
                COUNT(*),
                SUM(CASE WHEN is_active_flag = TRUE THEN 1 ELSE 0 END),
                COUNT(DISTINCT source_table)
            FROM assist_dev.ref.lu_fund
            UNION ALL
            SELECT 'collab_transmittal',
                COUNT(*),
                SUM(CASE WHEN is_active_flag = TRUE THEN 1 ELSE 0 END),
                COUNT(DISTINCT source_table)
            FROM assist_dev.ref.lu_collab_type
        )
    """).collect()[0]
    print(f"  Sample check — total ref rows: {total_ref_rows[0]:,} | "
          f"active: {total_ref_rows[1]:,} | distinct source tables: {total_ref_rows[2]}")
except Exception:
    print(f"  (Cross-group total check skipped — one or more groups may not have loaded)")

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
