# Databricks notebook source
# =============================================================================
# driver/cdc_dp1_driver.py
# DP1 CDC Delta Refresh Driver — Obligation & Expenditure Tracker
#
# Orchestrates all DP1 CDC child notebooks with PARTIAL TOLERANCE error handling:
#   - All dimension notebooks run in dependency order
#   - Fact notebook runs if BOTH critical dims (dim_loa + dim_line_item) succeeded
#   - Non-critical dim failures are logged as warnings and do not halt the job
#   - Overall job status = SUCCESS only if fact notebook succeeds
#
# Execution order:
#   [1] cdc_dim_agency        (non-critical: agency enrichment, not a fact FK gatekeeper)
#   [2] cdc_dim_ia            (non-critical: ia_sk uses sentinel if unresolved)
#   [3] cdc_dim_loa           (CRITICAL — fact will not run if this fails)
#   [4] cdc_dim_award         (non-critical: award_sk uses sentinel)
#   [5] cdc_dim_funding       (non-critical: funding_sk uses sentinel)
#   [6] cdc_dim_line_item     (CRITICAL — fact will not run if this fails)
#   [7] cdc_fact_obligation_expenditure (runs if steps 3 AND 6 succeeded)
#
# NOTE: dim_date has no CDC step. The date spine (2000–2035) is static.
#       Re-prime dim_date only if range extension is needed.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")
dbutils.widgets.text("timeout_seconds", "1800", "Per-notebook timeout (seconds)")
dbutils.widgets.text(
    "force_fact",  "false",
    "Set 'true' to run fact even if critical dims failed (use with caution)"
)

RUN_ID      = dbutils.widgets.get("run_id")   or f"dp1-cdc-{get_spark_app_id()}"
JOB         = dbutils.widgets.get("job_name")
TIMEOUT     = int(dbutils.widgets.get("timeout_seconds"))
FORCE_FACT  = dbutils.widgets.get("force_fact").lower() == "true"

PARAMS = {"run_id": RUN_ID, "job_name": JOB}

print("=" * 65)
print(f"  DP1 CDC Delta Refresh")
print(f"  Run ID    : {RUN_ID}")
print(f"  Job       : {JOB}")
print(f"  Timeout   : {TIMEOUT}s per notebook")
print(f"  ForceFact : {FORCE_FACT}")
print("=" * 65)

# COMMAND ----------
COMMON = "../common"
OET    = "../oet"

# Task definition: (step, label, path, is_critical_for_fact)
# is_critical_for_fact=True → fact is blocked if this step fails (unless force_fact=true)
TASKS = [
    (1, "cdc_dim_agency",                  f"{COMMON}/cdc_dim_agency",                False),
    (2, "cdc_dim_ia",                      f"{COMMON}/cdc_dim_ia",                    False),
    (3, "cdc_dim_loa",                     f"{COMMON}/cdc_dim_loa",                   True),
    (4, "cdc_dim_award",                   f"{COMMON}/cdc_dim_award",                 False),
    (5, "cdc_dim_funding",                 f"{COMMON}/cdc_dim_funding",               False),
    (6, "cdc_dim_line_item",               f"{COMMON}/cdc_dim_line_item",             True),
    (7, "cdc_fact_obligation_expenditure", f"{OET}/cdc_fact_obligation_expenditure",  True),
]

# COMMAND ----------
import time

results        = {}   # label → status
critical_ok    = set()  # labels of critical tasks that succeeded
job_start      = time.time()
job_halted     = False
halt_reason    = None

for step_num, label, path, is_critical in TASKS:

    # ── Gate check: fact blocked until critical dims confirm success ──────────
    if step_num == 7:
        critical_dims      = {"cdc_dim_loa", "cdc_dim_line_item"}
        missing_criticals  = critical_dims - critical_ok

        if missing_criticals and not FORCE_FACT:
            msg = (f"Fact step BLOCKED — critical dim(s) did not succeed: "
                   f"{missing_criticals}. Set force_fact=true to override.")
            print(f"\n  ✗  {msg}")
            results[label] = "BLOCKED"
            # Write BLOCKED record to pipeline_audit
            try:
                spark.sql(f"""
                    INSERT INTO {AUDIT_TABLE} {_AUDIT_COLS}
                    VALUES (
                        '{RUN_ID}', '{JOB}', '{label}', '{DATA_PRODUCT}', 'gold',
                        '{gold("oet","fact_obligation_expenditure")}', '', '', '{RUN_TYPE_CDC}',
                        'FAILED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0,
                        0, 0, 0, 0, 0,
                        'updated_dt', NULL, NULL,
                        'BLOCKED: {msg}', NULL,
                        '{get_cluster_id()}', '{get_spark_app_id()}',
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
                    )
                """)
            except Exception:
                pass
            job_halted  = True
            halt_reason = msg
            break
        elif missing_criticals and FORCE_FACT:
            print(f"\n  ⚠  force_fact=true — running fact despite failed critical dims: {missing_criticals}")

    print(f"\n{'─'*65}")
    print(f"  Step {step_num}/{len(TASKS)} : {label}")
    print(f"  Critical for fact : {is_critical}")
    print(f"{'─'*65}")

    t0  = time.time()
    err = None

    try:
        result  = dbutils.notebook.run(path, timeout_seconds=TIMEOUT, arguments=PARAMS)
        elapsed = round(time.time() - t0, 1)

        if result == "SUCCESS":
            status = "SUCCESS"
            if is_critical:
                critical_ok.add(label)
            print(f"  ✓  {label} — {elapsed}s")
        else:
            status = "WARN"
            print(f"  ⚠  {label} returned '{result}' — {elapsed}s")

    except Exception as e:
        elapsed = round(time.time() - t0, 1)
        status  = "FAILED"
        err     = str(e)
        print(f"  ✗  {label} FAILED — {elapsed}s")
        print(f"     {err[:200]}")

    results[label] = status

# COMMAND ----------
# ── Final summary ─────────────────────────────────────────────────────────────
total_elapsed = round(time.time() - job_start, 1)

print(f"\n{'='*65}")
print(f"  DP1 CDC RUN SUMMARY")
print(f"  Run ID  : {RUN_ID}")
print(f"  Elapsed : {total_elapsed}s  ({total_elapsed/60:.1f} min)")
print(f"{'='*65}")

for step_num, label, path, is_critical in TASKS:
    status  = results.get(label, "SKIPPED")
    icon    = {"SUCCESS":"✓","WARN":"⚠","FAILED":"✗","BLOCKED":"⊘","SKIPPED":"–"}.get(status,"?")
    crit_m  = "CRIT" if is_critical else "    "
    print(f"  {icon}  [{crit_m}]  {label:<44}  {status}")

print(f"{'='*65}")

# ── Determine overall job outcome ─────────────────────────────────────────────
fact_status  = results.get("cdc_fact_obligation_expenditure", "SKIPPED")
any_failed   = any(v == "FAILED" for v in results.values())
overall      = "SUCCESS" if fact_status == "SUCCESS" else "PARTIAL" if not job_halted else "FAILED"

# Warn on partial success
if overall == "PARTIAL":
    print(f"\n  ⚠  PARTIAL SUCCESS — fact appended but some dims had issues")
    print(f"     Check pipeline_audit for FAILED/WARN steps above")
elif overall == "FAILED":
    print(f"\n  ✗  FAILED — fact was blocked: {halt_reason}")
else:
    print(f"\n  ✓  CDC run complete — fact snapshot appended successfully")

# ── Driver-level audit row ────────────────────────────────────────────────────
driver_status = "SUCCESS" if overall == "SUCCESS" else "FAILED"
try:
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} {_AUDIT_COLS}
        VALUES (
            '{RUN_ID}', '{JOB}', 'DRIVER_COMPLETE', '{DATA_PRODUCT}', 'gold',
            'dp1_cdc_driver', 'aasbs,billing', 'all_dp1_sources', '{RUN_TYPE_CDC}',
            '{driver_status}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            {int(total_elapsed)},
            0, {sum(1 for v in results.values() if v=='SUCCESS')},
            {sum(1 for v in results.values() if v=='SUCCESS')}, 0, 0,
            'updated_dt', NULL, NULL,
            {'NULL' if overall!='FAILED' else f"'BLOCKED: {str(halt_reason)[:200]}'"}, NULL,
            '{get_cluster_id()}', '{get_spark_app_id()}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        )
    """)
    print(f"  pipeline_audit driver record written ({driver_status}) ✓")
except Exception as ae:
    print(f"  [WARN] Could not write driver audit record: {ae}")

# ── Exit ──────────────────────────────────────────────────────────────────────
if overall == "FAILED":
    raise RuntimeError(f"DP1 CDC job failed: {halt_reason}")

dbutils.notebook.exit(overall)
