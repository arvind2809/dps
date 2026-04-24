# Databricks notebook source
# =============================================================================
# wpr/prime_fact_role_assignment.py
# Placeholder for assist_dev.wpr.fact_role_assignment
#
# Status   : BLOCKED — all five source role tables absent from Silver DDL
# Strategy : No TRUNCATE, no INSERT. Writes BLOCKED_SILVER_GAP audit record.
#
# IMPROVEMENT I-DP8-6 — BLOCKED placeholder (built-in):
#   Five source tables are confirmed absent from Silver DDL:
#     silver_aasbs_acquisition_role  — ABSENT
#     silver_aasbs_award_role        — ABSENT
#     silver_aasbs_ia_role           — ABSENT
#     silver_aasbs_funding_role      — ABSENT
#     silver_aasbs_solicit_role      — ABSENT
#
#   When all five are ingested into Silver, remove the BLOCKED exit and
#   implement the full INSERT using the schema documented below.
#
# IMPROVEMENT I-DP8-7 — ia_responsible deliberately excluded (built-in):
#   silver_aasbs_ia_responsible IS present in Silver DDL and contains IA
#   role assignments (ia_id, user_id, responsible_role_cd, is_primary_yn).
#   It is NOT one of the five blocked tables (ia_role is separate).
#   ia_responsible is deliberately NOT used to seed this fact because:
#     (a) Loading only IA-entity rows while AWARD, FUNDING, SOLICITATION,
#         and ACQUISITION remain empty creates a partial fact that consumers
#         would treat as complete.
#     (b) A partial fact is worse than an empty fact for CO/COR compliance
#         reporting (FAR 1.602) — it would falsely show only IA assignments.
#     (c) The empty, documented fact is ready to activate the moment all
#         five role tables are ingested.
#   This decision is documented here and in the driver output.
#
# IMPROVEMENT I-DP8-8 — non-fatal to driver (built-in):
#   This notebook exits with 'BLOCKED_SILVER_GAP' — not 'FAILED'.
#   The driver treats BLOCKED_SILVER_GAP as a pre-documented expected outcome
#   and emits DRIVER_COMPLETE (not DRIVER_FAILURE) when all dims succeed.
#
# When this table is eventually activated, the full INSERT will source from:
#   grain       : one row per user × entity × role across all 5 entity types
#   user_sk     ← wpr.dim_user via role_table.user_id
#   responsible_role_sk ← wpr.dim_responsible_role via role_table.role_cd
#   entity_type_sk ← wpr.dim_entity_type via entity type constant per table
#   org_sk      ← wpr.dim_org_hierarchy via user's emp_org_id
#   agency_sk   ← common.dim_agency via entity's AAC
#   award_sk    ← common.dim_award (AWARD entity type)
#   ia_sk       ← common.dim_ia (IA and FUNDING entity types)
#
# Ref: FAR 1.602-1 (CO warrant), FAR 1.602-2(d) (COR delegation)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID   = dbutils.widgets.get("run_id")
ENV      = dbutils.widgets.get("env")
PRODUCT  = "DP8"
NOTEBOOK = "prime_fact_role_assignment"
TARGET   = "assist_dev.wpr.fact_role_assignment"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET,
                        source_schema="aasbs", source_table="BLOCKED_TABLES")

BLOCKED_TABLES = [
    "silver_aasbs_acquisition_role",
    "silver_aasbs_award_role",
    "silver_aasbs_ia_role",
    "silver_aasbs_funding_role",
    "silver_aasbs_solicit_role",
]

blocked_msg = (
    f"BLOCKED_SILVER_GAP — fact_role_assignment cannot be loaded. "
    f"All five source role tables are absent from the Silver DDL: "
    f"{', '.join(BLOCKED_TABLES)}. "
    f"Silver pipeline team must ingest all five tables before this fact "
    f"can be built. "
    f"Note (I-DP8-7): silver_aasbs_ia_responsible is present in Silver but "
    f"is deliberately excluded — loading only IA-entity rows would create a "
    f"misleading partial fact. The empty fact is the correct state on prime."
)

print(f"[{NOTEBOOK}] BLOCKED")
print(f"[{NOTEBOOK}] {blocked_msg}")

# Write a BLOCKED audit record so the block is permanently visible
# in pipeline_audit and distinguishable from a pipeline failure.
spark.sql(f"""
    INSERT INTO assist_dev.common.pipeline_audit
        (job_run_id, data_product, job_name, target_table, run_type,
         run_status, start_ts, error_message)
    VALUES (
        '{RUN_ID}', '{PRODUCT}', '{NOTEBOOK}',
        '{TARGET}', 'FULL_PRIME',
        'BLOCKED', current_timestamp(),
        '{blocked_msg.replace("'", "''")}'
    )
""")

# Verify fact table remains empty  (guard: should already be 0 on fresh cluster)
existing = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
if existing > 0:
    warn_msg = (
        f"[{NOTEBOOK}] WARNING: fact_role_assignment contains {existing:,} rows "
        f"despite BLOCKED status. Rows were not written by this pipeline — "
        f"investigate source."
    )
    print(warn_msg)
else:
    print(f"[{NOTEBOOK}] ✓ fact_role_assignment is empty — correct for BLOCKED state.")

print(
    f"\n[{NOTEBOOK}] Blocked tables (Silver ingestion required to activate):"
)
for tbl in BLOCKED_TABLES:
    print(f"  ✗  {tbl}")

print(
    f"\n[{NOTEBOOK}] Excluded (I-DP8-7 deliberate decision):"
    f"\n  –  silver_aasbs_ia_responsible (partial fact worse than empty fact)"
)

# Exit with BLOCKED_SILVER_GAP — driver treats this as non-fatal
# IMPROVEMENT I-DP8-8: driver emits DRIVER_COMPLETE when all dims succeed
# and this fact exits with BLOCKED_SILVER_GAP
dbutils.notebook.exit("BLOCKED_SILVER_GAP")
