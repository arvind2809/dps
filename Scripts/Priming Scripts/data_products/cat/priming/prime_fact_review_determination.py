# Databricks notebook source
# =============================================================================
# cat/prime_fact_review_determination.py
# Placeholder for assist_dev.cat.fact_review_determination
#
# Status   : BLOCKED — aasbs.central_collab_request absent from Silver DDL
# Strategy : No TRUNCATE, no INSERT. Writes BLOCKED_SILVER_GAP audit record.
#
# IMPROVEMENT I-DP10-6 — BLOCKED placeholder (built-in):
#   Primary source aasbs.central_collab_request is absent from Silver DDL.
#   This is the determination detail table — it holds:
#     reviewer_user_id, review_assigned_dt, determination_dt,
#     days_to_determination, determination_comments, is_blocking_finding_flag
#   All six meaningful Gold columns originate from this single absent table.
#
# IMPROVEMENT I-DP10-7 — silver_aasbs_review deliberately excluded (built-in):
#   silver_aasbs_review (17 columns: review header only) IS present in Silver:
#     id, review_type_cd, review_parent_table_name_cd, solicit_id,
#     award_mod_id, attach_hash_id, draft_review_yn, created_by_user_id, etc.
#   silver_aasbs_review is the REVIEW HEADER — it does not contain any of the
#   six determination detail columns.
#   Loading review header rows with six NULL determination columns would create
#   a partial fact that OIG and compliance consumers would treat as complete
#   and rely on for audit evidence purposes (5 U.S.C. App. 3).
#   An empty, documented fact is the correct state on prime.
#   silver_aasbs_review is deliberately excluded — same rationale as:
#     DP8 I-DP8-7 (ia_responsible excluded from fact_role_assignment)
#     DP9 exclusion of ia_responsible from review fact
#
# When central_collab_request is ingested into Silver, implement the INSERT:
#   grain: one row per determination (collab_request.id)
#   review_type_sk       ← cat.dim_review_type via collab_request.review_type_cd
#   reviewer_type_sk     ← cat.dim_reviewer_type via collab_request.reviewer_type_cd
#   finding_sk           ← cat.dim_finding via collab_request.finding_cd
#   award_sk             ← common.dim_award via collab_request → review → award_mod
#   ia_sk                ← common.dim_ia via collab_request → review → ia junction
#   agency_sk            ← common.dim_agency
#   reviewer_user_id     ← collab_request.reviewer_user_id
#   review_assigned_dt   ← collab_request.assigned_dt
#   determination_dt     ← collab_request.determination_dt
#   days_to_determination← DATEDIFF(determination_dt, assigned_dt)
#   determination_comments← collab_request.comments
#   is_blocking_finding_flag ← cat.dim_finding.is_blocking_flag via finding_sk
#
# Ref: 5 U.S.C. App. 3 (IG Act), OMB A-123, FAR Part 15 (price/tech review)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID   = dbutils.widgets.get("run_id")
ENV      = dbutils.widgets.get("env")
PRODUCT  = "DP10"
NOTEBOOK = "prime_fact_review_determination"
TARGET   = "assist_dev.cat.fact_review_determination"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET, run_type="FULL_PRIME"
)

blocked_msg = (
    "BLOCKED_SILVER_GAP — fact_review_determination cannot be loaded. "
    "Primary source aasbs.central_collab_request is absent from the Silver DDL. "
    "All six meaningful Gold columns (reviewer_user_id, review_assigned_dt, "
    "determination_dt, days_to_determination, determination_comments, "
    "is_blocking_finding_flag) originate exclusively from central_collab_request. "
    "silver_aasbs_review is deliberately excluded (I-DP10-7): review header rows "
    "with six NULL determination columns would create a misleading partial fact "
    "unsuitable for OIG audit use (5 U.S.C. App. 3). "
    "Silver pipeline team must ingest aasbs.central_collab_request to activate."
)

print(f"[{NOTEBOOK}] BLOCKED")
print(f"[{NOTEBOOK}] {blocked_msg}")

spark.sql(f"""
    INSERT INTO assist_dev.common.pipeline_audit
        (run_id, product, notebook, target_table, run_type,
         status, started_at, error_message)
    VALUES (
        '{RUN_ID}', '{PRODUCT}', '{NOTEBOOK}',
        '{TARGET}', 'FULL_PRIME',
        'BLOCKED', current_timestamp(),
        '{blocked_msg.replace("'", "''")}'
    )
""")

existing = spark.sql(f"SELECT COUNT(*) FROM {TARGET}").collect()[0][0]
if existing > 0:
    print(
        f"[{NOTEBOOK}] WARNING: {existing:,} rows found in fact_review_determination "
        f"despite BLOCKED status — investigate source."
    )
else:
    print(f"[{NOTEBOOK}] ✓ fact_review_determination is empty — correct for BLOCKED state.")

print(f"\n[{NOTEBOOK}] Blocked source: silver_aasbs_central_collab_request (absent)")
print(f"[{NOTEBOOK}] Excluded source (I-DP10-7): silver_aasbs_review (review header only — partial fact risk)")

dbutils.notebook.exit("BLOCKED_SILVER_GAP")
