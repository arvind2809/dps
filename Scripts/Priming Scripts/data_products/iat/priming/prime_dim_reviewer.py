# Databricks notebook source
# =============================================================================
# iat/prime_dim_reviewer.py
# Primes assist_dev.iat.dim_reviewer
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per DISTINCT ia_review.service_agency_reviewer_id
# SCD Type : 1 — reviewer attributes updated in-place
#
# Source tables (Silver):
#   silver_aasbs_ia_review  →  provides the distinct set of reviewer user IDs
#   silver_assist_users     →  name, active status per reviewer user ID
#
# IMPROVEMENT I-DP9-2 — Servicing-agency-only grain (built-in):
#   ia_review carries two reviewer references:
#     service_agency_reviewer_id  — BIGINT FK to assist.users.id (resolvable)
#     request_agency_reviewer     — STRING free-text name (not a FK, no user ID)
#   This dimension contains ONLY servicing agency reviewers because
#   service_agency_reviewer_id is the sole resolvable FK to assist_users.
#   Requesting agency reviewer names are stored as free text and cannot be
#   normalised into dim rows without a user ID.
#   The post-load check reports: total reviewer rows, reviewers matched to
#   a user record, and reviewers with NULL name (user ID not in assist_users).
#
# Field mapping (all confirmed against Silver DDL):
#   reviewer_user_id  ← ia_review.service_agency_reviewer_id  (NK — distinct)
#   reviewer_name     ← CONCAT(users.first_name, ' ', users.last_name)
#                       Middle initial included when present
#   reviewer_role_cd  ← CAST(NULL AS STRING)
#                       [SILVER GAP — no role column on ia_review;
#                        ia_responsible links by ia_id, not reviewer user_id]
#   reviewer_org      ← CAST(NULL AS STRING)
#                       [SILVER GAP — no org column on assist_users or ia_review]
#   is_active_flag    ← users.reg_status = 'ACTIVE' AND users.inactive_date IS NULL
#
# Ref: FAR 1.602-2(d) (COR/reviewer delegation), OMB A-123 (IA reviewers)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP9"
NOTEBOOK     = "prime_dim_reviewer"
TARGET_TABLE = "assist_dev.iat.dim_reviewer"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_ia_review")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(DISTINCT service_agency_reviewer_id)
        FROM {SILVER}.silver_aasbs_ia_review
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND service_agency_reviewer_id IS NOT NULL
    """).collect()[0][0]
    print(
        f"[{NOTEBOOK}] Distinct servicing agency reviewer IDs in ia_review: {rows_read:,}"
    )
    # IMPROVEMENT I-DP9-2: report excluded free-text requestor reviewers
    req_text_cnt = spark.sql(f"""
        SELECT COUNT(DISTINCT request_agency_reviewer)
        FROM {SILVER}.silver_aasbs_ia_review
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND request_agency_reviewer IS NOT NULL
    """).collect()[0][0]
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP9-2 — distinct request_agency_reviewer "
        f"free-text names (excluded by design — no resolvable user ID): "
        f"{req_text_cnt:,}"
    )

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            reviewer_user_id,
            reviewer_name,
            reviewer_role_cd,
            reviewer_org,
            is_active_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: DISTINCT servicing agency reviewer user ID
            -- IMPROVEMENT I-DP9-2: only service_agency_reviewer_id is included.
            -- request_agency_reviewer is free-text only and excluded by design.
            rev.service_agency_reviewer_id                      AS reviewer_user_id,

            -- Full name: first + middle initial (when present) + last
            CASE
                WHEN usr.first_name IS NOT NULL
                THEN TRIM(
                    COALESCE(usr.first_name, '')
                    || CASE WHEN usr.middle_initial IS NOT NULL
                            THEN ' ' || usr.middle_initial || ' '
                            ELSE ' ' END
                    || COALESCE(usr.last_name, '')
                )
                ELSE NULL
            END                                                 AS reviewer_name,

            -- SILVER GAP: reviewer_role_cd
            -- ia_review has no reviewer role code column.
            -- ia_responsible.responsible_role_cd links by ia_id, not by
            -- reviewer user_id — no confirmed join path exists.
            CAST(NULL AS STRING)                                AS reviewer_role_cd,

            -- SILVER GAP: reviewer_org
            -- silver_assist_users has no organisation column.
            -- assist.lu_emp_orgs holds the org hierarchy but the join from
            -- users to their org is not confirmed in the Silver DDL.
            CAST(NULL AS STRING)                                AS reviewer_org,

            -- is_active_flag: active when reg_status = 'ACTIVE' AND no
            -- inactive_date set. COALESCE handles NULL users (unmatched IDs).
            CASE
                WHEN usr.reg_status = 'ACTIVE'
                 AND usr.inactive_date IS NULL
                THEN TRUE
                WHEN usr.id IS NULL
                THEN CAST(NULL AS BOOLEAN)   -- reviewer ID not found in users
                ELSE FALSE
            END                                                 AS is_active_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM (
            -- Deduplicate to one row per reviewer user ID
            SELECT DISTINCT service_agency_reviewer_id
            FROM {SILVER}.silver_aasbs_ia_review
            WHERE COALESCE(is_deleted, FALSE) = FALSE
              AND service_agency_reviewer_id IS NOT NULL
        ) rev

        -- Resolve user details from assist_users
        LEFT JOIN {SILVER}.silver_assist_users usr
            ON  CAST(usr.id AS STRING) = CAST(rev.service_agency_reviewer_id AS STRING)
            AND COALESCE(usr.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total,
            SUM(CASE WHEN reviewer_name IS NOT NULL   THEN 1 ELSE 0 END)        AS matched_to_user,
            SUM(CASE WHEN reviewer_name IS NULL        THEN 1 ELSE 0 END)        AS no_user_match,
            SUM(CASE WHEN is_active_flag = TRUE        THEN 1 ELSE 0 END)        AS active,
            SUM(CASE WHEN is_active_flag = FALSE       THEN 1 ELSE 0 END)        AS inactive,
            SUM(CASE WHEN reviewer_role_cd IS NOT NULL THEN 1 ELSE 0 END)        AS has_role_cd,
            SUM(CASE WHEN reviewer_org IS NOT NULL     THEN 1 ELSE 0 END)        AS has_org
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP9-2 — reviewer coverage:\n"
        f"  total={stats[0]:,} | matched_to_user={stats[1]:,} | "
        f"no_user_match={stats[2]:,}"
    )
    print(
        f"  active={stats[3]:,} | inactive={stats[4]:,} | "
        f"is_active=NULL (unmatched)={stats[0]-stats[3]-stats[4]:,}"
    )
    print(
        f"  reviewer_role_cd={stats[5]:,} (expected 0 — Silver gap) | "
        f"reviewer_org={stats[6]:,} (expected 0 — Silver gap)"
    )

    if stats[2] > 0:
        print(
            f"\n  ⚠ {stats[2]:,} reviewer IDs from ia_review not found in "
            f"assist_users. These are external reviewers or users deleted from "
            f"the system. reviewer_name = NULL for these rows."
        )

    # Assert Silver gaps remain NULL
    assert stats[5] == 0, \
        "ASSERT FAILED: reviewer_role_cd must be NULL on prime (Silver gap)"
    assert stats[6] == 0, \
        "ASSERT FAILED: reviewer_org must be NULL on prime (Silver gap)"

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
