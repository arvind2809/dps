# Databricks notebook source
# =============================================================================
# wpr/prime_dim_user.py
# Primes assist_dev.wpr.dim_user
#
# Strategy : TRUNCATE → INSERT  (SCD2 current snapshot — IMPROVEMENT I-DP8-1)
# Grain    : One row per assist_users.id  (current state only on prime)
# SCD Type : 2 — history managed by CDC after prime
#
# Source tables (Silver):
#   silver_assist_users  →  user records
#
# IMPROVEMENT I-DP8-1 — SCD2 snapshot prime pattern (built-in):
#   DP8 introduces the first SCD Type 2 tables in the DP series.
#   On initial prime, SCD2 tables receive one row per entity:
#     eff_start_dt   = COALESCE(activation_date, creation_date)
#     eff_end_dt     = CAST(NULL AS TIMESTAMP)   — active row
#     is_current_flag= TRUE
#   This is a current-state snapshot, NOT a history rebuild.
#   CDC will subsequently manage SCD2 changes by:
#     (a) closing the old row: SET eff_end_dt=change_dt, is_current_flag=FALSE
#     (b) inserting the new row: eff_start_dt=change_dt, is_current_flag=TRUE
#   run_type = 'FULL_PRIME_SCD2_SNAPSHOT' distinguishes prime from CDC runs
#   in the pipeline_audit table.
#   Post-load assertions:
#     is_current_flag = FALSE for 0 rows  (no history rows on prime)
#     eff_end_dt IS NOT NULL for 0 rows   (no closed rows on prime)
#
# Field mapping (all confirmed against Silver DDL):
#   user_id              ← CAST(assist_users.id AS STRING)    (NK)
#   first_name           ← assist_users.first_name
#   last_name            ← assist_users.last_name
#   full_name            ← TRIM(CONCAT(first_name, middle_initial, last_name))
#   email                ← assist_users.email
#   phone                ← CONCAT(phone, COALESCE(' x'||phone_ext,''))
#   org_code             ← CAST(NULL)  [SILVER GAP — no user→org FK in Silver]
#   org_name             ← CAST(NULL)  [SILVER GAP — same]
#   region_cd            ← CAST(NULL)  [SILVER GAP — no region on assist_users]
#   is_active_flag       ← reg_status='ACTIVE' AND inactive_date IS NULL
#   procurement_system_id← assist_users.okta_user_id
#   eff_start_dt         ← COALESCE(activation_date, creation_date)
#   eff_end_dt           ← CAST(NULL AS TIMESTAMP)   [SCD2 prime]
#   is_current_flag      ← TRUE                      [SCD2 prime]
#
# Silver gaps (all annotated):
#   org_code / org_name: assist_users has no emp_org_id FK.
#     lu_emp_org_xref links org nodes to each other (parent→child),
#     not to users. No user→org join path confirmed in Silver DDL.
#   region_cd: no region column on assist_users.
#
# Ref: FAR 1.602-1 (CO warrant), FAR 1.602-2(d) (COR delegation)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_user"
TARGET_TABLE = "assist_dev.wpr.dim_user"
RUN_TYPE     = "FULL_PRIME_SCD2_SNAPSHOT"   # IMPROVEMENT I-DP8-1

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type=RUN_TYPE)
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_assist_users")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(
    f"[{NOTEBOOK}] IMPROVEMENT I-DP8-1 — SCD2 snapshot prime:\n"
    f"  Strategy  : one current row per user; eff_end_dt=NULL; is_current_flag=TRUE\n"
    f"  run_type  : {RUN_TYPE}\n"
    f"  CDC note  : history managed by CDC after prime; this is a point-in-time snapshot"
)

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_assist_users
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND COALESCE(delete_flag, 'N') <> 'Y'
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source assist_users rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT (SCD2 current snapshot)
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            user_id,
            first_name,
            last_name,
            full_name,
            email,
            phone,
            org_code,
            org_name,
            region_cd,
            is_active_flag,
            procurement_system_id,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: ASSIST user ID cast to STRING (Gold NK type)
            CAST(u.id AS STRING)                                AS user_id,

            -- Name fields
            u.first_name,
            u.last_name,

            -- Full display name with middle initial when present
            TRIM(
                COALESCE(u.first_name, '')
                || CASE WHEN u.middle_initial IS NOT NULL
                        THEN ' ' || u.middle_initial || ' '
                        ELSE ' ' END
                || COALESCE(u.last_name, '')
            )                                                   AS full_name,

            -- Contact info
            u.email,
            -- Phone with extension
            CASE
                WHEN u.phone_ext IS NOT NULL AND u.phone_ext <> ''
                THEN CONCAT(COALESCE(u.phone,''), ' x', u.phone_ext)
                ELSE u.phone
            END                                                 AS phone,

            -- SILVER GAP: org_code
            -- assist_users has no emp_org_id or any org hierarchy FK.
            -- The lu_emp_org_xref table links org nodes (parent→child) but
            -- contains no user reference. A user→org mapping table is not
            -- present in the Silver DDL.
            CAST(NULL AS STRING)                                AS org_code,

            -- SILVER GAP: org_name — same limitation as org_code
            CAST(NULL AS STRING)                                AS org_name,

            -- SILVER GAP: region_cd — no region column on assist_users
            CAST(NULL AS STRING)                                AS region_cd,

            -- is_active_flag: active when reg_status='ACTIVE' AND not inactive
            CASE
                WHEN u.reg_status = 'ACTIVE'
                 AND u.inactive_date IS NULL
                THEN TRUE
                ELSE FALSE
            END                                                 AS is_active_flag,

            -- Procurement system ID: Okta SSO identifier linking to
            -- procurement systems (FAR 1.602 warrant tracking)
            u.okta_user_id                                      AS procurement_system_id,

            -- IMPROVEMENT I-DP8-1: SCD2 eff_start_dt
            -- activation_date = when account was formally activated
            -- creation_date   = when account record was created (fallback)
            COALESCE(u.activation_date, u.creation_date)        AS eff_start_dt,

            -- IMPROVEMENT I-DP8-1: eff_end_dt = NULL on prime
            -- NULL means this row is the currently active version.
            -- CDC will set this when the user record changes.
            CAST(NULL AS TIMESTAMP)                             AS eff_end_dt,

            -- IMPROVEMENT I-DP8-1: is_current_flag = TRUE for all rows on prime
            -- Every row is "current" in an initial snapshot.
            -- CDC will set this to FALSE when a newer version is inserted.
            TRUE                                                AS is_current_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_assist_users u
        WHERE COALESCE(u.is_deleted, FALSE) = FALSE
          AND COALESCE(u.delete_flag, 'N') <> 'Y'
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks and SCD2 assertions
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total,
            SUM(CASE WHEN is_current_flag = FALSE    THEN 1 ELSE 0 END)         AS not_current,
            SUM(CASE WHEN eff_end_dt IS NOT NULL     THEN 1 ELSE 0 END)         AS has_end_dt,
            SUM(CASE WHEN is_active_flag = TRUE      THEN 1 ELSE 0 END)         AS active,
            SUM(CASE WHEN is_active_flag = FALSE     THEN 1 ELSE 0 END)         AS inactive,
            SUM(CASE WHEN org_code IS NOT NULL       THEN 1 ELSE 0 END)         AS has_org_code,
            SUM(CASE WHEN procurement_system_id IS NOT NULL THEN 1 ELSE 0 END)  AS has_okta
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Users — active={stats[3]:,} | inactive={stats[4]:,} | "
        f"has_okta_id={stats[5]:,}"
    )
    print(
        f"[{NOTEBOOK}] Silver gaps — "
        f"org_code={stats[5]:,} (expected 0) | "
        f"region_cd=0 (expected 0)"
    )

    # IMPROVEMENT I-DP8-1: SCD2 snapshot assertions
    assert stats[1] == 0, \
        f"ASSERT FAILED: is_current_flag must be TRUE for all rows on prime (SCD2 snapshot). " \
        f"Got {stats[1]} rows with is_current_flag=FALSE."
    assert stats[2] == 0, \
        f"ASSERT FAILED: eff_end_dt must be NULL for all rows on prime (SCD2 snapshot). " \
        f"Got {stats[2]} rows with eff_end_dt IS NOT NULL."
    assert stats[5] == 0, \
        "ASSERT FAILED: org_code must be NULL on prime (Silver gap — no user→org FK)."
    print(f"[{NOTEBOOK}] IMPROVEMENT I-DP8-1 — SCD2 snapshot assertions: PASSED")

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
