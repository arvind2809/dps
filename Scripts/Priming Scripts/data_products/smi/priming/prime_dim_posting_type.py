# Databricks notebook source
# =============================================================================
# smi/prime_dim_posting_type.py
# Primes assist_dev.smi.dim_posting_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per solicit.id  (natural key = solicit_id)
# SCD Type : 1 — posting type updated in-place when solicitation is amended
#
# Source tables (Silver — all confirmed against Silver DDL):
#   silver_aasbs_solicit                  → grain anchor (solicit.id = NK)
#   silver_aasbs_solicit_amendment        → bridge: solicit_amendment.solicit_id
#   silver_aasbs_solicit_posting_direct   → DIRECT / DIRECT_CONNECT postings
#   silver_aasbs_solicit_posting_connect  → OPEN / INFORMAL / FORMAL postings
#
# Field mapping (confirmed against Silver DDL):
#   solicit_id             ← solicit.id                          (NK)
#   posting_type_cd        ← derived via COALESCE logic          (IMPROVEMENT I-DP7-5)
#   posting_type_desc      ← human-readable label per type_cd
#   open_dt                ← COALESCE(direct.open_dt, connect.open_dt)
#   pm_responsible_user_id ← solicit_posting_connect.pm_responsible_user_id
#   ja_reason_cd           ← solicit_posting_direct.reason_for_direct_cd
#                            (J&A reason for other-than-full-and-open; FAR 6.302)
#   external_source_ref    ← solicit_posting_direct.external_source_ref
#
# IMPROVEMENT I-DP7-5 — COALESCE posting_type_cd logic (built-in):
#   Both posting tables link to solicit via solicit_amendment.solicit_id.
#   A solicitation has either a DIRECT posting, a CONNECT posting, or neither
#   (INFORMAL — no formal posting in ASSIST).
#   Priority CASE:
#     DIRECT_CONNECT: posting_direct exists AND reason_for_direct_cd indicates
#                     a connect/co-op arrangement
#     DIRECT:         posting_direct exists AND no connect indicator
#     OPEN/FORMAL:    posting_connect exists
#     INFORMAL:       neither posting table has a record for this solicit
#   Post-load integrity check verifies no solicitation has BOTH a direct
#   AND a connect posting (which would indicate a source data integrity issue).
#
# Join path to solicit:
#   solicit_posting_direct.solicit_amendment_id → solicit_amendment.id
#   solicit_amendment.solicit_id = solicit.id
#   Same path for solicit_posting_connect.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP7"
NOTEBOOK     = "prime_dim_posting_type"
TARGET_TABLE = "assist_dev.smi.dim_posting_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_solicit")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_solicit
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source solicit rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Pre-aggregate one posting record per solicit
    #
    # IMPROVEMENT I-DP7-5: pre-aggregated views surface the posting type
    # per solicitation before the main INSERT. Each view resolves through:
    #   posting_table.solicit_amendment_id
    #     → solicit_amendment.id (FK confirmed in Silver DDL)
    #     → solicit_amendment.solicit_id = solicit.id
    # MIN() used to pick a single posting record when multiple amendments
    # have posting records (takes the earliest for stability).
    # ─────────────────────────────────────────────────────────────────────

    # 2a — Direct postings per solicit
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_direct_per_solicit AS
        SELECT
            sa.solicit_id,
            MIN(pd.id)                      AS posting_direct_id,
            MIN(pd.reason_for_direct_cd)    AS reason_for_direct_cd,
            MIN(pd.external_source_ref)     AS external_source_ref,
            MIN(pd.open_dt)                 AS open_dt
        FROM {SILVER}.silver_aasbs_solicit_posting_direct pd
        JOIN {SILVER}.silver_aasbs_solicit_amendment sa
            ON  sa.id = pd.solicit_amendment_id
            AND COALESCE(sa.is_deleted, FALSE) = FALSE
        WHERE COALESCE(pd.is_deleted, FALSE) = FALSE
        GROUP BY sa.solicit_id
    """)

    # 2b — Connect postings per solicit
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_connect_per_solicit AS
        SELECT
            sa.solicit_id,
            MIN(pc.id)                          AS posting_connect_id,
            MIN(pc.pm_responsible_user_id)      AS pm_responsible_user_id,
            MIN(pc.open_dt)                     AS open_dt
        FROM {SILVER}.silver_aasbs_solicit_posting_connect pc
        JOIN {SILVER}.silver_aasbs_solicit_amendment sa
            ON  sa.id = pc.solicit_amendment_id
            AND COALESCE(sa.is_deleted, FALSE) = FALSE
        WHERE COALESCE(pc.is_deleted, FALSE) = FALSE
        GROUP BY sa.solicit_id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            solicit_id,
            posting_type_cd,
            posting_type_desc,
            open_dt,
            pm_responsible_user_id,
            ja_reason_cd,
            external_source_ref,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: solicitation identifier
            s.id                                                AS solicit_id,

            -- IMPROVEMENT I-DP7-5: posting_type_cd derived via COALESCE logic.
            -- A solicitation has at most one posting type at a given time:
            --   DIRECT_CONNECT: direct posting exists with external connect ref
            --   DIRECT:         direct posting exists (sole-source / J&A)
            --   OPEN:           connect posting exists (competitive)
            --   INFORMAL:       no formal posting record in ASSIST
            CASE
                WHEN pd.posting_direct_id IS NOT NULL
                 AND pd.external_source_ref IS NOT NULL
                    THEN 'DIRECT_CONNECT'
                WHEN pd.posting_direct_id IS NOT NULL
                    THEN 'DIRECT'
                WHEN pc.posting_connect_id IS NOT NULL
                    THEN 'OPEN'
                ELSE 'INFORMAL'
            END                                                 AS posting_type_cd,

            -- Human-readable label corresponding to the type code
            CASE
                WHEN pd.posting_direct_id IS NOT NULL
                 AND pd.external_source_ref IS NOT NULL
                    THEN 'Direct Connect (external source)'
                WHEN pd.posting_direct_id IS NOT NULL
                    THEN 'Direct Award (sole-source / J&A)'
                WHEN pc.posting_connect_id IS NOT NULL
                    THEN 'Open Competitive Solicitation'
                ELSE 'Informal / No Formal Posting'
            END                                                 AS posting_type_desc,

            -- open_dt: from whichever posting table is present; prefer direct
            COALESCE(pd.open_dt, pc.open_dt)                    AS open_dt,

            -- Program Manager responsible user (connect postings only)
            pc.pm_responsible_user_id,

            -- J&A reason code (FAR 6.302 — other-than-full-and-open competition)
            -- Only populated for DIRECT postings
            pd.reason_for_direct_cd                             AS ja_reason_cd,

            -- External source reference for DIRECT_CONNECT postings
            pd.external_source_ref,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_solicit s
        LEFT JOIN v_direct_per_solicit pd
            ON  pd.solicit_id = s.id
        LEFT JOIN v_connect_per_solicit pc
            ON  pc.solicit_id = s.id
        WHERE COALESCE(s.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP7-5: posting type distribution
    dist = spark.sql(f"""
        SELECT posting_type_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY posting_type_cd ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] IMPROVEMENT I-DP7-5 — posting_type_cd distribution:")
    for row in dist:
        print(f"  {str(row[0]):<20}  {row[1]:>8,}")

    # IMPROVEMENT I-DP7-5: integrity check — no solicit should have BOTH posting types
    both_count = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT s.solicit_id FROM v_direct_per_solicit s
            JOIN v_connect_per_solicit c ON c.solicit_id = s.solicit_id
        )
    """).collect()[0][0]
    if both_count > 0:
        print(
            f"[{NOTEBOOK}] WARNING: {both_count} solicitation(s) have BOTH a direct "
            f"AND a connect posting record. This indicates a source data integrity "
            f"issue — DIRECT type will take precedence in the loaded data."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Posting type integrity: no solicitation has both types.")

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
