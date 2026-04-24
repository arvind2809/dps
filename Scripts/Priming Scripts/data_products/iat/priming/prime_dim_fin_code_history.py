# Databricks notebook source
# =============================================================================
# iat/prime_dim_fin_code_history.py
# Primes assist_dev.iat.dim_fin_code_history
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per award_fin_log.id  (natural key = fin_log_id)
#
# Source tables (Silver):
#   silver_aasbs_award_fin_log  →  FIN code history records
#
# IMPROVEMENT I-DP9-3 — Partial load with sentinel FK annotation (built-in):
#   silver_aasbs_award_fin_log exists in the Silver DDL with 13 columns.
#   CRITICAL: it has NO award_id column and NO ia_id column.
#   Both award_sk and ia_sk are therefore sentinel -1 on prime.
#   This is documented in the DDL v1.1.0 Change Log.
#   The table DOES load meaningful data:
#     fin_cd, fin_base_10, is_current_flag, effective_dt (proxy), created_by_user_id
#   Post-load assertions verify award_sk = -1 and ia_sk = -1 for all rows.
#   When the Silver pipeline adds award_id to silver_aasbs_award_fin_log,
#   re-prime this dim (TRUNCATE→INSERT — idempotent) to activate FK resolution.
#
# Field mapping (all confirmed against Silver DDL):
#   fin_log_id         ← award_fin_log.id                     (NK)
#   award_sk           ← CAST(-1 AS BIGINT)  [SILVER-PENDING — no award_id FK]
#   ia_sk              ← CAST(-1 AS BIGINT)  [SILVER-PENDING — no ia_id FK]
#   fin_cd             ← award_fin_log.fin
#   fin_base_10        ← award_fin_log.fin_base_10
#   effective_dt       ← award_fin_log.created_dt              [PROXY]
#   superseded_dt      ← CAST(NULL AS TIMESTAMP)               [no column]
#   is_current_flag    ← id = MAX(id) OVER (PARTITION BY fin)
#   created_by_user_id ← award_fin_log.created_by_user_id
#
# Silver-pending detail:
#   award_sk: award_fin_log.award_id does not exist in Silver DDL.
#   Resolution: Silver pipeline team must add award_id column to
#   silver_aasbs_award_fin_log. Once added, the join path is:
#     award_fin_log.award_id → dim_award.award_id → award_sk.
#
#   ia_sk: award_fin_log has no ia_id or any FK to bridge to dim_ia.
#   Even after award_id is added, a secondary bridge would be needed:
#     award_fin_log.award_id → award_ia.award_id → award_ia.ia_id → dim_ia.
#
# Ref: Treasury FMS (FIN/ACT number assignment), BAAR/Pegasys routing codes
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP9"
NOTEBOOK     = "prime_dim_fin_code_history"
TARGET_TABLE = "assist_dev.iat.dim_fin_code_history"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award_fin_log")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(
    f"[{NOTEBOOK}] IMPROVEMENT I-DP9-3 — PARTIAL LOAD:\n"
    f"  award_sk and ia_sk will be sentinel -1 for all rows on prime.\n"
    f"  silver_aasbs_award_fin_log has no award_id or ia_id column.\n"
    f"  Re-prime when Silver pipeline adds award_id to activate FK resolution."
)

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_award_fin_log
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source award_fin_log rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Pre-aggregate is_current_flag
    # IMPROVEMENT I-DP9-3: pre-compute is_current_flag via MAX(id) OVER
    # PARTITION BY fin before the main INSERT.
    # For a given FIN code string, the highest id is the most recently
    # created entry, treated as the current active FIN code.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_fin_current_flags AS
        SELECT
            id,
            CASE
                WHEN id = MAX(id) OVER (PARTITION BY fin)
                THEN TRUE
                ELSE FALSE
            END  AS is_current_flag
        FROM {SILVER}.silver_aasbs_award_fin_log
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """)
    print(f"[{NOTEBOOK}] v_fin_current_flags view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            fin_log_id,
            award_sk,
            ia_sk,
            fin_cd,
            fin_base_10,
            effective_dt,
            superseded_dt,
            is_current_flag,
            created_by_user_id,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            afl.id                                              AS fin_log_id,

            -- IMPROVEMENT I-DP9-3: SILVER-PENDING — award_id FK absent.
            -- silver_aasbs_award_fin_log has no award_id column in the Silver DDL.
            -- Resolution path once award_id is added to Silver:
            --   award_fin_log.award_id → common.dim_award.award_id → award_sk.
            -- Post-load assertion confirms = -1 on prime.
            CAST(-1 AS BIGINT)                                  AS award_sk,

            -- IMPROVEMENT I-DP9-3: SILVER-PENDING — ia_id FK absent.
            -- No ia_id or any FK exists on award_fin_log in Silver.
            -- Bridge path once award_id is available:
            --   award_fin_log.award_id → award_ia.award_id → ia_id → dim_ia.
            CAST(-1 AS BIGINT)                                  AS ia_sk,

            -- FIN code: the Financial Identification Number used for
            -- BAAR/Pegasys routing per Treasury FMS guidance
            afl.fin                                             AS fin_cd,

            -- FIN code in base-10 representation for legacy systems
            afl.fin_base_10,

            -- PROXY: effective_dt = created_dt (no explicit effective date column).
            -- created_dt is the timestamp the FIN code record was created in ASSIST.
            afl.created_dt                                      AS effective_dt,

            -- superseded_dt: no column on award_fin_log — NULL on prime
            CAST(NULL AS TIMESTAMP)                             AS superseded_dt,

            -- is_current_flag: TRUE for the most recently created record per FIN
            -- code value (MAX(id) OVER PARTITION BY fin) — pre-computed in view
            fcf.is_current_flag,

            -- User who created this FIN code entry in ASSIST
            afl.created_by_user_id,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_award_fin_log afl
        JOIN v_fin_current_flags fcf
            ON  fcf.id = afl.id
        WHERE COALESCE(afl.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load checks and sentinel assertions
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                               AS total,
            SUM(CASE WHEN is_current_flag = TRUE   THEN 1 ELSE 0 END)            AS current_rows,
            SUM(CASE WHEN is_current_flag = FALSE  THEN 1 ELSE 0 END)            AS superseded_rows,
            SUM(CASE WHEN award_sk        = -1     THEN 1 ELSE 0 END)            AS award_sentinel,
            SUM(CASE WHEN ia_sk           = -1     THEN 1 ELSE 0 END)            AS ia_sentinel,
            SUM(CASE WHEN superseded_dt IS NOT NULL THEN 1 ELSE 0 END)           AS has_superseded_dt,
            COUNT(DISTINCT fin_cd)                                                AS distinct_fin_codes
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={stats[0]:,} | current={stats[1]:,} | "
        f"superseded={stats[2]:,} | distinct_fin_codes={stats[6]:,}"
    )
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP9-3 — sentinel FK check:\n"
        f"  award_sk=-1: {stats[3]:,} (expected={rows_written:,}) | "
        f"ia_sk=-1: {stats[4]:,} (expected={rows_written:,})"
    )

    # Assertions: both FKs must be sentinel on prime
    assert stats[3] == rows_written, \
        f"ASSERT FAILED: award_sk must be -1 for all rows (SILVER-PENDING). " \
        f"Got {rows_written - stats[3]} non-sentinel rows — unexpected FK resolution."
    assert stats[4] == rows_written, \
        f"ASSERT FAILED: ia_sk must be -1 for all rows (SILVER-PENDING). " \
        f"Got {rows_written - stats[4]} non-sentinel rows — unexpected FK resolution."
    assert stats[5] == 0, \
        "ASSERT FAILED: superseded_dt must be NULL on prime (no Silver source)"

    print(
        f"  ✓ award_sk = -1 for all rows (Silver-pending — award_id absent)\n"
        f"  ✓ ia_sk = -1 for all rows (Silver-pending — ia_id absent)\n"
        f"  ✓ superseded_dt = NULL for all rows"
    )

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully (partial load — see I-DP9-3).")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
