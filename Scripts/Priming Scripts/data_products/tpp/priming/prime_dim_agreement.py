# Databricks notebook source
# =============================================================================
# tpp/prime_dim_agreement.py
# Primes assist_dev.tpp.dim_agreement
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per agreement.agreement_id
# SCD Type : 1 — agreement attributes updated in-place
#
# Source tables (Silver):
#   silver_agreement_agreement       → base agreement record (NK = agreement_id)
#   silver_agreement_agreement_line  → line_count (COUNT per agreement_id)
#   silver_agreement_agreement_log   → last_transmitted_dt (MAX accepted response)
#
# Field mapping (all confirmed against Silver DDL):
#   agreement_id          ← agreement.agreement_id                (NK)
#   agreement_number      ← agreement.agreement_num
#   fund_status_cd        ← agreement.funding_status
#   obligations_available ← agreement.obligations_avail_amt
#   accounting_period     ← agreement.acct_period
#   total_agreement_amt   ← agreement.maximum_agreement_amt
#   line_count            ← COUNT(agreement_line.agreement_line_id) per agreement_id
#   last_transmitted_dt   ← MAX(agreement_log.response_date) WHERE
#                           transmission_status indicates acceptance
#
# Design decisions:
#   last_transmitted_dt proxy:  agreement_log.response_date is the date the
#     BAAR system responded to a transmission.  MAX() across accepted responses
#     per agreement gives the most recent confirmed transmission timestamp.
#     Filter: UPPER(transmission_status) LIKE '%ACCEPT%' captures 'ACCEPTED',
#     'ACCEPT', and variants.  NULL if no accepted transmission exists.
#
# IMPROVEMENT I-DP5-1 (built-in):
#   Two pre-aggregated views prevent fan-out:
#     v_line_count_per_agreement     — COUNT of lines per agreement_id
#     v_last_transmitted_per_agmt    — MAX(response_date) for accepted responses
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_dim_agreement"
TARGET_TABLE = "assist_dev.tpp.dim_agreement"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_agreement_agreement")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_agreement_agreement
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source agreement rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — IMPROVEMENT I-DP5-1: pre-aggregate to prevent fan-out
    # ─────────────────────────────────────────────────────────────────────

    # 2a — Line count per agreement
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_line_count_per_agreement AS
        SELECT
            agreement_id,
            COUNT(*)    AS line_count
        FROM {SILVER}.silver_agreement_agreement_line
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY agreement_id
    """)

    # 2b — Last accepted transmission date per agreement
    # agreement_log.response_date is when BAAR acknowledged receipt.
    # Filter to accepted transmissions only.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_last_transmitted_per_agmt AS
        SELECT
            agreement_id,
            MAX(response_date)  AS last_transmitted_dt
        FROM {SILVER}.silver_agreement_agreement_log
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND UPPER(COALESCE(transmission_status, '')) LIKE '%ACCEPT%'
          AND response_date IS NOT NULL
        GROUP BY agreement_id
    """)

    print(f"[{NOTEBOOK}] Supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            agreement_id,
            agreement_number,
            fund_status_cd,
            obligations_available,
            accounting_period,
            total_agreement_amt,
            line_count,
            last_transmitted_dt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            a.agreement_id,

            -- BAAR agreement number (human-readable identifier)
            a.agreement_num                                     AS agreement_number,

            -- Funding status code (Active, Closed, Cancelled, etc.)
            a.funding_status                                    AS fund_status_cd,

            -- Available obligation authority remaining on this agreement
            -- a.obligations_avail_amt                             AS obligations_available,
            NULL as obligations_available,

            -- BAAR accounting period (fiscal period string, e.g. '2025/04')
            a.acct_period                                       AS accounting_period,

            -- Total ceiling amount of the agreement
            a.maximum_agreement_amt                             AS total_agreement_amt,

            -- Line count: COUNT(agreement_line) per agreement_id
            -- IMPROVEMENT I-DP5-1: pre-aggregated view prevents fan-out
            COALESCE(lc.line_count, 0)                          AS line_count,

            -- Last accepted transmission date: MAX(agreement_log.response_date)
            -- for rows where transmission_status indicates acceptance.
            -- IMPROVEMENT I-DP5-1: pre-aggregated view prevents fan-out
            lt.last_transmitted_dt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_agreement_agreement a

        -- IMPROVEMENT I-DP5-1: join pre-aggregated views (not raw tables)
        LEFT JOIN v_line_count_per_agreement lc
            ON  lc.agreement_id = a.agreement_id

        LEFT JOIN v_last_transmitted_per_agmt lt
            ON  lt.agreement_id = a.agreement_id

        WHERE COALESCE(a.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                            AS total_rows,
            SUM(CASE WHEN last_transmitted_dt IS NOT NULL  THEN 1 ELSE 0 END) AS has_last_tx,
            SUM(CASE WHEN line_count > 0                   THEN 1 ELSE 0 END) AS has_lines,
            SUM(CASE WHEN obligations_available IS NOT NULL THEN 1 ELSE 0 END) AS has_obligs,
            ROUND(SUM(COALESCE(total_agreement_amt, 0)), 2)                   AS grand_total_amt
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={stats[0]:,} | has_last_tx={stats[1]:,} | "
        f"has_lines={stats[2]:,} | has_obligs={stats[3]:,}"
    )
    print(f"[{NOTEBOOK}] Grand total agreement amt: ${stats[4]:,.2f}")

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
