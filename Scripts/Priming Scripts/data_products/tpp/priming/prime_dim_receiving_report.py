# Databricks notebook source
# =============================================================================
# tpp/prime_dim_receiving_report.py
# Primes assist_dev.tpp.dim_receiving_report
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per rr_summary.id
# SCD Type : 1 — receiving report attributes updated in-place
#
# Source tables (Silver):
#   silver_aasbs_transmit_rr_summary  → base RR record (NK = id = rr_summary_id)
#   silver_aasbs_transmit_rr_detail   → mdl_amt (SUM(total_mdl_amount) per rr)
#
# Field mapping (all confirmed against Silver DDL):
#   rr_summary_id       ← rr_summary.id                           (NK)
#   pegasys_invoice_num ← rr_summary.pegasys_invoice_number
#   rr_acceptance_dt    ← rr_summary.accepted_dt
#   total_rr_amt        ← rr_summary.max_approved_amt
#                         (max_approved_amt = total value accepted on this RR)
#   rr_line_count       ← rr_summary.number_of_detail_lines       (direct column)
#   mdl_amt             ← SUM(rr_detail.total_mdl_amount) per rr_summary_id
#                         MDL = Maximum Dollar Limit per RR line per TFM §4700.10
#
# Design decision — rr_line_count:
#   rr_summary.number_of_detail_lines is used directly (authoritative count from
#   the transmission header) rather than COUNT(rr_detail) which may differ if
#   some detail rows are soft-deleted or not yet ingested.
#
# IMPROVEMENT I-DP5-1 (built-in):
#   v_mdl_per_rr pre-aggregates SUM(rr_detail.total_mdl_amount) per rr_summary_id.
#   rr_detail has one row per line; direct join without pre-aggregation would
#   fan-out one fact row per RR line.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_dim_receiving_report"
TARGET_TABLE = "assist_dev.tpp.dim_receiving_report"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_transmit_rr_summary")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_transmit_rr_summary
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source rr_summary rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — IMPROVEMENT I-DP5-1: pre-aggregate MDL per RR
    #
    # rr_detail has one row per RR line item.  Direct JOIN would produce
    # one Gold row per detail line rather than per RR header.
    # SUM(total_mdl_amount) gives the aggregate MDL cap for the full RR.
    # MDL = Maximum Dollar Limit per TFM Volume I, Part 2, §4700.10.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_mdl_per_rr AS
        SELECT
            rr_summary_id,
            SUM(COALESCE(total_mdl_amount, 0.00))   AS mdl_amt
        FROM {SILVER}.silver_aasbs_transmit_rr_detail
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY rr_summary_id
    """)

    print(f"[{NOTEBOOK}] MDL aggregation view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            rr_summary_id,
            pegasys_invoice_num,
            rr_acceptance_dt,
            total_rr_amt,
            rr_line_count,
            mdl_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            rr.id                                               AS rr_summary_id,

            -- Pegasys invoice number from the receiving report header
            rr.pegasys_invoice_number                           AS pegasys_invoice_num,

            -- Date the receiving report was accepted by the receiving official
            rr.accepted_dt                                      AS rr_acceptance_dt,

            -- Total RR value: max_approved_amt is the aggregate accepted amount
            -- for this receiving report per TFM guidance
            rr.max_approved_amt                                 AS total_rr_amt,

            -- Line count: authoritative value from the RR transmission header.
            -- Preferred over COUNT(rr_detail) to avoid soft-delete discrepancies.
            rr.number_of_detail_lines                           AS rr_line_count,

            -- MDL amount: SUM(rr_detail.total_mdl_amount) per rr_summary
            -- IMPROVEMENT I-DP5-1: pre-aggregated view prevents fan-out
            COALESCE(mdl.mdl_amt, 0.00)                         AS mdl_amt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_transmit_rr_summary rr

        -- IMPROVEMENT I-DP5-1: pre-aggregated MDL (not raw rr_detail)
        LEFT JOIN v_mdl_per_rr mdl
            ON  mdl.rr_summary_id = rr.id

        WHERE COALESCE(rr.is_deleted, FALSE) = FALSE
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
            COUNT(*)                                                              AS total_rows,
            SUM(CASE WHEN rr_acceptance_dt IS NOT NULL  THEN 1 ELSE 0 END)      AS accepted_count,
            SUM(CASE WHEN mdl_amt > 0                   THEN 1 ELSE 0 END)      AS has_mdl,
            ROUND(SUM(COALESCE(total_rr_amt, 0)), 2)                            AS grand_total_rr,
            ROUND(SUM(COALESCE(mdl_amt,     0)), 2)                             AS grand_total_mdl
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={stats[0]:,} | accepted={stats[1]:,} | has_mdl={stats[2]:,}"
    )
    print(
        f"[{NOTEBOOK}] Financial totals — "
        f"total_rr=${stats[3]:,.2f} | total_mdl=${stats[4]:,.2f}"
    )

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
