# Databricks notebook source
# =============================================================================
# psc/prime_dim_billing_period.py
# Primes assist_dev.psc.dim_billing_period
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per service_charge_schedule.id  (natural key = schedule_id)
# SCD Type : 1 — billing period attributes updated in-place
#
# Source tables (Silver — all confirmed against Silver DDL):
#   silver_aasbs_service_charge_schedule  → base schedule rows (grain)
#   silver_aasbs_line_item_accepted       → bridge for invoice join
#   silver_aasbs_invoice_item             → invoiced_amt aggregation
#
# Field mapping (all confirmed against Silver DDL):
#   schedule_id         ← service_charge_schedule.id            (NK)
#   billing_period_label ← CONCAT(YEAR(service_start_dt), '-M',
#                           LPAD(MONTH(service_start_dt), 2, '0'))
#   period_start_dt     ← service_charge_schedule.service_start_dt
#   period_end_dt       ← service_charge_schedule.service_end_dt
#   scheduled_amt       ← service_charge_schedule.scheduled_amt
#   invoiced_amt        ← SUM(invoice_item.invoice_item_amt) via pre-agg view
#   variance_pct        ← (invoiced_amt - scheduled_amt) / NULLIF(scheduled_amt, 0)
#
# invoiced_amt derivation — IMPROVEMENT I-DP6-3:
#   service_charge_schedule has no invoiced_amt column directly in Silver.
#   The only available path is:
#     schedule.line_item_id
#       → line_item_accepted.line_item_id  (bridge: one LIA per CLIN per mod)
#       → invoice_item.cost_line_item_accepted_id
#       → SUM(invoice_item.invoice_item_amt)
#   Date boundary filter applied:
#     invoice_item_begin_dt >= schedule.service_start_dt
#     AND invoice_item_end_dt <= schedule.service_end_dt
#   This isolates invoices to the schedule period.  Pre-aggregated view
#   v_invoiced_per_schedule performs this aggregation to prevent fan-out
#   in the main INSERT.
#
# billing_period_label format: 'YYYY-Mmm' (e.g. '2025-M03')
#   Derived from service_start_dt calendar month.  NULL if service_start_dt NULL.
#
# Ref: FAR 52.232-7 (T&M payment schedule),
#      FAR 52.232-1 (payments under fixed-price contracts)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP6"
NOTEBOOK     = "prime_dim_billing_period"
TARGET_TABLE = "assist_dev.psc.dim_billing_period"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_service_charge_schedule")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_service_charge_schedule
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source service_charge_schedule rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — IMPROVEMENT I-DP6-3: pre-aggregate invoiced_amt per schedule
    #
    # Three-hop join with date-boundary filtering:
    #   (1) schedule.line_item_id → line_item_accepted.line_item_id
    #       MIN(lia.id) per line_item_id to pick the primary accepted instance
    #   (2) line_item_accepted.id → invoice_item.cost_line_item_accepted_id
    #   (3) Filter: invoice_item dates fall within the schedule period
    #   (4) SUM(invoice_item.invoice_item_amt) gives invoiced_amt per schedule
    #
    # Without pre-aggregation the main INSERT fans out on every invoice_item
    # line within each schedule period.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_invoiced_per_schedule AS
        SELECT
            scs.id                              AS schedule_id,
            SUM(COALESCE(ii.invoice_item_amt, 0.00)) AS invoiced_amt
        FROM {SILVER}.silver_aasbs_service_charge_schedule scs
        -- Bridge: schedule line_item_id → latest line_item_accepted per CLIN
        JOIN (
            SELECT
                line_item_id,
                MAX(id) AS lia_id
            FROM {SILVER}.silver_aasbs_line_item_accepted
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY line_item_id
        ) lia ON lia.line_item_id = scs.line_item_id
        -- Invoice items for this line item accepted instance
        -- Date filter: invoice period must fall within the billing schedule period
        JOIN {SILVER}.silver_aasbs_invoice_item ii
            ON  ii.cost_line_item_accepted_id = lia.lia_id
            AND COALESCE(ii.is_deleted, FALSE) = FALSE
            AND ii.invoice_item_begin_dt >= scs.service_start_dt
            AND ii.invoice_item_end_dt   <= scs.service_end_dt
        WHERE COALESCE(scs.is_deleted, FALSE) = FALSE
          AND scs.service_start_dt IS NOT NULL
          AND scs.service_end_dt   IS NOT NULL
        GROUP BY scs.id
    """)
    print(f"[{NOTEBOOK}] Invoiced amount pre-aggregation view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            schedule_id,
            billing_period_label,
            period_start_dt,
            period_end_dt,
            scheduled_amt,
            invoiced_amt,
            variance_pct,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            scs.id                                              AS schedule_id,

            -- billing_period_label: YYYY-Mmm derived from service_start_dt
            -- e.g. '2025-M03' for a period starting March 2025
            CASE
                WHEN scs.service_start_dt IS NOT NULL
                THEN CONCAT(
                    YEAR(scs.service_start_dt), '-M',
                    LPAD(CAST(MONTH(scs.service_start_dt) AS STRING), 2, '0')
                )
                ELSE NULL
            END                                                 AS billing_period_label,

            CAST(scs.service_start_dt AS DATE)                 AS period_start_dt,
            CAST(scs.service_end_dt   AS DATE)                 AS period_end_dt,

            -- Scheduled billing amount for this period (FAR 52.232-7)
            COALESCE(scs.scheduled_amt, 0.00)                  AS scheduled_amt,

            -- IMPROVEMENT I-DP6-3: invoiced_amt from pre-aggregated view
            -- SUM(invoice_item.invoice_item_amt) date-bounded to this period
            COALESCE(inv.invoiced_amt, 0.00)                   AS invoiced_amt,

            -- variance_pct = (invoiced - scheduled) / scheduled
            -- NULLIF guard prevents division by zero on zero-scheduled periods
            CASE
                WHEN COALESCE(scs.scheduled_amt, 0.00) <> 0.00
                THEN ROUND(
                    (COALESCE(inv.invoiced_amt, 0.00) - COALESCE(scs.scheduled_amt, 0.00))
                    / scs.scheduled_amt,
                    4
                )
                ELSE CAST(NULL AS DECIMAL(7, 4))
            END                                                 AS variance_pct,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_service_charge_schedule scs
        LEFT JOIN v_invoiced_per_schedule inv
            ON  inv.schedule_id = scs.id
        WHERE COALESCE(scs.is_deleted, FALSE) = FALSE
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
            COUNT(*)                                                             AS total_rows,
            SUM(CASE WHEN invoiced_amt > 0         THEN 1 ELSE 0 END)          AS has_invoiced,
            SUM(CASE WHEN variance_pct IS NOT NULL  THEN 1 ELSE 0 END)         AS has_variance,
            SUM(CASE WHEN variance_pct IS NULL
                     AND scheduled_amt = 0          THEN 1 ELSE 0 END)         AS zero_scheduled,
            ROUND(SUM(scheduled_amt),  2)                                       AS total_scheduled,
            ROUND(SUM(invoiced_amt),   2)                                       AS total_invoiced
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Coverage — total={stats[0]:,} | "
        f"has_invoiced={stats[1]:,} | has_variance={stats[2]:,} | "
        f"zero_scheduled={stats[3]:,}"
    )
    print(
        f"[{NOTEBOOK}] Totals — "
        f"scheduled=${stats[4]:,.2f} | invoiced=${stats[5]:,.2f}"
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
