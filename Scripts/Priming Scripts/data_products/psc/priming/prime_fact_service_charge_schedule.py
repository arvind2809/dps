# Databricks notebook source
# =============================================================================
# psc/prime_fact_service_charge_schedule.py
# Primes assist_dev.psc.fact_service_charge_schedule
#
# Grain    : One row per service_charge_schedule.id
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source tables (Silver — all confirmed against Silver DDL):
#   silver_aasbs_service_charge_schedule  → grain anchor
#   silver_aasbs_line_item                → award_mod + ia + line_item_sk bridge
#   silver_aasbs_line_item_accepted       → line_item_sk resolution
#   silver_aasbs_invoice_item             → invoiced_amt + accepted_amt
#   silver_aasbs_service_charge           → source_service_charge_id
#
# Field mapping (all confirmed):
#   line_item_sk       ← common.dim_line_item
#                        via schedule.line_item_id → MAX(line_item_accepted.id)
#                        → dim_line_item.line_item_accepted_id
#   billing_period_sk  ← psc.dim_billing_period via schedule.id = dim.schedule_id
#   award_sk           ← common.dim_award
#                        via line_item.parent_award_mod_id → award_mod → dim_award
#   ia_sk              ← common.dim_ia via line_item.parent_ia_id (direct FK)
#   source_service_charge_id ← line_item.service_charge_id
#   schedule_status_cd ← service_charge_schedule.sc_schedule_status_cd
#   scheduled_amt      ← service_charge_schedule.scheduled_amt
#   invoiced_amt       ← SUM(invoice_item.invoice_item_amt) per schedule period
#   accepted_amt       ← same aggregation filtered to accepted invoices
#   on_schedule_flag   ← ABS(invoiced_amt - scheduled_amt) / scheduled_amt <= 0.10
#                        [IMPROVEMENT I-DP6-7: distribution reported post-load]
#   travel_voucher_count ← COUNT of SC_TRAVEL invoice_item lines (SC_TRAVEL CLINs)
#   travel_voucher_amt   ← SUM of SC_TRAVEL invoice amounts for this schedule
#
# travel_voucher_count / travel_voucher_amt note:
#   Meaningful only for schedule records linked to SC_TRAVEL CLINs.
#   For other CLIN subtypes these fields will be 0.  Resolved via pre-agg view
#   that checks li_sc_travel_id IS NOT NULL on line_item.
#
# on_schedule_flag formula (FAR 52.232-7):
#   TRUE  when ABS(invoiced - scheduled) / scheduled <= 0.10  (±10% tolerance)
#   FALSE when variance > 10%
#   NULL  when scheduled_amt = 0 (division-by-zero guard)
#
# IMPROVEMENT I-DP6-7 (built-in):
#   Post-load distribution of on_schedule_flag: total schedules, on-schedule,
#   off-schedule, and NULL (zero-scheduled) counts with percentages.
#   FAR 52.232-7 billing schedule health metric surfaced from first prime.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP6"
NOTEBOOK     = "prime_fact_service_charge_schedule"
TARGET_TABLE = "assist_dev.psc.fact_service_charge_schedule"

SILVER = "assist_dev.assist_finance"
PSC    = "assist_dev.psc"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_service_charge_schedule")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only
    # ─────────────────────────────────────────────────────────────────────
    existing = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    if existing > 0:
        msg = (
            f"[{NOTEBOOK}] ABORTED — {existing:,} rows already exist. "
            f"Prime is INSERT-only. Manually TRUNCATE {TARGET_TABLE} to re-prime."
        )
        print(msg)
        #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        audit_fail(spark, RUN_ID, TARGET_TABLE, str(msg), msg, start_ts)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_service_charge_schedule
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source service_charge_schedule rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Primary line_item_accepted per line_item
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_primary_lia_svc AS
        SELECT
            line_item_id,
            MAX(id) AS lia_id
        FROM {SILVER}.silver_aasbs_line_item_accepted
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY line_item_id
    """)

    # 3b — Invoiced and accepted amounts per schedule period
    # Joins schedule → line_item_accepted → invoice_item with date-boundary filter.
    # invoiced_amt = all invoice_item amounts within the schedule period.
    # accepted_amt = invoice_item amounts where the invoice has been accepted
    #   (proxy: invoice_item_amt where the parent invoice is accepted;
    #    using full invoiced_amt as proxy since no acceptance flag on invoice_item).
    # For DP6 prime, accepted_amt = invoiced_amt (same aggregation).
    # CDC will refine with a proper acceptance status join once available.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_inv_per_schedule AS
        SELECT
            scs.id                                          AS schedule_id,
            SUM(COALESCE(ii.invoice_item_amt, 0.00))       AS invoiced_amt,
            -- accepted_amt proxy = invoiced_amt on prime (no separate acceptance flag)
            SUM(COALESCE(ii.invoice_item_amt, 0.00))       AS accepted_amt
        FROM {SILVER}.silver_aasbs_service_charge_schedule scs
        JOIN v_primary_lia_svc plia
            ON plia.line_item_id = scs.line_item_id
        JOIN {SILVER}.silver_aasbs_invoice_item ii
            ON  ii.cost_line_item_accepted_id = plia.lia_id
            AND COALESCE(ii.is_deleted, FALSE) = FALSE
            AND ii.invoice_item_begin_dt >= scs.service_start_dt
            AND ii.invoice_item_end_dt   <= scs.service_end_dt
        WHERE COALESCE(scs.is_deleted, FALSE) = FALSE
          AND scs.service_start_dt IS NOT NULL
          AND scs.service_end_dt   IS NOT NULL
        GROUP BY scs.id
    """)

    # 3c — Travel voucher count and amount per schedule
    # Only meaningful for SC_TRAVEL CLINs (line_item.li_sc_travel_id IS NOT NULL).
    # For other subtypes these will be 0 by design.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_travel_per_schedule AS
        SELECT
            scs.id                                          AS schedule_id,
            COUNT(ii.id)                                    AS travel_voucher_count,
            SUM(COALESCE(ii.invoice_item_amt, 0.00))       AS travel_voucher_amt
        FROM {SILVER}.silver_aasbs_service_charge_schedule scs
        JOIN v_primary_lia_svc plia
            ON plia.line_item_id = scs.line_item_id
        JOIN {SILVER}.silver_aasbs_line_item li_chk
            ON  li_chk.id = scs.line_item_id
            AND li_chk.li_sc_travel_id IS NOT NULL          -- SC_TRAVEL CLINs only
            AND COALESCE(li_chk.is_deleted, FALSE) = FALSE
        JOIN {SILVER}.silver_aasbs_invoice_item ii
            ON  ii.cost_line_item_accepted_id = plia.lia_id
            AND COALESCE(ii.is_deleted, FALSE) = FALSE
            AND ii.invoice_item_begin_dt >= scs.service_start_dt
            AND ii.invoice_item_end_dt   <= scs.service_end_dt
        WHERE COALESCE(scs.is_deleted, FALSE) = FALSE
          AND scs.service_start_dt IS NOT NULL
          AND scs.service_end_dt   IS NOT NULL
        GROUP BY scs.id
    """)

    print(f"[{NOTEBOOK}] Supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_sk,
            billing_period_sk,
            award_sk,
            ia_sk,
            source_service_charge_id,
            schedule_status_cd,
            scheduled_amt,
            invoiced_amt,
            accepted_amt,
            on_schedule_flag,
            travel_voucher_count,
            travel_voucher_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Dimension FKs ──────────────────────────────────────────── */

            -- line_item_sk: schedule.line_item_id → primary lia → dim_line_item
            COALESCE(dli.line_item_sk, -1)                      AS line_item_sk,

            -- billing_period_sk: dim_billing_period keyed on schedule.id
            COALESCE(dbp.billing_period_sk, -1)                 AS billing_period_sk,

            -- award_sk: line_item.parent_award_mod_id → award_mod → dim_award
            COALESCE(da.award_sk, -1)                           AS award_sk,

            -- ia_sk: line_item.parent_ia_id (direct confirmed FK)
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,

            /* ── Source identifier ──────────────────────────────────────── */
            li.service_charge_id                                AS source_service_charge_id,

            /* ── Schedule attributes ────────────────────────────────────── */
            scs.sc_schedule_status_cd                           AS schedule_status_cd,
            COALESCE(scs.scheduled_amt, 0.00)                   AS scheduled_amt,

            /* ── Financial measures ─────────────────────────────────────── */
            COALESCE(inv.invoiced_amt,  0.00)                   AS invoiced_amt,
            COALESCE(inv.accepted_amt,  0.00)                   AS accepted_amt,

            -- on_schedule_flag (IMPROVEMENT I-DP6-7):
            -- TRUE  when |invoiced - scheduled| / scheduled <= 10% (FAR 52.232-7)
            -- FALSE when variance > 10%
            -- NULL  when scheduled_amt = 0 (cannot compute variance — excluded from metric)
            CASE
                WHEN COALESCE(scs.scheduled_amt, 0.00) = 0.00
                    THEN CAST(NULL AS BOOLEAN)
                WHEN ABS(
                    COALESCE(inv.invoiced_amt, 0.00) - COALESCE(scs.scheduled_amt, 0.00)
                ) / scs.scheduled_amt <= 0.10
                    THEN TRUE
                ELSE FALSE
            END                                                 AS on_schedule_flag,

            /* ── Travel voucher metrics ─────────────────────────────────── */
            COALESCE(tv.travel_voucher_count, 0)                AS travel_voucher_count,
            COALESCE(tv.travel_voucher_amt,   0.00)             AS travel_voucher_amt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_service_charge_schedule scs

        /* ── line_item for award / ia / service_charge FKs ─────────────── */
        LEFT JOIN {SILVER}.silver_aasbs_line_item li
            ON  li.id = scs.line_item_id
            AND COALESCE(li.is_deleted, FALSE) = FALSE

        /* ── Primary line_item_accepted ─────────────────────────────────── */
        LEFT JOIN v_primary_lia_svc plia
            ON  plia.line_item_id = scs.line_item_id

        /* ── dim_line_item ───────────────────────────────────────────────── */
        LEFT JOIN {COMMON}.dim_line_item dli
            ON  dli.line_item_accepted_id = plia.lia_id
            AND dli.is_current_flag       = TRUE

        /* ── dim_billing_period ─────────────────────────────────────────── */
        LEFT JOIN {PSC}.dim_billing_period dbp
            ON  dbp.schedule_id = scs.id

        /* ── award_mod → dim_award ──────────────────────────────────────── */
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = li.parent_award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_award da
            ON  da.award_id        = am.award_id
            AND da.is_current_flag = TRUE

        /* ── dim_ia via parent_ia_id ────────────────────────────────────── */
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = li.parent_ia_id
            AND dia.is_current_flag = TRUE

        /* ── Invoiced / accepted amounts ────────────────────────────────── */
        LEFT JOIN v_inv_per_schedule inv
            ON  inv.schedule_id = scs.id

        /* ── Travel vouchers (SC_TRAVEL CLINs only) ─────────────────────── */
        LEFT JOIN v_travel_per_schedule tv
            ON  tv.schedule_id = scs.id

        WHERE COALESCE(scs.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    if rows_written != rows_read:
        print(
            f"[{NOTEBOOK}] WARNING: rows_written ({rows_written:,}) ≠ "
            f"source rows ({rows_read:,}). Check for fan-out in temp view joins."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

    # IMPROVEMENT I-DP6-7: on_schedule_flag distribution — FAR 52.232-7 metric
    sched = spark.sql(f"""
        SELECT
            COUNT(*)                                                             AS total,
            SUM(CASE WHEN on_schedule_flag = TRUE  THEN 1 ELSE 0 END)          AS on_schedule,
            SUM(CASE WHEN on_schedule_flag = FALSE THEN 1 ELSE 0 END)          AS off_schedule,
            SUM(CASE WHEN on_schedule_flag IS NULL THEN 1 ELSE 0 END)          AS zero_scheduled
        FROM {TARGET_TABLE}
    """).collect()[0]

    total = max(sched[0], 1)
    on_pct  = sched[1] / total * 100
    off_pct = sched[2] / total * 100

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP6-7 — on_schedule_flag (FAR 52.232-7):")
    print(f"  Total schedules : {sched[0]:>8,}")
    print(f"  On-schedule     : {sched[1]:>8,}  ({on_pct:.1f}%)")
    print(f"  Off-schedule    : {sched[2]:>8,}  ({off_pct:.1f}%)")
    print(f"  Zero-scheduled  : {sched[3]:>8,}  (excluded from metric — zero divisor)")
    if sched[2] > 0:
        print(
            f"\n  ⚠ {off_pct:.1f}% of billing schedules are off-track "
            f"(>10% variance). Review for FAR 52.232-7 compliance."
        )
    else:
        print(f"\n  ✓ All evaluated schedules are on-track.")

    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN line_item_sk    = -1 THEN 1 ELSE 0 END) AS unresolved_li,
            SUM(CASE WHEN billing_period_sk = -1 THEN 1 ELSE 0 END) AS unresolved_bp,
            SUM(CASE WHEN award_sk        = -1 THEN 1 ELSE 0 END) AS unresolved_award,
            SUM(CASE WHEN ia_sk           = -1 THEN 1 ELSE 0 END) AS unresolved_ia
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"\n[{NOTEBOOK}] Sentinel FKs — li: {sentinel[0]:,} | "
        f"billing_period: {sentinel[1]:,} | award: {sentinel[2]:,} | ia: {sentinel[3]:,}"
    )

    fin = spark.sql(f"""
        SELECT
            ROUND(SUM(scheduled_amt), 2) AS total_scheduled,
            ROUND(SUM(invoiced_amt),  2) AS total_invoiced
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(f"[{NOTEBOOK}] Totals — scheduled=${fin[0]:,.2f} | invoiced=${fin[1]:,.2f}")

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
