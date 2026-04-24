# Databricks notebook source
# =============================================================================
# alt/prime_dim_closeout.py
# Primes assist_dev.alt.dim_closeout
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per acquisition_closeout.acquisition_id
# SCD Type : 1 — all attributes updated in-place
#
# Source tables (Silver):
#   silver_aasbs_acquisition_closeout           → closeout base record
#   silver_aasbs_acquisition_closeout_checklist → checklist item counts
#
# IMPROVEMENT #10 (v1.1.0) — safer checklist completion logic:
#   The checklist completion definition is changed from IS NOT NULL to an
#   explicit comparison against known completion indicator values from
#   acquisition_closeout_checklist.checklist_value_cd.
#
#   Problem with IS NOT NULL: if the source system stores 'PENDING', 'N/A',
#   or any non-null placeholder for an incomplete item, IS NOT NULL counts
#   those as complete.  This overstates checklist_completion_pct and could
#   incorrectly mark acquisitions as closeout-ready.
#
#   Fix: a checklist item is considered complete when checklist_value_cd
#   matches the known completion indicator ('COMPLETE').
#   The completion indicator set is defined as a constant at the top of the
#   notebook — update it if the source uses different codes.
#
#   Additionally, a post-load value distribution query is emitted so the
#   operator can inspect all distinct checklist_value_cd values and verify
#   the completion set is correct for the source data.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP2"
NOTEBOOK     = "prime_dim_closeout"
TARGET_TABLE = "assist_dev.alt.dim_closeout"

# ADDED for utils
TASK   = "prime_dim_closeout"
dbutils.widgets.text("job_name", "dp2_prime_full", "Job Name")
JOB_NAME = dbutils.widgets.get("job_name")

SILVER = "assist_dev.assist_finance"

# ── IMPROVEMENT #10: known completion indicator values ────────────────────────
# checklist_value_cd values that represent a completed checklist item.
# Update this set if the source system uses different codes.
# Common variants: 'COMPLETE', 'Y', 'YES', 'DONE', 'CLOSED'
COMPLETION_CODES = ("'COMPLETE'", "'Y'", "'YES'", "'DONE'", "'CLOSED'")
COMPLETION_CODES_SQL = ", ".join(COMPLETION_CODES)

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_acquisition_closeout")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(f"[{NOTEBOOK}] Completion indicator codes: {COMPLETION_CODES}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_acquisition_closeout"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver acquisition_closeout row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Pre-aggregate checklist counts per acquisition_closeout_id
    #
    # IMPROVEMENT #10: complete_items now uses explicit completion code
    # comparison rather than IS NOT NULL.
    #
    # Note: acquisition_closeout_checklist links via acquisition_closeout_id
    # FK (not acquisition_id directly).  We join via closeout.id below.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_checklist_agg AS
        SELECT
            cl.acquisition_closeout_id,
            COUNT(*)                                                    AS checklist_total_items,
            -- IMPROVEMENT #10: explicit completion code matching.
            -- Previously: IS NOT NULL (unsafe — counts any non-null placeholder).
            -- Now: compares against known completion indicator values.
            SUM(
                CASE WHEN UPPER(COALESCE(cl.checklist_value_cd, ''))
                         IN ({COMPLETION_CODES_SQL})
                     THEN 1 ELSE 0 END
            )                                                           AS checklist_complete_items,
            ROUND(
                100.0 * SUM(
                    CASE WHEN UPPER(COALESCE(cl.checklist_value_cd, ''))
                             IN ({COMPLETION_CODES_SQL})
                         THEN 1 ELSE 0 END
                ) / NULLIF(COUNT(*), 0),
                2
            )                                                           AS checklist_completion_pct
        FROM {SILVER}.silver_aasbs_acquisition_closeout_checklist cl
        WHERE COALESCE(cl.is_deleted, FALSE) = FALSE
        GROUP BY cl.acquisition_closeout_id
    """)
    print(f"[{NOTEBOOK}] Checklist aggregation complete (IMPROVEMENT #10 logic applied).")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            acquisition_id,
            co_cert_dt,
            fma_cert_dt,
            client_cert_dt,
            final_invoice_dt,
            closeout_complete_dt,
            checklist_total_items,
            checklist_complete_items,
            checklist_completion_pct,
            has_open_udo_flag,
            has_open_invoice_flag,
            closeout_prohibition_cds,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: acquisition_id on the closeout record
            ac.acquisition_id,

            -- Certification dates (CO, FMA, Client Agency — FAR 4.804 requirements)
            ac.co_checked_certified_dt                      AS co_cert_dt,
            ac.fma_certified_dt                             AS fma_cert_dt,
            -- client_cert_dt: not a direct column on acquisition_closeout in Silver DDL;
            -- sourced from checklist item or separate event in delta refresh.
            -- NULL on prime by design.
            CAST(NULL AS TIMESTAMP)                         AS client_cert_dt,

            -- Invoice and completion dates
            CAST(NULL AS TIMESTAMP)                         AS final_invoice_dt,
            CAST(NULL AS TIMESTAMP)                         AS closeout_complete_dt,

            -- Checklist stats (IMPROVEMENT #10: safe completion logic applied)
            COALESCE(cl.checklist_total_items,    0)        AS checklist_total_items,
            COALESCE(cl.checklist_complete_items, 0)        AS checklist_complete_items,
            COALESCE(cl.checklist_completion_pct, 0.00)     AS checklist_completion_pct,

            -- Open UDO and invoice prohibition flags
            -- contract_funds_reconciled_yn: 'Y' = no open UDO
            CASE WHEN UPPER(COALESCE(ac.contract_funds_reconciled_yn, 'N')) = 'N'
                 THEN TRUE ELSE FALSE END                   AS has_open_udo_flag,
            -- ytd_total_vendor_invoiced_amt > 0 used as open invoice proxy
            CASE WHEN COALESCE(ac.ytd_total_vendor_invoiced_amt, 0) > 0
                 THEN TRUE ELSE FALSE END                   AS has_open_invoice_flag,

            -- Prohibition codes: single code on closeout record
            ac.prohibition_cd                               AS closeout_prohibition_cds,

            current_timestamp()                             AS _gold_created_at,
            current_timestamp()                             AS _gold_updated_at,
            '{RUN_ID}'                                      AS _source_batch_id

        FROM {SILVER}.silver_aasbs_acquisition_closeout ac
        -- Join checklist via closeout.id = checklist.acquisition_closeout_id
        LEFT JOIN v_checklist_agg cl
            ON  cl.acquisition_closeout_id = ac.id
        WHERE COALESCE(ac.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    co_stats = spark.sql(f"""
        SELECT
            SUM(CASE WHEN co_cert_dt IS NOT NULL           THEN 1 ELSE 0 END) AS co_certified,
            SUM(CASE WHEN closeout_complete_dt IS NOT NULL THEN 1 ELSE 0 END) AS fully_closed,
            ROUND(AVG(checklist_completion_pct), 1)                           AS avg_completion_pct,
            SUM(CASE WHEN checklist_total_items > 0
                     AND checklist_complete_items = 0      THEN 1 ELSE 0 END) AS has_items_none_complete
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] CO certified     : {co_stats[0]:,} | "
        f"Fully closed: {co_stats[1]:,}"
    )
    print(
        f"[{NOTEBOOK}] Avg completion   : {co_stats[2]}%  "
        f"(IMPROVEMENT #10 — based on explicit completion codes)"
    )
    print(
        f"[{NOTEBOOK}] Items w/no complete : {co_stats[3]:,}  "
        f"(acquisitions with checklist rows but 0 matching completion codes)"
    )

    # ─────────────────────────────────────────────────────────────────────
    # IMPROVEMENT #10: checklist_value_cd distribution check
    #
    # Prints all distinct checklist_value_cd values from Silver.
    # Operator should verify that COMPLETION_CODES above covers all codes
    # that indicate a completed item in the source system.
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n[{NOTEBOOK}] IMPROVEMENT #10 — checklist_value_cd distribution in Silver:")
    value_dist = spark.sql(f"""
        SELECT
            checklist_value_cd,
            COUNT(*) AS cnt
        FROM {SILVER}.silver_aasbs_acquisition_closeout_checklist
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY checklist_value_cd
        ORDER BY cnt DESC
        LIMIT 20
    """).collect()

    for row in value_dist:
        code = row[0] if row[0] is not None else "(NULL)"
        is_complete = "✓ counted as COMPLETE" if (
            row[0] and row[0].upper() in [c.strip("'") for c in COMPLETION_CODES]
        ) else "✗ NOT counted as complete"
        print(f"  {code:<25}  {row[1]:>8,}  {is_complete}")

    if co_stats[3] > 0:
        print(
            f"\n  ⚠ {co_stats[3]:,} acquisitions have checklist items with no matching "
            f"completion code. Review the distribution above and update COMPLETION_CODES "
            f"in this notebook if required."
        )
    else:
        print(f"\n  ✓ All acquisitions with checklist items have at least one complete item.")
    #n = row_count(spark, TARGET_TABLE)
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
