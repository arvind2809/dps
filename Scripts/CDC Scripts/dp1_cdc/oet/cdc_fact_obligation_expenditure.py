# Databricks notebook source
# =============================================================================
# oet/cdc_fact_obligation_expenditure.py
# CDC (delta refresh) for assist_catalog.oet.fact_obligation_expenditure
#
# Strategy        : APPEND — insert new snapshot rows for all affected grains.
#                   Consumers read the latest snapshot via the view
#                   oet.v_clin_expenditure_detail which filters to MAX(snapshot_date_sk).
#
# Changed grain   : any (line_item_id × loa_id) pair where loa_ledger, invoice_item,
#                   acceptance_item, or billing has changed since watermark_from.
#                   For each affected pair, ALL ledger rows are re-aggregated (not
#                   just the changed ones) to produce an accurate current total.
#
# accrued_amt     : Conditionally populated.
#                   If DP4 has a SUCCESS run with watermark_to > our watermark_from,
#                   join to silver_aasbs_accrual_income for the affected grains.
#                   Otherwise seed as 0.00 (same as prime).
#
# Partial tolerance: This notebook runs if at least dim_loa AND dim_line_item
#                   succeeded. Unresolvable FKs use sentinel -1.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("oet", "fact_obligation_expenditure")
TASK     = "cdc_fact_obligation_expenditure"

# Silver sources
S_LEDGER = silver("aasbs", "loa_ledger")
S_INV    = silver("aasbs", "invoice_item")
S_ACC    = silver("aasbs", "acceptance_item")
S_LIA    = silver("aasbs", "line_item_accepted")
S_LOA    = silver("aasbs", "loa")
S_BILL   = silver("billing", "billing")
# Accrual source — used conditionally if DP4 has run
S_ACCR   = silver("aasbs", "accrual_income")

# Gold dimensions
G_LI     = gold("common", "dim_line_item")
G_LOA    = gold("common", "dim_loa")
G_FUND   = gold("common", "dim_funding")
G_AWARD  = gold("common", "dim_award")
G_IA     = gold("common", "dim_ia")
G_AGENCY = gold("common", "dim_agency")
G_DATE   = gold("common", "dim_date")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

# ── Check DP4 availability for accrued_amt ────────────────────────────────────
dp4_available = dp4_ran_since(spark, watermark_from)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "loa_ledger", watermark_from, watermark_to)

# COMMAND ----------
try:
    # ── Step 1: Identify affected (line_item_accepted_id, loa_id) grain pairs ─
    # A grain is "affected" if ANY of its contributing source rows changed.
    # Sources: loa_ledger, invoice_item, acceptance_item, billing (via agreement_number)

    affected_grains_sql = f"""
        CREATE OR REPLACE TEMP VIEW v_affected_grains AS
        -- Changed via loa_ledger
        SELECT DISTINCT ll.line_item_accepted_id, ll.loa_id, ll.funding_id, ll.ia_id
        FROM {S_LEDGER} ll
        WHERE ll.is_deleted = FALSE
          AND ll.line_item_accepted_id IS NOT NULL
          AND {wm_filter}

        UNION

        -- Changed via invoice_item → line_item_accepted
        SELECT DISTINCT ll.line_item_accepted_id, ll.loa_id, ll.funding_id, ll.ia_id
        FROM {S_LEDGER} ll
        JOIN (
            SELECT DISTINCT cost_line_item_accepted_id AS line_item_accepted_id
            FROM {S_INV}
            WHERE is_deleted = FALSE AND {wm_filter}
        ) chg_inv ON chg_inv.line_item_accepted_id = ll.line_item_accepted_id
        WHERE ll.is_deleted = FALSE AND ll.line_item_accepted_id IS NOT NULL

        UNION

        -- Changed via acceptance_item → line_item_accepted
        SELECT DISTINCT ll.line_item_accepted_id, ll.loa_id, ll.funding_id, ll.ia_id
        FROM {S_LEDGER} ll
        JOIN (
            SELECT DISTINCT line_item_accepted_id
            FROM {S_ACC}
            WHERE is_deleted = FALSE AND {wm_filter}
        ) chg_acc ON chg_acc.line_item_accepted_id = ll.line_item_accepted_id
        WHERE ll.is_deleted = FALSE AND ll.line_item_accepted_id IS NOT NULL

        UNION

        -- Changed via billing → agreement_number → loa
        SELECT DISTINCT ll.line_item_accepted_id, ll.loa_id, ll.funding_id, ll.ia_id
        FROM {S_LEDGER} ll
        JOIN {S_LOA} loa ON loa.id = ll.loa_id AND loa.is_deleted = FALSE
        JOIN (
            SELECT DISTINCT agreement_num
            FROM {silver("billing","billing")}
            WHERE is_deleted = FALSE AND {wm_filter}
        ) chg_bill ON chg_bill.agreement_num = loa.agreement_number
        WHERE ll.is_deleted = FALSE AND ll.line_item_accepted_id IS NOT NULL
    """
    spark.sql(affected_grains_sql)

    grains_count = spark.sql("SELECT COUNT(*) AS n FROM v_affected_grains").collect()[0]["n"]
    print(f"  [DETECT] {grains_count:,} affected (line_item_accepted_id × loa_id) grains")

    if grains_count == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    # ── Step 2: Re-aggregate ALL ledger rows for affected grains ──────────────
    # IMPORTANT: We aggregate ALL rows for the affected grain (not just changed ones)
    # to produce an accurate running total as of the snapshot timestamp.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ledger_agg AS
        SELECT
            ll.line_item_accepted_id,
            ll.loa_id,
            ll.funding_id,
            ll.ia_id,
            ll.transaction_type_cd,
            CASE WHEN MONTH(ll.created_dt) >= 10
                 THEN YEAR(ll.created_dt) + 1
                 ELSE YEAR(ll.created_dt) END                   AS fiscal_year,
            CASE WHEN MONTH(ll.created_dt) IN (10,11,12) THEN 1
                 WHEN MONTH(ll.created_dt) IN (1,2,3)  THEN 2
                 WHEN MONTH(ll.created_dt) IN (4,5,6)  THEN 3
                 ELSE 4 END                                     AS fiscal_quarter,
            CASE WHEN MONTH(ll.created_dt) >= 10
                 THEN MONTH(ll.created_dt) - 9
                 ELSE MONTH(ll.created_dt) + 3 END              AS fiscal_month,
            CAST(MAX(ll.created_dt) AS DATE)                    AS snapshot_date,
            SUM(ll.line_item_committed_amt)                     AS committed_amt,
            SUM(ll.line_item_certified_amt)                     AS certified_amt,
            SUM(ll.line_item_obligated_amt)                     AS obligated_amt,
            SUM(ll.service_charge_amt)                          AS service_charge_amt,
            CONCAT_WS(',', COLLECT_LIST(CAST(ll.id AS STRING))) AS source_loa_ledger_ids
        FROM {S_LEDGER} ll
        JOIN v_affected_grains ag
            ON  ag.line_item_accepted_id = ll.line_item_accepted_id
            AND ag.loa_id                = ll.loa_id
        WHERE ll.is_deleted = FALSE
        GROUP BY
            ll.line_item_accepted_id, ll.loa_id, ll.funding_id, ll.ia_id,
            ll.transaction_type_cd,
            CASE WHEN MONTH(ll.created_dt)>=10 THEN YEAR(ll.created_dt)+1 ELSE YEAR(ll.created_dt) END,
            CASE WHEN MONTH(ll.created_dt) IN (10,11,12) THEN 1
                 WHEN MONTH(ll.created_dt) IN (1,2,3) THEN 2
                 WHEN MONTH(ll.created_dt) IN (4,5,6) THEN 3 ELSE 4 END,
            CASE WHEN MONTH(ll.created_dt)>=10 THEN MONTH(ll.created_dt)-9 ELSE MONTH(ll.created_dt)+3 END
    """)

    # ── Step 3: Re-aggregate invoice and acceptance for affected grains ────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_invoice_agg AS
        SELECT
            ii.cost_line_item_accepted_id AS line_item_accepted_id,
            SUM(ii.invoice_item_amt)      AS invoiced_amt,
            COUNT(DISTINCT ii.invoice_id) AS invoice_count
        FROM {S_INV} ii
        JOIN v_affected_grains ag ON ag.line_item_accepted_id = ii.cost_line_item_accepted_id
        WHERE ii.is_deleted = FALSE AND ii.cost_line_item_accepted_id IS NOT NULL
        GROUP BY ii.cost_line_item_accepted_id
    """)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_acceptance_agg AS
        SELECT
            ai.line_item_accepted_id,
            SUM(ai.item_gsa_approved_amt)     AS accepted_amt,
            COUNT(DISTINCT ai.acceptance_id)  AS acceptance_count
        FROM {S_ACC} ai
        JOIN v_affected_grains ag ON ag.line_item_accepted_id = ai.line_item_accepted_id
        WHERE ai.is_deleted = FALSE AND ai.line_item_accepted_id IS NOT NULL
        GROUP BY ai.line_item_accepted_id
    """)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_billing_agg AS
        SELECT
            b.agreement_num                 AS agreement_number,
            SUM(b.billing_amount)           AS billed_amt,
            SUM(CASE WHEN COALESCE(b.credit_indicator,'N') = 'N'
                     THEN b.billing_amount ELSE 0 END) AS disbursed_amt
        FROM {S_BILL} b
        JOIN {S_LOA} loa ON loa.agreement_number = b.agreement_num AND loa.is_deleted = FALSE
        JOIN v_affected_grains ag ON ag.loa_id = loa.id
        WHERE b.is_deleted = FALSE
        GROUP BY b.agreement_num
    """)

    # ── Step 4: Accrual amounts — conditional on DP4 availability ─────────────
    if dp4_available:
        print("  [DP4] DP4 data available — joining accrual_income for accrued_amt")
        # silver_aasbs_accrual_income links to line_item_accepted_id
        # Table may not exist if DP4 has never been primed; guard with try/except
        try:
            spark.sql(f"""
                CREATE OR REPLACE TEMP VIEW v_accrual_agg AS
                SELECT
                    ai.line_item_accepted_id,
                    SUM(ai.calculated_income_amt) AS accrued_amt
                FROM {S_ACCR} ai
                JOIN v_affected_grains ag ON ag.line_item_accepted_id = ai.line_item_accepted_id
                WHERE ai.is_deleted = FALSE
                GROUP BY ai.line_item_accepted_id
            """)
            accrual_join = f"""
                LEFT JOIN v_accrual_agg acr
                    ON acr.line_item_accepted_id = la.line_item_accepted_id
            """
            accrual_col = "COALESCE(acr.accrued_amt, 0.00)"
        except Exception as acr_err:
            print(f"  [DP4] Accrual table join failed ({acr_err}) — seeding accrued_amt=0")
            accrual_join = ""
            accrual_col  = "CAST(0.00 AS DECIMAL(15,2))"
    else:
        print("  [DP4] DP4 not yet run for this period — accrued_amt=0.00")
        accrual_join = ""
        accrual_col  = "CAST(0.00 AS DECIMAL(15,2))"

    # ── Step 5: Combine and resolve SKs ───────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_fact_cdc_payload AS
        WITH combined AS (
            SELECT
                la.line_item_accepted_id,
                la.loa_id,
                la.funding_id,
                la.ia_id,
                la.transaction_type_cd,
                la.fiscal_year,
                la.fiscal_quarter,
                la.fiscal_month,
                la.snapshot_date,
                la.committed_amt,
                la.certified_amt,
                la.obligated_amt,
                la.service_charge_amt,
                la.source_loa_ledger_ids,
                lia.line_item_id,
                lia.budget_fy,
                loa_ref.agreement_number                AS loa_agreement_number,
                COALESCE(inv.invoiced_amt,   0.00)      AS invoiced_amt,
                COALESCE(inv.invoice_count,  0)         AS invoice_count,
                COALESCE(acc.accepted_amt,   0.00)      AS accepted_amt,
                COALESCE(acc.acceptance_count, 0)       AS acceptance_count,
                COALESCE(bill.billed_amt,    0.00)      AS billed_amt,
                COALESCE(bill.disbursed_amt, 0.00)      AS disbursed_amt,
                {accrual_col}                           AS accrued_amt
            FROM v_ledger_agg la
            LEFT JOIN {S_LIA} lia
                ON  lia.id = la.line_item_accepted_id AND lia.is_deleted = FALSE
            LEFT JOIN {S_LOA} loa_ref
                ON  loa_ref.id = la.loa_id AND loa_ref.is_deleted = FALSE
            LEFT JOIN v_invoice_agg    inv  ON inv.line_item_accepted_id = la.line_item_accepted_id
            LEFT JOIN v_acceptance_agg acc  ON acc.line_item_accepted_id = la.line_item_accepted_id
            LEFT JOIN v_billing_agg    bill ON bill.agreement_number = loa_ref.agreement_number
            {accrual_join}
        )
        SELECT
            -- Dimension FKs
            li.line_item_sk,
            COALESCE(dl.loa_sk,    -1)  AS loa_sk,
            COALESCE(df.funding_sk,-1)  AS funding_sk,
            COALESCE(da.award_sk,  -1)  AS award_sk,
            COALESCE(ia.ia_sk,     -1)  AS ia_sk,
            COALESCE(ag.agency_sk, -1)  AS agency_sk,
            dd.date_sk                  AS snapshot_date_sk,
            -- Fiscal period
            c.fiscal_year,
            c.fiscal_quarter,
            c.fiscal_month,
            c.budget_fy,
            -- Financial measures
            COALESCE(c.committed_amt,   0.00)   AS committed_amt,
            COALESCE(c.certified_amt,   0.00)   AS certified_amt,
            COALESCE(c.obligated_amt,   0.00)   AS obligated_amt,
            COALESCE(c.invoiced_amt,    0.00)   AS invoiced_amt,
            COALESCE(c.accepted_amt,    0.00)   AS accepted_amt,
            COALESCE(c.billed_amt,      0.00)   AS billed_amt,
            c.accrued_amt,
            -- Derived
            COALESCE(c.obligated_amt,0.00) - COALESCE(c.accepted_amt,0.00) AS udo_amt,
            COALESCE(c.service_charge_amt,0.00)                            AS service_charge_amt,
            COALESCE(c.disbursed_amt,   0.00)   AS disbursed_amt,
            COALESCE(c.obligated_amt,0.00) - COALESCE(c.invoiced_amt,0.00) AS undelivered_orders_amt,
            c.transaction_type_cd               AS loa_transaction_type_cd,
            c.invoice_count,
            c.acceptance_count,
            c.line_item_id                      AS source_line_item_id,
            c.source_loa_ledger_ids
        FROM combined c
        LEFT JOIN {G_LI}     li ON li.line_item_id = c.line_item_id AND li.is_current_flag = TRUE
        LEFT JOIN {G_LOA}    dl ON dl.loa_id        = c.loa_id       AND dl.is_current_flag = TRUE
        LEFT JOIN {G_FUND}   df ON df.funding_id    = c.funding_id   AND df.is_current_flag = TRUE
        LEFT JOIN {G_AWARD}  da ON da.award_sk       = li.award_sk    AND da.is_current_flag = TRUE
        LEFT JOIN {G_IA}     ia ON ia.ia_id          = c.ia_id        AND ia.is_current_flag = TRUE
        LEFT JOIN {G_AGENCY} ag ON ag.agency_sk      = ia.servicing_agency_sk AND ag.is_current_flag = TRUE
        LEFT JOIN {G_DATE}   dd ON dd.date_sk        = CAST(DATE_FORMAT(c.snapshot_date,'yyyyMMdd') AS INT)
        WHERE NOT (COALESCE(c.obligated_amt,0)=0 AND COALESCE(c.accepted_amt,0)=0
                   AND COALESCE(c.invoiced_amt,0)=0)
    """)

    # ── Step 6: APPEND new snapshot rows ──────────────────────────────────────
    # Consumers deduplicate via MAX(snapshot_date_sk) per grain in views.
    spark.sql(f"""
        INSERT INTO {TARGET}
        (line_item_sk, loa_sk, funding_sk, award_sk, ia_sk, agency_sk, snapshot_date_sk,
         fiscal_year, fiscal_quarter, fiscal_month, budget_fy,
         committed_amt, certified_amt, obligated_amt,
         invoiced_amt, accepted_amt, billed_amt,
         accrued_amt, udo_amt, service_charge_amt,
         disbursed_amt, undelivered_orders_amt,
         loa_transaction_type_cd, invoice_count, acceptance_count,
         source_line_item_id, source_loa_ledger_ids,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            line_item_sk, loa_sk, funding_sk, award_sk, ia_sk, agency_sk, snapshot_date_sk,
            fiscal_year, fiscal_quarter, fiscal_month, budget_fy,
            committed_amt, certified_amt, obligated_amt,
            invoiced_amt, accepted_amt, billed_amt,
            accrued_amt, udo_amt, service_charge_amt,
            disbursed_amt, undelivered_orders_amt,
            loa_transaction_type_cd, invoice_count, acceptance_count,
            source_line_item_id, source_loa_ledger_ids,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_fact_cdc_payload
    """)

    # ── Step 7: Quality checks ────────────────────────────────────────────────
    n = row_count(spark, TARGET)
    this_run = spark.sql(f"""
        SELECT
            COUNT(*)                                            AS rows_appended,
            SUM(CASE WHEN line_item_sk IS NULL  THEN 1 ELSE 0 END) AS null_li_sk,
            SUM(CASE WHEN loa_sk = -1           THEN 1 ELSE 0 END) AS sentinel_loa,
            SUM(CASE WHEN udo_amt < 0           THEN 1 ELSE 0 END) AS negative_udo,
            ROUND(SUM(obligated_amt), 2)                        AS run_obligated
        FROM {TARGET}
        WHERE _source_batch_id = '{RUN_ID}'
    """).collect()[0]

    rows_appended = this_run["rows_appended"]
    print(f"  [OK] Appended {rows_appended:,} new snapshot rows (total in table: {n:,})")
    print(f"  Quality — null_li_sk={this_run['null_li_sk']} sentinel_loa={this_run['sentinel_loa']} "
          f"neg_udo={this_run['negative_udo']} run_obligated=${this_run['run_obligated']:,.2f}")
    print(f"  DP4 accrued_amt populated: {dp4_available}")

    audit_success_cdc(spark, RUN_ID, TARGET,
                      grains_count, rows_appended,
                      rows_appended, 0, start_ts, watermark_to)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
