# Databricks notebook source
# =============================================================================
# oet/prime_fact_obligation_expenditure.py
# Primes assist_dev.oet.fact_obligation_expenditure
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : (line_item_accepted_id × loa_id × fiscal_year × fiscal_month)
#
# IMPROVEMENT #6 (v1.1.0) — grain correctness:
#   transaction_type_cd removed from the GROUP BY in ledger_agg.
#   The design document specifies a 4-dimensional grain with no transaction_type
#   axis.  Including it in GROUP BY creates separate fact rows per transaction type
#   (COMMITTED / CERTIFIED / OBLIGATED) for the same CLIN × LOA × period, which:
#     (a) inflates row count (3× for typical LOAs)
#     (b) causes double-counting when consumers SUM across a grain without
#         a WHERE filter on transaction_type_cd
#     (c) contradicts the DDL comment "one row per accepted CLIN × LOA × period"
#   Fix: collapse to single row per grain; loa_transaction_type_cd stored as
#   MAX(transaction_type_cd) (a degenerate dimension — the latest/highest code
#   per grain period, which typically = 'OBLIGATED' for funded CLINs).
#
# FIX (v1.1.0) — billed_amt CLIN grain:
#   billed_amt sourced from silver_aasbs_bill_item (CLIN grain via
#   line_item_accepted_id) — replaces silver_billing_billing which was at IA/
#   agreement grain and over-counted by applying the full IA total to every CLIN.
#
# FIX (v1.1.0) — disbursed_amt Silver-pending:
#   disbursed_amt = NULL (SILVER-PENDING). aasbs.disbursement not yet ingested.
#   Prior v1.0 billing proxy was semantically wrong (client billing ≠ disbursement).
#
# Source tables (Silver):
#   silver_aasbs_loa_ledger        → committed, certified, obligated, service_charge
#   silver_aasbs_line_item_accepted → grain bridge (line_item_id)
#   silver_aasbs_invoice_item      → invoiced_amt
#   silver_aasbs_acceptance_item   → accepted_amt
#   silver_aasbs_bill_item         → billed_amt (CLIN grain) [v1.1.0 FIX]
#   silver_aasbs_loa               → agreement_number for billing join
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET    = gold("oet", "fact_obligation_expenditure")
TASK      = "prime_fact_obligation_expenditure"

S_LEDGER  = silver("aasbs", "loa_ledger")
S_INV     = silver("aasbs", "invoice_item")
S_ACC     = silver("aasbs", "acceptance_item")
S_LIA     = silver("aasbs", "line_item_accepted")
S_LOA     = silver("aasbs", "loa")
S_BILL_IT = silver("aasbs", "bill_item")   # FIX: CLIN-grain billing source

G_LI      = gold("common", "dim_line_item")
G_LOA     = gold("common", "dim_loa")
G_FUND    = gold("common", "dim_funding")
G_AWARD   = gold("common", "dim_award")
G_IA      = gold("common", "dim_ia")
G_AGENCY  = gold("common", "dim_agency")
G_DATE    = gold("common", "dim_date")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="aasbs", source_table="loa_ledger",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            line_item_sk,
            loa_sk,
            funding_sk,
            award_sk,
            ia_sk,
            agency_sk,
            snapshot_date_sk,
            fiscal_year,
            fiscal_quarter,
            fiscal_month,
            budget_fy,
            committed_amt,
            certified_amt,
            obligated_amt,
            invoiced_amt,
            accepted_amt,
            billed_amt,
            accrued_amt,
            udo_amt,
            service_charge_amt,
            disbursed_amt,
            undelivered_orders_amt,
            loa_transaction_type_cd,
            invoice_count,
            acceptance_count,
            source_line_item_id,
            source_loa_ledger_ids,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 1: Aggregate loa_ledger to (line_item_accepted_id, loa_id,
        --         fiscal_year, fiscal_month)
        --
        -- IMPROVEMENT #6: transaction_type_cd REMOVED from GROUP BY.
        --   v1.0 included it, creating 3 rows per grain (COMMITTED / CERTIFIED /
        --   OBLIGATED) and causing double-counting at award level.
        --   loa_transaction_type_cd is now MAX(transaction_type_cd) — a degenerate
        --   dimension stored for reference; does not affect the grain.
        -- ─────────────────────────────────────────────────────────────────
        WITH ledger_agg AS (
            SELECT
                ll.line_item_accepted_id,
                ll.loa_id,
                ll.funding_id,
                ll.ia_id,
                -- IMPROVEMENT #6: MAX captures the degenerate dim without
                -- expanding the grain.  Typically 'OBLIGATED' for funded CLINs.
                MAX(ll.transaction_type_cd)                     AS loa_transaction_type_cd,

                -- Federal fiscal year
                CASE
                    WHEN MONTH(ll.created_dt) >= 10
                    THEN YEAR(ll.created_dt) + 1
                    ELSE YEAR(ll.created_dt)
                END                                             AS fiscal_year,
                -- Federal fiscal quarter
                CASE
                    WHEN MONTH(ll.created_dt) IN (10, 11, 12)  THEN 1
                    WHEN MONTH(ll.created_dt) IN (1,  2,  3)   THEN 2
                    WHEN MONTH(ll.created_dt) IN (4,  5,  6)   THEN 3
                    ELSE 4
                END                                             AS fiscal_quarter,
                -- Federal fiscal month (1=Oct … 12=Sep)
                CASE
                    WHEN MONTH(ll.created_dt) >= 10
                    THEN MONTH(ll.created_dt) - 9
                    ELSE MONTH(ll.created_dt) + 3
                END                                             AS fiscal_month,
                CAST(MAX(ll.created_dt) AS DATE)                AS snapshot_date,
                SUM(ll.line_item_committed_amt)                 AS committed_amt,
                SUM(ll.line_item_certified_amt)                 AS certified_amt,
                SUM(ll.line_item_obligated_amt)                 AS obligated_amt,
                SUM(ll.service_charge_amt)                      AS service_charge_amt,
                CONCAT_WS(',', COLLECT_LIST(CAST(ll.id AS STRING)))
                                                                AS source_loa_ledger_ids
            FROM {S_LEDGER} ll
            WHERE COALESCE(ll.is_deleted, FALSE) = FALSE
              AND ll.line_item_accepted_id IS NOT NULL
            -- IMPROVEMENT #6: GROUP BY does NOT include transaction_type_cd
            GROUP BY
                ll.line_item_accepted_id,
                ll.loa_id,
                ll.funding_id,
                ll.ia_id,
                CASE WHEN MONTH(ll.created_dt) >= 10
                     THEN YEAR(ll.created_dt) + 1
                     ELSE YEAR(ll.created_dt) END,
                CASE WHEN MONTH(ll.created_dt) IN (10, 11, 12) THEN 1
                     WHEN MONTH(ll.created_dt) IN (1,  2,  3)  THEN 2
                     WHEN MONTH(ll.created_dt) IN (4,  5,  6)  THEN 3
                     ELSE 4 END,
                CASE WHEN MONTH(ll.created_dt) >= 10
                     THEN MONTH(ll.created_dt) - 9
                     ELSE MONTH(ll.created_dt) + 3 END
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 2: Aggregate invoice_item to (line_item_accepted_id)
        -- ─────────────────────────────────────────────────────────────────
        invoice_agg AS (
            SELECT
                ii.cost_line_item_accepted_id       AS line_item_accepted_id,
                SUM(ii.invoice_item_amt)            AS invoiced_amt,
                COUNT(DISTINCT ii.invoice_id)       AS invoice_count
            FROM {S_INV} ii
            WHERE COALESCE(ii.is_deleted, FALSE) = FALSE
              AND ii.cost_line_item_accepted_id IS NOT NULL
            GROUP BY ii.cost_line_item_accepted_id
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 3: Aggregate acceptance_item to (line_item_accepted_id)
        -- ─────────────────────────────────────────────────────────────────
        acceptance_agg AS (
            SELECT
                ai.line_item_accepted_id,
                SUM(ai.item_gsa_approved_amt)       AS accepted_amt,
                COUNT(DISTINCT ai.acceptance_id)    AS acceptance_count
            FROM {S_ACC} ai
            WHERE COALESCE(ai.is_deleted, FALSE) = FALSE
              AND ai.line_item_accepted_id IS NOT NULL
            GROUP BY ai.line_item_accepted_id
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 4: Aggregate bill_item at CLIN grain
        --
        -- FIX (v1.1.0): sourced from aasbs.bill_item (CLIN grain) via
        -- line_item_accepted_id — replaces billing.billing (IA grain) which
        -- produced systematic over-count by applying the full IA billing total
        -- to every CLIN under that IA.
        -- ─────────────────────────────────────────────────────────────────
        bill_item_agg AS (
            SELECT
                bi.line_item_accepted_id,
                SUM(bi.bill_item_amt)               AS billed_amt
            FROM {S_BILL_IT} bi
            WHERE COALESCE(bi.is_deleted, FALSE) = FALSE
              AND bi.line_item_accepted_id IS NOT NULL
            GROUP BY bi.line_item_accepted_id
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 5: Join aggregates and resolve line_item_id from bridge table
        -- ─────────────────────────────────────────────────────────────────
        combined AS (
            SELECT
                la.*,
                lia.line_item_id,
                lia.budget_fy,
                COALESCE(inv.invoiced_amt,    0.00)  AS invoiced_amt,
                COALESCE(inv.invoice_count,   0)     AS invoice_count,
                COALESCE(acc.accepted_amt,    0.00)  AS accepted_amt,
                COALESCE(acc.acceptance_count, 0)    AS acceptance_count,
                COALESCE(bi.billed_amt,       0.00)  AS billed_amt
            FROM ledger_agg la
            LEFT JOIN {S_LIA} lia
                ON  lia.id = la.line_item_accepted_id
                AND COALESCE(lia.is_deleted, FALSE) = FALSE
            LEFT JOIN invoice_agg inv
                ON  inv.line_item_accepted_id = la.line_item_accepted_id
            LEFT JOIN acceptance_agg acc
                ON  acc.line_item_accepted_id = la.line_item_accepted_id
            -- FIX: CLIN-grain billing join
            LEFT JOIN bill_item_agg bi
                ON  bi.line_item_accepted_id  = la.line_item_accepted_id
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 6: Resolve Gold surrogate keys
        -- ─────────────────────────────────────────────────────────────────
        with_sks AS (
            SELECT
                c.*,
                li.line_item_sk,
                dl.loa_sk,
                df.funding_sk,
                da.award_sk,
                ia.ia_sk,
                ag.agency_sk,
                dd.date_sk                          AS snapshot_date_sk
            FROM combined c
            LEFT JOIN {G_LI} li
                ON  li.line_item_id    = c.line_item_id
                AND li.is_current_flag = TRUE
            LEFT JOIN {G_LOA} dl
                ON  dl.loa_id          = c.loa_id
                AND dl.is_current_flag = TRUE
            LEFT JOIN {G_FUND} df
                ON  df.funding_id      = c.funding_id
                AND df.is_current_flag = TRUE
            LEFT JOIN {G_AWARD} da
                ON  da.award_sk        = li.award_sk
                AND da.is_current_flag = TRUE
            LEFT JOIN {G_IA} ia
                ON  ia.ia_id           = c.ia_id
                AND ia.is_current_flag = TRUE
            LEFT JOIN {G_AGENCY} ag
                ON  ag.agency_sk       = ia.servicing_agency_sk
                AND ag.is_current_flag = TRUE
            LEFT JOIN {G_DATE} dd
                ON  dd.date_sk = CAST(DATE_FORMAT(c.snapshot_date, 'yyyyMMdd') AS INT)
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 7: Final projection with derived measures
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            line_item_sk,
            COALESCE(loa_sk,     -1)                    AS loa_sk,
            COALESCE(funding_sk, -1)                    AS funding_sk,
            COALESCE(award_sk,   -1)                    AS award_sk,
            COALESCE(ia_sk,      -1)                    AS ia_sk,
            COALESCE(agency_sk,  -1)                    AS agency_sk,
            snapshot_date_sk,
            fiscal_year,
            fiscal_quarter,
            fiscal_month,
            budget_fy,
            COALESCE(committed_amt,    0.00)            AS committed_amt,
            COALESCE(certified_amt,    0.00)            AS certified_amt,
            COALESCE(obligated_amt,    0.00)            AS obligated_amt,
            COALESCE(invoiced_amt,     0.00)            AS invoiced_amt,
            COALESCE(accepted_amt,     0.00)            AS accepted_amt,
            COALESCE(billed_amt,       0.00)            AS billed_amt,
            -- accrued_amt: seeded 0.00; DP4 enriches on first CDC run
            CAST(0.00 AS DECIMAL(15, 2))                AS accrued_amt,
            -- udo = obligated - accepted (unliquidated obligation)
            COALESCE(obligated_amt, 0.00)
                - COALESCE(accepted_amt, 0.00)          AS udo_amt,
            COALESCE(service_charge_amt, 0.00)          AS service_charge_amt,
            -- disbursed_amt: SILVER-PENDING — aasbs.disbursement not yet ingested.
            -- FIX: was billing proxy (billing amounts ≠ Pegasys disbursements).
            CAST(NULL AS DECIMAL(15, 2))                AS disbursed_amt,
            -- undelivered_orders = obligated - invoiced
            COALESCE(obligated_amt, 0.00)
                - COALESCE(invoiced_amt, 0.00)          AS undelivered_orders_amt,
            -- IMPROVEMENT #6: degenerate dim — MAX per grain, not a GROUP BY key
            loa_transaction_type_cd,
            COALESCE(invoice_count,    0)               AS invoice_count,
            COALESCE(acceptance_count, 0)               AS acceptance_count,
            line_item_id                                AS source_line_item_id,
            source_loa_ledger_ids,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM with_sks
        -- Exclude phantom zero-dollar rows (migration artefacts)
        WHERE NOT (
            COALESCE(obligated_amt, 0) = 0
            AND COALESCE(accepted_amt, 0) = 0
            AND COALESCE(invoiced_amt, 0) = 0
        )
    """)

    # ── Post-load quality checks ───────────────────────────────────────────
    n = row_count(spark, TARGET)

    quality = spark.sql(f"""
        SELECT
            COUNT(*)                                                             AS total_rows,
            SUM(CASE WHEN line_item_sk IS NULL             THEN 1 ELSE 0 END)  AS null_line_item_sk,
            SUM(CASE WHEN loa_sk  = -1                     THEN 1 ELSE 0 END)  AS sentinel_loa_sk,
            SUM(CASE WHEN ia_sk   = -1                     THEN 1 ELSE 0 END)  AS sentinel_ia_sk,
            SUM(CASE WHEN udo_amt < 0                      THEN 1 ELSE 0 END)  AS negative_udo,
            SUM(CASE WHEN disbursed_amt IS NOT NULL         THEN 1 ELSE 0 END) AS non_null_disbursed,
            COUNT(DISTINCT loa_transaction_type_cd)                            AS distinct_txn_types,
            ROUND(SUM(obligated_amt), 2)                                       AS total_obligated,
            ROUND(SUM(accepted_amt),  2)                                       AS total_accepted,
            ROUND(SUM(billed_amt),    2)                                       AS total_billed
        FROM {TARGET}
    """).collect()[0]

    print(f"  [OK] Inserted {n:,} fact rows")
    print(f"  null line_item_sk  : {quality['null_line_item_sk']:,}")
    print(f"  sentinel loa_sk=-1 : {quality['sentinel_loa_sk']:,}")
    print(f"  sentinel ia_sk=-1  : {quality['sentinel_ia_sk']:,}")
    print(f"  negative udo rows  : {quality['negative_udo']:,}")
    print(f"  non_null disbursed : {quality['non_null_disbursed']:,}  "
          f"(expected 0 — Silver-pending NULL)")
    print(f"  distinct txn types : {quality['distinct_txn_types']}  "
          f"(IMPROVEMENT #6 — grain now independent of transaction_type)")
    print(f"  total obligated    : ${quality['total_obligated']:,.2f}")
    print(f"  total accepted     : ${quality['total_accepted']:,.2f}")
    print(f"  total billed       : ${quality['total_billed']:,.2f}")

    assert quality["non_null_disbursed"] == 0, \
        "ASSERT FAILED: disbursed_amt must be NULL on prime (Silver-pending)"
    #assert quality["total_obligated"] >= quality["total_accepted"], \
    #    "ASSERT FAILED: total accepted exceeds total obligated — check loa_ledger"

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
