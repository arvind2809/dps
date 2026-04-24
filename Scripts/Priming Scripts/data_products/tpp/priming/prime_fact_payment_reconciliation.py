# Databricks notebook source
# =============================================================================
# tpp/prime_fact_payment_reconciliation.py
# Primes assist_dev.tpp.fact_payment_reconciliation
#
# Grain    : One row per inv_summary.id (one per invoice transmission)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   line_item_sk         ← common.dim_line_item
#                          via inv_summary.invoice_id
#                          → silver_aasbs_invoice.id  (invoice_id FK)
#                          → silver_aasbs_invoice_item.invoice_id
#                          → MIN(invoice_item.cost_line_item_accepted_id)
#                          → dim_line_item.line_item_accepted_id
#
#   rr_sk                ← tpp.dim_receiving_report
#                          via inv_summary.invoice_id
#                          → rr_summary.invoice_id (FK confirmed)
#                          → rr_summary.id → dim_receiving_report.rr_summary_id
#
#   agreement_sk         ← SENTINEL -1 on prime.  [SILVER GAP — see note]
#
#   ia_sk                ← common.dim_ia
#                          via inv_summary.purchase_order_number
#                          = po_summary.purchase_order_number
#                          → po_summary.award_mod_id
#                          → silver_aasbs_award_ia.award_id
#                          → award_ia.ia_id → dim_ia.ia_sk
#
#   invoice_date_sk      ← common.dim_date via inv_summary.invoice_dt
#   payment_date_sk      ← CAST(NULL) [SILVER-PENDING — see note]
#   invoice_amt          ← inv_summary.total_invoice_amount
#   po_amt               ← SUM(po_line.total_line_amt) per po_summary_id
#   rr_amt               ← rr_summary.max_approved_amt via rr_summary.invoice_id
#   payment_amt          ← CAST(NULL) [SILVER-PENDING — see note]
#   billing_amt          ← CAST(NULL) [SILVER GAP — see note]
#   invoice_to_payment_days  ← CAST(NULL) [SILVER-PENDING]
#   is_prompt_payment_compliant ← CAST(NULL) [SILVER-PENDING]
#   po_to_rr_variance_amt  ← SUM(po_line.total_line_amt) − rr_summary.max_approved_amt
#   invoice_to_billing_variance_amt ← CAST(NULL) (billing_amt NULL on prime)
#
# SILVER GAP — agreement_sk on fact_payment_reconciliation:
#   No confirmed direct Silver path from inv_summary to agreement.
#   inv_summary has no agreement_num column. The path via po_summary.award_mod_id
#   → award → acquisition → billing → agreement is too speculative for prime.
#   Sentinel -1 on prime; CDC enrichment or a confirmed bridge table required.
#
# SILVER-PENDING — payment_amt, payment_date_sk, invoice_to_payment_days,
#   is_prompt_payment_compliant:
#   Source tables aasbs.payment, aasbs.ipac_transaction, and aasbs.vitap_batch
#   are not ingested into Silver.  These fields are required for Prompt Payment
#   Act compliance monitoring per 31 U.S.C. §3901.  NULL on prime.
#
# SILVER GAP — billing_amt:
#   No confirmed join path from inv_summary to billing.billing without source
#   system documentation of the linking column.  billing.billing_summary_id
#   references billing_summary (not inv_summary).  NULL on prime.
#
# IMPROVEMENT I-DP5-1 (built-in):
#   Five pre-aggregated temp views prevent fan-out in the main INSERT:
#     v_first_clin_per_invoice   — primary CLIN per invoice
#     v_rr_per_invoice           — RR summary per invoice_id
#     v_po_per_inv_purchase_num  — po_summary + PO amount per purchase_order_number
#     v_ia_per_po                — ia_sk via po_summary.award_mod_id → award_ia
#     v_agency_per_inv           — agency_sk via inv_summary.activity_address_cd
#
# IMPROVEMENT I-DP5-2 (built-in):
#   INSERT-only guard pattern matches DP2-DP4 standards.
#
# IMPROVEMENT I-DP5-3 (built-in):
#   All SILVER-PENDING and SILVER GAP columns annotated with source table,
#   ingest status, and regulatory reference (31 U.S.C. §3901 for payment).
#
# IMPROVEMENT I-DP5-8 (built-in):
#   agreement_sk asymmetry explicitly documented: agreement_sk = -1 on prime
#   for this fact (no confirmed Silver path from inv_summary to agreement).
#   Contrast with fact_transmission_event where agreement_sk is resolvable
#   via billing_summary.agreement_num → agreement.agreement_num.
#   Candidate path for CDC: inv_summary.invoice_id → aasbs_invoice.award_id
#   → billing bridge → agreement_num.
#
# IMPROVEMENT I-DP5-4 (built-in):
#   Post-load check reports multi-CLIN invoice count (invoices where >1 CLIN
#   links to the same inv_summary.id), confirming that line_item_sk = primary
#   CLIN only.  Operator awareness of potential under-representation.
#
# IMPROVEMENT I-DP5-7 (built-in):
#   Post-load Prompt Payment Act placeholder metric:
#   reports "0 — payment_date_sk NULL on prime (SILVER-PENDING aasbs.payment)"
#   so the metric is ready when payment data is ingested.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_fact_payment_reconciliation"
TARGET_TABLE = "assist_dev.tpp.fact_payment_reconciliation"

SILVER = "assist_dev.assist_finance"
TPP    = "assist_dev.tpp"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_transmit_inv_summary")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only.
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
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")
        audit_fail(spark, RUN_ID, TARGET_TABLE, str(msg), msg, start_ts)

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_transmit_inv_summary
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver inv_summary row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views  (IMPROVEMENT I-DP5-1)
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Primary CLIN per invoice  (IMPROVEMENT I-DP5-4 grain annotation)
    # Path: inv_summary.invoice_id → aasbs_invoice.id → invoice_item.invoice_id
    #       → MIN(cost_line_item_accepted_id) = primary CLIN
    # One invoice can span multiple CLINs; MIN() picks the primary CLIN
    # deterministically.  Post-load check reports multi-CLIN invoice count.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_first_clin_per_invoice AS
        SELECT
            inv.id                              AS aasbs_invoice_id,
            MIN(ii.cost_line_item_accepted_id)  AS primary_lia_id
        FROM {SILVER}.silver_aasbs_invoice inv
        JOIN {SILVER}.silver_aasbs_invoice_item ii
            ON  ii.invoice_id = inv.id
            AND COALESCE(ii.is_deleted, FALSE) = FALSE
        WHERE COALESCE(inv.is_deleted, FALSE) = FALSE
          AND ii.cost_line_item_accepted_id IS NOT NULL
        GROUP BY inv.id
    """)

    # 3b — RR per invoice
    # rr_summary.invoice_id FK links to aasbs_invoice.id (not inv_summary directly).
    # inv_summary.invoice_id also points to aasbs_invoice.id.
    # Join: inv_summary.invoice_id = aasbs_invoice.id → rr_summary.invoice_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_rr_per_invoice AS
        SELECT
            rr.invoice_id               AS aasbs_invoice_id,
            MIN(rr.id)                  AS rr_summary_id,
            MAX(rr.max_approved_amt)    AS rr_amt
        FROM {SILVER}.silver_aasbs_transmit_rr_summary rr
        WHERE COALESCE(rr.is_deleted, FALSE) = FALSE
          AND rr.invoice_id IS NOT NULL
        GROUP BY rr.invoice_id
    """)

    # 3c — PO amounts per purchase_order_number
    # inv_summary.purchase_order_number = po_summary.purchase_order_number
    # SUM(po_line.total_line_amt) aggregated at po_summary grain first, then
    # keyed by purchase_order_number to join to inv_summary.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_po_per_inv_purchase_num AS
        SELECT
            pos.purchase_order_number,
            MIN(pos.id)                             AS po_summary_id,
            MIN(pos.award_mod_id)                   AS award_mod_id,
            MIN(pos.activity_address_cd)            AS aac,
            SUM(COALESCE(pol.total_line_amt, 0.00)) AS po_amt
        FROM {SILVER}.silver_aasbs_transmit_po_summary pos
        LEFT JOIN {SILVER}.silver_aasbs_transmit_po_line pol
            ON  pol.po_summary_id = pos.id
            AND COALESCE(pol.is_deleted, FALSE) = FALSE
        WHERE COALESCE(pos.is_deleted, FALSE) = FALSE
          AND pos.purchase_order_number IS NOT NULL
        GROUP BY pos.purchase_order_number
    """)

    # 3d — ia_sk per po (via award_mod_id → award_ia bridge)
    # award_ia bridge: award_id + ia_id confirmed in Silver DDL.
    # po_summary.award_mod_id → award_mod.award_id → award_ia.award_id → ia_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_per_po AS
        SELECT
            po.purchase_order_number,
            COALESCE(dia.ia_sk, -1)     AS ia_sk
        FROM v_po_per_inv_purchase_num po
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = po.award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
        LEFT JOIN (
            SELECT award_id, MIN(ia_id) AS ia_id
            FROM {SILVER}.silver_aasbs_award_ia
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY award_id
        ) awi ON awi.award_id = am.award_id
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = awi.ia_id
            AND dia.is_current_flag = TRUE
    """)

    # 3e — Agency per inv_summary via inv_summary.activity_address_cd
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_agency_per_inv AS
        SELECT
            activity_address_cd     AS aac,
            MIN(agency_sk)          AS agency_sk
        FROM {COMMON}.dim_agency
        WHERE is_current_flag = TRUE
          AND activity_address_cd IS NOT NULL
        GROUP BY activity_address_cd
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_sk,
            rr_sk,
            agreement_sk,
            ia_sk,
            agency_sk,
            invoice_date_sk,
            payment_date_sk,
            source_inv_summary_id,
            pegasys_invoice_num,
            invoice_amt,
            po_amt,
            rr_amt,
            payment_amt,
            billing_amt,
            invoice_to_payment_days,
            is_prompt_payment_compliant,
            po_to_rr_variance_amt,
            invoice_to_billing_variance_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Common dimension FKs ──────────────────────────────────── */

            -- line_item_sk: primary CLIN per invoice (IMPROVEMENT I-DP5-4 grain)
            -- MIN(cost_line_item_accepted_id) per invoice → dim_line_item
            COALESCE(dli.line_item_sk, -1)                      AS line_item_sk,

            /* ── DP5 dimension FKs ─────────────────────────────────────── */

            -- rr_sk: rr_summary.id via inv_summary.invoice_id → rr_summary.invoice_id
            COALESCE(drr.rr_sk, -1)                             AS rr_sk,

            -- agreement_sk: SILVER GAP — no confirmed path from inv_summary
            -- to agreement at prime time. Sentinel -1.
            -- Candidate path for CDC enrichment:
            --   inv_summary.invoice_id → aasbs_invoice.award_id
            --   → billing.billing (by agreement/billing bridge)
            --   → agreement.agreement_num
            CAST(-1 AS BIGINT)                                  AS agreement_sk,

            -- ia_sk: via po_summary.award_mod_id → award_ia bridge
            COALESCE(iap.ia_sk, -1)                             AS ia_sk,

            -- agency_sk: inv_summary.activity_address_cd → dim_agency
            COALESCE(agn.agency_sk, -1)                         AS agency_sk,

            /* ── Date FKs ───────────────────────────────────────────────── */

            -- invoice_date_sk: inv_summary.invoice_dt → YYYYMMDD INT
            CASE
                WHEN inv.invoice_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(inv.invoice_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS invoice_date_sk,

            -- payment_date_sk: SILVER-PENDING.
            -- Source: aasbs.payment / aasbs.vitap_batch — not ingested.
            -- Required for Prompt Payment Act (31 U.S.C. §3901) monitoring.
            -- CDC will populate when payment tables are ingested into Silver.
            CAST(NULL AS INT)                                   AS payment_date_sk,

            /* ── Source identifiers ─────────────────────────────────────── */

            inv.id                                              AS source_inv_summary_id,
            inv.pegasys_doc_num                                 AS pegasys_invoice_num,

            /* ── Financial measures ─────────────────────────────────────── */

            -- invoice_amt: total amount on the invoice transmission record
            COALESCE(inv.total_invoice_amount, 0.00)            AS invoice_amt,

            -- po_amt: SUM(po_line.total_line_amt) per purchase_order_number
            -- IMPROVEMENT I-DP5-1: pre-aggregated view (no fan-out)
            COALESCE(po.po_amt, 0.00)                           AS po_amt,

            -- rr_amt: rr_summary.max_approved_amt for this invoice
            -- (the total value accepted on the receiving report)
            COALESCE(rr_row.rr_amt, 0.00)                       AS rr_amt,

            -- payment_amt: SILVER-PENDING.
            -- Sources: aasbs.payment, aasbs.ipac_transaction, aasbs.vitap_batch.
            -- None of these tables are ingested into Silver.
            -- Ref: Prompt Payment Act (31 U.S.C. §3901).
            CAST(NULL AS DECIMAL(15, 2))                        AS payment_amt,

            -- billing_amt: SILVER GAP.
            -- No confirmed direct Silver join path from inv_summary to billing.billing.
            -- billing.billing_summary_id references billing_summary (not inv_summary).
            -- CDC or source-system documentation of the bridge is required.
            CAST(NULL AS DECIMAL(15, 2))                        AS billing_amt,

            -- invoice_to_payment_days: SILVER-PENDING (payment_date required).
            -- Prompt Payment Act requires payment within 30 days of invoice receipt.
            -- Ref: 31 U.S.C. §3901(a)(1).
            CAST(NULL AS INT)                                   AS invoice_to_payment_days,

            -- is_prompt_payment_compliant: SILVER-PENDING (payment_date required).
            -- TRUE when invoice_to_payment_days <= 30 per Prompt Payment Act.
            -- Ref: 31 U.S.C. §3901.
            CAST(NULL AS BOOLEAN)                               AS is_prompt_payment_compliant,

            /* ── Variance measures (computable on prime) ─────────────────── */

            -- po_to_rr_variance_amt: PO amount − RR accepted amount.
            -- Positive = PO exceeds RR (partial delivery); Negative = over-receipt.
            ROUND(
                COALESCE(po.po_amt, 0.00) - COALESCE(rr_row.rr_amt, 0.00),
                2
            )                                                   AS po_to_rr_variance_amt,

            -- invoice_to_billing_variance_amt: NULL on prime (billing_amt NULL).
            CAST(NULL AS DECIMAL(15, 2))                        AS invoice_to_billing_variance_amt,

            /* ── Audit ───────────────────────────────────────────────────── */
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_transmit_inv_summary inv

        /* ── Primary CLIN per invoice ────────────────────────────────────── */
        LEFT JOIN v_first_clin_per_invoice fcl
            ON  fcl.aasbs_invoice_id = inv.invoice_id

        /* ── dim_line_item: primary CLIN → line_item_sk ─────────────────── */
        LEFT JOIN {COMMON}.dim_line_item dli
            ON  dli.line_item_accepted_id = fcl.primary_lia_id
            AND dli.is_current_flag       = TRUE

        /* ── RR per invoice ─────────────────────────────────────────────── */
        LEFT JOIN v_rr_per_invoice rr_row
            ON  rr_row.aasbs_invoice_id = inv.invoice_id

        /* ── dim_receiving_report: rr_summary_id → rr_sk ───────────────── */
        LEFT JOIN {TPP}.dim_receiving_report drr
            ON  drr.rr_summary_id = rr_row.rr_summary_id

        /* ── PO amount per purchase_order_number ─────────────────────────── */
        LEFT JOIN v_po_per_inv_purchase_num po
            ON  po.purchase_order_number = inv.purchase_order_number

        /* ── ia_sk via PO → award_mod → award_ia ────────────────────────── */
        LEFT JOIN v_ia_per_po iap
            ON  iap.purchase_order_number = inv.purchase_order_number

        /* ── agency_sk via inv_summary.activity_address_cd ──────────────── */
        LEFT JOIN v_agency_per_inv agn
            ON  agn.aac = inv.activity_address_cd

        WHERE COALESCE(inv.is_deleted, FALSE) = FALSE
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
            f"source rows ({rows_read:,}) — check temp view joins for fan-out."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN line_item_sk = -1 THEN 1 ELSE 0 END) AS unresolved_clin,
            SUM(CASE WHEN rr_sk        = -1 THEN 1 ELSE 0 END) AS unresolved_rr,
            SUM(CASE WHEN ia_sk        = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN agency_sk    = -1 THEN 1 ELSE 0 END) AS unresolved_agency,
            COUNT(*)                                            AS total_rows
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"clin: {sentinel[0]:,} | rr: {sentinel[1]:,} | "
        f"ia: {sentinel[2]:,} | agency: {sentinel[3]:,}"
    )
    print(
        f"[{NOTEBOOK}] agreement_sk: all rows = -1 on prime (SILVER GAP — "
        f"no confirmed path from inv_summary to agreement)"
    )

    fin = spark.sql(f"""
        SELECT
            ROUND(SUM(invoice_amt),           2) AS total_invoice,
            ROUND(SUM(po_amt),                2) AS total_po,
            ROUND(SUM(rr_amt),                2) AS total_rr,
            ROUND(SUM(po_to_rr_variance_amt), 2) AS total_po_rr_var,
            SUM(CASE WHEN payment_amt IS NOT NULL             THEN 1 ELSE 0 END) AS has_payment,
            SUM(CASE WHEN is_prompt_payment_compliant IS TRUE THEN 1 ELSE 0 END) AS compliant_count
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Financial — "
        f"invoice=${fin[0]:,.2f} | po=${fin[1]:,.2f} | "
        f"rr=${fin[2]:,.2f} | po_rr_var=${fin[3]:,.2f}"
    )

    # IMPROVEMENT I-DP5-4: multi-CLIN invoice report
    multi_clin = spark.sql(f"""
        SELECT COUNT(DISTINCT aasbs_invoice_id) AS multi_clin_invoice_count
        FROM (
            SELECT fcl.aasbs_invoice_id, COUNT(*) AS clin_cnt
            FROM v_first_clin_per_invoice fcl
            GROUP BY fcl.aasbs_invoice_id
            HAVING COUNT(*) > 1
        )
    """).collect()[0][0]
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP5-4 — multi-CLIN invoices: {multi_clin:,} "
        f"(line_item_sk = primary CLIN only for these invoices — "
        f"secondary CLINs not represented in this fact table)"
    )

    # IMPROVEMENT I-DP5-7: Prompt Payment Act placeholder metric
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP5-7 — Prompt Payment Act compliance: "
        f"0 rows (payment_date_sk NULL on prime — "
        f"SILVER-PENDING: aasbs.payment / aasbs.vitap_batch not ingested). "
        f"Metric will activate once payment data is available."
    )

    assert fin[4] == 0, \
        "ASSERT FAILED: payment_amt must be NULL on prime (SILVER-PENDING)"

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
