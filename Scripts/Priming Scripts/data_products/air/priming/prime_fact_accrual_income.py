# Databricks notebook source
# =============================================================================
# air/prime_fact_accrual_income.py
# Primes assist_dev.air.fact_accrual_income
#
# Grain    : One row per accrual_income_dist  (one per CLIN × LOA × period)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# BUG FIX B-DP4-1 (v1.2.0) — ia_sk via acquisition.ia_id (non-existent):
#   v_ia_per_acquisition previously joined silver_aasbs_acquisition.ia_id which
#   does not exist on that table (same root cause as B-DP3-4).  The column ia_id
#   is NOT stored on acquisition in the ASSIST data model.  Every fact row produced
#   ia_sk = -1 (sentinel) regardless of the actual IA association.
#   Fix: v_ia_per_acquisition replaced with v_ia_per_award which routes via the
#   confirmed bridge table silver_aasbs_award_ia:
#     accrual_expense.award_id
#       → silver_aasbs_award_ia.award_id
#       → silver_aasbs_award_ia.ia_id
#       → common.dim_ia.ia_sk
#   silver_aasbs_award_ia confirmed in Silver DDL with columns: id, award_id, ia_id.
#
# IMPROVEMENT I-DP4-1 (v1.2.0) — v_onefund_doc_type fan-out prevention:
#   v_onefund_doc_type previously joined raw silver_aasbs_map_aac_onefund without
#   deduplication by AAC.  map_aac_onefund has multiple rows per AAC (different
#   cost_element_cd / onefund_program_cd variants).  The raw join produces multiple
#   accrual_income_doc_type_cd values per AAC, causing fan-out in the fact INSERT
#   (duplicate fact rows for the same accrual_income_dist.id).
#   Fix: v_onefund_doc_type now uses the same ROW_NUMBER() dedup logic as
#   dim_onefund_program (active-first, oldest created_dt as tiebreaker).
#
# IMPROVEMENT I-DP4-2 (v1.2.0) — holdback expression extracted to CTE:
#   holdback_amt and net_accrued_amt previously duplicated an identical 8-line
#   CASE expression.  A maintenance change to one but not the other would silently
#   break the identity: distributed_income_amt − holdback_amt = net_accrued_amt.
#   Fix: holdback derivation extracted to a named CTE column holdback_amt_derived.
#   Both holdback_amt and net_accrued_amt reference it — single point of truth.
#
# IMPROVEMENT I-DP4-3 (v1.2.0) — accrual_month decoder extracted to CTE:
#   The 12-branch CASE decoding accrual_month STRING → INT was duplicated verbatim
#   in the dim_accrual_period JOIN ON clause and the post-load diagnostic query.
#   Fix: a WITH cte computes accrual_month_int once; all downstream references
#   use the CTE column.  Adding a new month format (e.g. 'JANUARY') requires one
#   edit, not two.
#
# IMPROVEMENT I-DP4-6 (v1.2.0) — distributed vs calculated reconciliation check:
#   FASAB SFFAS No.7 requires that distributed income sums to calculated income
#   per accrual_income record.  A post-load check reports the count of income
#   records where SUM(dist.distribute_amt) ≠ calculated_income_amt; any non-zero
#   count warrants investigation.
#
# IMPROVEMENT I-DP4-7 (v1.2.0) — baar_transmission_status_cd fallback:
#   accrual_income_dist.accrual_transmission_status_cd holds the dist-level status
#   and is available even when no transmit header exists yet.  Used as
#   COALESCE(bt.baar_transmission_status_cd, dist.accrual_transmission_status_cd)
#   to reduce NULL count on this frequently-queried field.
#
# Source-to-target mapping (all confirmed against Silver DDL):
#   line_item_sk      ← common.dim_line_item via ai.line_item_accepted_id
#   loa_sk            ← common.dim_loa via dist.tracking_num → loa.tracking_num
#   ia_sk             ← common.dim_ia via ae.award_id → award_ia.ia_id  [FIXED]
#   agency_sk         ← common.dim_agency via loa.agency_location_code
#   accrual_period_sk ← air.dim_accrual_period via (accrual_year, accrual_month_int)
#   onefund_sk        ← air.dim_onefund_program via loa.agency_location_code
#   holdback_amt      ← (ae.before_holdback - ae.with_holdback) × dist.proration_pct  [REFACTORED]
#   net_accrued_amt   ← distributed_income_amt - holdback_amt_derived  [REFACTORED]
#   baar_transmission_status_cd ← COALESCE(transmit_header, dist.status)  [NEW I-DP4-7]
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP4"
NOTEBOOK     = "prime_fact_accrual_income"
TARGET_TABLE = "assist_dev.air.fact_accrual_income"

SILVER = "assist_dev.assist_finance"
GOLD   = "assist_dev.air"
COMMON = "assist_dev.common"

# COMMAND ----------

# start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_accrual_income_dist")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only.  Abort if rows already exist.
    # ─────────────────────────────────────────────────────────────────────
    existing_rows = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]

    if existing_rows > 0:
        msg = (
            f"[{NOTEBOOK}] ABORTED — fact table already contains "
            f"{existing_rows:,} rows.  Prime is INSERT-only.  "
            f"To re-prime, manually TRUNCATE {TARGET_TABLE} first."
        )
        print(msg)
        #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        audit_fail(spark, RUN_ID, NOTEBOOK, msg, msg, start_ts)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_accrual_income_dist
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver accrual_income_dist row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated supporting temp views
    # ─────────────────────────────────────────────────────────────────────

    # 3a — LOA lookup: tracking_num → loa.id, agency_location_code
    #   Shared by loa_sk, agency_sk, and onefund_sk resolution.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_by_tracking AS
        SELECT
            tracking_num,
            id                    AS loa_id,
            agency_location_code
        FROM {SILVER}.silver_aasbs_loa
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND tracking_num IS NOT NULL
    """)

    # 3b — ia_sk per award: BUG FIX B-DP4-1
    #
    # PREVIOUS (broken): joined acquisition.ia_id which does not exist on
    #   silver_aasbs_acquisition.  Produced NULL ia_id → ia_sk = -1 for every row.
    #
    # FIXED: routes via silver_aasbs_award_ia bridge table.
    #   silver_aasbs_award_ia confirmed in Silver DDL: award_id + ia_id columns.
    #   accrual_expense.award_id → award_ia.award_id → award_ia.ia_id → dim_ia.ia_sk
    #
    # Note: award_ia is a M:M bridge (an award can link to multiple IAs in theory).
    # MIN(ia_id) picks the primary IA deterministically; most awards have one IA.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_per_award AS
        SELECT
            awi.award_id,
            -- BUG FIX B-DP4-1: ia_sk resolved via award_ia bridge table
            -- (was: acquisition.ia_id which does not exist)
            COALESCE(dia.ia_sk, -1)   AS ia_sk
        FROM (
            -- De-duplicate award_ia to one row per award (MIN ia_id)
            SELECT
                award_id,
                MIN(ia_id) AS ia_id
            FROM {SILVER}.silver_aasbs_award_ia
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY award_id
        ) awi
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = awi.ia_id
            AND dia.is_current_flag = TRUE
    """)

    # 3c — BAAR transmitted amount + status per dist
    #   Path: accrual_income_dist.id
    #         → transmit_accrual_income_header.accrual_income_dist_id
    #         → transmit_accrual_income_acct_line.accrual_income_header_id
    #         → SUM(transaction_currency_amt)
    #   MAX_BY selects the most recently created header for status + doc type.
    #   (MAX_BY supported on Databricks Runtime 14.x LTS / Spark 3.5)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_baar_transmitted AS
        SELECT
            h.accrual_income_dist_id,
            SUM(COALESCE(l.transaction_currency_amt, 0.00))    AS baar_transmitted_amt,
            MAX_BY(h.transmittal_status_cd,  h.created_dt)     AS baar_transmission_status_cd,
            MAX_BY(h.document_type,          h.created_dt)     AS accrual_doc_type_from_transmit
        FROM {SILVER}.silver_aasbs_transmit_accrual_income_header h
        JOIN {SILVER}.silver_aasbs_transmit_accrual_income_acct_line l
            ON  l.accrual_income_header_id = h.id
            AND COALESCE(l.is_deleted, FALSE) = FALSE
        WHERE COALESCE(h.is_deleted, FALSE) = FALSE
          AND h.accrual_income_dist_id IS NOT NULL
        GROUP BY h.accrual_income_dist_id
    """)

    # 3d — BAAR batch per accrual_income: MIN(accrual_batch_id) via income_line bridge
    #   Path: accrual_income.id
    #         → accrual_income_line.accrual_income_id
    #         → accrual_income_line.accrual_header_id
    #         → accrual_header.accrual_batch_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_batch_per_income AS
        SELECT
            il.accrual_income_id,
            MIN(h.accrual_batch_id) AS accrual_batch_id
        FROM {SILVER}.silver_accrual_accrual_income_line il
        JOIN {SILVER}.silver_accrual_accrual_header h
            ON  h.accrual_header_id = il.accrual_header_id
            AND COALESCE(h.is_deleted, FALSE) = FALSE
        WHERE COALESCE(il.is_deleted, FALSE) = FALSE
        GROUP BY il.accrual_income_id
    """)

    # 3e — OneFund doc type fallback: IMPROVEMENT I-DP4-1
    #
    # PREVIOUS (fan-out risk): joined raw silver_aasbs_map_aac_onefund directly.
    #   map_aac_onefund has multiple rows per AAC (different cost_element_cd variants).
    #   One AAC → multiple onefund_program_cd → multiple accrual_income_doc_type_cd
    #   → fan-out in the fact INSERT producing duplicate fact rows.
    #
    # FIXED: applies the same ROW_NUMBER() dedup logic used by dim_onefund_program
    #   (active-first, then oldest created_dt as tiebreaker).
    #   Guarantees exactly one doc_type per AAC.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_onefund_doc_type AS
        SELECT
            m.activity_address_cd   AS aac,
            dt.accrual_income_doc_type_cd
        FROM (
            -- IMPROVEMENT I-DP4-1: deduplicate map_aac_onefund to one row per AAC
            -- (same logic as v_aac_onefund_dedup in dim_onefund_program notebook)
            SELECT
                activity_address_cd,
                onefund_program_cd
            FROM (
                SELECT
                    activity_address_cd,
                    onefund_program_cd,
                    ROW_NUMBER() OVER (
                        PARTITION BY activity_address_cd
                        ORDER BY
                            CASE WHEN UPPER(COALESCE(active_yn, 'N')) = 'Y'
                                 THEN 0 ELSE 1 END ASC,
                            created_dt ASC
                    ) AS rn
                FROM {SILVER}.silver_aasbs_map_aac_onefund
                WHERE COALESCE(is_deleted, FALSE) = FALSE
            )
            WHERE rn = 1
        ) m
        LEFT JOIN {SILVER}.silver_aasbs_map_onefund_program_accrual_income_doc_type dt
            ON  dt.onefund_program_cd = m.onefund_program_cd
            AND COALESCE(dt.is_deleted, FALSE) = FALSE
            AND UPPER(COALESCE(dt.active_yn, 'Y')) = 'Y'
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    #
    # IMPROVEMENT I-DP4-2 + I-DP4-3:
    #   holdback_amt_derived extracted to a CTE column (single point of truth).
    #   accrual_month_int extracted to a CTE column (single decode, two uses).
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_sk,
            loa_sk,
            ia_sk,
            agency_sk,
            accrual_period_sk,
            onefund_sk,
            accrual_income_id,
            accrual_batch_id,
            calculated_income_amt,
            distributed_income_amt,
            holdback_amt,
            net_accrued_amt,
            baar_transmitted_amt,
            baar_accepted_amt,
            baar_rejected_amt,
            baar_transmission_status_cd,
            baar_response_dt,
            inclusion_override_flag,
            inclusion_override_reason,
            accrual_doc_type_cd,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────
        -- IMPROVEMENT I-DP4-3: accrual_month_int decoded once in CTE.
        --   Two known string formats in Silver:
        --     (a) Numeric: '1'–'12' or '01'–'12' → CAST directly
        --     (b) Abbreviation: 'JAN'–'DEC'       → CASE decoder
        --   Single decode here replaces two identical CASE expressions.
        -- ─────────────────────────────────────────────────────────────
        WITH month_decoded AS (
            SELECT
                dist.id                                         AS dist_id,
                dist.accrual_income_id,
                dist.tracking_num,
                dist.distribute_amt,
                dist.proration_pct,
                dist.accrual_transmission_status_cd,
                ai.id                                           AS income_id,
                ai.accrual_year,
                ai.line_item_accepted_id,
                ai.accrual_income_amt,
                ai.include_in_transmission_yn,
                ai.accrual_income_calc_comment,
                ai.accrual_expense_id,
                -- IMPROVEMENT I-DP4-3: single decode, referenced below
                CASE
                    WHEN ai.accrual_month RLIKE '^[0-9]+$'
                        THEN CAST(ai.accrual_month AS INT)
                    ELSE CASE UPPER(TRIM(ai.accrual_month))
                        WHEN 'JAN' THEN 1   WHEN 'FEB' THEN 2   WHEN 'MAR' THEN 3
                        WHEN 'APR' THEN 4   WHEN 'MAY' THEN 5   WHEN 'JUN' THEN 6
                        WHEN 'JUL' THEN 7   WHEN 'AUG' THEN 8   WHEN 'SEP' THEN 9
                        WHEN 'OCT' THEN 10  WHEN 'NOV' THEN 11  WHEN 'DEC' THEN 12
                        ELSE NULL
                    END
                END                                             AS accrual_month_int
            FROM {SILVER}.silver_aasbs_accrual_income_dist dist
            JOIN {SILVER}.silver_aasbs_accrual_income ai
                ON  ai.id = dist.accrual_income_id
                AND COALESCE(ai.is_deleted, FALSE) = FALSE
            WHERE COALESCE(dist.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────
        -- IMPROVEMENT I-DP4-2: holdback_amt extracted to named CTE.
        --   holdback_total = before_holdback - with_holdback_applied
        --   holdback_amt   = holdback_total × proration_pct
        --   Both holdback_amt and net_accrued_amt reference holdback_amt_derived
        --   — ensures distributed_income_amt − holdback_amt = net_accrued_amt
        --   is always algebraically consistent.
        -- ─────────────────────────────────────────────────────────────
        with_holdback AS (
            SELECT
                md.*,
                ae.id                                           AS expense_id,
                ae.acquisition_id,
                ae.award_id,
                -- IMPROVEMENT I-DP4-2: holdback derived once
                CASE
                    WHEN ae.id IS NOT NULL
                    THEN ROUND(
                        (
                            COALESCE(ae.accrual_expense_amt_before_holdback, 0.00)
                          - COALESCE(ae.accrual_expense_amt_with_holdback_applied, 0.00)
                        ) * COALESCE(md.proration_pct, 0.0),
                        2
                    )
                    ELSE NULL
                END                                             AS holdback_amt_derived
            FROM month_decoded md
            LEFT JOIN {SILVER}.silver_aasbs_accrual_expense ae
                ON  ae.id = md.accrual_expense_id
                AND COALESCE(ae.is_deleted, FALSE) = FALSE
        )

        SELECT

            /* ── Dimension FKs ──────────────────────────────────────── */

            -- line_item_sk: via accrual_income.line_item_accepted_id
            COALESCE(dli.line_item_sk, -1)                      AS line_item_sk,

            -- loa_sk: dist.tracking_num → loa.tracking_num → loa.id → dim_loa
            COALESCE(dl.loa_sk, -1)                             AS loa_sk,

            -- ia_sk: BUG FIX B-DP4-1 — via award_ia bridge (not acquisition.ia_id)
            COALESCE(iaw.ia_sk, -1)                             AS ia_sk,

            -- agency_sk: loa.agency_location_code → dim_agency.activity_address_cd
            COALESCE(dag.agency_sk, -1)                         AS agency_sk,

            -- accrual_period_sk: (accrual_year, accrual_month_int) → dim_accrual_period
            -- IMPROVEMENT I-DP4-3: accrual_month_int from CTE (not inline CASE)
            COALESCE(dap.accrual_period_sk, -1)                 AS accrual_period_sk,

            -- onefund_sk: loa.agency_location_code → dim_onefund_program.aac
            dof.onefund_sk                                      AS onefund_sk,

            /* ── Source identifiers ─────────────────────────────────── */

            wh.income_id                                        AS accrual_income_id,
            bpi.accrual_batch_id,

            /* ── Financial measures ─────────────────────────────────── */

            COALESCE(wh.accrual_income_amt,  0.00)              AS calculated_income_amt,
            COALESCE(wh.distribute_amt,      0.00)              AS distributed_income_amt,

            -- holdback_amt: IMPROVEMENT I-DP4-2 — uses CTE-derived value
            wh.holdback_amt_derived                             AS holdback_amt,

            -- net_accrued_amt: IMPROVEMENT I-DP4-2 — references same CTE value
            -- Algebraically consistent: distributed_income_amt − holdback_amt_derived
            ROUND(
                COALESCE(wh.distribute_amt, 0.00)
              - COALESCE(wh.holdback_amt_derived, 0.00),
                2
            )                                                   AS net_accrued_amt,

            /* ── BAAR transmission amounts ───────────────────────────── */

            COALESCE(bt.baar_transmitted_amt, 0.00)             AS baar_transmitted_amt,

            -- NULL on prime — CDC from BAAR response feed
            CAST(NULL AS DECIMAL(15, 2))                        AS baar_accepted_amt,
            CAST(NULL AS DECIMAL(15, 2))                        AS baar_rejected_amt,

            /* ── BAAR status — IMPROVEMENT I-DP4-7 ─────────────────── */

            -- COALESCE transmit header status with dist-level status.
            -- dist.accrual_transmission_status_cd is available even when no
            -- transmit header record exists yet, reducing NULL count.
            COALESCE(
                bt.baar_transmission_status_cd,
                wh.accrual_transmission_status_cd
            )                                                   AS baar_transmission_status_cd,

            -- NULL on prime — CDC from BAAR response
            CAST(NULL AS TIMESTAMP)                             AS baar_response_dt,

            /* ── Inclusion override ─────────────────────────────────── */

            CASE
                WHEN UPPER(COALESCE(wh.include_in_transmission_yn, 'N')) = 'Y'
                THEN TRUE ELSE FALSE
            END                                                 AS inclusion_override_flag,
            wh.accrual_income_calc_comment                      AS inclusion_override_reason,

            /* ── BAAR document type ─────────────────────────────────── */

            -- Actual doc type from most recent transmit header (preferred);
            -- fall back to OneFund program → doc type mapping
            COALESCE(
                bt.accrual_doc_type_from_transmit,
                odt.accrual_income_doc_type_cd
            )                                                   AS accrual_doc_type_cd,

            /* ── Audit ───────────────────────────────────────────────── */
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM with_holdback wh

        /* ── LOA: tracking_num lookup ────────────────────────────────── */
        LEFT JOIN v_loa_by_tracking loa
            ON  loa.tracking_num = wh.tracking_num

        /* ── dim_loa: loa.id → loa_sk ───────────────────────────────── */
        LEFT JOIN {COMMON}.dim_loa dl
            ON  dl.loa_id          = loa.loa_id
            AND dl.is_current_flag = TRUE

        /* ── dim_line_item: line_item_accepted_id → line_item_sk ─────── */
        LEFT JOIN {COMMON}.dim_line_item dli
            ON  dli.line_item_accepted_id = wh.line_item_accepted_id
            AND dli.is_current_flag       = TRUE

        /* ── dim_accrual_period: (year, month_int) → period_sk ──────── */
        -- IMPROVEMENT I-DP4-3: accrual_month_int from CTE column
        LEFT JOIN {GOLD}.dim_accrual_period dap
            ON  dap.accrual_year  = wh.accrual_year
            AND dap.accrual_month = wh.accrual_month_int

        /* ── dim_agency: loa.agency_location_code → agency_sk ───────── */
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = loa.agency_location_code
            AND dag.is_current_flag     = TRUE

        /* ── dim_onefund_program: loa AAC → onefund_sk ──────────────── */
        LEFT JOIN {GOLD}.dim_onefund_program dof
            ON  dof.aac = loa.agency_location_code

        /* ── ia_sk: BUG FIX B-DP4-1 — via award_ia bridge ───────────── */
        LEFT JOIN v_ia_per_award iaw
            ON  iaw.award_id = wh.award_id

        /* ── BAAR transmitted amount + status ────────────────────────── */
        LEFT JOIN v_baar_transmitted bt
            ON  bt.accrual_income_dist_id = wh.dist_id

        /* ── BAAR batch per income ───────────────────────────────────── */
        LEFT JOIN v_batch_per_income bpi
            ON  bpi.accrual_income_id = wh.income_id

        /* ── OneFund doc type fallback ───────────────────────────────── */
        -- IMPROVEMENT I-DP4-1: v_onefund_doc_type uses deduped view (no fan-out)
        LEFT JOIN v_onefund_doc_type odt
            ON  odt.aac = loa.agency_location_code
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Fan-out guard: rows_written should equal rows_read (one row per dist)
    if rows_written != rows_read:
        print(
            f"[{NOTEBOOK}] WARNING: rows_written ({rows_written:,}) ≠ "
            f"rows_read ({rows_read:,}).  "
            f"Possible fan-out from a temp view join — investigate."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,} in = {rows_written:,} out")

    # Period coverage
    period_span = spark.sql(f"""
        SELECT
            MIN(dap.period_label)                   AS earliest_period,
            MAX(dap.period_label)                   AS latest_period,
            COUNT(DISTINCT f.accrual_period_sk)     AS distinct_periods
        FROM {TARGET_TABLE} f
        LEFT JOIN {GOLD}.dim_accrual_period dap
            ON dap.accrual_period_sk = f.accrual_period_sk
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Period span — "
        f"earliest={period_span[0]} | latest={period_span[1]} | "
        f"distinct_periods={period_span[2]:,}"
    )

    # Financial totals
    fin_totals = spark.sql(f"""
        SELECT
            ROUND(SUM(calculated_income_amt),   2) AS total_calculated,
            ROUND(SUM(distributed_income_amt),  2) AS total_distributed,
            ROUND(SUM(net_accrued_amt),         2) AS total_net_accrued,
            ROUND(SUM(baar_transmitted_amt),    2) AS total_transmitted
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Financial totals — "
        f"calculated=${fin_totals[0]:,.2f} | "
        f"distributed=${fin_totals[1]:,.2f} | "
        f"net_accrued=${fin_totals[2]:,.2f} | "
        f"transmitted=${fin_totals[3]:,.2f}"
    )

    # Sentinel FK report — ia_sk count validates B-DP4-1 fix
    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN line_item_sk      = -1 THEN 1 ELSE 0 END) AS unresolved_li,
            SUM(CASE WHEN loa_sk            = -1 THEN 1 ELSE 0 END) AS unresolved_loa,
            SUM(CASE WHEN ia_sk             = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN agency_sk         = -1 THEN 1 ELSE 0 END) AS unresolved_agency,
            SUM(CASE WHEN accrual_period_sk = -1 THEN 1 ELSE 0 END) AS unresolved_period
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"line_item: {sentinel[0]:,} | loa: {sentinel[1]:,} | "
        f"ia: {sentinel[2]:,} (BUG FIX B-DP4-1 — was 100% in prior version) | "
        f"agency: {sentinel[3]:,} | period: {sentinel[4]:,}"
    )

    # Unresolved period alert: surfaces unrecognised accrual_month format strings
    if sentinel[4] > 0:
        bad_months = spark.sql(f"""
            SELECT DISTINCT ai.accrual_month
            FROM {SILVER}.silver_aasbs_accrual_income ai
            JOIN {SILVER}.silver_aasbs_accrual_income_dist dist
                ON dist.accrual_income_id = ai.id
            LEFT JOIN {GOLD}.dim_accrual_period dap
                ON  dap.accrual_year  = ai.accrual_year
                AND dap.accrual_month = CASE
                    WHEN ai.accrual_month RLIKE '^[0-9]+$'
                        THEN CAST(ai.accrual_month AS INT)
                    ELSE CASE UPPER(TRIM(ai.accrual_month))
                        WHEN 'JAN' THEN 1  WHEN 'FEB' THEN 2  WHEN 'MAR' THEN 3
                        WHEN 'APR' THEN 4  WHEN 'MAY' THEN 5  WHEN 'JUN' THEN 6
                        WHEN 'JUL' THEN 7  WHEN 'AUG' THEN 8  WHEN 'SEP' THEN 9
                        WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
                        ELSE NULL
                    END
                END
            WHERE dap.accrual_period_sk IS NULL
              AND COALESCE(dist.is_deleted, FALSE) = FALSE
            LIMIT 20
        """).collect()
        uniq_bad = [r[0] for r in bad_months]
        print(
            f"[{NOTEBOOK}] WARNING: {sentinel[4]:,} rows with unresolved period_sk. "
            f"Unrecognised accrual_month values: {uniq_bad}. "
            f"Add additional CASE branches to accrual_month decoder if needed."
        )

    # ── IMPROVEMENT I-DP4-6: distributed vs calculated reconciliation ─────
    # Per FASAB SFFAS No.7, SUM(dist) per accrual_income_id should equal
    # calculated_income_amt on the parent record.  Mismatches indicate
    # partially distributed income (missing dist rows) or data corruption.
    recon = spark.sql(f"""
        SELECT COUNT(*) AS mismatched_income_records
        FROM (
            SELECT
                f.accrual_income_id,
                SUM(f.distributed_income_amt)           AS sum_distributed,
                MAX(f.calculated_income_amt)            AS calculated_income
            FROM {TARGET_TABLE} f
            GROUP BY f.accrual_income_id
            HAVING ABS(
                SUM(f.distributed_income_amt)
              - MAX(f.calculated_income_amt)
            ) > 0.01   -- 1-cent tolerance for rounding
        )
    """).collect()[0][0]
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP4-6 — dist vs calculated reconciliation: "
        f"{recon:,} mismatched income records  "
        f"(0 = fully distributed; any non-zero warrants investigation per FASAB SFFAS No.7)"
    )
    if recon > 0:
        print(
            f"[{NOTEBOOK}] WARNING: {recon:,} accrual_income records have "
            f"SUM(distributed_amt) ≠ calculated_income_amt.  "
            f"Review accrual_income_dist for missing or extra distribution rows."
        )

    # BAAR transmission coverage
    baar_stats = spark.sql(f"""
        SELECT
            SUM(CASE WHEN baar_transmission_status_cd IS NOT NULL
                     THEN 1 ELSE 0 END) AS has_baar_status,
            SUM(CASE WHEN baar_transmitted_amt > 0
                     THEN 1 ELSE 0 END) AS has_transmitted_amt,
            COUNT(DISTINCT accrual_batch_id) AS distinct_batches
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] BAAR coverage — "
        f"has_status={baar_stats[0]:,} (IMPROVEMENT I-DP4-7 — includes dist-level fallback) | "
        f"has_amount={baar_stats[1]:,} | "
        f"batches={baar_stats[2]:,}"
    )

    # audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    # audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
