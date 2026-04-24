# Databricks notebook source
# =============================================================================
# psc/prime_fact_clin_pricing.py
# Primes assist_dev.psc.fact_clin_pricing
#
# Grain    : One row per line_item_id × loa_id
#            (IMPROVEMENT I-DP6-2 — see grain annotation below)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   line_item_sk       ← common.dim_line_item
#                        via MIN(line_item_accepted.id) per line_item_id
#                        → dim_line_item.line_item_accepted_id
#   clin_subtype_sk    ← psc.dim_clin_subtype via line_item.id
#   cost_rate_type_sk  ← psc.dim_cost_rate_type (SC_LABOR only)
#                        via li_sc_labor.cost_rate_type_cd
#   award_sk           ← common.dim_award
#                        via line_item.parent_award_mod_id
#                        → award_mod.award_id → dim_award
#   ia_sk              ← common.dim_ia
#                        via line_item.parent_ia_id  (direct FK — confirmed)
#   loa_sk             ← common.dim_loa per LOA in the burn order
#   planned_amt        ← li_sc_labor.planned_amt / li_sc_fixed_fee.planned_amt
#                        / li_sc_travel.planned_amt / line_item.line_item_total_amt
#   fixed_fee_amt      ← li_sc_fixed_fee.planned_amt (CPFF CLINs)
#   travel_planned_amt ← li_sc_travel.planned_amt
#   actual_amt         ← NOT available on service_charge directly;
#                        derived as SUM(invoice_item.invoice_item_amt) per CLIN
#   actual_hours       ← SILVER-PENDING (service_charge_labor_cat not ingested)
#   planned_hours      ← SILVER-PENDING (service_charge_labor_cat not ingested)
#   planned_rate       ← SILVER-PENDING (service_charge_labor_cat not ingested)
#   award_fee_amt      ← SILVER-PENDING (award_fee_plan not ingested)
#   award_fee_surcharge_rate ← SILVER-PENDING (award_fee_eval not ingested)
#   loa_burn_order     ← loa_ledger burn order via line_item → line_item_accepted
#                        (no explicit burn_order column — row rank used)
#   loa_proration_pct  ← accrual_income_dist.proration_pct (not directly on loa_ledger;
#                        NULL on prime — no direct Silver join path confirmed)
#
# IMPROVEMENT I-DP6-2 — GRAIN ANNOTATION:
#   fact_clin_pricing includes loa_burn_order and loa_sk, which confirms the
#   designed grain is (line_item × LOA), NOT just per line_item.
#   A single CLIN funded by N LOAs produces N rows in this fact, one per LOA.
#   Pre-aggregated view v_loa_per_li expands line_item → line_item_accepted
#   → loa_ledger to produce one row per (line_item_id, loa_id) with the
#   LOA rank (burn order) assigned by ROW_NUMBER() over MIN(loa_ledger.id).
#   This is the primary fan-out concern for this fact — the view is essential.
#
# IMPROVEMENT I-DP6-5 — Silver-pending NULL annotations:
#   planned_hours, planned_rate  ← aasbs.service_charge_labor_cat (not ingested)
#   award_fee_amt                ← aasbs.award_fee_plan           (not ingested)
#   award_fee_surcharge_rate     ← aasbs.award_fee_eval           (not ingested)
#   actual_hours                 ← aasbs.service_charge_labor_cat (not ingested)
#   Ref: FAR 15.404 (price reasonableness), FAR 16.306 (CPFF contracts)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP6"
NOTEBOOK     = "prime_fact_clin_pricing"
TARGET_TABLE = "assist_dev.psc.fact_clin_pricing"

SILVER = "assist_dev.assist_finance"
PSC    = "assist_dev.psc"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_line_item")
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
        SELECT COUNT(DISTINCT li.id)
        FROM {SILVER}.silver_aasbs_line_item li
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND (
              li.li_deliverable_id IS NOT NULL OR li.li_sc_labor_id IS NOT NULL OR
              li.li_sc_fixed_fee_id IS NOT NULL OR li.li_sc_travel_id IS NOT NULL OR
              li.li_award_fee_id IS NOT NULL
          )
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Distinct subtype CLINs (source grain before LOA expansion): {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Primary line_item_accepted per line_item (for line_item_sk + invoice join)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_primary_lia AS
        SELECT
            line_item_id,
            MAX(id)     AS lia_id
        FROM {SILVER}.silver_aasbs_line_item_accepted
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY line_item_id
    """)

    # 3b — IMPROVEMENT I-DP6-2: expand line_item × LOA burn order
    # Path: line_item → line_item_accepted → loa_ledger.loa_id
    # ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY MIN(loa_ledger.id))
    # provides a deterministic burn order when no explicit priority exists.
    # One row per (line_item_id, loa_id) — this is the expanded grain.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_per_li AS
        SELECT
            li.line_item_id,
            li.loa_id,
            ROW_NUMBER() OVER (
                PARTITION BY li.line_item_id
                ORDER BY MIN(li.ll_id) ASC
            )                       AS loa_burn_order
        FROM (
            SELECT
                lia.line_item_id,
                ll.loa_id,
                MIN(ll.id)          AS ll_id
            FROM {SILVER}.silver_aasbs_line_item_accepted lia
            JOIN {SILVER}.silver_aasbs_loa_ledger ll
                ON  ll.line_item_accepted_id = lia.id
                AND COALESCE(ll.is_deleted, FALSE) = FALSE
            WHERE COALESCE(lia.is_deleted, FALSE) = FALSE
            GROUP BY lia.line_item_id, ll.loa_id
        ) li
        GROUP BY li.line_item_id, li.loa_id
    """)

    # 3c — Actual amount per line_item from invoice_item
    # SUM(invoice_item.invoice_item_amt) per line_item_accepted → line_item
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_actual_per_li AS
        SELECT
            lia.line_item_id,
            SUM(COALESCE(ii.invoice_item_amt, 0.00))    AS actual_amt
        FROM {SILVER}.silver_aasbs_line_item_accepted lia
        JOIN {SILVER}.silver_aasbs_invoice_item ii
            ON  ii.cost_line_item_accepted_id = lia.id
            AND COALESCE(ii.is_deleted, FALSE) = FALSE
        WHERE COALESCE(lia.is_deleted, FALSE) = FALSE
        GROUP BY lia.line_item_id
    """)

    # 3d — Planned amount per line_item (COALESCE across all subtype tables)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_planned_per_li AS
        SELECT
            li.id               AS line_item_id,
            -- Planned amount: sourced from the active subtype table.
            -- li_sc_labor.planned_amt, li_sc_fixed_fee.planned_amt, li_sc_travel.planned_amt
            -- all represent the planned billing amount for their CLIN subtype.
            -- For DELIVERABLE and AWARD_FEE: line_item.line_item_total_amt is used.
            COALESCE(
                sl.planned_amt,
                sff.planned_amt,
                st.planned_amt,
                li.line_item_total_amt
            )                   AS planned_amt,
            -- fixed_fee_amt: li_sc_fixed_fee.planned_amt (CPFF CLINs)
            sff.planned_amt     AS fixed_fee_amt,
            -- travel_planned_amt: li_sc_travel.planned_amt
            st.planned_amt      AS travel_planned_amt,
            -- cost_rate_type_cd: SC_LABOR only
            sl.cost_rate_type_cd
        FROM {SILVER}.silver_aasbs_line_item li
        LEFT JOIN {SILVER}.silver_aasbs_li_sc_labor sl
            ON  sl.id = li.li_sc_labor_id
            AND COALESCE(sl.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_li_sc_fixed_fee sff
            ON  sff.id = li.li_sc_fixed_fee_id
            AND COALESCE(sff.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_li_sc_travel st
            ON  st.id = li.li_sc_travel_id
            AND COALESCE(st.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # One row per line_item × LOA (IMPROVEMENT I-DP6-2 grain)
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_sk,
            clin_subtype_sk,
            cost_rate_type_sk,
            award_sk,
            ia_sk,
            loa_sk,
            planned_amt,
            planned_hours,
            planned_rate,
            award_fee_amt,
            award_fee_surcharge_rate,
            fixed_fee_amt,
            travel_planned_amt,
            actual_amt,
            actual_hours,
            loa_burn_order,
            loa_proration_pct,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Common dimension FKs ──────────────────────────────────── */

            -- line_item_sk: primary line_item_accepted → dim_line_item
            COALESCE(dli.line_item_sk, -1)                      AS line_item_sk,

            -- clin_subtype_sk: from psc.dim_clin_subtype by line_item_id
            COALESCE(dcs.clin_subtype_sk, -1)                   AS clin_subtype_sk,

            -- cost_rate_type_sk: SC_LABOR CLINs only; NULL for others
            dcr.cost_rate_type_sk,

            -- award_sk: line_item.parent_award_mod_id → award_mod → dim_award
            COALESCE(da.award_sk, -1)                           AS award_sk,

            -- ia_sk: line_item.parent_ia_id (direct FK — confirmed in Silver DDL)
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,

            -- loa_sk: per LOA in the burn-order expansion
            COALESCE(dl.loa_sk, -1)                             AS loa_sk,

            /* ── Financial measures ─────────────────────────────────────── */

            -- planned_amt: from subtype table, falling back to line_item_total_amt
            COALESCE(pln.planned_amt, 0.00)                     AS planned_amt,

            -- SILVER-PENDING: planned_hours
            -- Source: aasbs.service_charge_labor_cat — not yet ingested.
            -- Required for labour CLIN cost analysis per FAR 15.404.
            CAST(NULL AS DECIMAL(10, 2))                        AS planned_hours,

            -- SILVER-PENDING: planned_rate (cost per hour)
            -- Source: aasbs.service_charge_labor_cat — not yet ingested.
            CAST(NULL AS DECIMAL(10, 4))                        AS planned_rate,

            -- SILVER-PENDING: award_fee_amt
            -- Source: aasbs.award_fee_plan — not yet ingested.
            -- Ref: FAR 16.306 (cost-plus award-fee contracts).
            CAST(NULL AS DECIMAL(15, 2))                        AS award_fee_amt,

            -- SILVER-PENDING: award_fee_surcharge_rate
            -- Source: aasbs.award_fee_eval — not yet ingested.
            CAST(NULL AS DECIMAL(7, 4))                         AS award_fee_surcharge_rate,

            -- fixed_fee_amt: li_sc_fixed_fee.planned_amt (CPFF CLINs)
            pln.fixed_fee_amt,

            -- travel_planned_amt: li_sc_travel.planned_amt
            pln.travel_planned_amt,

            -- actual_amt: SUM(invoice_item.invoice_item_amt) per CLIN
            COALESCE(act.actual_amt, 0.00)                      AS actual_amt,

            -- SILVER-PENDING: actual_hours
            -- Source: aasbs.service_charge_labor_cat — not yet ingested.
            CAST(NULL AS DECIMAL(10, 2))                        AS actual_hours,

            /* ── LOA burn-order fields ───────────────────────────────────── */

            -- IMPROVEMENT I-DP6-2: loa_burn_order from v_loa_per_li
            -- One row per line_item × LOA; burn order = LOA funding priority rank
            loa_x_li.loa_burn_order,

            -- loa_proration_pct: no direct Silver column on loa_ledger.
            -- accrual_income_dist.proration_pct holds this for accrual context
            -- but not the billing proration specifically. NULL on prime.
            CAST(NULL AS DECIMAL(7, 4))                         AS loa_proration_pct,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        /* ── FROM: line_item expanded by LOA burn order ─────────────────── */
        FROM {SILVER}.silver_aasbs_line_item li

        /* ── IMPROVEMENT I-DP6-2: LOA expansion (grain = line_item × LOA) */
        JOIN v_loa_per_li loa_x_li
            ON  loa_x_li.line_item_id = li.id

        /* ── Common dims ─────────────────────────────────────────────────── */
        LEFT JOIN v_primary_lia plia
            ON  plia.line_item_id = li.id

        LEFT JOIN {COMMON}.dim_line_item dli
            ON  dli.line_item_accepted_id = plia.lia_id
            AND dli.is_current_flag       = TRUE

        LEFT JOIN {PSC}.dim_clin_subtype dcs
            ON  dcs.line_item_id = li.id

        LEFT JOIN v_planned_per_li pln
            ON  pln.line_item_id = li.id

        LEFT JOIN {PSC}.dim_cost_rate_type dcr
            ON  dcr.cost_rate_type_cd = pln.cost_rate_type_cd

        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = li.parent_award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE

        LEFT JOIN {COMMON}.dim_award da
            ON  da.award_id        = am.award_id
            AND da.is_current_flag = TRUE

        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = li.parent_ia_id
            AND dia.is_current_flag = TRUE

        LEFT JOIN {COMMON}.dim_loa dl
            ON  dl.loa_id          = loa_x_li.loa_id
            AND dl.is_current_flag = TRUE

        LEFT JOIN v_actual_per_li act
            ON  act.line_item_id = li.id

        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND (
              li.li_deliverable_id IS NOT NULL OR li.li_sc_labor_id IS NOT NULL OR
              li.li_sc_fixed_fee_id IS NOT NULL OR li.li_sc_travel_id IS NOT NULL OR
              li.li_award_fee_id IS NOT NULL
          )
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP6-2: burn order distribution
    burn_dist = spark.sql(f"""
        SELECT loa_burn_order, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY loa_burn_order
        ORDER BY loa_burn_order
    """).collect()
    print(f"[{NOTEBOOK}] IMPROVEMENT I-DP6-2 — LOA burn order distribution:")
    for row in burn_dist:
        print(f"  burn_order={row[0]:>3}  rows={row[1]:>8,}")
    expansion = rows_written / max(rows_read, 1)
    print(f"  Average LOA count per CLIN: {expansion:.2f}x  ({rows_read:,} CLINs → {rows_written:,} rows)")

    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN line_item_sk   = -1 THEN 1 ELSE 0 END) AS unresolved_li,
            SUM(CASE WHEN award_sk       = -1 THEN 1 ELSE 0 END) AS unresolved_award,
            SUM(CASE WHEN ia_sk          = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN loa_sk         = -1 THEN 1 ELSE 0 END) AS unresolved_loa,
            SUM(CASE WHEN planned_hours IS NOT NULL THEN 1 ELSE 0 END) AS non_null_planned_hours
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"li: {sentinel[0]:,} | award: {sentinel[1]:,} | "
        f"ia: {sentinel[2]:,} | loa: {sentinel[3]:,}"
    )
    assert sentinel[4] == 0, \
        "ASSERT FAILED: planned_hours must be NULL on prime (SILVER-PENDING)"

    fin = spark.sql(f"""
        SELECT
            ROUND(SUM(planned_amt), 2) AS total_planned,
            ROUND(SUM(actual_amt),  2) AS total_actual
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(f"[{NOTEBOOK}] Financial — planned=${fin[0]:,.2f} | actual=${fin[1]:,.2f}")

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
