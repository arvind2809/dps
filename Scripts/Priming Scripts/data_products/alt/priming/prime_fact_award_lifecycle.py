# Databricks notebook source
# MAGIC %md
# MAGIC ## prime_fact_award_lifecycle — DP2 Award Lifecycle Tracker
# MAGIC **Target:**  `assist_dev.alt.fact_award_lifecycle`
# MAGIC **Grain:**   CLIN (`line_item_sk`) × Acquisition (`acquisition_sk`) — current-state snapshot.
# MAGIC **Strategy:** INSERT only (no TRUNCATE). One initial load; CDC maintains future updates.
# MAGIC
# MAGIC **Sources (Silver → Gold):**
# MAGIC
# MAGIC | Silver Source | Provides |
# MAGIC |---|---|
# MAGIC | `silver_aasbs_line_item_accepted` | Grain anchor — one row per accepted CLIN |
# MAGIC | `silver_aasbs_award_mod` | Latest mod dates, mod counts, phase context |
# MAGIC | `silver_aasbs_award` | acquisition_id resolution for award_mod joins |
# MAGIC | `silver_aasbs_acceptance` | Latest acceptance date / count |
# MAGIC | `silver_aasbs_invoice` | Invoice counts (total / pending) |
# MAGIC | `silver_aasbs_accrual_expense` | Holdback amount (latest per CLIN) |
# MAGIC | `assist_dev.common.dim_line_item` | line_item_sk |
# MAGIC | `assist_dev.common.dim_award` | award_sk, base_award_amt, award date |
# MAGIC | `assist_dev.alt.dim_acquisition` | acquisition_sk, phase derivation |
# MAGIC | `assist_dev.alt.dim_solicitation` | solicitation_sk |
# MAGIC | `assist_dev.alt.dim_contractor` | contractor_sk (latest mod company) |
# MAGIC | `assist_dev.alt.dim_closeout` | closeout_sk |
# MAGIC | `assist_dev.common.dim_ia` | ia_sk |
# MAGIC | `assist_dev.common.dim_agency` | agency_sk |
# MAGIC | `assist_dev.common.dim_date` | award_date_sk, last_mod_date_sk |
# MAGIC
# MAGIC **FIX (v1.1.0) — acquisition_id join on award_mod:**
# MAGIC   `silver_aasbs_award_mod` does not carry `acquisition_id` directly;
# MAGIC   it links only via `award_id`.  The `acquisition_id` is on the `award` table.
# MAGIC   `v_award_mod_latest` and `v_latest_contractor` previously grouped on
# MAGIC   `award_mod.acquisition_id` (column does not exist), producing NULL joins
# MAGIC   for all mod counts and contractor lookups.  Fixed by routing through
# MAGIC   `award_mod.award_id → silver_aasbs_award.acquisition_id`.
# MAGIC
# MAGIC **Phase mapping (acquisition_status_cd → proc_phase):**
# MAGIC
# MAGIC | Status Code Pattern | proc_phase_cd | proc_phase_desc |
# MAGIC |---|---|---|
# MAGIC | CLOSE, CLOSEOUT, COMPLETE | CLOSEOUT | Closeout |
# MAGIC | POST_AWARD, INVOICE, ACCEPTANCE | POST_AWARD | Post-Award |
# MAGIC | AWARD, AWARDED | AWARD | Award |
# MAGIC | SOL, SOLICITATION, EVAL | SOLICITATION | Solicitation |
# MAGIC | All others / NULL | PRE_AWARD | Pre-Award |
# MAGIC
# MAGIC **Amount derivation:**
# MAGIC   - `base_award_amt`        — from `common.dim_award` (original mod 0 amount).
# MAGIC   - `current_obligated_amt` — CLIN-level proxy from `line_item_accepted.obligated_ceiling_amt`.
# MAGIC   - `current_accepted_amt`  — SUM of accepted amounts at CLIN grain.
# MAGIC   - `current_udo_amt`       — derived as `current_obligated_amt − current_accepted_amt` (floor 0.00).
# MAGIC   - `accrual_holdback_amt`  — seeded 0.00 on prime; DP4 enriches via CDC.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC   - `line_item_accepted.line_item_id` → `line_item.id` → `dim_line_item.line_item_sk`.
# MAGIC   - `line_item_accepted.acquisition_id` drives the acquisition FK join.
# MAGIC   - Latest contractor: most recent `award_mod` row by `mod_num DESC` via `award_mod_company`.
# MAGIC   - `days_in_pre_award`: acquisition.created_dt to award.award_dt (NULL if no award yet).
# MAGIC   - `days_since_last_mod`: latest `award_mod.mod_signed_dt` to current date.
# MAGIC   - `days_to_pop_end`: `line_item.pop_end_dt` to current date (negative = PoP expired).
# MAGIC   - Sentinel -1 used for all unresolvable surrogate key FKs.

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP2"
NOTEBOOK     = "prime_fact_award_lifecycle"
TARGET_TABLE = "assist_dev.alt.fact_award_lifecycle"

SILVER = "assist_dev.assist_finance"
GOLD   = "assist_dev.alt"
COMMON = "assist_dev.common"

# ADDED for utils
TASK   = "prime_fact_award_lifecycle"
dbutils.widgets.text("job_name", "dp2_prime_full", "Job Name")
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_line_item_accepted")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only. Verify table is empty before loading.
    # If rows already exist this prime has already run — abort cleanly.
    # ─────────────────────────────────────────────────────────────────────
    existing_rows = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]

    if existing_rows > 0:
        msg = (
            f"[{NOTEBOOK}] ABORTED — fact table already contains {existing_rows:,} rows. "
            f"Prime is INSERT-only. To re-prime, manually TRUNCATE {TARGET_TABLE} first."
        )
        print(msg)
        #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        #audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)

        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_line_item_accepted"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver line_item_accepted row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    #
    # All complex aggregations are isolated into temp views to prevent
    # fan-out in the main INSERT and to allow plan reuse across joins.
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Latest award_mod per acquisition.
    #
    # FIX: silver_aasbs_award_mod does not have an acquisition_id column —
    # it links to acquisition via award_mod.award_id → award.acquisition_id.
    # Previously this view grouped on award_mod.acquisition_id (non-existent
    # column), producing NULL acquisition_id for every row and breaking all
    # downstream joins that used this view.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_award_mod_latest AS
        SELECT
            aw.acquisition_id,
            MAX(am.mod_num)         AS latest_mod_num,
            COUNT(am.id)            AS mods_count,
            MAX(am.m1p_mod_start_dt)   AS last_mod_signed_dt
        FROM {SILVER}.silver_aasbs_award_mod am
        JOIN {SILVER}.silver_aasbs_award aw
            ON  aw.id = am.award_id
            AND COALESCE(aw.is_deleted, FALSE) = FALSE
        WHERE COALESCE(am.is_deleted, FALSE) = FALSE
        GROUP BY aw.acquisition_id
    """)

    # 3b — Latest contractor per acquisition.
    #
    # FIX: same root cause as v_award_mod_latest — routed through
    # award_mod.award_id → award.acquisition_id so the join is correct.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_latest_contractor AS
        SELECT
            aw.acquisition_id,
            amc.id AS award_mod_company_id
        FROM {SILVER}.silver_aasbs_award_mod am
        JOIN {SILVER}.silver_aasbs_award aw
            ON  aw.id = am.award_id
            AND COALESCE(aw.is_deleted, FALSE) = FALSE
        JOIN {SILVER}.silver_aasbs_award_mod_company amc
            ON  amc.award_mod_id = am.id
            AND COALESCE(amc.is_deleted, FALSE) = FALSE
        JOIN v_award_mod_latest aml
            ON  aml.acquisition_id = aw.acquisition_id
            AND aml.latest_mod_num = am.mod_num
        WHERE COALESCE(am.is_deleted, FALSE) = FALSE
    """)

    # 3c — Invoice counts per line_item_accepted_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_invoice_counts AS
        SELECT
            id as line_item_accepted_id,
            COUNT(*)                                                        AS total_invoices_count,
            SUM(
                CASE WHEN invoice_status_cd NOT IN ('ACCEPTED', 'CANCELLED', 'REJECTED')
                     THEN 1 ELSE 0 END
            )                                                               AS pending_invoices_count
        FROM {SILVER}.silver_aasbs_invoice
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY id
    """)

    # 3d — Accepted amounts per line_item_accepted_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_accepted_amts AS
        SELECT
            invoice_id as line_item_accepted_id,
            SUM(COALESCE(total_approved_invoice_amt, 0.00)) AS current_accepted_amt
        FROM {SILVER}.silver_aasbs_acceptance
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY invoice_id
    """)

    # 3e — Latest accrual holdback per line_item_accepted_id
    # Holdback seeded 0.00 on prime per design decision.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_accrual AS
        SELECT
            line_item_accepted_id,
            COALESCE(holdback_amt, 0.00) AS accrual_holdback_amt
        FROM (
            SELECT
                line_item_accepted_id,
                udo_amt as holdback_amt,
                ROW_NUMBER() OVER (
                    PARTITION BY line_item_accepted_id
                    ORDER BY accrual_month_end_dt DESC, id DESC
                ) AS rn
            FROM {SILVER}.silver_aasbs_accrual_expense
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        ) ranked
        WHERE rn = 1
    """)

    print(f"[{NOTEBOOK}] Supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_sk,
            award_sk,
            acquisition_sk,
            solicitation_sk,
            contractor_sk,
            closeout_sk,
            ia_sk,
            agency_sk,
            award_date_sk,
            last_mod_date_sk,
            current_proc_phase_cd,
            current_proc_phase_desc,
            days_in_pre_award,
            days_since_last_mod,
            days_to_pop_end,
            mods_count,
            total_invoices_count,
            pending_invoices_count,
            base_award_amt,
            current_obligated_amt,
            current_accepted_amt,
            current_udo_amt,
            accrual_holdback_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Dimension FKs ──────────────────────────────────────────── */

            COALESCE(dli.line_item_sk,   -1)                            AS line_item_sk,
            COALESCE(da.award_sk,        -1)                            AS award_sk,
            COALESCE(daq.acquisition_sk, -1)                            AS acquisition_sk,
            ds.solicitation_sk                                          AS solicitation_sk,
            dc.contractor_sk                                            AS contractor_sk,
            dco.closeout_sk                                             AS closeout_sk,
            COALESCE(dia.ia_sk,          -1)                            AS ia_sk,
            COALESCE(daq.agency_sk,      -1)                            AS agency_sk,
            -- NULL as agency_sk,

            /* ── Date dimension FKs ─────────────────────────────────────── */

            CAST(DATE_FORMAT(da.award_start_dt, 'yyyyMMdd') AS INT)           AS award_date_sk,
            CAST(DATE_FORMAT(aml.last_mod_signed_dt, 'yyyyMMdd') AS INT) AS last_mod_date_sk,

            /* ── Procurement phase ──────────────────────────────────────── */

            CASE
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%CLOSE%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%COMPLETE%'
                    THEN 'CLOSEOUT'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%POST_AWARD%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%INVOICE%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%ACCEPT%'
                    THEN 'POST_AWARD'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%AWARD%'
                    THEN 'AWARD'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%SOL%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%EVAL%'
                    THEN 'SOLICITATION'
                ELSE 'PRE_AWARD'
            END                                                         AS current_proc_phase_cd,

            CASE
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%CLOSE%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%COMPLETE%'
                    THEN 'Closeout'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%POST_AWARD%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%INVOICE%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%ACCEPT%'
                    THEN 'Post-Award'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%AWARD%'
                    THEN 'Award'
                WHEN UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%SOL%'
                  OR UPPER(COALESCE(daq.acquisition_status_cd, '')) LIKE '%EVAL%'
                    THEN 'Solicitation'
                ELSE 'Pre-Award'
            END                                                         AS current_proc_phase_desc,

            /* ── Cycle time metrics ─────────────────────────────────────── */

            CASE
                WHEN da.award_start_dt IS NOT NULL AND daq.created_dt IS NOT NULL
                THEN DATEDIFF(CAST(da.award_start_dt AS DATE), CAST(daq.created_dt AS DATE))
                ELSE NULL
            END                                                         AS days_in_pre_award,

            CASE
                WHEN aml.last_mod_signed_dt IS NOT NULL
                THEN DATEDIFF(CURRENT_DATE(), CAST(aml.last_mod_signed_dt AS DATE))
                ELSE NULL
            END                                                         AS days_since_last_mod,

            -- Negative values indicate PoP has expired (uninvoiced obligations at risk)
            CASE
                WHEN dli.li_pop_end_dt IS NOT NULL
                THEN DATEDIFF(CAST(dli.li_pop_end_dt AS DATE), CURRENT_DATE())
                ELSE NULL
            END                                                         AS days_to_pop_end,

            /* ── Modification and invoice counts ────────────────────────── */

            COALESCE(aml.mods_count,             0)                     AS mods_count,
            COALESCE(inv.total_invoices_count,   0)                     AS total_invoices_count,
            COALESCE(inv.pending_invoices_count, 0)                     AS pending_invoices_count,

            /* ── Financial measures ─────────────────────────────────────── */

            -- COALESCE(da.base_award_amt, 0.00)                           AS base_award_amt,
            0 as base_award_amt,

            -- CLIN-level ceiling proxy; CDC refines via loa_ledger join (DP1 scope)
            COALESCE(dli.obligated_ceiling_amt, 0.00)                   AS current_obligated_amt,

            COALESCE(acc.current_accepted_amt, 0.00)                    AS current_accepted_amt,

            -- UDO: floor at 0.00 — negative UDO not meaningful at CLIN grain
            GREATEST(
                COALESCE(dli.obligated_ceiling_amt, 0.00)
              - COALESCE(acc.current_accepted_amt,  0.00),
                0.00
            )                                                           AS current_udo_amt,

            -- Holdback seeded 0.00 on prime; DP4 enriches from accrual pipeline
            COALESCE(acr.accrual_holdback_amt, 0.00)                    AS accrual_holdback_amt,

            /* ── Audit ───────────────────────────────────────────────────── */
            current_timestamp()                                         AS _gold_created_at,
            current_timestamp()                                         AS _gold_updated_at,
            '{RUN_ID}'                                                  AS _source_batch_id

        /* ── FROM: Grain anchor ──────────────────────────────────────────── */
        FROM {SILVER}.silver_aasbs_line_item_accepted lia

        /* ADDED for missing columns */
        LEFT JOIN {SILVER}.silver_aasbs_award am
            on am.latest_award_mod_id = lia.award_mod_id

        /* ── Common dims (DP1 must have run first) ─────────────────────── */

        LEFT JOIN {COMMON}.dim_line_item dli
            ON  dli.line_item_accepted_id = lia.id
            AND dli.is_current_flag       = TRUE

        LEFT JOIN {COMMON}.dim_award da
            ON  da.award_mod_id         = lia.award_mod_id
            AND da.is_current_flag  = TRUE

        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = lia.id
            AND dia.is_current_flag = TRUE

        -- LEFT JOIN {COMMON}.dim_agency dag
        --    ON  dag.activity_address_code = lia.activity_address_code
        --    AND dag.is_current_flag       = TRUE

        /* ── DP2-specific dims ──────────────────────────────────────────── */

        LEFT JOIN {GOLD}.dim_acquisition daq
            --ON  daq.acquisition_id  = lia.acquisition_id
            ON  daq.acquisition_id  = am.acquisition_id
            AND daq.is_current_flag = TRUE

        LEFT JOIN {GOLD}.dim_solicitation ds
            ON  ds.acquisition_sk = daq.acquisition_sk

        LEFT JOIN {GOLD}.dim_closeout dco
            --ON  dco.acquisition_id = lia.acquisition_id
            ON  dco.acquisition_id = am.acquisition_id

        /* ── Aggregated sub-queries ──────────────────────────────────────── */

        -- FIX: v_award_mod_latest now joins via award to get acquisition_id
        LEFT JOIN v_award_mod_latest aml
            -- ON  aml.acquisition_id = lia.acquisition_id
            ON  aml.acquisition_id = am.acquisition_id

        -- FIX: v_latest_contractor now also routes via award.acquisition_id
        LEFT JOIN v_latest_contractor lc
            -- ON  lc.acquisition_id = lia.acquisition_id
            ON  lc.acquisition_id = am.acquisition_id

        LEFT JOIN {GOLD}.dim_contractor dc
            ON  dc.award_mod_company_id = lc.award_mod_company_id
            AND dc.is_current_flag      = TRUE

        LEFT JOIN v_invoice_counts inv
            ON  inv.line_item_accepted_id = lia.id

        LEFT JOIN v_accepted_amts acc
            ON  acc.line_item_accepted_id = lia.id

        LEFT JOIN v_accrual acr
            ON  acr.line_item_accepted_id = lia.id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Phase distribution
    phase_dist = spark.sql(f"""
        SELECT current_proc_phase_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY current_proc_phase_cd
        ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] Phase distribution:")
    for row in phase_dist:
        print(f"  {row[0]:20s} {row[1]:>8,}")

    # Sentinel FK report — validates the FIX by checking mods_count is non-zero
    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN line_item_sk    = -1 THEN 1 ELSE 0 END) AS unresolved_li,
            SUM(CASE WHEN award_sk        = -1 THEN 1 ELSE 0 END) AS unresolved_award,
            SUM(CASE WHEN acquisition_sk  = -1 THEN 1 ELSE 0 END) AS unresolved_acq,
            SUM(CASE WHEN ia_sk           = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN agency_sk       = -1 THEN 1 ELSE 0 END) AS unresolved_agency,
            SUM(CASE WHEN mods_count = 0       THEN 1 ELSE 0 END) AS zero_mod_count
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"line_item: {sentinel[0]:,}, award: {sentinel[1]:,}, "
        f"acquisition: {sentinel[2]:,}, ia: {sentinel[3]:,}, "
        f"agency: {sentinel[4]:,}"
    )
    print(
        f"[{NOTEBOOK}] zero mods_count rows: {sentinel[5]:,}  "
        f"(expected 0 after FIX — non-zero confirms award_mod join is working)"
    )

    neg_udo = spark.sql(f"""
        SELECT COUNT(*) FROM {TARGET_TABLE} WHERE current_udo_amt < 0
    """).collect()[0][0]
    if neg_udo > 0:
        print(f"[{NOTEBOOK}] WARNING: {neg_udo:,} rows have negative current_udo_amt.")

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
