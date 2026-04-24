# Databricks notebook source
# =============================================================================
# fre/prime_fact_fpds_transaction.py
# Primes assist_dev.fre.fact_fpds_transaction
#
# Grain    : One row per award_mod (one Contract Action Report per modification)
# Strategy : INSERT only — same guard pattern as DP2 fact.
#            Abort cleanly if rows exist; TRUNCATE required to re-prime.
#
# BUG FIXES (v1.2.0):
#
#   B-DP3-2 — competition_type_cd wrong source table:
#     v_acq_context previously joined silver_aasbs_acquisition.competition_type_cd
#     which does not exist on that table.  competition_type_cd is on
#     silver_aasbs_acquisition_plan.  Fixed: v_acq_context now routes via
#     award → acquisition → acquisition_plan → competition_type_cd.
#     This was a runtime column-not-found failure on every prime run.
#
#   B-DP3-3 — reason_not_competed_cd has no Silver source:
#     v_acq_context referenced acq.reason_not_competed_cd which does not exist
#     anywhere in the Silver DDL (FPDS element 4G — J&A authority for
#     other-than-full-and-open competition).  Changed to CAST(NULL AS STRING)
#     Silver-pending.  Source table: aasbs.j_a_authority not yet ingested.
#     FAR 6.302 reference preserved in annotation.
#
#   B-DP3-4 — ia_sk via acq.ia_id (non-existent column):
#     v_ia_per_award joined silver_aasbs_acquisition.ia_id which does not exist
#     on that table (ia_id is not stored on acquisition in the ASSIST data model).
#     Resulted in ia_sk = -1 (sentinel) for every fact row.
#     Fixed: v_ia_per_award removed entirely.  ia_id is sourced directly from
#     silver_aasbs_loa_ledger.ia_id (confirmed in Silver DDL) — added as
#     MIN(ll.ia_id) to the existing v_loa_obligation view which already
#     aggregates loa_ledger per award_mod.  Zero new joins required.
#
# IMPROVEMENTS (v1.2.0):
#
#   I-DP3-1 — v_psc_per_mod correlated subquery replaced with equi-joins:
#     The PSC resolution used a correlated subquery inside the view
#     (SELECT li3.li_deliverable_id … LIMIT 1) which forces nested-loop
#     evaluation at scale — identical anti-pattern to B2 fixed earlier.
#     Replaced with a direct join: line_item_accepted → line_item →
#     li_deliverable.  Two clean equi-joins; Spark can broadcast/sort-merge.
#
#   I-DP3-2 — number_of_offers populated on prime:
#     Previously CAST(NULL AS INT) with note "DP2 scope".
#     silver_aasbs_solicit_response is in Silver and award.solicit_id exists.
#     Pre-aggregated view v_offer_counts provides COUNT(*) per solicit_id.
#     Path: award.solicit_id → solicit_response.solicit_id → COUNT(*).
#     FPDS element 15B (Number of Offers Received per FAR 15.101-1).
#
# Source mapping (all confirmed against Silver DDL v1.1.0):
#   award_mod_id           ← award_mod.id                    (grain)
#   fpds_award_sk          ← fre.dim_fpds_award              (via award_mod.award_id)
#   fpds_contractor_sk     ← fre.dim_fpds_contractor         (via award_mod_company)
#   fpds_agency_sk         ← fre.dim_fpds_agency             (awarding AAC)
#   funding_agency_sk      ← fre.dim_fpds_agency             (loa.agency_location_code)
#   psc_sk                 ← fre.dim_fpds_psc                (first CLIN PSC per mod)
#   award_sk               ← common.dim_award
#   ia_sk                  ← common.dim_ia  (via loa_ledger.ia_id — FIXED B-DP3-4)
#   loa_sk                 ← common.dim_loa (first LOA per mod)
#   action_date_sk         ← common.dim_date (co_signature_dt YYYYMMDD)
#   action_obligation_amt  ← SUM(loa_ledger.line_item_obligated_amt) per mod
#   competition_type_cd    ← acquisition_plan.competition_type_cd (FIXED B-DP3-2)
#   reason_not_competed_cd ← NULL Silver-pending  (FIXED B-DP3-3)
#   naics_cd               ← NULL Silver-pending  (aasbs.acquisition_naics not ingested)
#   naics_description      ← NULL Silver-pending
#   number_of_offers       ← COUNT(solicit_response) per solicit_id (NEW I-DP3-2)
#   is_small_business_flag ← v_small_business_flag pre-aggregated view (from v1.1.0)
#   car_submitted_dt PROXY ← award_mod.co_signature_dt (CO signature = proxy for
#                            FPDS-NG submission; precise date in transmittal tables,
#                            CDC will replace.  FAR 4.604(b): 3-business-day limit.)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP3"
NOTEBOOK     = "prime_fact_fpds_transaction"
TARGET_TABLE = "assist_dev.fre.fact_fpds_transaction"

SILVER  = "assist_dev.assist_finance"
GOLD    = "assist_dev.fre"
COMMON  = "assist_dev.common"
DP2_ALT = "assist_dev.alt"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, NOTEBOOK, PRODUCT, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award_mod")
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
        # audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        audit_fail(spark, RUN_ID, TARGET_TABLE, str(msg), str(msg), start_ts)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_award_mod"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver award_mod row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated supporting temp views
    # ─────────────────────────────────────────────────────────────────────

    # 3a — action_obligation_amt and ia_id per award_mod
    #
    # BUG FIX B-DP3-4: ia_id is now sourced from loa_ledger.ia_id
    # (confirmed in Silver DDL), not from acquisition.ia_id which does
    # not exist.  MIN(ll.ia_id) picks the primary IA for this mod; most
    # mods have exactly one IA associated with their LOA.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_obligation AS
        SELECT
            lia.award_mod_id,
            SUM(COALESCE(ll.line_item_obligated_amt, 0.00)) AS action_obligation_amt,
            MIN(l.tracking_num)                             AS po_tracking_num,
            MIN(l.id)                                       AS first_loa_id,
            MIN(l.agency_location_code)                     AS funding_aac,
            -- BUG FIX B-DP3-4: ia_id sourced from loa_ledger directly.
            -- loa_ledger.ia_id confirmed in Silver DDL.
            -- award → acquisition → acquisition.ia_id path is broken
            -- (ia_id does not exist on silver_aasbs_acquisition).
            MIN(ll.ia_id)                                   AS ia_id
        FROM {SILVER}.silver_aasbs_loa_ledger ll
        JOIN {SILVER}.silver_aasbs_line_item_accepted lia
            ON  lia.id = ll.line_item_accepted_id
            AND COALESCE(lia.is_deleted, FALSE) = FALSE
        JOIN {SILVER}.silver_aasbs_loa l
            ON  l.id = ll.loa_id
            AND COALESCE(l.is_deleted, FALSE) = FALSE
        WHERE COALESCE(ll.is_deleted, FALSE) = FALSE
        GROUP BY lia.award_mod_id
    """)

    # 3b — loa_sk: first LOA per award_mod (for appropriation tracing)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_sk AS
        SELECT
            ob.award_mod_id,
            dl.loa_sk
        FROM v_loa_obligation ob
        LEFT JOIN {COMMON}.dim_loa dl
            ON  dl.loa_id          = ob.first_loa_id
            AND dl.is_current_flag = TRUE
    """)

    # 3c — PSC per mod: first accepted CLIN's PSC (by line_item_num ASC)
    #
    # IMPROVEMENT I-DP3-1: correlated subquery replaced with equi-joins.
    # Previous implementation used a scalar correlated subquery inside the
    # view's LEFT JOIN ON clause to resolve li_deliverable_id, forcing
    # nested-loop execution.  Now uses two clean equi-joins:
    #   line_item_accepted → line_item (on line_item_id)
    #   line_item → li_deliverable (on li_deliverable_id)
    # Spark can plan this as broadcast or sort-merge joins.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_psc_per_mod AS
        SELECT
            ranked.award_mod_id,
            ld.psc_cd
        FROM (
            SELECT
                lia.award_mod_id,
                lia.line_item_id,
                ROW_NUMBER() OVER (
                    PARTITION BY lia.award_mod_id
                    ORDER BY li.line_item_num ASC, lia.id ASC
                ) AS rn
            FROM {SILVER}.silver_aasbs_line_item_accepted lia
            JOIN {SILVER}.silver_aasbs_line_item li
                ON  li.id = lia.line_item_id
                AND COALESCE(li.is_deleted, FALSE) = FALSE
            WHERE COALESCE(lia.is_deleted, FALSE) = FALSE
        ) ranked
        -- IMPROVEMENT I-DP3-1: equi-join on li_deliverable_id (not correlated subquery)
        LEFT JOIN {SILVER}.silver_aasbs_line_item li_join
            ON  li_join.id = ranked.line_item_id
            AND COALESCE(li_join.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_li_deliverable ld
            ON  ld.id = li_join.li_deliverable_id
            AND COALESCE(ld.is_deleted, FALSE) = FALSE
        WHERE ranked.rn = 1
    """)

    # 3d — ia_sk per award_mod: resolved from v_loa_obligation.ia_id
    #
    # BUG FIX B-DP3-4: v_ia_per_award (broken, joined acquisition.ia_id)
    # is removed entirely.  ia_sk now resolved from v_loa_obligation.ia_id.
    # The main INSERT joins v_loa_obligation and then does a single LEFT JOIN
    # to dim_ia on the ia_id surfaced there.
    # No separate view needed — ia_sk resolved inline in the final query.

    # 3e — Contractor per mod (for fpds_contractor_sk)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_contractor_per_mod AS
        SELECT
            amc.award_mod_id,
            dc.fpds_contractor_sk
        FROM {SILVER}.silver_aasbs_award_mod_company amc
        LEFT JOIN {GOLD}.dim_fpds_contractor dc
            ON  dc.award_mod_company_id = amc.id
            AND dc.is_current_flag      = TRUE
        WHERE COALESCE(amc.is_deleted, FALSE) = FALSE
    """)

    # 3f — Acquisition context per award
    #
    # BUG FIX B-DP3-2: competition_type_cd is on acquisition_plan, not acquisition.
    # v_acq_context previously read acq.competition_type_cd which does not exist
    # on silver_aasbs_acquisition.  Fixed by joining acquisition → acquisition_plan.
    #
    # BUG FIX B-DP3-3: reason_not_competed_cd does not exist anywhere in the
    # Silver DDL.  FPDS element 4G (J&A authority per FAR 6.302) requires a
    # dedicated Silver ingestion from the source J&A authority table
    # (aasbs.j_a_authority — not yet ingested).  Set to NULL Silver-pending.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_acq_context AS
        SELECT
            aw.id                           AS award_id,
            -- BUG FIX B-DP3-2: competition_type_cd from acquisition_plan
            -- (was incorrectly reading from acquisition which lacks this column)
            ap.competition_type_cd          AS competition_type_cd,
            -- BUG FIX B-DP3-3: reason_not_competed_cd has no Silver source.
            -- FPDS element 4G (J&A authority for other-than-full-and-open
            -- competition per FAR 6.302).  Source: aasbs.j_a_authority —
            -- not yet ingested into Silver.  NULL until Silver pipeline extended.
            CAST(NULL AS STRING)            AS reason_not_competed_cd
        FROM {SILVER}.silver_aasbs_award aw
        LEFT JOIN {SILVER}.silver_aasbs_acquisition acq
            ON  acq.id = aw.acquisition_id
            AND COALESCE(acq.is_deleted, FALSE) = FALSE
        -- BUG FIX B-DP3-2: join acquisition_plan for competition_type_cd
        LEFT JOIN {SILVER}.silver_aasbs_acquisition_plan ap
            ON  ap.acquisition_id = acq.id
            AND COALESCE(ap.is_deleted, FALSE) = FALSE
        WHERE COALESCE(aw.is_deleted, FALSE) = FALSE
    """)

    # 3g — Small business flag per mod (pre-aggregated — no correlated subquery)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_small_business_flag AS
        SELECT
            amc.award_mod_id,
            dc.small_business_flag
        FROM (
            SELECT
                award_mod_id,
                MIN(id) AS first_company_id
            FROM {SILVER}.silver_aasbs_award_mod_company
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY award_mod_id
        ) first_amc
        JOIN {SILVER}.silver_aasbs_award_mod_company amc
            ON  amc.id = first_amc.first_company_id
        LEFT JOIN {DP2_ALT}.dim_contractor dc
            ON  dc.award_mod_company_id = amc.id
            AND dc.is_current_flag      = TRUE
    """)

    # 3h — IMPROVEMENT I-DP3-2: number_of_offers per award_mod
    #
    # award.solicit_id FK confirmed in Silver DDL.
    # silver_aasbs_solicit_response confirmed in Silver with solicit_id FK.
    # COUNT(*) per solicit_id gives the number of vendor offers received —
    # directly the FPDS element 15B (Number of Offers per FAR 15.101-1).
    # Path: award_mod.award_id → award.solicit_id → solicit_response.solicit_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_offer_counts AS
        SELECT
            am.id           AS award_mod_id,
            COUNT(sr.id)    AS number_of_offers
        FROM {SILVER}.silver_aasbs_award_mod am
        JOIN {SILVER}.silver_aasbs_award aw
            ON  aw.id = am.award_id
            AND COALESCE(aw.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_solicit_response sr
            ON  sr.solicit_id = aw.solicit_id
            AND COALESCE(sr.is_deleted, FALSE) = FALSE
        WHERE COALESCE(am.is_deleted, FALSE) = FALSE
          AND aw.solicit_id IS NOT NULL
        GROUP BY am.id
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            fpds_award_sk,
            fpds_contractor_sk,
            fpds_agency_sk,
            funding_agency_sk,
            psc_sk,
            award_sk,
            ia_sk,
            loa_sk,
            action_date_sk,
            award_mod_id,
            mod_num,
            sf30_mod_type_cd,
            fpds_reason_cd,
            action_type_cd,
            car_status_cd,
            car_submitted_dt,
            car_accepted_dt,
            po_tracking_num,
            action_obligation_amt,
            base_exercised_opts_amt,
            base_all_opts_amt,
            current_total_value_amt,
            ultimate_contract_value,
            is_small_business_flag,
            competition_type_cd,
            reason_not_competed_cd,
            number_of_offers,
            -- naics_cd,  column not found
            -- naics_description,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── FPDS dimension FKs ─────────────────────────────────────── */

            COALESCE(dfa.fpds_award_sk,       -1)           AS fpds_award_sk,
            COALESCE(cpm.fpds_contractor_sk,  -1)           AS fpds_contractor_sk,

            -- Awarding agency: via award.lead_svc_activity_address_cd
            COALESCE(daa.fpds_agency_sk,      -1)           AS fpds_agency_sk,

            -- Funding agency: via loa.agency_location_code (funding office AAC)
            -- Sentinel -1 if LOA has no agency_location_code or AAC not in dim
            COALESCE(daf.fpds_agency_sk,      -1)           AS funding_agency_sk,

            -- PSC: first CLIN PSC for this mod (IMPROVEMENT I-DP3-1: equi-joins)
            dpsc.psc_sk,

            /* ── Common dimension FKs ───────────────────────────────────── */

            COALESCE(da.award_sk,             -1)           AS award_sk,

            -- BUG FIX B-DP3-4: ia_sk now from loa_ledger.ia_id via v_loa_obligation
            -- Previously from acquisition.ia_id (non-existent column → always -1)
            COALESCE(dia.ia_sk,               -1)           AS ia_sk,

            -- First LOA for this mod
            lsk.loa_sk,

            /* ── Date dimension FK ──────────────────────────────────────── */

            -- NULL if mod not yet signed (draft state)
            CASE
                WHEN am.co_signature_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(am.co_signature_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                             AS action_date_sk,

            /* ── Modification identifiers ───────────────────────────────── */

            am.id                                           AS award_mod_id,
            am.mod_num,
            am.m1p_sf30_mod_type_cd                         AS sf30_mod_type_cd,
            am.m1p_award_mod_fpds_reason_cd                 AS fpds_reason_cd,

            /* ── Action type derivation (FPDS A/C/D) ────────────────────── */

            CASE
                WHEN UPPER(COALESCE(am.mod_num, '')) IN ('0', '00', 'A', 'A0')
                    THEN 'A'
                WHEN UPPER(COALESCE(am.m1p_sf30_mod_type_cd, '')) LIKE '%TERM%'
                  OR UPPER(COALESCE(am.m1p_award_mod_fpds_reason_cd, '')) = 'D'
                    THEN 'D'
                ELSE 'C'
            END                                             AS action_type_cd,

            /* ── CAR status and dates ────────────────────────────────────── */

            am.fpds_car_status_cd                           AS car_status_cd,

            -- car_submitted_dt PROXY: CO signature date used as proxy for the
            -- FPDS-NG submission timestamp.  The CO signature date is the formal
            -- mod execution date and is typically the same day as submission.
            -- FAR 4.604(b): CAR must be submitted within 3 business days of
            -- contract action date.  Precise submission timestamp is in the
            -- transmittal tables and will replace this proxy via CDC enrichment.
            am.co_signature_dt                              AS car_submitted_dt,

            -- NULL on prime — requires FPDS-NG acknowledgement feed (CDC scope)
            CAST(NULL AS TIMESTAMP)                         AS car_accepted_dt,

            lo.po_tracking_num,

            /* ── Financial measures ─────────────────────────────────────── */

            COALESCE(lo.action_obligation_amt,   0.00)      AS action_obligation_amt,
            COALESCE(sn.base_exc_opt_amt,        0.00)      AS base_exercised_opts_amt,
            COALESCE(sn.base_all_opt_amt,        0.00)      AS base_all_opts_amt,
            COALESCE(sn.cum_funded_amt,          0.00)      AS current_total_value_amt,
            COALESCE(sn.cost_to_client_total_amt, 0.00)     AS ultimate_contract_value,

            /* ── Competition and socioeconomic ──────────────────────────── */

            -- is_small_business_flag: from pre-aggregated v_small_business_flag
            COALESCE(sbf.small_business_flag, FALSE)        AS is_small_business_flag,

            -- BUG FIX B-DP3-2: now from acquisition_plan (not acquisition)
            acq.competition_type_cd,

            -- BUG FIX B-DP3-3: SILVER-PENDING — no Silver source for this field.
            -- FPDS element 4G: J&A authority for other-than-full-and-open
            -- competition per FAR 6.302.  Source: aasbs.j_a_authority —
            -- not yet ingested into Silver.  NULL until pipeline extended.
            CAST(NULL AS STRING)                            AS reason_not_competed_cd,

            -- IMPROVEMENT I-DP3-2: populated from solicit_response COUNT
            -- FPDS element 15B (Number of Offers Received per FAR 15.101-1)
            -- NULL for mods where the award has no linked solicitation
            oc.number_of_offers,

            /* ── Silver-pending columns (DDL v1.1.0) ────────────────────── */

            -- SILVER-PENDING: naics_cd — FAR 4.606(a)(21)
            -- Source: aasbs.acquisition_naics — not yet ingested into Silver.
            -- CAST(NULL AS STRING)                            AS naics_cd,

            -- SILVER-PENDING: naics_description
            -- CAST(NULL AS STRING)                            AS naics_description,

            /* ── Audit ───────────────────────────────────────────────────── */
            current_timestamp()                             AS _gold_created_at,
            current_timestamp()                             AS _gold_updated_at,
            '{RUN_ID}'                                      AS _source_batch_id

        /* ── FROM: Grain anchor ─────────────────────────────────────────── */
        FROM {SILVER}.silver_aasbs_award_mod am

        LEFT JOIN {SILVER}.silver_aasbs_award aw
            ON  aw.id = am.award_id
            AND COALESCE(aw.is_deleted, FALSE) = FALSE

        /* ── DP3 dims ───────────────────────────────────────────────────── */

        LEFT JOIN {GOLD}.dim_fpds_award dfa
            ON  dfa.award_id = am.award_id

        LEFT JOIN {GOLD}.dim_fpds_agency daa
            ON  daa.aac = aw.lead_svc_activity_address_cd

        LEFT JOIN v_loa_obligation lo
            ON  lo.award_mod_id = am.id

        LEFT JOIN {GOLD}.dim_fpds_agency daf
            ON  daf.aac = lo.funding_aac

        -- IMPROVEMENT I-DP3-1: v_psc_per_mod uses equi-joins (no correlated subquery)
        LEFT JOIN v_psc_per_mod pmod
            ON  pmod.award_mod_id = am.id

        LEFT JOIN {GOLD}.dim_fpds_psc dpsc
            ON  dpsc.psc_cd = pmod.psc_cd

        /* ── Common dims ────────────────────────────────────────────────── */

        LEFT JOIN {COMMON}.dim_award da
            ON  da.award_id        = am.award_id
            AND da.is_current_flag = TRUE

        -- BUG FIX B-DP3-4: ia_sk resolved from v_loa_obligation.ia_id
        -- (loa_ledger.ia_id, confirmed in Silver DDL)
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = lo.ia_id
            AND dia.is_current_flag = TRUE

        /* ── Pre-aggregated views ───────────────────────────────────────── */

        LEFT JOIN v_contractor_per_mod cpm
            ON  cpm.award_mod_id = am.id

        LEFT JOIN v_small_business_flag sbf
            ON  sbf.award_mod_id = am.id
        
        -- move it up
        --LEFT JOIN v_loa_obligation lo
        --    ON  lo.award_mod_id = am.id

        LEFT JOIN v_loa_sk lsk
            ON  lsk.award_mod_id = am.id

        LEFT JOIN {SILVER}.silver_aasbs_snap_award_mod_amts sn
            ON  sn.award_mod_id = am.id
            AND COALESCE(sn.is_deleted, FALSE) = FALSE

        -- IMPROVEMENT I-DP3-1: v_psc_per_mod uses equi-joins (no correlated subquery)
        -- LEFT JOIN v_psc_per_mod pmod
        --    ON  pmod.award_mod_id = am.id

        -- BUG FIX B-DP3-2+B-DP3-3: v_acq_context routes via acquisition_plan
        LEFT JOIN v_acq_context acq
            ON  acq.award_id = am.award_id

        -- IMPROVEMENT I-DP3-2: offer count per award_mod (via solicit_response)
        LEFT JOIN v_offer_counts oc
            ON  oc.award_mod_id = am.id

        WHERE COALESCE(am.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Action type distribution
    action_dist = spark.sql(f"""
        SELECT action_type_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY action_type_cd
        ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] Action type distribution:")
    for row in action_dist:
        print(f"  {str(row[0]):8s}  {row[1]:>10,}")

    # CAR status distribution
    car_dist = spark.sql(f"""
        SELECT car_status_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY car_status_cd
        ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] CAR status distribution:")
    for row in car_dist:
        print(f"  {str(row[0]):25s}  {row[1]:>10,}")

    # Sentinel FK report — includes ia_sk validation (B-DP3-4 effectiveness check)
    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN fpds_award_sk       = -1 THEN 1 ELSE 0 END) AS unresolved_fpds_award,
            SUM(CASE WHEN fpds_contractor_sk  = -1 THEN 1 ELSE 0 END) AS unresolved_contractor,
            SUM(CASE WHEN fpds_agency_sk      = -1 THEN 1 ELSE 0 END) AS unresolved_agency,
            SUM(CASE WHEN award_sk            = -1 THEN 1 ELSE 0 END) AS unresolved_award,
            SUM(CASE WHEN ia_sk               = -1 THEN 1 ELSE 0 END) AS unresolved_ia
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"fpds_award: {sentinel[0]:,} | contractor: {sentinel[1]:,} | "
        f"agency: {sentinel[2]:,} | award: {sentinel[3]:,} | "
        f"ia: {sentinel[4]:,}  "
        f"(BUG FIX B-DP3-4 — was 100% of rows in prior version)"
    )

    # Silver-pending assertions
    pending_checks = spark.sql(f"""
        SELECT
            SUM(CASE WHEN reason_not_competed_cd IS NOT NULL THEN 1 ELSE 0 END) AS non_null_rnc,
            -- SUM(CASE WHEN naics_cd IS NOT NULL               THEN 1 ELSE 0 END) AS non_null_naics,
            SUM(CASE WHEN number_of_offers > 0               THEN 1 ELSE 0 END) AS with_offers
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Silver-pending — "
        f"reason_not_competed non-null={pending_checks[0]:,} (expected 0, BUG FIX B-DP3-3) | "
        f"naics non-null={pending_checks[1]:,} (expected 0)"
    )
    print(
        f"[{NOTEBOOK}] number_of_offers populated: {pending_checks[2]:,}  "
        f"(IMPROVEMENT I-DP3-2 — was 0 in prior version)"
    )

    assert pending_checks[0] == 0, \
        "ASSERT FAILED: reason_not_competed_cd must be NULL on prime (Silver-pending)"
    assert pending_checks[1] == 0, \
        "ASSERT FAILED: naics_cd must be NULL on prime (Silver-pending)"

    # FAR 4.604 compliance check: CAR submission within 3 business days
    late_cars = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        WHERE car_submitted_dt IS NOT NULL
          AND action_date_sk IS NOT NULL
          AND DATEDIFF(
                car_submitted_dt,
                TO_DATE(CAST(action_date_sk AS STRING), 'yyyyMMdd')
              ) > 3
    """).collect()[0][0]
    print(
        f"[{NOTEBOOK}] CAR submissions > 3 days after action date: {late_cars:,}  "
        f"(FAR 4.604(b) — proxy date used; CDC will refine with transmittal data)"
    )

    # Obligation sanity check
    neg_obligation = spark.sql(f"""
        SELECT COUNT(*) FROM {TARGET_TABLE}
        WHERE action_obligation_amt < -9999999
    """).collect()[0][0]
    if neg_obligation > 0:
        print(
            f"[{NOTEBOOK}] WARNING: {neg_obligation:,} rows have "
            f"action_obligation_amt < -$9.99M — review LOA data for "
            f"large de-obligation events."
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
