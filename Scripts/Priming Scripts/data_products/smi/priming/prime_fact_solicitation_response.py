# Databricks notebook source
# =============================================================================
# smi/prime_fact_solicitation_response.py
# Primes assist_dev.smi.fact_solicitation_response
#
# Grain    : One row per solicit_response.id
#            (one row per contractor per solicitation)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   solicitation_sk   ← alt.dim_solicitation
#                       via solicit_response.solicit_id
#   acquisition_sk    ← alt.dim_acquisition
#                       via solicit_response.solicit_id → solicit.acquisition_id
#   contractor_sk     ← alt.dim_contractor
#                       via solicit_response.solicit_company_id
#                       → solicit_company.ipartner_id
#                       → award_mod_company.ipartner_id → dim_contractor
#   posting_type_sk   ← smi.dim_posting_type via solicit_response.solicit_id
#   piid_sk           ← smi.dim_piid
#                       via award.solicit_response_id = solicit_response.id
#                       → award.id = dim_piid.piid_id  (winners only)
#   agency_sk         ← common.dim_agency
#                       via solicit.acquisition_id → acquisition.activity_address_cd
#   ia_sk             ← common.dim_ia  (IMPROVEMENT I-DP7-3)
#                       Winners:    award.acquisition_id → award_ia.award_id
#                                   → MIN(award_ia.ia_id) → dim_ia.ia_sk
#                       Non-winners: CAST(-1 AS BIGINT)  (no award exists)
#   response_date_sk  ← common.dim_date
#                       via solicit_response.latest_response_update_dt
#   award_date_sk     ← common.dim_date (winners only)
#                       via award.latest_award_mod_id
#                       → award_mod.co_signature_dt
#   is_winner_flag    ← solicit_response.winning_response_yn = 'Y'
#   is_small_business_flag ← IMPROVEMENT I-DP7-4
#                       via solicit_response.response_company_duns
#                       → alt.dim_contractor.duns_num → small_business_flag
#   award_total_amt   ← snap_award_mod_amts.cum_funded_amt (winners only)
#                       via award.latest_award_mod_id → snap.award_mod_id
#   bid_total_amt     ← NULL — DEFERRED (source system limitation)
#   bid_clin_count    ← NULL — DEFERRED (solicit_response_detail not in Silver)
#   bid_vs_award_variance ← NULL (bid_total_amt deferred)
#   competition_rank  ← NULL (bid data deferred)
#
# IMPROVEMENT I-DP7-1 (built-in):
#   Pre-flight guard verifies alt.dim_solicitation, alt.dim_acquisition,
#   and alt.dim_contractor (DP2) are populated before proceeding.
#
# IMPROVEMENT I-DP7-3 (built-in):
#   ia_sk: winner rows resolved via award_ia bridge; non-winner rows = -1.
#   Three-bucket post-load report:
#     (a) ia_sk resolved (winner rows with real ia_sk)
#     (b) ia_sk expected sentinel (non-winner rows — correct)
#     (c) ia_sk unexpected sentinel (winner rows where ia_sk still -1 — investigate)
#
# IMPROVEMENT I-DP7-4 (built-in):
#   is_small_business_flag via response_company_duns → alt.dim_contractor.duns_num.
#   Post-load: distribution of TRUE/FALSE/unresolvable (DUNS not in dim_contractor).
#
# IMPROVEMENT I-DP7-6 (built-in):
#   bid_total_amt, bid_clin_count, bid_vs_award_variance, competition_rank
#   annotated as DEFERRED — source system limitation, not a Silver gap.
#   ASSIST does not capture contractor bid prices; resolution requires a
#   source system enhancement, not merely Silver ingestion.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP7"
NOTEBOOK     = "prime_fact_solicitation_response"
TARGET_TABLE = "assist_dev.smi.fact_solicitation_response"

SILVER = "assist_dev.assist_finance"
SMI    = "assist_dev.smi"
ALT    = "assist_dev.alt"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_solicit_response")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # IMPROVEMENT I-DP7-1: Pre-flight — DP2 dims must be populated
    # alt.dim_solicitation, alt.dim_acquisition, alt.dim_contractor are
    # all sourced from DP2.  Missing DP2 dims → all fact FKs = sentinel -1.
    # ─────────────────────────────────────────────────────────────────────
    print(f"[{NOTEBOOK}] IMPROVEMENT I-DP7-1 — Pre-flight: checking DP2 dims...")
    dp2_checks = {
        "alt.dim_solicitation" : f"{ALT}.dim_solicitation",
        "alt.dim_acquisition"  : f"{ALT}.dim_acquisition",
        "alt.dim_contractor"   : f"{ALT}.dim_contractor",
    }
    preflight_ok = True
    for label, tbl in dp2_checks.items():
        cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").collect()[0][0]
        status = "OK" if cnt > 0 else "EMPTY"
        print(f"  {label:30s}  rows={cnt:>10,}  [{status}]")
        if cnt == 0:
            preflight_ok = False

    if not preflight_ok:
        err = (
            f"[{NOTEBOOK}] BLOCKED — one or more DP2 dims are empty. "
            f"DP2 prime must complete successfully before DP7 fact can run."
        )
        print(f"\n  ✗ {err}")
        audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
        dbutils.notebook.exit("BLOCKED_DP2_NOT_PRIMED")

    print(f"\n  ✓ DP2 pre-flight passed.")

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
        FROM {SILVER}.silver_aasbs_solicit_response
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source solicit_response rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Acquisition per solicitation
    # solicit_response.solicit_id → solicit.acquisition_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_acq_per_solicit AS
        SELECT
            s.id            AS solicit_id,
            s.acquisition_id
        FROM {SILVER}.silver_aasbs_solicit s
        WHERE COALESCE(s.is_deleted, FALSE) = FALSE
    """)

    # 3b — Award per solicit_response (winners only)
    # award.solicit_response_id links the winning response to its award.
    # Provides: award.id (for piid_sk), award.acquisition_id (for ia_sk),
    # award.latest_award_mod_id (for award_date + snap amounts).
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_award_per_response AS
        SELECT
            aw.solicit_response_id,
            aw.id                       AS award_id,
            aw.acquisition_id           AS award_acquisition_id,
            aw.latest_award_mod_id
        FROM {SILVER}.silver_aasbs_award aw
        WHERE COALESCE(aw.is_deleted, FALSE) = FALSE
          AND aw.solicit_response_id IS NOT NULL
    """)

    # 3c — ia_sk for winners: award_ia bridge
    # IMPROVEMENT I-DP7-3: ia_sk resolved only for winner rows.
    # Path: award.acquisition_id → award_ia.award_id → MIN(ia_id) → dim_ia
    # Non-winner rows remain at -1 (expected — no award to bridge through).
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_per_award_response AS
        SELECT
            awr.solicit_response_id,
            COALESCE(dia.ia_sk, -1) AS ia_sk
        FROM v_award_per_response awr
        LEFT JOIN {SILVER}.silver_aasbs_award aw2
            ON  aw2.id = awr.award_id
            AND COALESCE(aw2.is_deleted, FALSE) = FALSE
        LEFT JOIN (
            SELECT award_id, MIN(ia_id) AS ia_id
            FROM {SILVER}.silver_aasbs_award_ia
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY award_id
        ) awi ON awi.award_id = aw2.id
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = awi.ia_id
            AND dia.is_current_flag = TRUE
    """)

    # 3d — Award total and date per solicit_response
    # cum_funded_amt from snap_award_mod_amts (latest mod snapshot)
    # award_date from award_mod.co_signature_dt (CO execution date)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_award_amounts AS
        SELECT
            awr.solicit_response_id,
            COALESCE(sn.cum_funded_amt, 0.00) AS award_total_amt,
            am.co_signature_dt                AS award_co_signature_dt
        FROM v_award_per_response awr
        LEFT JOIN {SILVER}.silver_aasbs_snap_award_mod_amts sn
            ON  sn.award_mod_id = awr.latest_award_mod_id
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = awr.latest_award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
    """)

    # 3e — contractor_sk per solicit_response via ipartner chain
    # IMPROVEMENT I-DP7-4: is_small_business_flag also resolved here
    # Path: solicit_response.solicit_company_id → solicit_company.ipartner_id
    #       → award_mod_company.ipartner_id → dim_contractor
    # Also resolves is_small_business_flag via dim_contractor.small_business_flag
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_contractor_per_response AS
        SELECT
            sr.id                               AS solicit_response_id,
            dc_ipart.contractor_sk,
            dc_ipart.small_business_flag        AS contractor_sb_flag,
            -- IMPROVEMENT I-DP7-4: SB flag also via DUNS (simpler fallback)
            -- solicit_response.response_company_duns → dim_contractor.duns_num
            dc_duns.small_business_flag         AS duns_sb_flag
        FROM {SILVER}.silver_aasbs_solicit_response sr
        -- Path A: via ipartner_id chain
        LEFT JOIN {SILVER}.silver_aasbs_solicit_company sc
            ON  sc.id = sr.solicit_company_id
            AND COALESCE(sc.is_deleted, FALSE) = FALSE
        LEFT JOIN (
            SELECT
                ipartner_id,
                MIN(id) AS amc_id
            FROM {SILVER}.silver_aasbs_award_mod_company
            WHERE COALESCE(is_deleted, FALSE) = FALSE
              AND ipartner_id IS NOT NULL
            GROUP BY ipartner_id
        ) amc_dedup ON amc_dedup.ipartner_id = sc.ipartner_id
        LEFT JOIN {ALT}.dim_contractor dc_ipart
            ON  dc_ipart.award_mod_company_id = amc_dedup.amc_id
            AND dc_ipart.is_current_flag      = TRUE
        -- Path B: via response_company_duns (IMPROVEMENT I-DP7-4)
        LEFT JOIN {ALT}.dim_contractor dc_duns
            ON  dc_duns.duns_num        = sr.response_company_duns
            AND dc_duns.is_current_flag = TRUE
        WHERE COALESCE(sr.is_deleted, FALSE) = FALSE
    """)

    # 3e — agency_sk per solicitation
    # acquisition.activity_address_cd → dim_agency.activity_address_cd
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_agency_per_solicit AS
        SELECT
            acq.id          AS acquisition_id,
            dag.agency_sk
        FROM {SILVER}.silver_aasbs_acquisition acq
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = acq.activity_address_cd
            AND dag.is_current_flag     = TRUE
        WHERE COALESCE(acq.is_deleted, FALSE) = FALSE
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            solicitation_sk,
            acquisition_sk,
            contractor_sk,
            posting_type_sk,
            piid_sk,
            agency_sk,
            ia_sk,
            response_date_sk,
            award_date_sk,
            source_response_id,
            response_status_cd,
            is_winner_flag,
            is_small_business_flag,
            bid_total_amt,
            bid_clin_count,
            award_total_amt,
            bid_vs_award_variance,
            competition_rank,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── DP2 dimension FKs ──────────────────────────────────────── */

            -- solicitation_sk: solicit_response.solicit_id → alt.dim_solicitation
            COALESCE(dsol.solicitation_sk, -1)                  AS solicitation_sk,

            -- acquisition_sk: via solicit.acquisition_id → alt.dim_acquisition
            COALESCE(dacq.acquisition_sk, -1)                   AS acquisition_sk,

            -- contractor_sk: via ipartner_id chain (pre-aggregated view)
            COALESCE(cpr.contractor_sk, -1)                     AS contractor_sk,

            /* ── DP7 dimension FKs ──────────────────────────────────────── */

            -- posting_type_sk: smi.dim_posting_type keyed on solicit_id
            COALESCE(dpt.posting_type_sk, -1)                   AS posting_type_sk,

            -- piid_sk: dim_piid keyed on award.id (winners only; NULL for losers)
            dp.piid_sk,

            /* ── Common dimension FKs ───────────────────────────────────── */

            -- agency_sk: acquisition.activity_address_cd → dim_agency
            COALESCE(agn.agency_sk, -1)                         AS agency_sk,

            -- ia_sk: IMPROVEMENT I-DP7-3
            -- Winners:     resolved via award_ia bridge (v_ia_per_award_response)
            -- Non-winners: sentinel -1 (expected — no award exists)
            COALESCE(iaw.ia_sk, -1)                             AS ia_sk,

            /* ── Date FKs ───────────────────────────────────────────────── */

            -- response_date_sk: latest_response_update_dt → YYYYMMDD INT
            CASE
                WHEN sr.latest_response_update_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(sr.latest_response_update_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS response_date_sk,

            -- award_date_sk: CO signature date on latest mod (winners only)
            CASE
                WHEN aam.award_co_signature_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(aam.award_co_signature_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS award_date_sk,

            /* ── Source identifiers ─────────────────────────────────────── */
            sr.id                                               AS source_response_id,

            /* ── Response attributes ────────────────────────────────────── */

            -- response_status_cd: derived from winning flag and no-response flag
            CASE
                WHEN UPPER(COALESCE(sr.winning_response_yn, 'N')) = 'Y'
                    THEN 'AWARDED'
                WHEN UPPER(COALESCE(sr.no_response_yn, 'N')) = 'Y'
                    THEN 'NO_RESPONSE'
                ELSE 'SUBMITTED'
            END                                                 AS response_status_cd,

            -- is_winner_flag: winning_response_yn = 'Y'
            CASE
                WHEN UPPER(COALESCE(sr.winning_response_yn, 'N')) = 'Y'
                THEN TRUE ELSE FALSE
            END                                                 AS is_winner_flag,

            -- IMPROVEMENT I-DP7-4: is_small_business_flag
            -- Primary: ipartner chain SB flag via dim_contractor
            -- Fallback: DUNS-based SB flag from dim_contractor.duns_num
            -- FALSE (not NULL) when neither path resolves, to preserve NOT NULL contract
            COALESCE(cpr.contractor_sb_flag, cpr.duns_sb_flag, FALSE) AS is_small_business_flag,

            /* ── DEFERRED columns (IMPROVEMENT I-DP7-6) ────────────────── */

            -- bid_total_amt: DEFERRED — source system limitation.
            -- ASSIST does not capture contractor bid prices in solicit_response.
            -- This is NOT a Silver gap — the source system itself does not collect
            -- bid amounts. Resolution requires a source system enhancement, not
            -- merely Silver ingestion. NULL on prime and until source is enhanced.
            CAST(NULL AS DECIMAL(15, 2))                        AS bid_total_amt,

            -- bid_clin_count: DEFERRED — solicit_response_detail table not in
            -- Silver DDL (not ingested). Required to count priced CLIN lines per
            -- response. NULL pending Silver ingestion.
            CAST(NULL AS INT)                                   AS bid_clin_count,

            /* ── Award amounts (winners only) ───────────────────────────── */

            -- award_total_amt: snap_award_mod_amts.cum_funded_amt for winners
            -- NULL for non-winners (no award to reference)
            CASE
                WHEN UPPER(COALESCE(sr.winning_response_yn, 'N')) = 'Y'
                THEN aam.award_total_amt
                ELSE CAST(NULL AS DECIMAL(15, 2))
            END                                                 AS award_total_amt,

            /* ── Derived variance/rank (DEFERRED — depend on bid_total_amt) */

            -- bid_vs_award_variance: NULL — bid_total_amt is deferred
            CAST(NULL AS DECIMAL(15, 2))                        AS bid_vs_award_variance,

            -- competition_rank: NULL — requires bid amounts to compute price rank
            CAST(NULL AS INT)                                   AS competition_rank,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_solicit_response sr

        /* ── DP2 dim: solicitation ──────────────────────────────────────── */
        LEFT JOIN {ALT}.dim_solicitation dsol
            ON  dsol.solicit_id = sr.solicit_id

        /* ── DP2 dim: acquisition via solicit ───────────────────────────── */
        LEFT JOIN v_acq_per_solicit aps
            ON  aps.solicit_id = sr.solicit_id
        LEFT JOIN {ALT}.dim_acquisition dacq
            ON  dacq.acquisition_id = aps.acquisition_id

        /* ── DP2 dim: contractor via ipartner chain ─────────────────────── */
        LEFT JOIN v_contractor_per_response cpr
            ON  cpr.solicit_response_id = sr.id

        /* ── DP7 dim: posting type ──────────────────────────────────────── */
        LEFT JOIN {SMI}.dim_posting_type dpt
            ON  dpt.solicit_id = sr.solicit_id

        /* ── DP7 dim: PIID (winners only) ───────────────────────────────── */
        LEFT JOIN v_award_per_response apr
            ON  apr.solicit_response_id = sr.id
        LEFT JOIN {SMI}.dim_piid dp
            ON  dp.piid_id = apr.award_id

        /* ── Common: agency ─────────────────────────────────────────────── */
        LEFT JOIN v_agency_per_solicit agn
            ON  agn.acquisition_id = aps.acquisition_id

        /* ── IMPROVEMENT I-DP7-3: ia_sk winner-only via award_ia bridge ── */
        LEFT JOIN v_ia_per_award_response iaw
            ON  iaw.solicit_response_id = sr.id

        /* ── Award amounts and date ─────────────────────────────────────── */
        LEFT JOIN v_award_amounts aam
            ON  aam.solicit_response_id = sr.id

        WHERE COALESCE(sr.is_deleted, FALSE) = FALSE
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
            f"source rows ({rows_read:,}). Check temp view joins for fan-out."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

    # IMPROVEMENT I-DP7-3: three-bucket ia_sk report
    winner_stats = spark.sql(f"""
        SELECT
            SUM(CASE WHEN is_winner_flag = TRUE  THEN 1 ELSE 0 END) AS winner_count,
            SUM(CASE WHEN is_winner_flag = FALSE THEN 1 ELSE 0 END) AS non_winner_count,
            SUM(CASE WHEN is_winner_flag = TRUE AND ia_sk > 0 THEN 1 ELSE 0 END) AS winner_ia_resolved,
            SUM(CASE WHEN is_winner_flag = TRUE AND ia_sk = -1 THEN 1 ELSE 0 END) AS winner_ia_sentinel,
            SUM(CASE WHEN is_winner_flag = FALSE AND ia_sk = -1 THEN 1 ELSE 0 END) AS nonwinner_sentinel_expected
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP7-3 — ia_sk three-bucket report:")
    print(f"  Winners                          : {winner_stats[0]:>8,}")
    print(f"  Non-winners                      : {winner_stats[1]:>8,}")
    print(f"  ia_sk resolved (winners)         : {winner_stats[2]:>8,}  ✓ expected")
    print(f"  ia_sk unexpected sentinel (winners): {winner_stats[3]:>8,}"
          f"  {'⚠ investigate' if winner_stats[3] > 0 else '✓ OK'}")
    print(f"  ia_sk expected sentinel (non-win): {winner_stats[4]:>8,}  ✓ expected")

    # IMPROVEMENT I-DP7-4: SB flag distribution
    sb_stats = spark.sql(f"""
        SELECT
            SUM(CASE WHEN is_small_business_flag = TRUE  THEN 1 ELSE 0 END) AS sb_count,
            SUM(CASE WHEN is_small_business_flag = FALSE THEN 1 ELSE 0 END) AS non_sb_count
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP7-4 — is_small_business_flag:")
    print(f"  Small business  : {sb_stats[0]:>8,}")
    print(f"  Not small biz   : {sb_stats[1]:>8,}")
    total_rows = max(rows_written, 1)
    sb_pct = sb_stats[0] / total_rows * 100
    if sb_pct == 0:
        print(
            f"  ⚠ 0% SB responses — possible DUNS format mismatch between "
            f"solicit_response.response_company_duns and dim_contractor.duns_num. "
            f"Verify DUNS format consistency between DP2 and DP7 source data."
        )

    # IMPROVEMENT I-DP7-6: deferred column assertions
    deferred_check = spark.sql(f"""
        SELECT
            SUM(CASE WHEN bid_total_amt IS NOT NULL THEN 1 ELSE 0 END) AS bid_non_null,
            SUM(CASE WHEN bid_clin_count IS NOT NULL THEN 1 ELSE 0 END) AS clin_non_null
        FROM {TARGET_TABLE}
    """).collect()[0]
    assert deferred_check[0] == 0, \
        "ASSERT FAILED: bid_total_amt must be NULL (DEFERRED — source system limitation)"
    assert deferred_check[1] == 0, \
        "ASSERT FAILED: bid_clin_count must be NULL (DEFERRED — solicit_response_detail not ingested)"
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP7-6 — Deferred field assertions: PASSED")

    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN solicitation_sk = -1 THEN 1 ELSE 0 END) AS unresolved_sol,
            SUM(CASE WHEN acquisition_sk  = -1 THEN 1 ELSE 0 END) AS unresolved_acq,
            SUM(CASE WHEN contractor_sk   = -1 THEN 1 ELSE 0 END) AS unresolved_contr,
            SUM(CASE WHEN agency_sk       = -1 THEN 1 ELSE 0 END) AS unresolved_agency
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"\n[{NOTEBOOK}] Sentinel FKs — "
        f"solicitation: {sentinel[0]:,} | acquisition: {sentinel[1]:,} | "
        f"contractor: {sentinel[2]:,} | agency: {sentinel[3]:,}"
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
