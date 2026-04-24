# Databricks notebook source
# =============================================================================
# iat/prime_fact_ia_amendment.py
# Primes assist_dev.iat.fact_ia_amendment
#
# Grain    : One row per ia_amendment.id
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   ia_sk              ← common.dim_ia via ia_amendment.ia_id
#   award_sk           ← CAST(-1) SENTINEL [SILVER-PENDING — I-DP9-5]
#                        ia_amendment has no award_id.
#                        Multi-hop path via acquisition_mod_ia is N:N
#                        (one IA funds many acquisitions) — no safe
#                        single-row resolution on prime.
#   amend_action_sk    ← iat.dim_amend_action via ia_amendment.ia_amend_action_cd
#   funding_sk         ← common.dim_funding via MIN(funding.id) per ia_id
#                        Pre-aggregated v_funding_per_ia
#   loa_sk             ← common.dim_loa PROXY — via funding.latest_funding_amendment_id
#                        → funding_amendment_loa.loa_id  (I-DP9-6)
#   agency_sk          ← common.dim_agency via ia.activity_address_cd
#                        Pre-aggregated v_agency_per_ia
#   amend_date_sk      ← COALESCE(service_agency_signed_dt, created_dt)
#                        → DATE_FORMAT YYYYMMDD INT
#   amendment_num      ← ia_amendment.piid_ext
#   amendment_status_cd← DERIVED: FINAL / PENDING_REQUEST / DRAFT
#                        (both signed → FINAL; service only → PENDING_REQUEST;
#                        neither → DRAFT)
#   cost_change_amt    ← LAG window: delta of (direct_cost + charges) per IA
#                        IMPROVEMENT I-DP9-4 — pre-computed in v_ia_amendment_with_deltas
#   revised_total_cost_amt ← ia_amendment.direct_cost_est_amt + charges_est_amt
#   pop_extension_days ← DATEDIFF(end_dt, LAG(end_dt)) per IA sequence
#                        IMPROVEMENT I-DP9-4 — pre-computed in v_ia_amendment_with_deltas
#   revised_pop_end_dt ← ia_amendment.end_dt
#   servicing_aac_cd   ← ia.serv_agency_cd via join ia_amendment.ia_id → ia.id
#   mod_type_cds       ← CAST(NULL) [SILVER GAP]
#   validation_override_flag   ← CAST(NULL) [SILVER GAP]
#   validation_override_reason ← CAST(NULL) [SILVER GAP]
#   loa_change_amt     ← SUM(funding_amendment_loa.loa_change_amt) per IA
#                        PROXY via funding.latest_funding_amendment_id (I-DP9-6)
#
# IMPROVEMENT I-DP9-4 — v_ia_amendment_with_deltas (built-in):
#   cost_change_amt and pop_extension_days both require LAG window functions.
#   Pre-computing in a named view before the INSERT:
#     (a) keeps the main INSERT readable and testable
#     (b) ensures both deltas use the same PARTITION BY ia_id ORDER BY id
#     (c) correctly handles NULL for the first amendment in each IA's history
#
# IMPROVEMENT I-DP9-5 — three-bucket sentinel FK report (built-in):
#   Expected sentinels on prime: award_sk (-1), loa_sk (-1)
#   Unexpected sentinels (investigate if non-zero): ia_sk, amend_action_sk, agency_sk
#   The post-load check reports each bucket separately.
#
# IMPROVEMENT I-DP9-6 — loa_change_amt PROXY annotation (built-in):
#   funding.latest_funding_amendment_id gives the most recent funding
#   amendment per IA, not the specific amendment paired to this ia_amendment.
#   No direct FK between ia_amendment and funding_amendment exists.
#   SUM(loa_change_amt) from funding_amendment_loa via this path is an IA-level
#   proxy, not an amendment-level delta. Annotated in code and post-load output.
#
# Ref: FAR Part 17.5 (Economy Act orders), 31 U.S.C. §1535, OMB A-123
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP9"
NOTEBOOK     = "prime_fact_ia_amendment"
TARGET_TABLE = "assist_dev.iat.fact_ia_amendment"

SILVER = "assist_dev.assist_finance"
IAT    = "assist_dev.iat"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_ia_amendment")
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
        FROM {SILVER}.silver_aasbs_ia_amendment
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source ia_amendment rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    # ─────────────────────────────────────────────────────────────────────

    # IMPROVEMENT I-DP9-4: v_ia_amendment_with_deltas
    # LAG window functions for cost_change_amt and pop_extension_days.
    # Both deltas use PARTITION BY ia_id ORDER BY id (insertion sequence).
    # NULL for the first amendment per IA (no prior row to diff against).
    #
    # revised_total_cost_amt = direct_cost_est_amt + charges_est_amt
    # cost_change_amt        = revised_total - LAG(revised_total)
    # pop_extension_days     = DATEDIFF(end_dt, LAG(end_dt))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_amendment_with_deltas AS
        SELECT
            id,
            ia_id,
            piid_ext,
            ia_amend_action_cd,
            action_description,
            start_dt,
            end_dt,
            direct_cost_est_amt,
            charges_est_amt,
            service_agency_signer_id,
            service_agency_signed_dt,
            request_agency_signed_dt,
            attach_hash_id,
            created_by_user_id,
            created_dt,
            updated_dt,
            serv_aac_placeholder,
            -- revised_total_cost: sum of both cost estimate fields
            COALESCE(direct_cost_est_amt, 0.00)
                + COALESCE(charges_est_amt, 0.00)                AS revised_total_cost_amt,
            -- cost_change_amt: delta from prior amendment in this IA's history
            -- NULL for the first amendment (LAG returns NULL when no prior row)
            ROUND(
                (COALESCE(direct_cost_est_amt, 0.00)
                 + COALESCE(charges_est_amt, 0.00))
                - LAG(
                    COALESCE(direct_cost_est_amt, 0.00)
                    + COALESCE(charges_est_amt, 0.00),
                    1, NULL
                ) OVER (PARTITION BY ia_id ORDER BY id ASC),
                2
            )                                                     AS cost_change_amt,
            -- pop_extension_days: calendar days added to PoP by this amendment
            -- NULL for the first amendment. Negative = PoP reduction.
            CASE
                WHEN end_dt IS NOT NULL
                 AND LAG(end_dt, 1, NULL) OVER (
                     PARTITION BY ia_id ORDER BY id ASC
                 ) IS NOT NULL
                THEN DATEDIFF(
                    CAST(end_dt AS DATE),
                    CAST(
                        LAG(end_dt, 1, NULL) OVER (
                            PARTITION BY ia_id ORDER BY id ASC
                        ) AS DATE
                    )
                )
                ELSE NULL
            END                                                   AS pop_extension_days
        FROM (
            -- Subquery to carry serv_agency_cd from ia for servicing_aac_cd
            SELECT
                iam.*,
                ia.serv_agency_cd AS serv_aac_placeholder
            FROM {SILVER}.silver_aasbs_ia_amendment iam
            LEFT JOIN {SILVER}.silver_aasbs_ia ia
                ON  ia.id = iam.ia_id
                AND COALESCE(ia.is_deleted, FALSE) = FALSE
            WHERE COALESCE(iam.is_deleted, FALSE) = FALSE
        )
    """)

    # 3b — agency_sk per ia_id
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_agency_per_ia AS
        SELECT
            ia.id                   AS ia_id,
            COALESCE(dag.agency_sk, -1) AS agency_sk
        FROM {SILVER}.silver_aasbs_ia ia
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = ia.activity_address_cd
            AND dag.is_current_flag     = TRUE
        WHERE COALESCE(ia.is_deleted, FALSE) = FALSE
    """)

    # 3c — funding_sk per ia_id (MIN funding.id per IA)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_funding_per_ia AS
        SELECT
            f.ia_id,
            COALESCE(dfund.funding_sk, -1)  AS funding_sk,
            MIN(f.id)                        AS min_funding_id,
            MIN(f.latest_funding_amendment_id) AS latest_fa_id
        FROM {SILVER}.silver_aasbs_funding f
        LEFT JOIN {COMMON}.dim_funding dfund
            ON  dfund.funding_id = f.id
        WHERE COALESCE(f.is_deleted, FALSE) = FALSE
        GROUP BY f.ia_id, dfund.funding_sk
    """)

    # 3d — loa_sk per ia_id via latest funding amendment LOA (PROXY — I-DP9-6)
    # Path: funding.latest_funding_amendment_id → funding_amendment_loa.loa_id
    # This is the latest FA's LOA, not this specific ia_amendment's LOA.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_per_ia AS
        SELECT
            vf.ia_id,
            COALESCE(dl.loa_sk, -1)              AS loa_sk,
            SUM(COALESCE(fal.loa_change_amt, 0)) AS loa_change_amt
        FROM v_funding_per_ia vf
        LEFT JOIN {SILVER}.silver_aasbs_funding_amendment_loa fal
            ON  fal.funding_amendment_id = vf.latest_fa_id
            AND COALESCE(fal.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_loa dl
            ON  dl.loa_id          = fal.loa_id
            AND dl.is_current_flag = TRUE
        GROUP BY vf.ia_id, dl.loa_sk
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            ia_sk,
            award_sk,
            amend_action_sk,
            funding_sk,
            loa_sk,
            agency_sk,
            amend_date_sk,
            source_ia_amendment_id,
            amendment_num,
            amendment_status_cd,
            cost_change_amt,
            revised_total_cost_amt,
            pop_extension_days,
            revised_pop_end_dt,
            servicing_aac_cd,
            mod_type_cds,
            validation_override_flag,
            validation_override_reason,
            loa_change_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Common dimension FKs ──────────────────────────────────── */

            -- ia_sk: direct confirmed FK via ia_amendment.ia_id
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,

            -- award_sk: SILVER-PENDING (IMPROVEMENT I-DP9-5 — expected sentinel)
            -- ia_amendment has no award_id column. The path via acquisition_mod_ia
            -- is N:N (one IA funds many acquisitions — no safe single FK on prime).
            CAST(-1 AS BIGINT)                                  AS award_sk,

            /* ── DP9 dimension FKs ─────────────────────────────────────── */

            -- amend_action_sk: ia_amendment.ia_amend_action_cd → dim_amend_action
            COALESCE(daa.amend_action_sk, -1)                   AS amend_action_sk,

            -- funding_sk: MIN(funding.id) per ia_id → dim_funding (pre-aggregated)
            COALESCE(vf.funding_sk, -1)                         AS funding_sk,

            -- loa_sk: PROXY via latest funding amendment LOA per IA (I-DP9-6)
            -- Expected sentinel on prime for IAs with no funding_amendment_loa.
            COALESCE(vl.loa_sk, -1)                             AS loa_sk,

            -- agency_sk: ia.activity_address_cd → dim_agency (pre-aggregated)
            COALESCE(vag.agency_sk, -1)                         AS agency_sk,

            /* ── Date FK ───────────────────────────────────────────────── */

            -- amend_date_sk: prefer service_agency_signed_dt (effective date)
            -- fall back to created_dt for unsigned/draft amendments
            CASE
                WHEN COALESCE(iam.service_agency_signed_dt, iam.created_dt) IS NOT NULL
                THEN CAST(
                    DATE_FORMAT(
                        COALESCE(iam.service_agency_signed_dt, iam.created_dt),
                        'yyyyMMdd'
                    ) AS INT
                )
                ELSE NULL
            END                                                 AS amend_date_sk,

            /* ── Source identifier ─────────────────────────────────────── */
            iam.id                                              AS source_ia_amendment_id,

            /* ── Amendment attributes ───────────────────────────────────── */

            -- amendment_num: stored in piid_ext (amendment sequence number)
            CASE
                WHEN iam.piid_ext IS NOT NULL
                THEN TRY_CAST(iam.piid_ext AS INT)
                ELSE NULL
            END                                                 AS amendment_num,

            -- amendment_status_cd: derived from signature date presence
            -- FINAL: both service and request agency have signed
            -- PENDING_REQUEST: service signed, waiting for requesting agency
            -- DRAFT: neither signed yet
            CASE
                WHEN iam.service_agency_signed_dt IS NOT NULL
                 AND iam.request_agency_signed_dt IS NOT NULL
                    THEN 'FINAL'
                WHEN iam.service_agency_signed_dt IS NOT NULL
                    THEN 'PENDING_REQUEST'
                ELSE 'DRAFT'
            END                                                 AS amendment_status_cd,

            /* ── Financial / PoP measures ───────────────────────────────── */

            -- cost_change_amt: LAG delta of revised total cost per IA sequence
            -- IMPROVEMENT I-DP9-4: pre-computed in v_ia_amendment_with_deltas
            -- NULL for first amendment in IA history (no prior row to diff)
            iam.cost_change_amt,

            -- revised_total_cost_amt: direct + charges for this amendment
            iam.revised_total_cost_amt,

            -- pop_extension_days: calendar days added to PoP by this amendment
            -- IMPROVEMENT I-DP9-4: pre-computed via DATEDIFF LAG in view
            -- NULL for first amendment; negative for PoP reductions
            iam.pop_extension_days,

            -- revised_pop_end_dt: new period of performance end date
            iam.end_dt                                          AS revised_pop_end_dt,

            -- servicing_aac_cd: servicing agency AAC from parent IA
            iam.serv_aac_placeholder                            AS servicing_aac_cd,

            /* ── Silver gap columns ─────────────────────────────────────── */

            -- mod_type_cds: SILVER GAP — no mod_type column on ia_amendment
            CAST(NULL AS STRING)                                AS mod_type_cds,

            -- validation_override_flag: SILVER GAP — no validation columns
            CAST(NULL AS BOOLEAN)                               AS validation_override_flag,

            -- validation_override_reason: SILVER GAP
            CAST(NULL AS STRING)                                AS validation_override_reason,

            /* ── LOA change amount ──────────────────────────────────────── */

            -- loa_change_amt: PROXY via latest funding amendment per IA (I-DP9-6)
            -- SUM(funding_amendment_loa.loa_change_amt) for latest FA per IA.
            -- Not per-amendment — uses the IA's most recent funding amendment.
            -- Annotated as PROXY; CDC will refine with per-amendment FK.
            COALESCE(vl.loa_change_amt, 0.00)                  AS loa_change_amt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM v_ia_amendment_with_deltas iam

        -- ia_sk: dim_ia via ia_amendment.ia_id
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = iam.ia_id
            AND dia.is_current_flag = TRUE

        -- amend_action_sk: dim_amend_action via ia_amend_action_cd
        LEFT JOIN {IAT}.dim_amend_action daa
            ON  daa.amend_action_cd = iam.ia_amend_action_cd

        -- funding_sk: pre-aggregated (MIN funding per IA)
        LEFT JOIN v_funding_per_ia vf
            ON  vf.ia_id = iam.ia_id

        -- loa_sk + loa_change_amt: pre-aggregated PROXY
        LEFT JOIN v_loa_per_ia vl
            ON  vl.ia_id = iam.ia_id

        -- agency_sk: pre-aggregated via ia.activity_address_cd
        LEFT JOIN v_agency_per_ia vag
            ON  vag.ia_id = iam.ia_id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # IMPROVEMENT I-DP9-5: three-bucket sentinel FK report
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    if rows_written != rows_read:
        print(
            f"[{NOTEBOOK}] WARNING: rows_written ({rows_written:,}) ≠ "
            f"source rows ({rows_read:,}). Check for fan-out in pre-agg views."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

    # IMPROVEMENT I-DP9-5: three-bucket sentinel report
    # Bucket A: expected sentinel (award_sk, loa_sk — documented on prime)
    # Bucket B: unexpected sentinel (ia_sk, amend_action_sk, agency_sk)
    sentinel = spark.sql(f"""
        SELECT
            -- Expected sentinels on prime (documented gap or proxy)
            SUM(CASE WHEN award_sk        = -1 THEN 1 ELSE 0 END) AS award_sentinel,
            SUM(CASE WHEN loa_sk          = -1 THEN 1 ELSE 0 END) AS loa_sentinel,
            -- Unexpected sentinels (should be resolved from Silver)
            SUM(CASE WHEN ia_sk           = -1 THEN 1 ELSE 0 END) AS ia_sentinel_unexpected,
            SUM(CASE WHEN amend_action_sk = -1 THEN 1 ELSE 0 END) AS action_sentinel_unexpected,
            SUM(CASE WHEN agency_sk       = -1 THEN 1 ELSE 0 END) AS agency_sentinel_unexpected,
            -- Amendment status distribution
            COUNT(CASE WHEN amendment_status_cd = 'FINAL'           THEN 1 END) AS final_cnt,
            COUNT(CASE WHEN amendment_status_cd = 'PENDING_REQUEST' THEN 1 END) AS pending_cnt,
            COUNT(CASE WHEN amendment_status_cd = 'DRAFT'           THEN 1 END) AS draft_cnt
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP9-5 — Sentinel FK three-bucket report:")
    print(f"  EXPECTED sentinels on prime (documented):")
    print(f"    award_sk = -1      : {sentinel[0]:,}  (N:N via acquisition_mod_ia — no safe resolution)")
    print(f"    loa_sk = -1        : {sentinel[1]:,}  (PROXY path — may be -1 when no funding_amendment_loa)")
    print(f"  UNEXPECTED sentinels (investigate if non-zero):")
    ok_ia     = "✓ OK" if sentinel[2] == 0 else f"⚠ INVESTIGATE — {sentinel[2]:,} unresolved IAs"
    ok_action = "✓ OK" if sentinel[3] == 0 else f"⚠ INVESTIGATE — {sentinel[3]:,} unresolved codes"
    ok_agency = "✓ OK" if sentinel[4] == 0 else f"⚠ INVESTIGATE — {sentinel[4]:,} unresolved agencies"
    print(f"    ia_sk = -1         : {sentinel[2]:,}  {ok_ia}")
    print(f"    amend_action_sk=-1 : {sentinel[3]:,}  {ok_action}")
    print(f"    agency_sk = -1     : {sentinel[4]:,}  {ok_agency}")

    print(f"\n  Amendment status distribution:")
    print(f"    FINAL           : {sentinel[5]:,}")
    print(f"    PENDING_REQUEST : {sentinel[6]:,}")
    print(f"    DRAFT           : {sentinel[7]:,}")

    fin = spark.sql(f"""
        SELECT
            ROUND(SUM(revised_total_cost_amt), 2) AS total_cost,
            ROUND(SUM(cost_change_amt), 2)         AS total_cost_delta,
            ROUND(SUM(loa_change_amt), 2)          AS total_loa_delta,
            SUM(CASE WHEN mod_type_cds IS NOT NULL THEN 1 ELSE 0 END) AS has_mod_type
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"\n  Financial — total_cost=${fin[0]:,.2f} | "
        f"cost_delta=${fin[1]:,.2f} | loa_delta=${fin[2]:,.2f} (PROXY)"
    )
    assert fin[3] == 0, \
        "ASSERT FAILED: mod_type_cds must be NULL on prime (Silver gap)"

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
