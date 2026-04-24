# Databricks notebook source
# =============================================================================
# iat/prime_fact_ia_review.py
# Primes assist_dev.iat.fact_ia_review
#
# Grain    : One row per ia_review.id
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   ia_sk              ← common.dim_ia via ia_review.ia_id
#   reviewer_sk        ← iat.dim_reviewer via ia_review.service_agency_reviewer_id
#   agency_sk          ← common.dim_agency via ia.activity_address_cd
#                        Pre-aggregated v_agency_per_ia_review
#   review_date_sk     ← common.dim_date via DATE_FORMAT(ia_review.review_dt)
#   review_fiscal_year ← YEAR(review_dt) + (MONTH >= 10 ? 1 : 0)
#                        US federal fiscal year Oct 1 – Sep 30
#                        IMPROVEMENT I-DP9-7
#   review_type_cd     ← CAST(NULL) [SILVER GAP — no review_type_cd on ia_review]
#   has_attachment_flag← ia_review.attach_hash_id IS NOT NULL
#   review_completed_flag ← CAST(NULL) [SILVER GAP — no completion status]
#   days_overdue       ← CAST(NULL) [SILVER GAP — OMB A-123 deadline not in Silver]
#
# IMPROVEMENT I-DP9-7 — FY derivation and Silver gap documentation (built-in):
#   review_fiscal_year: YEAR(review_dt) + CASE WHEN MONTH(review_dt) >= 10 THEN 1 ELSE 0 END
#   Reference: OMB A-123 mandates annual IA reviews within each federal FY.
#   Federal FY runs Oct 1 – Sep 30 per 31 U.S.C. §1102.
#   A review dated November 2024 belongs to FY2025.
#
#   Three Silver gap columns annotated with regulatory significance:
#     review_type_cd:      OMB A-123 distinguishes Annual, Renewal, Ad-hoc reviews.
#                          No type column on ia_review in Silver.
#     review_completed_flag: OMB A-123 tracks completion within the fiscal year.
#                            No completion status column on ia_review.
#     days_overdue:          Requires OMB A-123 annual review deadline date per IA per FY.
#                            No deadline column in Silver.
#
# Ref: FAR Part 17.5, 31 U.S.C. §1535, OMB A-123 (annual IA review requirement),
#      31 U.S.C. §1102 (federal fiscal year definition)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP9"
NOTEBOOK     = "prime_fact_ia_review"
TARGET_TABLE = "assist_dev.iat.fact_ia_review"

SILVER = "assist_dev.assist_finance"
IAT    = "assist_dev.iat"
COMMON = "assist_dev.common"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME"
)
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
        audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_ia_review
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source ia_review rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp view: agency_sk per ia_id
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_agency_per_ia_review AS
        SELECT
            ia.id                         AS ia_id,
            COALESCE(dag.agency_sk, -1)   AS agency_sk
        FROM {SILVER}.silver_aasbs_ia ia
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = ia.activity_address_cd
            AND dag.is_current_flag     = TRUE
        WHERE COALESCE(ia.is_deleted, FALSE) = FALSE
    """)
    print(f"[{NOTEBOOK}] v_agency_per_ia_review view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            ia_sk,
            reviewer_sk,
            agency_sk,
            review_date_sk,
            source_ia_review_id,
            review_fiscal_year,
            review_type_cd,
            has_attachment_flag,
            review_completed_flag,
            days_overdue,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── Common dimension FKs ──────────────────────────────────── */

            -- ia_sk: direct confirmed FK via ia_review.ia_id
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,

            /* ── DP9 dimension FKs ─────────────────────────────────────── */

            -- reviewer_sk: servicing agency reviewer → dim_reviewer
            COALESCE(drev.reviewer_sk, -1)                      AS reviewer_sk,

            /* ── Common dimension FKs cont'd ───────────────────────────── */

            -- agency_sk: via ia.activity_address_cd (pre-aggregated)
            COALESCE(vag.agency_sk, -1)                         AS agency_sk,

            /* ── Date FK ───────────────────────────────────────────────── */

            -- review_date_sk: ia_review.review_dt → YYYYMMDD INT
            CASE
                WHEN ir.review_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(ir.review_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS review_date_sk,

            /* ── Source identifier ─────────────────────────────────────── */
            ir.id                                               AS source_ia_review_id,

            /* ── Review attributes ──────────────────────────────────────── */

            -- IMPROVEMENT I-DP9-7: review_fiscal_year
            -- US Federal fiscal year runs Oct 1 – Sep 30 (31 U.S.C. §1102).
            -- OMB A-123 requires annual IA reviews within each FY.
            -- Derivation: FY = calendar year + 1 when month is Oct–Dec.
            -- e.g. review_dt = 2024-11-20 → FY2025
            --      review_dt = 2025-03-15 → FY2025
            CASE
                WHEN ir.review_dt IS NOT NULL
                THEN YEAR(ir.review_dt)
                     + CASE WHEN MONTH(ir.review_dt) >= 10 THEN 1 ELSE 0 END
                ELSE NULL
            END                                                 AS review_fiscal_year,

            -- SILVER GAP: review_type_cd
            -- OMB A-123 distinguishes Annual, Renewal, and Ad-hoc review types.
            -- silver_aasbs_ia_review has no review_type_cd column.
            -- ia_review.review_num is a sequence number, not a type code.
            CAST(NULL AS STRING)                                AS review_type_cd,

            -- has_attachment_flag: TRUE when a supporting document hash exists
            ia_review.attach_hash_id IS NOT NULL                AS has_attachment_flag,

            -- SILVER GAP: review_completed_flag
            -- OMB A-123 tracks whether the annual review was completed within
            -- the fiscal year. No completion status column on ia_review.
            CAST(NULL AS BOOLEAN)                               AS review_completed_flag,

            -- SILVER GAP: days_overdue
            -- IMPROVEMENT I-DP9-7: requires the OMB A-123 annual review deadline
            -- date per IA per fiscal year. No deadline column exists in Silver.
            -- Computation when available:
            --   days_overdue = DATEDIFF(review_dt, deadline_dt) when review_dt > deadline_dt
            --   0 when reviewed on time, NULL when not yet reviewed.
            CAST(NULL AS INT)                                   AS days_overdue,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_ia_review ir

        -- ia_sk: dim_ia via ia_review.ia_id
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = ir.ia_id
            AND dia.is_current_flag = TRUE

        -- reviewer_sk: dim_reviewer via service_agency_reviewer_id
        LEFT JOIN {IAT}.dim_reviewer drev
            ON  CAST(drev.reviewer_user_id AS STRING)
              = CAST(ir.service_agency_reviewer_id AS STRING)

        -- agency_sk: pre-aggregated via ia.activity_address_cd
        LEFT JOIN v_agency_per_ia_review vag
            ON  vag.ia_id = ir.ia_id

        -- Inline join for has_attachment_flag — reuse from ir alias
        LEFT JOIN {SILVER}.silver_aasbs_ia_review ia_review
            ON  ia_review.id = ir.id

        WHERE COALESCE(ir.is_deleted, FALSE) = FALSE
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

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                               AS total,
            SUM(CASE WHEN ia_sk        = -1 THEN 1 ELSE 0 END)                  AS ia_sentinel,
            SUM(CASE WHEN reviewer_sk  = -1 THEN 1 ELSE 0 END)                  AS reviewer_sentinel,
            SUM(CASE WHEN agency_sk    = -1 THEN 1 ELSE 0 END)                  AS agency_sentinel,
            SUM(CASE WHEN has_attachment_flag = TRUE  THEN 1 ELSE 0 END)        AS has_attachment,
            SUM(CASE WHEN review_type_cd IS NOT NULL  THEN 1 ELSE 0 END)        AS has_type_cd,
            SUM(CASE WHEN review_completed_flag IS NOT NULL THEN 1 ELSE 0 END)  AS has_completed,
            SUM(CASE WHEN days_overdue IS NOT NULL    THEN 1 ELSE 0 END)        AS has_overdue,
            MIN(review_fiscal_year)                                              AS min_fy,
            MAX(review_fiscal_year)                                              AS max_fy
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"ia: {stats[1]:,} | reviewer: {stats[2]:,} | agency: {stats[3]:,}"
    )
    print(
        f"[{NOTEBOOK}] Metrics — "
        f"has_attachment={stats[4]:,} | "
        f"FY range: {stats[8]} – {stats[9]}"
    )
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP9-7 — Silver gap assertions:\n"
        f"  review_type_cd:       {stats[5]:,} (expected 0)\n"
        f"  review_completed_flag:{stats[6]:,} (expected 0)\n"
        f"  days_overdue:         {stats[7]:,} (expected 0)"
    )

    assert stats[5] == 0, \
        "ASSERT FAILED: review_type_cd must be NULL on prime (Silver gap)"
    assert stats[6] == 0, \
        "ASSERT FAILED: review_completed_flag must be NULL on prime (Silver gap)"
    assert stats[7] == 0, \
        "ASSERT FAILED: days_overdue must be NULL on prime (Silver gap — OMB A-123 deadline not in Silver)"
    print(f"  ✓ All three Silver gap assertions passed")

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
