# Databricks notebook source
# =============================================================================
# common/prime_dim_ia.py
# Primes assist_dev.common.dim_ia
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per ia.id
# SCD Type : 2  (eff_start_dt, eff_end_dt, is_current_flag)
#
# Source tables (Silver):
#   silver_aasbs_ia                         → base IA record
#   silver_aasbs_lu_ia_status               → ia_status_desc decode
#   silver_aasbs_lu_activity_address_code   → servicing_agency_sk + program_cd + region_cd
#   assist_dev.common.dim_agency        → servicing_agency_sk resolution
#
# IMPROVEMENT #1 (v1.1.0):
#   program_cd and region_cd are now populated on prime.
#   silver_aasbs_lu_activity_address_code already joined for agency resolution —
#   extracting aac.program_cd and aac.region_cd costs zero additional joins.
#   Previously these were always NULL on prime (delta refresh dependency removed).
#
# IMPROVEMENT #2 (v1.1.0):
#   ia_type_cd now derives a meaningful high-level grouping from instrument_type_cd
#   via a three-bucket CASE expression (GT_AND_C / ORDER / INTRA_AGENCY).
#   Previously ia_type_cd duplicated instrument_type_cd exactly, making it redundant.
#
# NULL on prime (by design — enriched by delta refresh or downstream jobs):
#   ia_end_dt           — from amendment ginv_pop_end_dt (delta refresh)
#   requesting_agency_sk — not directly on IA; resolved via LOA/funding chain
#   total_direct_cost_est_amt — aggregated from funding_amendment_loa
#   total_charges_est_amt     — same
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_ia")
TASK     = "prime_dim_ia"

# Silver sources
S_IA     = silver("aasbs", "ia")
S_STATUS = silver("aasbs", "lu_ia_status")
S_AAC    = silver("aasbs", "lu_activity_address_code")

# Gold dim (must exist — primed by prime_dim_agency earlier in DP1 job)
G_AGENCY = gold("common", "dim_agency")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="aasbs", source_table="ia",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            ia_id,
            ia_num,
            ia_type_cd,
            ia_type_desc,
            ia_status_cd,
            ia_status_desc,
            instrument_type_cd,
            instrument_type_desc,
            fiscal_year,
            ia_start_dt,
            ia_end_dt,
            total_direct_cost_est_amt,
            total_charges_est_amt,
            servicing_agency_sk,
            requesting_agency_sk,
            program_cd,
            region_cd,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 1: Base IA record with status decode
        -- ─────────────────────────────────────────────────────────────────
        WITH ia_base AS (
            SELECT
                ia.id                               AS ia_id,
                ia.piid                             AS ia_num,
                ia.instrument_type_cd,
                ia.ia_status_cd,
                ia.fiscal_year,
                ia.ia_began_dt                      AS ia_start_dt,
                -- ia_end_dt: not on ia row directly; sourced from latest IA amendment
                -- ginv_pop_end_dt in delta refresh.  NULL on prime by design.
                CAST(NULL AS TIMESTAMP)             AS ia_end_dt,
                -- Cost estimates: aggregated from funding_amendment_loa in delta refresh.
                -- NULL on prime by design.
                CAST(NULL AS DECIMAL(15, 2))        AS total_direct_cost_est_amt,
                CAST(NULL AS DECIMAL(15, 2))        AS total_charges_est_amt,
                ia.activity_address_cd              AS serv_aac
            FROM {S_IA} ia
            WHERE COALESCE(ia.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 2: Decode status description
        -- ─────────────────────────────────────────────────────────────────
        with_status AS (
            SELECT
                ib.*,
                COALESCE(st.description, ib.ia_status_cd) AS ia_status_desc
            FROM ia_base ib
            LEFT JOIN {S_STATUS} st
                ON  st.cd = ib.ia_status_cd
                AND COALESCE(st.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 3: Resolve servicing_agency_sk and extract program/region
        --
        -- IMPROVEMENT #1: program_cd and region_cd extracted from the AAC
        --   table in the same join already used for agency_sk resolution.
        --   Zero additional cost — both columns are on lu_activity_address_code.
        -- ─────────────────────────────────────────────────────────────────
        with_agency AS (
            SELECT
                ws.*,
                ag.agency_sk                        AS servicing_agency_sk,
                -- IMPROVEMENT #1: populate program_cd and region_cd on prime
                --   aac.program_cd  — GSA program code (AAS, ITS_NSD, FAS, etc.)
                --   aac.region_cd   — GSA region code (1–11)
                aac.program_cd,
                aac.region_cd,
                -- requesting_agency_sk: not directly on IA record.
                -- Resolved via LOA/funding chain in delta refresh.
                -- NULL on prime by design.
                CAST(NULL AS BIGINT)                AS requesting_agency_sk
            FROM with_status ws
            -- AAC join: resolves both agency attributes and program/region
            LEFT JOIN {S_AAC} aac
                ON  aac.cd = ws.serv_aac
                AND COALESCE(aac.is_deleted, FALSE) = FALSE
            -- dim_agency join: resolves surrogate key
            LEFT JOIN {G_AGENCY} ag
                ON  ag.activity_address_cd = ws.serv_aac
                AND ag.is_current_flag     = TRUE
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 4: Final projection with ia_type_cd derivation
        --
        -- IMPROVEMENT #2: ia_type_cd now carries a meaningful high-level
        --   grouping derived from instrument_type_cd, not a duplicate of it.
        --   Groupings:
        --     GT_AND_C     — Government-wide Task and Delivery Order Contracts
        --     ORDER        — Individual interagency orders (MIPR, Economy Act,
        --                    IAA, IOA, MOU)
        --     INTRA_AGENCY — Internal GSA instruments
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            ia_id,
            ia_num,

            -- IMPROVEMENT #2: ia_type_cd derived as high-level grouping
            CASE
                WHEN UPPER(COALESCE(instrument_type_cd, '')) IN ('GTC', 'GT', 'GTAC', 'GT_AND_C')
                    THEN 'GT_AND_C'
                WHEN UPPER(COALESCE(instrument_type_cd, '')) IN (
                    'IAA', 'MIPR', 'ECONOMY_ACT', 'MOU', 'IOA', 'BOA', 'CROSS_SERVICING'
                )
                    THEN 'ORDER'
                WHEN instrument_type_cd IS NOT NULL
                    THEN 'INTRA_AGENCY'
                ELSE NULL
            END                                             AS ia_type_cd,

            -- ia_type_desc: human-readable label matching the ia_type_cd grouping
            CASE
                WHEN UPPER(COALESCE(instrument_type_cd, '')) IN ('GTC', 'GT', 'GTAC', 'GT_AND_C')
                    THEN 'Government-wide Task and Delivery Order Contract'
                WHEN UPPER(COALESCE(instrument_type_cd, '')) IN (
                    'IAA', 'MIPR', 'ECONOMY_ACT', 'MOU', 'IOA', 'BOA', 'CROSS_SERVICING'
                )
                    THEN 'Interagency Order'
                WHEN instrument_type_cd IS NOT NULL
                    THEN 'Intra-Agency Instrument'
                ELSE NULL
            END                                             AS ia_type_desc,

            ia_status_cd,
            ia_status_desc,

            -- instrument_type_cd: kept as the source-level instrument detail
            -- (MIPR, Economy Act, IAA, GTC, etc.) — now distinct from ia_type_cd
            instrument_type_cd,
            -- instrument_type_desc: pass through the status decode as best available;
            -- a dedicated lu_instrument_type join can replace this in a future iteration
            COALESCE(ia_status_desc, instrument_type_cd) AS instrument_type_desc,

            fiscal_year,
            ia_start_dt,
            ia_end_dt,                  -- NULL on prime — see header
            total_direct_cost_est_amt,  -- NULL on prime — see header
            total_charges_est_amt,      -- NULL on prime — see header
            COALESCE(servicing_agency_sk, -1) AS servicing_agency_sk,
            requesting_agency_sk,       -- NULL on prime — see header
            program_cd,                 -- IMPROVEMENT #1: now populated on prime
            region_cd,                  -- IMPROVEMENT #1: now populated on prime
            CURRENT_TIMESTAMP()         AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)     AS eff_end_dt,
            TRUE                        AS is_current_flag,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM with_agency
    """)

    # ── Post-load quality checks ───────────────────────────────────────────
    n = row_count(spark, TARGET)

    quality = spark.sql(f"""
        SELECT
            COUNT(*)                                                           AS total_rows,
            SUM(CASE WHEN ia_num IS NULL                 THEN 1 ELSE 0 END)   AS null_ia_num,
            SUM(CASE WHEN servicing_agency_sk = -1       THEN 1 ELSE 0 END)   AS sentinel_agency,
            SUM(CASE WHEN program_cd IS NOT NULL         THEN 1 ELSE 0 END)   AS populated_program_cd,
            SUM(CASE WHEN region_cd IS NOT NULL          THEN 1 ELSE 0 END)   AS populated_region_cd,
            SUM(CASE WHEN ia_type_cd IS NOT NULL         THEN 1 ELSE 0 END)   AS populated_ia_type_cd,
            SUM(CASE WHEN ia_type_cd = instrument_type_cd THEN 1 ELSE 0 END)  AS ia_type_equals_instrument
        FROM {TARGET}
    """).collect()[0]

    print(f"  [OK] Inserted {n:,} IA rows")
    print(f"  null ia_num          : {quality['null_ia_num']:,}")
    print(f"  sentinel agency_sk   : {quality['sentinel_agency']:,}")
    print(f"  program_cd populated : {quality['populated_program_cd']:,}  "
          f"(IMPROVEMENT #1 — was always 0 in v1.0)")
    print(f"  region_cd populated  : {quality['populated_region_cd']:,}  "
          f"(IMPROVEMENT #1 — was always 0 in v1.0)")
    print(f"  ia_type_cd populated : {quality['populated_ia_type_cd']:,}  "
          f"(IMPROVEMENT #2)")
    print(f"  ia_type = instrument : {quality['ia_type_equals_instrument']:,}  "
          f"(expected 0 after IMPROVEMENT #2 — any non-zero is a CASE gap)")

    assert quality["ia_type_equals_instrument"] == 0, (
        f"ASSERT FAILED: ia_type_cd must differ from instrument_type_cd "
        f"(IMPROVEMENT #2 did not fully differentiate). "
        f"Review CASE expression for uncovered instrument_type_cd values."
    )

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
