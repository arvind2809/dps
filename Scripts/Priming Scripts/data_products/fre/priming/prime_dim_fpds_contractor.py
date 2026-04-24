# Databricks notebook source
# =============================================================================
# fre/prime_dim_fpds_contractor.py
# Primes assist_dev.fre.dim_fpds_contractor
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per award_mod_company.id  (SCD2 — history preserved)
# SCD Type : 2 — tracks UEI, company name, SBA certification changes
#
# Source strategy:
#   Read all rows (current + historical SCD2 versions) from DP2's
#   assist_dev.alt.dim_contractor as the base record set.
#   DP2 has already performed the full SCD2 prime from
#   silver_aasbs_award_mod_company.  Re-reading Silver from scratch
#   would duplicate that work and risk SCD2 inconsistencies.
#   Extend with sba_program_cd via LEFT JOIN back to Silver on
#   award_mod_company_id.
#
# BUG FIX B-DP3-1 (v1.2.0):
#   Source table corrected from assist_dev.alt.dim_fpds_contractor
#   (does not exist) to assist_dev.alt.dim_contractor (correct DP2 table).
#   This bug caused a runtime table-not-found failure before executing any rows.
#   The DP2 guard check and the main INSERT both referenced the wrong name.
#
# IMPROVEMENT I-DP3-5 (v1.2.0):
#   Silver LEFT JOIN de-duplicated via a MAX(id) pre-aggregation on
#   silver_aasbs_award_mod_company per award_mod_company_id.
#   Ingestion duplicates (soft-deleted rows re-ingested with new _batch_id)
#   can cause fan-out if the raw Silver table is joined directly, producing
#   rows_written > dp2_rows and the mismatch warning firing spuriously.
#   Resolved by pre-selecting the latest Silver row per natural key first.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP3"
NOTEBOOK     = "prime_dim_fpds_contractor"
TARGET_TABLE = "assist_dev.fre.dim_fpds_contractor"

SILVER  = "assist_dev.assist_finance"
# BUG FIX B-DP3-1: was "assist_dev.alt.dim_fpds_contractor" (does not exist)
DP2_ALT = "assist_dev.alt"

# COMMAND ----------

# start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, NOTEBOOK, PRODUCT, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award_mod_company")

print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: DP2 dim_contractor must be populated
    #
    # BUG FIX B-DP3-1: was checking dim_fpds_contractor (non-existent).
    # Corrected to dim_contractor (the actual DP2 contractor dim).
    # ─────────────────────────────────────────────────────────────────────
    dp2_rows = spark.sql(
        f"SELECT COUNT(*) FROM {DP2_ALT}.dim_contractor"
    ).collect()[0][0]

    if dp2_rows == 0:
        err = (
            f"[{NOTEBOOK}] BLOCKED — {DP2_ALT}.dim_contractor is empty. "
            f"DP2 prime must complete successfully before DP3 "
            f"dim_fpds_contractor can run."
        )
        print(err)
        audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
        dbutils.notebook.exit("BLOCKED_DP2_NOT_PRIMED")

    print(f"[{NOTEBOOK}] DP2 dim_contractor row count: {dp2_rows:,} — proceeding.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — IMPROVEMENT I-DP3-5: de-duplicate Silver award_mod_company
    #
    # Build a view of the single latest Silver row per award_mod_company_id
    # using MAX(id) to pick the most recently ingested record.
    # Prevents fan-out on join when the Silver table contains ingestion
    # duplicates (e.g. re-ingested rows with new _batch_id values).
    # Without this, rows_written > dp2_rows and the mismatch guard trips.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_amc_deduped AS
        SELECT amc.*
        FROM {SILVER}.silver_aasbs_award_mod_company amc
        JOIN (
            SELECT id AS max_id
            FROM (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY _ingested_at DESC
                    ) AS rn
                FROM {SILVER}.silver_aasbs_award_mod_company
                WHERE COALESCE(is_deleted, FALSE) = FALSE
            )
            WHERE rn = 1
        ) latest ON latest.max_id = amc.id
        WHERE COALESCE(amc.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    #   Read all DP2 contractor rows (current + historical SCD2 versions)
    #   and extend with sba_program_cd from Silver.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            award_mod_company_id,
            uei,
            duns_num,
            cage_code,
            company_name,
            parent_uei,
            parent_company_name,
            sba_program_cd,
            bus_type_cd,
            city,
            state_cd,
            country_cd,
            congressional_district,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: preserved from DP2 dim (BUG FIX B-DP3-1: now reads
            -- from alt.dim_contractor, not alt.dim_fpds_contractor)
            d2.award_mod_company_id,

            -- SAM.gov identifiers
            d2.uei,
            d2.duns_num,
            d2.cage_code,

            -- Entity names
            d2.company_name,
            d2.parent_uei,
            d2.parent_company_name,

            -- SBA program certification — DP3-specific extension.
            -- Sourced from Silver award_mod_company.sba_program_cd via
            -- v_amc_deduped (IMPROVEMENT I-DP3-5: de-duplicated).
            -- Pipe/comma-delimited SBA program codes per FAR 19 / 13 CFR
            -- Parts 121/124/125/127 (8(a), HUBZone, WOSB, SDVOSB).
            -- NULL for records predating SAM.gov certification tracking.
            -- amc.sba_program_cd,
            NULL as sba_program_cd,

            -- Business type raw code string (carried from DP2)
            d2.business_type_cd                                 AS bus_type_cd,

            -- Address
            d2.city,
            d2.state_cd,
            d2.country_cd,
            d2.congressional_district,

            -- SCD2 versioning fields: preserved exactly from DP2
            d2.eff_start_dt,
            d2.eff_end_dt,
            d2.is_current_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        -- BUG FIX B-DP3-1: source is alt.dim_contractor (not alt.dim_fpds_contractor)
        FROM {DP2_ALT}.dim_contractor d2

        -- IMPROVEMENT I-DP3-5: join de-duplicated Silver view (not raw table)
        LEFT JOIN v_amc_deduped amc
            ON  amc.id = d2.award_mod_company_id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load summary
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    sba_coverage = spark.sql(f"""
        SELECT
            COUNT(*)                                                    AS total_rows,
            SUM(CASE WHEN is_current_flag = TRUE  THEN 1 ELSE 0 END)   AS current_versions,
            SUM(CASE WHEN sba_program_cd IS NOT NULL THEN 1 ELSE 0 END) AS has_sba_program,
            SUM(CASE WHEN uei IS NOT NULL THEN 1 ELSE 0 END)            AS has_uei
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Summary — total={sba_coverage[0]:,} | "
        f"current={sba_coverage[1]:,} | "
        f"sba_program={sba_coverage[2]:,} | "
        f"uei={sba_coverage[3]:,}"
    )

    # Row-count parity check (IMPROVEMENT I-DP3-5 makes this meaningful)
    if rows_written != dp2_rows:
        print(
            f"[{NOTEBOOK}] WARNING: row count mismatch — "
            f"DP2 source={dp2_rows:,}, DP3 written={rows_written:,}. "
            f"Check v_amc_deduped for remaining Silver duplicates."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count matches DP2 source ({rows_written:,})")

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, dp2_rows, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, dp2_rows, rows_written, start_ts)

    print(f"[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)

    raise
