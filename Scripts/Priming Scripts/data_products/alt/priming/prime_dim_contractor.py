# Databricks notebook source
# =============================================================================
# alt/prime_dim_contractor.py
# Primes assist_dev.alt.dim_contractor
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per award_mod_company.id
# SCD Type : 2 — on prime every row is current (eff_end_dt=NULL, is_current=TRUE).
#             CDC closes prior versions when UEI/company name changes on new mods.
#
# Source tables (Silver):
#   silver_aasbs_award_mod_company  → contractor identity and SAM.gov attributes
#
# Business type flag derivation:
#   award_mod_company.business_type is a pipe-delimited or comma-delimited string
#   of SAM.gov business type code tokens (e.g. 'SMALL_BUSINESS,WOMAN_OWNED').
#   LIKE pattern matching is used on the UPPER()-normalised value.
#   Tokens align with SAM.gov API businessTypes field per 13 CFR Part 121
#   and 48 CFR 4.1102.
#   Adjustment required if source stores compact abbreviations (e.g. 'SB', 'WO')
#   rather than full token names — see IMPROVEMENT #9 post-load check.
#
# IMPROVEMENT #9 (v1.1.0):
#   Post-load quality check added to detect business_type format mismatches.
#   The LIKE pattern derivation silently returns FALSE for all flags when the
#   source stores compact codes (e.g. 'SB', 'WO', 'HUB') rather than full SAM
#   token names.  The check compares:
#     - Rows with non-NULL business_type  (source has data)
#     - Rows where ALL flags are FALSE    (no token matched)
#   If these two numbers are similar, the LIKE patterns are not matching the
#   source format.  A sample of distinct business_type values is printed so the
#   operator can inspect the source encoding and adjust the CASE expressions.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP2"
NOTEBOOK     = "prime_dim_contractor"
TARGET_TABLE = "assist_dev.alt.dim_contractor"

SILVER = "assist_dev.assist_finance"

# ADDED for utils
TASK   = "prime_dim_contractor"
dbutils.widgets.text("job_name", "dp2_prime_full", "Job Name")
JOB_NAME = dbutils.widgets.get("job_name")

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award_mod_company")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_award_mod_company"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver award_mod_company row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT
    #
    # Business type flags derived via LIKE on business_type string.
    # Expected format: full SAM.gov token names (e.g. 'SMALL_BUSINESS').
    # See IMPROVEMENT #9 post-load check for format mismatch detection.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            award_mod_company_id,
            uei,
            duns_num,
            cage_code,
            company_name,
            dba_name,
            parent_uei,
            parent_company_name,
            business_type_cd,
            small_business_flag,
            woman_owned_flag,
            veteran_owned_flag,
            hubzone_flag,
            sdvosb_flag,
            city,
            state_cd,
            country_cd,
            zip_code,
            congressional_district,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            amc.id                                              AS award_mod_company_id,

            -- SAM.gov identifiers (13 CFR 121; 48 CFR 4.605)
            amc.company_uei as uei,
            amc.company_duns as duns_num,
            ip.ipartner_cage_code as cage_code,
            -- amc.cage_code,

            -- Entity names
            amc.solicit_company_name as company_name,
            NULL as dba_name,
            -- amc.parent_uei,
            ip.ipartner_parent_uei as parent_uei,
            -- amc.parent_company_name,
            ip.ipartner_parent_name as parent_company_name,
            -- Raw business type string preserved for lineage and CDC comparison
            amc.business_type_cd                                   AS business_type_cd,

            -- ── Business type flag derivation ────────────────────────────
            -- Source format assumption: full SAM.gov API token names,
            -- pipe or comma-delimited (e.g. 'SMALL_BUSINESS|WOMAN_OWNED').
            -- UPPER() normalisation applied before LIKE matching.
            -- If source uses short codes (SB, WO, HUB, SDVO), all flags will
            -- be FALSE — see IMPROVEMENT #9 post-load check.
            CASE WHEN UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%SMALL_BUSINESS%'
                 THEN TRUE ELSE FALSE END                        AS small_business_flag,

            CASE WHEN UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%WOMEN_OWNED%'
                  OR  UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%WOMAN_OWNED%'
                 THEN TRUE ELSE FALSE END                        AS woman_owned_flag,

            CASE WHEN UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%VETERAN_OWNED_BUSINESS%'
                 THEN TRUE ELSE FALSE END                        AS veteran_owned_flag,

            CASE WHEN UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%HUBZONE%'
                 THEN TRUE ELSE FALSE END                        AS hubzone_flag,

            CASE WHEN UPPER(COALESCE(amc.business_type_cd, '')) LIKE '%SERVICE_DISABLED_VETERAN%'
                 THEN TRUE ELSE FALSE END                        AS sdvosb_flag,
            -- ─────────────────────────────────────────────────────────────

            -- amc.city,
            -- amc.state_cd,
            -- amc.country_cd,
            -- amc.zip_code,
            ip.city,
            ip.state,
            ip.country,
            ip.zip,
            NULL as congressional_district,

            -- SCD2: all rows set as current active version on prime.
            -- CDC closes prior versions when UEI or company_name changes.
            current_timestamp()         AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)     AS eff_end_dt,
            TRUE                        AS is_current_flag,
            current_timestamp()         AS _gold_created_at,
            current_timestamp()         AS _gold_updated_at,
            '{RUN_ID}'                  AS _source_batch_id

        FROM {SILVER}.silver_aasbs_award_mod_company amc
        -- ADDED for missing columns
        LEFT JOIN {SILVER}.silver_table_master_industry_partners ip
            ON ip.ipartner_id = amc.ipartner_id

        WHERE COALESCE(amc.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    uei_stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                        AS total_rows,
            SUM(CASE WHEN uei IS NOT NULL           THEN 1 ELSE 0 END)     AS rows_with_uei,
            SUM(CASE WHEN duns_num IS NOT NULL       THEN 1 ELSE 0 END)     AS rows_with_duns,
            SUM(CASE WHEN small_business_flag = TRUE THEN 1 ELSE 0 END)     AS small_biz_count
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] UEI coverage    : {uei_stats[1]:,}/{uei_stats[0]:,} rows"
    )
    print(
        f"[{NOTEBOOK}] DUNS coverage   : {uei_stats[2]:,} | "
        f"Small business  : {uei_stats[3]:,}"
    )

    # ─────────────────────────────────────────────────────────────────────
    # IMPROVEMENT #9: business_type format mismatch detection
    #
    # Compares rows where business_type is non-NULL against rows where every
    # flag resolved FALSE.  A high ratio indicates the LIKE patterns are not
    # matching the source encoding.
    # ─────────────────────────────────────────────────────────────────────
    fmt_check = spark.sql(f"""
        SELECT
            SUM(CASE WHEN business_type_cd IS NOT NULL
                     THEN 1 ELSE 0 END)                         AS rows_with_biz_type,
            SUM(CASE WHEN business_type_cd IS NOT NULL
                      AND small_business_flag = FALSE
                      AND woman_owned_flag    = FALSE
                      AND veteran_owned_flag  = FALSE
                      AND hubzone_flag        = FALSE
                      AND sdvosb_flag         = FALSE
                     THEN 1 ELSE 0 END)                         AS all_flags_false_with_data
        FROM {TARGET_TABLE}
    """).collect()[0]

    rows_with_bt    = fmt_check[0]
    all_false_bt    = fmt_check[1]
    mismatch_pct    = (all_false_bt / rows_with_bt * 100) if rows_with_bt > 0 else 0

    print(
        f"[{NOTEBOOK}] IMPROVEMENT #9 — business_type format check:"
    )
    print(
        f"  rows with business_type_cd non-null : {rows_with_bt:,}"
    )
    print(
        f"  rows with all flags FALSE           : {all_false_bt:,}  "
        f"({mismatch_pct:.1f}% of non-null rows)"
    )

    if mismatch_pct > 50.0:
        # Print a sample of raw business_type values from Silver to help diagnose
        sample = spark.sql(f"""
            SELECT DISTINCT business_type_cd
            FROM {TARGET_TABLE}
            WHERE business_type_cd IS NOT NULL
            LIMIT 10
        """).collect()
        sample_vals = [r[0] for r in sample]
        print(
            f"  ⚠ WARNING: {mismatch_pct:.0f}% of contractors with business_type data "
            f"have ALL flags FALSE — possible encoding mismatch."
        )
        print(f"  Sample business_type_cd values: {sample_vals}")
        print(
            f"  If the source uses compact codes (SB, WO, HUB, SDVO) rather than "
            f"full SAM token names, update the CASE expressions in this notebook."
        )
    else:
        print(f"  ✓  Flag derivation looks healthy (≤50% all-false on non-null rows)")

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
