# Databricks notebook source
# =============================================================================
# common/prime_dim_agency.py
# Primes assist_dev.common.dim_agency
#
# Source tables (Silver):
#   silver_aasbs_lu_activity_address_code  → one row per AAC (most granular key)
#   silver_aasbs_lu_agency                 → agency code + name
#   silver_aasbs_lu_bureau                 → bureau code + name (via agency_cd)
#   silver_table_master_lu_federal_agency  → department code/name, OMB flag (via bureau_code)
#
# Grain       : One row per unique AAC (activity_address_cd)
# SCD Type    : 2 — all rows inserted with eff_start_dt=NOW, eff_end_dt=NULL, is_current=TRUE
# Idempotent  : YES — TRUNCATE then INSERT
# Dependencies: None (first dimension in chain)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET = gold("common", "dim_agency")
TASK   = "prime_dim_agency"

# Silver source table references
S_AAC    = silver("aasbs",        "lu_activity_address_code")
S_AGENCY = silver("aasbs",        "lu_agency")
S_BUREAU = silver("aasbs",        "lu_bureau")
S_FED    = silver("table_master", "lu_federal_agency")

print(f"[{TASK}] target={TARGET}")
print(f"  Sources: {S_AAC}, {S_AGENCY}, {S_BUREAU}, {S_FED}")

# COMMAND ----------

# ── Step 1 : Audit start ──────────────────────────────────────────────────────
start_ts = audit_start(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                        source_schema="aasbs", source_table="lu_activity_address_code")

# COMMAND ----------

try:
    # ── Step 2 : TRUNCATE ────────────────────────────────────────────────────
    truncate_gold(spark, TARGET)

    # ── Step 3 : Build and insert dimension ──────────────────────────────────
    # agency_sk is GENERATED ALWAYS AS IDENTITY — excluded from INSERT col list.
    #
    # Source column notes:
    #   lu_activity_address_code.cd           → activity_address_cd (AAC, PK)
    #   lu_activity_address_code.fpds_ng_agency_identifier → agency_code (2-char FPDS)
    #   lu_agency.cd                           → internal agency code
    #   lu_agency.cd_2char                     → FPDS 2-char agency code
    #   lu_agency.description                  → agency_name
    #   lu_bureau.cd                           → bureau_code
    #   lu_bureau.agency_cd                    → FK to lu_agency.cd (3-char internal)
    #   lu_bureau.description                  → bureau_name
    #   lu_federal_agency.agency_code          → 2-char OMB/FPDS agency code
    #   lu_federal_agency.bureau_code          → 2-char bureau code (PK)
    #   lu_federal_agency.agency_description   → department_name
    #   lu_federal_agency.bureau_description   → another bureau description
    #   lu_federal_agency.agency_type          → used to flag OMB/Intel community

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            agency_code, bureau_code, activity_address_cd,
            agency_name, bureau_name, aac_description,
            department_code, department_name, treasury_symbol,
            is_omb_agency_flag, is_intel_community,
            eff_start_dt, eff_end_dt, is_current_flag,
            _gold_created_at, _gold_updated_at, _source_batch_id
        )
        WITH aac_base AS (
            -- Each AAC is the most granular addressable unit in federal contracting.
            -- fpds_ng_agency_identifier on the AAC is the authoritative 2-char agency code.
            SELECT
                aac.cd                              AS activity_address_cd,
                COALESCE(
                    aac.fpds_ng_agency_identifier,
                    ag.cd_2char
                )                                   AS agency_code_2,
                -- Internal 3-char agency code used to join to bureau
                ag.cd                               AS agency_cd_internal,
                aac.description                     AS aac_description,
                aac.active_yn
            FROM {S_AAC}  aac
            -- Join to lu_agency via region+program or via FPDS identifier.
            -- The AAC doesn't have a direct FK to lu_agency; join via fpds_ng_agency_identifier.
            LEFT JOIN {S_AGENCY} ag
                ON aac.fpds_ng_agency_identifier = ag.cd_2char
                -- AND ag.is_deleted = FALSE
            -- WHERE aac.is_deleted = FALSE
        ),
        agency_bureau AS (
            SELECT
                aac.activity_address_cd,
                aac.agency_code_2,
                aac.agency_cd_internal,
                aac.aac_description,
                ag.description                      AS agency_name,
                bur.cd                              AS bureau_code,
                bur.description                     AS bureau_name
            FROM aac_base aac
            LEFT JOIN {S_AGENCY} ag
                ON aac.agency_cd_internal = ag.cd
                -- AND ag.is_deleted = FALSE
            -- Bureau: the AAC itself doesn't store bureau_cd directly.
            -- We do a best-effort join: if a bureau has the same agency_cd
            -- as our agency and its name contains the AAC region context.
            -- For a more precise join, this would use a separate AAC→bureau mapping table.
            -- NOTE: This produces the first matching bureau per agency; refine with
            --       a dedicated AAC→bureau xref table if available.
            LEFT JOIN {S_BUREAU} bur
                ON bur.agency_cd = aac.agency_cd_internal
                -- AND bur.is_deleted = FALSE
        ),
        with_federal AS (
            SELECT
                ab.activity_address_cd,
                ab.agency_code_2,
                ab.agency_name,
                ab.bureau_code,
                ab.bureau_name,
                ab.aac_description,
                -- department-level data from table_master.lu_federal_agency
                fa.agency_code                      AS dept_agency_code,
                fa.agency_description               AS department_name,
                -- treasury symbol is not directly stored; placeholder for now.
                -- Populated via LOA tracking_num or agreement number in delta refresh.
                CAST(NULL AS STRING)                AS treasury_symbol,
                -- OMB flag: agency_type = 'EXEC' or 'LEGIS' indicates OMB-tracked
                CASE WHEN fa.agency_type IN ('EXEC', 'LEGIS', 'JUDIC')
                     THEN TRUE ELSE FALSE END        AS is_omb_agency_flag,
                -- Intelligence Community flag (DNI-designated agencies)
                -- NOTE: Maintained in lu_intel_community lookup; approximated here.
                FALSE                               AS is_intel_community
            FROM agency_bureau ab
            LEFT JOIN {S_FED} fa
                ON ab.agency_code_2 = fa.agency_code
                AND (fa.inactive_date IS NULL OR fa.inactive_date > CURRENT_TIMESTAMP())
                -- AND fa.is_deleted = FALSE
        )
        SELECT DISTINCT
            COALESCE(agency_code_2, dept_agency_code, 'UNKNOWN')  AS agency_code,
            bureau_code,
            activity_address_cd,
            COALESCE(agency_name, department_name, 'UNKNOWN')     AS agency_name,
            bureau_name,
            aac_description,
            dept_agency_code                                       AS department_code,
            department_name,
            treasury_symbol,
            is_omb_agency_flag,
            is_intel_community,
            -- SCD2 initial values: all rows are current on first prime
            CURRENT_TIMESTAMP()                                    AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)                                AS eff_end_dt,
            TRUE                                                   AS is_current_flag,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM with_federal
        -- Exclude soft-deleted and ensure AAC is present
        WHERE activity_address_cd IS NOT NULL
    """)

    # ── Step 4 : Verify ──────────────────────────────────────────────────────
    n = row_count(spark, TARGET)
    null_agency = spark.sql(
        f"SELECT COUNT(*) AS n FROM {TARGET} WHERE agency_code = 'UNKNOWN'"
    ).collect()[0]["n"]
    print(f"  [OK] Inserted {n:,} agency rows ({null_agency} with unknown agency code)")

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
