# Databricks notebook source
# =============================================================================
# common/cdc_dim_agency.py
# CDC (delta refresh) for assist_catalog.common.dim_agency
#
# Watermark   : Hybrid — pipeline_audit.watermark_to + Silver updated_dt filter
# SCD2 tracked (new version triggered):
#     agency_name, bureau_name, is_omb_agency_flag, is_intel_community
# In-place update (no new version):
#     aac_description, department_code, department_name, treasury_symbol
#
# Source tables:
#   silver_aasbs_lu_activity_address_code  (watermark on updated_dt)
#   silver_aasbs_lu_agency
#   silver_aasbs_lu_bureau
#   silver_table_master_lu_federal_agency
#
# NOTE: dim_date has no CDC notebook — the date spine is static (2000–2035).
#       It is only re-primed if the range needs extending.
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_agency")
TASK     = "cdc_dim_agency"

S_AAC    = silver("aasbs",        "lu_activity_address_code")
S_AGENCY = silver("aasbs",        "lu_agency")
S_BUREAU = silver("aasbs",        "lu_bureau")
S_FED    = silver("table_master", "lu_federal_agency")

# SCD2 config
TRACKED_COLS  = ["agency_name", "bureau_name", "is_omb_agency_flag", "is_intel_community"]
INPLACE_COLS  = ["aac_description", "department_code", "department_name", "treasury_symbol"]
NATURAL_KEY   = "activity_address_cd"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
# ── Watermark ─────────────────────────────────────────────────────────────────
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "lu_activity_address_code",
                             watermark_from, watermark_to)

# COMMAND ----------
try:
    # ── Step 1: Find changed AAC rows within the watermark window ─────────────
    # lu_* tables don't always have updated_dt populated; use the safety belt
    # (created_dt fallback) defined in changed_rows_filter().
    changed_aac_df = spark.sql(f"""
        SELECT cd AS activity_address_cd
        FROM   {S_AAC}
        WHERE  is_deleted = FALSE
          AND  {wm_filter}
    """)
    rows_read = changed_aac_df.count()
    print(f"  [DETECT] {rows_read:,} changed AAC rows in window")

    if rows_read == 0:
        print("  [SKIP] No changes detected — exiting cleanly")
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    # Register changed AACs as a temp view for the MERGE join
    changed_aac_df.createOrReplaceTempView("v_changed_aacs")

    # ── Step 2: Rebuild current-state data for changed AACs ───────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_agency_cdc_source AS
        WITH aac_base AS (
            SELECT
                aac.cd                              AS activity_address_cd,
                COALESCE(
                    aac.fpds_ng_agency_identifier,
                    ag.cd_2char,
                    'UNKNOWN'
                )                                   AS agency_code,
                ag.cd                               AS agency_cd_internal,
                aac.description                     AS aac_description,
                ag.description                      AS agency_name,
                bur.cd                              AS bureau_code,
                bur.description                     AS bureau_name
            FROM {S_AAC} aac
            JOIN v_changed_aacs chg ON chg.activity_address_cd = aac.cd
            LEFT JOIN {S_AGENCY} ag
                ON aac.fpds_ng_agency_identifier = ag.cd_2char
                AND ag.is_deleted = FALSE
            LEFT JOIN {S_BUREAU} bur
                ON bur.agency_cd = ag.cd
                AND bur.is_deleted = FALSE
            WHERE aac.is_deleted = FALSE
        ),
        with_federal AS (
            SELECT
                ab.*,
                fa.agency_code                      AS dept_agency_code,
                fa.agency_description               AS department_name,
                CASE WHEN fa.agency_type IN ('EXEC','LEGIS','JUDIC')
                     THEN TRUE ELSE FALSE END        AS is_omb_agency_flag,
                FALSE                               AS is_intel_community,
                CAST(NULL AS STRING)                AS treasury_symbol
            FROM aac_base ab
            LEFT JOIN {S_FED} fa
                ON ab.agency_code = fa.agency_code
                AND (fa.inactive_date IS NULL OR fa.inactive_date > CURRENT_TIMESTAMP())
                AND fa.is_deleted = FALSE
        )
        SELECT DISTINCT
            COALESCE(agency_code, dept_agency_code, 'UNKNOWN') AS agency_code,
            bureau_code,
            activity_address_cd,
            COALESCE(agency_name, department_name, 'UNKNOWN')  AS agency_name,
            bureau_name,
            aac_description,
            dept_agency_code                                    AS department_code,
            department_name,
            treasury_symbol,
            is_omb_agency_flag,
            is_intel_community
        FROM with_federal
        WHERE activity_address_cd IS NOT NULL
    """)

    # ── Step 3: Apply hybrid SCD2 merge ───────────────────────────────────────
    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark,
        target_fqn     = TARGET,
        source_df_view = "v_agency_cdc_source",
        natural_key    = NATURAL_KEY,
        tracked_cols   = TRACKED_COLS,
        inplace_cols   = INPLACE_COLS,
        batch_id       = RUN_ID,
    )

    # ── Step 4: Handle net-new AACs (not in Gold at all yet) ──────────────────
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
        SELECT
            src.agency_code, src.bureau_code, src.activity_address_cd,
            src.agency_name, src.bureau_name, src.aac_description,
            src.department_code, src.department_name, src.treasury_symbol,
            src.is_omb_agency_flag, src.is_intel_community,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_agency_cdc_source src
        LEFT JOIN {TARGET} tgt
          ON tgt.{NATURAL_KEY} = src.{NATURAL_KEY}
        WHERE tgt.{NATURAL_KEY} IS NULL
    """)

    net_new = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {TARGET}
        WHERE is_current_flag = TRUE
          AND _gold_created_at >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
          AND _source_batch_id = '{RUN_ID}'
    """).collect()[0]["n"] - rows_inserted

    rows_inserted += max(net_new, 0)
    total_written  = rows_inserted + rows_updated
    print(f"  [OK] net_new={max(net_new,0)} | total_written={total_written:,}")

    audit_success_cdc(spark, RUN_ID, TARGET,
                      rows_read, total_written,
                      rows_inserted, rows_updated, start_ts, watermark_to)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
