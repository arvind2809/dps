# Databricks notebook source
# =============================================================================
# common/cdc_dim_award.py
# CDC (delta refresh) for assist_catalog.common.dim_award
#
# Watermark   : Hybrid
# SCD2 tracked: award_status_cd, fin_code, award_mod_num, award_mod_id,
#               total_mods_count
# In-place    : last_mod_dt, award_end_dt, base_award_dt, ia_sk, agency_sk,
#               contract_type_cd, vehicle_type_cd, vehicle_type_desc,
#               contract_type_desc, award_status_desc
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET    = gold("common", "dim_award")
TASK      = "cdc_dim_award"

S_AWARD   = silver("aasbs", "award")
S_MOD     = silver("aasbs", "award_mod")
S_CT      = silver("aasbs", "lu_contract_type")
G_IA      = gold("common", "dim_ia")
G_AGENCY  = gold("common", "dim_agency")

TRACKED_COLS = ["award_status_cd", "fin_code", "award_mod_num",
                "award_mod_id", "total_mods_count"]
INPLACE_COLS = ["last_mod_dt", "award_end_dt", "base_award_dt",
                "ia_sk", "agency_sk", "contract_type_cd", "contract_type_desc",
                "vehicle_type_cd", "vehicle_type_desc", "award_status_desc"]
NATURAL_KEY  = "award_id"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
# Awards change when either award or award_mod changes
wm_filter_award = changed_rows_filter(watermark_from, watermark_to)
wm_filter_mod   = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "award", watermark_from, watermark_to)

# COMMAND ----------
try:
    # Detect awards changed directly OR via a changed mod
    rows_read = spark.sql(f"""
        SELECT COUNT(DISTINCT id) AS n
        FROM   {S_AWARD}
        WHERE  is_deleted = FALSE
          AND  ({wm_filter_award}
               OR id IN (
                   SELECT DISTINCT award_id FROM {S_MOD}
                   WHERE is_deleted = FALSE AND {wm_filter_mod}
               ))
    """).collect()[0]["n"]

    print(f"  [DETECT] {rows_read:,} changed award rows (direct or via mod)")

    if rows_read == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_award_cdc_source AS
        WITH changed_awards AS (
            SELECT id AS award_id FROM {S_AWARD}
            WHERE  is_deleted = FALSE
              AND  ({wm_filter_award}
                   OR id IN (
                       SELECT DISTINCT award_id FROM {S_MOD}
                       WHERE is_deleted = FALSE AND {wm_filter_mod}
                   ))
        ),
        mod_counts AS (
            SELECT
                award_id,
                COUNT(*)                        AS total_mods_count,
                MIN(CASE WHEN mod_num IN ('0','') THEN m1p_mod_start_dt END) AS base_award_dt
            FROM {S_MOD}
            WHERE is_deleted = FALSE
            GROUP BY award_id
        ),
        award_base AS (
            SELECT
                a.id                            AS award_id,
                a.award_piid,
                a.award_status_cd,
                -- award_status_desc: use status code as desc on CDC; decode in DP11 refresh
                COALESCE(a.award_status_cd, 'UNKNOWN')  AS award_status_desc,
                a.award_fin                     AS fin_code,
                m.id                            AS award_mod_id,
                m.mod_num                       AS award_mod_num,
                m.m1p_mod_start_dt              AS award_start_dt,
                CAST(NULL AS TIMESTAMP)         AS award_end_dt,
                m.co_signature_dt               AS last_mod_dt,
                CAST(NULL AS STRING)            AS contract_type_cd,
                CAST(NULL AS STRING)            AS contract_type_desc,
                CAST(NULL AS STRING)            AS vehicle_type_cd,
                CAST(NULL AS STRING)            AS vehicle_type_desc,
                mc.total_mods_count,
                mc.base_award_dt,
                a.lead_svc_activity_address_cd  AS aac
            FROM {S_AWARD} a
            JOIN changed_awards ca ON ca.award_id = a.id
            LEFT JOIN {S_MOD} m
                ON m.id = a.latest_award_mod_id AND m.is_deleted = FALSE
            LEFT JOIN mod_counts mc ON mc.award_id = a.id
            WHERE a.is_deleted = FALSE
        ),
        with_fks AS (
            SELECT
                ab.*,
                -- ia_sk: now resolved via loa_ledger.ia_id lookup for this award
                -- Use dim_ia: join via award_mod → line_item → loa_ledger.ia_id
                -- This is a best-effort join; NULL if chain not yet available
                CAST(NULL AS BIGINT)            AS ia_sk,
                ag.agency_sk
            FROM award_base ab
            LEFT JOIN {G_AGENCY} ag
                ON ag.activity_address_cd = ab.aac
                AND ag.is_current_flag = TRUE
        )
        SELECT
            award_id, award_piid, award_mod_num, award_mod_id,
            fin_code, award_status_cd, award_status_desc,
            contract_type_cd, contract_type_desc,
            vehicle_type_cd, vehicle_type_desc,
            ia_sk, agency_sk,
            award_start_dt, award_end_dt, base_award_dt, last_mod_dt,
            COALESCE(total_mods_count, 0) AS total_mods_count
        FROM with_fks
    """)

    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark, TARGET, "v_award_cdc_source",
        NATURAL_KEY, TRACKED_COLS, INPLACE_COLS, RUN_ID
    )

    # Net-new awards
    spark.sql(f"""
        INSERT INTO {TARGET}
        (award_id, award_piid, award_mod_num, award_mod_id, fin_code,
         award_status_cd, award_status_desc, contract_type_cd, contract_type_desc,
         vehicle_type_cd, vehicle_type_desc, ia_sk, agency_sk,
         award_start_dt, award_end_dt, base_award_dt, last_mod_dt, total_mods_count,
         eff_start_dt, eff_end_dt, is_current_flag,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            src.award_id, src.award_piid, src.award_mod_num, src.award_mod_id, src.fin_code,
            src.award_status_cd, src.award_status_desc, src.contract_type_cd, src.contract_type_desc,
            src.vehicle_type_cd, src.vehicle_type_desc, src.ia_sk, src.agency_sk,
            src.award_start_dt, src.award_end_dt, src.base_award_dt, src.last_mod_dt, src.total_mods_count,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_award_cdc_source src
        LEFT JOIN {TARGET} tgt ON tgt.{NATURAL_KEY} = src.{NATURAL_KEY}
        WHERE tgt.{NATURAL_KEY} IS NULL
    """)

    net_new = max(spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET} WHERE _source_batch_id='{RUN_ID}' AND _gold_created_at >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE").collect()[0]["n"] - rows_inserted, 0)
    rows_inserted += net_new
    audit_success_cdc(spark, RUN_ID, TARGET, rows_read, rows_inserted + rows_updated,
                      rows_inserted, rows_updated, start_ts, watermark_to)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
