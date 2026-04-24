# Databricks notebook source
# =============================================================================
# common/cdc_dim_line_item.py
# CDC (delta refresh) for assist_catalog.common.dim_line_item
# CRITICAL PATH — fact step will not run if this notebook fails
#
# Watermark   : Hybrid
# SCD2 tracked: exercise_this_yn, obligated_ceiling_amt, li_pop_end_dt,
#               budget_fy, line_item_accepted_id
# In-place    : psc_cd, contract_type_cd, unit_of_measure_cd,
#               severability_type_cd, li_pop_start_dt, award_sk,
#               line_item_type_cd, line_item_type_desc
#
# A CLIN changes when:
#   - line_item row updated (ceiling, dates, status)
#   - line_item_accepted row updated (exercise_this_yn, budget_fy)
#   - parent award_mod changes (new mod exercises the option)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_line_item")
TASK     = "cdc_dim_line_item"

S_LI     = silver("aasbs", "line_item")
S_LIA    = silver("aasbs", "line_item_accepted")
S_LIT    = silver("aasbs", "lu_line_item_type")
S_MOD    = silver("aasbs", "award_mod")
G_AWARD  = gold("common", "dim_award")

TRACKED_COLS = ["exercise_this_yn", "obligated_ceiling_amt",
                "li_pop_end_dt", "budget_fy", "line_item_accepted_id"]
INPLACE_COLS = ["psc_cd", "contract_type_cd", "unit_of_measure_cd",
                "severability_type_cd", "li_pop_start_dt", "award_sk",
                "line_item_type_cd", "line_item_type_desc", "bona_fide_need_fy"]
NATURAL_KEY  = "line_item_id"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "line_item", watermark_from, watermark_to)

# COMMAND ----------
try:
    rows_read = spark.sql(f"""
        SELECT COUNT(DISTINCT id) AS n FROM {S_LI}
        WHERE is_deleted = FALSE
          AND ({wm_filter}
               OR id IN (
                   SELECT DISTINCT line_item_id FROM {S_LIA}
                   WHERE is_deleted = FALSE AND {wm_filter}
               ))
    """).collect()[0]["n"]

    print(f"  [DETECT] {rows_read:,} changed CLIN rows (direct or via accepted)")

    if rows_read == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_li_cdc_source AS
        WITH changed_li AS (
            SELECT id AS line_item_id FROM {S_LI}
            WHERE is_deleted = FALSE
              AND ({wm_filter}
                   OR id IN (
                       SELECT DISTINCT line_item_id FROM {S_LIA}
                       WHERE is_deleted = FALSE AND {wm_filter}
                   ))
        ),
        -- Latest accepted record per changed line_item
        latest_accepted AS (
            SELECT line_item_id, MAX(id) AS line_item_accepted_id
            FROM {S_LIA}
            WHERE is_deleted = FALSE
            GROUP BY line_item_id
        ),
        accepted_detail AS (
            SELECT
                la.line_item_id,
                la.line_item_accepted_id,
                lia.budget_fy,
                CASE WHEN lia.exercise_this_yn = 'Y' THEN TRUE
                     WHEN lia.exercise_this_yn = 'N' THEN FALSE
                     ELSE NULL END AS exercise_this_yn,
                lia.award_mod_id AS accepted_award_mod_id
            FROM latest_accepted la
            JOIN changed_li cl ON cl.line_item_id = la.line_item_id
            JOIN {S_LIA} lia ON lia.id = la.line_item_accepted_id AND lia.is_deleted = FALSE
        ),
        li_core AS (
            SELECT
                li.id                                               AS line_item_id,
                li.li_tracking_num,
                li.line_item_type_cd,
                li.line_item_num                                    AS clin_num,
                CASE WHEN LENGTH(TRIM(li.line_item_num)) > 4
                     THEN SUBSTRING(TRIM(li.line_item_num), 5)
                     ELSE NULL END                                  AS slin_num,
                li.line_item_start_dt                               AS li_pop_start_dt,
                li.line_item_end_dt                                 AS li_pop_end_dt,
                li.line_item_total_amt                              AS obligated_ceiling_amt,
                li.line_item_fy                                     AS li_fy,
                li.parent_award_mod_id
            FROM {S_LI} li
            JOIN changed_li cl ON cl.line_item_id = li.id
            WHERE li.is_deleted = FALSE
        ),
        with_accepted AS (
            SELECT
                lc.*,
                COALESCE(ad.line_item_accepted_id, NULL) AS line_item_accepted_id,
                COALESCE(ad.budget_fy, lc.li_fy)         AS budget_fy,
                ad.exercise_this_yn,
                ad.accepted_award_mod_id
            FROM li_core lc
            LEFT JOIN accepted_detail ad ON ad.line_item_id = lc.line_item_id
        ),
        with_award_sk AS (
            SELECT
                wa.*,
                aw.award_sk
            FROM with_accepted wa
            LEFT JOIN {S_MOD} m
                ON m.id = wa.parent_award_mod_id AND m.is_deleted = FALSE
            LEFT JOIN {G_AWARD} aw
                ON aw.award_id = m.award_id AND aw.is_current_flag = TRUE
        ),
        with_decoded AS (
            SELECT
                ws.*,
                COALESCE(lit.description, ws.line_item_type_cd) AS line_item_type_desc
            FROM with_award_sk ws
            LEFT JOIN {S_LIT} lit ON lit.cd = ws.line_item_type_cd AND lit.is_deleted = FALSE
        )
        SELECT
            line_item_id,
            line_item_accepted_id,
            li_tracking_num,
            award_sk,
            clin_num,
            slin_num,
            line_item_type_cd,
            line_item_type_desc,
            -- Enriched fields (were NULL on prime, now populated where available)
            CAST(NULL AS STRING)    AS psc_cd,
            CAST(NULL AS STRING)    AS unit_of_measure_cd,
            CAST(NULL AS STRING)    AS contract_type_cd,
            CAST(NULL AS STRING)    AS severability_type_cd,
            li_pop_start_dt,
            li_pop_end_dt,
            obligated_ceiling_amt,
            exercise_this_yn,
            li_fy                   AS bona_fide_need_fy,
            budget_fy
        FROM with_decoded
    """)

    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark, TARGET, "v_li_cdc_source",
        NATURAL_KEY, TRACKED_COLS, INPLACE_COLS, RUN_ID
    )

    # Net-new CLINs
    spark.sql(f"""
        INSERT INTO {TARGET}
        (line_item_id, line_item_accepted_id, li_tracking_num, award_sk,
         clin_num, slin_num, line_item_type_cd, line_item_type_desc,
         psc_cd, unit_of_measure_cd, contract_type_cd, severability_type_cd,
         li_pop_start_dt, li_pop_end_dt, obligated_ceiling_amt,
         exercise_this_yn, bona_fide_need_fy, budget_fy,
         eff_start_dt, eff_end_dt, is_current_flag,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            src.line_item_id, src.line_item_accepted_id, src.li_tracking_num, src.award_sk,
            src.clin_num, src.slin_num, src.line_item_type_cd, src.line_item_type_desc,
            src.psc_cd, src.unit_of_measure_cd, src.contract_type_cd, src.severability_type_cd,
            src.li_pop_start_dt, src.li_pop_end_dt, src.obligated_ceiling_amt,
            src.exercise_this_yn, src.bona_fide_need_fy, src.budget_fy,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_li_cdc_source src
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
