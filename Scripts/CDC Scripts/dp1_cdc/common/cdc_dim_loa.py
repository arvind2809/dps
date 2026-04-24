# Databricks notebook source
# =============================================================================
# common/cdc_dim_loa.py
# CDC (delta refresh) for assist_catalog.common.dim_loa
# CRITICAL PATH — fact step will not run if this notebook fails
#
# Watermark   : Hybrid
# SCD2 tracked: loa_status_cd, treasury_account_symbol, agreement_end_dt
# In-place    : fund_cd, transmitted_dt, org_code, object_class_cd,
#               program_activity_cd, budget_activity_cd, bbfy, ebfy
#
# Delta enrichment vs prime:
#   object_class_cd  — now sourced from billing.acct_classification_code
#                      joined via loa.agreement_number = billing.agreement_num
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET     = gold("common", "dim_loa")
TASK       = "cdc_dim_loa"

S_LOA      = silver("aasbs", "loa")
S_STATUS   = silver("aasbs", "lu_loa_status")
S_FUNDTYPE = silver("aasbs", "lu_fund_type")
S_BILL     = silver("billing", "billing")
G_AGENCY   = gold("common", "dim_agency")

TRACKED_COLS = ["loa_status_cd", "loa_status_desc", "treasury_account_symbol", "agreement_end_dt"]
INPLACE_COLS = ["fund_cd", "transmitted_dt", "org_code", "object_class_cd",
                "program_activity_cd", "budget_activity_cd", "bbfy", "ebfy",
                "fund_type_cd", "fund_type_desc", "agency_sk"]
NATURAL_KEY  = "loa_id"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "loa", watermark_from, watermark_to)

# COMMAND ----------
try:
    rows_read = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {S_LOA}
        WHERE is_deleted = FALSE AND {wm_filter}
    """).collect()[0]["n"]

    print(f"  [DETECT] {rows_read:,} changed LOA rows")

    if rows_read == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_loa_cdc_source AS
        WITH changed_loa AS (
            SELECT id AS loa_id FROM {S_LOA}
            WHERE  is_deleted = FALSE AND {wm_filter}
        ),
        loa_enriched AS (
            SELECT
                loa.id                                          AS loa_id,
                loa.tracking_num,
                loa.loa_status_cd,
                COALESCE(st.description, loa.loa_status_cd)    AS loa_status_desc,
                loa.agreement_number,
                CAST(loa.ginv_line_num AS STRING)               AS agreement_line_num,
                -- TAS assembly (same logic as prime)
                CONCAT_WS('-',
                    NULLIF(loa.tsym_alloc_trans_agency_cd, ''),
                    NULLIF(loa.requesting_agency_cd, ''),
                    CONCAT(COALESCE(loa.tsym_availability_type,''),
                           COALESCE(loa.appropriation,'')),
                    NULLIF(loa.treasury_sub_account, '')
                )                                               AS treasury_account_symbol,
                loa.customer_fund                               AS fund_cd,
                loa.fund_type_cd,
                COALESCE(ft.description, loa.fund_type_cd)     AS fund_type_desc,
                -- object_class_cd: now enriched from billing via agreement_number
                -- billing.acct_classification_code is the closest proxy for
                -- federal object class (e.g. 25.1 = Advisory/Assistance Services)
                MAX(b.acct_classification_code)                 AS object_class_cd,
                -- program_activity_cd: from billing.activity_code
                MAX(b.activity_code)                            AS program_activity_cd,
                CAST(NULL AS STRING)                            AS budget_activity_cd,
                loa.customer_act_num                            AS org_code,
                CAST(NULL AS STRING)                            AS cost_center,
                CAST(NULL AS STRING)                            AS project_code,
                loa.first_fy_available_year                     AS bbfy,
                loa.last_fy_available_year                      AS ebfy,
                loa.agreement_end_dt,
                loa.transmitted_dt,
                loa.requesting_agency_cd                        AS requesting_agency_cd_raw
            FROM {S_LOA} loa
            JOIN changed_loa cl ON cl.loa_id = loa.id
            LEFT JOIN {S_STATUS}   st ON st.cd = loa.loa_status_cd AND st.is_deleted = FALSE
            LEFT JOIN {S_FUNDTYPE} ft ON ft.cd = loa.fund_type_cd  AND ft.is_deleted = FALSE
            -- Billing enrichment: join to get object_class and program_activity
            LEFT JOIN {S_BILL} b
                ON  b.agreement_num = loa.agreement_number
                AND b.is_deleted    = FALSE
            WHERE loa.is_deleted = FALSE
            GROUP BY
                loa.id, loa.tracking_num, loa.loa_status_cd, st.description,
                loa.agreement_number, loa.ginv_line_num,
                loa.tsym_alloc_trans_agency_cd, loa.requesting_agency_cd,
                loa.tsym_availability_type, loa.appropriation,
                loa.treasury_sub_account, loa.customer_fund,
                loa.fund_type_cd, ft.description, loa.customer_act_num,
                loa.first_fy_available_year, loa.last_fy_available_year,
                loa.agreement_end_dt, loa.transmitted_dt
        ),
        with_agency AS (
            SELECT
                le.*,
                ag.agency_sk
            FROM loa_enriched le
            LEFT JOIN {G_AGENCY} ag
                ON  LEFT(le.requesting_agency_cd_raw, 2) = ag.agency_code
                AND ag.is_current_flag = TRUE
        )
        SELECT
            loa_id, tracking_num,
            loa_status_cd, loa_status_desc,
            agreement_number, agreement_line_num,
            treasury_account_symbol,
            fund_cd, fund_type_cd, fund_type_desc,
            object_class_cd, program_activity_cd, budget_activity_cd,
            org_code, cost_center, project_code,
            bbfy, ebfy, agreement_end_dt, transmitted_dt,
            agency_sk
        FROM with_agency
    """)

    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark, TARGET, "v_loa_cdc_source",
        NATURAL_KEY, TRACKED_COLS, INPLACE_COLS, RUN_ID
    )

    # Net-new LOAs
    spark.sql(f"""
        INSERT INTO {TARGET}
        (loa_id, tracking_num, loa_status_cd, loa_status_desc,
         agreement_number, agreement_line_num, treasury_account_symbol,
         fund_cd, fund_type_cd, fund_type_desc,
         object_class_cd, program_activity_cd, budget_activity_cd,
         org_code, cost_center, project_code, bbfy, ebfy,
         agreement_end_dt, transmitted_dt, agency_sk,
         eff_start_dt, eff_end_dt, is_current_flag,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            src.loa_id, src.tracking_num, src.loa_status_cd, src.loa_status_desc,
            src.agreement_number, src.agreement_line_num, src.treasury_account_symbol,
            src.fund_cd, src.fund_type_cd, src.fund_type_desc,
            src.object_class_cd, src.program_activity_cd, src.budget_activity_cd,
            src.org_code, src.cost_center, src.project_code, src.bbfy, src.ebfy,
            src.agreement_end_dt, src.transmitted_dt, src.agency_sk,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_loa_cdc_source src
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
