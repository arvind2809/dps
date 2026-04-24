# Databricks notebook source
# =============================================================================
# common/cdc_dim_funding.py
# CDC (delta refresh) for assist_catalog.common.dim_funding
#
# Watermark   : Hybrid
# SCD2 tracked: fund_status_cd, billing_type_cd, fund_category_cd
# In-place    : total_funded_amt, fiscal_year, fund_type_cd, fund_type_desc,
#               ia_sk, agency_sk, funding_amendment_id
#
# Delta enrichment vs prime:
#   total_funded_amt — now computed as SUM(loa_ledger.loa_funded_amt)
#                      for all LOAs linked to this funding's IA
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_funding")
TASK     = "cdc_dim_funding"

S_FUND   = silver("aasbs", "funding")
S_AMEND  = silver("aasbs", "funding_amendment")
S_FSTAT  = silver("aasbs", "lu_fund_status")
S_FCAT   = silver("aasbs", "lu_fund_category")
S_BT     = silver("aasbs", "lu_billing_type")
S_LOA_L  = silver("aasbs", "loa_ledger")
G_IA     = gold("common", "dim_ia")

TRACKED_COLS = ["fund_status_cd", "fund_status_desc", "billing_type_cd", "fund_category_cd"]
INPLACE_COLS = ["total_funded_amt", "fiscal_year", "fund_type_cd", "fund_type_desc",
                "ia_sk", "agency_sk", "funding_amendment_id"]
NATURAL_KEY  = "funding_id"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "funding", watermark_from, watermark_to)

# COMMAND ----------
try:
    rows_read = spark.sql(f"""
        SELECT COUNT(DISTINCT id) AS n FROM {S_FUND}
        WHERE is_deleted = FALSE
          AND ({wm_filter}
               OR latest_funding_amendment_id IN (
                   SELECT id FROM {S_AMEND}
                   WHERE is_deleted = FALSE AND {wm_filter}
               ))
    """).collect()[0]["n"]

    print(f"  [DETECT] {rows_read:,} changed funding rows")

    if rows_read == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_funding_cdc_source AS
        WITH changed_fund AS (
            SELECT id AS funding_id FROM {S_FUND}
            WHERE  is_deleted = FALSE
              AND  ({wm_filter}
                   OR latest_funding_amendment_id IN (
                       SELECT id FROM {S_AMEND} WHERE is_deleted=FALSE AND {wm_filter}
                   ))
        ),
        -- total_funded_amt: sum of all loa_funded_amt for this funding_id
        funded_totals AS (
            SELECT
                ll.funding_id,
                SUM(ll.loa_funded_amt) AS total_funded_amt
            FROM {S_LOA_L} ll
            JOIN changed_fund cf ON cf.funding_id = ll.funding_id
            WHERE ll.is_deleted = FALSE
            GROUP BY ll.funding_id
        ),
        fund_base AS (
            SELECT
                f.id                                            AS funding_id,
                f.ia_id,
                f.fund_status_cd,
                COALESCE(fs.description, f.fund_status_cd)     AS fund_status_desc,
                f.fund_category_cd,
                COALESCE(fc.description, f.fund_category_cd)   AS fund_category_desc,
                f.billing_type_cd,
                COALESCE(bt.description, f.billing_type_cd)    AS billing_type_desc,
                f.latest_funding_amendment_id                   AS funding_amendment_id,
                CAST(NULL AS STRING)                            AS fund_type_cd,
                CAST(NULL AS STRING)                            AS fund_type_desc,
                COALESCE(ft.total_funded_amt, 0.00)             AS total_funded_amt
            FROM {S_FUND} f
            JOIN changed_fund cf ON cf.funding_id = f.id
            LEFT JOIN {S_FSTAT} fs ON fs.cd = f.fund_status_cd AND fs.is_deleted = FALSE
            LEFT JOIN {S_FCAT}  fc ON fc.cd = f.fund_category_cd AND fc.is_deleted = FALSE
            LEFT JOIN {S_BT}    bt ON bt.cd = f.billing_type_cd AND bt.is_deleted = FALSE
            LEFT JOIN funded_totals ft ON ft.funding_id = f.id
            WHERE f.is_deleted = FALSE
        ),
        with_ia_sk AS (
            SELECT
                fb.*,
                ia.ia_sk,
                ia.servicing_agency_sk  AS agency_sk,
                ia.fiscal_year
            FROM fund_base fb
            LEFT JOIN {G_IA} ia
                ON ia.ia_id = fb.ia_id AND ia.is_current_flag = TRUE
        )
        SELECT
            funding_id, funding_amendment_id,
            fund_status_cd, fund_status_desc,
            fund_category_cd, fund_category_desc,
            billing_type_cd, billing_type_desc,
            fund_type_cd, fund_type_desc,
            ia_sk, agency_sk,
            total_funded_amt, fiscal_year
        FROM with_ia_sk
    """)

    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark, TARGET, "v_funding_cdc_source",
        NATURAL_KEY, TRACKED_COLS, INPLACE_COLS, RUN_ID
    )

    # Net-new funding packages
    spark.sql(f"""
        INSERT INTO {TARGET}
        (funding_id, funding_amendment_id,
         fund_status_cd, fund_status_desc, fund_category_cd, fund_category_desc,
         billing_type_cd, billing_type_desc, fund_type_cd, fund_type_desc,
         ia_sk, agency_sk, total_funded_amt, fiscal_year,
         eff_start_dt, eff_end_dt, is_current_flag,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            src.funding_id, src.funding_amendment_id,
            src.fund_status_cd, src.fund_status_desc,
            src.fund_category_cd, src.fund_category_desc,
            src.billing_type_cd, src.billing_type_desc,
            src.fund_type_cd, src.fund_type_desc,
            src.ia_sk, src.agency_sk, src.total_funded_amt, src.fiscal_year,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_funding_cdc_source src
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
