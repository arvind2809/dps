# Databricks notebook source
# =============================================================================
# common/cdc_dim_ia.py
# CDC (delta refresh) for assist_catalog.common.dim_ia
#
# Watermark   : Hybrid — pipeline_audit.watermark_to + Silver updated_dt filter
# SCD2 tracked (new version triggered):
#     ia_status_cd, ia_status_desc, ia_end_dt,
#     total_direct_cost_est_amt, total_charges_est_amt, servicing_agency_sk
# In-place update (no new version):
#     program_cd, region_cd, ia_type_cd, ia_type_desc, fiscal_year,
#     instrument_type_cd, instrument_type_desc
#
# Source tables: silver_aasbs_ia, silver_aasbs_lu_ia_status,
#                silver_aasbs_lu_activity_address_code, assist_catalog.common.dim_agency
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils_cdc

# COMMAND ----------
dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_cdc", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "cdc-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_ia")
TASK     = "cdc_dim_ia"

S_IA     = silver("aasbs", "ia")
S_STATUS = silver("aasbs", "lu_ia_status")
S_AAC    = silver("aasbs", "lu_activity_address_code")
G_AGENCY = gold("common", "dim_agency")

TRACKED_COLS = [
    "ia_status_cd", "ia_status_desc", "ia_end_dt",
    "total_direct_cost_est_amt", "total_charges_est_amt", "servicing_agency_sk"
]
INPLACE_COLS = [
    "program_cd", "region_cd", "ia_type_cd", "ia_type_desc",
    "fiscal_year", "instrument_type_cd", "instrument_type_desc"
]
NATURAL_KEY  = "ia_id"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------
watermark_from, watermark_to = get_watermark(spark, TARGET)
wm_filter = changed_rows_filter(watermark_from, watermark_to)

start_ts = audit_start_cdc(spark, RUN_ID, JOB_NAME, TASK, TARGET,
                             "aasbs", "ia", watermark_from, watermark_to)

# COMMAND ----------
try:
    # ── Step 1: Detect changed IAs ────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {S_IA}
        WHERE is_deleted = FALSE AND {wm_filter}
    """).collect()[0]["n"]

    print(f"  [DETECT] {rows_read:,} changed IA rows")

    if rows_read == 0:
        audit_success_cdc(spark, RUN_ID, TARGET, 0, 0, 0, 0, start_ts, watermark_to)
        dbutils.notebook.exit("SUCCESS")

    # ── Step 2: Rebuild current state for changed IAs ─────────────────────────
    # ia_end_dt: sourced from funding_amendment.ginv_pop_end_dt via latest amendment.
    # We join funding → funding_amendment to get the most recent ginv_pop_end_dt.
    # total_direct_cost_est_amt: sum of loa_funded_amt for the IA's LOAs (proxy).
    # Both of these were NULL on prime and are enriched here.

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_cdc_source AS
        WITH changed_ia AS (
            SELECT id AS ia_id
            FROM   {S_IA}
            WHERE  is_deleted = FALSE AND {wm_filter}
        ),
        ia_with_end AS (
            -- Derive ia_end_dt from latest funding_amendment.ginv_pop_end_dt
            -- Funding → FundingAmendment chain per IA
            SELECT
                ia.id                               AS ia_id,
                ia.piid                             AS ia_num,
                ia.ia_status_cd,
                ia.instrument_type_cd,
                ia.fiscal_year,
                ia.ia_began_dt                      AS ia_start_dt,
                ia.activity_address_cd              AS serv_aac,
                ia.serv_agency_cd,
                -- Best available ia_end_dt: max ginv_pop_end_dt across all
                -- funding amendments belonging to this IA's funding packages
                MAX(fa.ginv_pop_end_dt)             AS ia_end_dt,
                -- Cost estimates: sum of loa_dollar_amt as proxy for direct cost
                CAST(NULL AS DECIMAL(15,2))         AS total_direct_cost_est_amt,
                CAST(NULL AS DECIMAL(15,2))         AS total_charges_est_amt
            FROM {S_IA} ia
            JOIN changed_ia ci ON ci.ia_id = ia.id
            LEFT JOIN {silver("aasbs","funding")} f
                ON  f.ia_id     = ia.id
                AND f.is_deleted = FALSE
            LEFT JOIN {silver("aasbs","funding_amendment")} fa
                ON  fa.id        = f.latest_funding_amendment_id
                AND fa.is_deleted = FALSE
            WHERE ia.is_deleted = FALSE
            GROUP BY ia.id, ia.piid, ia.ia_status_cd, ia.instrument_type_cd,
                     ia.fiscal_year, ia.ia_began_dt, ia.activity_address_cd,
                     ia.serv_agency_cd
        ),
        with_status AS (
            SELECT
                iwe.*,
                COALESCE(st.description, iwe.ia_status_cd) AS ia_status_desc
            FROM ia_with_end iwe
            LEFT JOIN {S_STATUS} st
                ON st.cd = iwe.ia_status_cd AND st.is_deleted = FALSE
        ),
        with_sks AS (
            SELECT
                ws.*,
                ag.agency_sk                        AS servicing_agency_sk,
                aac.program_cd,
                aac.region_cd
            FROM with_status ws
            LEFT JOIN {G_AGENCY} ag
                ON ag.activity_address_cd = ws.serv_aac
                AND ag.is_current_flag = TRUE
            LEFT JOIN {S_AAC} aac
                ON aac.cd = ws.serv_aac
                AND aac.is_deleted = FALSE
        )
        SELECT
            ia_id,
            ia_num,
            CASE instrument_type_cd
                WHEN 'M'  THEN 'MIPR'
                WHEN 'S'  THEN 'ECONOMY_ACT'
                WHEN 'GT' THEN 'GTC'
                ELSE COALESCE(instrument_type_cd, 'UNKNOWN')
            END                                     AS ia_type_cd,
            CASE instrument_type_cd
                WHEN 'M'  THEN 'Military Interdepartmental Purchase Request'
                WHEN 'S'  THEN 'Economy Act Order (31 U.S.C. 1535)'
                WHEN 'GT' THEN 'Governmentwide Acquisition Contract'
                ELSE COALESCE(instrument_type_cd, 'Unknown')
            END                                     AS ia_type_desc,
            ia_status_cd,
            ia_status_desc,
            instrument_type_cd,
            CASE instrument_type_cd
                WHEN 'M'  THEN 'MIPR'
                WHEN 'S'  THEN 'Economy Act'
                WHEN 'GT' THEN 'GT&C'
                ELSE COALESCE(instrument_type_cd, 'Unknown')
            END                                     AS instrument_type_desc,
            fiscal_year,
            ia_start_dt,
            ia_end_dt,
            total_direct_cost_est_amt,
            total_charges_est_amt,
            servicing_agency_sk,
            CAST(NULL AS BIGINT)                    AS requesting_agency_sk,
            program_cd,
            region_cd
        FROM with_sks
    """)

    # ── Step 3: Apply hybrid SCD2 ─────────────────────────────────────────────
    rows_closed, rows_inserted, rows_updated = scd2_apply_changes(
        spark, TARGET, "v_ia_cdc_source",
        NATURAL_KEY, TRACKED_COLS, INPLACE_COLS, RUN_ID
    )

    # ── Step 4: Net-new IAs ───────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET}
        (ia_id, ia_num, ia_type_cd, ia_type_desc, ia_status_cd, ia_status_desc,
         instrument_type_cd, instrument_type_desc, fiscal_year,
         ia_start_dt, ia_end_dt, total_direct_cost_est_amt, total_charges_est_amt,
         servicing_agency_sk, requesting_agency_sk, program_cd, region_cd,
         eff_start_dt, eff_end_dt, is_current_flag,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            src.ia_id, src.ia_num, src.ia_type_cd, src.ia_type_desc,
            src.ia_status_cd, src.ia_status_desc,
            src.instrument_type_cd, src.instrument_type_desc, src.fiscal_year,
            src.ia_start_dt, src.ia_end_dt,
            src.total_direct_cost_est_amt, src.total_charges_est_amt,
            src.servicing_agency_sk, src.requesting_agency_sk,
            src.program_cd, src.region_cd,
            CURRENT_TIMESTAMP(), CAST(NULL AS TIMESTAMP), TRUE,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{RUN_ID}'
        FROM v_ia_cdc_source src
        LEFT JOIN {TARGET} tgt
            ON tgt.{NATURAL_KEY} = src.{NATURAL_KEY}
        WHERE tgt.{NATURAL_KEY} IS NULL
    """)

    net_new = max(spark.sql(f"""
        SELECT COUNT(*) AS n FROM {TARGET}
        WHERE _source_batch_id = '{RUN_ID}' AND _gold_created_at >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
    """).collect()[0]["n"] - rows_inserted, 0)

    rows_inserted += net_new
    audit_success_cdc(spark, RUN_ID, TARGET, rows_read,
                      rows_inserted + rows_updated,
                      rows_inserted, rows_updated, start_ts, watermark_to)

except Exception as e:
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
