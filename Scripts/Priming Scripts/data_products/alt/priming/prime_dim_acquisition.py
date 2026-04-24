# Databricks notebook source
#%md
## prime_dim_acquisition — DP2 Award Lifecycle Tracker
#**Target:**  `assist_dev.alt.dim_acquisition`
#**SCD Type:** 2 — all rows inserted with `eff_start_dt = NOW()`, `eff_end_dt = NULL`, `is_current_flag = TRUE` on prime.
#**Strategy:** TRUNCATE → INSERT (fully idempotent).
#**Sources:**
#  - `assist_dev.assist_finance.silver_aasbs_acquisition` (base record)
#  - `assist_dev.assist_finance.silver_aasbs_acquisition_plan` (IGCE, performance type — LEFT JOIN on acquisition_id)
#  - `assist_dev.common.dim_ia` (ia_sk lookup via ia_id)
#  - `assist_dev.common.dim_agency` (agency_sk lookup via activity_address_code)
#**Assumptions:**
#  - DP1 has already run successfully; `common.dim_ia` and `common.dim_agency` are populated.
#  - If `ia_sk` or `agency_sk` cannot be resolved, sentinel value `-1` is inserted.
#  - `acquisition_plan` is optional per acquisition; missing plan rows yield NULLs for IGCE/performance fields.
#  - Lookup decode tables (lu_acquisition_status, lu_acquisition_type, etc.) joined from Silver.
#  - `lu_intel_community` is not in DP2 Silver scope — `intel_community_cd` sourced directly from `acquisition.intel_community`.

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Parameters (injected by driver via dbutils.widgets)
# ---------------------------------------------------------------------------
dbutils.widgets.text("run_id",  "", "Pipeline Run ID")
dbutils.widgets.text("env",     "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP2"
NOTEBOOK     = "prime_dim_acquisition"
TARGET_TABLE = "assist_dev.alt.dim_acquisition"
TASK   = "prime_dim_acquisition"

dbutils.widgets.text("job_name", "dp2_prime_full", "Job Name")
JOB_NAME = dbutils.widgets.get("job_name")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SILVER = "assist_dev.assist_finance"
GOLD   = "assist_dev.alt"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")

start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_acquisition")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # -----------------------------------------------------------------------
    # Step 1 — TRUNCATE target
    # -----------------------------------------------------------------------
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # -----------------------------------------------------------------------
    # Step 2 — Read Silver sources
    # -----------------------------------------------------------------------
    rows_read = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_acquisition").collect()[0][0]
    print(f"[{NOTEBOOK}] Silver acquisition row count: {rows_read:,}")

    # -----------------------------------------------------------------------
    # Step 3 — INSERT
    # Acquisition plan joined LEFT (optional 1:1 on acquisition_id).
    # Lookup tables for decoded descriptions.
    # ia_sk / agency_sk resolved from common dims; sentinel -1 if unresolvable.
    # -----------------------------------------------------------------------
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            acquisition_id,
            acquisition_status_cd,
            acquisition_status_desc,
            acquisition_type_cd,
            acquisition_type_desc,
            competition_type_cd,
            competition_type_desc,
            commercial_type_cd,
            processing_speed_cd,
            expedited_type_cd,
            emergency_acq_cd,
            cor_delegation_cd,
            consolidated_contract_cd,
            supply_chain_risk_cd,
            surveillance_spec_cd,
            intel_community_cd,
            igce_amt,
            igce_source,
            performance_type_cd,
            ia_sk,
            agency_sk,
            created_dt,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            a.id                                                        AS acquisition_id,

            -- Status (decoded)
            --a.acquisition_status                                        AS acquisition_status_cd,
            a.acquisition_status_cd,
            COALESCE(lus.description, a.acquisition_status_cd)            AS acquisition_status_desc,

            -- Type (decoded)
            a.acquisition_type_cd                                          AS acquisition_type_cd,
            COALESCE(lut.description, a.acquisition_type_cd)                AS acquisition_type_desc,

            -- Competition (decoded from Silver lookup)
            lct.cd                                          AS competition_type_cd,
            COALESCE(lct.description, lct.cd)    AS competition_type_desc,

            -- Commercial determination
            ap.commercial_type_cd                                           AS commercial_type_cd,

            -- Processing speed / expedited
            ap.processing_speed_cd                                          AS processing_speed_cd,
            ap.expedited_type_cd                                            AS expedited_type_cd,

            -- FAR Part 18 emergency
            ap.ap_emergency_acq_cd                                             AS emergency_acq_cd,

            -- COR delegation
            ap.ap_cor_delegation_cd                                            AS cor_delegation_cd,

            ap.contract_type_cd                                     AS consolidated_contract_cd,

            -- CMMC supply chain risk
            ap.ap_supply_chain_risk_cd                                         AS supply_chain_risk_cd,

            -- DD-254 surveillance spec
            ap.ap_surveillance_spec_cd                                         AS surveillance_spec_cd,

            -- Intelligence community indicator (stored directly on acquisition)
            ap.ap_intel_community_cd                                           AS intel_community_cd,

            -- IGCE from acquisition_plan (NULL if no plan exists)
            -- ap.igce_amt                                                 AS 
            NULL as igce_amt,
            -- ap.igce_source                                              AS 
            NULL as igce_source,

            -- Performance type from acquisition_plan
            a.performance_type_cd                                         AS performance_type_cd,

            -- ia_sk — resolved from common.dim_ia (current version) via ia_id
            -- Assumption: dim_ia is keyed on ia_id (natural key ia.id in source)
            COALESCE(dia.ia_sk, -1)                                     AS ia_sk,

            -- agency_sk — resolved from common.dim_agency via activity_address_code
            COALESCE(dag.agency_sk, -1)                                 AS agency_sk,

            -- Dates
            ap.created_dt                                                AS created_dt,

            -- SCD2 fields: prime sets all rows as current
            current_timestamp()                                         AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)                                     AS eff_end_dt,
            TRUE                                                        AS is_current_flag,

            -- Audit
            current_timestamp()                                         AS _gold_created_at,
            current_timestamp()                                         AS _gold_updated_at,
            '{RUN_ID}'                                                  AS _source_batch_id

        FROM {SILVER}.silver_aasbs_acquisition a

        -- Acquisition plan (optional 1:1)
        LEFT JOIN {SILVER}.silver_aasbs_acquisition_plan ap
            ON ap.acquisition_id = a.id

        -- Lookup: acquisition status description
        LEFT JOIN {SILVER}.silver_aasbs_lu_acquisition_status lus
            ON lus.cd = a.acquisition_status_cd
            -- ON lus.acquisition_status = a.acquisition_status

        -- Lookup: acquisition type description
        LEFT JOIN {SILVER}.silver_aasbs_lu_acquisition_type lut
            ON lut.cd = a.acquisition_type_cd

        -- Lookup: competition type description
        LEFT JOIN {SILVER}.silver_aasbs_lu_competition_type lct
            ON lct.cd = ap.competition_type_cd

        -- Resolve ia_sk from common.dim_ia (current version only)
        LEFT JOIN {COMMON}.dim_ia dia
            ON dia.ia_id = a.id
           AND dia.is_current_flag = TRUE

        -- Resolve agency_sk from common.dim_agency (current version only)
        -- acquisition.activity_address_code is the 6-char AAC tied to the ordering office
        LEFT JOIN {COMMON}.dim_agency dag
            ON dag.activity_address_cd = a.activity_address_cd
           AND dag.is_current_flag = TRUE
    """)

    # -----------------------------------------------------------------------
    # Step 4 — Count inserted rows for audit
    # -----------------------------------------------------------------------
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Sentinel report
    sentinel_count = spark.sql(f"""
        SELECT
            SUM(CASE WHEN ia_sk     = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN agency_sk = -1 THEN 1 ELSE 0 END) AS unresolved_agency
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(f"[{NOTEBOOK}] Sentinel FKs — ia_sk=-1: {sentinel_count[0]:,}, agency_sk=-1: {sentinel_count[1]:,}")

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
