# Databricks notebook source
# MAGIC %md
# MAGIC ## prime_dim_onefund_program — DP4 Accrual Income Reporter
# MAGIC **Target:**  `assist_dev.air.dim_onefund_program`
# MAGIC **SCD Type:** 1 — OneFund mappings updated in-place. Prime performs TRUNCATE → INSERT.
# MAGIC **Strategy:** TRUNCATE → INSERT (fully idempotent).
# MAGIC **Grain:** One row per Activity Address Code (AAC). Natural key = `aac`.
# MAGIC
# MAGIC **Sources:**
# MAGIC   - `silver_aasbs_map_aac_onefund` — grain driver. Maps each AAC to its OneFund
# MAGIC     fund code, activity code, program code, and OMB object classification code.
# MAGIC     Where an AAC has multiple active mapping rows (e.g., cost element variants),
# MAGIC     the first active row ordered by `created_dt ASC` is used (ROW_NUMBER dedup).
# MAGIC   - `silver_aasbs_lu_onefund_program` — program description text.
# MAGIC     Joined via `map_aac_onefund.onefund_program_cd = lu_onefund_program.cd`.
# MAGIC   - `silver_aasbs_lu_onefund_activity` — activity description text.
# MAGIC     Joined via `map_aac_onefund.onefund_activity_cd = lu_onefund_activity.cd`.
# MAGIC   - `silver_aasbs_map_onefund_program_accrual_income_doc_type` — BAAR document type
# MAGIC     for income transmission. Joined via `onefund_program_cd`.
# MAGIC   - `silver_aasbs_lu_accrual_income_doc_type` — BAAR doc type description.
# MAGIC     Joined via `accrual_income_doc_type_cd = lu_accrual_income_doc_type.cd`.
# MAGIC
# MAGIC **Field Mapping:**
# MAGIC   - `aac`                = `map_aac_onefund.activity_address_cd`   (Natural Key)
# MAGIC   - `onefund_fund_cd`    = `map_aac_onefund.fund_cd`
# MAGIC   - `onefund_activity_cd`= `map_aac_onefund.onefund_activity_cd`
# MAGIC   - `onefund_program_cd` = `map_aac_onefund.onefund_program_cd`
# MAGIC   - `object_class_cd`    = `map_aac_onefund.object_class_cd`      (OMB Object Classification)
# MAGIC   - `baar_doc_type_cd`   = `map_onefund_program_accrual_income_doc_type.accrual_income_doc_type_cd`
# MAGIC   - `baar_doc_type_desc` = `lu_accrual_income_doc_type.description`
# MAGIC   - `program_desc`       = `lu_onefund_program.description`
# MAGIC   - `activity_desc`      = `lu_onefund_activity.description`
# MAGIC
# MAGIC **Assumptions:**
# MAGIC   - AAC is the natural key. Where multiple `map_aac_onefund` rows exist per AAC,
# MAGIC     the first active row by `created_dt ASC` is used. Inactive rows (`active_yn = 'N'`)
# MAGIC     are excluded from the dedup window but the winner is still loaded even if the
# MAGIC     AAC's only row is inactive — inactive AACs may have historical accrual records.
# MAGIC   - `onefund_cd` and `fund_cd` are two separate fields in the source; `fund_cd` is
# MAGIC     stored in `onefund_fund_cd` as the GSA OneFund fund identifier.
# MAGIC   - BAAR doc type mapping is 1:1 per program code; if a program has no mapping,
# MAGIC     `baar_doc_type_cd` will be NULL.

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP4"
NOTEBOOK     = "prime_dim_onefund_program"
TARGET_TABLE = "assist_dev.air.dim_onefund_program"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

# start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_map_aac_onefund")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # -----------------------------------------------------------------------
    # Step 1 — TRUNCATE
    # -----------------------------------------------------------------------
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # -----------------------------------------------------------------------
    # Step 2 — Source count
    # -----------------------------------------------------------------------
    rows_read = spark.sql(
        f"""SELECT COUNT(DISTINCT activity_address_cd)
            FROM {SILVER}.silver_aasbs_map_aac_onefund
            WHERE COALESCE(is_deleted, FALSE) = FALSE"""
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Distinct active AACs in map_aac_onefund: {rows_read:,}")

    # -----------------------------------------------------------------------
    # Step 3 — Dedup: one row per AAC
    # Where multiple rows exist per AAC, pick the first active row by created_dt.
    # If no active row exists for an AAC, fall back to the first row regardless
    # of active_yn (inactive AACs may still have historical accrual records).
    # -----------------------------------------------------------------------
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_aac_onefund_dedup AS
        SELECT
            activity_address_cd,
            onefund_cd,
            fund_cd,
            onefund_activity_cd,
            onefund_program_cd,
            object_class_cd
        FROM (
            SELECT
                activity_address_cd,
                onefund_cd,
                fund_cd,
                onefund_activity_cd,
                onefund_program_cd,
                object_class_cd,
                active_yn,
                ROW_NUMBER() OVER (
                    PARTITION BY activity_address_cd
                    -- Active rows first, then oldest-created row as tiebreaker
                    ORDER BY
                        CASE WHEN UPPER(COALESCE(active_yn,'N')) = 'Y' THEN 0 ELSE 1 END ASC,
                        created_dt ASC
                ) AS rn
            FROM {SILVER}.silver_aasbs_map_aac_onefund
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        ) ranked
        WHERE rn = 1
    """)

    # -----------------------------------------------------------------------
    # Step 4 — INSERT
    # -----------------------------------------------------------------------
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            aac,
            onefund_fund_cd,
            onefund_activity_cd,
            onefund_program_cd,
            object_class_cd,
            baar_doc_type_cd,
            baar_doc_type_desc,
            program_desc,
            activity_desc,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural Key: Activity Address Code
            m.activity_address_cd                               AS aac,

            -- OneFund fund identifier (GSA internal fund code)
            m.fund_cd                                           AS onefund_fund_cd,

            -- OneFund activity code
            m.onefund_activity_cd                               AS onefund_activity_cd,

            -- OneFund program code
            m.onefund_program_cd                                AS onefund_program_cd,

            -- OMB Object Classification code (per OMB Circular A-11 §83)
            -- e.g., 25.2 (Other Services), 41.0 (Grants), etc.
            m.object_class_cd                                   AS object_class_cd,

            -- BAAR document type code for accrual income transmission
            -- from the OneFund program → BAAR doc type mapping table
            dt.accrual_income_doc_type_cd                       AS baar_doc_type_cd,

            -- BAAR document type description
            ldt.description                                     AS baar_doc_type_desc,

            -- OneFund program description
            lp.description                                      AS program_desc,

            -- OneFund activity description
            la.description                                      AS activity_desc,

            -- Audit
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM v_aac_onefund_dedup m

        -- OneFund program description
        LEFT JOIN {SILVER}.silver_aasbs_lu_onefund_program lp
            ON  lp.cd = m.onefund_program_cd
            AND COALESCE(lp.is_deleted, FALSE) = FALSE

        -- OneFund activity description
        LEFT JOIN {SILVER}.silver_aasbs_lu_onefund_activity la
            ON  la.cd = m.onefund_activity_cd
            AND COALESCE(la.is_deleted, FALSE) = FALSE

        -- BAAR doc type mapping: OneFund program → accrual income doc type
        LEFT JOIN {SILVER}.silver_aasbs_map_onefund_program_accrual_income_doc_type dt
            ON  dt.onefund_program_cd = m.onefund_program_cd
            AND COALESCE(dt.is_deleted, FALSE) = FALSE
            AND UPPER(COALESCE(dt.active_yn,'Y')) = 'Y'

        -- BAAR doc type description lookup
        LEFT JOIN {SILVER}.silver_aasbs_lu_accrual_income_doc_type ldt
            ON  ldt.cd = dt.accrual_income_doc_type_cd
            AND COALESCE(ldt.is_deleted, FALSE) = FALSE
    """)

    # -----------------------------------------------------------------------
    # Step 5 — Post-load summary
    # -----------------------------------------------------------------------
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    coverage = spark.sql(f"""
        SELECT
            COUNT(*)                                                            AS total_rows,
            SUM(CASE WHEN baar_doc_type_cd  IS NOT NULL THEN 1 ELSE 0 END)    AS has_baar_doc_type,
            SUM(CASE WHEN program_desc      IS NOT NULL THEN 1 ELSE 0 END)    AS has_program_desc,
            SUM(CASE WHEN onefund_program_cd IS NOT NULL THEN 1 ELSE 0 END)   AS has_program_cd
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Coverage — total={coverage[0]:,} | "
        f"baar_doc_type={coverage[1]:,} | "
        f"program_desc={coverage[2]:,} | "
        f"program_cd={coverage[3]:,}"
    )

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
