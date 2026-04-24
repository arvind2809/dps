# Databricks notebook source
# MAGIC %md
# MAGIC ## prime_dim_fpds_psc — DP3 FPDS Reporting Extract
# MAGIC **Target:**  `assist_dev.fre.dim_fpds_psc`
# MAGIC **SCD Type:** 1 — PSC descriptions updated in-place. Prime performs TRUNCATE → INSERT.
# MAGIC **Strategy:** TRUNCATE → INSERT (fully idempotent).
# MAGIC **Source:** `assist_dev.assist_finance.silver_aasbs_lus_psc`
# MAGIC **Design Decisions:**
# MAGIC   - `psc_cd` = `lus_psc.psc_code` (4-char GSA Product and Service Code).
# MAGIC   - `psc_description` = `lus_psc.psc_full_name` preferred; falls back to `psc_name` if NULL.
# MAGIC   - `psc_category` = `lus_psc.psc_type` — values expected: 'Product', 'Service', 'R&D'.
# MAGIC   - `supply_category` = `lus_psc.level_1_category_name` (populated for Products only).
# MAGIC   - `service_category` = `lus_psc.level_2_category_name` (populated for Services only).
# MAGIC   - `naics_crosswalk` = `lus_psc.parent_psc_code` as a proxy (Q3 design decision).
# MAGIC     The authoritative PSC-to-NAICS crosswalk from the GSA PSC Manual is not in Silver scope.
# MAGIC     `parent_psc_code` provides a hierarchical classification code that serves as a valid
# MAGIC     grouping proxy until the full crosswalk is loaded via a supplemental reference table.
# MAGIC     CDC or a manual enrichment step will replace proxy values when the crosswalk is available.
# MAGIC   - Inactive PSC records (`active = 'N'`) are included on prime for historical reporting
# MAGIC     coverage; FPDS-NG may reference inactive codes on older CARs.
# MAGIC **Assumptions:**
# MAGIC   - `psc_code` is the natural key (deduplication not expected; source is a reference table).
# MAGIC   - `psc_include = 'Y'` filter is NOT applied — all PSC codes loaded for full FPDS history.

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP3"
NOTEBOOK     = "prime_dim_fpds_psc"
TARGET_TABLE = "assist_dev.fre.dim_fpds_psc"

SILVER = "assist_dev.assist_finance"
#RUN_ID = "12333"
#ENV = "dev"



# COMMAND ----------


#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, NOTEBOOK, PRODUCT, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_lus_psc")
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
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_lus_psc"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver lus_psc row count: {rows_read:,}")

    # -----------------------------------------------------------------------
    # Step 3 — INSERT
    # -----------------------------------------------------------------------
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            psc_cd,
            psc_description,
            psc_category,
            supply_category,
            service_category,
            naics_crosswalk,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: 4-character PSC code
            p.psc_code                                          AS psc_cd,

            -- Description: prefer full name, fall back to short name
            COALESCE(
                NULLIF(TRIM(p.psc_full_name), ''),
                NULLIF(TRIM(p.psc_name),      '')
            )                                                   AS psc_description,

            -- Category: Product / Service / R&D
            -- psc_type is the source field aligned to GSA PSC Manual taxonomy
            p.psc_type                                          AS psc_category,

            -- Supply category: level_1_category_name (populated for Products)
            -- NULL for Service / R&D codes — intentional
            CASE
                WHEN UPPER(COALESCE(p.psc_type,'')) NOT LIKE '%SERVICE%'
                 AND UPPER(COALESCE(p.psc_type,'')) NOT LIKE '%R&D%'
                THEN p.level_1_category_name
                ELSE NULL
            END                                                 AS supply_category,

            -- Service category: level_2_category_name (populated for Services/R&D)
            -- NULL for Product codes — intentional
            CASE
                WHEN UPPER(COALESCE(p.psc_type,'')) LIKE '%SERVICE%'
                  OR UPPER(COALESCE(p.psc_type,'')) LIKE '%R&D%'
                THEN p.level_2_category_name
                ELSE NULL
            END                                                 AS service_category,

            -- naics_crosswalk: parent_psc_code as proxy (Q3 design decision).
            -- Rationale: GSA PSC Manual NAICS crosswalk not in Silver scope at prime time.
            -- parent_psc_code is a 2-char hierarchical grouping code that provides a
            -- valid category-level classification proxy.
            -- NOTE: CDC or a reference load will replace this once the crosswalk is loaded.
            p.parent_psc_code                                   AS naics_crosswalk,

            -- Audit
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lus_psc p
        WHERE COALESCE(p.is_deleted, FALSE) = FALSE
    """)

    # -----------------------------------------------------------------------
    # Step 4 — Post-load summary
    # -----------------------------------------------------------------------
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    psc_stats = spark.sql(f"""
        SELECT
            psc_category,
            COUNT(*) AS cnt,
            SUM(CASE WHEN naics_crosswalk IS NOT NULL THEN 1 ELSE 0 END) AS has_crosswalk_proxy
        FROM {TARGET_TABLE}
        GROUP BY psc_category
        ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] PSC category distribution:")
    for row in psc_stats:
        print(f"  {str(row[0]):20s}  count={row[1]:>5,}  crosswalk_proxy={row[2]:>5,}")

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
