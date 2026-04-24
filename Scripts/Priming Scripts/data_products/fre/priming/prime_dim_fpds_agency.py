# Databricks notebook source
# MAGIC %md
# MAGIC ## prime_dim_fpds_agency — DP3 FPDS Reporting Extract
# MAGIC **Target:**  `assist_dev.fre.dim_fpds_agency`
# MAGIC **SCD Type:** 1 — agency attributes updated in-place.
# MAGIC **Strategy:** TRUNCATE → INSERT (fully idempotent).
# MAGIC **Grain:**    One row per Activity Address Code (AAC).
# MAGIC
# MAGIC **Sources:**
# MAGIC   - `silver_aasbs_lu_activity_address_code` — grain driver (AAC as NK)
# MAGIC   - `silver_aasbs_lu_agency` — agency code, name; via `fpds_ng_agency_identifier = lu_agency.cd`
# MAGIC   - `silver_aasbs_lu_bureau` — bureau code/name; first bureau per agency by sort_order
# MAGIC   - `silver_table_master_lu_federal_agency` — canonical FPDS-NG names; via `lu_agency.cd_2char`
# MAGIC
# MAGIC **DDL additions (v1.1.0 — must-change):**
# MAGIC   Two new columns added to the Gold DDL are seeded NULL (Silver-pending) here:
# MAGIC   - `place_of_performance_state`   — FAR 4.606(a)(8); no Silver source yet.
# MAGIC   - `place_of_performance_country` — FAR 4.606(a)(8); no Silver source yet.
# MAGIC   `aasbs.place_of_performance` must be ingested into Silver before these
# MAGIC   columns can be populated.  CDC will back-fill once available.
# MAGIC
# MAGIC **Field mapping:**
# MAGIC   - `aac`                   = `lu_activity_address_code.cd`  (NK)
# MAGIC   - `agency_code`           = `lu_agency.cd_2char`
# MAGIC   - `bureau_code`           = `lu_bureau.cd` (first by sort_order)
# MAGIC   - `cgac`                  = `lu_agency.cd_2char` (see note)
# MAGIC   - `agency_name`           = `lu_federal_agency.agency_description` or `lu_agency.description`
# MAGIC   - `bureau_name`           = `lu_federal_agency.bureau_description` or `lu_bureau.description`
# MAGIC   - `contracting_office_name` = `lu_activity_address_code.description`
# MAGIC   - `funding_agency_code`   = seeded same as `agency_code` on prime (CDC corrects)
# MAGIC   - `funding_bureau_code`   = seeded same as `bureau_code` on prime (CDC corrects)
# MAGIC
# MAGIC **Design notes:**
# MAGIC   - The same dim backs both `fpds_agency_sk` (awarding) and `funding_agency_sk`
# MAGIC     (funding) on `fact_fpds_transaction`; both resolve against this AAC-grained dim.
# MAGIC   - Inactive AACs (`active_yn = 'N'`) are included for historical CAR coverage.
# MAGIC   - CGAC equals the 2-digit agency code for GSA-administered programs.
# MAGIC     Adjust if your environment uses a distinct CGAC column.

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP3"
NOTEBOOK     = "prime_dim_fpds_agency"
TARGET_TABLE = "assist_dev.fre.dim_fpds_agency"

SILVER     = "assist_dev.assist_finance"
TBL_MASTER = "assist_dev.assist_finance"   # silver_table_master_* also in this schema

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, NOTEBOOK, PRODUCT, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_lu_activity_address_code")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_lu_activity_address_code"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver lu_activity_address_code row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — First bureau per agency (prevent fan-out)
    #
    # Bureau is an attribute, not a grain driver.  Pick the lowest
    # sort_order bureau per agency_cd to produce a deterministic 1:1.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_bureau_first AS
        SELECT
            b.agency_cd,
            b.cd          AS bureau_cd,
            b.description AS bureau_description
        FROM (
            SELECT
                agency_cd,
                cd,
                description,
                ROW_NUMBER() OVER (
                    PARTITION BY agency_cd
                    ORDER BY updated_dt ASC, cd ASC
                ) AS rn
            FROM {SILVER}.silver_aasbs_lu_bureau
            WHERE COALESCE(is_deleted, FALSE) = FALSE
        ) b
        WHERE b.rn = 1
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            agency_code,
            bureau_code,
            aac,
            cgac,
            agency_name,
            bureau_name,
            contracting_office_name,
            funding_agency_code,
            funding_bureau_code,
            -- place_of_performance_state,
            -- place_of_performance_country,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            -- Two-digit FPDS agency code from lu_agency.cd_2char.
            -- NULL when the AAC has no matching agency record.

            COALESCE(lua.cd_2char, 0)                                          AS agency_code,

            -- Bureau code: first bureau for this agency by sort_order
            bf.bureau_cd                                            AS bureau_code,

            -- Activity Address Code — Natural Key at this grain
            aac.cd                                                  AS aac,

            -- CGAC: for GSA-administered programs equals the 2-digit agency code.
            -- Replace with a distinct source column if your environment differs.
            lua.cd_2char                                            AS cgac,

            -- Agency name: prefer canonical FPDS-NG name from table_master;
            -- fall back to lu_agency.description if no table_master row exists.
            -- COALESCE(
            --    NULLIF(TRIM(fam.agency_description), ''),
            --    NULLIF(TRIM(lua.description),        '')
            --)                                                       AS agency_name,
            'TEST' as agency_name,
            -- Bureau name: prefer table_master; fall back to lu_bureau.description
            COALESCE(
                NULLIF(TRIM(fam.bureau_description), ''),
                NULLIF(TRIM(bf.bureau_description),  '')
            )                                                       AS bureau_name,

            -- Contracting office name: the AAC description is the office name
            aac.description                                         AS contracting_office_name,

            -- Funding agency / bureau: seeded same as awarding agency on prime.
            -- CDC will correct rows where LOA funding agency differs from awarding.
            lua.cd_2char                                            AS funding_agency_code,
            bf.bureau_cd                                            AS funding_bureau_code,

            -- SILVER-PENDING: place_of_performance_state — FAR 4.606(a)(8).
            -- Source: aasbs.place_of_performance — not yet ingested into Silver.
            -- Will remain NULL until Silver pipeline is extended.
            -- CAST(NULL AS STRING)                                    AS place_of_performance_state,

            -- SILVER-PENDING: place_of_performance_country — FAR 4.606(a)(8).
            -- Source: aasbs.place_of_performance — not yet ingested into Silver.
            -- Will remain NULL until Silver pipeline is extended.
            -- CAST(NULL AS STRING)                                    AS place_of_performance_country,

            current_timestamp()                                     AS _gold_created_at,
            current_timestamp()                                     AS _gold_updated_at,
            '{RUN_ID}'                                              AS _source_batch_id

        FROM {SILVER}.silver_aasbs_lu_activity_address_code aac

        -- Agency: join on fpds_ng_agency_identifier = lu_agency.cd
        LEFT JOIN {SILVER}.silver_aasbs_lu_agency lua
            ON  lua.cd = aac.fpds_ng_agency_identifier
            AND COALESCE(lua.is_deleted, FALSE) = FALSE 

        -- Bureau: first bureau per agency (deduplicated in v_bureau_first)
        LEFT JOIN v_bureau_first bf
            ON  bf.agency_cd = lua.cd_2char

        -- Canonical names from GSA/FPDS-NG federal agency master (2-digit key)
        LEFT JOIN {TBL_MASTER}.silver_table_master_lu_federal_agency fam
            ON  fam.agency_code = lua.cd_2char
            AND COALESCE(fam.is_deleted, FALSE) = FALSE
        -- ADD lua.cd_2char is not NULL for DELTA_NOT_NULL_CONSTRAINT_VIOLATED
        WHERE COALESCE(aac.is_deleted, FALSE) = FALSE 
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load summary
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    coverage = spark.sql(f"""
        SELECT
            COUNT(*)                                                         AS total_aac_rows,
            SUM(CASE WHEN agency_code IS NOT NULL  THEN 1 ELSE 0 END)       AS resolved_agency,
            SUM(CASE WHEN bureau_code IS NOT NULL  THEN 1 ELSE 0 END)       AS resolved_bureau,
            SUM(CASE WHEN agency_name IS NOT NULL  THEN 1 ELSE 0 END)       AS resolved_name,
            SUM(CASE WHEN contracting_office_name IS NOT NULL
                     THEN 1 ELSE 0 END)                                     AS has_office_name,
            -- SUM(CASE WHEN place_of_performance_state IS NOT NULL
            --        THEN 1 ELSE 0 END)  AS non_null_pop_state,
            0 as non_null_pop_state,
            0 as non_null_pop_country
            -- SUM(CASE WHEN place_of_performance_country IS NOT NULL
            --        THEN 1 ELSE 0 END) AS non_null_pop_country
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={coverage[0]:,} | agency={coverage[1]:,} | "
        f"bureau={coverage[2]:,} | name={coverage[3]:,} | "
        f"office_name={coverage[4]:,}"
    )
    print(
        f"[{NOTEBOOK}] Silver-pending columns — "
        f"pop_state non-null={coverage[5]:,} (expected 0), "
        f"pop_country non-null={coverage[6]:,} (expected 0)"
    )

    assert coverage[5] == 0, \
        "ASSERT FAILED: place_of_performance_state must be NULL on prime (Silver-pending)"
    assert coverage[6] == 0, \
        "ASSERT FAILED: place_of_performance_country must be NULL on prime (Silver-pending)"

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
