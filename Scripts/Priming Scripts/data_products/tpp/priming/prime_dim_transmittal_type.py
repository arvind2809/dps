# Databricks notebook source
# =============================================================================
# tpp/prime_dim_transmittal_type.py
# Primes assist_dev.tpp.dim_transmittal_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per transmittal_type_cd  (PO, RR, INV, BAAR, BILLING, etc.)
# SCD Type : 1 — reference data updated in-place
#
# Source tables (Silver):
#   silver_aasbs_transmit_lu_transmittal_type      → base reference rows
#   silver_aasbs_transmit_transmittal_spec_version → latest spec version per type
#
# Field mapping (all confirmed against Silver DDL):
#   transmittal_type_cd   ← lu_transmittal_type.cd                 (NK)
#   transmittal_type_desc ← lu_transmittal_type.description
#   flat_file_code        ← lu_transmittal_type.flatfile_cd
#   spec_version          ← MAX(transmittal_spec_version.specification_version) per cd
#   envelope_format       ← CAST(NULL)  [SILVER GAP — see note below]
#   poc_name              ← CAST(NULL)  [SILVER GAP — see note below]
#   poc_email             ← CAST(NULL)  [SILVER GAP — see note below]
#
# SILVER GAP — envelope_format:
#   No envelope format field exists at the transmittal-type level in Silver.
#   silver_aasbs_transmit_transmittal_envelope holds per-AAC config (envelope_version)
#   not a per-type canonical format code. CDC enrichment or a supplemental
#   reference load is required. Column remains NULL on prime.
#
# SILVER GAP — poc_name / poc_email:
#   silver_aasbs_transmit_transmittal_poc holds POC records, but they are linked
#   via silver_aasbs_transmit_aac_envelope.transmittal_poc_id (per-AAC, not per-type).
#   No direct per-type POC assignment exists in Silver at prime time.
#   Populated per-AAC in dim_aac_envelope instead. NULL here by design.
#
# IMPROVEMENT I-DP5-1 (built-in):
#   spec_version resolved via pre-aggregated view v_spec_version_per_type to
#   prevent fan-out — transmittal_spec_version can have multiple version rows
#   per transmittal_type_cd. MAX(specification_version) picks the current active.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_dim_transmittal_type"
TARGET_TABLE = "assist_dev.tpp.dim_transmittal_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_transmit_lu_transmittal_type")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_transmit_lu_transmittal_type
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_transmittal_type rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — IMPROVEMENT I-DP5-1: pre-aggregate spec_version per type
    #
    # transmittal_spec_version has multiple rows per transmittal_type_cd
    # (one per specification revision).  MAX() picks the latest active version.
    # Without pre-aggregation this would fan-out in the main INSERT.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_spec_version_per_type AS
        SELECT
            transmittal_type_cd,
            MAX(specification_version)  AS spec_version
        FROM {SILVER}.silver_aasbs_transmit_transmittal_spec_version
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY transmittal_type_cd
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            transmittal_type_cd,
            transmittal_type_desc,
            flat_file_code,
            spec_version,
            envelope_format,
            poc_name,
            poc_email,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            ltt.cd                                              AS transmittal_type_cd,

            -- Description
            ltt.description                                     AS transmittal_type_desc,

            -- Flat-file format code used in Pegasys transmissions
            -- Source: lu_transmittal_type.flatfile_cd
            ltt.flatfile_cd                                     AS flat_file_code,

            -- Latest active specification version for this transmittal type
            -- IMPROVEMENT I-DP5-1: resolved via pre-aggregated view (MAX per type)
            sv.spec_version,

            -- SILVER GAP: envelope_format — no per-type format code in Silver.
            -- transmittal_envelope holds per-AAC config; no canonical type-level
            -- format. Annotated NULL pending CDC or supplemental reference load.
            CAST(NULL AS STRING)                                AS envelope_format,

            -- SILVER GAP: poc_name, poc_email — transmittal_poc linked per-AAC
            -- (via aac_envelope.transmittal_poc_id), not per transmittal type.
            -- Per-AAC POC is populated in dim_aac_envelope instead.
            CAST(NULL AS STRING)                                AS poc_name,
            CAST(NULL AS STRING)                                AS poc_email,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_transmit_lu_transmittal_type ltt

        -- IMPROVEMENT I-DP5-1: join pre-aggregated view (not raw table)
        LEFT JOIN v_spec_version_per_type sv
            ON  sv.transmittal_type_cd = ltt.cd

        WHERE COALESCE(ltt.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    coverage = spark.sql(f"""
        SELECT
            COUNT(*)                                                           AS total_types,
            SUM(CASE WHEN spec_version  IS NOT NULL  THEN 1 ELSE 0 END)      AS has_spec_version,
            SUM(CASE WHEN flat_file_code IS NOT NULL THEN 1 ELSE 0 END)      AS has_flat_file_code,
            SUM(CASE WHEN envelope_format IS NOT NULL THEN 1 ELSE 0 END)     AS has_envelope_format,
            SUM(CASE WHEN poc_name IS NOT NULL        THEN 1 ELSE 0 END)     AS has_poc
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={coverage[0]:,} | "
        f"spec_version={coverage[1]:,} | "
        f"flat_file_code={coverage[2]:,}"
    )
    print(
        f"[{NOTEBOOK}] Silver gaps — "
        f"envelope_format={coverage[3]:,} (expected 0, Silver gap) | "
        f"poc={coverage[4]:,} (expected 0, Silver gap)"
    )

    # Print distinct types loaded for operator validation
    types_dist = spark.sql(f"""
        SELECT transmittal_type_cd, transmittal_type_desc, spec_version
        FROM {TARGET_TABLE}
        ORDER BY transmittal_type_cd
    """).collect()
    print(f"[{NOTEBOOK}] Transmittal types loaded:")
    for row in types_dist:
        print(f"  {str(row[0]):<12}  {str(row[1]):<35}  spec={row[2]}")

    assert coverage[3] == 0, "ASSERT FAILED: envelope_format must be NULL on prime (Silver gap)"
    assert coverage[4] == 0, "ASSERT FAILED: poc fields must be NULL on prime (Silver gap)"

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
