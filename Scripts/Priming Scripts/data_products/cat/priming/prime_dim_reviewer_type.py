# Databricks notebook source
# =============================================================================
# cat/prime_dim_reviewer_type.py
# Primes assist_dev.cat.dim_reviewer_type
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_reviewer_type.cd
#
# Source: silver_aasbs_lu_reviewer_type  (cd, description, sort_order, active_yn)
#
# Simplest dim in DP10 — cd + desc only, no derived fields.
# IMPROVEMENT I-DP10-1 note: row count verification only (no CASE derivations
# needed for this table).
#
# Ref: ASSIST pre-award review workflow documentation
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP10"
NOTEBOOK     = "prime_dim_reviewer_type"
TARGET_TABLE = "assist_dev.cat.dim_reviewer_type"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_lu_reviewer_type
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_reviewer_type rows: {rows_read:,}")

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (reviewer_type_cd, reviewer_type_desc,
         _gold_created_at, _gold_updated_at, _source_batch_id)
        SELECT
            lrevt.cd                                            AS reviewer_type_cd,
            lrevt.description                                   AS reviewer_type_desc,
            current_timestamp(),
            current_timestamp(),
            '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_lu_reviewer_type lrevt
        WHERE COALESCE(lrevt.is_deleted, FALSE) = FALSE
    """)

    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    dist = spark.sql(
        f"SELECT reviewer_type_cd, reviewer_type_desc FROM {TARGET_TABLE} ORDER BY reviewer_type_cd"
    ).collect()
    print(f"[{NOTEBOOK}] Loaded reviewer types:")
    for row in dist:
        print(f"  {str(row[0]):<30}  {row[1]}")

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
