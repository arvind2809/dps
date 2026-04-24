# Databricks notebook source
# =============================================================================
# wpr/prime_dim_client.py
# Primes assist_dev.wpr.dim_client
#
# Strategy : TRUNCATE → INSERT  (SCD2 current snapshot — IMPROVEMENT I-DP8-1)
# Grain    : One row per clients.client_id  (current state only on prime)
# SCD Type : 2 — history managed by CDC after prime
#
# Source tables (Silver):
#   silver_table_master_clients  →  client agency records
#
# IMPROVEMENT I-DP8-1 — SCD2 snapshot prime (built-in):
#   Same SCD2 priming pattern as dim_user:
#     eff_start_dt   = clients.creation_date
#     eff_end_dt     = CAST(NULL AS TIMESTAMP)
#     is_current_flag= TRUE for all rows on prime
#   run_type = 'FULL_PRIME_SCD2_SNAPSHOT'
#
# IMPROVEMENT I-DP8-5 — agency_sk = NULL (not sentinel) (built-in):
#   clients.agency_code is a 2-character agency identifier (e.g. 'GS', 'DO').
#   common.dim_agency is keyed on activity_address_cd, a 6-character AAC code.
#   These are incompatible code systems — a direct join would produce
#   systematically wrong FK resolutions.
#   agency_sk = CAST(NULL AS BIGINT), NOT sentinel -1.
#   Sentinel -1 implies a FK lookup was attempted and failed for a valid entity.
#   NULL here means the FK cannot be resolved with the available code format.
#   CDC resolution path: confirm a bridge from agency_code (2-char) to AAC
#   (6-char) and extend the dim load once confirmed.
#
# Field mapping (all confirmed against Silver DDL):
#   client_id      ← clients.client_id                    (NK)
#   client_name    ← clients.client_name
#   client_code    ← clients.client_code
#   contact_name   ← clients.gsa_contact
#   contact_email  ← clients.email
#   contact_phone  ← CONCAT(phone, COALESCE(' x'||phone_ext,''))
#   agency_sk      ← CAST(NULL AS BIGINT)  [FORMAT MISMATCH — I-DP8-5]
#   eff_start_dt   ← clients.creation_date  [SCD2 prime]
#   eff_end_dt     ← CAST(NULL AS TIMESTAMP) [SCD2 prime]
#   is_current_flag← TRUE                   [SCD2 prime]
#
# Ref: FAR 1.602-2(d) (client contact for COR delegation notification)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_client"
TARGET_TABLE = "assist_dev.wpr.dim_client"
RUN_TYPE     = "FULL_PRIME_SCD2_SNAPSHOT"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type=RUN_TYPE)
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_table_master_clients")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(
    f"[{NOTEBOOK}] IMPROVEMENT I-DP8-1 — SCD2 snapshot prime  "
    f"| IMPROVEMENT I-DP8-5 — agency_sk=NULL (format mismatch)"
)

try:

    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_table_master_clients
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND COALESCE(delete_flag, 'N') <> 'Y'
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source clients rows: {rows_read:,}")

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            client_id,
            client_name,
            client_code,
            contact_name,
            contact_email,
            contact_phone,
            agency_sk,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            c.client_id,
            c.client_name,
            c.client_code,

            -- Primary GSA-side contact (free-text name field)
            c.gsa_contact                                       AS contact_name,

            -- Contact details
            c.email                                             AS contact_email,
            CASE
                WHEN c.phone_ext IS NOT NULL AND c.phone_ext <> ''
                THEN CONCAT(COALESCE(c.phone,''), ' x', c.phone_ext)
                ELSE c.phone
            END                                                 AS contact_phone,

            -- IMPROVEMENT I-DP8-5: agency_sk = NULL (not sentinel -1).
            -- clients.agency_code is a 2-char agency identifier ('GS','DO',etc.).
            -- common.dim_agency is keyed on 6-char activity_address_cd (AAC).
            -- These are different code systems — a join would be wrong.
            -- NULL = code format incompatibility (not a failed FK lookup).
            -- CDC resolution: bridge from 2-char agency_code to 6-char AAC.
            CAST(NULL AS BIGINT)                                AS agency_sk,

            -- IMPROVEMENT I-DP8-1: SCD2 prime fields
            COALESCE(c.creation_date, current_timestamp())      AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)                             AS eff_end_dt,
            TRUE                                                AS is_current_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_table_master_clients c
        WHERE COALESCE(c.is_deleted, FALSE) = FALSE
          AND COALESCE(c.delete_flag, 'N') <> 'Y'
    """)

    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                           AS total,
            SUM(CASE WHEN is_current_flag = FALSE   THEN 1 ELSE 0 END)       AS not_current,
            SUM(CASE WHEN eff_end_dt IS NOT NULL    THEN 1 ELSE 0 END)       AS has_end_dt,
            SUM(CASE WHEN agency_sk IS NOT NULL     THEN 1 ELSE 0 END)       AS has_agency_sk,
            SUM(CASE WHEN client_active_flag = TRUE THEN 1 ELSE 0 END)       AS active_src
        FROM {TARGET_TABLE}
        LEFT JOIN {SILVER}.silver_table_master_clients c_src
            ON c_src.client_id = {TARGET_TABLE}.client_id
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — total={stats[0]:,} | "
        f"agency_sk_NULL={stats[0]-stats[3]:,} (expected all — format mismatch)"
    )

    # IMPROVEMENT I-DP8-1: SCD2 assertions
    assert stats[1] == 0, \
        f"ASSERT FAILED: is_current_flag must be TRUE for all rows on prime. Got {stats[1]} FALSE rows."
    assert stats[2] == 0, \
        f"ASSERT FAILED: eff_end_dt must be NULL for all rows on prime. Got {stats[2]} non-NULL rows."
    # IMPROVEMENT I-DP8-5: agency_sk = NULL assertion
    assert stats[3] == 0, \
        f"ASSERT FAILED: agency_sk must be NULL on prime (format mismatch). Got {stats[3]} non-NULL rows."
    print(f"[{NOTEBOOK}] ✓ All SCD2 and agency_sk assertions passed.")

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
