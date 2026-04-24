# Databricks notebook source
# =============================================================================
# wpr/prime_dim_industry_partner.py
# Primes assist_dev.wpr.dim_industry_partner
#
# Strategy : TRUNCATE → INSERT  (SCD2 current snapshot — IMPROVEMENT I-DP8-1)
# Grain    : One row per industry_partners.ipartner_id  (current state on prime)
# SCD Type : 2 — history managed by CDC after prime
#
# Source tables (Silver):
#   silver_table_master_industry_partners  →  industry partner / contractor records
#
# Field mapping (all confirmed against Silver DDL — cleanest table in DP8):
#   partner_id         ← industry_partners.ipartner_id           (NK)
#   company_name       ← industry_partners.ipartner_company_name
#   contract_status_cd ← industry_partners.ipartner_inc_status
#   ftin               ← industry_partners.ipartner_ftin
#   address_city       ← industry_partners.city
#   address_state_cd   ← industry_partners.state
#   address_country_cd ← industry_partners.country
#   eff_start_dt       ← industry_partners.creation_date  [SCD2 prime]
#   eff_end_dt         ← CAST(NULL AS TIMESTAMP)          [SCD2 prime]
#   is_current_flag    ← TRUE                             [SCD2 prime]
#
# Note: small_business_class, UEI, and DUNS are present in Silver
# (ipartner.small_business_class, uei, duns_num) but are NOT in the Gold
# DDL for dim_industry_partner — those fields live in alt.dim_contractor (DP2).
# dim_industry_partner is the registry identity table; dim_contractor is the
# procurement classification table. Deliberate separation by design.
#
# IMPROVEMENT I-DP8-1 — SCD2 snapshot prime (built-in):
#   Same pattern as dim_user and dim_client.
#   run_type = 'FULL_PRIME_SCD2_SNAPSHOT'
#   Post-load assertions: is_current_flag=FALSE → 0 rows; eff_end_dt IS NOT NULL → 0 rows.
#
# Ref: FAR Part 9 (contractor qualifications), FAR Part 19 (SB — via alt.dim_contractor)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_industry_partner"
TARGET_TABLE = "assist_dev.wpr.dim_industry_partner"
RUN_TYPE     = "FULL_PRIME_SCD2_SNAPSHOT"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type=RUN_TYPE)
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_table_master_industry_partners")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(
    f"[{NOTEBOOK}] IMPROVEMENT I-DP8-1 — SCD2 snapshot prime: "
    f"one row per partner, eff_end_dt=NULL, is_current_flag=TRUE"
)

try:

    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_table_master_industry_partners
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND COALESCE(delete_flag, 'N') <> 'Y'
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source industry_partners rows: {rows_read:,}")

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            partner_id,
            company_name,
            contract_status_cd,
            ftin,
            address_city,
            address_state_cd,
            address_country_cd,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            ip.ipartner_id                                      AS partner_id,

            -- Company legal name
            ip.ipartner_company_name                            AS company_name,

            -- Incorporation/contract status code
            ip.ipartner_inc_status                              AS contract_status_cd,

            -- Federal Tax Identification Number
            ip.ipartner_ftin                                    AS ftin,

            -- Physical address components
            ip.city                                             AS address_city,
            ip.state                                            AS address_state_cd,
            ip.country                                          AS address_country_cd,

            -- IMPROVEMENT I-DP8-1: SCD2 prime fields
            -- eff_start_dt = account creation date (initial record date)
            COALESCE(ip.creation_date, current_timestamp())     AS eff_start_dt,
            CAST(NULL AS TIMESTAMP)                             AS eff_end_dt,
            TRUE                                                AS is_current_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_table_master_industry_partners ip
        WHERE COALESCE(ip.is_deleted, FALSE) = FALSE
          AND COALESCE(ip.delete_flag, 'N') <> 'Y'
    """)

    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                         AS total,
            SUM(CASE WHEN is_current_flag = FALSE  THEN 1 ELSE 0 END)      AS not_current,
            SUM(CASE WHEN eff_end_dt IS NOT NULL   THEN 1 ELSE 0 END)      AS has_end_dt,
            SUM(CASE WHEN ftin IS NOT NULL         THEN 1 ELSE 0 END)      AS has_ftin,
            SUM(CASE WHEN address_city IS NOT NULL THEN 1 ELSE 0 END)      AS has_city
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — total={stats[0]:,} | "
        f"has_ftin={stats[3]:,} | has_city={stats[4]:,}"
    )

    # IMPROVEMENT I-DP8-1: SCD2 snapshot assertions
    assert stats[1] == 0, \
        f"ASSERT FAILED: is_current_flag must be TRUE for all rows on prime. Got {stats[1]} FALSE rows."
    assert stats[2] == 0, \
        f"ASSERT FAILED: eff_end_dt must be NULL for all rows on prime. Got {stats[2]} non-NULL rows."
    print(f"[{NOTEBOOK}] ✓ SCD2 snapshot assertions passed.")

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
