# Databricks notebook source
# =============================================================================
# tpp/prime_dim_aac_envelope.py
# Primes assist_dev.tpp.dim_aac_envelope
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per Activity Address Code (aac_envelope.activity_address_cd)
# SCD Type : 1 — envelope config updated in-place
#
# Source tables (Silver):
#   silver_aasbs_transmit_aac_envelope         → envelope config per AAC (NK)
#   silver_aasbs_transmit_transmittal_poc       → poc_name, poc_email
#   silver_aasbs_transmit_billing_summary       → last_transmission_dt (proxy)
#   silver_aasbs_transmit_transmittal_suspension → is_suspended_flag, dates
#
# Field mapping (all confirmed against Silver DDL):
#   aac                  ← aac_envelope.activity_address_cd      (NK)
#   envelope_status_cd   ← aac_envelope.transmittal_status_cd
#   poc_name             ← transmittal_poc.poc_name via aac_envelope.transmittal_poc_id
#   poc_email            ← transmittal_poc.poc_email via same
#   last_transmission_dt ← MAX(billing_summary.generated_dt) per activity_address_cd
#   is_suspended_flag    ← EXISTS(suspension WHERE type_cd matches AND NOW() in window)
#   suspension_start_dt  ← transmittal_suspension.suspend_start_dt (latest active)
#   suspension_end_dt    ← transmittal_suspension.suspend_end_dt   (latest active)
#
# Design decisions:
#   last_transmission_dt: billing_summary.generated_dt used as the best available
#     proxy for "last time a transmission was sent from this AAC".  The field
#     reflects the document generation timestamp for billing records, which is the
#     closest approximation to transmission time available in Silver on prime.
#     CDC will refine this from transmittal.sent_dt once that join is enriched.
#
#   is_suspended_flag: transmittal_suspension is keyed by transmittal_type_cd
#     (not by AAC directly).  aac_envelope.transmittal_type_cd bridges these.
#     A suspension is "active" if current_timestamp() falls between suspend_start_dt
#     and suspend_end_dt (or suspend_end_dt IS NULL = indefinite suspension).
#     MAX(suspend_start_dt) used where multiple suspension records exist per type.
#
# IMPROVEMENT I-DP5-1 (built-in):
#   Three pre-aggregated temp views prevent fan-out:
#     v_last_tx_per_aac        — MAX(generated_dt) per activity_address_cd
#     v_suspension_per_type    — latest active suspension per transmittal_type_cd
#   One-row-per-AAC is guaranteed before the main INSERT via LEFT JOINs.
#
# IMPROVEMENT I-DP5-6 (built-in):
#   Pre-flight guard verifies tpp.dim_transmittal_type is populated before
#   proceeding.  The suspension join keys on transmittal_type_cd; if the type
#   dim is empty the suspension flag will silently be wrong for all AACs.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_dim_aac_envelope"
TARGET_TABLE = "assist_dev.tpp.dim_aac_envelope"

SILVER = "assist_dev.assist_finance"
TPP    = "assist_dev.tpp"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="dim_transmittal_type")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # IMPROVEMENT I-DP5-6: pre-flight guard
    # dim_transmittal_type must be populated before dim_aac_envelope runs.
    # The suspension join uses transmittal_type_cd; an empty type dim would
    # silently produce wrong is_suspended_flag values for all AAC rows.
    # ─────────────────────────────────────────────────────────────────────
    type_count = spark.sql(
        f"SELECT COUNT(*) FROM {TPP}.dim_transmittal_type"
    ).collect()[0][0]

    if type_count == 0:
        err = (
            f"[{NOTEBOOK}] BLOCKED — {TPP}.dim_transmittal_type is empty. "
            f"prime_dim_transmittal_type must complete before prime_dim_aac_envelope."
        )
        print(err)
        #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
        audit_fail(spark, RUN_ID, TARGET_TABLE, str(err), err, start_ts)
        dbutils.notebook.exit("BLOCKED_TYPE_DIM_EMPTY")

    print(f"[{NOTEBOOK}] Pre-flight ✓ dim_transmittal_type={type_count:,} rows")

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(DISTINCT activity_address_cd)
        FROM {SILVER}.silver_aasbs_transmit_aac_envelope
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Distinct AACs in aac_envelope: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — IMPROVEMENT I-DP5-1: pre-aggregate to prevent fan-out
    # ─────────────────────────────────────────────────────────────────────

    # 2a — Last transmission date per AAC
    # Proxy: MAX(billing_summary.generated_dt) per activity_address_cd.
    # generated_dt is the BAAR document generation timestamp — the closest
    # available approximation to "last transmission from this AAC" at prime time.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_last_tx_per_aac AS
        SELECT
            activity_address_cd                 AS aac,
            MAX(generated_dt)                   AS last_transmission_dt
        FROM {SILVER}.silver_aasbs_transmit_billing_summary
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND activity_address_cd IS NOT NULL
        GROUP BY activity_address_cd
    """)

    # 2b — Latest active suspension per transmittal_type_cd
    # transmittal_suspension is keyed by type, not AAC.
    # "Active" = suspend_end_dt IS NULL (indefinite) OR <= current date.
    # MAX(suspend_start_dt) picks the most recently started suspension per type.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_suspension_per_type AS
        SELECT
            transmittal_type_cd,
            MAX(suspend_start_dt)   AS suspension_start_dt,
            MAX(suspend_end_dt)     AS suspension_end_dt,
            -- Active if suspend_end_dt IS NULL or in the future
            CASE
                WHEN MAX(suspend_end_dt) IS NULL THEN TRUE
                WHEN MAX(suspend_end_dt) >= current_timestamp() THEN TRUE
                ELSE FALSE
            END                     AS is_suspended_flag
        FROM {SILVER}.silver_aasbs_transmit_transmittal_suspension
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY transmittal_type_cd
    """)

    print(f"[{NOTEBOOK}] Supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Dedup aac_envelope to one row per AAC
    # Multiple rows can exist per AAC (different transmittal_type_cd variants).
    # Use ROW_NUMBER() ordering by active status first, then most recently
    # created row as tiebreaker.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_aac_envelope_dedup AS
        SELECT *
        FROM (
            SELECT
                ae.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ae.activity_address_cd
                    ORDER BY ae.created_dt DESC
                ) AS rn
            FROM {SILVER}.silver_aasbs_transmit_aac_envelope ae
            WHERE COALESCE(ae.is_deleted, FALSE) = FALSE
        )
        WHERE rn = 1
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            aac,
            envelope_status_cd,
            poc_name,
            poc_email,
            last_transmission_dt,
            is_suspended_flag,
            suspension_start_dt,
            suspension_end_dt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: Activity Address Code
            ae.activity_address_cd                              AS aac,

            -- Envelope transmission status code
            ae.transmittal_status_cd                            AS envelope_status_cd,

            -- POC: resolved via aac_envelope.transmittal_poc_id → transmittal_poc
            poc.poc_name,
            poc.poc_email,

            -- Last transmission date: MAX(billing_summary.generated_dt) per AAC
            -- Proxy for "last time a document was generated for transmission from
            -- this AAC". CDC will refine from transmittal.sent_dt.
            ltx.last_transmission_dt,

            -- Suspension flag: TRUE if an active suspension exists for this
            -- AAC's transmittal_type_cd. NULL if no suspension record found.
            COALESCE(sp.is_suspended_flag, FALSE)               AS is_suspended_flag,
            sp.suspension_start_dt,
            sp.suspension_end_dt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM v_aac_envelope_dedup ae

        -- POC details via transmittal_poc_id FK
        LEFT JOIN {SILVER}.silver_aasbs_transmit_transmittal_poc poc
            ON  poc.id = ae.transmittal_poc_id
            AND COALESCE(poc.is_deleted, FALSE) = FALSE

        -- Last transmission date (pre-aggregated by AAC)
        LEFT JOIN v_last_tx_per_aac ltx
            ON  ltx.aac = ae.activity_address_cd

        -- Suspension flag (pre-aggregated by transmittal_type_cd)
        LEFT JOIN v_suspension_per_type sp
            ON  sp.transmittal_type_cd = ae.transmittal_type_cd
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    coverage = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total_rows,
            SUM(CASE WHEN poc_name IS NOT NULL          THEN 1 ELSE 0 END)       AS has_poc,
            SUM(CASE WHEN last_transmission_dt IS NOT NULL THEN 1 ELSE 0 END)   AS has_last_tx,
            SUM(CASE WHEN is_suspended_flag = TRUE      THEN 1 ELSE 0 END)       AS suspended_count
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Coverage — "
        f"total={coverage[0]:,} | poc={coverage[1]:,} | "
        f"has_last_tx={coverage[2]:,} | suspended={coverage[3]:,}"
    )

    if rows_written != rows_read:
        print(
            f"[{NOTEBOOK}] WARNING: rows_written ({rows_written:,}) ≠ "
            f"distinct AACs ({rows_read:,}). "
            f"Check v_aac_envelope_dedup for remaining duplicates."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

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
