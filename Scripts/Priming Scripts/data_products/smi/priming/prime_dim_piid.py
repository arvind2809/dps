# Databricks notebook source
# =============================================================================
# smi/prime_dim_piid.py
# Primes assist_dev.smi.dim_piid
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per award.id  (one resulting award PIID per winner)
# SCD Type : 1 — PIID attributes updated in-place
#
# Source tables (Silver — all confirmed against Silver DDL):
#   silver_aasbs_award      → award.award_piid (piid_value), award.id (NK)
#   silver_aasbs_award_mod  → award_mod.award_piid_ext (piid_ext, latest mod)
#
# Field mapping:
#   piid_id          ← award.id                                  (NK)
#   piid_value       ← award.award_piid                          (full PIID string)
#   base_piid        ← award.award_piid (base = full at award level; no mod suffix)
#   piid_ext         ← award_mod.award_piid_ext via award.latest_award_mod_id
#   aac              ← SUBSTRING(award_piid, 1, 6)               (positional parse)
#   instrument_type_cd ← SUBSTRING(award_piid, 9, 1)             (positional parse)
#   sequence_num     ← SUBSTRING(award_piid, 10, 4)              (positional parse)
#   piid_fiscal_year ← CAST(SUBSTRING(award_piid, 7, 2) AS INT)  (positional parse)
#                      + 2000 offset for 2-digit year
#
# IMPROVEMENT I-DP7-2 — PIID component parsing (built-in):
#   Per FAR 4.1602, a conforming PIID is structured as:
#     Positions 1–6  : Agency/Activity Address Code (AAC)
#     Positions 7–8  : Fiscal year (2-digit, e.g. '25' = FY2025)
#     Position  9    : Instrument type code (letter: C=contract, D=delivery order,
#                      A=BPA call, B=purchase order, etc.)
#     Positions 10–13: Sequence number (4 digits)
#   Total conforming length = 13 characters minimum.
#   Non-conforming PIIDs (LENGTH < 13, NULL, or containing invalid chars)
#   produce NULL for all parsed components rather than incorrect partial values.
#   Post-load check reports: total awards, conforming PIID count, non-conforming
#   count, and piid_ext coverage.
#   Note: PIID parsing is best-effort. Complex IDV formats, legacy PIIDs, or
#   migration records may not conform to FAR 4.1602 positional structure.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP7"
NOTEBOOK     = "prime_dim_piid"
TARGET_TABLE = "assist_dev.smi.dim_piid"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_award
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND award_piid IS NOT NULL
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source award rows with a PIID: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT
    # IMPROVEMENT I-DP7-2: FAR 4.1602 positional parse with conformance guard
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            piid_id,
            piid_value,
            base_piid,
            piid_ext,
            aac,
            instrument_type_cd,
            sequence_num,
            piid_fiscal_year,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: award.id (one PIID per resulting award)
            aw.id                                               AS piid_id,

            -- Full PIID string as-stored in ASSIST
            aw.award_piid                                       AS piid_value,

            -- base_piid: at the award level the base PIID = full PIID
            -- (modification suffix is tracked separately via piid_ext)
            aw.award_piid                                       AS base_piid,

            -- piid_ext: modification/amendment extension from latest award_mod
            -- award_mod.award_piid_ext confirmed in Silver DDL
            am.award_piid_ext                                   AS piid_ext,

            -- ─────────────────────────────────────────────────────────────
            -- IMPROVEMENT I-DP7-2: FAR 4.1602 positional parse
            -- Applied only when PIID length >= 13 (conforming format).
            -- Non-conforming or NULL PIIDs produce NULL for all components.
            -- ─────────────────────────────────────────────────────────────

            -- aac: positions 1–6 (Activity Address Code)
            CASE
                WHEN LENGTH(TRIM(aw.award_piid)) >= 13
                THEN SUBSTRING(TRIM(aw.award_piid), 1, 6)
                ELSE CAST(NULL AS STRING)
            END                                                 AS aac,

            -- instrument_type_cd: position 9
            -- C=contract, D=delivery order, A=BPA call, B=purchase order
            CASE
                WHEN LENGTH(TRIM(aw.award_piid)) >= 13
                THEN SUBSTRING(TRIM(aw.award_piid), 9, 1)
                ELSE CAST(NULL AS STRING)
            END                                                 AS instrument_type_cd,

            -- sequence_num: positions 10–13
            CASE
                WHEN LENGTH(TRIM(aw.award_piid)) >= 13
                THEN SUBSTRING(TRIM(aw.award_piid), 10, 4)
                ELSE CAST(NULL AS STRING)
            END                                                 AS sequence_num,

            -- piid_fiscal_year: positions 7–8 as 2-digit year → 4-digit FY
            -- 2000 offset applied (positions 7-8 = '25' → FY2025)
            -- NULL if PIID non-conforming or year digits not numeric
            CASE
                WHEN LENGTH(TRIM(aw.award_piid)) >= 13
                 AND SUBSTRING(TRIM(aw.award_piid), 7, 2) RLIKE '^[0-9]{2}$'
                THEN CAST(SUBSTRING(TRIM(aw.award_piid), 7, 2) AS INT) + 2000
                ELSE CAST(NULL AS INT)
            END                                                 AS piid_fiscal_year,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_award aw
        -- Latest award mod for piid_ext
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = aw.latest_award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
        WHERE COALESCE(aw.is_deleted, FALSE) = FALSE
          AND aw.award_piid IS NOT NULL
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP7-2: PIID conformance and parse coverage report
    parse_stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total,
            SUM(CASE WHEN aac IS NOT NULL                  THEN 1 ELSE 0 END)   AS conforming_piid,
            SUM(CASE WHEN aac IS NULL                      THEN 1 ELSE 0 END)   AS non_conforming,
            SUM(CASE WHEN piid_ext IS NOT NULL             THEN 1 ELSE 0 END)   AS has_piid_ext,
            COUNT(DISTINCT piid_fiscal_year)                                     AS distinct_fy
        FROM {TARGET_TABLE}
    """).collect()[0]

    conforming_pct = parse_stats[1] / max(parse_stats[0], 1) * 100
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP7-2 — PIID parse conformance (FAR 4.1602):")
    print(f"  Total awards        : {parse_stats[0]:>8,}")
    print(f"  Conforming (≥13ch)  : {parse_stats[1]:>8,}  ({conforming_pct:.1f}%)")
    print(f"  Non-conforming      : {parse_stats[2]:>8,}  (parsed components NULL)")
    print(f"  Has piid_ext        : {parse_stats[3]:>8,}")
    print(f"  Distinct FY in PIIDs: {parse_stats[4]:>8,}")

    if parse_stats[2] > 0:
        sample = spark.sql(f"""
            SELECT DISTINCT piid_value FROM {TARGET_TABLE}
            WHERE aac IS NULL LIMIT 5
        """).collect()
        samples = [r[0] for r in sample]
        print(
            f"\n  ⚠ {parse_stats[2]:,} non-conforming PIIDs — "
            f"sample values: {samples}. "
            f"These may be legacy/migrated records or IDV formats."
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
