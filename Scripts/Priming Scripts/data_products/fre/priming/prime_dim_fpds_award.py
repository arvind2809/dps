# Databricks notebook source
# =============================================================================
# fre/prime_dim_fpds_award.py
# Primes assist_dev.fre.dim_fpds_award
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per award.id
# SCD Type : 1 — attributes updated in-place
#
# Source tables (Silver):
#   silver_aasbs_award               → PIID, bundled/consolidated flags,
#                                       performance-based indicator
#   silver_aasbs_award_mod           → fpds_car_status_cd (latest mod only)
#   silver_aasbs_acquisition         → bridge to acquisition_plan
#   silver_aasbs_acquisition_plan    → contract_type_cd, IDV classification fields,
#                                       who_can_use_cd
#   silver_aasbs_transmit_po_summary → transmittal.id linkage
#   silver_aasbs_transmit_transmittal → last_submitted_dt (latest sent_dt per PIID)
#
# Field mapping (all confirmed against Silver DDL):
#   award_id               ← award.id                           (NK)
#   piid                   ← award.award_piid
#   idv_piid               ← NULL  [no Silver source — FPDS-NG back-feed required]
#   idv_agency_id          ← NULL  [same limitation]
#   referenced_idv_type_cd ← acquisition_plan.idv_referenced_class_cd
#   contract_type_cd       ← acquisition_plan.contract_type_cd  (FAR Part 16)
#   bundled_contract_cd    ← award.bundled_contract_cd          (FAR 7.107)
#   consolidated_contract_cd ← award.consolidated_contract_cd  (FAR 7.107-3)
#   multi_year_contract_cd ← NULL  [not in Silver scope on prime]
#   performance_type_cd    ← award.perf_based_svc_contract_cd  (FAR 37.6)
#   who_can_use_cd         ← acquisition_plan.idv_who_can_use_cd
#   fpds_car_status_cd     ← award_mod.fpds_car_status_cd       (latest mod)
#   last_submitted_dt      ← MAX(transmittal.sent_dt) per PIID
#
# IMPROVEMENT I-DP3-4 (v1.2.0):
#   Added post-load contract_type_cd distribution check. Surfaces the rate
#   at which awards have NULL contract_type_cd (no acquisition_plan linked),
#   enabling the operator to assess the coverage of the plan-to-award join.
#   Also prints a distinct-value distribution for contract_type_cd so that
#   unexpected or missing FPDS values (FFP, T&M, CPFF, etc.) are visible.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP3"
NOTEBOOK     = "prime_dim_fpds_award"
TARGET_TABLE = "assist_dev.fre.dim_fpds_award"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, NOTEBOOK, PRODUCT, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_award")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(
        f"SELECT COUNT(*) FROM {SILVER}.silver_aasbs_award"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver award row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Latest transmittal per award PIID
    #   Path: transmit_po_summary.contract_number = award.award_piid
    #         transmit_po_summary.transmittal_id   → transmittal.sent_dt
    #   MAX(sent_dt) = most recent CAR submission date per award.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_last_transmittal AS
        SELECT
            ps.contract_number          AS award_piid,
            MAX(t.sent_dt)              AS last_submitted_dt
        FROM {SILVER}.silver_aasbs_transmit_po_summary ps
        JOIN {SILVER}.silver_aasbs_transmit_transmittal t
            ON  t.id = ps.transmittal_id
            AND COALESCE(t.is_deleted, FALSE) = FALSE
        WHERE COALESCE(ps.is_deleted, FALSE) = FALSE
          AND ps.contract_number IS NOT NULL
        GROUP BY ps.contract_number
    """)
    print(f"[{NOTEBOOK}] Latest transmittal temp view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            award_id,
            piid,
            idv_piid,
            idv_agency_id,
            referenced_idv_type_cd,
            contract_type_cd,
            bundled_contract_cd,
            consolidated_contract_cd,
            multi_year_contract_cd,
            performance_type_cd,
            who_can_use_cd,
            fpds_car_status_cd,
            last_submitted_dt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key
            a.id                                                AS award_id,

            -- Base PIID (FAR 4.1602, FPDS data dictionary)
            a.award_piid                                        AS piid,

            -- IDV parent PIID — SILVER-PENDING.
            -- Not stored in aasbs Silver scope; requires FPDS-NG back-feed or
            -- a dedicated IDV reference table.  CDC will populate when available.
            CAST(NULL AS STRING)                                AS idv_piid,

            -- IDV issuing agency — same limitation as idv_piid
            CAST(NULL AS STRING)                                AS idv_agency_id,

            -- Referenced IDV type (FSS, GWAC, BPA, BOA, IDIQ)
            -- Source: acquisition_plan.idv_referenced_class_cd
            -- NULL for awards without an acquisition_plan record
            ap.idv_referenced_class_cd                          AS referenced_idv_type_cd,

            -- Contract type code per FAR Part 16 / FPDS data dictionary
            -- Source: acquisition_plan.contract_type_cd
            -- NULL for awards created outside a formal pre-award plan workflow
            ap.contract_type_cd                                 AS contract_type_cd,

            -- Bundled contract determination per FAR 7.107
            a.bundled_contract_cd                               AS bundled_contract_cd,

            -- Consolidated contract determination per FAR 7.107-3
            a.consolidated_contract_cd                          AS consolidated_contract_cd,

            -- Multi-year contract indicator — SILVER-PENDING.
            -- Not available in Silver scope on prime; derivable from contract
            -- type classification or FAR authority flags via CDC enrichment.
            CAST(NULL AS STRING)                                AS multi_year_contract_cd,

            -- Performance-based acquisition indicator per FAR 37.6
            a.perf_based_svc_contract_cd                        AS performance_type_cd,

            -- Who can use (IDV eligibility): from acquisition_plan
            -- NULL for non-IDV awards
            ap.idv_who_can_use_cd                               AS who_can_use_cd,

            -- FPDS CAR status from latest award_mod
            -- award.latest_award_mod_id maintained by ASSIST for O(1) lookup
            am.fpds_car_status_cd                               AS fpds_car_status_cd,

            -- Last CAR submission date: latest transmittal sent_dt for this PIID
            -- NULL if no PO has been transmitted to FPDS-NG yet
            lt.last_submitted_dt                                AS last_submitted_dt,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_award a

        -- Latest award_mod: O(1) lookup via award.latest_award_mod_id
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  am.id = a.latest_award_mod_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE

        -- Acquisition: bridge to acquisition_plan
        LEFT JOIN {SILVER}.silver_aasbs_acquisition acq
            ON  acq.id = a.acquisition_id
            AND COALESCE(acq.is_deleted, FALSE) = FALSE

        -- Acquisition plan: 1:1 with acquisition; provides contract classification
        -- Not every award has a plan (pre-award workflow required); LEFT JOIN.
        LEFT JOIN {SILVER}.silver_aasbs_acquisition_plan ap
            ON  ap.acquisition_id = acq.id
            AND COALESCE(ap.is_deleted, FALSE) = FALSE

        -- Last transmittal per PIID
        LEFT JOIN v_last_transmittal lt
            ON  lt.award_piid = a.award_piid

        WHERE COALESCE(a.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load summary
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # CAR status distribution
    car_stats = spark.sql(f"""
        SELECT fpds_car_status_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY fpds_car_status_cd
        ORDER BY cnt DESC
    """).collect()
    print(f"[{NOTEBOOK}] CAR status distribution:")
    for row in car_stats:
        print(f"  {str(row[0]):25s}  {row[1]:>8,}")

    # NULL coverage report
    null_report = spark.sql(f"""
        SELECT
            SUM(CASE WHEN contract_type_cd   IS NULL THEN 1 ELSE 0 END) AS null_contract_type,
            SUM(CASE WHEN fpds_car_status_cd IS NULL THEN 1 ELSE 0 END) AS null_car_status,
            SUM(CASE WHEN last_submitted_dt  IS NULL THEN 1 ELSE 0 END) AS null_last_submitted,
            SUM(CASE WHEN idv_piid           IS NULL THEN 1 ELSE 0 END) AS null_idv_piid
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] NULL coverage — "
        f"contract_type={null_report[0]:,} | "
        f"car_status={null_report[1]:,} | "
        f"last_submitted={null_report[2]:,} | "
        f"idv_piid={null_report[3]:,} (Silver-pending)"
    )

    # ── IMPROVEMENT I-DP3-4: contract_type_cd distribution check ─────────
    # Prints distinct contract_type_cd values and their counts.
    # High null_contract_type count indicates awards without acquisition_plan
    # records — common for awards created outside the formal plan workflow.
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP3-4 — contract_type_cd distribution:")
    ct_dist = spark.sql(f"""
        SELECT
            COALESCE(contract_type_cd, '(NULL — no acq plan)') AS contract_type_cd,
            COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY contract_type_cd
        ORDER BY cnt DESC
        LIMIT 15
    """).collect()
    for row in ct_dist:
        print(f"  {row[0]:<30s}  {row[1]:>8,}")

    null_ct_pct = null_report[0] / rows_written * 100 if rows_written > 0 else 0
    if null_ct_pct > 30:
        print(
            f"\n  ⚠ {null_ct_pct:.0f}% of awards have NULL contract_type_cd — "
            f"these awards lack an acquisition_plan record. "
            f"Investigate whether plans are being created in the ASSIST workflow."
        )
    else:
        print(f"\n  ✓ {100 - null_ct_pct:.0f}% of awards have a contract_type_cd resolved.")

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
