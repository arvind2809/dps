# Databricks notebook source
# =============================================================================
# psc/prime_dim_clin_subtype.py
# Primes assist_dev.psc.dim_clin_subtype
#
# Strategy : TRUNCATE → INSERT  (fully idempotent)
# Grain    : One row per line_item.id  (natural key)
# SCD Type : 1 — CLIN subtype attributes updated in-place
#
# Source tables (Silver — all confirmed against Silver DDL):
#   silver_aasbs_line_item          → base CLIN record (subtype FK carrier)
#   silver_aasbs_li_deliverable     → DELIVERABLE subtype details
#   silver_aasbs_li_sc_labor        → SC_LABOR subtype details
#   silver_aasbs_li_sc_fixed_fee    → SC_FIXED_FEE subtype details
#   silver_aasbs_li_sc_travel       → SC_TRAVEL subtype details
#   silver_aasbs_li_award_fee       → AWARD_FEE subtype details
#   silver_aasbs_service_charge     → is_credit_flag, act_num (SC CLINs only)
#   silver_aasbs_lu_proc_phase      → proc_phase_desc decode
#
# IMPROVEMENT I-DP6-1 (built-in):
#   The five CLIN subtypes are stored in separate Silver subtype tables linked
#   to line_item via mutually exclusive FK columns:
#     line_item.li_deliverable_id   = li_deliverable.id
#     line_item.li_sc_labor_id      = li_sc_labor.id
#     line_item.li_sc_fixed_fee_id  = li_sc_fixed_fee.id
#     line_item.li_sc_travel_id     = li_sc_travel.id
#     line_item.li_award_fee_id     = li_award_fee.id
#   Each line_item has at most one non-null FK (subtypes are mutually exclusive).
#   The INSERT uses a UNION ALL of five branches — one per subtype — each joining
#   line_item to its corresponding subtype table.  clin_subtype_cd is set as a
#   literal string per branch.
#   Post-load distribution by clin_subtype_cd validates all five branches loaded.
#
# Field availability by subtype:
#   proc_phase_cd    : SC_LABOR, SC_FIXED_FEE, SC_TRAVEL only (NULL for others)
#   project_num      : SC_LABOR, SC_FIXED_FEE, SC_TRAVEL only (NULL for others)
#   is_credit_flag   : SC_LABOR, SC_FIXED_FEE, SC_TRAVEL (where service_charge_id exists)
#   act_num          : same as is_credit_flag
#   severability_type_cd : all except AWARD_FEE (not on li_award_fee in Silver)
#   psc_cd           : DELIVERABLE only  (FAR 4.606(a)(20))
#   contract_type_cd : DELIVERABLE only  (FAR Part 16)
#
# Ref: FAR 15.404 (price reasonableness), FAR 52.232-7 (T&M payments)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP6"
NOTEBOOK     = "prime_dim_clin_subtype"
TARGET_TABLE = "assist_dev.psc.dim_clin_subtype"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_line_item")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_line_item
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND (
              li_deliverable_id IS NOT NULL OR li_sc_labor_id  IS NOT NULL OR
              li_sc_fixed_fee_id IS NOT NULL OR li_sc_travel_id IS NOT NULL OR
              li_award_fee_id   IS NOT NULL
          )
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source line_item rows with a subtype FK: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT via UNION ALL across the five subtype branches
    #
    # IMPROVEMENT I-DP6-1: each branch:
    #   (a) filters line_item to rows where the relevant subtype FK is non-null
    #   (b) JOINs to the subtype table on the FK
    #   (c) sets clin_subtype_cd as a literal
    #   (d) sets subtype-specific columns; NULLs for non-applicable fields
    #
    # service_charge join (for is_credit_flag + act_num):
    #   line_item.service_charge_id = service_charge.id
    #   Only SC-type CLINs have a non-null service_charge_id.
    #   DELIVERABLE and AWARD_FEE CLINs will have NULL for these fields.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            line_item_id,
            clin_subtype_cd,
            proc_phase_cd,
            proc_phase_desc,
            project_num,
            is_credit_flag,
            act_num,
            severability_type_cd,
            psc_cd,
            contract_type_cd,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Branch 1: DELIVERABLE CLINs
        --   line_item.li_deliverable_id = li_deliverable.id
        --   Carries psc_cd, contract_type_cd, severability_type_cd
        --   No proc_phase_cd or project_num (delivery-type CLIN, not SC)
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            li.id                                               AS line_item_id,
            'DELIVERABLE'                                       AS clin_subtype_cd,
            CAST(NULL AS STRING)                                AS proc_phase_cd,
            CAST(NULL AS STRING)                                AS proc_phase_desc,
            CAST(NULL AS STRING)                                AS project_num,
            -- DELIVERABLE CLINs have no service_charge_id by design
            CAST(NULL AS BOOLEAN)                               AS is_credit_flag,
            CAST(NULL AS STRING)                                AS act_num,
            ld.severability_type_cd,
            ld.psc_cd,                    -- FAR 4.606(a)(20) Product/Service Code
            ld.contract_type_cd,          -- FAR Part 16 contract type
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_line_item li
        JOIN {SILVER}.silver_aasbs_li_deliverable ld
            ON  ld.id = li.li_deliverable_id
            AND COALESCE(ld.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND li.li_deliverable_id IS NOT NULL

        UNION ALL

        -- ─────────────────────────────────────────────────────────────────
        -- Branch 2: SC_LABOR CLINs (Service Charge — Labor)
        --   line_item.li_sc_labor_id = li_sc_labor.id
        --   Carries proc_phase_cd, project_num, severability_type_cd
        --   is_credit_flag / act_num from service_charge header
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            li.id                                               AS line_item_id,
            'SC_LABOR'                                          AS clin_subtype_cd,
            sl.proc_phase_cd,
            COALESCE(pp.description, sl.proc_phase_cd)         AS proc_phase_desc,
            sl.project_num,
            CASE
                WHEN UPPER(COALESCE(sc.is_credit_yn, 'N')) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_credit_flag,
            sc.act_number                                       AS act_num,
            sl.severability_type_cd,
            CAST(NULL AS STRING)                                AS psc_cd,
            CAST(NULL AS STRING)                                AS contract_type_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_line_item li
        JOIN {SILVER}.silver_aasbs_li_sc_labor sl
            ON  sl.id = li.li_sc_labor_id
            AND COALESCE(sl.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_service_charge sc
            ON  sc.id = li.service_charge_id
            AND COALESCE(sc.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_lu_proc_phase pp
            ON  pp.cd = sl.proc_phase_cd
            AND COALESCE(pp.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND li.li_sc_labor_id IS NOT NULL

        UNION ALL

        -- ─────────────────────────────────────────────────────────────────
        -- Branch 3: SC_FIXED_FEE CLINs (Service Charge — Fixed Fee / CPFF)
        --   line_item.li_sc_fixed_fee_id = li_sc_fixed_fee.id
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            li.id                                               AS line_item_id,
            'SC_FIXED_FEE'                                      AS clin_subtype_cd,
            sff.proc_phase_cd,
            COALESCE(pp.description, sff.proc_phase_cd)        AS proc_phase_desc,
            sff.project_num,
            CASE
                WHEN UPPER(COALESCE(sc.is_credit_yn, 'N')) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_credit_flag,
            sc.act_number                                       AS act_num,
            sff.severability_type_cd,
            CAST(NULL AS STRING)                                AS psc_cd,
            CAST(NULL AS STRING)                                AS contract_type_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_line_item li
        JOIN {SILVER}.silver_aasbs_li_sc_fixed_fee sff
            ON  sff.id = li.li_sc_fixed_fee_id
            AND COALESCE(sff.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_service_charge sc
            ON  sc.id = li.service_charge_id
            AND COALESCE(sc.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_lu_proc_phase pp
            ON  pp.cd = sff.proc_phase_cd
            AND COALESCE(pp.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND li.li_sc_fixed_fee_id IS NOT NULL

        UNION ALL

        -- ─────────────────────────────────────────────────────────────────
        -- Branch 4: SC_TRAVEL CLINs (Service Charge — Travel)
        --   line_item.li_sc_travel_id = li_sc_travel.id
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            li.id                                               AS line_item_id,
            'SC_TRAVEL'                                         AS clin_subtype_cd,
            st.proc_phase_cd,
            COALESCE(pp.description, st.proc_phase_cd)         AS proc_phase_desc,
            st.project_num,
            CASE
                WHEN UPPER(COALESCE(sc.is_credit_yn, 'N')) = 'Y' THEN TRUE
                ELSE FALSE
            END                                                 AS is_credit_flag,
            sc.act_number                                       AS act_num,
            st.severability_type_cd,
            CAST(NULL AS STRING)                                AS psc_cd,
            CAST(NULL AS STRING)                                AS contract_type_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_line_item li
        JOIN {SILVER}.silver_aasbs_li_sc_travel st
            ON  st.id = li.li_sc_travel_id
            AND COALESCE(st.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_service_charge sc
            ON  sc.id = li.service_charge_id
            AND COALESCE(sc.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_lu_proc_phase pp
            ON  pp.cd = st.proc_phase_cd
            AND COALESCE(pp.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND li.li_sc_travel_id IS NOT NULL

        UNION ALL

        -- ─────────────────────────────────────────────────────────────────
        -- Branch 5: AWARD_FEE CLINs
        --   line_item.li_award_fee_id = li_award_fee.id
        --   li_award_fee has no proc_phase_cd, project_num, or severability_type_cd
        --   in the Silver DDL — these are NULL for AWARD_FEE by design.
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            li.id                                               AS line_item_id,
            'AWARD_FEE'                                         AS clin_subtype_cd,
            CAST(NULL AS STRING)                                AS proc_phase_cd,
            CAST(NULL AS STRING)                                AS proc_phase_desc,
            CAST(NULL AS STRING)                                AS project_num,
            CAST(NULL AS BOOLEAN)                               AS is_credit_flag,
            CAST(NULL AS STRING)                                AS act_num,
            -- severability_type_cd not on li_award_fee in Silver DDL
            CAST(NULL AS STRING)                                AS severability_type_cd,
            CAST(NULL AS STRING)                                AS psc_cd,
            CAST(NULL AS STRING)                                AS contract_type_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM {SILVER}.silver_aasbs_line_item li
        JOIN {SILVER}.silver_aasbs_li_award_fee af
            ON  af.id = li.li_award_fee_id
            AND COALESCE(af.is_deleted, FALSE) = FALSE
        WHERE COALESCE(li.is_deleted, FALSE) = FALSE
          AND li.li_award_fee_id IS NOT NULL
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load checks
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # IMPROVEMENT I-DP6-1: distribution by subtype — all five branches must appear
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP6-1 — clin_subtype_cd distribution:")
    dist = spark.sql(f"""
        SELECT
            clin_subtype_cd,
            COUNT(*)                                             AS cnt,
            SUM(CASE WHEN psc_cd IS NOT NULL       THEN 1 ELSE 0 END) AS has_psc,
            SUM(CASE WHEN proc_phase_cd IS NOT NULL THEN 1 ELSE 0 END) AS has_proc_phase
        FROM {TARGET_TABLE}
        GROUP BY clin_subtype_cd
        ORDER BY cnt DESC
    """).collect()
    expected_subtypes = {"DELIVERABLE","SC_LABOR","SC_FIXED_FEE","SC_TRAVEL","AWARD_FEE"}
    loaded_subtypes   = set()
    for row in dist:
        loaded_subtypes.add(row[0])
        print(f"  {str(row[0]):<15}  count={row[1]:>8,}  has_psc={row[2]:>6,}  has_proc_phase={row[3]:>6,}")

    missing = expected_subtypes - loaded_subtypes
    if missing:
        print(f"\n  ⚠ WARNING: subtypes not loaded — {missing}. "
              f"Check if source Silver tables have data for these subtype FKs.")
    else:
        print(f"\n  ✓ All five CLIN subtypes loaded.")

    # Uniqueness check on natural key
    dupes = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT line_item_id FROM {TARGET_TABLE}
            GROUP BY line_item_id HAVING COUNT(*) > 1
        )
    """).collect()[0][0]
    if dupes > 0:
        print(f"  ⚠ WARNING: {dupes} duplicate line_item_id values — "
              f"investigate whether any line_item has multiple subtype FKs set.")
    else:
        print(f"  ✓ Natural key uniqueness: OK")

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
