# Databricks notebook source
# =============================================================================
# common/prime_dim_line_item.py
# Primes assist_dev.common.dim_line_item
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per latest line_item_accepted.id per line_item_id
# SCD Type : 2  (eff_start_dt, eff_end_dt, is_current_flag)
#
# Source tables (Silver):
#   silver_aasbs_line_item              → base CLIN record
#   silver_aasbs_line_item_accepted     → accepted instance (grain anchor)
#   silver_aasbs_li_deliverable         → psc_cd, contract_type_cd,
#                                         severability_type_cd, unit_of_measure_cd
#   silver_aasbs_lu_line_item_type      → line_item_type_desc decode
#   silver_aasbs_award_mod              → award_sk resolution bridge
#   assist_dev.common.dim_award     → award_sk resolution
#
# IMPROVEMENT #5 (v1.1.0):
#   Four columns now populated on prime from silver_aasbs_li_deliverable:
#     psc_cd              — Product/Service Code (FAR 4.606(a)(20))
#     contract_type_cd    — CLIN-level contract type (FFP, T&M, CPFF, etc.)
#     severability_type_cd — Severable / Nonseverable / Mixed
#     unit_of_measure_cd  — Unit of measure
#
#   Join path: silver_aasbs_line_item.li_deliverable_id = silver_aasbs_li_deliverable.id
#   li_deliverable is a subtype table — only DELIVERABLE-type CLINs have a
#   matching li_deliverable row.  Service charge and other CLIN types will
#   still have NULL for psc_cd etc., which is correct (SC CLINs don't have PSC).
#
#   Columns previously always NULL on prime; now populated for all DELIVERABLE CLINs.
#   Removes delta refresh dependency for the most commonly queried CLIN attributes.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_line_item")
TASK     = "prime_dim_line_item"

# Silver sources
S_LI     = silver("aasbs", "line_item")
S_LIA    = silver("aasbs", "line_item_accepted")
S_LID    = silver("aasbs", "li_deliverable")   # IMPROVEMENT #5
S_LIT    = silver("aasbs", "lu_line_item_type")
S_MOD    = silver("aasbs", "award_mod")

# Gold dim (must exist — primed by prime_dim_award earlier in DP1 job)
G_AWARD  = gold("common", "dim_award")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="aasbs", source_table="line_item",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            line_item_id,
            line_item_accepted_id,
            li_tracking_num,
            award_sk,
            clin_num,
            slin_num,
            line_item_type_cd,
            line_item_type_desc,
            psc_cd,
            unit_of_measure_cd,
            contract_type_cd,
            severability_type_cd,
            li_pop_start_dt,
            li_pop_end_dt,
            obligated_ceiling_amt,
            exercise_this_yn,
            bona_fide_need_fy,
            budget_fy,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 1: Latest accepted record per line_item_id
        --   Picks MAX(id) as "latest" accepted instance for each CLIN.
        -- ─────────────────────────────────────────────────────────────────
        WITH latest_accepted AS (
            SELECT
                line_item_id,
                MAX(id) AS line_item_accepted_id
            FROM {S_LIA}
            WHERE COALESCE(is_deleted, FALSE) = FALSE
            GROUP BY line_item_id
        ),

        accepted_detail AS (
            SELECT
                la.line_item_id,
                la.line_item_accepted_id,
                lia.budget_fy,
                lia.exercise_this_yn,
                lia.award_mod_id         AS accepted_award_mod_id
            FROM latest_accepted la
            JOIN {S_LIA} lia
                ON  lia.id = la.line_item_accepted_id
                AND COALESCE(lia.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 2: Base CLIN record with CLIN/SLIN number derivation
        -- ─────────────────────────────────────────────────────────────────
        li_core AS (
            SELECT
                li.id                               AS line_item_id,
                li.li_tracking_num,
                li.line_item_type_cd,
                -- CLIN number: full line_item_num (e.g. '0001', '0001AA')
                li.line_item_num                    AS clin_num,
                -- SLIN number: suffix after position 4 when num > 4 chars
                -- '0001AA' → clin='0001', slin='AA'
                CASE
                    WHEN LENGTH(TRIM(li.line_item_num)) > 4
                    THEN SUBSTRING(TRIM(li.line_item_num), 5)
                    ELSE NULL
                END                                 AS slin_num,
                li.line_item_start_dt               AS li_pop_start_dt,
                li.line_item_end_dt                 AS li_pop_end_dt,
                li.line_item_total_amt              AS obligated_ceiling_amt,
                li.line_item_fy                     AS li_fy,
                li.parent_award_mod_id,
                -- IMPROVEMENT #5: li_deliverable_id FK for subtype join
                li.li_deliverable_id
            FROM {S_LI} li
            WHERE COALESCE(li.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 3: Merge accepted detail into CLIN core
        -- ─────────────────────────────────────────────────────────────────
        with_accepted AS (
            SELECT
                lc.*,
                COALESCE(ad.line_item_accepted_id, NULL)    AS line_item_accepted_id,
                COALESCE(ad.budget_fy, lc.li_fy)           AS budget_fy,
                -- exercise_this_yn: 'Y'/'N' character → BOOLEAN
                CASE
                    WHEN ad.exercise_this_yn = 'Y' THEN TRUE
                    WHEN ad.exercise_this_yn = 'N' THEN FALSE
                    ELSE NULL
                END                                         AS exercise_this_yn
            FROM li_core lc
            LEFT JOIN accepted_detail ad
                ON  ad.line_item_id = lc.line_item_id
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 4: Resolve award_sk via parent_award_mod_id chain
        -- ─────────────────────────────────────────────────────────────────
        with_award_sk AS (
            SELECT
                wa.*,
                COALESCE(aw.award_sk, -1)   AS award_sk
            FROM with_accepted wa
            LEFT JOIN {S_MOD} m
                ON  m.id = wa.parent_award_mod_id
                AND COALESCE(m.is_deleted, FALSE) = FALSE
            LEFT JOIN {G_AWARD} aw
                ON  aw.award_id        = m.award_id
                AND aw.is_current_flag = TRUE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 5: Decode line_item_type description
        -- ─────────────────────────────────────────────────────────────────
        with_decoded AS (
            SELECT
                ws.*,
                COALESCE(lit.description, ws.line_item_type_cd) AS line_item_type_desc
            FROM with_award_sk ws
            LEFT JOIN {S_LIT} lit
                ON  lit.cd = ws.line_item_type_cd
                AND COALESCE(lit.is_deleted, FALSE) = FALSE
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 6: IMPROVEMENT #5 — join li_deliverable for CLIN attributes
        --
        -- li_deliverable is the subtype table for DELIVERABLE CLINs.
        -- Join key: line_item.li_deliverable_id = li_deliverable.id
        -- (li_deliverable.id has its own sequence, not = line_item.id)
        --
        -- Only DELIVERABLE-type CLINs have a matching li_deliverable row.
        -- For SC, Travel, ODC, Award Fee CLINs: psc_cd etc. remain NULL.
        -- This is correct behaviour — only DELIVERABLE CLINs carry PSC codes.
        --
        -- Provides: psc_cd, contract_type_cd, severability_type_cd,
        --           unit_of_measure_cd, bona_fide_need_fy
        -- ─────────────────────────────────────────────────────────────────
        with_deliverable AS (
            SELECT
                wd.*,
                ld.psc_cd,
                ld.contract_type_cd,
                ld.severability_type_cd,
                ld.unit_of_measure_cd,
                -- bona_fide_need_fy: on li_deliverable (not on line_item itself)
                ld.bona_fide_need_fy
            FROM with_decoded wd
            LEFT JOIN {S_LID} ld
                ON  ld.id = wd.li_deliverable_id
                AND COALESCE(ld.is_deleted, FALSE) = FALSE
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 7: Final projection
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            line_item_id,
            line_item_accepted_id,
            li_tracking_num,
            award_sk,
            clin_num,
            slin_num,
            line_item_type_cd,
            line_item_type_desc,
            -- IMPROVEMENT #5: all four now populated from li_deliverable
            -- (NULL for non-DELIVERABLE CLINs — correct by design)
            psc_cd,
            unit_of_measure_cd,
            contract_type_cd,
            severability_type_cd,
            li_pop_start_dt,
            li_pop_end_dt,
            obligated_ceiling_amt,
            exercise_this_yn,
            bona_fide_need_fy,
            budget_fy,
            CURRENT_TIMESTAMP()     AS eff_start_dt,
            CAST(NULL AS TIMESTAMP) AS eff_end_dt,
            TRUE                    AS is_current_flag,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM with_deliverable
    """)

    # ── Post-load quality checks ───────────────────────────────────────────
    n = row_count(spark, TARGET)

    quality = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total_rows,
            SUM(CASE WHEN award_sk = -1                   THEN 1 ELSE 0 END)    AS sentinel_award_sk,
            SUM(CASE WHEN clin_num IS NULL                THEN 1 ELSE 0 END)    AS null_clin_num,
            SUM(CASE WHEN psc_cd IS NOT NULL              THEN 1 ELSE 0 END)    AS populated_psc_cd,
            SUM(CASE WHEN contract_type_cd IS NOT NULL    THEN 1 ELSE 0 END)    AS populated_contract_type,
            SUM(CASE WHEN severability_type_cd IS NOT NULL THEN 1 ELSE 0 END)   AS populated_severability,
            SUM(CASE WHEN unit_of_measure_cd IS NOT NULL  THEN 1 ELSE 0 END)    AS populated_uom,
            SUM(CASE WHEN line_item_type_cd = 'DELIVERABLE'
                     AND psc_cd IS NULL                   THEN 1 ELSE 0 END)    AS deliverable_missing_psc
        FROM {TARGET}
    """).collect()[0]

    print(f"  [OK] Inserted {n:,} line item rows")
    print(f"  sentinel award_sk=-1 : {quality['sentinel_award_sk']:,}")
    print(f"  null clin_num        : {quality['null_clin_num']:,}")
    print(f"  psc_cd populated     : {quality['populated_psc_cd']:,}  "
          f"(IMPROVEMENT #5 — was 0 in v1.0)")
    print(f"  contract_type popl.  : {quality['populated_contract_type']:,}  "
          f"(IMPROVEMENT #5 — was 0 in v1.0)")
    print(f"  severability popl.   : {quality['populated_severability']:,}  "
          f"(IMPROVEMENT #5 — was 0 in v1.0)")
    print(f"  uom populated        : {quality['populated_uom']:,}  "
          f"(IMPROVEMENT #5 — was 0 in v1.0)")
    print(f"  DELIVERABLE w/o PSC  : {quality['deliverable_missing_psc']:,}  "
          f"(DELIVERABLE CLINs without a li_deliverable row — investigate if > 0)")

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
