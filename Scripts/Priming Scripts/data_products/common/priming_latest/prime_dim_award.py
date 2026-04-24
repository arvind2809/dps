# Databricks notebook source
# =============================================================================
# common/prime_dim_award.py
# Primes assist_dev.common.dim_award
#
# Strategy : TRUNCATE → INSERT (fully idempotent)
# Grain    : One row per award.id joined to its latest_award_mod_id
# SCD Type : 2  (eff_start_dt, eff_end_dt, is_current_flag)
#
# Source tables (Silver):
#   silver_aasbs_award                → base award record
#   silver_aasbs_award_mod            → latest mod details, mod count
#   silver_aasbs_acquisition          → ia_id for ia_sk resolution (IMPROVEMENT #3)
#   silver_aasbs_lu_contract_type     → contract_type_desc decode
#   silver_aasbs_lu_vehicle_type      → vehicle_type_desc decode
#   assist_dev.common.dim_ia      → ia_sk resolution
#   assist_dev.common.dim_agency  → agency_sk resolution
#
# IMPROVEMENT #3 (v1.1.0):
#   ia_sk is now resolved on prime via the path:
#     award.acquisition_id → acquisition.ia_id → dim_ia.ia_id → ia_sk
#   Previously NULL on prime (deferred to delta refresh via loa_ledger multi-hop).
#   silver_aasbs_acquisition is available in Silver and carries ia_id directly.
#   Resolving ia_sk on prime eliminates a significant source of sentinel -1 FK
#   values on fact_obligation_expenditure rows that join through dim_award.
#
# NULL on prime (by design — enriched by delta refresh):
#   award_end_dt     — requires latest amendment pop_end_dt (delta refresh)
#   vehicle_type_cd  — not on award row; from IDV relationship (delta refresh)
#   vehicle_type_desc — same
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET   = gold("common", "dim_award")
TASK     = "prime_dim_award"

# Silver sources
S_AWARD  = silver("aasbs", "award")
S_MOD    = silver("aasbs", "award_mod")
S_ACQ    = silver("aasbs", "acquisition")     # IMPROVEMENT #3: ia_id resolution
S_CT     = silver("aasbs", "lu_contract_type")
S_VT     = silver("aasbs", "lu_vehicle_type")

# Gold dims (must exist — primed earlier in DP1 job)
G_IA     = gold("common", "dim_ia")
G_AGENCY = gold("common", "dim_agency")

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="aasbs", source_table="award",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    # ─────────────────────────────────────────────────────────────────────
    # Step 1: Pre-aggregate mod counts per award to avoid fan-out
    #   Counts total mods and identifies the base award date from mod_num='0'
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_mod_counts AS
        SELECT
            award_id,
            COUNT(*)                                                    AS total_mods_count,
            MIN(CASE
                WHEN mod_num IN ('0', '00', 'A', 'A0')
                THEN m1p_mod_start_dt
                END)                                                    AS base_award_dt
        FROM {S_MOD}
        WHERE COALESCE(is_deleted, FALSE) = FALSE
        GROUP BY award_id
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 2: IMPROVEMENT #3 — resolve ia_sk at prime time
    #   award.acquisition_id → acquisition.ia_id → dim_ia.ia_id → ia_sk
    #   Previously this was always NULL on prime. delta refresh resolved
    #   via the longer loa_ledger.ia_id path. This shorter path works fine
    #   because acquisition carries ia_id as a direct FK.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_per_award AS
        SELECT
            aw.id       AS award_id,
            dia.ia_sk
        FROM {S_AWARD} aw
        -- Join acquisition to get ia_id (IMPROVEMENT #3)
        JOIN {S_ACQ} acq
            ON  acq.id = aw.acquisition_id
            AND COALESCE(acq.is_deleted, FALSE) = FALSE
        -- Resolve ia_sk from dim_ia
        LEFT JOIN {G_IA} dia
            -- ON  dia.ia_id           = acq.ia_id
            ON dia.is_current_flag = TRUE
        WHERE COALESCE(aw.is_deleted, FALSE) = FALSE
    """)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            award_id,
            award_piid,
            award_mod_num,
            award_mod_id,
            fin_code,
            award_status_cd,
            award_status_desc,
            contract_type_cd,
            contract_type_desc,
            vehicle_type_cd,
            vehicle_type_desc,
            ia_sk,
            agency_sk,
            award_start_dt,
            award_end_dt,
            base_award_dt,
            last_mod_dt,
            total_mods_count,
            eff_start_dt,
            eff_end_dt,
            is_current_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Join award to its latest mod, aggregated mod counts, ia resolution
        -- ─────────────────────────────────────────────────────────────────
        WITH award_with_mod AS (
            SELECT
                a.id                                AS award_id,
                a.award_piid,
                a.award_status_cd,
                a.award_fin                         AS fin_code,
                a.lead_svc_activity_address_cd      AS aac,
                m.id                                AS award_mod_id,
                m.mod_num                           AS award_mod_num,
                m.m1p_mod_start_dt                  AS award_start_dt,
                -- award_end_dt: not directly on mod; from amendment pop_end_dt
                -- in delta refresh.  NULL on prime by design.
                CAST(NULL AS TIMESTAMP)             AS award_end_dt,
                m.co_signature_dt                   AS last_mod_dt,
                -- contract_type_cd: stored at line_item level in source.
                -- CAST NULL here; enriched via dim_line_item join in delta refresh.
                CAST(NULL AS STRING)                AS contract_type_cd,
                mc.total_mods_count,
                mc.base_award_dt
            FROM {S_AWARD} a
            LEFT JOIN {S_MOD} m
                ON  m.id = a.latest_award_mod_id
                AND COALESCE(m.is_deleted, FALSE) = FALSE
            LEFT JOIN v_mod_counts mc
                ON  mc.award_id = a.id
            WHERE COALESCE(a.is_deleted, FALSE) = FALSE
        ),

        with_decoded AS (
            SELECT
                aw.*,
                COALESCE(ct.description, aw.contract_type_cd) AS contract_type_desc,
                -- vehicle_type_cd: from IDV relationship — not on source award row.
                -- NULL on prime; enriched in delta refresh.
                CAST(NULL AS STRING)                AS vehicle_type_cd,
                CAST(NULL AS STRING)                AS vehicle_type_desc
            FROM award_with_mod aw
            LEFT JOIN {S_CT} ct
                ON  ct.cd = aw.contract_type_cd
                AND COALESCE(ct.is_deleted, FALSE) = FALSE
        ),

        with_fks AS (
            SELECT
                wd.*,
                -- IMPROVEMENT #3: ia_sk resolved on prime via acquisition.ia_id
                -- Previously CAST(NULL AS BIGINT) — deferred to delta refresh.
                -- Now populated for all awards that have a linked acquisition.
                COALESCE(iap.ia_sk, -1)             AS ia_sk,
                -- agency_sk: resolved via lead_svc_activity_address_cd (AAC)
                COALESCE(ag.agency_sk, -1)          AS agency_sk
            FROM with_decoded wd
            LEFT JOIN v_ia_per_award iap
                ON  iap.award_id = wd.award_id
            LEFT JOIN {G_AGENCY} ag
                ON  ag.activity_address_cd = wd.aac
                AND ag.is_current_flag     = TRUE
        )

        SELECT
            award_id,
            award_piid,
            award_mod_num,
            award_mod_id,
            fin_code,
            award_status_cd,
            COALESCE(award_status_cd, 'UNKNOWN')    AS award_status_desc,
            contract_type_cd,
            contract_type_desc,
            vehicle_type_cd,        -- NULL on prime — see header
            vehicle_type_desc,      -- NULL on prime — see header
            ia_sk,                  -- IMPROVEMENT #3: resolved on prime
            agency_sk,
            award_start_dt,
            award_end_dt,           -- NULL on prime — see header
            base_award_dt,
            last_mod_dt,
            total_mods_count,
            CURRENT_TIMESTAMP()     AS eff_start_dt,
            CAST(NULL AS TIMESTAMP) AS eff_end_dt,
            TRUE                    AS is_current_flag,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM with_fks
    """)

    # ── Post-load quality checks ───────────────────────────────────────────
    n = row_count(spark, TARGET)

    quality = spark.sql(f"""
        SELECT
            COUNT(*)                                                          AS total_rows,
            SUM(CASE WHEN award_piid IS NULL              THEN 1 ELSE 0 END) AS null_piid,
            SUM(CASE WHEN ia_sk    = -1                   THEN 1 ELSE 0 END) AS sentinel_ia_sk,
            SUM(CASE WHEN agency_sk = -1                  THEN 1 ELSE 0 END) AS sentinel_agency_sk,
            SUM(CASE WHEN total_mods_count IS NULL
                     OR total_mods_count = 0              THEN 1 ELSE 0 END) AS zero_mod_count
        FROM {TARGET}
    """).collect()[0]

    print(f"  [OK] Inserted {n:,} award rows")
    print(f"  null piid            : {quality['null_piid']:,}")
    print(f"  sentinel ia_sk = -1  : {quality['sentinel_ia_sk']:,}  "
          f"(IMPROVEMENT #3 — should be lower than v1.0 which was all rows)")
    print(f"  sentinel agency_sk=-1: {quality['sentinel_agency_sk']:,}")
    print(f"  zero mod count rows  : {quality['zero_mod_count']:,}")

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
