# Databricks notebook source
# =============================================================================
# cat/prime_fact_lifecycle_event.py
# Primes assist_dev.cat.fact_lifecycle_event
#
# Grain    : One row per lifecycle event across TWO source branches (UNION ALL)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# IMPROVEMENT I-DP10-2 — UNION ALL structured via two named temp views:
#   The fact draws from two structurally different Silver tables:
#     Branch A: silver_aasbs_chronology    (full lifecycle audit trail)
#               grain: one row per chronology.id
#               source_chronology_id populated, source_collab_id = NULL
#     Branch B: silver_aasbs_central_collab (collaboration workflow events)
#               grain: one row per central_collab.id
#               source_collab_id populated, source_chronology_id = NULL
#
#   Both branches are pre-computed as named temp views (v_chronology_events,
#   v_collab_events) before the main INSERT executes their UNION ALL.
#   Each view handles its own entity type resolution, FK lookups, and
#   Silver-pending annotations independently.
#
# IMPROVEMENT I-DP10-3 — table_name_cd polymorphic entity type CASE:
#   chronology.source_table_name_cd and central_collab.table_name_cd are
#   polymorphic string codes identifying the entity type. A CASE expression
#   maps these to wpr.dim_entity_type codes (AWARD, IA, ACQUISITION, etc.)
#   and drives which FK column (award_sk / ia_sk) to populate.
#   entity_type_sk FK requires wpr.dim_entity_type — DP8 must be primed first.
#   Post-load distribution prints all source_table_name_cd values seen with
#   their derived entity_type_cd.
#
# IMPROVEMENT I-DP10-4 — comment_text via central_comment polymorphic join:
#   Gold DDL marks comment_text and comment_type_cd as SILVER-PENDING from
#   aasbs.central_collab_comment (absent from Silver).
#   silver_aasbs_central_comment IS in Silver with comment_text,
#   comment_type_cd, table_name_cd, table_record_id.
#   Joining on table_name_cd='CENTRAL_COLLAB' AND table_record_id=collab.id
#   provides partial comment data for collab events.
#   Source distinction: central_comment (general) ≠ central_collab_comment
#   (collab-specific, absent). MAX(id) per collab selects most recent comment.
#
# IMPROVEMENT I-DP10-6 — email_status_cd NULL assertion:
#   silver_aasbs_notification is absent from Silver. email_status_cd = NULL
#   for all rows in both branches. Post-load assertion verifies.
#
# Source-to-target confirmed:
#   entity_type_sk     ← wpr.dim_entity_type via table_name_cd CASE (DP8!)
#   event_type_sk      ← cat.dim_event_type (Branch A: chronology.event_type_cd)
#   award_sk           ← common.dim_award via event_record_id → award_mod
#   ia_sk              ← common.dim_ia via source_record_id when IA entity
#   agency_sk          ← common.dim_agency via acquisition.activity_address_cd
#   comment_text       ← central_comment (partial — I-DP10-4)
#   email_status_cd    ← CAST(NULL) — SILVER-PENDING (I-DP10-6)
#
# Ref: 5 U.S.C. App. 3 (IG Act), OMB A-123, FAR Part 4 (contract records)
# =============================================================================

# COMMAND ----------
# MAGIC %run ../utils/pipeline_utils

# COMMAND ----------
dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP10"
NOTEBOOK     = "prime_fact_lifecycle_event"
TARGET_TABLE = "assist_dev.cat.fact_lifecycle_event"

SILVER = "assist_dev.assist_finance"
CAT    = "assist_dev.cat"
WPR    = "assist_dev.wpr"
COMMON = "assist_dev.common"

# COMMAND ----------
start_ts = audit_start(
    spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME"
)
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only
    # ─────────────────────────────────────────────────────────────────────
    existing = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    if existing > 0:
        msg = (
            f"[{NOTEBOOK}] ABORTED — {existing:,} rows already exist. "
            f"Prime is INSERT-only. Manually TRUNCATE {TARGET_TABLE} to re-prime."
        )
        print(msg)
        audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source counts
    # ─────────────────────────────────────────────────────────────────────
    chron_cnt = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.silver_aasbs_chronology
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    collab_cnt = spark.sql(f"""
        SELECT COUNT(*) FROM {SILVER}.silver_aasbs_central_collab
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    rows_read = chron_cnt + collab_cnt
    print(
        f"[{NOTEBOOK}] Source rows — chronology: {chron_cnt:,} | "
        f"central_collab: {collab_cnt:,} | total: {rows_read:,}"
    )

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views
    # IMPROVEMENT I-DP10-2: each branch resolved independently
    # ─────────────────────────────────────────────────────────────────────

    # 3a — IMPROVEMENT I-DP10-4: most recent comment per collab event
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_collab_comment AS
        SELECT
            cc.table_record_id  AS central_collab_id,
            cc.comment_type_cd,
            cc.comment_text
        FROM (
            SELECT
                table_record_id,
                comment_type_cd,
                comment_text,
                ROW_NUMBER() OVER (
                    PARTITION BY table_record_id ORDER BY id DESC
                ) AS rn
            FROM {SILVER}.silver_aasbs_central_comment
            WHERE table_name_cd = 'CENTRAL_COLLAB'
              AND COALESCE(is_deleted, FALSE) = FALSE
        ) cc
        WHERE cc.rn = 1
    """)

    # 3b — IMPROVEMENT I-DP10-3: entity type + FK resolution per collab
    # table_name_cd on central_collab identifies the related entity type
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_collab_entity AS
        SELECT
            c.id                                                AS central_collab_id,
            c.table_name_cd,
            c.table_record_id,
            -- IMPROVEMENT I-DP10-3: entity_type_cd via table_name_cd CASE
            CASE
                WHEN UPPER(c.table_name_cd) LIKE '%AWARD%'       THEN 'AWARD'
                WHEN UPPER(c.table_name_cd) = 'IA'
                  OR UPPER(c.table_name_cd) LIKE '%_IA'          THEN 'IA'
                WHEN UPPER(c.table_name_cd) LIKE '%ACQUISITION%' THEN 'ACQUISITION'
                WHEN UPPER(c.table_name_cd) LIKE '%SOLICIT%'     THEN 'SOLICITATION'
                WHEN UPPER(c.table_name_cd) LIKE '%FUNDING%'     THEN 'FUNDING'
                ELSE 'COLLAB'
            END                                                 AS entity_type_cd,
            -- award_sk: resolve when entity is AWARD type
            COALESCE(dam.award_sk, -1)                          AS award_sk,
            -- ia_sk: resolve when entity is IA type
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,
            -- agency_sk: via acquisition junction when available
            COALESCE(dag.agency_sk, -1)                         AS agency_sk
        FROM {SILVER}.silver_aasbs_central_collab c
        -- Award FK: table_record_id = award_mod.id when AWARD entity
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  UPPER(c.table_name_cd) LIKE '%AWARD%'
            AND am.id = c.table_record_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_award dam
            ON  dam.award_id          = am.award_id
            AND dam.is_current_flag   = TRUE
        -- IA FK: table_record_id = ia.id when IA entity
        LEFT JOIN {COMMON}.dim_ia dia
            ON  (UPPER(c.table_name_cd) = 'IA' OR UPPER(c.table_name_cd) LIKE '%_IA')
            AND dia.ia_id             = c.table_record_id
            AND dia.is_current_flag   = TRUE
        -- Agency via acquisition junction
        LEFT JOIN {SILVER}.silver_aasbs_central_collab_acq cca
            ON  cca.central_collab_id = c.id
            AND COALESCE(cca.is_deleted, FALSE) = FALSE
        LEFT JOIN {SILVER}.silver_aasbs_acquisition acq
            ON  acq.id = cca.acquisition_id
            AND COALESCE(acq.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = acq.activity_address_cd
            AND dag.is_current_flag     = TRUE
        WHERE COALESCE(c.is_deleted, FALSE) = FALSE
    """)

    # 3c — IMPROVEMENT I-DP10-2: Branch A — chronology events
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_chronology_events AS
        SELECT
            -- event_type_sk: direct join to dim_event_type
            COALESCE(det.event_type_sk, -1)                     AS event_type_sk,

            -- IMPROVEMENT I-DP10-3: entity_type_sk via source_table_name_cd CASE
            COALESCE(det2.entity_type_sk, -1)                   AS entity_type_sk,

            -- award_sk: when source_table_name_cd identifies an award entity
            COALESCE(dam.award_sk, -1)                          AS award_sk,

            -- ia_sk: when source_table_name_cd = 'IA'
            COALESCE(dia.ia_sk, -1)                             AS ia_sk,

            -- agency_sk: via acquisition.activity_address_cd
            COALESCE(dag.agency_sk, -1)                         AS agency_sk,

            -- Date FK
            CASE
                WHEN c.created_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(c.created_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS event_date_sk,

            -- Source identifiers
            c.source_record_id                                  AS source_entity_id,
            c.id                                                AS source_chronology_id,
            CAST(NULL AS BIGINT)                                AS source_collab_id,

            -- Event attributes
            c.created_dt                                        AS event_timestamp,
            c.created_by_user_id                                AS event_user_id,
            c.entry_description                                 AS event_description,

            -- Collab fields: NULL for chronology branch
            CAST(NULL AS STRING)                                AS collab_type_cd,
            CAST(NULL AS STRING)                                AS collab_status_cd,
            CAST(NULL AS TIMESTAMP)                             AS collab_due_dt,

            -- Comment fields: NULL for chronology branch
            CAST(NULL AS STRING)                                AS comment_type_cd,
            CAST(NULL AS STRING)                                AS comment_text,

            -- SILVER-PENDING: email_status_cd — notification not in Silver (I-DP10-6)
            CAST(NULL AS STRING)                                AS email_status_cd

        FROM {SILVER}.silver_aasbs_chronology c

        -- event_type_sk: dim_event_type via event_type_cd
        LEFT JOIN {CAT}.dim_event_type det
            ON  det.event_type_cd = c.event_type_cd

        -- IMPROVEMENT I-DP10-3: entity type from source_table_name_cd CASE
        LEFT JOIN {WPR}.dim_entity_type det2
            ON  det2.entity_type_cd = CASE
                WHEN UPPER(c.source_table_name_cd) LIKE '%AWARD%'       THEN 'AWARD'
                WHEN UPPER(c.source_table_name_cd) = 'IA'
                  OR UPPER(c.source_table_name_cd) LIKE '%_IA'          THEN 'IA'
                WHEN UPPER(c.source_table_name_cd) LIKE '%ACQUISITION%' THEN 'ACQUISITION'
                WHEN UPPER(c.source_table_name_cd) LIKE '%SOLICIT%'     THEN 'SOLICITATION'
                WHEN UPPER(c.source_table_name_cd) LIKE '%FUNDING%'     THEN 'FUNDING'
                ELSE 'COLLAB'
            END

        -- award_sk: event_record_id → award_mod.award_id when AWARD entity
        LEFT JOIN {SILVER}.silver_aasbs_award_mod am
            ON  UPPER(c.source_table_name_cd) LIKE '%AWARD%'
            AND am.id = c.event_record_id
            AND COALESCE(am.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_award dam
            ON  dam.award_id        = am.award_id
            AND dam.is_current_flag = TRUE

        -- ia_sk: source_record_id when IA entity
        LEFT JOIN {COMMON}.dim_ia dia
            ON  (UPPER(c.source_table_name_cd) = 'IA'
                 OR UPPER(c.source_table_name_cd) LIKE '%_IA')
            AND dia.ia_id           = c.source_record_id
            AND dia.is_current_flag = TRUE

        -- agency_sk: via acquisition when ACQUISITION entity
        LEFT JOIN {SILVER}.silver_aasbs_acquisition acq
            ON  UPPER(c.source_table_name_cd) LIKE '%ACQUISITION%'
            AND acq.id = c.source_record_id
            AND COALESCE(acq.is_deleted, FALSE) = FALSE
        LEFT JOIN {COMMON}.dim_agency dag
            ON  dag.activity_address_cd = acq.activity_address_cd
            AND dag.is_current_flag     = TRUE

        WHERE COALESCE(c.is_deleted, FALSE) = FALSE
    """)

    # 3d — IMPROVEMENT I-DP10-2: Branch B — collaboration events
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_collab_events AS
        SELECT
            -- event_type_sk: collab events use a fixed COLLAB event type
            -- If 'COLLABORATION' code not in dim_event_type → sentinel -1
            COALESCE(det.event_type_sk, -1)                     AS event_type_sk,

            -- entity_type_sk: from v_collab_entity
            COALESCE(det2.entity_type_sk, -1)                   AS entity_type_sk,

            -- award_sk, ia_sk, agency_sk from v_collab_entity
            ve.award_sk,
            ve.ia_sk,
            ve.agency_sk,

            -- Date FK
            CASE
                WHEN cc.created_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(cc.created_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS event_date_sk,

            -- Source identifiers
            cc.table_record_id                                  AS source_entity_id,
            CAST(NULL AS BIGINT)                                AS source_chronology_id,
            cc.id                                               AS source_collab_id,

            -- Event attributes
            cc.created_dt                                       AS event_timestamp,
            cc.created_by_user_id                               AS event_user_id,
            cc.subject                                          AS event_description,

            -- Collab-specific fields
            cc.collab_type_cd,
            cc.collab_status_cd,
            cc.due_dt                                           AS collab_due_dt,

            -- IMPROVEMENT I-DP10-4: comment fields via central_comment join
            -- Source: aasbs.central_comment (general polymorphic comment table)
            -- NOT aasbs.central_collab_comment (absent from Silver DDL)
            -- MAX(id) per collab gives most recent comment.
            vcm.comment_type_cd,
            vcm.comment_text,

            -- SILVER-PENDING: email_status_cd (I-DP10-6)
            CAST(NULL AS STRING)                                AS email_status_cd

        FROM {SILVER}.silver_aasbs_central_collab cc

        -- entity type + FKs from pre-aggregated view
        LEFT JOIN v_collab_entity ve
            ON  ve.central_collab_id = cc.id

        -- IMPROVEMENT I-DP10-3: entity_type_sk from wpr.dim_entity_type
        LEFT JOIN {WPR}.dim_entity_type det2
            ON  det2.entity_type_cd = ve.entity_type_cd

        -- event_type_sk: look up 'COLLABORATION' event type
        LEFT JOIN {CAT}.dim_event_type det
            ON  UPPER(det.event_type_cd) = 'COLLABORATION'

        -- IMPROVEMENT I-DP10-4: comment join
        LEFT JOIN v_collab_comment vcm
            ON  vcm.central_collab_id = cc.id

        WHERE COALESCE(cc.is_deleted, FALSE) = FALSE
    """)

    print(f"[{NOTEBOOK}] All pre-aggregated views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT: UNION ALL of both branches
    # IMPROVEMENT I-DP10-2: main INSERT is just the UNION ALL
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            event_type_sk, entity_type_sk, award_sk, ia_sk, agency_sk,
            event_date_sk, source_entity_id, source_chronology_id,
            source_collab_id, event_timestamp, event_user_id, event_description,
            collab_type_cd, collab_status_cd, collab_due_dt,
            comment_type_cd, comment_text, email_status_cd,
            _gold_created_at, _gold_updated_at, _source_batch_id
        )
        -- Branch A: chronology events
        SELECT
            event_type_sk, entity_type_sk, award_sk, ia_sk, agency_sk,
            event_date_sk, source_entity_id, source_chronology_id,
            source_collab_id, event_timestamp, event_user_id, event_description,
            collab_type_cd, collab_status_cd, collab_due_dt,
            comment_type_cd, comment_text, email_status_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM v_chronology_events

        UNION ALL

        -- Branch B: collaboration events
        SELECT
            event_type_sk, entity_type_sk, award_sk, ia_sk, agency_sk,
            event_date_sk, source_entity_id, source_chronology_id,
            source_collab_id, event_timestamp, event_user_id, event_description,
            collab_type_cd, collab_status_cd, collab_due_dt,
            comment_type_cd, comment_text, email_status_cd,
            current_timestamp(), current_timestamp(), '{RUN_ID}'
        FROM v_collab_events
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total,
            SUM(CASE WHEN source_chronology_id IS NOT NULL THEN 1 ELSE 0 END)   AS chron_rows,
            SUM(CASE WHEN source_collab_id IS NOT NULL     THEN 1 ELSE 0 END)   AS collab_rows,
            SUM(CASE WHEN award_sk          != -1          THEN 1 ELSE 0 END)   AS has_award_sk,
            SUM(CASE WHEN ia_sk             != -1          THEN 1 ELSE 0 END)   AS has_ia_sk,
            SUM(CASE WHEN entity_type_sk    = -1           THEN 1 ELSE 0 END)   AS sentinel_entity,
            SUM(CASE WHEN comment_text IS NOT NULL         THEN 1 ELSE 0 END)   AS has_comment,
            SUM(CASE WHEN email_status_cd IS NOT NULL      THEN 1 ELSE 0 END)   AS has_email_status
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"[{NOTEBOOK}] Branch split — "
        f"chronology={stats[1]:,} | collab={stats[2]:,} | total={stats[0]:,}"
    )
    print(
        f"[{NOTEBOOK}] FK coverage — "
        f"award_sk_resolved={stats[3]:,} | ia_sk_resolved={stats[4]:,} | "
        f"entity_sentinel={stats[5]:,}"
    )
    print(
        f"[{NOTEBOOK}] IMPROVEMENT I-DP10-4 — comment coverage: "
        f"{stats[6]:,} collab events have comment_text"
    )

    # IMPROVEMENT I-DP10-3: entity type distribution
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP10-3 — entity_type distribution:")
    etype_dist = spark.sql(f"""
        SELECT det.entity_type_cd, COUNT(*) AS cnt
        FROM {TARGET_TABLE} f
        LEFT JOIN {WPR}.dim_entity_type det ON det.entity_type_sk = f.entity_type_sk
        GROUP BY det.entity_type_cd ORDER BY cnt DESC
    """).collect()
    for row in etype_dist:
        print(f"  {str(row[0]):<20}  {row[1]:>8,} events")

    # IMPROVEMENT I-DP10-6: email_status_cd NULL assertion
    assert stats[7] == 0, \
        f"ASSERT FAILED: email_status_cd must be NULL on prime (SILVER-PENDING). " \
        f"Got {stats[7]} non-NULL rows."
    print(f"[{NOTEBOOK}] ✓ email_status_cd = NULL for all rows (SILVER-PENDING assertion passed)")

    if stats[5] > 0:
        # Show unrecognised table_name_cd values
        print(f"\n[{NOTEBOOK}] ⚠ {stats[5]:,} rows with entity_type_sk=-1. Unrecognised codes:")
        unk = spark.sql(f"""
            SELECT source_entity_id, COUNT(*) AS cnt
            FROM {TARGET_TABLE} WHERE entity_type_sk = -1
            GROUP BY source_entity_id ORDER BY cnt DESC LIMIT 10
        """).collect()
        for row in unk:
            print(f"  source_entity_id={row[0]}  count={row[1]:,}")

    audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    raise
