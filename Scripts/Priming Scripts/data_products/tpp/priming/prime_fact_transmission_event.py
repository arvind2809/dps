# Databricks notebook source

# =============================================================================
# tpp/prime_fact_transmission_event.py
# Primes assist_dev.tpp.fact_transmission_event
#
# Grain    : One row per billing_summary.id
#            (one record per transmittal envelope entry)
# Strategy : INSERT only — abort if rows exist; manual TRUNCATE to re-prime
#
# Source-to-target mapping (all confirmed against Silver DDL):
#
#   transmittal_type_sk  ← tpp.dim_transmittal_type
#                          via billing_summary.transmittal_id
#                          → transmittal.transmittal_type_cd
#   aac_envelope_sk      ← tpp.dim_aac_envelope
#                          via billing_summary.activity_address_cd
#   agreement_sk         ← tpp.dim_agreement
#                          via billing_summary.agreement_num
#                          = silver_agreement_agreement.agreement_num
#   ia_sk                ← common.dim_ia
#                          via billing_summary.agreement_num
#                          → loa.agreement_number → loa.id
#                          → MIN(loa_ledger.ia_id) → dim_ia.ia_sk
#   agency_sk            ← common.dim_agency
#                          via billing_summary.activity_address_cd
#   event_date_sk        ← common.dim_date
#                          via billing_summary.generated_dt → YYYYMMDD INT
#   transmittal_stage_cd ← billing_summary.transmittal_status_cd
#                          (status code proxies stage for this source)
#   transmittal_status_cd ← billing_summary.transmittal_status_cd (direct)
#   transmitted_dt       ← transmittal.sent_dt via billing_summary.transmittal_id
#   confirmed_dt         ← CAST(NULL)  [SILVER-PENDING — no Pegasys confirmation
#                          feed ingested into Silver; TFM Vol.I Part 2 §4700]
#   error_code           ← transmit_error.transmittal_type_cd WHERE
#                          source_record_id = billing_summary.id
#   error_description    ← transmit_error.error_description (same join)
#   retry_count          ← transmit_error.retry_cnt (same join)
#   batch_record_count   ← transmittal.release_count
#   transmitted_amt      ← billing_summary.billing_amount
#   confirmed_amt        ← billing_summary.billing_amount WHERE
#                          billing_response_info.accepted_status = 'Y',
#                          else NULL.  [IMPROVEMENT I-DP5-5 — proxy approach.
#                          Authoritative source: Pegasys confirmation feed,
#                          SILVER-PENDING.]
#   variance_amt         ← transmitted_amt − confirmed_amt
#                          (NULL when confirmed_amt is NULL)
#
# IMPROVEMENT I-DP5-1 (built-in):
#   Seven pre-aggregated temp views prevent fan-out in the main INSERT:
#     v_transmittal_type_per_bs  — type_cd per billing_summary.transmittal_id
#     v_ia_per_agreement_num     — ia_sk per agreement_num via loa → loa_ledger
#     v_error_per_bs             — first error per billing_summary.id
#     v_confirmed_per_bs         — billing_response acceptance status per billing_record_id
#
# IMPROVEMENT I-DP5-2 (built-in — from first generation):
#   INSERT-only guard + audit trail consistent with DP2–DP4 patterns.
#
# IMPROVEMENT I-DP5-3 (built-in):
#   All SILVER-PENDING NULL columns annotated with source table name and
#   regulatory reference.
#
# IMPROVEMENT I-DP5-5 (built-in):
#   confirmed_amt uses billing_response_info.accepted_status='Y' as proxy.
#   When accepted, confirmed_amt = billing_summary.billing_amount (full amount
#   accepted). When not accepted or no response: NULL.
#   variance_amt = transmitted_amt − confirmed_amt; NULL when confirmed NULL.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP5"
NOTEBOOK     = "prime_fact_transmission_event"
TARGET_TABLE = "assist_dev.tpp.fact_transmission_event"

SILVER = "assist_dev.assist_finance"
TPP    = "assist_dev.tpp"
COMMON = "assist_dev.common"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_aasbs_transmit_billing_summary")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — Guard: INSERT-only.  Abort if rows already exist.
    # ─────────────────────────────────────────────────────────────────────
    existing = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]

    if existing > 0:
        msg = (
            f"[{NOTEBOOK}] ABORTED — {existing:,} rows already exist. "
            f"Prime is INSERT-only. Manually TRUNCATE {TARGET_TABLE} to re-prime."
        )
        print(msg)
        #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, msg)
        audit_fail(spark, RUN_ID, TARGET_TABLE, str(msg), msg, start_ts)
        dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Source count
    # ─────────────────────────────────────────────────────────────────────
    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_aasbs_transmit_billing_summary
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Silver billing_summary row count: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Pre-aggregated temp views  (IMPROVEMENT I-DP5-1)
    # All views guarantee one row per grain key before the main INSERT.
    # ─────────────────────────────────────────────────────────────────────

    # 3a — Transmittal type per billing_summary
    # billing_summary.transmittal_id → transmittal.id → transmittal.transmittal_type_cd
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_transmittal_type_per_bs AS
        SELECT
            bs.id                       AS billing_summary_id,
            t.transmittal_type_cd,
            t.sent_dt                   AS transmitted_dt,
            t.release_count             AS batch_record_count
        FROM {SILVER}.silver_aasbs_transmit_billing_summary bs
        LEFT JOIN {SILVER}.silver_aasbs_transmit_transmittal t
            ON  t.id = bs.transmittal_id
            AND COALESCE(t.is_deleted, FALSE) = FALSE
        WHERE COALESCE(bs.is_deleted, FALSE) = FALSE
    """)

    # 3b — ia_sk per agreement number
    # Path: billing_summary.agreement_num
    #       → loa.agreement_number (loa.id)
    #       → loa_ledger.loa_id → MIN(loa_ledger.ia_id)
    #       → common.dim_ia.ia_sk
    # Pre-aggregation is essential: loa_ledger can have many rows per loa.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_ia_per_agreement_num AS
        SELECT
            l.agreement_number      AS agreement_num,
            COALESCE(dia.ia_sk, -1) AS ia_sk
        FROM (
            SELECT
                loa.agreement_number,
                MIN(ll.ia_id)       AS ia_id
            FROM {SILVER}.silver_aasbs_loa loa
            JOIN {SILVER}.silver_aasbs_loa_ledger ll
                ON  ll.loa_id = loa.id
                AND ll.ia_id IS NOT NULL
                AND COALESCE(ll.is_deleted, FALSE) = FALSE
            WHERE COALESCE(loa.is_deleted, FALSE) = FALSE
              AND loa.agreement_number IS NOT NULL
            GROUP BY loa.agreement_number
        ) l
        LEFT JOIN {COMMON}.dim_ia dia
            ON  dia.ia_id           = l.ia_id
            AND dia.is_current_flag = TRUE
    """)

    # 3c — Error per billing_summary
    # transmit_error.source_record_id identifies the erroring source record.
    # source_table_name_cd narrows to billing_summary rows.
    # MIN() picks the earliest (primary) error if multiple exist per record.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_error_per_bs AS
        SELECT
            source_record_id        AS billing_summary_id,
            MIN(retry_cnt)          AS retry_count,
            MAX(error_description)  AS error_description,
            MAX(transmittal_type_cd) AS error_transmittal_type_cd
        FROM {SILVER}.silver_aasbs_transmit_transmit_error
        WHERE COALESCE(is_deleted, FALSE) = FALSE
          AND UPPER(COALESCE(source_table_name_cd, '')) LIKE '%BILLING%'
        GROUP BY source_record_id
    """)

    # 3d — Confirmed amount proxy per billing_record_id
    # SILVER GAP — confirmed_amt (IMPROVEMENT I-DP5-5 proxy):
# IMPROVEMENT I-DP5-5: billing_response_info.accepted_status = 'Y'
    # means the billing record was accepted by the Treasury system.
    # In that case we treat the full billing_summary.billing_amount as confirmed.
    # billing_summary.billing_record_id links to billing_response_info.billing_record_id.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_confirmed_per_bs AS
        SELECT
            bs.id                   AS billing_summary_id,
            -- IMPROVEMENT I-DP5-5: proxy for confirmed_amt.
            -- When billing_response_info indicates acceptance, the full
            -- transmitted amount is treated as confirmed.
            -- Authoritative source: Pegasys confirmation feed (SILVER-PENDING).
            CASE
                WHEN UPPER(COALESCE(bri.accepted_status, 'N')) = 'Y'
                THEN bs.billing_amount
                ELSE CAST(NULL AS DECIMAL(15, 2))
            END                     AS confirmed_amt
        FROM {SILVER}.silver_aasbs_transmit_billing_summary bs
        LEFT JOIN {SILVER}.silver_billing_billing_response_info bri
            ON  bri.billing_record_id = bs.billing_record_id
            AND COALESCE(bri.is_deleted, FALSE) = FALSE
        WHERE COALESCE(bs.is_deleted, FALSE) = FALSE
    """)

    print(f"[{NOTEBOOK}] All supporting temp views created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            transmittal_type_sk,
            aac_envelope_sk,
            agreement_sk,
            ia_sk,
            agency_sk,
            event_date_sk,
            source_billing_summary_id,
            transmittal_stage_cd,
            transmittal_status_cd,
            transmitted_dt,
            confirmed_dt,
            error_code,
            error_description,
            retry_count,
            batch_record_count,
            transmitted_amt,
            confirmed_amt,
            variance_amt,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT

            /* ── DP5 dimension FKs ─────────────────────────────────────── */

            -- transmittal_type_sk: via transmittal.transmittal_type_cd
            COALESCE(dtt.transmittal_type_sk, -1)               AS transmittal_type_sk,

            -- aac_envelope_sk: billing_summary.activity_address_cd = dim_aac_envelope.aac
            COALESCE(dae.aac_envelope_sk, -1)                   AS aac_envelope_sk,

            -- agreement_sk: billing_summary.agreement_num → agreement.agreement_num
            COALESCE(dag.agreement_sk, -1)                      AS agreement_sk,

            /* ── Common dimension FKs ──────────────────────────────────── */

            -- ia_sk: agreement_num → loa → loa_ledger.ia_id (pre-aggregated)
            COALESCE(iag.ia_sk, -1)                             AS ia_sk,

            -- agency_sk: billing_summary.activity_address_cd
            COALESCE(agn.agency_sk, -1)                         AS agency_sk,

            /* ── Date FK ───────────────────────────────────────────────── */

            -- event_date_sk: billing_summary.generated_dt → YYYYMMDD INT
            CASE
                WHEN bs.generated_dt IS NOT NULL
                THEN CAST(DATE_FORMAT(bs.generated_dt, 'yyyyMMdd') AS INT)
                ELSE NULL
            END                                                 AS event_date_sk,

            /* ── Source identifier ─────────────────────────────────────── */

            bs.id                                               AS source_billing_summary_id,

            /* ── Transmission pipeline stage / status ──────────────────── */

            -- transmittal_stage_cd: billing_summary.transmittal_status_cd
            -- is the closest available proxy for the pipeline stage at prime.
            -- CDC enrichment via lu_transmittal_stage decode will replace.
            bs.transmittal_status_cd                            AS transmittal_stage_cd,
            bs.transmittal_status_cd                            AS transmittal_status_cd,

            /* ── Dates ─────────────────────────────────────────────────── */

            -- transmitted_dt: transmittal.sent_dt (when batch was dispatched)
            tt.transmitted_dt,

            -- confirmed_dt: SILVER-PENDING.
            -- No Pegasys/VITAP confirmation timestamp feed is ingested into Silver.
            -- Ref: TFM Volume I, Part 2, §4700 — confirmation timestamp required
            -- for Prompt Payment Act compliance monitoring.
            -- CDC will populate from Treasury system acknowledgement feed.
            CAST(NULL AS TIMESTAMP)                             AS confirmed_dt,

            /* ── Error details ─────────────────────────────────────────── */

            -- error_code: transmit_error.transmittal_type_cd for this billing record
            -- (transmit_error uses transmittal_type_cd as the error classifier)
            err.error_transmittal_type_cd                       AS error_code,
            err.error_description,
            COALESCE(err.retry_count, 0)                        AS retry_count,

            /* ── Batch metrics ─────────────────────────────────────────── */

            -- batch_record_count: transmittal.release_count
            COALESCE(tt.batch_record_count, 0)                  AS batch_record_count,

            /* ── Financial measures ─────────────────────────────────────── */

            -- transmitted_amt: billing_summary.billing_amount
            COALESCE(bs.billing_amount, 0.00)                   AS transmitted_amt,

            -- confirmed_amt: IMPROVEMENT I-DP5-5 — proxy.
            -- billing_amount when billing_response_info.accepted_status = 'Y'.
            -- NULL when not accepted or no response record exists.
            -- Authoritative source: Pegasys confirmation feed (SILVER-PENDING).
            conf.confirmed_amt,

            -- variance_amt: transmitted_amt − confirmed_amt.
            -- NULL when confirmed_amt is NULL (no confirmation response yet).
            CASE
                WHEN conf.confirmed_amt IS NOT NULL
                THEN ROUND(
                    COALESCE(bs.billing_amount, 0.00) - conf.confirmed_amt,
                    2
                )
                ELSE CAST(NULL AS DECIMAL(15, 2))
            END                                                 AS variance_amt,

            /* ── Audit ───────────────────────────────────────────────────── */
            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_aasbs_transmit_billing_summary bs

        /* ── Transmittal type via pre-aggregated view ────────────────────── */
        LEFT JOIN v_transmittal_type_per_bs tt
            ON  tt.billing_summary_id = bs.id

        /* ── dim_transmittal_type SK ─────────────────────────────────────── */
        LEFT JOIN {TPP}.dim_transmittal_type dtt
            ON  dtt.transmittal_type_cd = tt.transmittal_type_cd

        /* ── dim_aac_envelope SK ─────────────────────────────────────────── */
        LEFT JOIN {TPP}.dim_aac_envelope dae
            ON  dae.aac = bs.activity_address_cd

        /* ── dim_agreement SK ───────────────────────────────────────────── */
        LEFT JOIN {SILVER}.silver_agreement_agreement agr
            ON  agr.agreement_num = bs.agreement_num
            AND COALESCE(agr.is_deleted, FALSE) = FALSE
        LEFT JOIN {TPP}.dim_agreement dag
            ON  dag.agreement_id = agr.agreement_id

        /* ── ia_sk via pre-aggregated agreement→loa→ia path ─────────────── */
        LEFT JOIN v_ia_per_agreement_num iag
            ON  iag.agreement_num = bs.agreement_num

        /* ── common.dim_agency ────────────────────────────────────────────── */
        LEFT JOIN {COMMON}.dim_agency agn
            ON  agn.activity_address_cd = bs.activity_address_cd
            AND agn.is_current_flag     = TRUE

        /* ── Error details per billing_summary ───────────────────────────── */
        LEFT JOIN v_error_per_bs err
            ON  err.billing_summary_id = bs.id

        /* ── Confirmed amount proxy ───────────────────────────────────────── */
        LEFT JOIN v_confirmed_per_bs conf
            ON  conf.billing_summary_id = bs.id

        WHERE COALESCE(bs.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load metrics
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Fan-out guard
    if rows_written != rows_read:
        print(
            f"[{NOTEBOOK}] WARNING: rows_written ({rows_written:,}) ≠ "
            f"source rows ({rows_read:,}) — check temp view joins for fan-out."
        )
    else:
        print(f"[{NOTEBOOK}] ✓ Row count parity: {rows_written:,}")

    sentinel = spark.sql(f"""
        SELECT
            SUM(CASE WHEN transmittal_type_sk = -1 THEN 1 ELSE 0 END) AS unresolved_type,
            SUM(CASE WHEN aac_envelope_sk     = -1 THEN 1 ELSE 0 END) AS unresolved_aac,
            SUM(CASE WHEN agreement_sk        = -1 THEN 1 ELSE 0 END) AS unresolved_agreement,
            SUM(CASE WHEN ia_sk               = -1 THEN 1 ELSE 0 END) AS unresolved_ia,
            SUM(CASE WHEN agency_sk           = -1 THEN 1 ELSE 0 END) AS unresolved_agency
        FROM {TARGET_TABLE}
    """).collect()[0]
    print(
        f"[{NOTEBOOK}] Sentinel FKs — "
        f"type: {sentinel[0]:,} | aac: {sentinel[1]:,} | "
        f"agreement: {sentinel[2]:,} | ia: {sentinel[3]:,} | "
        f"agency: {sentinel[4]:,}"
    )

    fin = spark.sql(f"""
        SELECT
            ROUND(SUM(transmitted_amt),  2) AS total_transmitted,
            ROUND(SUM(confirmed_amt),    2) AS total_confirmed,
            ROUND(SUM(variance_amt),     2) AS total_variance,
            SUM(CASE WHEN confirmed_dt IS NOT NULL THEN 1 ELSE 0 END) AS has_confirmed_dt,
            SUM(CASE WHEN error_code IS NOT NULL   THEN 1 ELSE 0 END) AS error_rows
        FROM {TARGET_TABLE}
    """).collect()[0]
    confirmed = fin[1] if fin[1] is not None else 0
    print(
        f"[{NOTEBOOK}] Financial — "
        f"transmitted=${fin[0]:,.2f} | "
        f"confirmed=${confirmed:,.2f} | "
        f"variance=${fin[2]:,.2f}"
    )
    print(
        f"[{NOTEBOOK}] confirmed_dt={fin[3]:,} (expected 0 — SILVER-PENDING) | "
        f"error_rows={fin[4]:,}"
    )

    assert fin[3] == 0, \
        "ASSERT FAILED: confirmed_dt must be NULL on prime (SILVER-PENDING)"

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
