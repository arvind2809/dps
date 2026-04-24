# Databricks notebook source
# =============================================================================
# wpr/prime_dim_entity_type.py
# Primes assist_dev.wpr.dim_entity_type
#
# Strategy : TRUNCATE → INSERT with hardcoded VALUES (fully idempotent)
# Grain    : One row per entity_type_cd  (6 rows total)
#
# IMPROVEMENT I-DP8-3 — Static seed table pattern (built-in):
#   dim_entity_type has NO Silver source. The Gold DDL has no pipeline.source
#   TBLPROPERTY. The 6 entity type codes are design-time constants from the
#   ASSIST data model — they define which types of entities can have role
#   assignments.
#
#   This is the first static seed Gold table in the DP series. The pattern:
#     (1) TRUNCATE for idempotency
#     (2) INSERT INTO … VALUES with hardcoded rows (no Silver join)
#     (3) Post-load ASSERT exactly 6 rows
#
#   The seed values are authoritative — they mirror the entity type constants
#   used in all five source role tables (acquisition_role, award_role, etc.)
#   that are currently blocked. They are stable by design: new entity types
#   would require source system changes, not pipeline changes.
#
# Seed rows (6 constants from the ASSIST data model):
#   AWARD        — Award / Contract entity type
#   IA           — Interagency Agreement
#   FUNDING      — Funding Package
#   SOLICITATION — Solicitation
#   ACQUISITION  — Acquisition
#   COLLAB       — Collaboration Request
#
# Ref: FAR 1.602 (award-level assignments), FAR Part 17.5 (IA roles),
#      ASSIST data model documentation
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_entity_type"
TARGET_TABLE = "assist_dev.wpr.dim_entity_type"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="entity_type_cd")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(
    f"[{NOTEBOOK}] IMPROVEMENT I-DP8-3 — STATIC SEED TABLE:\n"
    f"  No Silver source — 6 hardcoded ASSIST data model constants.\n"
    f"  This is the first static seed Gold table in the DP series."
)

EXPECTED_ROWS = 6

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE for idempotency
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — INSERT hardcoded VALUES  (IMPROVEMENT I-DP8-3)
    # No Silver source. Values are ASSIST data model design-time constants.
    # These correspond to the entity types across all five blocked role tables:
    #   AWARD       ← aasbs.award_role
    #   IA          ← aasbs.ia_role
    #   FUNDING     ← aasbs.funding_role
    #   SOLICITATION← aasbs.solicit_role
    #   ACQUISITION ← aasbs.acquisition_role
    #   COLLAB      ← aasbs.central_collab (collaboration assignments)
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (entity_type_cd, entity_type_desc, _gold_created_at, _gold_updated_at, _source_batch_id)
        VALUES
        -- Award/Contract entity type
        ('AWARD',        'Award / Contract',           current_timestamp(), current_timestamp(), '{RUN_ID}'),
        -- Interagency Agreement entity type (FAR Part 17.5)
        ('IA',           'Interagency Agreement',       current_timestamp(), current_timestamp(), '{RUN_ID}'),
        -- Funding Package entity type (funding amendment scope)
        ('FUNDING',      'Funding Package',             current_timestamp(), current_timestamp(), '{RUN_ID}'),
        -- Solicitation entity type (pre-award)
        ('SOLICITATION', 'Solicitation',                current_timestamp(), current_timestamp(), '{RUN_ID}'),
        -- Acquisition entity type (overall acquisition lifecycle)
        ('ACQUISITION',  'Acquisition',                 current_timestamp(), current_timestamp(), '{RUN_ID}'),
        -- Collaboration Request entity type (internal review workflows)
        ('COLLAB',       'Collaboration Request',        current_timestamp(), current_timestamp(), '{RUN_ID}')
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Post-load: assert exactly 6 rows  (IMPROVEMENT I-DP8-3)
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written} rows into {TARGET_TABLE}")

    assert rows_written == EXPECTED_ROWS, \
        f"ASSERT FAILED: expected exactly {EXPECTED_ROWS} rows in dim_entity_type " \
        f"(static seed), got {rows_written}."
    print(f"[{NOTEBOOK}] ✓ Exactly {EXPECTED_ROWS} entity type rows loaded.")

    # Print loaded seed values for audit trail
    seed = spark.sql(f"""
        SELECT entity_type_cd, entity_type_desc FROM {TARGET_TABLE} ORDER BY entity_type_cd
    """).collect()
    print(f"[{NOTEBOOK}] Loaded entity types:")
    for row in seed:
        print(f"  {row[0]:<15}  {row[1]}")

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, 0, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, 0, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
