# Databricks notebook source
# =============================================================================
# pipeline_utils.py
# Shared utilities for all ASSIST OCFO Gold priming and delta-refresh notebooks.
#
# Usage (in every child notebook):
#   %run ../utils/pipeline_utils
#
# Provides:
#   silver(schema, table)         → fully qualified Silver table name
#   gold(schema, table)           → fully qualified Gold table name
#   truncate_gold(table)          → TRUNCATE TABLE with row count logging
#   row_count(table)              → returns COUNT(*) as int
#   audit_start(...)              → INSERT RUNNING row to pipeline_audit
#   audit_success(...)            → UPDATE row to SUCCESS
#   audit_fail(...)               → UPDATE row to FAILED
#   get_cluster_id()              → current cluster ID from context
#   get_spark_app_id()            → current Spark application ID
# =============================================================================

# COMMAND ----------

import time
import traceback
from datetime import datetime, timezone

# COMMAND ----------

# ── Catalog / schema constants ────────────────────────────────────────────────
CATALOG        = "assist_dev"
SILVER_SCHEMA  = "assist_finance"   # flat Silver namespace
GOLD_COMMON    = "common"
GOLD_OET       = "oet"
DATA_PRODUCT   = "DP1"

# ── Secret scope ─────────────────────────────────────────────────────────────
# Credentials stored in Databricks Secret Scope "assist-ocfo-secrets".
# Keys: postgres_host, postgres_port, postgres_db, postgres_user, postgres_password
# Note: Silver→Gold priming reads from Delta Lake only — no Postgres JDBC required.
#       These are included for Bronze-layer jobs that share this utils module.
SECRET_SCOPE   = "assist-ocfo-secrets"

# COMMAND ----------

# ── Name helpers ──────────────────────────────────────────────────────────────

def silver(source_schema: str, table: str) -> str:
    """Return fully qualified Silver table name.
    e.g. silver('aasbs', 'loa') → 'assist_dev.assist_finance.silver_aasbs_loa'
    """
    return f"{CATALOG}.{SILVER_SCHEMA}.silver_{source_schema}_{table}"


def gold(product_schema: str, table: str) -> str:
    """Return fully qualified Gold table name.
    e.g. gold('common', 'dim_agency') → 'assist_dev.common.dim_agency'
    """
    return f"{CATALOG}.{product_schema}.{table}"


# ── DDL helpers ───────────────────────────────────────────────────────────────

def truncate_gold(spark, table_fqn: str) -> int:
    """TRUNCATE a Gold table and return the previous row count for audit logging."""
    prev_count = row_count(spark, table_fqn)
    spark.sql(f"TRUNCATE TABLE {table_fqn}")
    print(f"  [TRUNCATE] {table_fqn} — removed {prev_count:,} existing rows")
    return prev_count


def row_count(spark, table_fqn: str) -> int:
    """Return current COUNT(*) of a Delta table."""
    return spark.sql(f"SELECT COUNT(*) AS n FROM {table_fqn}").collect()[0]["n"]


# ── Context helpers ───────────────────────────────────────────────────────────

def get_cluster_id() -> str:
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
    except Exception:
        return "unknown"


def get_spark_app_id() -> str:
    try:
        return spark.sparkContext.applicationId
    except Exception:
        return "unknown"


# ── Pipeline audit helpers ────────────────────────────────────────────────────
# common.pipeline_audit has GENERATED ALWAYS AS IDENTITY on run_sk —
# never include run_sk in the INSERT column list.

AUDIT_TABLE = f"{CATALOG}.{GOLD_COMMON}.pipeline_audit"

_AUDIT_COLS = """(
    job_run_id, job_name, task_name, data_product, target_layer,
    target_table, source_schema, source_table, run_type,
    run_status, start_ts, end_ts, duration_seconds,
    rows_read, rows_written, rows_inserted, rows_updated, rows_soft_deleted,
    watermark_col, watermark_from, watermark_to,
    error_message, error_stack, databricks_cluster_id, spark_app_id,
    _gold_created_at, _gold_updated_at, _source_batch_id
)"""


def audit_start(
    spark,
    job_run_id: str,
    job_name: str,
    task_name: str,
    target_table: str,
    source_schema: str = "",
    source_table: str = "",
    run_type: str = "FULL_PRIME",
) -> float:
    """
    Insert a RUNNING row into pipeline_audit.
    Returns start_ts as a float (time.time()) for duration calculation.
    """
    start_ts = time.time()
    start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    cluster_id = get_cluster_id()
    app_id     = get_spark_app_id()

    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} {_AUDIT_COLS}
        VALUES (
            '{job_run_id}', '{job_name}', '{task_name}', '{DATA_PRODUCT}', 'gold',
            '{target_table}', '{source_schema}', '{source_table}', '{run_type}',
            'RUNNING', TIMESTAMP '{start_iso}', NULL, NULL,
            0, 0, 0, 0, 0,
            NULL, NULL, NULL,
            NULL, NULL, '{cluster_id}', '{app_id}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{job_run_id}'
        )
    """)
    print(f"  [AUDIT] Started  → {task_name} ({job_run_id})")
    return start_ts


def audit_success(
    spark,
    job_run_id: str,
    target_table: str,
    rows_written: int,
    rows_inserted: int,
    start_ts: float,
) -> None:
    """Update the audit row to SUCCESS with row counts and duration."""
    duration = int(time.time() - start_ts)
    spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET run_status       = 'SUCCESS',
            end_ts           = CURRENT_TIMESTAMP(),
            duration_seconds = {duration},
            rows_written     = {rows_written},
            rows_inserted    = {rows_inserted},
            _gold_updated_at = CURRENT_TIMESTAMP()
        WHERE job_run_id = '{job_run_id}'
          AND target_table = '{target_table}'
          AND run_status   = 'RUNNING'
    """)
    print(
        f"  [AUDIT] Success  → {target_table} "
        f"| {rows_written:,} rows | {duration}s"
    )


def audit_fail(
    spark,
    job_run_id: str,
    target_table: str,
    err_msg: str,
    err_stack: str,
    start_ts: float,
) -> None:
    """Update the audit row to FAILED with error details."""
    duration   = int(time.time() - start_ts)
    # Escape single quotes for SQL
    safe_msg   = str(err_msg)[:1000].replace("'", "''")
    safe_stack = str(err_stack)[:4000].replace("'", "''")
    spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET run_status       = 'FAILED',
            end_ts           = CURRENT_TIMESTAMP(),
            duration_seconds = {duration},
            error_message    = '{safe_msg}',
            error_stack      = '{safe_stack}',
            _gold_updated_at = CURRENT_TIMESTAMP()
        WHERE job_run_id = '{job_run_id}'
          AND target_table = '{target_table}'
          AND run_status   = 'RUNNING'
    """)
    print(f"  [AUDIT] FAILED   → {target_table} | {safe_msg[:120]}")


# COMMAND ----------

print("pipeline_utils loaded ✓")
print(f"  CATALOG       : {CATALOG}")
print(f"  SILVER_SCHEMA : {SILVER_SCHEMA}")
print(f"  AUDIT_TABLE   : {AUDIT_TABLE}")
