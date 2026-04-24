# Databricks notebook source
#%md
## pipeline_utils.py — Shared Pipeline Utilities
#**Product:** All Data Products (DP1–DP11)
#**Layer:** Utility / Shared
#**Description:** Common helper functions for audit logging, secret retrieval,
#and JDBC connection management. Identical copy to DP1 utils — no DP2-specific changes.
#Sourced into child notebooks via `%run ../utils/pipeline_utils`.

# COMMAND ----------

import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------

def get_secret(key: str, scope: str = "assist-ocfo-secrets") -> str:
    """
    Retrieve a secret from the Databricks secret scope.

    Args:
        key:   Secret key name within the scope.
        scope: Secret scope name. Defaults to 'assist-ocfo-secrets'.

    Returns:
        Decrypted secret value as a string.
    """
    return dbutils.secrets.get(scope=scope, key=key)


# ---------------------------------------------------------------------------
# JDBC helpers
# ---------------------------------------------------------------------------

def get_jdbc_url(host: str, port: str, db: str) -> str:
    """Build a PostgreSQL JDBC URL."""
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_read(spark: SparkSession, jdbc_url: str, user: str, password: str,
              query: str, num_partitions: int = 8,
              partition_col: str = None, lower: int = None, upper: int = None):
    """
    Read from PostgreSQL via JDBC.

    For large tables supply partition_col / lower / upper to enable parallel reads.
    Falls back to single-partition read when partition args are absent.
    """
    reader = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", f"({query}) AS _q")
        .option("fetchsize", "5000")
    )
    if partition_col and lower is not None and upper is not None:
        reader = (
            reader
            .option("partitionColumn", partition_col)
            .option("lowerBound", str(lower))
            .option("upperBound", str(upper))
            .option("numPartitions", str(num_partitions))
        )
    return reader.load()


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def audit_start(spark: SparkSession, run_id: str, product: str,
                notebook: str, target_table: str,
                run_type: str = "FULL_PRIME") -> float:
    """
    Write a RUNNING audit row and return the start timestamp (float epoch).

    Args:
        spark:        Active SparkSession.
        run_id:       Databricks job_run_id or manually supplied batch ID.
        product:      Data product label e.g. 'DP2'.
        notebook:     Short notebook name e.g. 'prime_dim_acquisition'.
        target_table: Fully-qualified Gold table name.
        run_type:     'FULL_PRIME' | 'CDC_DELTA'. Defaults to 'FULL_PRIME'.

    Returns:
        start_ts: float Unix epoch for elapsed-time calculation.
    """
    start_ts = time.time()
    spark.sql(f"""
        INSERT INTO assist_dev.common.pipeline_audit
            (run_id, product, notebook, target_table, run_type,
             status, started_at, rows_read, rows_written, error_message)
        VALUES (
            '{run_id}', '{product}', '{notebook}', '{target_table}', '{run_type}',
            'RUNNING', current_timestamp(), NULL, NULL, NULL
        )
    """)
    return start_ts


def audit_success(spark: SparkSession, run_id: str, notebook: str,
                  target_table: str, start_ts: float,
                  rows_read: int, rows_written: int) -> None:
    """Update the RUNNING audit row to SUCCESS."""
    elapsed = round(time.time() - start_ts, 1)
    spark.sql(f"""
        UPDATE assist_dev.common.pipeline_audit
        SET
            status        = 'SUCCESS',
            finished_at   = current_timestamp(),
            elapsed_sec   = {elapsed},
            rows_read     = {rows_read},
            rows_written  = {rows_written}
        WHERE run_id = '{run_id}'
          AND notebook = '{notebook}'
          AND target_table = '{target_table}'
          AND status = 'RUNNING'
    """)


def audit_failure(spark: SparkSession, run_id: str, notebook: str,
                  target_table: str, start_ts: float, error_msg: str) -> None:
    """Update the RUNNING audit row to FAILED with error details."""
    elapsed = round(time.time() - start_ts, 1)
    safe_msg = error_msg.replace("'", "''")[:2000]
    spark.sql(f"""
        UPDATE assist_dev.common.pipeline_audit
        SET
            status        = 'FAILED',
            finished_at   = current_timestamp(),
            elapsed_sec   = {elapsed},
            error_message = '{safe_msg}'
        WHERE run_id = '{run_id}'
          AND notebook = '{notebook}'
          AND target_table = '{target_table}'
          AND status = 'RUNNING'
    """)


# ---------------------------------------------------------------------------
# Row count helper
# ---------------------------------------------------------------------------

def count_silver(spark: SparkSession, table: str) -> int:
    """Return row count for a Silver table (used as rows_read metric)."""
    return spark.table(table).count()
