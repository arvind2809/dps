# Databricks notebook source
# =============================================================================
# pipeline_utils.py  (shared — identical copy used by both prime and CDC jobs)
# See dp1_prime/utils/pipeline_utils.py for full documentation.
# =============================================================================
# COMMAND ----------
import time, traceback
from datetime import datetime, timezone

CATALOG       = "assist_catalog"
SILVER_SCHEMA = "assist_finance"
GOLD_COMMON   = "common"
GOLD_OET      = "oet"
DATA_PRODUCT  = "DP1"
SECRET_SCOPE  = "assist-ocfo-secrets"

def silver(source_schema, table):
    return f"{CATALOG}.{SILVER_SCHEMA}.silver_{source_schema}_{table}"

def gold(product_schema, table):
    return f"{CATALOG}.{product_schema}.{table}"

def truncate_gold(spark, table_fqn):
    prev = row_count(spark, table_fqn)
    spark.sql(f"TRUNCATE TABLE {table_fqn}")
    print(f"  [TRUNCATE] {table_fqn} — removed {prev:,} rows")
    return prev

def row_count(spark, table_fqn):
    return spark.sql(f"SELECT COUNT(*) AS n FROM {table_fqn}").collect()[0]["n"]

def get_cluster_id():
    try: return spark.conf.get("spark.databricks.clusterUsageTags.clusterId","unknown")
    except: return "unknown"

def get_spark_app_id():
    try: return spark.sparkContext.applicationId
    except: return "unknown"

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

def audit_start(spark, job_run_id, job_name, task_name, target_table,
                source_schema="", source_table="", run_type="FULL_PRIME"):
    start_ts  = time.time()
    start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} {_AUDIT_COLS}
        VALUES ('{job_run_id}','{job_name}','{task_name}','{DATA_PRODUCT}','gold',
                '{target_table}','{source_schema}','{source_table}','{run_type}',
                'RUNNING',TIMESTAMP '{start_iso}',NULL,NULL,0,0,0,0,0,
                NULL,NULL,NULL,NULL,NULL,'{get_cluster_id()}','{get_spark_app_id()}',
                CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),'{job_run_id}')
    """)
    print(f"  [AUDIT] Started → {task_name}")
    return start_ts

def audit_success(spark, job_run_id, target_table, rows_written, rows_inserted, start_ts):
    dur = int(time.time()-start_ts)
    spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET run_status='SUCCESS', end_ts=CURRENT_TIMESTAMP(), duration_seconds={dur},
            rows_written={rows_written}, rows_inserted={rows_inserted},
            _gold_updated_at=CURRENT_TIMESTAMP()
        WHERE job_run_id='{job_run_id}' AND target_table='{target_table}' AND run_status='RUNNING'
    """)
    print(f"  [AUDIT] Success → {target_table} | {rows_written:,} rows | {dur}s")

def audit_fail(spark, job_run_id, target_table, err_msg, err_stack, start_ts):
    dur  = int(time.time()-start_ts)
    msg  = str(err_msg)[:1000].replace("'","''")
    stk  = str(err_stack)[:4000].replace("'","''")
    spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET run_status='FAILED', end_ts=CURRENT_TIMESTAMP(), duration_seconds={dur},
            error_message='{msg}', error_stack='{stk}', _gold_updated_at=CURRENT_TIMESTAMP()
        WHERE job_run_id='{job_run_id}' AND target_table='{target_table}' AND run_status='RUNNING'
    """)
    print(f"  [AUDIT] FAILED → {target_table} | {msg[:120]}")

# COMMAND ----------
print("pipeline_utils loaded ✓")
