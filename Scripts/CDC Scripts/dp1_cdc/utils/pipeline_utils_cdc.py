# Databricks notebook source
# =============================================================================
# pipeline_utils_cdc.py
# CDC-specific utilities for DP1 delta refresh notebooks.
# Extends pipeline_utils with watermark management, SCD2 merge helpers,
# and DP4 availability checks.
#
# Usage in every CDC child notebook:
#   %run ../utils/pipeline_utils_cdc
#   (This file itself %runs pipeline_utils, so only one %run needed.)
# =============================================================================

# COMMAND ----------
# MAGIC %run ./pipeline_utils

# COMMAND ----------
# ── Run type constant ─────────────────────────────────────────────────────────
RUN_TYPE_CDC = "DELTA_REFRESH"

# ── Safety-belt window (seconds) ─────────────────────────────────────────────
# If no prior watermark exists (first CDC run after prime), look back this
# many seconds from now as the watermark_from fallback.
# Default: 86400 = 24 hours. Adjust via widget if needed.
CDC_FALLBACK_LOOKBACK_SECONDS = 86400

# COMMAND ----------
# ── Watermark helpers ─────────────────────────────────────────────────────────

def get_watermark(spark, target_table: str) -> tuple:
    """
    Determine (watermark_from, watermark_to) for a CDC run.

    Strategy — Hybrid:
      1. PRIMARY: Read the latest SUCCESS row in pipeline_audit for this
         target_table where run_type = 'DELTA_REFRESH' or 'FULL_PRIME'.
         watermark_from = that row's watermark_to timestamp.
      2. SAFETY BELT: watermark_from must also satisfy
         Silver.updated_dt >= watermark_from.  If no prior watermark exists,
         fall back to NOW() - CDC_FALLBACK_LOOKBACK_SECONDS.
      3. watermark_to = CURRENT_TIMESTAMP() at time of call.

    Returns:
        (watermark_from_str, watermark_to_str)  — ISO timestamp strings
        suitable for use in Spark SQL predicates.
    """
    from datetime import datetime, timezone, timedelta

    watermark_to_ts = datetime.now(tz=timezone.utc)
    watermark_to    = watermark_to_ts.strftime("%Y-%m-%d %H:%M:%S")

    try:
        result = spark.sql(f"""
            SELECT watermark_to
            FROM   {AUDIT_TABLE}
            WHERE  target_table = '{target_table}'
              AND  run_status   = 'SUCCESS'
              AND  watermark_to IS NOT NULL
            ORDER BY end_ts DESC
            LIMIT 1
        """).collect()

        if result and result[0]["watermark_to"]:
            watermark_from = str(result[0]["watermark_to"])
            print(f"  [WATERMARK] Prior run found → from={watermark_from}")
        else:
            # No prior CDC or prime watermark → fallback lookback
            fallback_ts    = watermark_to_ts - timedelta(seconds=CDC_FALLBACK_LOOKBACK_SECONDS)
            watermark_from = fallback_ts.strftime("%Y-%m-%d %H:%M:%S")
            print(f"  [WATERMARK] No prior watermark — fallback lookback → from={watermark_from}")

    except Exception as e:
        fallback_ts    = watermark_to_ts - timedelta(seconds=CDC_FALLBACK_LOOKBACK_SECONDS)
        watermark_from = fallback_ts.strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [WATERMARK] Audit query error ({e}) — using fallback from={watermark_from}")

    print(f"  [WATERMARK] Window: {watermark_from}  →  {watermark_to}")
    return watermark_from, watermark_to


def audit_start_cdc(
    spark,
    job_run_id: str,
    job_name: str,
    task_name: str,
    target_table: str,
    source_schema: str,
    source_table: str,
    watermark_from: str,
    watermark_to: str,
    watermark_col: str = "updated_dt",
) -> float:
    """
    Insert a RUNNING row into pipeline_audit with watermark columns populated.
    Returns start_ts float for duration calculation.
    """
    start_ts  = time.time()
    start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cluster_id = get_cluster_id()
    app_id     = get_spark_app_id()

    safe_wf = watermark_from.replace("'", "''")
    safe_wt = watermark_to.replace("'", "''")

    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} {_AUDIT_COLS}
        VALUES (
            '{job_run_id}', '{job_name}', '{task_name}', '{DATA_PRODUCT}', 'gold',
            '{target_table}', '{source_schema}', '{source_table}', '{RUN_TYPE_CDC}',
            'RUNNING', TIMESTAMP '{start_iso}', NULL, NULL,
            0, 0, 0, 0, 0,
            '{watermark_col}', TIMESTAMP '{safe_wf}', TIMESTAMP '{safe_wt}',
            NULL, NULL, '{cluster_id}', '{app_id}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{job_run_id}'
        )
    """)
    print(f"  [AUDIT] CDC Started → {task_name} | window={watermark_from} → {watermark_to}")
    return start_ts


def audit_success_cdc(
    spark,
    job_run_id: str,
    target_table: str,
    rows_read: int,
    rows_written: int,
    rows_inserted: int,
    rows_updated: int,
    start_ts: float,
    watermark_to: str,
) -> None:
    """Update CDC audit row to SUCCESS with full row counts."""
    duration = int(time.time() - start_ts)
    safe_wt  = watermark_to.replace("'", "''")
    spark.sql(f"""
        UPDATE {AUDIT_TABLE}
        SET run_status       = 'SUCCESS',
            end_ts           = CURRENT_TIMESTAMP(),
            duration_seconds = {duration},
            rows_read        = {rows_read},
            rows_written     = {rows_written},
            rows_inserted    = {rows_inserted},
            rows_updated     = {rows_updated},
            watermark_to     = TIMESTAMP '{safe_wt}',
            _gold_updated_at = CURRENT_TIMESTAMP()
        WHERE job_run_id   = '{job_run_id}'
          AND target_table = '{target_table}'
          AND run_status   = 'RUNNING'
    """)
    print(
        f"  [AUDIT] CDC Success → {target_table} | "
        f"read={rows_read:,} written={rows_written:,} "
        f"ins={rows_inserted:,} upd={rows_updated:,} | {duration}s"
    )


# COMMAND ----------
# ── SCD2 hybrid MERGE helpers ─────────────────────────────────────────────────

def scd2_apply_changes(
    spark,
    target_fqn: str,
    source_df_view: str,
    natural_key: str,
    tracked_cols: list,
    inplace_cols: list,
    batch_id: str,
) -> tuple:
    """
    Apply hybrid SCD2 + in-place update logic to a Gold dimension table.

    Strategy (3-stage):
      Stage 1 — MERGE to close SCD2 rows where any tracked_col changed.
                Sets eff_end_dt = CURRENT_TIMESTAMP(), is_current_flag = FALSE.
      Stage 2 — INSERT new current versions for every row that was just closed.
      Stage 3 — MERGE to UPDATE in-place for rows where only inplace_cols changed
                (is_current_flag = TRUE and no tracked_col changed).

    Parameters:
        target_fqn     : fully qualified Gold table name
        source_df_view : temp view name containing new Silver data (current state)
        natural_key    : source natural key column name (e.g. 'ia_id', 'loa_id')
        tracked_cols   : list of columns that trigger a new SCD2 version
        inplace_cols   : list of columns updated in-place without versioning
        batch_id       : current job_run_id for _source_batch_id

    Returns:
        (rows_closed, rows_inserted, rows_updated)
    """
    tracked_change_cond = " OR ".join(
        [f"tgt.{c} IS DISTINCT FROM src.{c}" for c in tracked_cols]
    )
    inplace_change_cond = " OR ".join(
        [f"tgt.{c} IS DISTINCT FROM src.{c}" for c in inplace_cols]
    )
    inplace_set = ", ".join(
        [f"tgt.{c} = src.{c}" for c in inplace_cols]
    )

    # ── Stage 1: Close SCD2 rows where tracked fields changed ────────────────
    spark.sql(f"""
        MERGE INTO {target_fqn} AS tgt
        USING {source_df_view} AS src
          ON  tgt.{natural_key}    = src.{natural_key}
          AND tgt.is_current_flag  = TRUE
        WHEN MATCHED AND ({tracked_change_cond}) THEN
          UPDATE SET
            tgt.eff_end_dt        = CURRENT_TIMESTAMP(),
            tgt.is_current_flag   = FALSE,
            tgt._gold_updated_at  = CURRENT_TIMESTAMP()
    """)

    rows_closed = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {target_fqn}
        WHERE is_current_flag = FALSE
          AND eff_end_dt >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
    """).collect()[0]["n"]

    # ── Stage 2: Insert new current versions for closed rows ─────────────────
    # We re-join source to target to find rows just closed (is_current_flag=FALSE,
    # eff_end_dt set in last minute) and INSERT the src data as new current rows.
    all_data_cols = tracked_cols + inplace_cols

    # Build dynamic column list from source view schema (minus SK, SCD2, audit)
    src_cols = [
        f.name for f in spark.table(source_df_view).schema.fields
    ]

    spark.sql(f"""
        INSERT INTO {target_fqn}
        SELECT
            {", ".join(["src." + c for c in src_cols])},
            CURRENT_TIMESTAMP() AS eff_start_dt,
            CAST(NULL AS TIMESTAMP) AS eff_end_dt,
            TRUE AS is_current_flag,
            CURRENT_TIMESTAMP() AS _gold_created_at,
            CURRENT_TIMESTAMP() AS _gold_updated_at,
            '{batch_id}' AS _source_batch_id
        FROM {source_df_view} src
        JOIN {target_fqn} tgt
          ON  tgt.{natural_key}   = src.{natural_key}
          AND tgt.is_current_flag = FALSE
          AND tgt.eff_end_dt      >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
    """)

    rows_inserted = rows_closed  # one new version per closed row

    # ── Stage 3: In-place update for minor changes on still-current rows ──────
    spark.sql(f"""
        MERGE INTO {target_fqn} AS tgt
        USING {source_df_view} AS src
          ON  tgt.{natural_key}   = src.{natural_key}
          AND tgt.is_current_flag = TRUE
        WHEN MATCHED AND NOT ({tracked_change_cond})
                     AND ({inplace_change_cond}) THEN
          UPDATE SET
            {inplace_set},
            tgt._gold_updated_at = CURRENT_TIMESTAMP(),
            tgt._source_batch_id = '{batch_id}'
    """)

    # Count in-place updates (rows touched but not versioned)
    rows_updated = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {target_fqn}
        WHERE is_current_flag = TRUE
          AND _source_batch_id = '{batch_id}'
          AND _gold_updated_at >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
    """).collect()[0]["n"] - rows_inserted  # subtract newly inserted current rows

    print(f"  [SCD2] {target_fqn}: closed={rows_closed} new_versions={rows_inserted} in_place={max(rows_updated,0)}")
    return rows_closed, rows_inserted, max(rows_updated, 0)


# COMMAND ----------
# ── DP4 availability check ────────────────────────────────────────────────────

def dp4_ran_since(spark, watermark_from: str) -> bool:
    """
    Check whether the DP4 (Accrual Income) pipeline has completed a successful
    DELTA_REFRESH or FULL_PRIME run with watermark_to > watermark_from.

    Used to decide whether to populate accrued_amt on the DP1 fact CDC append.

    Returns True if DP4 has run and its data is available for the current window.
    """
    try:
        result = spark.sql(f"""
            SELECT COUNT(*) AS n
            FROM   {AUDIT_TABLE}
            WHERE  data_product  = 'DP4'
              AND  run_status    = 'SUCCESS'
              AND  watermark_to  > TIMESTAMP '{watermark_from}'
        """).collect()[0]["n"]

        available = result > 0
        print(f"  [DP4 CHECK] DP4 data available since {watermark_from}: {available}")
        return available
    except Exception as e:
        print(f"  [DP4 CHECK] Error checking DP4 status ({e}) — defaulting to False")
        return False


# COMMAND ----------
# ── Changed-row detection helper ──────────────────────────────────────────────

def changed_rows_filter(watermark_from: str, watermark_to: str,
                         dt_col: str = "updated_dt") -> str:
    """
    Returns a SQL WHERE fragment applying the hybrid watermark filter:
      PRIMARY:      {dt_col} >= '{watermark_from}' AND {dt_col} < '{watermark_to}'
      SAFETY BELT:  OR ({dt_col} IS NULL AND created_dt >= '{watermark_from}')

    The safety belt catches rows where updated_dt was never set (NULL) but the
    row was created within the window — common with insert-only CDC patterns.
    """
    return f"""
        (
            ({dt_col} >= TIMESTAMP '{watermark_from}'
             AND {dt_col} <  TIMESTAMP '{watermark_to}')
            OR
            ({dt_col} IS NULL
             AND created_dt >= TIMESTAMP '{watermark_from}'
             AND created_dt <  TIMESTAMP '{watermark_to}')
        )
    """


# COMMAND ----------
print("pipeline_utils_cdc loaded ✓")
print(f"  Extends  : pipeline_utils (CATALOG={CATALOG}, AUDIT_TABLE={AUDIT_TABLE})")
print(f"  Fallback : {CDC_FALLBACK_LOOKBACK_SECONDS}s lookback when no prior watermark")
