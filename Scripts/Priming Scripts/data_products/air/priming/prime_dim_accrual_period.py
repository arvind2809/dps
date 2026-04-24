# Databricks notebook source
# =============================================================================
# air/prime_dim_accrual_period.py
# Primes assist_dev.air.dim_accrual_period
#
# Strategy : TRUNCATE → INSERT  (fully idempotent — no Silver dependency)
# Grain    : One row per calendar month  (420 rows for default 2000–2040 range)
# SCD Type : 1 — static reference; attributes never change once generated
#
# Source: Mathematically generated federal fiscal calendar spine.
#         No Silver tables required; this notebook can run independently.
#
# Federal Fiscal Year (FY) rules (31 U.S.C. §1102):
#   FY starts 1 October, ends 30 September.
#   fy         = cal_year + 1  when cal_month >= 10, else cal_year
#   fy_month   = (cal_month - 10) % 12 + 1   (1=Oct … 12=Sep)
#   fy_quarter = ((fy_month - 1) // 3) + 1   (1=Oct–Dec, 2=Jan–Mar,
#                                               3=Apr–Jun, 4=Jul–Sep)
#   is_year_end_flag = TRUE when cal_month == 9  (September = FY year-end)
#
# period_label format: FY{fy}-M{fy_month:02d} ({cal_month_abbr} {cal_year})
#   e.g., FY2025-M01 (Oct 2024), FY2025-M12 (Sep 2025)
#
# IMPROVEMENT I-DP4-4 (v1.2.0):
#   Widget defaults updated from end_cal_year=2035 to end_cal_year=2040 to
#   align with dim_date which was extended to 2040-12-31 in v1.1.0.
#   Accrual periods for FY2036–FY2040 would previously produce
#   accrual_period_sk=-1 (sentinel) on the fact table because the dim_accrual_period
#   spine did not cover those years.  This also updates the driver widget default.
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",         "", "Pipeline Run ID")
dbutils.widgets.text("env",            "dev", "Environment")
# IMPROVEMENT I-DP4-4: end_cal_year default changed from 2035 → 2040
# to align with dim_date (v1.1.0) and prevent sentinel -1 on future periods.
dbutils.widgets.text("start_cal_year", "2000",
    "Start calendar year — Oct of this year = first spine row")
dbutils.widgets.text("end_cal_year",   "2040",
    "End calendar year — Sep of this year = last spine row  "
    "[CHANGED v1.2.0: was 2035, now 2040 to match dim_date range]")

RUN_ID         = dbutils.widgets.get("run_id")
ENV            = dbutils.widgets.get("env")
START_CAL_YEAR = int(dbutils.widgets.get("start_cal_year"))
END_CAL_YEAR   = int(dbutils.widgets.get("end_cal_year"))

PRODUCT      = "DP4"
NOTEBOOK     = "prime_dim_accrual_period"
TARGET_TABLE = "assist_dev.air.dim_accrual_period"

# COMMAND ----------

import calendar
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, BooleanType
)

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="v_accrual_periods")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")
print(f"[{NOTEBOOK}] Spine range: Oct {START_CAL_YEAR} → Sep {END_CAL_YEAR}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Generate fiscal calendar rows in Python
    #   One dict per calendar month: Oct START_CAL_YEAR → Sep END_CAL_YEAR
    # ─────────────────────────────────────────────────────────────────────
    MONTH_ABBR = {
        1: "Jan", 2: "Feb",  3: "Mar",  4: "Apr",  5: "May",  6: "Jun",
        7: "Jul", 8: "Aug",  9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    rows = []
    cal_year  = START_CAL_YEAR
    cal_month = 10   # spine begins in October

    while not (cal_year == END_CAL_YEAR and cal_month == 10):
        # Federal fiscal year and month
        fy       = cal_year + 1 if cal_month >= 10 else cal_year
        fy_month = (cal_month - 10) % 12 + 1
        fy_qtr   = ((fy_month - 1) // 3) + 1

        # Calendar period boundaries
        _, last_day  = calendar.monthrange(cal_year, cal_month)
        period_start = date(cal_year, cal_month, 1)
        period_end   = date(cal_year, cal_month, last_day)

        rows.append({
            "accrual_year"       : cal_year,
            "accrual_month"      : cal_month,
            "accrual_fy"         : fy,
            "accrual_fy_month"   : fy_month,
            "accrual_fy_quarter" : fy_qtr,
            "period_label"       : (
                f"FY{fy}-M{fy_month:02d} "
                f"({MONTH_ABBR[cal_month]} {cal_year})"
            ),
            "period_start_dt"    : str(period_start),
            "period_end_dt"      : str(period_end),
            "is_year_end_flag"   : (cal_month == 9),
        })

        if cal_month == 12:
            cal_month = 1
            cal_year += 1
        else:
            cal_month += 1

    rows_read = len(rows)
    print(f"[{NOTEBOOK}] Generated {rows_read:,} calendar periods")
    print(f"[{NOTEBOOK}] First: {rows[0]['period_label']}  "
          f"Last:  {rows[-1]['period_label']}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — Create Spark DataFrame and INSERT into Delta
    # ─────────────────────────────────────────────────────────────────────
    schema = StructType([
        StructField("accrual_year",       IntegerType(), False),
        StructField("accrual_month",      IntegerType(), False),
        StructField("accrual_fy",         IntegerType(), False),
        StructField("accrual_fy_month",   IntegerType(), False),
        StructField("accrual_fy_quarter", IntegerType(), False),
        StructField("period_label",       StringType(),  False),
        StructField("period_start_dt",    StringType(),  False),
        StructField("period_end_dt",      StringType(),  False),
        StructField("is_year_end_flag",   BooleanType(), False),
    ])

    (spark.createDataFrame([Row(**r) for r in rows], schema=schema)
         .createOrReplaceTempView("v_accrual_periods"))

    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            accrual_year,
            accrual_month,
            accrual_fy,
            accrual_fy_month,
            accrual_fy_quarter,
            period_label,
            period_start_dt,
            period_end_dt,
            is_year_end_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            accrual_year,
            accrual_month,
            accrual_fy,
            accrual_fy_month,
            accrual_fy_quarter,
            period_label,
            CAST(period_start_dt AS DATE),
            CAST(period_end_dt   AS DATE),
            is_year_end_flag,
            current_timestamp(),
            current_timestamp(),
            '{RUN_ID}'
        FROM v_accrual_periods
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — Post-load validation
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # Distribution by FY quarter — should be equal across Q1–Q4
    fy_qtr_dist = spark.sql(f"""
        SELECT accrual_fy_quarter, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY accrual_fy_quarter
        ORDER BY accrual_fy_quarter
    """).collect()
    print(f"[{NOTEBOOK}] Distribution by FY quarter:")
    for row in fy_qtr_dist:
        print(f"  Q{row[0]}: {row[1]:>5,} months")

    ye_count = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_year_end_flag = TRUE"
    ).collect()[0][0]
    # Expected: one September per FY = (END_CAL_YEAR - START_CAL_YEAR) year-end rows
    expected_ye = END_CAL_YEAR - START_CAL_YEAR
    print(f"[{NOTEBOOK}] Year-end periods (September): {ye_count:,}  "
          f"(expected {expected_ye} for FY{START_CAL_YEAR+1}–FY{END_CAL_YEAR})")

    # Natural key uniqueness check
    dupes = spark.sql(f"""
        SELECT COUNT(*) FROM (
            SELECT accrual_year, accrual_month
            FROM {TARGET_TABLE}
            GROUP BY accrual_year, accrual_month
            HAVING COUNT(*) > 1
        )
    """).collect()[0][0]
    if dupes > 0:
        print(f"[{NOTEBOOK}] WARNING: {dupes} duplicate (year, month) keys found.")
    else:
        print(f"[{NOTEBOOK}] ✓ Natural key uniqueness: OK")

    # IMPROVEMENT I-DP4-4: verify the spine covers FY2036+ periods
    # (dim_date now extends to 2040; accrual periods must match)
    late_periods = spark.sql(f"""
        SELECT COUNT(*) FROM {TARGET_TABLE}
        WHERE accrual_fy >= 2036
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] FY2036+ periods: {late_periods:,}  "
          f"(IMPROVEMENT I-DP4-4 — should be 60 for FY2036–FY2040 × 12 months)")

    assert ye_count == expected_ye, (
        f"ASSERT FAILED: expected {expected_ye} year-end rows, got {ye_count}. "
        f"Check spine generation loop."
    )

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
