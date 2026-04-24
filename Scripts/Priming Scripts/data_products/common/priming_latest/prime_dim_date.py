# Databricks notebook source
# =============================================================================
# common/prime_dim_date.py
# Primes assist_dev.common.dim_date
#
# Strategy   : TRUNCATE → INSERT (fully idempotent — static generated spine)
# Grain      : One row per calendar day
# Spine range: 2000-01-01 to 2040-12-31  (14,976 rows)
#
# FIX (v1.1.0): Extended spine end date from 2035-12-31 to 2040-12-31 to
#   match the DDL annotation.  Federal contracts and multi-year no-year funds
#   can have periods of performance extending 10+ years; the extra 5 years
#   cost nothing at generation time and prevent dim_date FK misses on
#   long-running awards.
#
# Silver gap (is_federal_holiday):
#   Only the four fixed-date federal holidays are computed inline
#   (New Year's Day, Independence Day, Veterans Day, Christmas).
#   The seven floating-date holidays (MLK Day, Presidents Day, Memorial Day,
#   Juneteenth, Labor Day, Columbus Day, Thanksgiving) require a reference
#   table for correct observed-date derivation and are set FALSE here.
#   These will be back-filled via CDC once a federal holiday calendar table
#   is ingested into Silver (DP11 scope).
# =============================================================================

# COMMAND ----------

# MAGIC
# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id",   "", "Pipeline Run ID")
dbutils.widgets.text("job_name", "dp1_prime_full", "Job Name")

RUN_ID   = dbutils.widgets.get("run_id")   or "manual-" + get_spark_app_id()
JOB_NAME = dbutils.widgets.get("job_name")

TARGET = gold("common", "dim_date")
TASK   = "prime_dim_date"

print(f"[{TASK}] target={TARGET}")

# COMMAND ----------

start_ts = audit_start(
    spark, RUN_ID, JOB_NAME, TASK, TARGET,
    source_schema="generated", source_table="date_spine",
)

# COMMAND ----------

try:
    truncate_gold(spark, TARGET)

    spark.sql(f"""
        INSERT INTO {TARGET}
        (
            date_sk,
            calendar_date,
            day_of_week,
            day_of_week_num,
            day_of_month,
            day_of_year,
            week_of_year,
            calendar_month,
            month_name,
            calendar_quarter,
            calendar_year,
            federal_fiscal_fy,
            federal_fiscal_qtr,
            federal_fiscal_month,
            is_weekend_flag,
            is_federal_holiday,
            is_last_day_of_month,
            is_last_day_of_fy,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 1: Generate daily spine.
        -- FIX: end date extended from 2035-12-31 to 2040-12-31 to match
        --      the DDL annotation and cover long-running federal contracts.
        -- ─────────────────────────────────────────────────────────────────
        WITH date_spine AS (
            SELECT EXPLODE(
                SEQUENCE(DATE '2000-01-01', DATE '2040-12-31', INTERVAL 1 DAY)
            ) AS calendar_date
        ),

        -- ─────────────────────────────────────────────────────────────────
        -- Step 2: Derive calendar and federal fiscal attributes.
        -- ─────────────────────────────────────────────────────────────────
        enriched AS (
            SELECT
                calendar_date,

                -- Surrogate key: YYYYMMDD integer (also serves as natural key)
                CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT)     AS date_sk,

                DATE_FORMAT(calendar_date, 'EEEE')                      AS day_of_week,

                -- Databricks DAYOFWEEK returns 1=Sunday … 7=Saturday.
                -- Remap to ISO 8601 convention: 1=Monday … 7=Sunday.
                CASE
                    WHEN DAYOFWEEK(calendar_date) = 1 THEN 7
                    ELSE DAYOFWEEK(calendar_date) - 1
                END                                                      AS day_of_week_num,

                DAYOFMONTH(calendar_date)                                AS day_of_month,
                DAYOFYEAR(calendar_date)                                 AS day_of_year,
                WEEKOFYEAR(calendar_date)                                AS week_of_year,
                MONTH(calendar_date)                                     AS calendar_month,
                DATE_FORMAT(calendar_date, 'MMMM')                      AS month_name,
                QUARTER(calendar_date)                                   AS calendar_quarter,
                YEAR(calendar_date)                                      AS calendar_year,

                -- Federal fiscal year: Oct 1 start per 31 U.S.C. §1102.
                -- FY2025 = 1 Oct 2024 – 30 Sep 2025.
                CASE
                    WHEN MONTH(calendar_date) >= 10
                    THEN YEAR(calendar_date) + 1
                    ELSE YEAR(calendar_date)
                END                                                      AS federal_fiscal_fy,

                -- Federal fiscal quarter
                -- Q1=Oct–Dec, Q2=Jan–Mar, Q3=Apr–Jun, Q4=Jul–Sep
                CASE
                    WHEN MONTH(calendar_date) IN (10, 11, 12) THEN 1
                    WHEN MONTH(calendar_date) IN (1,  2,  3)  THEN 2
                    WHEN MONTH(calendar_date) IN (4,  5,  6)  THEN 3
                    ELSE 4
                END                                                      AS federal_fiscal_qtr,

                -- Federal fiscal month: 1=Oct … 12=Sep
                CASE
                    WHEN MONTH(calendar_date) >= 10
                    THEN MONTH(calendar_date) - 9
                    ELSE MONTH(calendar_date) + 3
                END                                                      AS federal_fiscal_month,

                -- Weekend: Saturday (Databricks=7) or Sunday (Databricks=1)
                DAYOFWEEK(calendar_date) IN (1, 7)                       AS is_weekend_flag,

                -- Federal holiday flag — SILVER GAP.
                -- Fixed-date rules only; covers 4 of 11 US federal holidays.
                -- Floating-date holidays are FALSE until a Silver holiday
                -- calendar table is available (planned DP11 enrichment).
                CASE
                    WHEN MONTH(calendar_date) = 1  AND DAYOFMONTH(calendar_date) = 1
                        THEN TRUE   -- New Year's Day (1 Jan)
                    WHEN MONTH(calendar_date) = 7  AND DAYOFMONTH(calendar_date) = 4
                        THEN TRUE   -- Independence Day (4 Jul)
                    WHEN MONTH(calendar_date) = 11 AND DAYOFMONTH(calendar_date) = 11
                        THEN TRUE   -- Veterans Day (11 Nov)
                    WHEN MONTH(calendar_date) = 12 AND DAYOFMONTH(calendar_date) = 25
                        THEN TRUE   -- Christmas Day (25 Dec)
                    ELSE FALSE      -- floating holidays: MLK Day, Presidents Day,
                                    -- Memorial Day, Juneteenth, Labor Day,
                                    -- Columbus Day, Thanksgiving — back-fill via CDC
                END                                                      AS is_federal_holiday,

                -- Last calendar day of the month
                calendar_date = LAST_DAY(calendar_date)                 AS is_last_day_of_month,

                -- Last day of federal fiscal year = 30 September
                (MONTH(calendar_date) = 9 AND DAYOFMONTH(calendar_date) = 30)
                                                                         AS is_last_day_of_fy

            FROM date_spine
        )

        -- ─────────────────────────────────────────────────────────────────
        -- Step 3: Final projection
        -- ─────────────────────────────────────────────────────────────────
        SELECT
            date_sk,
            calendar_date,
            day_of_week,
            day_of_week_num,
            day_of_month,
            day_of_year,
            week_of_year,
            calendar_month,
            month_name,
            calendar_quarter,
            calendar_year,
            federal_fiscal_fy,
            federal_fiscal_qtr,
            federal_fiscal_month,
            is_weekend_flag,
            is_federal_holiday,
            is_last_day_of_month,
            is_last_day_of_fy,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{RUN_ID}'
        FROM enriched
    """)

    n = row_count(spark, TARGET)
    print(f"  [OK] Inserted {n:,} date rows  (expected 14,976 for 2000-01-01 → 2040-12-31)")

    # ── Post-load sanity checks ───────────────────────────────────────────
    checks = spark.sql(f"""
        SELECT
            COUNT(*)                                                         AS total_rows,
            MIN(calendar_date)                                               AS min_date,
            MAX(calendar_date)                                               AS max_date,
            SUM(CASE WHEN federal_fiscal_fy IS NULL   THEN 1 ELSE 0 END)    AS null_fy,
            SUM(CASE WHEN is_federal_holiday = TRUE   THEN 1 ELSE 0 END)    AS holiday_rows,
            SUM(CASE WHEN is_last_day_of_fy = TRUE    THEN 1 ELSE 0 END)    AS fy_end_rows
        FROM {TARGET}
    """).collect()[0]

    print(f"  date range  : {checks['min_date']} → {checks['max_date']}")
    print(f"  null_fy     : {checks['null_fy']:,}")
    print(f"  holiday rows: {checks['holiday_rows']:,}  "
          f"(fixed-date only — floating holidays back-filled via Silver)")
    print(f"  fy_end rows : {checks['fy_end_rows']:,}  "
          f"(expected 41 for FY2000–FY2040 inclusive)")

    assert checks["null_fy"] == 0, \
        "ASSERT FAILED: federal_fiscal_fy must be non-null for every row"
    assert checks["fy_end_rows"] == 41, (
        f"ASSERT FAILED: expected 41 Sep-30 rows (FY2000–FY2040), "
        f"got {checks['fy_end_rows']}"
    )

    audit_success(spark, RUN_ID, TARGET, n, n, start_ts)

except Exception as e:
    import traceback
    audit_fail(spark, RUN_ID, TARGET, str(e), traceback.format_exc(), start_ts)
    raise

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
