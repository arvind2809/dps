# Databricks notebook source
# =============================================================================
# wpr/prime_dim_org_hierarchy.py
# Primes assist_dev.wpr.dim_org_hierarchy
#
# Strategy : TRUNCATE → INSERT  (fully idempotent, SCD Type 1)
# Grain    : One row per lu_emp_orgs.emp_org_id  (natural key)
#
# Source tables (Silver):
#   silver_assist_lu_emp_orgs      →  org name, code, tier (depth level)
#   silver_assist_lu_emp_org_xref  →  parent-child adjacency: emp_org_id→child
#
# IMPROVEMENT I-DP8-2 — tier_id for org_level; recursive CTE for org_path (built-in):
#   lu_emp_orgs.tier_id is the hierarchy depth level (1=root, 2=division,
#   3=branch) and is used directly for Gold org_level. This is simpler and
#   more reliable than computing depth via recursive traversal.
#
#   org_path is computed via a Spark SQL RECURSIVE CTE over lu_emp_org_xref:
#     Anchor : root nodes — orgs whose emp_org_id does NOT appear as any
#              emp_org_child_id in xref (i.e., no parent).
#     Recursive: join child org to parent, prepend parent path.
#   Result: '/Root/Division/Branch' style path string with '/' separator.
#
#   parent_org_id: direct LEFT JOIN on lu_emp_org_xref WHERE
#     emp_org_child_id = this org's emp_org_id. One parent per org; NULL
#     for root nodes (not present as a child in xref).
#
#   Post-load checks (IMPROVEMENT I-DP8-2):
#     - org_level distribution (rows per tier)
#     - max org_level (sanity check for unexpected deep hierarchies)
#     - circular reference guard: count rows where org_id = parent_org_id
#
# Field mapping (all confirmed against Silver DDL):
#   org_id        ← lu_emp_orgs.emp_org_id           (NK)
#   org_code      ← lu_emp_orgs.emp_org_short_name
#   org_name      ← lu_emp_orgs.emp_org_name
#   parent_org_id ← lu_emp_org_xref.emp_org_id WHERE xref.emp_org_child_id = org_id
#   org_level     ← lu_emp_orgs.tier_id              (direct — I-DP8-2)
#   org_path      ← Recursive CTE result             (I-DP8-2)
#   region_cd     ← CAST(NULL AS STRING)             [SILVER GAP]
#   is_active_flag← CAST(NULL AS BOOLEAN)            [SILVER GAP]
#
# Ref: GSA org hierarchy, FAR 1.602 (contracting office assignment)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../../../utils/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline Run ID")
dbutils.widgets.text("env",    "dev", "Environment")

RUN_ID       = dbutils.widgets.get("run_id")
ENV          = dbutils.widgets.get("env")
PRODUCT      = "DP8"
NOTEBOOK     = "prime_dim_org_hierarchy"
TARGET_TABLE = "assist_dev.wpr.dim_org_hierarchy"

SILVER = "assist_dev.assist_finance"

# COMMAND ----------

#start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE, run_type="FULL_PRIME")
start_ts = audit_start(spark, RUN_ID, PRODUCT, NOTEBOOK, TARGET_TABLE,
                        source_schema="aasbs", source_table="silver_assist_lu_emp_orgs")
print(f"[{NOTEBOOK}] Starting — run_id={RUN_ID}, target={TARGET_TABLE}")

try:

    # ─────────────────────────────────────────────────────────────────────
    # Step 1 — TRUNCATE
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"TRUNCATE TABLE {TARGET_TABLE}")
    print(f"[{NOTEBOOK}] Truncated {TARGET_TABLE}")

    rows_read = spark.sql(f"""
        SELECT COUNT(*)
        FROM {SILVER}.silver_assist_lu_emp_orgs
        WHERE COALESCE(is_deleted, FALSE) = FALSE
    """).collect()[0][0]
    print(f"[{NOTEBOOK}] Source lu_emp_orgs rows: {rows_read:,}")

    # ─────────────────────────────────────────────────────────────────────
    # Step 2 — Pre-aggregate parent_org_id per org
    # lu_emp_org_xref: emp_org_id = parent, emp_org_child_id = child
    # Each child has at most one parent. Left join to xref to find parent.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_parent_per_org AS
        SELECT
            o.emp_org_id,
            x.emp_org_id   AS parent_org_id   -- NULL for root nodes
        FROM {SILVER}.silver_assist_lu_emp_orgs o
        LEFT JOIN {SILVER}.silver_assist_lu_emp_org_xref x
            ON  x.emp_org_child_id = o.emp_org_id
            AND COALESCE(x.is_deleted, FALSE) = FALSE
        WHERE COALESCE(o.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3 — IMPROVEMENT I-DP8-2: recursive CTE for org_path
    # Builds the '/Root/Division/Branch' path by traversing the hierarchy
    # bottom-up from the anchor (root nodes) to leaves.
    #
    # Anchor: orgs whose emp_org_id is NOT in xref.emp_org_child_id set
    #   (i.e. they have no parent — root nodes)
    # Recursive step: join each child to its parent path + append own name
    # Maximum depth guard: MAXRECURSION is implicit in Spark; tier_id
    #   provides an independent depth check.
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_org_paths AS
        WITH RECURSIVE org_tree AS (
            -- Anchor: root nodes — not present as emp_org_child_id in xref
            SELECT
                o.emp_org_id,
                o.emp_org_name                  AS org_path
            FROM {SILVER}.silver_assist_lu_emp_orgs o
            WHERE COALESCE(o.is_deleted, FALSE) = FALSE
              AND o.emp_org_id NOT IN (
                  SELECT emp_org_child_id
                  FROM {SILVER}.silver_assist_lu_emp_org_xref
                  WHERE COALESCE(is_deleted, FALSE) = FALSE
                    AND emp_org_child_id IS NOT NULL
              )
            UNION ALL
            -- Recursive: append child org name to parent path
            SELECT
                x.emp_org_child_id              AS emp_org_id,
                CONCAT(ot.org_path, ' / ', child.emp_org_name) AS org_path
            FROM org_tree ot
            JOIN {SILVER}.silver_assist_lu_emp_org_xref x
                ON  x.emp_org_id = ot.emp_org_id
                AND COALESCE(x.is_deleted, FALSE) = FALSE
            JOIN {SILVER}.silver_assist_lu_emp_orgs child
                ON  child.emp_org_id = x.emp_org_child_id
                AND COALESCE(child.is_deleted, FALSE) = FALSE
        )
        SELECT emp_org_id, org_path FROM org_tree
    """)
    print(f"[{NOTEBOOK}] Recursive CTE org_paths view created.")

    # ─────────────────────────────────────────────────────────────────────
    # Step 4 — INSERT
    # ─────────────────────────────────────────────────────────────────────
    spark.sql(f"""
        INSERT INTO {TARGET_TABLE}
        (
            org_id,
            org_code,
            org_name,
            parent_org_id,
            org_level,
            org_path,
            region_cd,
            is_active_flag,
            _gold_created_at,
            _gold_updated_at,
            _source_batch_id
        )
        SELECT
            -- Natural key: org hierarchy node ID
            o.emp_org_id                                        AS org_id,

            -- org_code: short name is closest to a code value in Silver
            coalesce(o.emp_org_short_name, o.emp_org_name )                               AS org_code,

            -- Full org name
            o.emp_org_name                                      AS org_name,

            -- Parent org ID from adjacency table; NULL for root nodes
            p.parent_org_id,

            -- IMPROVEMENT I-DP8-2: org_level = tier_id (direct — hierarchy depth)
            -- tier_id is already the depth level (1=root, 2=division, 3=branch).
            -- Using it directly avoids recursive traversal for this field.
            CAST(o.tier_id AS INT)                              AS org_level,

            -- IMPROVEMENT I-DP8-2: org_path from recursive CTE
            -- '/Root/Division/Branch' path built via lu_emp_org_xref traversal
            op.org_path,

            -- SILVER GAP: region_cd — no region column on lu_emp_orgs.
            -- gsa_org_id is present but no confirmed region mapping exists.
            CAST(NULL AS STRING)                                AS region_cd,

            -- SILVER GAP: is_active_flag — no active_yn column on lu_emp_orgs.
            CAST(NULL AS BOOLEAN)                               AS is_active_flag,

            current_timestamp()                                 AS _gold_created_at,
            current_timestamp()                                 AS _gold_updated_at,
            '{RUN_ID}'                                          AS _source_batch_id

        FROM {SILVER}.silver_assist_lu_emp_orgs o
        LEFT JOIN v_parent_per_org p
            ON  p.emp_org_id = o.emp_org_id
        LEFT JOIN v_org_paths op
            ON  op.emp_org_id = o.emp_org_id
        WHERE COALESCE(o.is_deleted, FALSE) = FALSE
    """)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5 — Post-load checks
    # IMPROVEMENT I-DP8-2: hierarchy validation
    # ─────────────────────────────────────────────────────────────────────
    rows_written = spark.sql(
        f"SELECT COUNT(*) FROM {TARGET_TABLE}"
    ).collect()[0][0]
    print(f"[{NOTEBOOK}] Inserted {rows_written:,} rows into {TARGET_TABLE}")

    # org_level distribution
    level_dist = spark.sql(f"""
        SELECT org_level, COUNT(*) AS cnt
        FROM {TARGET_TABLE}
        GROUP BY org_level ORDER BY org_level
    """).collect()
    print(f"\n[{NOTEBOOK}] IMPROVEMENT I-DP8-2 — org_level distribution:")
    for row in level_dist:
        label = {1:"root", 2:"division", 3:"branch"}.get(row[0], "level")
        print(f"  tier {row[0]} ({label:<10}): {row[1]:>6,} orgs")

    stats = spark.sql(f"""
        SELECT
            MAX(org_level)                                                        AS max_level,
            SUM(CASE WHEN parent_org_id IS NULL      THEN 1 ELSE 0 END)         AS root_count,
            SUM(CASE WHEN org_path IS NULL            THEN 1 ELSE 0 END)        AS no_path,
            -- Circular reference guard: org_id must not equal parent_org_id
            SUM(CASE WHEN org_id = parent_org_id     THEN 1 ELSE 0 END)         AS circular_self
        FROM {TARGET_TABLE}
    """).collect()[0]

    print(
        f"  max_level={stats[0]} | root_nodes={stats[1]:,} | "
        f"no_path={stats[2]:,}"
    )

    # Circular reference guard
    if stats[3] > 0:
        print(
            f"  ⚠ WARNING: {stats[3]} org(s) where org_id = parent_org_id "
            f"(circular self-reference). Investigate lu_emp_org_xref data quality."
        )
    else:
        print(f"  ✓ No self-referential circular references detected.")

    if stats[0] and stats[0] > 6:
        print(
            f"  ⚠ WARNING: max org_level={stats[0]} exceeds expected depth of 3. "
            f"Verify lu_emp_org_xref for unexpected deep hierarchies."
        )

    #audit_success(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, rows_read, rows_written)
    audit_success(spark, RUN_ID, TARGET_TABLE, rows_read, rows_written, start_ts)
    print(f"\n[{NOTEBOOK}] Completed successfully.")
    dbutils.notebook.exit("SUCCESS")

except Exception as e:
    err = str(e)
    print(f"[{NOTEBOOK}] FAILED: {err}")
    #audit_failure(spark, RUN_ID, NOTEBOOK, TARGET_TABLE, start_ts, err)
    audit_fail(spark, RUN_ID, TARGET_TABLE, str(e), traceback.format_exc(), start_ts)
    raise
