# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: Unity Catalog objects
# MAGIC Creates the schemas, landing volume, folders and ops tables the pipeline needs.
# MAGIC Safe to re-run: every statement is `IF NOT EXISTS`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.config import OPS_DQ_RESULTS, OPS_RUN_LOG, from_widgets

cfg = from_widgets(dbutils)
print(cfg)

# COMMAND ----------

# MAGIC %md ## 1. Catalog and schemas

# COMMAND ----------

if cfg.catalog != "workspace":
    # Free Edition gives you the 'workspace' catalog. A separate catalog needs CREATE CATALOG rights.
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")

schemas = {
    cfg.bronze_schema: "Raw TfL files landed as-is, all columns as strings, plus lineage metadata.",
    cfg.silver_schema: "Cleaned, typed, validated and de-duplicated journeys and stations.",
    cfg.gold_schema: "Business-ready aggregates for dashboards and analysis.",
    cfg.ops_schema: "Pipeline operations: data quality results and run log.",
}
for schema, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema} COMMENT '{comment}'")
    print(f"schema ready: {cfg.catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## 2. Landing storage

# COMMAND ----------

if cfg.storage_mode == "volume":
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}.{cfg.volume_name} "
        "COMMENT 'Landing zone for raw TfL journey CSVs and BikePoint JSON'"
    )
else:
    # adls mode: the external location must already exist (see SETUP_GUIDE Part 12).
    print("adls mode: using external location at", cfg.landing_root)

for path in [cfg.raw_journeys_path, cfg.raw_stations_path, cfg.checkpoint_root, cfg.schema_root, cfg.export_path]:
    dbutils.fs.mkdirs(path)
    print("folder ready:", path)

# COMMAND ----------

# MAGIC %md ## 3. Ops tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.table('ops', OPS_DQ_RESULTS)} (
  run_id STRING, check_name STRING, layer STRING, table_name STRING,
  status STRING, severity STRING, observed STRING, expected STRING,
  details STRING, checked_at TIMESTAMP
) COMMENT 'One row per data quality check per pipeline run'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.table('ops', OPS_RUN_LOG)} (
  run_id STRING, silver_rows BIGINT, quarantined_rows BIGINT,
  checks_total INT, checks_passed INT, critical_failures STRING,
  warnings STRING, status STRING, finished_at TIMESTAMP
) COMMENT 'One row per pipeline run with headline counts and outcome'
""")

print("Setup complete.")
print("Next: upload TfL CSVs to", cfg.raw_journeys_path)
