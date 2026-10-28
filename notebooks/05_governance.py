# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Governance with Unity Catalog
# MAGIC * Table and column **comments** (show up in Catalog Explorer and AI search).
# MAGIC * **Tags** for layer, domain and PII classification.
# MAGIC * **Grants**: an analyst group can read gold only, never bronze or silver.
# MAGIC * **Lineage** needs no code: Unity Catalog records it from the reads and writes in notebooks 01 to 03.
# MAGIC   Open any gold table in Catalog Explorer, then the *Lineage* tab.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")
dbutils.widgets.text("analyst_group", "account users", "Group to grant gold read access")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.config import (BRONZE_JOURNEYS, GOLD_BIKE_MODEL, GOLD_DAILY, GOLD_HOURLY,
                              GOLD_STATION_DAILY, GOLD_TOP_ROUTES, SILVER_JOURNEYS,
                              SILVER_QUARANTINE, from_widgets)

cfg = from_widgets(dbutils)
group = dbutils.widgets.get("analyst_group")


def safe_sql(statement: str) -> None:
    """Run a governance statement; report rather than fail if the edition does not allow it."""
    try:
        spark.sql(statement)
        print("OK   ", statement.split("\n")[0][:110])
    except Exception as e:
        print("SKIP ", statement.split("\n")[0][:110], "|", str(e).split("\n")[0][:120])

# COMMAND ----------

# MAGIC %md ## 1. Table comments and tags

# COMMAND ----------

tables = {
    ("bronze", BRONZE_JOURNEYS): ("Raw TfL Santander Cycles journey files, one row per CSV line, strings only.", "bronze"),
    ("silver", SILVER_JOURNEYS): ("One row per valid journey. Typed, validated, de-duplicated on rental_id.", "silver"),
    ("silver", SILVER_QUARANTINE): ("Journeys that failed validation, with dq_reason explaining why.", "silver"),
    ("gold", GOLD_DAILY): ("Daily journey KPIs: trips, durations, e-bike share, active stations and bikes.", "gold"),
    ("gold", GOLD_HOURLY): ("Demand by ISO weekday and hour, with average trips per observed day.", "gold"),
    ("gold", GOLD_STATION_DAILY): ("Departures, arrivals and net flow per station per day, with rebalancing signal.", "gold"),
    ("gold", GOLD_TOP_ROUTES): ("Top 50 station-to-station routes by trips (round trips excluded).", "gold"),
    ("gold", GOLD_BIKE_MODEL): ("Trips, share and average duration by bike model (classic vs e-bike).", "gold"),
}

for (layer, name), (comment, tag_layer) in tables.items():
    t = cfg.table(layer, name)
    if not spark.catalog.tableExists(t):
        print("missing, skipped:", t)
        continue
    safe_sql(f"COMMENT ON TABLE {t} IS '{comment}'")
    safe_sql(f"ALTER TABLE {t} SET TAGS ('layer' = '{tag_layer}', 'domain' = 'mobility', "
             f"'source' = 'tfl_open_data', 'pii' = 'none')")

# COMMAND ----------

# MAGIC %md ## 2. Column comments on silver

# COMMAND ----------

silver = cfg.table("silver", SILVER_JOURNEYS)
column_docs = {
    "rental_id": "TfL journey identifier. Primary key.",
    "start_ts": "Journey start, local London time.",
    "end_ts": "Journey end, local London time.",
    "duration_s": "Journey duration in seconds, from TfL source (ms / 1000).",
    "day_of_week": "ISO weekday, 1 = Monday, 7 = Sunday.",
    "is_round_trip": "True when the bike was returned to the station it left from.",
    "bike_model": "CLASSIC, EBIKE or UNKNOWN (older files do not record model).",
    "_source_file": "Landing file the row came from (lineage).",
    "_ingested_at": "When bronze ingested the row.",
}
for col, doc in column_docs.items():
    safe_sql(f"ALTER TABLE {silver} ALTER COLUMN {col} COMMENT '{doc}'")

# COMMAND ----------

# MAGIC %md ## 3. Access model: analysts read gold only

# COMMAND ----------

safe_sql(f"GRANT USE CATALOG ON CATALOG {cfg.catalog} TO `{group}`")
safe_sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {cfg.catalog}.{cfg.gold_schema} TO `{group}`")
# bronze and silver: no grants to analysts, engineers only
safe_sql(f"REVOKE SELECT ON SCHEMA {cfg.catalog}.{cfg.silver_schema} FROM `{group}`")
safe_sql(f"REVOKE SELECT ON SCHEMA {cfg.catalog}.{cfg.bronze_schema} FROM `{group}`")

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {cfg.catalog}.{cfg.gold_schema}"))
