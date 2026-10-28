# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Export dashboard data
# MAGIC Writes one file, `dashboard_data.json`, from the gold tables and the latest quality run.
# MAGIC Download it from Catalog Explorer and commit it to `docs/data/` to refresh the live GitHub Pages dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")
dbutils.widgets.text("run_id", "manual", "Run id (set by job)")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from lakehouse.config import (GOLD_BIKE_MODEL, GOLD_DAILY, GOLD_HOURLY, GOLD_STATION_DAILY,
                              GOLD_TOP_ROUTES, OPS_DQ_RESULTS, SILVER_QUARANTINE, from_widgets)
from lakehouse.export import build_dashboard_payload, to_json

cfg = from_widgets(dbutils)
run_id = dbutils.widgets.get("run_id") or "manual"

# COMMAND ----------

dq = spark.table(cfg.table("ops", OPS_DQ_RESULTS))
latest_run = dq.orderBy(F.col("checked_at").desc()).select("run_id").first()
dq_rows = [r.asDict() for r in dq.filter(F.col("run_id") == latest_run["run_id"]).collect()] if latest_run else []

payload = build_dashboard_payload(
    daily=spark.table(cfg.table("gold", GOLD_DAILY)),
    hourly=spark.table(cfg.table("gold", GOLD_HOURLY)),
    station_flow=spark.table(cfg.table("gold", GOLD_STATION_DAILY)),
    routes=spark.table(cfg.table("gold", GOLD_TOP_ROUTES)),
    bike_mix=spark.table(cfg.table("gold", GOLD_BIKE_MODEL)),
    quarantined_rows=spark.table(cfg.table("silver", SILVER_QUARANTINE)).count(),
    dq_results=dq_rows,
)

target = f"{cfg.export_path}/dashboard_data.json"
dbutils.fs.put(target, to_json(payload), overwrite=True)
print("written:", target)
print(payload["meta"])
