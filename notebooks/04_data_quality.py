# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality gate
# MAGIC Runs 14 checks across silver and gold, writes every result to `tfl_ops.dq_results`,
# MAGIC logs the run to `tfl_ops.pipeline_runs`, and **fails the job** if any critical check fails,
# MAGIC so a bad load never reaches the dashboard export.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")
dbutils.widgets.text("run_id", "manual", "Run id (set by job)")
dbutils.widgets.text("freshness_days", "120", "Freshness threshold (days)")

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import Row

from lakehouse.config import (GOLD_BIKE_MODEL, GOLD_DAILY, GOLD_HOURLY, GOLD_STATION_DAILY,
                              OPS_DQ_RESULTS, OPS_RUN_LOG, SILVER_JOURNEYS, SILVER_QUARANTINE,
                              from_widgets)
from lakehouse.quality import assert_no_critical_failures, gold_checks, silver_checks, summarise

cfg = from_widgets(dbutils)
run_id = dbutils.widgets.get("run_id") or "manual"
freshness_days = int(dbutils.widgets.get("freshness_days") or 120)

# COMMAND ----------

silver = spark.table(cfg.table("silver", SILVER_JOURNEYS))
silver_rows = silver.count()
quarantined = spark.table(cfg.table("silver", SILVER_QUARANTINE)).count()

results = silver_checks(silver, quarantined, table=cfg.table("silver", SILVER_JOURNEYS),
                        freshness_days=freshness_days)
results += gold_checks(
    silver_rows,
    spark.table(cfg.table("gold", GOLD_DAILY)),
    spark.table(cfg.table("gold", GOLD_HOURLY)),
    spark.table(cfg.table("gold", GOLD_STATION_DAILY)),
    spark.table(cfg.table("gold", GOLD_BIKE_MODEL)),
)
for r in results:
    r["run_id"] = run_id

# COMMAND ----------

results_df = spark.createDataFrame([Row(**r) for r in results]).select(
    "run_id", "check_name", "layer", "table_name", "status", "severity",
    "observed", "expected", "details", "checked_at",
)
results_df.write.mode("append").saveAsTable(cfg.table("ops", OPS_DQ_RESULTS))
display(results_df)

# COMMAND ----------

s = summarise(results)
status = "FAILED" if s["critical_failures"] else ("PASSED_WITH_WARNINGS" if s["warnings"] else "PASSED")
spark.createDataFrame([Row(
    run_id=run_id, silver_rows=silver_rows, quarantined_rows=quarantined,
    checks_total=s["total"], checks_passed=s["passed"],
    critical_failures=",".join(s["critical_failures"]), warnings=",".join(s["warnings"]),
    status=status, finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
)]).write.mode("append").saveAsTable(cfg.table("ops", OPS_RUN_LOG))

print(f"{s['passed']}/{s['total']} checks passed. Status: {status}")

# COMMAND ----------

# Stops the job here on any critical failure (export task will not run)
assert_no_critical_failures(results)
