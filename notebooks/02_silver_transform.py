# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: clean, type, validate, de-duplicate
# MAGIC 1. Standardise both TfL file layouts onto one typed schema (ANSI-safe parsing).
# MAGIC 2. Derive analysis columns (date, hour, ISO weekday, weekend flag, round-trip flag).
# MAGIC 3. Apply validation rules. Failing rows go to a **quarantine** table with a reason, not the bin.
# MAGIC 4. De-duplicate on `rental_id` and **MERGE** into silver, so re-runs and re-uploads are idempotent.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.config import (BRONZE_JOURNEYS, BRONZE_STATIONS, SILVER_JOURNEYS,
                              SILVER_QUARANTINE, SILVER_STATIONS, from_widgets)
from lakehouse.transforms import bronze_to_silver, stations_to_silver

cfg = from_widgets(dbutils)
silver_table = cfg.table("silver", SILVER_JOURNEYS)
quarantine_table = cfg.table("silver", SILVER_QUARANTINE)

# COMMAND ----------

# MAGIC %md ## 1. Transform

# COMMAND ----------

bronze = spark.table(cfg.table("bronze", BRONZE_JOURNEYS))
silver_df, quarantine_df = bronze_to_silver(bronze)

# COMMAND ----------

# MAGIC %md ## 2. MERGE into silver (upsert on rental_id)

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    silver_df.limit(0).write.saveAsTable(silver_table)
    # Liquid clustering on the column most queries filter by
    spark.sql(f"ALTER TABLE {silver_table} CLUSTER BY (journey_date)")
    print("created", silver_table)

(
    DeltaTable.forName(spark, silver_table).alias("t")
    .merge(silver_df.alias("s"), "t.rental_id = s.rental_id")
    .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
    .whenNotMatchedInsertAll()
    .execute()
)

last_op = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").select("operationMetrics").collect()[0][0]
print("MERGE metrics:", {k: last_op.get(k) for k in ["numTargetRowsInserted", "numTargetRowsUpdated", "numSourceRows"]})

# COMMAND ----------

# MAGIC %md ## 3. Quarantine (rebuilt each run from the full bronze table)

# COMMAND ----------

quarantine_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)

q = spark.table(quarantine_table)
print("quarantined rows:", q.count())
display(q.groupBy("dq_reason").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md ## 4. Stations dimension (optional)

# COMMAND ----------

if spark.catalog.tableExists(cfg.table("bronze", BRONZE_STATIONS)):
    stations = stations_to_silver(spark.table(cfg.table("bronze", BRONZE_STATIONS)))
    stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("silver", SILVER_STATIONS))
    print("silver stations:", spark.table(cfg.table("silver", SILVER_STATIONS)).count())
else:
    print("No bronze stations table. Skipping.")

# COMMAND ----------

print("silver journeys rows:", spark.table(silver_table).count())
display(spark.table(silver_table).limit(10))
