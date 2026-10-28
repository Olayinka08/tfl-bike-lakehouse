# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: incremental ingestion with Auto Loader
# MAGIC * **Journeys:** TfL usage CSVs picked up incrementally by Auto Loader (`cloudFiles`).
# MAGIC   Each file is processed exactly once; the checkpoint remembers what has been loaded.
# MAGIC * **Stations (optional):** TfL BikePoint API JSON, reloaded in full each run.
# MAGIC
# MAGIC Bronze keeps every value as a string. Typing and validation happen in silver,
# MAGIC so a malformed file never blocks ingestion.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from lakehouse.config import BRONZE_JOURNEYS, BRONZE_STATIONS, from_widgets
from lakehouse.transforms import add_ingestion_metadata, normalise_column_names

cfg = from_widgets(dbutils)

# COMMAND ----------

# MAGIC %md ## 1. Journeys (Auto Loader, availableNow trigger)

# COMMAND ----------

files = [f for f in dbutils.fs.ls(cfg.raw_journeys_path) if f.name.lower().endswith(".csv")]
print(f"{len(files)} CSV file(s) in landing:")
for f in files:
    print(f"  {f.name}  ({f.size / 1_048_576:.1f} MB)")
if not files:
    raise FileNotFoundError(f"No CSV files in {cfg.raw_journeys_path}. Upload TfL usage files first.")

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "false")  # keep raw strings in bronze
    .option("cloudFiles.schemaLocation", f"{cfg.schema_root}/journeys")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", "*.csv")
    .load(cfg.raw_journeys_path)
)

# metadata first (needs the hidden _metadata column), then clean column names
bronze = normalise_column_names(add_ingestion_metadata(raw, F.col("_metadata.file_path")))

query = (
    bronze.writeStream.option("checkpointLocation", f"{cfg.checkpoint_root}/bronze_journeys")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # process everything new, then stop (batch-style streaming)
    .toTable(cfg.table("bronze", BRONZE_JOURNEYS))
)
query.awaitTermination()

# COMMAND ----------

bronze_table = cfg.table("bronze", BRONZE_JOURNEYS)
summary = spark.table(bronze_table).groupBy("_source_file").agg(
    F.count("*").alias("rows"), F.max("_ingested_at").alias("ingested_at")
).orderBy("_source_file")
display(summary)
print("bronze journeys rows:", spark.table(bronze_table).count())

# COMMAND ----------

# MAGIC %md ## 2. Stations (optional BikePoint JSON)

# COMMAND ----------

try:
    station_files = [f for f in dbutils.fs.ls(cfg.raw_stations_path) if f.name.lower().endswith(".json")]
except Exception:
    station_files = []

if station_files:
    stations_raw = spark.read.option("multiLine", "true").json(cfg.raw_stations_path)
    stations_raw = add_ingestion_metadata(stations_raw, F.col("_metadata.file_path"))
    (
        stations_raw.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.table("bronze", BRONZE_STATIONS))
    )
    print("bronze stations rows:", spark.table(cfg.table("bronze", BRONZE_STATIONS)).count())
else:
    print("No BikePoint JSON found. Skipping stations (pipeline still works without it).")
