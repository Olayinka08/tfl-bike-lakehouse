# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: business-ready aggregates
# MAGIC | Table | Business question |
# MAGIC |---|---|
# MAGIC | `daily_summary` | How many journeys per day, how long, how many e-bikes? |
# MAGIC | `hourly_demand` | When is demand highest (commute peaks vs weekend leisure)? |
# MAGIC | `station_daily_flow` | Which docks run empty or overflow (rebalancing signal)? |
# MAGIC | `top_routes` | Which station-to-station routes are busiest? |
# MAGIC | `bike_model_mix` | How do classic bikes and e-bikes compare? |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("storage_mode", "volume", ["volume", "adls"], "Storage mode")
dbutils.widgets.text("adls_account", "", "ADLS account (adls mode)")
dbutils.widgets.text("adls_container", "", "ADLS container (adls mode)")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.config import (GOLD_BIKE_MODEL, GOLD_DAILY, GOLD_HOURLY, GOLD_STATION_DAILY,
                              GOLD_TOP_ROUTES, SILVER_JOURNEYS, SILVER_STATIONS, from_widgets)
from lakehouse.transforms import (enrich_with_stations, gold_bike_model_mix, gold_daily_summary,
                                  gold_hourly_demand, gold_station_daily_flow, gold_top_routes)

cfg = from_widgets(dbutils)
silver = spark.table(cfg.table("silver", SILVER_JOURNEYS))

# COMMAND ----------

station_flow = gold_station_daily_flow(silver)
if spark.catalog.tableExists(cfg.table("silver", SILVER_STATIONS)):
    station_flow = enrich_with_stations(station_flow, spark.table(cfg.table("silver", SILVER_STATIONS)))
    print("station_daily_flow enriched with coordinates and dock counts")

gold_tables = {
    GOLD_DAILY: gold_daily_summary(silver),
    GOLD_HOURLY: gold_hourly_demand(silver),
    GOLD_STATION_DAILY: station_flow,
    GOLD_TOP_ROUTES: gold_top_routes(silver),
    GOLD_BIKE_MODEL: gold_bike_model_mix(silver),
}

for name, df in gold_tables.items():
    target = cfg.table("gold", name)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {spark.table(target).count():,} rows")

# COMMAND ----------

# MAGIC %md ## Preview

# COMMAND ----------

display(spark.table(cfg.table("gold", GOLD_DAILY)))

# COMMAND ----------

display(spark.table(cfg.table("gold", GOLD_TOP_ROUTES)).orderBy("route_rank").limit(10))
