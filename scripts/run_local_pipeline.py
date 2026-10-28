"""Run the full Bronze -> Silver -> Gold -> DQ -> export pipeline locally with PySpark.

Same transformation code as the Databricks notebooks, no Delta or cloud needed.
Useful for a quick end-to-end check and for producing a dashboard file.

Usage:
    python scripts/run_local_pipeline.py --input data/sample --out docs/data/dashboard_data.json
    python scripts/run_local_pipeline.py --input data/tfl --label "TfL Santander Cycles open data"
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.export import build_dashboard_payload, to_json
from lakehouse.quality import gold_checks, silver_checks, summarise
from lakehouse.transforms import (
    add_ingestion_metadata,
    bronze_to_silver,
    gold_bike_model_mix,
    gold_daily_summary,
    gold_hourly_demand,
    gold_station_daily_flow,
    gold_top_routes,
    normalise_column_names,
)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/sample")
    p.add_argument("--out", default="docs/data/dashboard_data.json")
    p.add_argument("--label", default="Synthetic sample in TfL format (replace with real export)")
    a = p.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("tfl-lakehouse-local")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.option("header", "true").csv(os.path.join(a.input, "*.csv"))
    bronze = normalise_column_names(add_ingestion_metadata(raw, F.input_file_name()))
    print(f"bronze rows:      {bronze.count():,}")

    silver, quarantine = bronze_to_silver(bronze)
    silver, quarantine = silver.cache(), quarantine.cache()
    silver_rows, q_rows = silver.count(), quarantine.count()
    print(f"silver rows:      {silver_rows:,}")
    print(f"quarantined rows: {q_rows:,}")

    daily, hourly = gold_daily_summary(silver), gold_hourly_demand(silver)
    flow, routes, mix = gold_station_daily_flow(silver), gold_top_routes(silver), gold_bike_model_mix(silver)

    results = silver_checks(silver, q_rows, freshness_days=100000)
    results += gold_checks(silver_rows, daily, hourly, flow, mix)
    s = summarise(results)
    print(f"quality:          {s['passed']}/{s['total']} passed, critical failures: {s['critical_failures'] or 'none'}")

    payload = build_dashboard_payload(daily, hourly, flow, routes, mix, q_rows, results, a.label)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(to_json(payload))
    print("dashboard data:  ", a.out)
    spark.stop()
    return 1 if s["critical_failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
