import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tfl-lakehouse-tests")
        .config("spark.sql.ansi.enabled", "true")      # match Databricks serverless
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Europe/London")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


NEW_HEADER = [
    "Number", "Start date", "Start station number", "Start station", "End date",
    "End station number", "End station", "Bike number", "Bike model",
    "Total duration", "Total duration (ms)",
]


def journey(n, start="2026-06-01 08:00", end="2026-06-01 08:20", s_id="001001", s_name="Waterloo",
            e_id="001002", e_name="Holborn", bike="12345", model="CLASSIC", ms="1200000"):
    return (str(n), start, s_id, s_name, end, e_id, e_name, bike, model, "20m 0s", ms)


@pytest.fixture
def make_bronze(spark):
    """Build a bronze-shaped DataFrame from raw TfL-style tuples (new layout)."""
    from lakehouse.transforms import add_ingestion_metadata, normalise_column_names

    def _make(rows):
        df = spark.createDataFrame(rows, NEW_HEADER)
        return normalise_column_names(add_ingestion_metadata(df))

    return _make
