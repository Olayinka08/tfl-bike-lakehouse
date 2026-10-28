"""Pure PySpark transformations for every medallion layer.

Rules for this module:
* Functions take DataFrames and return DataFrames. No reads, no writes.
* Everything is ANSI-safe (Databricks serverless runs with ANSI mode on),
  so parsing uses try_cast / try_to_timestamp instead of plain casts.
* The same functions are unit tested locally in /tests.
"""

import re
from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MAX_DURATION_SECONDS = 24 * 60 * 60  # journeys over 24h are treated as invalid
TIMESTAMP_FORMATS = ["yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm"]


# ---------------------------------------------------------------- bronze ----
def snake_case(name: str) -> str:
    """'Total duration (ms)' -> 'total_duration_ms', 'EndStation Id' -> 'endstation_id'."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return cleaned or "unnamed"


def normalise_column_names(df: DataFrame) -> DataFrame:
    """Delta tables cannot hold spaces or brackets in column names by default.

    Columns starting with '_' are pipeline metadata (_source_file, _ingested_at,
    _rescued_data) and are left untouched.
    """
    return df.toDF(*[c if c.startswith("_") else snake_case(c) for c in df.columns])


def add_ingestion_metadata(df: DataFrame, source_file_col=None) -> DataFrame:
    """Lineage columns added at landing. On Databricks pass F.col('_metadata.file_path')."""
    source = source_file_col if source_file_col is not None else F.lit("local")
    return df.withColumn("_source_file", source).withColumn(
        "_ingested_at", F.current_timestamp()
    )


# ---------------------------------------------------------------- silver ----
def _col_or_null(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def _first_present(df: DataFrame, *names):
    return F.coalesce(*[_col_or_null(df, n) for n in names])


def _parse_ts(column):
    return F.coalesce(*[F.try_to_timestamp(column, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def _try_cast(column, dtype: str):
    return F.expr(f"try_cast({column} as {dtype})")


def standardise_journeys(bronze: DataFrame) -> DataFrame:
    """Map both TfL file layouts (pre-2022 and current) onto one typed schema."""
    df = bronze.select(
        _first_present(bronze, "number", "rental_id").alias("_rental_id"),
        _first_present(bronze, "start_date").alias("_start"),
        _first_present(bronze, "end_date").alias("_end"),
        _first_present(bronze, "start_station_number", "startstation_id").alias("start_station_id"),
        _first_present(bronze, "start_station", "startstation_name").alias("start_station_name"),
        _first_present(bronze, "end_station_number", "endstation_id").alias("end_station_id"),
        _first_present(bronze, "end_station", "endstation_name").alias("end_station_name"),
        _first_present(bronze, "bike_number", "bike_id").alias("bike_id"),
        _col_or_null(bronze, "bike_model").alias("_bike_model"),
        _col_or_null(bronze, "total_duration_ms").alias("_duration_ms"),
        _col_or_null(bronze, "duration").alias("_duration_s"),
        _col_or_null(bronze, "_source_file").alias("_source_file"),
        (F.col("_ingested_at") if "_ingested_at" in bronze.columns
         else F.current_timestamp()).alias("_ingested_at"),
    )

    df = (
        df.withColumn("rental_id", _try_cast("_rental_id", "bigint"))
        .withColumn("start_ts", _parse_ts(F.col("_start")))
        .withColumn("end_ts", _parse_ts(F.col("_end")))
        .withColumn(
            "duration_s",
            F.coalesce(
                (_try_cast("_duration_ms", "bigint") / 1000).cast("bigint"),
                _try_cast("_duration_s", "bigint"),
                (F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts")),
            ),
        )
        .withColumn("start_station_id", F.trim("start_station_id"))
        .withColumn("end_station_id", F.trim("end_station_id"))
        .withColumn("start_station_name", F.trim("start_station_name"))
        .withColumn("end_station_name", F.trim("end_station_name"))
        .withColumn("bike_id", F.trim("bike_id"))
        .withColumn(
            "bike_model",
            F.when(F.upper(F.trim("_bike_model")).isin("PBSC_EBIKE", "EBIKE", "E-BIKE"), "EBIKE")
            .when(F.upper(F.trim("_bike_model")) == "CLASSIC", "CLASSIC")
            .when(F.col("_bike_model").isNull() | (F.trim("_bike_model") == ""), "UNKNOWN")
            .otherwise(F.upper(F.trim("_bike_model"))),
        )
    )
    return df.drop("_rental_id", "_start", "_end", "_bike_model", "_duration_ms", "_duration_s")


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("journey_date", F.to_date("start_ts"))
        .withColumn("start_hour", F.hour("start_ts"))
        # dayofweek: 1=Sunday..7=Saturday -> convert to ISO 1=Monday..7=Sunday
        .withColumn("day_of_week", ((F.dayofweek("start_ts") + 5) % 7) + 1)
        .withColumn("day_name", F.date_format("start_ts", "EEEE"))
        .withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
        .withColumn("duration_min", F.round(F.col("duration_s") / 60.0, 2))
        .withColumn("is_round_trip", F.col("start_station_id") == F.col("end_station_id"))
    )


def validate_journeys(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split into (valid, quarantine). Quarantine rows carry a dq_reason."""
    rules = [
        (F.col("rental_id").isNull(), "missing_rental_id"),
        (F.col("start_ts").isNull(), "unparseable_start"),
        (F.col("end_ts").isNull(), "unparseable_end"),
        (F.col("end_ts") < F.col("start_ts"), "end_before_start"),
        (F.col("duration_s").isNull() | (F.col("duration_s") <= 0), "non_positive_duration"),
        (F.col("duration_s") > MAX_DURATION_SECONDS, "duration_over_24h"),
        (F.col("start_station_id").isNull() | (F.col("start_station_id") == ""), "missing_start_station"),
        (F.col("end_station_id").isNull() | (F.col("end_station_id") == ""), "missing_end_station"),
    ]
    reasons = [F.when(cond, F.lit(label)) for cond, label in rules]
    flagged = df.withColumn("dq_reason", F.concat_ws(";", *reasons))
    valid = flagged.filter(F.col("dq_reason") == "").drop("dq_reason")
    quarantine = flagged.filter(F.col("dq_reason") != "").withColumn(
        "quarantined_at", F.current_timestamp()
    )
    return valid, quarantine


def deduplicate_journeys(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested row per rental_id (re-uploaded files are safe)."""
    w = Window.partitionBy("rental_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


SILVER_COLUMNS = [
    "rental_id", "start_ts", "end_ts", "journey_date", "start_hour", "day_of_week",
    "day_name", "is_weekend", "duration_s", "duration_min", "start_station_id",
    "start_station_name", "end_station_id", "end_station_name", "is_round_trip",
    "bike_id", "bike_model", "_source_file", "_ingested_at",
]


def bronze_to_silver(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Full silver pipeline: standardise, derive, validate, deduplicate."""
    standard = add_derived_columns(standardise_journeys(bronze))
    valid, quarantine = validate_journeys(standard)
    return deduplicate_journeys(valid).select(*SILVER_COLUMNS), quarantine


def stations_to_silver(raw: DataFrame) -> DataFrame:
    """TfL BikePoint API JSON -> one row per docking station.

    TerminalName in additionalProperties matches 'Start station number' in
    current journey files, so it becomes the join key.
    """
    terminal = F.filter("additionalProperties", lambda p: p["key"] == "TerminalName")
    return (
        raw.select(
            F.col("id").alias("bikepoint_id"),
            F.element_at(terminal, 1)["value"].alias("station_id"),
            F.col("commonName").alias("station_name"),
            F.col("lat").cast("double").alias("latitude"),
            F.col("lon").cast("double").alias("longitude"),
            _try_cast(
                "element_at(filter(additionalProperties, p -> p.key = 'NbDocks'), 1).value", "int"
            ).alias("docks"),
        )
        .filter(F.col("station_id").isNotNull())
        .dropDuplicates(["station_id"])
    )


# ------------------------------------------------------------------ gold ----
def gold_daily_summary(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("journey_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("duration_s") / 3600.0, 1).alias("total_ride_hours"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.percentile_approx("duration_min", 0.5), 2).alias("median_duration_min"),
            F.round(F.avg(F.when(F.col("bike_model") == "EBIKE", 1).otherwise(0)), 4).alias("ebike_share"),
            F.round(F.avg(F.col("is_round_trip").cast("int")), 4).alias("round_trip_share"),
            F.countDistinct("start_station_id").alias("active_stations"),
            F.countDistinct("bike_id").alias("active_bikes"),
        )
        .withColumn("is_weekend", ((F.dayofweek("journey_date") + 5) % 7 + 1).isin(6, 7))
        .orderBy("journey_date")
    )


def gold_hourly_demand(silver: DataFrame) -> DataFrame:
    days_per_dow = silver.groupBy("day_of_week").agg(F.countDistinct("journey_date").alias("days_observed"))
    return (
        silver.groupBy("day_of_week", "day_name", "start_hour", "is_weekend")
        .agg(F.count("*").alias("trips"), F.round(F.avg("duration_min"), 2).alias("avg_duration_min"))
        .join(days_per_dow, "day_of_week")
        .withColumn("avg_trips_per_day", F.round(F.col("trips") / F.col("days_observed"), 1))
        .orderBy("day_of_week", "start_hour")
    )


def gold_station_daily_flow(silver: DataFrame) -> DataFrame:
    # group on id only (names can change spelling over time), keep one name per id
    departures = silver.groupBy("journey_date", F.col("start_station_id").alias("station_id")).agg(
        F.count("*").alias("departures"), F.max("start_station_name").alias("dep_name")
    )
    arrivals = silver.groupBy("journey_date", F.col("end_station_id").alias("station_id")).agg(
        F.count("*").alias("arrivals"), F.max("end_station_name").alias("arr_name")
    )
    return (
        departures.join(arrivals, ["journey_date", "station_id"], "full_outer")
        .select(
            "journey_date",
            "station_id",
            F.coalesce("dep_name", "arr_name").alias("station_name"),
            F.coalesce("departures", F.lit(0)).alias("departures"),
            F.coalesce("arrivals", F.lit(0)).alias("arrivals"),
        )
        .withColumn("net_flow", F.col("arrivals") - F.col("departures"))
        # positive net flow = bikes pile up (needs collection); negative = runs empty (needs restocking)
        .withColumn(
            "rebalancing_signal",
            F.when(F.col("net_flow") >= 10, "SURPLUS").when(F.col("net_flow") <= -10, "DEFICIT").otherwise("BALANCED"),
        )
    )


def gold_top_routes(silver: DataFrame, top_n: int = 50) -> DataFrame:
    w = Window.orderBy(F.col("trips").desc(), F.col("start_station_id"), F.col("end_station_id"))
    return (
        silver.filter(~F.col("is_round_trip"))
        .groupBy("start_station_id", "end_station_id")
        .agg(
            F.max("start_station_name").alias("start_station_name"),
            F.max("end_station_name").alias("end_station_name"),
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        )
        .withColumn("route_rank", F.row_number().over(w))
        .filter(F.col("route_rank") <= top_n)
    )


def gold_bike_model_mix(silver: DataFrame) -> DataFrame:
    total = silver.count()
    return (
        silver.groupBy("bike_model")
        .agg(F.count("*").alias("trips"), F.round(F.avg("duration_min"), 2).alias("avg_duration_min"))
        .withColumn("share", F.round(F.col("trips") / F.lit(total if total else 1), 4))
        .orderBy(F.col("trips").desc())
    )


def enrich_with_stations(df: DataFrame, stations: DataFrame) -> DataFrame:
    """Optional: add coordinates and dock counts when the BikePoint file was loaded."""
    dims = stations.select("station_id", "latitude", "longitude", "docks")
    return df.join(F.broadcast(dims), "station_id", "left")
