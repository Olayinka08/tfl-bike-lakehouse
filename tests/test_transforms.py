from datetime import date, datetime

from conftest import journey
from pyspark.sql import functions as F

from lakehouse.transforms import (
    bronze_to_silver,
    gold_bike_model_mix,
    gold_daily_summary,
    gold_hourly_demand,
    gold_station_daily_flow,
    gold_top_routes,
    normalise_column_names,
    snake_case,
    stations_to_silver,
)


# ------------------------------------------------------------ bronze -------
def test_snake_case_handles_tfl_headers():
    assert snake_case("Total duration (ms)") == "total_duration_ms"
    assert snake_case("EndStation Id") == "endstation_id"
    assert snake_case("  Start station number ") == "start_station_number"


def test_normalise_keeps_metadata_columns(spark):
    df = spark.createDataFrame([(1, "x")], ["Start date", "_source_file"])
    assert normalise_column_names(df).columns == ["start_date", "_source_file"]


# ------------------------------------------------------------ silver -------
def test_new_layout_is_typed_correctly(make_bronze):
    silver, quarantine = bronze_to_silver(make_bronze([journey(1)]))
    row = silver.collect()[0]
    assert quarantine.count() == 0
    assert row["rental_id"] == 1
    assert row["start_ts"] == datetime(2026, 6, 1, 8, 0)
    assert row["duration_s"] == 1200
    assert row["duration_min"] == 20.0
    assert row["journey_date"] == date(2026, 6, 1)
    assert row["bike_model"] == "CLASSIC"


def test_old_layout_is_supported(spark):
    old_header = ["Rental Id", "Duration", "Bike Id", "End Date", "EndStation Id",
                  "EndStation Name", "Start Date", "StartStation Id", "StartStation Name"]
    df = spark.createDataFrame(
        [("77", "600", "999", "01/03/2019 09:10", "12", "Soho", "01/03/2019 09:00", "34", "Bank")], old_header
    )
    from lakehouse.transforms import add_ingestion_metadata
    silver, _ = bronze_to_silver(normalise_column_names(add_ingestion_metadata(df)))
    row = silver.collect()[0]
    assert row["rental_id"] == 77
    assert row["duration_s"] == 600
    assert row["start_station_id"] == "34"
    assert row["bike_model"] == "UNKNOWN"


def test_ebike_model_is_normalised(make_bronze):
    silver, _ = bronze_to_silver(make_bronze([journey(1, model="PBSC_EBIKE")]))
    assert silver.collect()[0]["bike_model"] == "EBIKE"


def test_iso_weekday_and_weekend_flag(make_bronze):
    # 2026-06-01 is a Monday, 2026-06-07 is a Sunday
    rows = [journey(1), journey(2, start="2026-06-07 12:00", end="2026-06-07 12:20")]
    silver, _ = bronze_to_silver(make_bronze(rows))
    by_id = {r["rental_id"]: r for r in silver.collect()}
    assert (by_id[1]["day_of_week"], by_id[1]["is_weekend"]) == (1, False)
    assert (by_id[2]["day_of_week"], by_id[2]["is_weekend"], by_id[2]["day_name"]) == (7, True, "Sunday")


def test_round_trip_flag(make_bronze):
    silver, _ = bronze_to_silver(make_bronze([journey(1, e_id="001001", e_name="Waterloo")]))
    assert silver.collect()[0]["is_round_trip"] is True


def test_bad_rows_are_quarantined_with_reasons(make_bronze):
    rows = [
        journey(1),                                  # valid
        journey(2, start="not-a-date"),              # unparseable start
        journey(3, ms="-60000"),                     # negative duration
        journey(4, s_id=""),                         # missing start station
        journey(5, ms=str(30 * 3600 * 1000)),        # over 24h
        journey(6, start="2026-06-01 09:00", end="2026-06-01 08:00"),  # end before start
        ("abc",) + journey(7)[1:],                   # non-numeric rental id
    ]
    silver, quarantine = bronze_to_silver(make_bronze(rows))
    assert [r["rental_id"] for r in silver.collect()] == [1]
    reasons = {r["dq_reason"] for r in quarantine.collect()}
    assert "unparseable_start" in reasons
    assert "non_positive_duration" in reasons
    assert "missing_start_station" in reasons
    assert "duration_over_24h" in reasons
    assert "end_before_start" in reasons
    assert "missing_rental_id" in reasons
    assert quarantine.count() == 6


def test_duplicates_keep_latest_ingest(spark, make_bronze):
    bronze = make_bronze([journey(1, bike="OLD"), journey(1, bike="NEW")])
    # make the second row look ingested later
    bronze = bronze.withColumn(
        "_ingested_at",
        F.when(F.col("bike_number") == "NEW", F.lit("2030-01-01").cast("timestamp")).otherwise(F.col("_ingested_at")),
    )
    silver, _ = bronze_to_silver(bronze)
    rows = silver.collect()
    assert len(rows) == 1 and rows[0]["bike_id"] == "NEW"


def test_ansi_mode_does_not_crash_on_garbage(make_bronze):
    rows = [journey(1, start="??", end="??", ms="lots")]
    silver, quarantine = bronze_to_silver(make_bronze(rows))
    assert silver.count() == 0 and quarantine.count() == 1


# -------------------------------------------------------------- gold -------
def _silver(make_bronze):
    rows = [
        journey(1),
        journey(2, s_id="001002", s_name="Holborn", e_id="001001", e_name="Waterloo"),
        journey(3, model="PBSC_EBIKE"),
        journey(4, start="2026-06-02 18:00", end="2026-06-02 18:30", ms="1800000"),
        journey(5, e_id="001001", e_name="Waterloo"),  # round trip
    ]
    silver, _ = bronze_to_silver(make_bronze(rows))
    return silver


def test_daily_summary(make_bronze):
    daily = {r["journey_date"]: r for r in gold_daily_summary(_silver(make_bronze)).collect()}
    assert daily[date(2026, 6, 1)]["trips"] == 4
    assert daily[date(2026, 6, 1)]["ebike_share"] == 0.25
    assert daily[date(2026, 6, 2)]["trips"] == 1


def test_hourly_demand_sums_to_total(make_bronze):
    hourly = gold_hourly_demand(_silver(make_bronze))
    assert hourly.agg(F.sum("trips")).collect()[0][0] == 5


def test_station_flow_balances(make_bronze):
    flow = gold_station_daily_flow(_silver(make_bronze))
    totals = flow.agg(F.sum("departures"), F.sum("arrivals"), F.sum("net_flow")).collect()[0]
    assert tuple(totals) == (5, 5, 0)


def test_station_flow_one_row_per_station_day_even_if_names_differ(make_bronze):
    rows = [journey(1, s_name="Waterloo"), journey(2, s_name="Waterloo Stn")]
    silver, _ = bronze_to_silver(make_bronze(rows))
    flow = gold_station_daily_flow(silver).filter("station_id = '001001'")
    assert flow.count() == 1


def test_top_routes_excludes_round_trips_and_ranks(make_bronze):
    routes = gold_top_routes(_silver(make_bronze)).orderBy("route_rank").collect()
    assert routes[0]["start_station_id"] == "001001" and routes[0]["trips"] == 3
    assert all(r["start_station_id"] != r["end_station_id"] for r in routes)


def test_bike_model_mix_shares_sum_to_one(make_bronze):
    mix = gold_bike_model_mix(_silver(make_bronze)).collect()
    assert abs(sum(r["share"] for r in mix) - 1.0) < 1e-6


def test_stations_to_silver_extracts_terminal_name(spark):
    raw = spark.createDataFrame(
        [("BikePoints_1", "River Street, Clerkenwell", 51.52, -0.10,
          [{"key": "TerminalName", "value": "001023"}, {"key": "NbDocks", "value": "19"}])],
        "id string, commonName string, lat double, lon double, additionalProperties array<struct<key:string,value:string>>",
    )
    row = stations_to_silver(raw).collect()[0]
    assert (row["station_id"], row["docks"], row["latitude"]) == ("001023", 19, 51.52)
