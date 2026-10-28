"""Turn gold tables into one compact JSON payload for the GitHub Pages dashboard."""

import json
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _rows(df: DataFrame) -> List[Dict]:
    return [r.asDict() for r in df.collect()]


def _jsonable(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def build_dashboard_payload(
    daily: DataFrame,
    hourly: DataFrame,
    station_flow: DataFrame,
    routes: DataFrame,
    bike_mix: DataFrame,
    quarantined_rows: int,
    dq_results: Optional[List[Dict]] = None,
    source_label: str = "TfL Santander Cycles open data",
) -> Dict:
    d = daily.agg(
        F.min("journey_date").alias("date_from"), F.max("journey_date").alias("date_to"),
        F.sum("trips").alias("total_trips"), F.count("*").alias("days"),
        (F.sum(F.col("avg_duration_min") * F.col("trips")) / F.sum("trips")).alias("avg_duration_min"),
        (F.sum(F.col("ebike_share") * F.col("trips")) / F.sum("trips")).alias("ebike_share"),
    ).collect()[0]

    hourly_profile = (
        hourly.groupBy("is_weekend", "start_hour")
        .agg(F.round(F.avg("avg_trips_per_day"), 1).alias("avg_trips"))
        .orderBy("start_hour")
    )
    profile = {"weekday": [0.0] * 24, "weekend": [0.0] * 24}
    for r in hourly_profile.collect():
        profile["weekend" if r["is_weekend"] else "weekday"][r["start_hour"]] = float(r["avg_trips"])

    stations = station_flow.groupBy("station_id").agg(
        F.max("station_name").alias("station_name"),
        F.sum("departures").alias("departures"),
        F.sum("arrivals").alias("arrivals"),
        F.sum("net_flow").alias("net_flow"),
    )
    station_count = stations.count()

    dq = dq_results or []
    payload = {
        "meta": {
            "source": source_label,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "date_from": _jsonable(d["date_from"]),
            "date_to": _jsonable(d["date_to"]),
            "days": d["days"],
            "total_trips": int(d["total_trips"] or 0),
            "quarantined_rows": int(quarantined_rows),
            "stations": station_count,
            "avg_duration_min": round(float(d["avg_duration_min"] or 0), 1),
            "ebike_share": round(float(d["ebike_share"] or 0), 4),
            "dq_passed": sum(1 for r in dq if r["status"] == "PASS"),
            "dq_total": len(dq),
        },
        "daily": [
            {"date": _jsonable(r["journey_date"]), "trips": r["trips"],
             "avg_duration_min": r["avg_duration_min"], "ebike_share": r["ebike_share"],
             "is_weekend": r["is_weekend"]}
            for r in _rows(daily.orderBy("journey_date"))
        ],
        "hourly": profile,
        "top_stations": _rows(stations.orderBy(F.col("departures").desc()).limit(15)),
        "surplus_stations": _rows(stations.orderBy(F.col("net_flow").desc()).limit(8)),
        "deficit_stations": _rows(stations.orderBy(F.col("net_flow").asc()).limit(8)),
        "top_routes": _rows(routes.orderBy("route_rank").limit(10).select(
            "route_rank", "start_station_name", "end_station_name", "trips", "avg_duration_min")),
        "bike_mix": _rows(bike_mix.select("bike_model", "trips", "share", "avg_duration_min")),
        "dq_results": [
            {k: _jsonable(r[k]) for k in ("check_name", "layer", "status", "severity", "observed", "expected")}
            for r in dq
        ],
    }
    return payload


def to_json(payload: Dict) -> str:
    return json.dumps(payload, indent=1, default=_jsonable)
