"""Data quality checks for silver and gold.

Each check returns a dict that is written to the ops.dq_results Delta table,
so quality history is queryable and chartable. Critical failures stop the
Databricks job; warnings are logged only.
"""

from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CRITICAL = "critical"
WARNING = "warning"


def _result(check, layer, table, passed, severity, observed, expected, details=""):
    return {
        "check_name": check,
        "layer": layer,
        "table_name": table,
        "status": "PASS" if passed else "FAIL",
        "severity": severity,
        "observed": str(observed),
        "expected": str(expected),
        "details": details,
        "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }


# ---------------------------------------------------------------- silver ----
def silver_checks(
    silver: DataFrame,
    quarantine_count: int,
    table: str = "silver.journeys",
    max_quarantine_rate: float = 0.05,
    freshness_days: int = 120,
    today: Optional[date] = None,
) -> List[Dict]:
    stats = silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("rental_id").alias("distinct_ids"),
        F.sum(F.when(F.col("rental_id").isNull() | F.col("start_ts").isNull()
                     | F.col("end_ts").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.sum(F.when(F.col("end_ts") < F.col("start_ts"), 1).otherwise(0)).alias("time_travel"),
        F.sum(F.when((F.col("duration_s") <= 0) | (F.col("duration_s") > 86400), 1).otherwise(0)).alias("bad_duration"),
        F.max("journey_date").alias("latest_date"),
    ).collect()[0]

    rows = stats["rows"] or 0
    total_in = rows + quarantine_count
    q_rate = (quarantine_count / total_in) if total_in else 0.0
    today = today or date.today()
    latest = stats["latest_date"]
    age_days = (today - latest).days if latest else None

    return [
        _result("row_count_positive", "silver", table, rows > 0, CRITICAL, rows, "> 0"),
        _result("rental_id_unique", "silver", table, stats["distinct_ids"] == rows, CRITICAL,
                f"{stats['distinct_ids']} distinct / {rows} rows", "distinct == rows"),
        _result("no_null_keys", "silver", table, (stats["null_keys"] or 0) == 0, CRITICAL,
                stats["null_keys"] or 0, 0),
        _result("end_after_start", "silver", table, (stats["time_travel"] or 0) == 0, CRITICAL,
                stats["time_travel"] or 0, 0),
        _result("duration_in_range", "silver", table, (stats["bad_duration"] or 0) == 0, CRITICAL,
                stats["bad_duration"] or 0, 0, "duration must be in (0, 24h]"),
        _result("quarantine_rate", "silver", table, q_rate <= max_quarantine_rate, WARNING,
                f"{q_rate:.2%}", f"<= {max_quarantine_rate:.0%}",
                f"{quarantine_count} of {total_in} rows quarantined"),
        _result("freshness", "silver", table, age_days is not None and age_days <= freshness_days, WARNING,
                f"{age_days} days old" if age_days is not None else "no data", f"<= {freshness_days} days",
                f"latest journey_date = {latest}"),
    ]


# ------------------------------------------------------------------ gold ----
def gold_checks(silver_rows: int, daily: DataFrame, hourly: DataFrame,
                station_flow: DataFrame, bike_mix: DataFrame) -> List[Dict]:
    """Reconcile every gold table back to the silver row count."""
    daily_trips = daily.agg(F.sum("trips")).collect()[0][0] or 0
    daily_dupes = daily.count() - daily.select("journey_date").distinct().count()
    hourly_trips = hourly.agg(F.sum("trips")).collect()[0][0] or 0
    flow = station_flow.agg(F.sum("departures").alias("d"), F.sum("arrivals").alias("a"),
                            F.sum("net_flow").alias("n")).collect()[0]
    mix = bike_mix.agg(F.sum("trips").alias("t"), F.sum("share").alias("s")).collect()[0]

    return [
        _result("daily_reconciles_to_silver", "gold", "gold.daily_summary",
                daily_trips == silver_rows, CRITICAL, daily_trips, silver_rows),
        _result("daily_unique_dates", "gold", "gold.daily_summary",
                daily_dupes == 0, CRITICAL, daily_dupes, 0),
        _result("hourly_reconciles_to_silver", "gold", "gold.hourly_demand",
                hourly_trips == silver_rows, CRITICAL, hourly_trips, silver_rows),
        _result("departures_reconcile_to_silver", "gold", "gold.station_daily_flow",
                (flow["d"] or 0) == silver_rows, CRITICAL, flow["d"] or 0, silver_rows),
        _result("arrivals_reconcile_to_silver", "gold", "gold.station_daily_flow",
                (flow["a"] or 0) == silver_rows, CRITICAL, flow["a"] or 0, silver_rows),
        _result("net_flow_sums_to_zero", "gold", "gold.station_daily_flow",
                (flow["n"] or 0) == 0, CRITICAL, flow["n"] or 0, 0,
                "every departure is someone else's arrival"),
        _result("bike_mix_reconciles", "gold", "gold.bike_model_mix",
                (mix["t"] or 0) == silver_rows and abs((mix["s"] or 0) - 1.0) < 0.01, CRITICAL,
                f"trips={mix['t']}, share={mix['s']}", f"trips={silver_rows}, share=1.0"),
    ]


def summarise(results: List[Dict]) -> Dict:
    return {
        "total": len(results),
        "passed": sum(r["status"] == "PASS" for r in results),
        "critical_failures": [r["check_name"] for r in results
                              if r["status"] == "FAIL" and r["severity"] == CRITICAL],
        "warnings": [r["check_name"] for r in results
                     if r["status"] == "FAIL" and r["severity"] == WARNING],
    }


class DataQualityError(Exception):
    pass


def assert_no_critical_failures(results: List[Dict]) -> None:
    failed = summarise(results)["critical_failures"]
    if failed:
        raise DataQualityError(f"Critical data quality checks failed: {', '.join(failed)}")
