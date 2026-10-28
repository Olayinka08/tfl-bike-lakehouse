from datetime import date

import pytest
from conftest import journey

from lakehouse.quality import DataQualityError, assert_no_critical_failures, gold_checks, silver_checks, summarise
from lakehouse.transforms import (
    bronze_to_silver,
    gold_bike_model_mix,
    gold_daily_summary,
    gold_hourly_demand,
    gold_station_daily_flow,
)


def _pipeline(make_bronze, rows):
    silver, quarantine = bronze_to_silver(make_bronze(rows))
    return silver, quarantine.count()


def test_clean_data_passes_everything(make_bronze):
    silver, q = _pipeline(make_bronze, [journey(i) for i in range(1, 21)])
    n = silver.count()
    results = silver_checks(silver, q, today=date(2026, 6, 10))
    results += gold_checks(n, gold_daily_summary(silver), gold_hourly_demand(silver),
                           gold_station_daily_flow(silver), gold_bike_model_mix(silver))
    s = summarise(results)
    assert s["total"] == 14
    assert s["passed"] == 14
    assert_no_critical_failures(results)


def test_high_quarantine_rate_is_a_warning_not_a_failure(make_bronze):
    rows = [journey(1)] + [journey(i, start="bad") for i in range(2, 6)]
    silver, q = _pipeline(make_bronze, rows)
    results = silver_checks(silver, q, today=date(2026, 6, 10))
    s = summarise(results)
    assert "quarantine_rate" in s["warnings"]
    assert s["critical_failures"] == []


def test_stale_data_raises_freshness_warning(make_bronze):
    silver, q = _pipeline(make_bronze, [journey(1)])
    s = summarise(silver_checks(silver, q, freshness_days=30, today=date(2027, 1, 1)))
    assert "freshness" in s["warnings"]


def test_empty_silver_is_critical(make_bronze, spark):
    silver, q = _pipeline(make_bronze, [journey(1)])
    empty = silver.limit(0)
    results = silver_checks(empty, 0, today=date(2026, 6, 10))
    with pytest.raises(DataQualityError, match="row_count_positive"):
        assert_no_critical_failures(results)


def test_gold_reconciliation_catches_dropped_rows(make_bronze):
    silver, _ = _pipeline(make_bronze, [journey(i) for i in range(1, 6)])
    broken_daily = gold_daily_summary(silver.limit(3))  # simulate a gold bug losing rows
    results = gold_checks(5, broken_daily, gold_hourly_demand(silver),
                          gold_station_daily_flow(silver), gold_bike_model_mix(silver))
    assert "daily_reconciles_to_silver" in summarise(results)["critical_failures"]


def test_silver_duplicate_ids_are_critical(make_bronze):
    silver, _ = _pipeline(make_bronze, [journey(1), journey(2)])
    duplicated = silver.unionByName(silver.filter("rental_id = 1"))
    results = silver_checks(duplicated, 0, today=date(2026, 6, 10))
    assert "rental_id_unique" in summarise(results)["critical_failures"]
