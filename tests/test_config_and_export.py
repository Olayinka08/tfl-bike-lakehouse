import json

import pytest
from conftest import journey

from lakehouse.config import LakehouseConfig
from lakehouse.export import build_dashboard_payload, to_json
from lakehouse.transforms import (
    bronze_to_silver,
    gold_bike_model_mix,
    gold_daily_summary,
    gold_hourly_demand,
    gold_station_daily_flow,
    gold_top_routes,
)


def test_volume_paths_default_to_free_edition_layout():
    cfg = LakehouseConfig()
    assert cfg.landing_root == "/Volumes/workspace/tfl_bronze/landing"
    assert cfg.raw_journeys_path.endswith("/raw/journeys")
    assert cfg.table("gold", "daily_summary") == "workspace.tfl_gold.daily_summary"


def test_adls_mode_switches_only_the_storage_root():
    cfg = LakehouseConfig(storage_mode="adls", adls_account="tfllake", adls_container="landing")
    assert cfg.landing_root == "abfss://landing@tfllake.dfs.core.windows.net"
    assert cfg.table("silver", "journeys") == "workspace.tfl_silver.journeys"


def test_adls_mode_requires_account_and_container():
    with pytest.raises(ValueError):
        _ = LakehouseConfig(storage_mode="adls").landing_root


def test_dashboard_payload_is_valid_json(make_bronze):
    silver, quarantine = bronze_to_silver(make_bronze([journey(i) for i in range(1, 6)]))
    payload = build_dashboard_payload(
        gold_daily_summary(silver), gold_hourly_demand(silver), gold_station_daily_flow(silver),
        gold_top_routes(silver), gold_bike_model_mix(silver), quarantine.count(), [],
    )
    parsed = json.loads(to_json(payload))
    assert parsed["meta"]["total_trips"] == 5
    assert len(parsed["hourly"]["weekday"]) == 24
    assert parsed["daily"][0]["date"] == "2026-06-01"
