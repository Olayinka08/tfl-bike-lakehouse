"""Central configuration for the lakehouse.

One switch, STORAGE_MODE, decides where raw files land:

* "volume" (default): a Unity Catalog Volume. Works on Databricks Free Edition.
* "adls":   an Azure Data Lake Storage Gen2 container registered as a Unity
            Catalog external location. Use this once you move to a paid Azure
            workspace. No notebook code changes, only these settings.

Every value can be overridden by a notebook widget or job parameter.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "workspace"
    storage_mode: str = "volume"  # "volume" or "adls"

    # Schemas follow the medallion pattern, plus one for pipeline operations
    bronze_schema: str = "tfl_bronze"
    silver_schema: str = "tfl_silver"
    gold_schema: str = "tfl_gold"
    ops_schema: str = "tfl_ops"

    # Volume used for raw landing, checkpoints and exports (volume mode)
    volume_name: str = "landing"

    # ADLS Gen2 settings (adls mode only)
    adls_account: str = ""      # e.g. "tfllakehouse"
    adls_container: str = ""    # e.g. "landing"

    # ---------- derived paths ----------
    @property
    def landing_root(self) -> str:
        if self.storage_mode == "adls":
            if not (self.adls_account and self.adls_container):
                raise ValueError("adls mode needs adls_account and adls_container")
            return f"abfss://{self.adls_container}@{self.adls_account}.dfs.core.windows.net"
        if self.storage_mode != "volume":
            raise ValueError(f"Unknown storage_mode: {self.storage_mode}")
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.volume_name}"

    @property
    def raw_journeys_path(self) -> str:
        return f"{self.landing_root}/raw/journeys"

    @property
    def raw_stations_path(self) -> str:
        return f"{self.landing_root}/raw/stations"

    @property
    def checkpoint_root(self) -> str:
        return f"{self.landing_root}/_checkpoints"

    @property
    def schema_root(self) -> str:
        return f"{self.landing_root}/_schemas"

    @property
    def export_path(self) -> str:
        return f"{self.landing_root}/exports/dashboard"

    # ---------- table names ----------
    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "ops": self.ops_schema,
        }[layer]
        return f"{self.catalog}.{schema}.{name}"


# Table names used across notebooks, kept in one place
BRONZE_JOURNEYS = "journeys_raw"
BRONZE_STATIONS = "stations_raw"
SILVER_JOURNEYS = "journeys"
SILVER_QUARANTINE = "journeys_quarantine"
SILVER_STATIONS = "stations"
GOLD_DAILY = "daily_summary"
GOLD_HOURLY = "hourly_demand"
GOLD_STATION_DAILY = "station_daily_flow"
GOLD_TOP_ROUTES = "top_routes"
GOLD_BIKE_MODEL = "bike_model_mix"
OPS_DQ_RESULTS = "dq_results"
OPS_RUN_LOG = "pipeline_runs"


def from_widgets(dbutils) -> LakehouseConfig:
    """Build config from Databricks widgets (falls back to defaults)."""

    def get(name, default):
        try:
            value = dbutils.widgets.get(name)
            return value if value else default
        except Exception:  # widget not defined
            return default

    d = LakehouseConfig()
    return LakehouseConfig(
        catalog=get("catalog", d.catalog),
        storage_mode=get("storage_mode", d.storage_mode),
        adls_account=get("adls_account", d.adls_account),
        adls_container=get("adls_container", d.adls_container),
    )
